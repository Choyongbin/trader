package uiapi

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"binance_trader/internal/credentials"
	"binance_trader/internal/external/asof"
	"binance_trader/internal/live/autopipeline"
	"binance_trader/internal/live/binance"
	"binance_trader/internal/live/runtimefeature"
	"binance_trader/internal/live/warmstate"
	"github.com/gorilla/websocket"
)

const (
	FeatureRegistryHash            = "a37306b80ecbf701103ab691445d07a39624ce995d84f8206d6314dae3045bef"
	EntryPolicyHash                = "4fae120d9d54a5732dbf2009950beba69773bb0a28a7c3915b616e295cd3dd31"
	RiskPolicyHash                 = "21e3332b5cf286dd39827973a259fbd91dde325ca067f6a067237bb6f24596f7"
	FrozenModelTimeAlignmentStatus = "UNVERIFIED"
	wsSendQueueSize                = 128
	wsWriteTimeout                 = 5 * time.Second
)

type wsClient struct {
	server    *Server
	conn      *websocket.Conn
	send      chan []byte
	done      chan struct{}
	closeOnce sync.Once
}

type environmentState struct {
	AutoRunning    bool
	AutoState      string
	KillSwitch     bool
	ExchangeKnown  bool
	Balance        Balance
	Positions      []Position
	Orders         []Order
	Processed      map[string]Order
	LastAutoResult *autopipeline.Result
}

type Options struct {
	WarmupPath             string
	PaperOrders            map[TradingEnvironment]bool
	FixtureConnected       map[TradingEnvironment]bool
	TestnetBackend         TestnetBackend
	SpotAssets             SpotAssetProvider
	AutoPipeline           *autopipeline.Pipeline
	AutoFeatureSource      bool
	AutoIntentSink         autopipeline.IntentSink
	LiveFeatureRuntime     bool
	LiveBootstrap          bool
	BootstrapStabilization time.Duration
	ListenAddress          string
	AutoExecutionStatePath string
	LiveSnapshotPath       string
}

type Server struct {
	mu                       sync.RWMutex
	startedAt                time.Time
	provider                 credentials.Provider
	credentials              credentials.CredentialStore
	credError                string
	csrf                     string
	static                   fs.FS
	warmupPath               string
	runtimePath              string
	states                   map[TradingEnvironment]*environmentState
	market                   Market
	marketSourceUpdated      map[string]time.Time
	blockerSince             map[TradingEnvironment]map[string]time.Time
	now                      func() time.Time
	logs                     []string
	clients                  map[*wsClient]struct{}
	upgrader                 websocket.Upgrader
	paperOrders              map[TradingEnvironment]bool
	testnet                  TestnetBackend
	spotAssets               SpotAssetProvider
	autoPipeline             *autopipeline.Pipeline
	autoFeatureSource        bool
	autoIntentSink           autopipeline.IntentSink
	liveFeatureRuntime       bool
	liveBootstrap            bool
	bootstrapStabilization   time.Duration
	runtimeStats             runtimefeature.Stats
	featureNotReadyDecisions uint64
	featureLatency           latencyRing
	autoIntents              uint64
	testnetRefreshMu         sync.Mutex
	// entrySubmitMu is the common side-effect boundary for manual and automatic
	// entries. STOP takes the same lock, so once STOP returns no entry which had
	// not crossed this boundary can be submitted.
	entrySubmitMu          sync.Mutex
	autoExecutionMu        sync.Mutex
	autoExecutionHook      func(string) // deterministic test-only scheduling hook
	autoLifecycleMu        sync.Mutex
	autoDecisionQueue      chan queuedAutoDecision
	autoLastDecisionMs     int64
	autoQueueOverflows     uint64
	autoStaleDecisions     uint64
	listenAddress          string
	autoExecutionStatePath string
	liveSnapshotPath       string
	autoExecutionState     *autoExecutionState
	autoRecoveryBlocked    bool
	// Tests that exercise the broker state machine may opt past the research
	// gate directly. No runtime option or environment variable can enable it.
	allowModelInTests        bool
	lastTestnetRefresh       time.Time
	autoOwnedPosition        bool
	autoProtectiveIDs        []string
	autoExitDeadline         time.Time
	autoCloseRequestID       string
	marketMessages           uint64
	futuresMessages          uint64
	spotMessages             uint64
	futuresRawMessages       uint64
	spotRawMessages          uint64
	markRawMessages          uint64
	publicParseErrors        uint64
	futuresReconnects        uint64
	spotReconnects           uint64
	sourceStaleEvents        uint64
	featureDecisions         uint64
	eligibleDecisions        uint64
	noTradeSignals           uint64
	longSignals              uint64
	shortSignals             uint64
	actualOrderSubmits       uint64
	autoExecutionAttempts    uint64
	confirmedEntryFills      uint64
	protectiveOrderSubmits   uint64
	failedUnknownSubmissions uint64
	reconciliationEvents     uint64
	marketReceiveLatency     latencyRing
	pipelineLatency          latencyRing
	inferenceLatency         latencyRing
	policyLatency            latencyRing
	manualRequests           uint64
	apiErrors                uint64
}

func NewServer(provider credentials.Provider, static fs.FS, options Options) (*Server, error) {
	if provider == nil || static == nil {
		return nil, fmt.Errorf("credential provider and static filesystem are required")
	}
	tokenBytes := make([]byte, 24)
	if _, err := rand.Read(tokenBytes); err != nil {
		return nil, err
	}
	s := &Server{
		startedAt: time.Now().UTC(),
		provider:  provider,
		csrf:      hex.EncodeToString(tokenBytes),
		static:    static,
		states: map[TradingEnvironment]*environmentState{
			TradingEnvironmentTestnet: {AutoState: "STOPPED", Balance: Balance{Status: "ACCOUNT NOT CONNECTED"}, Processed: map[string]Order{}},
			TradingEnvironmentMainnet: {AutoState: "STOPPED", Balance: Balance{Status: "ACCOUNT NOT CONNECTED"}, Processed: map[string]Order{}},
		},
		marketSourceUpdated: map[string]time.Time{},
		blockerSince: map[TradingEnvironment]map[string]time.Time{
			TradingEnvironmentTestnet: {}, TradingEnvironmentMainnet: {},
		},
		now:                    time.Now,
		clients:                map[*wsClient]struct{}{},
		paperOrders:            options.PaperOrders,
		warmupPath:             options.WarmupPath,
		testnet:                options.TestnetBackend,
		spotAssets:             options.SpotAssets,
		autoPipeline:           options.AutoPipeline,
		autoFeatureSource:      options.AutoFeatureSource,
		autoIntentSink:         options.AutoIntentSink,
		liveFeatureRuntime:     options.LiveFeatureRuntime,
		liveBootstrap:          options.LiveBootstrap,
		bootstrapStabilization: options.BootstrapStabilization,
	}
	if s.bootstrapStabilization <= 0 {
		s.bootstrapStabilization = 5 * time.Minute
	}
	s.listenAddress = options.ListenAddress
	if s.listenAddress == "" {
		s.listenAddress = "127.0.0.1:8080"
	}
	s.autoExecutionStatePath = options.AutoExecutionStatePath
	if s.autoExecutionStatePath == "" && options.LiveFeatureRuntime {
		s.autoExecutionStatePath = filepath.FromSlash("data/live_state/BTCUSDT/v1/auto-execution-state.json")
	}
	s.liveSnapshotPath = options.LiveSnapshotPath
	if s.liveSnapshotPath == "" {
		s.liveSnapshotPath = filepath.FromSlash(warmstate.RuntimeSnapshotPath)
	}
	s.autoDecisionQueue = make(chan queuedAutoDecision, 8)
	if s.paperOrders == nil {
		s.paperOrders = map[TradingEnvironment]bool{}
	}
	if s.autoIntentSink == nil {
		s.autoIntentSink = &autopipeline.RecordingBroker{}
	}
	if s.warmupPath == "" {
		s.warmupPath = filepath.FromSlash("data/live_capture/BTCUSDT/v1/capture-state.json")
		s.runtimePath = filepath.FromSlash(warmstate.RuntimeStatusPath)
	} else {
		s.runtimePath = filepath.Join(filepath.Dir(s.warmupPath), "runtime-status.json")
	}
	for environment, connected := range options.FixtureConnected {
		if connected {
			wallet, available, margin, used, pnl := 10000.0, 9500.0, 10000.0, 500.0, 0.0
			s.states[environment].Balance = Balance{true, "CONNECTED", &wallet, &available, &margin, &used, &pnl}
		}
	}
	s.reloadCredentials()
	s.loadAutoExecutionState()
	if warmstate.Read(s.warmupPath, time.Now()).Status == "GAP" {
		s.logLocked("Warm state rejected due GAP")
	}
	s.upgrader.CheckOrigin = s.sameOrigin
	return s, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/status", s.getStatus)
	mux.HandleFunc("GET /api/environments", s.getEnvironments)
	mux.HandleFunc("GET /api/account", s.getAccount)
	mux.HandleFunc("GET /api/portfolio", s.getPortfolio)
	mux.HandleFunc("GET /api/positions", s.getPositions)
	mux.HandleFunc("GET /api/market", s.getMarket)
	mux.HandleFunc("GET /api/models", s.getModels)
	mux.HandleFunc("GET /api/auto-trading", s.getAutoTrading)
	mux.HandleFunc("GET /api/entry-blockers", s.getEntryBlockers)
	mux.HandleFunc("POST /api/auto-trading/start", s.requireCSRF(s.startAuto))
	mux.HandleFunc("POST /api/auto-trading/stop", s.requireCSRF(s.stopAuto))
	mux.HandleFunc("POST /api/auto-trading/emergency-stop", s.requireCSRF(s.stopAuto))
	mux.HandleFunc("GET /api/orders", s.getOrders)
	mux.HandleFunc("POST /api/manual-order", s.requireCSRF(s.manualOrder))
	mux.HandleFunc("POST /api/position/close", s.requireCSRF(s.closePosition))
	mux.HandleFunc("POST /api/credentials/reload", s.requireCSRF(s.reloadCredentialsHTTP))
	mux.HandleFunc("GET /api/system", s.getSystem)
	mux.HandleFunc("GET /api/logs", s.getLogs)
	mux.HandleFunc("GET /ws", s.webSocket)
	mux.Handle("/", http.FileServer(http.FS(s.static)))
	return s.securityHeaders(mux)
}

func (s *Server) securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.sameOrigin(r) {
			http.Error(w, "cross-origin request rejected", http.StatusForbidden)
			return
		}
		_, port, _ := strings.Cut(s.listenAddress, ":")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; connect-src 'self' ws://127.0.0.1:"+port+" ws://localhost:"+port+"; style-src 'self' 'unsafe-inline'; script-src 'self'; img-src 'self' data:; base-uri 'none'; frame-ancestors 'none'")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		next.ServeHTTP(w, r)
	})
}

func (s *Server) sameOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	_, port, ok := strings.Cut(s.listenAddress, ":")
	return ok && (origin == "http://127.0.0.1:"+port || origin == "http://localhost:"+port)
}

func (s *Server) requireCSRF(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := r.Header.Get("X-CSRF-Token")
		if len(token) != len(s.csrf) || subtle.ConstantTimeCompare([]byte(token), []byte(s.csrf)) != 1 {
			writeError(w, http.StatusForbidden, "CSRF token rejected")
			return
		}
		next(w, r)
	}
}

func (s *Server) reloadCredentials() {
	store, err := s.provider.Load()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		s.credentials = credentials.CredentialStore{}
		s.credError = "credential file unavailable"
		return
	}
	s.credentials, s.credError = store, ""
}

func (s *Server) credentialAvailable(environment TradingEnvironment) bool {
	if environment == TradingEnvironmentTestnet {
		return s.credentials.Testnet.Available()
	}
	return s.credentials.Mainnet.Available()
}

func (s *Server) warmup() map[string]any {
	status := warmstate.ReadCurrent(s.warmupPath, s.runtimePath, time.Now())
	previous := warmstate.Read(s.warmupPath, time.Now())
	return map[string]any{"status": status.Status, "required_ms": status.RequiredMs, "available_ms": status.AvailableMs, "missing_ms": status.MissingMs, "progress_percent": status.ProgressPercent, "ready": status.Ready, "last_checkpoint_ms": status.LastCheckpointMs, "last_event_time_ms": status.LastEventTimeMs, "last_receive_time_ms": status.LastReceiveTimeMs, "current_capture_start_ms": status.CurrentCaptureStartMs, "input_connected": status.InputConnected, "source": status.Source, "futures_bars": status.FuturesBars, "spot_bars": status.SpotBars, "futures_gaps": status.FuturesGaps, "spot_gaps": status.SpotGaps, "sessions": status.SessionCount, "external_observations": status.ExternalObservations, "bootstrap_state": status.BootstrapState, "bootstrap_ready": status.BootstrapReady, "handoff_ready": status.HandoffReady, "stabilization_ready": status.StabilizationReady, "stabilization_elapsed": status.StabilizationElapsed, "stabilization_ms": status.StabilizationMs, "stabilization_target_ms": status.StabilizationTargetMs, "bootstrap_blocker": status.BootstrapBlocker, "blocker_source": status.BlockerSource, "futures_history_ms": status.FuturesHistoryMs, "spot_history_ms": status.SpotHistoryMs, "historical_capture": map[string]any{"status": previous.Status, "available_ms": previous.AvailableMs, "last_checkpoint_ms": previous.LastCheckpointMs}}
}

func (s *Server) getStatus(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	presence, market := s.credentials.Presence(), s.market
	credError := s.credError
	testAuto, mainAuto := s.states[TradingEnvironmentTestnet].AutoRunning, s.states[TradingEnvironmentMainnet].AutoRunning
	s.mu.RUnlock()
	mode := "PUBLIC_ONLY SHADOW"
	defaultEnvironment := "PUBLIC_ONLY"
	if strings.EqualFold(strings.TrimSpace(os.Getenv("BINANCE_ENV")), "TESTNET") {
		mode = "TESTNET READ ONLY"
		defaultEnvironment = "TESTNET"
	}
	if defaultEnvironment == "TESTNET" && s.testnet != nil && s.ordersEnabled(TradingEnvironmentTestnet) {
		mode = "TESTNET LIVE ORDERS"
		if _, ok := s.autoBackend(); ok && s.autoOrdersEnabled(TradingEnvironmentTestnet) {
			mode = "TESTNET LIVE AUTO ORDERS"
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"service": "BTCUSDT TRADING CONSOLE V1", "execution_default": defaultEnvironment, "execution_mode": mode, "market_data": "BINANCE PUBLIC", "csrf_token": s.csrf, "credentials": presence, "credential_error": credError, "warmup": s.warmup(), "market": market, "auto_trading": map[string]bool{"TESTNET": testAuto, "MAINNET": mainAuto}, "mainnet_orders_default_enabled": false})
}

func (s *Server) getEnvironments(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, []map[string]any{{"id": "TESTNET", "label": "BINANCE DEMO", "orders_enabled": s.ordersEnabled(TradingEnvironmentTestnet)}, {"id": "MAINNET", "label": "MAINNET DISABLED", "orders_enabled": s.ordersEnabled(TradingEnvironmentMainnet)}})
}

func environmentFromRequest(w http.ResponseWriter, r *http.Request) (TradingEnvironment, bool) {
	environment, ok := ParseEnvironment(r.URL.Query().Get("environment"))
	if !ok {
		writeError(w, http.StatusBadRequest, "explicit environment is required")
	}
	return environment, ok
}

func (s *Server) getAccount(w http.ResponseWriter, r *http.Request) {
	environment, ok := environmentFromRequest(w, r)
	if !ok {
		return
	}
	if environment == TradingEnvironmentTestnet && s.testnet != nil {
		s.refreshTestnet(r.Context())
	}
	s.mu.RLock()
	value := s.states[environment].Balance
	s.mu.RUnlock()
	writeJSON(w, http.StatusOK, map[string]any{"environment": environment, "account": value})
}
func (s *Server) getPositions(w http.ResponseWriter, r *http.Request) {
	environment, ok := environmentFromRequest(w, r)
	if !ok {
		return
	}
	if environment == TradingEnvironmentTestnet && s.testnet != nil {
		s.refreshTestnet(r.Context())
	}
	s.mu.RLock()
	rows := append([]Position(nil), s.states[environment].Positions...)
	s.mu.RUnlock()
	writeJSON(w, http.StatusOK, map[string]any{"environment": environment, "positions": rows})
}
func (s *Server) getOrders(w http.ResponseWriter, r *http.Request) {
	environment, ok := environmentFromRequest(w, r)
	if !ok {
		return
	}
	if environment == TradingEnvironmentTestnet && s.testnet != nil {
		s.refreshTestnet(r.Context())
	}
	s.mu.RLock()
	rows := append([]Order(nil), s.states[environment].Orders...)
	s.mu.RUnlock()
	writeJSON(w, http.StatusOK, map[string]any{"environment": environment, "orders": rows})
}
func (s *Server) getMarket(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	value := s.market
	s.mu.RUnlock()
	writeJSON(w, http.StatusOK, value)
}
func (s *Server) getModels(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, []ModelProfile{{"btc-feature-v2-production-v1", "BTC Feature V2 Production V1", "V1", FeatureRegistryHash, EntryPolicyHash, RiskPolicyHash, "FROZEN_TIME_ALIGNMENT_UNVERIFIED", 5}})
}
func (s *Server) getAutoTrading(w http.ResponseWriter, r *http.Request) {
	environment, ok := environmentFromRequest(w, r)
	if !ok {
		return
	}
	if environment == TradingEnvironmentTestnet && s.testnet != nil {
		s.refreshTestnet(r.Context())
	}
	blockers := s.entryBlockers(environment)
	s.mu.RLock()
	running := s.states[environment].AutoRunning
	autoState := s.states[environment].AutoState
	lastResult := s.states[environment].LastAutoResult
	accountConnected := s.states[environment].Balance.Connected
	positionOpen := len(s.states[environment].Positions) != 0
	marketConnected := s.market.Connected
	featureSource := s.autoFeatureSource && (!s.liveFeatureRuntime || s.runtimeStats.Status.InputConnected)
	s.mu.RUnlock()
	warmup := s.warmup()
	warmupReady, _ := warmup["ready"].(bool)
	_, autoBrokerReady := s.autoBackend()
	brokerReady := environment == TradingEnvironmentTestnet && s.testnet != nil && autoBrokerReady
	modelReady := s.autoPipeline != nil
	autoOrdersEnabled := s.autoOrdersEnabled(environment)
	operationalBlockers := 0
	for _, blocker := range blockers {
		if blocker.Code != "AUTO_TRADING_RUNNING" {
			operationalBlockers++
		}
	}
	overall := operationalBlockers == 0 && modelReady && featureSource && autoOrdersEnabled
	reason := ""
	for _, blocker := range blockers {
		if blocker.Code != "AUTO_TRADING_RUNNING" {
			reason = blocker.Code
			break
		}
	}
	startBlockedReason := ""
	if len(blockers) != 0 {
		startBlockedReason = blockers[0].Code
	}
	signal := "NOT_READY"
	if warmupReady && featureSource && lastResult != nil {
		signal = lastResult.FinalSignal
	} else {
		lastResult = nil
	}
	if reason == "" && !autoOrdersEnabled {
		reason = "AUTO_ORDERS_DISABLED"
	}
	writeJSON(w, http.StatusOK, map[string]any{"environment": environment, "running": running, "state": autoState, "signal": signal, "last_result": lastResult, "selected_model": autopipeline.ProfileID, "blockers": blockers, "start_blocked_reason": startBlockedReason, "readiness": map[string]any{"market_public_data": marketConnected, "feature_warmup": warmupReady, "feature_registry": modelReady, "model": modelReady, "model_time_alignment_status": FrozenModelTimeAlignmentStatus, "model_entry_validated": s.allowModelInTests, "entry_policy": modelReady, "risk_policy": modelReady, "account": accountConnected, "broker": brokerReady, "feature_source": featureSource, "position": map[bool]string{true: "OPEN", false: "FLAT"}[positionOpen], "orders_enabled": s.ordersEnabled(environment), "auto_orders_enabled": autoOrdersEnabled, "overall": overall, "blocked_reason": reason}})
}

func (s *Server) startAuto(w http.ResponseWriter, r *http.Request) {
	environment, ok := environmentFromRequest(w, r)
	if !ok {
		return
	}
	var selection struct {
		ModelProfileID string `json:"model_profile_id"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<10)).Decode(&selection); err != nil || selection.ModelProfileID != autopipeline.ProfileID {
		writeError(w, http.StatusBadRequest, "frozen model profile required")
		return
	}
	s.entrySubmitMu.Lock()
	defer s.entrySubmitMu.Unlock()
	if environment == TradingEnvironmentTestnet && s.testnet != nil {
		s.refreshTestnet(r.Context())
	}
	blockers := s.entryBlockers(environment)
	if len(blockers) != 0 {
		s.mu.Lock()
		s.states[environment].AutoState = "BLOCKED"
		s.logLocked("Auto START blocked: " + blockers[0].Code)
		s.mu.Unlock()
		writeJSON(w, http.StatusConflict, map[string]any{"environment": environment, "running": false, "state": "BLOCKED", "error": "auto trading safety gate rejected start", "blocked_reason": blockers[0].Code})
		return
	}
	if environment != TradingEnvironmentTestnet {
		writeJSON(w, http.StatusConflict, map[string]any{"environment": environment, "running": false, "state": "BLOCKED", "error": "auto trading safety gate rejected start", "blocked_reason": "BROKER_UNAVAILABLE"})
		return
	}
	if _, ok := s.autoBackend(); !ok {
		writeJSON(w, http.StatusConflict, map[string]any{"environment": environment, "running": false, "state": "BLOCKED", "error": "auto trading safety gate rejected start", "blocked_reason": "AUTO_BROKER_UNAVAILABLE"})
		return
	}
	if !s.autoOrdersEnabled(environment) {
		s.mu.Lock()
		s.states[environment].AutoState = "BLOCKED"
		s.logLocked("Auto START blocked: AUTO_ORDERS_DISABLED")
		s.mu.Unlock()
		writeJSON(w, http.StatusConflict, map[string]any{"environment": environment, "running": false, "state": "BLOCKED", "error": "auto trading safety gate rejected start", "blocked_reason": "AUTO_ORDERS_DISABLED"})
		return
	}
	s.mu.Lock()
	s.states[environment].AutoState = "STARTING"
	s.states[environment].AutoRunning = true
	s.states[environment].AutoState = "RUNNING"
	s.logLocked("Demo auto execution started: " + string(environment))
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"environment": environment, "running": true, "state": "RUNNING", "execution": "TESTNET LIVE AUTO ORDERS"})
}

func (s *Server) stopAuto(w http.ResponseWriter, r *http.Request) {
	environment, ok := environmentFromRequest(w, r)
	if !ok {
		return
	}
	s.mu.Lock()
	s.states[environment].AutoState = "STOPPING"
	s.states[environment].AutoRunning = false
	if r.URL.Path == "/api/auto-trading/emergency-stop" {
		s.states[environment].KillSwitch = true
		s.logLocked("Kill switch activated: " + string(environment))
	}
	s.logLocked("new entries stopped: " + string(environment))
	s.mu.Unlock()

	// Wait until any execution which already crossed the common boundary has
	// either observed STOP and aborted or completed its exchange submission.
	s.entrySubmitMu.Lock()
	defer s.entrySubmitMu.Unlock()
	s.mu.Lock()
	riskState := "NONE"
	switch {
	case environment == TradingEnvironmentTestnet && s.autoRecoveryBlocked:
		s.states[environment].AutoState = "UNKNOWN_EXECUTION_STATE"
		riskState = "OPERATOR_RECONCILIATION_REQUIRED"
	case environment == TradingEnvironmentTestnet && s.autoOwnedPosition:
		s.states[environment].AutoState = "POSITION_PROTECTED_STOPPED"
		riskState = "EXISTING_POSITION_RISK_MANAGEMENT_ACTIVE"
	default:
		s.states[environment].AutoState = "STOPPED"
	}
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"environment": environment, "running": false, "existing_risk_management": "PRESERVED", "existing_risk_state": riskState, "kill_switch_active": r.URL.Path == "/api/auto-trading/emergency-stop"})
}

func (s *Server) ordersEnabled(environment TradingEnvironment) bool {
	if environment == TradingEnvironmentMainnet {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(os.Getenv("BINANCE_ENV")), "TESTNET") && strings.EqualFold(strings.TrimSpace(os.Getenv("BINANCE_TESTNET_ENABLE_ORDERS")), "true")
}

func (s *Server) manualOrder(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.manualRequests++
	s.mu.Unlock()
	var request ManualOrderRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid manual order")
		return
	}
	if _, ok := ParseEnvironment(string(request.Environment)); !ok || request.Symbol != "BTCUSDT" || (request.Side != "LONG" && request.Side != "SHORT") || request.OrderType != "MARKET" || request.MarginAmountUSDT <= 0 || request.Leverage < 1 || request.Leverage > 125 || strings.TrimSpace(request.RequestID) == "" {
		writeError(w, http.StatusBadRequest, "manual order validation failed")
		return
	}
	if request.Environment == TradingEnvironmentMainnet && request.LiveConfirmation != "CONFIRM LIVE ORDER" {
		writeError(w, http.StatusForbidden, "explicit Mainnet confirmation required")
		return
	}
	actualTestnet := request.Environment == TradingEnvironmentTestnet && s.testnet != nil && !s.paperOrders[request.Environment]
	if actualTestnet {
		s.entrySubmitMu.Lock()
		defer s.entrySubmitMu.Unlock()
		s.refreshTestnet(r.Context())
	}
	s.mu.Lock()
	state := s.states[request.Environment]
	if existing, ok := state.Processed[request.RequestID]; ok {
		s.mu.Unlock()
		writeJSON(w, http.StatusOK, existing)
		return
	}
	if state.AutoRunning || len(state.Positions) != 0 || !s.credentialAvailable(request.Environment) || !s.ordersEnabled(request.Environment) || (request.Environment == TradingEnvironmentTestnet && !s.paperOrders[request.Environment] && s.testnet == nil) || request.Environment == TradingEnvironmentMainnet {
		s.mu.Unlock()
		writeError(w, http.StatusConflict, "manual order safety gate rejected submit")
		return
	}
	if state.KillSwitch || (request.Environment == TradingEnvironmentTestnet && !s.paperOrders[request.Environment] && !state.ExchangeKnown) {
		s.mu.Unlock()
		writeError(w, http.StatusConflict, "new entry blocked: kill switch or exchange state unknown")
		return
	}
	s.mu.Unlock()
	if actualTestnet {
		s.mu.Lock()
		s.actualOrderSubmits++
		s.mu.Unlock()
		execution, err := s.testnet.SubmitManual(r.Context(), request)
		if err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		s.mu.Lock()
		state.Processed[request.RequestID] = execution.Order
		state.Orders = append([]Order{execution.Order}, execution.Protective...)
		state.Positions = []Position{execution.Position}
		s.logLocked("Testnet manual order accepted: " + request.Side)
		s.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]any{"order": execution.Order, "position": execution.Position, "protective_orders": execution.Protective, "execution": "TESTNET LIVE ORDERS"})
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state = s.states[request.Environment]
	price := s.market.FuturesPrice
	if price <= 0 {
		price = 100000
	}
	notional := request.MarginAmountUSDT * float64(request.Leverage)
	quantity := math.Floor((notional/price)*1000) / 1000
	if quantity <= 0 {
		writeError(w, http.StatusBadRequest, "normalized quantity is zero")
		return
	}
	order, position, protective := buildPaperExecution(request, price, quantity)
	state.Processed[request.RequestID] = order
	state.Orders = append([]Order{order}, state.Orders...)
	state.Orders = append(protective, state.Orders...)
	state.Positions = []Position{position}
	s.logLocked("paper manual order accepted: " + string(request.Environment) + " " + request.Side)
	writeJSON(w, http.StatusOK, map[string]any{"order": order, "position": position, "execution": "PAPER_VALIDATION"})
}

func buildPaperExecution(request ManualOrderRequest, price, executedQuantity float64) (Order, Position, []Order) {
	now := time.Now().UTC()
	order := Order{now, request.Environment, request.Symbol, request.Side, request.OrderType, executedQuantity, price, "FILLED_PAPER", request.RequestID, "", false}
	position := Position{Environment: request.Environment, Symbol: request.Symbol, Side: request.Side, QuantityBTC: executedQuantity, NotionalUSDT: executedQuantity * price, EntryPrice: price, MarkPrice: price, Leverage: request.Leverage, MarginUSDT: request.MarginAmountUSDT, OpenedAt: now}
	protective := []Order{}
	if request.TakeProfit != nil && request.TakeProfit.Enabled {
		value := protectivePrice(price, request.Side, true, *request.TakeProfit)
		position.TakeProfit = &value
		protective = append(protective, Order{now, request.Environment, request.Symbol, opposite(request.Side), "TAKE_PROFIT_MARKET", executedQuantity, value, "NEW_PAPER", request.RequestID + "-tp", "TP", true})
	}
	if request.StopLoss != nil && request.StopLoss.Enabled {
		value := protectivePrice(price, request.Side, false, *request.StopLoss)
		position.StopLoss = &value
		protective = append(protective, Order{now, request.Environment, request.Symbol, opposite(request.Side), "STOP_MARKET", executedQuantity, value, "NEW_PAPER", request.RequestID + "-sl", "SL", true})
	}
	return order, position, protective
}

func protectivePrice(entry float64, side string, takeProfit bool, cfg ProtectiveOrderConfig) float64 {
	if cfg.Mode == "PRICE" {
		return cfg.Value
	}
	direction := 1.0
	if side == "SHORT" {
		direction = -1
	}
	if !takeProfit {
		direction *= -1
	}
	return entry * (1 + direction*cfg.Value/100)
}
func opposite(side string) string {
	if side == "LONG" {
		return "SHORT"
	}
	return "LONG"
}

func (s *Server) closePosition(w http.ResponseWriter, r *http.Request) {
	environment, ok := environmentFromRequest(w, r)
	if !ok {
		return
	}
	if environment == TradingEnvironmentMainnet {
		writeError(w, http.StatusForbidden, "Mainnet execution disabled")
		return
	}
	if environment == TradingEnvironmentTestnet && s.testnet != nil && !s.paperOrders[environment] {
		s.entrySubmitMu.Lock()
		defer s.entrySubmitMu.Unlock()
		if !s.ordersEnabled(environment) {
			writeError(w, http.StatusConflict, "order safety gate rejected close")
			return
		}
		requestID := r.URL.Query().Get("request_id")
		if requestID == "" {
			requestID = fmt.Sprintf("ui-close-%d", time.Now().UnixMilli())
		}
		s.mu.Lock()
		s.actualOrderSubmits++
		s.mu.Unlock()
		s.mu.RLock()
		ownedProtective := append([]string(nil), s.autoProtectiveIDs...)
		if !s.autoOwnedPosition {
			ownedProtective = make([]string, 0, 2*len(s.states[environment].Processed))
			for ownerID := range s.states[environment].Processed {
				ownedProtective = append(ownedProtective, ownerID+"-tp", ownerID+"-sl")
			}
		}
		s.mu.RUnlock()
		var order binance.Order
		var err error
		if owned, ok := s.testnet.(interface {
			ClosePositionOwned(context.Context, string, []string) (binance.Order, error)
		}); ok {
			order, err = owned.ClosePositionOwned(r.Context(), requestID, ownedProtective)
		} else {
			order, err = s.testnet.ClosePosition(r.Context(), requestID)
		}
		if err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		s.mu.Lock()
		s.lastTestnetRefresh = time.Time{}
		s.mu.Unlock()
		s.refreshTestnet(r.Context())
		writeJSON(w, http.StatusOK, map[string]any{"environment": environment, "position": "FLAT", "execution": "TESTNET LIVE ORDERS", "order_id": order.OrderID})
		return
	}
	s.mu.Lock()
	s.states[environment].Positions = nil
	s.states[environment].AutoRunning = false
	s.logLocked("paper position closed: " + string(environment))
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"environment": environment, "position": "FLAT", "execution": "PAPER_VALIDATION"})
}

func (s *Server) reloadCredentialsHTTP(w http.ResponseWriter, _ *http.Request) {
	s.reloadCredentials()
	s.mu.RLock()
	presence := s.credentials.Presence()
	s.mu.RUnlock()
	writeJSON(w, http.StatusOK, map[string]any{"credentials": presence, "active_session": "RESTART REQUIRED"})
}

func (s *Server) getSystem(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	market := s.market
	futuresAt, spotAt := s.marketSourceUpdated["futures"], s.marketSourceUpdated["spot"]
	accountStatus := "NOT_CONNECTED"
	if s.states[TradingEnvironmentTestnet].Balance.Connected {
		accountStatus = "CONNECTED"
	}
	warmup := s.warmup()
	runtimeStats := s.runtimeStats
	counters := map[string]any{
		"market_messages": s.marketMessages, "futures_messages": s.futuresMessages, "spot_messages": s.spotMessages,
		"futures_raw_messages": s.futuresRawMessages, "spot_raw_messages": s.spotRawMessages, "mark_raw_messages": s.markRawMessages, "public_parse_errors": s.publicParseErrors,
		"futures_reconnects": s.futuresReconnects, "spot_reconnects": s.spotReconnects,
		"futures_logical_gaps": runtimeStats.FuturesGaps, "spot_logical_gaps": runtimeStats.SpotGaps,
		"canonical_futures_bars": runtimeStats.FuturesBars, "canonical_spot_bars": runtimeStats.SpotBars, "canonical_duplicate_events": runtimeStats.DuplicateEvents, "canonical_reverse_events": runtimeStats.ReverseEvents, "canonical_id_gaps": runtimeStats.IDGaps,
		"external_source_updates": runtimeStats.ExternalUpdates, "future_observations": runtimeStats.FutureObservations,
		"source_stale_events": s.sourceStaleEvents,
		"feature_decisions":   s.featureDecisions, "eligible_decisions": s.eligibleDecisions, "feature_not_ready_decisions": s.featureNotReadyDecisions,
		"no_trade_signals": s.noTradeSignals, "long_signals": s.longSignals, "short_signals": s.shortSignals,
		"manual_order_requests": s.manualRequests, "auto_execution_intents": s.autoIntents,
		"actual_order_submits": s.actualOrderSubmits, "auto_execution_attempts": s.autoExecutionAttempts, "confirmed_entry_fills": s.confirmedEntryFills,
		"protective_order_submissions": s.protectiveOrderSubmits, "failed_unknown_submissions": s.failedUnknownSubmissions,
		"auto_queue_overflows": s.autoQueueOverflows, "auto_stale_decisions": s.autoStaleDecisions, "api_errors": s.apiErrors, "reconciliation_events": s.reconciliationEvents,
	}
	latency := map[string]any{"market_receive_ms": s.marketReceiveLatency.summary(), "feature_us": s.featureLatency.summary(), "model_inference_us": s.inferenceLatency.summary(), "policy_us": s.policyLatency.summary(), "pipeline_us": s.pipelineLatency.summary()}
	started := s.startedAt
	featureInput := s.autoFeatureSource && runtimeStats.Status.InputConnected
	s.mu.RUnlock()
	nowMs := time.Now().UnixMilli()
	sources := map[string]string{}
	for _, x := range []struct {
		name string
		max  int64
	}{{"metrics", asof.MetricsMaxFreshAgeMs}, {"mark", asof.KlineMaxFreshAgeMs}, {"index", asof.KlineMaxFreshAgeMs}, {"premium", asof.KlineMaxFreshAgeMs}, {"funding", asof.FundingMaxFreshAgeMs}} {
		stamp := runtimeStats.SourceLastMs[x.name]
		status := "UNAVAILABLE"
		if stamp > 0 {
			status = "FRESH"
			if nowMs-stamp > x.max {
				status = "STALE"
			}
		}
		sources[x.name] = status
	}
	metricSources := map[string]any{}
	staleMetricSource := ""
	staleMetricSince := int64(0)
	for _, name := range []string{"metrics_oi", "metrics_global", "metrics_top_account", "metrics_top_position", "metrics_taker"} {
		detail, ok := runtimeStats.MetricSources[name]
		if !ok {
			detail.Status = "UNAVAILABLE"
			detail.FreshnessLimitMs = asof.MetricsMaxFreshAgeMs
		}
		metricSources[name] = detail
		if detail.Status == "STALE" && staleMetricSource == "" {
			staleMetricSource = name
			staleMetricSince = detail.LastSourceTimestampMs + detail.FreshnessLimitMs
		}
	}
	if sources["metrics"] == "STALE" && staleMetricSource == "" {
		staleMetricSource = "metrics_join"
		staleMetricSince = runtimeStats.SourceLastMs["metrics"] + asof.MetricsMaxFreshAgeMs
	}
	autoBlocker := map[string]any{"code": "NONE", "source": "", "since_ms": int64(0)}
	if staleMetricSource != "" {
		autoBlocker = map[string]any{"code": "REQUIRED_SOURCE_STALE", "source": staleMetricSource, "since_ms": staleMetricSince}
	}
	input := "NOT_CONNECTED"
	if featureInput {
		input = "CONNECTED"
	}
	writeJSON(w, http.StatusOK, map[string]any{"connections": map[string]any{"futures_ws": !futuresAt.IsZero() && time.Since(futuresAt) <= 10*time.Second, "spot_ws": !spotAt.IsZero() && time.Since(spotAt) <= 10*time.Second, "rest": "READ_ONLY", "account_api": accountStatus}, "sources": sources, "metrics_sources": metricSources, "external_polls": runtimeStats.ExternalPolls, "feature_diagnostics": map[string]any{"last_eligibility_reason": runtimeStats.LastEligibilityReason, "lookback_missing": runtimeStats.LookbackMissing, "metrics_timestamp_spread_ms": runtimeStats.MetricsTimestampSpreadMs, "metrics_timestamp_mismatch": runtimeStats.MetricsTimestampMismatch}, "auto_blocker": autoBlocker, "warmup": warmup, "live_feature_input": input, "feature_engine_state": warmup["status"], "session_started_at": started, "uptime_ms": time.Since(started).Milliseconds(), "latency": latency, "counters": counters, "market_connected": market.Connected, "frozen": map[string]any{"feature_registry_hash": FeatureRegistryHash, "entry_policy_hash": EntryPolicyHash, "risk_policy_hash": RiskPolicyHash, "model_time_alignment_status": FrozenModelTimeAlignmentStatus, "models": 5}})
}

func (s *Server) refreshTestnet(ctx context.Context) {
	s.refreshTestnetWithForce(ctx, false)
}

func (s *Server) refreshTestnetForce(ctx context.Context) {
	s.refreshTestnetWithForce(ctx, true)
}

func (s *Server) refreshTestnetWithForce(ctx context.Context, force bool) {
	s.testnetRefreshMu.Lock()
	defer s.testnetRefreshMu.Unlock()
	s.mu.RLock()
	recent := !force && !s.lastTestnetRefresh.IsZero() && time.Since(s.lastTestnetRefresh) < time.Second
	s.mu.RUnlock()
	if recent {
		return
	}
	balance, positions, orders, err := s.testnet.Refresh(ctx)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		s.apiErrors++
		s.logLocked("Demo account read failed; exchange state unknown")
		s.states[TradingEnvironmentTestnet].Balance = Balance{Status: "ACCOUNT UNAVAILABLE"}
		s.states[TradingEnvironmentTestnet].Positions = nil
		s.states[TradingEnvironmentTestnet].Orders = nil
		s.states[TradingEnvironmentTestnet].ExchangeKnown = false
		return
	}
	wasConnected := s.states[TradingEnvironmentTestnet].Balance.Connected
	s.states[TradingEnvironmentTestnet].Balance, s.states[TradingEnvironmentTestnet].Positions, s.states[TradingEnvironmentTestnet].Orders = balance, positions, orders
	s.states[TradingEnvironmentTestnet].ExchangeKnown = true
	s.reconciliationEvents++
	if !wasConnected && balance.Connected {
		s.logLocked("Demo account connected")
	}
	s.lastTestnetRefresh = time.Now()
}

func (s *Server) getLogs(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	rows := append([]string(nil), s.logs...)
	s.mu.RUnlock()
	writeJSON(w, http.StatusOK, map[string]any{"logs": rows})
}
func (s *Server) logLocked(value string) {
	s.logs = append([]string{time.Now().UTC().Format(time.RFC3339) + " " + value}, s.logs...)
	if len(s.logs) > 100 {
		s.logs = s.logs[:100]
	}
}

func (s *Server) SetMarket(value Market) {
	s.mu.Lock()
	s.market = value
	s.marketMessages++
	s.mu.Unlock()
	s.broadcast(map[string]any{"type": "market", "market": value})
}

func (s *Server) setMarketSource(kind string, value Market, sourceMs int64) {
	s.mu.Lock()
	s.marketSourceUpdated[kind] = time.Now().UTC()
	switch kind {
	case "futures":
		s.futuresMessages++
	case "spot":
		s.spotMessages++
	}
	if sourceMs > 0 {
		latency := float64(time.Now().UnixMilli() - sourceMs)
		s.marketReceiveLatency.add(latency)
	}
	s.mu.Unlock()
	s.SetMarket(value)
}

func (s *Server) webSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	client := &wsClient{server: s, conn: conn, send: make(chan []byte, wsSendQueueSize), done: make(chan struct{})}
	s.mu.Lock()
	s.clients[client] = struct{}{}
	s.mu.Unlock()
	go client.writePump()
	defer client.close()
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
	}
}

func (c *wsClient) writePump() {
	defer c.close()
	for {
		select {
		case payload := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(wsWriteTimeout))
			if err := c.conn.WriteMessage(websocket.TextMessage, payload); err != nil {
				return
			}
		case <-c.done:
			return
		}
	}
}

func (c *wsClient) close() {
	c.closeOnce.Do(func() {
		c.server.mu.Lock()
		delete(c.server.clients, c)
		c.server.mu.Unlock()
		close(c.done)
		_ = c.conn.Close()
	})
}

func (s *Server) broadcast(value any) {
	b, err := json.Marshal(value)
	if err != nil {
		return
	}
	s.mu.RLock()
	clients := make([]*wsClient, 0, len(s.clients))
	for c := range s.clients {
		clients = append(clients, c)
	}
	s.mu.RUnlock()
	for _, c := range clients {
		select {
		case <-c.done:
		case c.send <- b:
		default:
			c.close()
		}
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
