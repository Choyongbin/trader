package uiapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"
	"time"

	"binance_trader/internal/credentials"
	featurev2 "binance_trader/internal/feature/main/v2"
	"binance_trader/internal/live/autopipeline"
	"binance_trader/internal/live/binance"
)

type failingReadBackend struct{ submits int }

func (*failingReadBackend) Refresh(context.Context) (Balance, []Position, []Order, error) {
	return Balance{}, nil, nil, errors.New("fixture read timeout")
}
func (b *failingReadBackend) SubmitManual(context.Context, ManualOrderRequest) (ManualExecution, error) {
	b.submits++
	return ManualExecution{}, nil
}
func (*failingReadBackend) ClosePosition(context.Context, string) (binance.Order, error) {
	return binance.Order{}, nil
}

func TestExchangeUnknownRejectsNewEntries(t *testing.T) {
	t.Setenv("BINANCE_ENV", "TESTNET")
	t.Setenv("BINANCE_TESTNET_ENABLE_ORDERS", "true")
	backend := &failingReadBackend{}
	static := fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("console")}}
	s, err := NewServer(fakeProvider{credentials.CredentialStore{Testnet: credentials.EnvironmentCredentials{APIKey: "key", APISecret: "secret"}}}, static, Options{TestnetBackend: backend})
	if err != nil {
		t.Fatal(err)
	}
	response := perform(s.Handler(), http.MethodGet, "/api/entry-blockers?environment=TESTNET", "", nil)
	var body struct {
		Blockers []EntryBlocker `json:"blockers"`
	}
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &body) != nil {
		t.Fatal("blocker API failed")
	}
	found := false
	for _, b := range body.Blockers {
		if b.Code == "EXCHANGE_STATE_UNKNOWN" {
			found = true
		}
	}
	if !found {
		t.Fatal("unknown exchange state hidden")
	}
	order := ManualOrderRequest{Environment: TradingEnvironmentTestnet, Symbol: "BTCUSDT", Side: "LONG", OrderType: "MARKET", MarginAmountUSDT: 100, Leverage: 1, RequestID: "unknown-state"}
	if got := perform(s.Handler(), http.MethodPost, "/api/manual-order", s.csrf, order); got.Code != http.StatusConflict {
		t.Fatalf("manual accepted: %s", got.Body.String())
	}
	if backend.submits != 0 {
		t.Fatal("unknown state submitted order")
	}
	if got := perform(s.Handler(), http.MethodPost, "/api/auto-trading/start?environment=TESTNET", s.csrf, map[string]string{"model_profile_id": autopipeline.ProfileID}); got.Code != http.StatusConflict {
		t.Fatal("auto start accepted unknown state")
	}
}

func TestAutoStateMachineReadyFixtureAndStop(t *testing.T) {
	t.Setenv("BINANCE_ENV", "TESTNET")
	t.Setenv("BINANCE_TESTNET_ENABLE_ORDERS", "true")
	t.Setenv("BINANCE_TESTNET_ENABLE_AUTO_ORDERS", "true")
	path := filepath.Join(t.TempDir(), "capture-state.json")
	checkpoint := map[string]any{"RequiredWarmupMs": 14_400_000, "AvailableContiguousHistoryMs": 14_400_000, "WarmupReady": true, "LastUpdatedMs": time.Now().UnixMilli()}
	body, _ := json.Marshal(checkpoint)
	if err := os.WriteFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
	pipeline, err := autopipeline.LoadFrozen(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	backend := &fakeTestnetBackend{}
	static := fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("console")}}
	s, err := NewServer(fakeProvider{credentials.CredentialStore{Testnet: credentials.EnvironmentCredentials{APIKey: "key", APISecret: "secret"}}}, static, Options{WarmupPath: path, TestnetBackend: backend, AutoPipeline: pipeline, AutoFeatureSource: true})
	if err != nil {
		t.Fatal(err)
	}
	s.allowModelInTests = true
	now := time.Now().UTC()
	s.SetMarket(Market{FuturesPrice: 100000, SpotPrice: 99999, MarkPrice: 100001, UpdatedAt: now, Connected: true})
	s.mu.Lock()
	for _, kind := range []string{"futures", "spot", "mark"} {
		s.marketSourceUpdated[kind] = now
	}
	s.mu.Unlock()
	response := perform(s.Handler(), http.MethodPost, "/api/auto-trading/start?environment=TESTNET", s.csrf, map[string]string{"model_profile_id": autopipeline.ProfileID})
	if response.Code != http.StatusOK || !s.states[TradingEnvironmentTestnet].AutoRunning || s.states[TradingEnvironmentTestnet].AutoState != "RUNNING" {
		t.Fatalf("start=%d %s", response.Code, response.Body.String())
	}
	feature := featurev2.Snapshot{DecisionTimestampMs: now.UnixMilli()}
	response = perform(s.Handler(), http.MethodPost, "/api/auto-trading/stop?environment=TESTNET", s.csrf, nil)
	if response.Code != http.StatusOK || s.states[TradingEnvironmentTestnet].AutoRunning || s.states[TradingEnvironmentTestnet].AutoState != "STOPPED" {
		t.Fatal("STOP did not disable new intents")
	}
	if _, err = s.ProcessFeatureSnapshot(feature); err == nil {
		t.Fatal("feature accepted after STOP")
	}
	if backend.submitCalls != 0 || backend.autoSubmitCalls != 0 {
		t.Fatal("actual Demo submit called")
	}
	restarted, err := NewServer(fakeProvider{credentials.CredentialStore{}}, static, Options{WarmupPath: path, AutoPipeline: pipeline, AutoFeatureSource: true})
	if err != nil || restarted.states[TradingEnvironmentTestnet].AutoState != "STOPPED" {
		t.Fatal("restart auto-resumed")
	}
}

func TestFrozenModelTimeAlignmentBlocksOtherwiseReadyAutoStart(t *testing.T) {
	t.Setenv("BINANCE_ENV", "TESTNET")
	t.Setenv("BINANCE_TESTNET_ENABLE_ORDERS", "true")
	t.Setenv("BINANCE_TESTNET_ENABLE_AUTO_ORDERS", "true")
	path := filepath.Join(t.TempDir(), "capture-state.json")
	checkpoint := map[string]any{"RequiredWarmupMs": 14_400_000, "AvailableContiguousHistoryMs": 14_400_000, "WarmupReady": true, "LastUpdatedMs": time.Now().UnixMilli()}
	body, _ := json.Marshal(checkpoint)
	if err := os.WriteFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
	pipeline, err := autopipeline.LoadFrozen(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	backend := &fakeTestnetBackend{}
	static := fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("console")}}
	s, err := NewServer(fakeProvider{credentials.CredentialStore{Testnet: credentials.EnvironmentCredentials{APIKey: "key", APISecret: "secret"}}}, static, Options{WarmupPath: path, TestnetBackend: backend, AutoPipeline: pipeline, AutoFeatureSource: true})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	s.SetMarket(Market{FuturesPrice: 100000, SpotPrice: 99999, MarkPrice: 100001, UpdatedAt: now, Connected: true})
	s.mu.Lock()
	for _, kind := range []string{"futures", "spot", "mark"} {
		s.marketSourceUpdated[kind] = now
	}
	s.mu.Unlock()
	response := perform(s.Handler(), http.MethodPost, "/api/auto-trading/start?environment=TESTNET", s.csrf, map[string]string{"model_profile_id": autopipeline.ProfileID})
	if response.Code != http.StatusConflict || s.states[TradingEnvironmentTestnet].AutoRunning || backend.autoSubmitCalls != 0 {
		t.Fatalf("status=%d running=%t submits=%d body=%s", response.Code, s.states[TradingEnvironmentTestnet].AutoRunning, backend.autoSubmitCalls, response.Body.String())
	}
	var result struct {
		BlockedReason string `json:"blocked_reason"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil || result.BlockedReason != "FROZEN_MODEL_TIME_ALIGNMENT_UNVERIFIED" {
		t.Fatalf("result=%+v err=%v body=%s", result, err, response.Body.String())
	}
}

func TestStopDoesNotHideExistingOrUnknownAutomaticRisk(t *testing.T) {
	for _, fixture := range []struct {
		name, want string
		owned      bool
		unknown    bool
	}{
		{name: "protected", owned: true, want: "POSITION_PROTECTED_STOPPED"},
		{name: "unknown", owned: true, unknown: true, want: "UNKNOWN_EXECUTION_STATE"},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			s := testServer(t)
			s.mu.Lock()
			s.states[TradingEnvironmentTestnet].AutoRunning = true
			s.autoOwnedPosition = fixture.owned
			s.autoRecoveryBlocked = fixture.unknown
			s.mu.Unlock()
			response := perform(s.Handler(), http.MethodPost, "/api/auto-trading/stop?environment=TESTNET", s.csrf, nil)
			if response.Code != http.StatusOK || s.states[TradingEnvironmentTestnet].AutoState != fixture.want {
				t.Fatalf("status=%d state=%s body=%s", response.Code, s.states[TradingEnvironmentTestnet].AutoState, response.Body.String())
			}
		})
	}
}

func TestExecuteAutoIntentUsesDedicatedDemoAutoBackend(t *testing.T) {
	t.Setenv("BINANCE_ENV", "TESTNET")
	t.Setenv("BINANCE_TESTNET_ENABLE_ORDERS", "true")
	t.Setenv("BINANCE_TESTNET_ENABLE_AUTO_ORDERS", "true")
	backend := &fakeTestnetBackend{}
	static := fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("console")}}
	s, err := NewServer(fakeProvider{credentials.CredentialStore{Testnet: credentials.EnvironmentCredentials{APIKey: "key", APISecret: "secret"}}}, static, Options{TestnetBackend: backend})
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.states[TradingEnvironmentTestnet].AutoRunning = true
	s.states[TradingEnvironmentTestnet].AutoState = "RUNNING"
	s.mu.Unlock()
	intent := autopipeline.ExecutionIntent{Environment: "TESTNET", Symbol: "BTCUSDT", Side: "LONG", CandidateID: "fixture", QuantityBTC: .001, NotionalUSDT: 100, RiskBudgetUSDT: 1, Leverage: 2, MarginUSDT: 50, EntryReferencePrice: 100000, TPPrice: 101000, SLPrice: 99000, HorizonSeconds: 900, ClientOrderID: "dry-fixture", ModelProfileID: autopipeline.ProfileID, DecisionTimestampMs: time.Now().UnixMilli()}
	execution, err := s.executeAutoIntent(context.Background(), intent)
	if err != nil {
		t.Fatal(err)
	}
	if backend.autoSubmitCalls != 1 || execution.Order.ClientOrderID != "auto-entry" {
		t.Fatalf("submit=%d execution=%+v", backend.autoSubmitCalls, execution)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.autoOwnedPosition || len(s.autoProtectiveIDs) != 2 || s.states[TradingEnvironmentTestnet].AutoState != "POSITION_OPEN" || s.actualOrderSubmits != 1 {
		t.Fatalf("auto state owned=%t protective=%v state=%s submits=%d", s.autoOwnedPosition, s.autoProtectiveIDs, s.states[TradingEnvironmentTestnet].AutoState, s.actualOrderSubmits)
	}
}

func TestAutoOrderGateIsSeparateFromManualOrderGate(t *testing.T) {
	t.Setenv("BINANCE_ENV", "TESTNET")
	t.Setenv("BINANCE_TESTNET_ENABLE_ORDERS", "true")
	t.Setenv("BINANCE_TESTNET_ENABLE_AUTO_ORDERS", "false")
	backend := &fakeTestnetBackend{}
	static := fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("console")}}
	s, err := NewServer(fakeProvider{credentials.CredentialStore{Testnet: credentials.EnvironmentCredentials{APIKey: "key", APISecret: "secret"}}}, static, Options{TestnetBackend: backend})
	if err != nil {
		t.Fatal(err)
	}
	if s.autoOrdersEnabled(TradingEnvironmentTestnet) {
		t.Fatal("automatic orders armed by manual order flag alone")
	}
	s.mu.Lock()
	s.states[TradingEnvironmentTestnet].AutoRunning = true
	s.mu.Unlock()
	intent := autopipeline.ExecutionIntent{Environment: "TESTNET", Symbol: "BTCUSDT", Side: "LONG", CandidateID: "fixture", QuantityBTC: .001, NotionalUSDT: 100, RiskBudgetUSDT: 1, Leverage: 2, MarginUSDT: 50, EntryReferencePrice: 100000, TPPrice: 101000, SLPrice: 99000, HorizonSeconds: 900, ModelProfileID: autopipeline.ProfileID, DecisionTimestampMs: time.Now().UnixMilli()}
	if _, err = s.executeAutoIntent(context.Background(), intent); err == nil {
		t.Fatal("automatic order submitted without dedicated auto flag")
	}
	if backend.autoSubmitCalls != 0 {
		t.Fatal("backend received automatic order while disarmed")
	}
}

type horizonBackend struct {
	open          bool
	closeCalls    int
	closeIDs      []string
	closeSide     string
	closeQty      float64
	unrelated     bool
	side          string
	failClose     bool
	badProtective bool
}

func (b *horizonBackend) Refresh(context.Context) (Balance, []Position, []Order, error) {
	wallet, available, margin, used, pnl := 1000.0, 900.0, 1000.0, 100.0, 0.0
	balance := Balance{true, "CONNECTED", &wallet, &available, &margin, &used, &pnl}
	if !b.open {
		if b.unrelated {
			return balance, nil, []Order{{ClientOrderID: "manual-algo"}}, nil
		}
		return balance, nil, nil, nil
	}
	positionSide := b.side
	if positionSide == "" {
		positionSide = "LONG"
	}
	position := Position{Environment: TradingEnvironmentTestnet, Symbol: "BTCUSDT", Side: positionSide, QuantityBTC: .001, EntryPrice: 100000, MarkPrice: 100000, Leverage: 1, MarginUSDT: 100}
	protectiveSide := "SHORT"
	if positionSide == "SHORT" {
		protectiveSide = "LONG"
	}
	orders := []Order{
		{Environment: TradingEnvironmentTestnet, Symbol: "BTCUSDT", Side: protectiveSide, OrderType: "TAKE_PROFIT_MARKET", QuantityBTC: .001, Price: 101000, Status: "NEW", ClientOrderID: "tp", Protective: "TP", ReduceOnly: true},
		{Environment: TradingEnvironmentTestnet, Symbol: "BTCUSDT", Side: protectiveSide, OrderType: "STOP_MARKET", QuantityBTC: .001, Price: 99000, Status: "NEW", ClientOrderID: "sl", Protective: "SL", ReduceOnly: true},
	}
	if b.badProtective {
		orders[0].ReduceOnly = false
	}
	if b.unrelated {
		orders = append(orders, Order{ClientOrderID: "manual-algo"})
	}
	return balance, []Position{position}, orders, nil
}
func (*horizonBackend) SubmitManual(context.Context, ManualOrderRequest) (ManualExecution, error) {
	return ManualExecution{}, errors.New("unused")
}
func (b *horizonBackend) ClosePosition(context.Context, string) (binance.Order, error) {
	b.closeCalls++
	if b.failClose {
		return binance.Order{}, errors.New("injected lost exit response")
	}
	b.open = false
	return binance.Order{Status: "FILLED"}, nil
}

func TestUnknownHorizonExitIsNotResubmitted(t *testing.T) {
	backend := &horizonBackend{open: true, failClose: true}
	static := fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("console")}}
	s, err := NewServer(fakeProvider{credentials.CredentialStore{}}, static, Options{TestnetBackend: backend})
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.autoOwnedPosition = true
	s.autoProtectiveIDs = []string{"tp", "sl"}
	s.autoExitDeadline = time.Now().Add(-time.Second)
	s.autoCloseRequestID = "lost-exit"
	s.autoExecutionState = &autoExecutionState{ExecutionState: "POSITION_PROTECTED", Symbol: "BTCUSDT", EntryClientOrderID: "entry", EntrySide: "LONG", EntryRequestedQuantity: .001, EntryNormalizedQuantity: "0.001", EntryFilledQuantity: .001, EntryFillTimestampMs: time.Now().UnixMilli(), TPClientAlgoID: "tp", SLClientAlgoID: "sl", TPTriggerPrice: 101000, SLTriggerPrice: 99000, ProtectiveQuantity: .001, HorizonSeconds: 900, HorizonDeadlineMs: time.Now().Add(-time.Second).UnixMilli(), HorizonCloseRequestID: "lost-exit"}
	s.mu.Unlock()
	if _, err = s.reconcileAutoLifecycle(context.Background()); err == nil || !s.autoRecoveryBlocked {
		t.Fatalf("first reconcile err=%v blocked=%t", err, s.autoRecoveryBlocked)
	}
	if _, err = s.reconcileAutoLifecycle(context.Background()); err == nil {
		t.Fatal("EXIT_PENDING retry did not remain unknown")
	}
	if backend.closeCalls != 1 {
		t.Fatalf("duplicate exit calls=%d", backend.closeCalls)
	}
}
func (b *horizonBackend) CloseAutoPosition(ctx context.Context, requestID string, ids []string, side string, quantity float64) (binance.Order, error) {
	b.closeIDs = append([]string(nil), ids...)
	b.closeSide = side
	b.closeQty = quantity
	return b.ClosePosition(ctx, requestID)
}

func TestShortHorizonCloseUsesOwnedSideAndQuantity(t *testing.T) {
	backend := &horizonBackend{open: true, side: "SHORT"}
	static := fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("console")}}
	s, err := NewServer(fakeProvider{credentials.CredentialStore{}}, static, Options{TestnetBackend: backend})
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.autoOwnedPosition = true
	s.autoProtectiveIDs = []string{"tp", "sl"}
	s.autoExitDeadline = time.Now().Add(-time.Second)
	s.autoCloseRequestID = "short-close"
	s.mu.Unlock()
	if _, err = s.reconcileAutoLifecycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if backend.closeSide != "SHORT" || backend.closeQty != .001 || backend.closeCalls != 1 {
		t.Fatalf("side=%s qty=%g calls=%d", backend.closeSide, backend.closeQty, backend.closeCalls)
	}
}
func (*horizonBackend) SubmitAuto(context.Context, autopipeline.ExecutionIntent) (ManualExecution, error) {
	return ManualExecution{}, errors.New("unused")
}
func (*horizonBackend) CleanupAutoProtective(context.Context, []string) error { return nil }

func TestAutoOwnedPositionClosesAtFrozenHoldingHorizon(t *testing.T) {
	backend := &horizonBackend{open: true, unrelated: true}
	static := fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("console")}}
	s, err := NewServer(fakeProvider{credentials.CredentialStore{Testnet: credentials.EnvironmentCredentials{APIKey: "key", APISecret: "secret"}}}, static, Options{TestnetBackend: backend})
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.states[TradingEnvironmentTestnet].AutoRunning = true
	s.states[TradingEnvironmentTestnet].AutoState = "POSITION_OPEN"
	s.autoOwnedPosition = true
	s.autoProtectiveIDs = []string{"tp", "sl"}
	s.autoExitDeadline = time.Now().Add(-time.Second)
	s.autoCloseRequestID = "horizon-close"
	s.mu.Unlock()
	holding, err := s.reconcileAutoOwnedPosition(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if holding || backend.closeCalls != 1 {
		t.Fatalf("holding=%t closeCalls=%d", holding, backend.closeCalls)
	}
	if len(backend.closeIDs) != 2 || backend.closeIDs[0] != "tp" || backend.closeIDs[1] != "sl" {
		t.Fatalf("owned cleanup IDs=%v", backend.closeIDs)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.autoOwnedPosition || !s.autoExitDeadline.IsZero() || s.autoCloseRequestID != "" || s.states[TradingEnvironmentTestnet].AutoState != "RUNNING" {
		t.Fatalf("horizon cleanup state: owned=%t deadline=%v closeID=%q state=%s", s.autoOwnedPosition, s.autoExitDeadline, s.autoCloseRequestID, s.states[TradingEnvironmentTestnet].AutoState)
	}
}

func TestConcurrentHorizonReconciliationClosesOnce(t *testing.T) {
	backend := &horizonBackend{open: true}
	static := fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("console")}}
	s, err := NewServer(fakeProvider{credentials.CredentialStore{}}, static, Options{TestnetBackend: backend})
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.autoOwnedPosition = true
	s.autoProtectiveIDs = []string{"tp", "sl"}
	s.autoExitDeadline = time.Now().Add(-time.Second)
	s.autoCloseRequestID = "one-close"
	s.mu.Unlock()
	done := make(chan error, 2)
	for range 2 {
		go func() {
			_, reconcileErr := s.reconcileAutoLifecycle(context.Background())
			done <- reconcileErr
		}()
	}
	for range 2 {
		if reconcileErr := <-done; reconcileErr != nil {
			t.Fatal(reconcileErr)
		}
	}
	if backend.closeCalls != 1 {
		t.Fatalf("close calls=%d want=1", backend.closeCalls)
	}
}
