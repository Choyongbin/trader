package uiapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"binance_trader/internal/credentials"
	"binance_trader/internal/live/autopipeline"
	"binance_trader/internal/live/binance"
	"binance_trader/internal/live/runtimefeature"
)

type fakeProvider struct{ store credentials.CredentialStore }

func (p fakeProvider) Load() (credentials.CredentialStore, error) { return p.store, nil }
func (p fakeProvider) Path() string                               { return "fixture" }

func testServer(t *testing.T) *Server {
	t.Helper()
	dir := t.TempDir()
	warmup := filepath.Join(dir, "capture-state.json")
	if err := os.WriteFile(warmup, []byte(`{"RequiredWarmupMs":14400000,"AvailableContiguousHistoryMs":14400000,"MissingDurationMs":0,"WarmupReady":true,"Status":"READY"}`), 0600); err != nil {
		t.Fatal(err)
	}
	static := fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("console")}}
	provider := fakeProvider{credentials.CredentialStore{
		Testnet: credentials.EnvironmentCredentials{APIKey: "test-key-value", APISecret: "test-secret-value"},
		Mainnet: credentials.EnvironmentCredentials{APIKey: "main-key-value", APISecret: "main-secret-value"},
	}}
	server, err := NewServer(provider, static, Options{WarmupPath: warmup, PaperOrders: map[TradingEnvironment]bool{TradingEnvironmentTestnet: true}, FixtureConnected: map[TradingEnvironment]bool{TradingEnvironmentTestnet: true, TradingEnvironmentMainnet: true}})
	if err != nil {
		t.Fatal(err)
	}
	server.SetMarket(Market{FuturesPrice: 100000, SpotPrice: 99990, MarkPrice: 99995, Connected: true})
	return server
}

func TestSystemExposesStaleMetricSubsourceBlocker(t *testing.T) {
	s := testServer(t)
	s.runtimeStats.MetricSources = map[string]runtimefeature.MetricSourceHealth{
		"metrics_taker": {LastSourceTimestampMs: 1_000, CurrentAgeMs: 600_001, FreshnessLimitMs: 600_000, Status: "STALE", LastPollResult: "PASS", HTTPStatus: 200},
	}
	recorder := perform(s.Handler(), http.MethodGet, "/api/system", "", nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("system status=%d", recorder.Code)
	}
	var body struct {
		MetricsSources map[string]runtimefeature.MetricSourceHealth `json:"metrics_sources"`
		AutoBlocker    map[string]any                               `json:"auto_blocker"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.MetricsSources["metrics_taker"].Status != "STALE" || body.AutoBlocker["source"] != "metrics_taker" || body.AutoBlocker["code"] != "REQUIRED_SOURCE_STALE" {
		t.Fatalf("metric detail missing: %+v", body)
	}
}

func perform(handler http.Handler, method, target, token string, body any) *httptest.ResponseRecorder {
	var input bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&input).Encode(body)
	}
	request := httptest.NewRequest(method, target, &input)
	if token != "" {
		request.Header.Set("X-CSRF-Token", token)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

func TestSecretSafetyCSRFAndEnvironmentRequirement(t *testing.T) {
	s := testServer(t)
	h := s.Handler()
	status := perform(h, http.MethodGet, "/api/status", "", nil)
	body := status.Body.String()
	for _, secret := range []string{"test-key-value", "test-secret-value", "main-key-value", "main-secret-value"} {
		if strings.Contains(body, secret) {
			t.Fatalf("secret exposed: %s", secret)
		}
	}
	if perform(h, http.MethodGet, "/api/account", "", nil).Code != http.StatusBadRequest {
		t.Fatal("missing environment accepted")
	}
	if perform(h, http.MethodPost, "/api/auto-trading/stop?environment=TESTNET", "", nil).Code != http.StatusForbidden {
		t.Fatal("missing CSRF accepted")
	}
	request := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	request.Header.Set("Origin", "https://example.com")
	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusForbidden {
		t.Fatal("cross origin accepted")
	}
}

func TestManualFlowSeparationDoubleSubmitAndAutoGate(t *testing.T) {
	oldTest, oldMain := os.Getenv("BINANCE_TESTNET_ENABLE_ORDERS"), os.Getenv("BINANCE_MAINNET_ENABLE_ORDERS")
	t.Cleanup(func() {
		_ = os.Setenv("BINANCE_TESTNET_ENABLE_ORDERS", oldTest)
		_ = os.Setenv("BINANCE_MAINNET_ENABLE_ORDERS", oldMain)
	})
	_ = os.Setenv("BINANCE_TESTNET_ENABLE_ORDERS", "true")
	_ = os.Setenv("BINANCE_MAINNET_ENABLE_ORDERS", "false")
	s := testServer(t)
	h := s.Handler()
	token := s.csrf
	order := ManualOrderRequest{Environment: TradingEnvironmentTestnet, Symbol: "BTCUSDT", Side: "LONG", OrderType: "MARKET", MarginAmountUSDT: 100, Leverage: 5, RequestID: "one", TakeProfit: &ProtectiveOrderConfig{Enabled: true, Mode: "PERCENT", Value: .75}, StopLoss: &ProtectiveOrderConfig{Enabled: true, Mode: "PERCENT", Value: .5}}
	if got := perform(h, http.MethodPost, "/api/manual-order", token, order); got.Code != http.StatusOK {
		t.Fatal(got.Body.String())
	}
	if got := perform(h, http.MethodPost, "/api/manual-order", token, order); got.Code != http.StatusOK {
		t.Fatal("duplicate was not adopted")
	}
	testOrders := perform(h, http.MethodGet, "/api/orders?environment=TESTNET", "", nil).Body.String()
	mainOrders := perform(h, http.MethodGet, "/api/orders?environment=MAINNET", "", nil).Body.String()
	if strings.Count(testOrders, "FILLED_PAPER") != 1 || strings.Contains(mainOrders, "FILLED_PAPER") {
		t.Fatal("environment data mixed or duplicate order")
	}
	if perform(h, http.MethodPost, "/api/auto-trading/start?environment=TESTNET", token, map[string]string{"model_profile_id": "btc-feature-v2-production-v1"}).Code != http.StatusConflict {
		t.Fatal("auto started with manual position")
	}
	if perform(h, http.MethodPost, "/api/position/close?environment=TESTNET", token, nil).Code != http.StatusOK {
		t.Fatal("close failed")
	}
	if perform(h, http.MethodPost, "/api/auto-trading/start?environment=TESTNET", token, map[string]string{"model_profile_id": "btc-feature-v2-production-v1"}).Code != http.StatusConflict {
		t.Fatal("unarmed auto execution started")
	}
	if perform(h, http.MethodPost, "/api/manual-order", token, ManualOrderRequest{Environment: TradingEnvironmentMainnet, Symbol: "BTCUSDT", Side: "SHORT", OrderType: "MARKET", MarginAmountUSDT: 100, Leverage: 3, RequestID: "main", LiveConfirmation: "CONFIRM LIVE ORDER"}).Code != http.StatusConflict {
		t.Fatal("Mainnet orders-disabled guard failed")
	}
}

func TestEntryRejectCreatesNoProtectiveOrders(t *testing.T) {
	s := testServer(t)
	h := s.Handler()
	bad := ManualOrderRequest{Environment: TradingEnvironmentTestnet, Symbol: "BTCUSDT", Side: "LONG", OrderType: "MARKET", MarginAmountUSDT: -1, Leverage: 5, RequestID: "bad", TakeProfit: &ProtectiveOrderConfig{Enabled: true, Mode: "PERCENT", Value: 1}}
	if perform(h, http.MethodPost, "/api/manual-order", s.csrf, bad).Code != http.StatusBadRequest {
		t.Fatal("bad entry accepted")
	}
	s.mu.RLock()
	count := len(s.states[TradingEnvironmentTestnet].Orders)
	s.mu.RUnlock()
	if count != 0 {
		t.Fatalf("protective order created after reject: %d", count)
	}
}

func TestPartialFillProtectiveOrdersUseExecutedQuantity(t *testing.T) {
	request := ManualOrderRequest{Environment: TradingEnvironmentTestnet, Symbol: "BTCUSDT", Side: "SHORT", OrderType: "MARKET", MarginAmountUSDT: 100, Leverage: 3, RequestID: "partial", TakeProfit: &ProtectiveOrderConfig{Enabled: true, Mode: "PERCENT", Value: 1}, StopLoss: &ProtectiveOrderConfig{Enabled: true, Mode: "PERCENT", Value: 1}}
	_, position, protective := buildPaperExecution(request, 100000, .003)
	if position.QuantityBTC != .003 || len(protective) != 2 {
		t.Fatalf("position=%+v protective=%+v", position, protective)
	}
	for _, order := range protective {
		if order.QuantityBTC != .003 {
			t.Fatalf("protective quantity=%g", order.QuantityBTC)
		}
	}
}

type fakeTestnetBackend struct {
	refreshCalls, submitCalls, autoSubmitCalls, cleanupCalls int
}

func (f *fakeTestnetBackend) Refresh(context.Context) (Balance, []Position, []Order, error) {
	f.refreshCalls++
	wallet, available, margin, used, pnl := 123.0, 120.0, 124.0, 4.0, 1.0
	return Balance{true, "CONNECTED", &wallet, &available, &margin, &used, &pnl}, nil, nil, nil
}
func (f *fakeTestnetBackend) SubmitManual(context.Context, ManualOrderRequest) (ManualExecution, error) {
	f.submitCalls++
	return ManualExecution{}, nil
}
func (f *fakeTestnetBackend) ClosePosition(context.Context, string) (binance.Order, error) {
	return binance.Order{}, nil
}
func (f *fakeTestnetBackend) SubmitAuto(_ context.Context, intent autopipeline.ExecutionIntent) (ManualExecution, error) {
	f.autoSubmitCalls++
	now := time.Now().UTC()
	entry := Order{Time: now, Environment: TradingEnvironmentTestnet, Symbol: intent.Symbol, Side: intent.Side, OrderType: "MARKET", QuantityBTC: intent.QuantityBTC, Price: intent.EntryReferencePrice, Status: "FILLED", ClientOrderID: "auto-entry"}
	tp := Order{Time: now, Environment: TradingEnvironmentTestnet, Symbol: intent.Symbol, Side: opposite(intent.Side), OrderType: "TAKE_PROFIT_MARKET", QuantityBTC: intent.QuantityBTC, Price: intent.TPPrice, Status: "NEW", ClientOrderID: "auto-tp", Protective: "TP"}
	sl := Order{Time: now, Environment: TradingEnvironmentTestnet, Symbol: intent.Symbol, Side: opposite(intent.Side), OrderType: "STOP_MARKET", QuantityBTC: intent.QuantityBTC, Price: intent.SLPrice, Status: "NEW", ClientOrderID: "auto-sl", Protective: "SL"}
	position := Position{Environment: TradingEnvironmentTestnet, Symbol: intent.Symbol, Side: intent.Side, QuantityBTC: intent.QuantityBTC, NotionalUSDT: intent.NotionalUSDT, EntryPrice: intent.EntryReferencePrice, MarkPrice: intent.EntryReferencePrice, Leverage: intent.Leverage, MarginUSDT: intent.MarginUSDT, TakeProfit: &tp.Price, StopLoss: &sl.Price, OpenedAt: now}
	return ManualExecution{Order: entry, Position: position, Protective: []Order{tp, sl}}, nil
}
func (f *fakeTestnetBackend) CleanupAutoProtective(context.Context, []string) error {
	f.cleanupCalls++
	return nil
}

func TestActualTestnetReadBindingAndMainnetIsolation(t *testing.T) {
	static := fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("console")}}
	provider := fakeProvider{credentials.CredentialStore{Testnet: credentials.EnvironmentCredentials{APIKey: "key", APISecret: "secret"}}}
	backend := &fakeTestnetBackend{}
	s, err := NewServer(provider, static, Options{TestnetBackend: backend})
	if err != nil {
		t.Fatal(err)
	}
	if body := perform(s.Handler(), http.MethodGet, "/api/account?environment=TESTNET", "", nil).Body.String(); !strings.Contains(body, `"connected":true`) {
		t.Fatal(body)
	}
	if body := perform(s.Handler(), http.MethodGet, "/api/account?environment=MAINNET", "", nil).Body.String(); strings.Contains(body, `"connected":true`) {
		t.Fatal("Testnet state leaked into Mainnet")
	}
	if backend.refreshCalls != 1 || backend.submitCalls != 0 {
		t.Fatalf("refresh=%d submit=%d", backend.refreshCalls, backend.submitCalls)
	}
}

func TestProtectiveUIOrderUsesTriggerPrice(t *testing.T) {
	order := uiOrder(binance.Order{StopPrice: "123.4", OriginalQuantity: "0.001"}, "TP")
	if order.Price != 123.4 || order.QuantityBTC != 0.001 {
		t.Fatalf("protective order=%+v", order)
	}
}

var _ fs.FS
