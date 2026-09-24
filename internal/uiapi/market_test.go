package uiapi

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"testing"
	"testing/fstest"
	"time"

	"binance_trader/internal/credentials"
	"binance_trader/internal/live/autopipeline"
	"binance_trader/internal/live/binance"
	"binance_trader/internal/live/runtimefeature"
	"binance_trader/internal/live/warmstate"
)

func TestRequiredMetricBlockerSource(t *testing.T) {
	stats := runtimefeature.Stats{MetricSources: map[string]runtimefeature.MetricSourceHealth{
		"metrics_oi":    {Status: "FRESH"},
		"metrics_taker": {Status: "STALE"},
	}}
	if got := requiredMetricBlockerSource(stats); got != "metrics_taker" {
		t.Fatalf("blocker source=%s", got)
	}
}

func TestSharedRuntimeTraderUIAdapter(t *testing.T) {
	static := fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("console")}}
	s, err := NewServer(fakeProvider{credentials.CredentialStore{}}, static, Options{})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixMilli()
	s.consumeSharedLiveEvent(binance.CaptureEvent{Source: "futures_aggTrade", SourceTimestampMs: now, Price: 100})
	s.consumeSharedLiveEvent(binance.CaptureEvent{Source: "spot_aggTrade", SourceTimestampMs: now, Price: 99})
	s.consumeSharedLiveEvent(binance.CaptureEvent{Source: "mark_price", SourceTimestampMs: now, Price: 100, Raw: json.RawMessage(`{"i":"99.5"}`)})
	if !s.market.Connected || s.market.FuturesPrice != 100 || s.market.SpotPrice != 99 || s.market.IndexPrice != 99.5 || s.futuresMessages != 1 || s.spotMessages != 1 {
		t.Fatalf("shared adapter: %+v", s.market)
	}
}

func TestParsePublicMillis(t *testing.T) {
	for _, input := range []string{`1789742533445`, `"1789742533445"`} {
		got, err := parsePublicMillis(json.RawMessage(input))
		if err != nil || got != 1789742533445 {
			t.Fatalf("timestamp %s: got %d, error %v", input, got, err)
		}
	}
	for _, input := range []string{`null`, `"bad"`, `{}`} {
		if _, err := parsePublicMillis(json.RawMessage(input)); err == nil {
			t.Fatalf("invalid timestamp accepted: %s", input)
		}
	}
}

func TestUnavailableFeatureSignalIsNotReady(t *testing.T) {
	static := fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("console")}}
	s, err := NewServer(fakeProvider{credentials.CredentialStore{}}, static, Options{})
	if err != nil {
		t.Fatal(err)
	}
	s.states[TradingEnvironmentTestnet].LastAutoResult = &autopipeline.Result{FinalSignal: "LONG"}
	r := perform(s.Handler(), http.MethodGet, "/api/auto-trading?environment=TESTNET", "", nil)
	if r.Code != http.StatusOK {
		t.Fatal(r.Code)
	}
	var body struct {
		Signal     string               `json:"signal"`
		LastResult *autopipeline.Result `json:"last_result"`
	}
	if err := json.Unmarshal(r.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Signal != "NOT_READY" || body.LastResult != nil {
		t.Fatalf("unready signal: %+v", body)
	}
}

func TestLiveInputConnectedButHistoricalGapDoesNotStartAuto(t *testing.T) {
	t.Setenv("BINANCE_ENV", "TESTNET")
	static := fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("console")}}
	path := filepath.Join(t.TempDir(), "capture-state.json")
	s, err := NewServer(fakeProvider{credentials.CredentialStore{}}, static, Options{WarmupPath: path, AutoFeatureSource: true, LiveFeatureRuntime: true})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	state := warmstate.Status{Source: "traderui_live_runtime", Status: "WARMING_UP", RequiredMs: warmstate.RequiredWarmupMs, AvailableMs: 30000, MissingMs: warmstate.RequiredWarmupMs - 30000, InputConnected: true, CurrentCaptureStartMs: now.UnixMilli() - 31000, LastEventTimeMs: now.UnixMilli() - 1000, LastCheckpointMs: now.UnixMilli()}
	if err := warmstate.PublishRuntime(s.runtimePath, state); err != nil {
		t.Fatal(err)
	}
	s.runtimeStats.Status.InputConnected = true
	r := perform(s.Handler(), http.MethodGet, "/api/auto-trading?environment=TESTNET", "", nil)
	var body struct {
		Signal    string `json:"signal"`
		Readiness struct {
			BlockedReason string `json:"blocked_reason"`
			FeatureSource bool   `json:"feature_source"`
			FeatureWarmup bool   `json:"feature_warmup"`
		} `json:"readiness"`
	}
	if r.Code != http.StatusOK || json.Unmarshal(r.Body.Bytes(), &body) != nil {
		t.Fatal("auto API failed")
	}
	if body.Signal != "NOT_READY" || body.Readiness.BlockedReason != "FEATURE_WARMUP" || !body.Readiness.FeatureSource || body.Readiness.FeatureWarmup {
		t.Fatalf("unsafe warmup promotion: %+v", body)
	}
}

func TestTraderUIStaleLocalStateCannotOverwritePersistedReady(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime-status.json")
	t0 := time.UnixMilli(1_800_000_000_000)
	s := warmstate.Status{Source: "traderui_live_runtime", Status: "STABILIZING", BootstrapState: "STABILIZING", RequiredMs: warmstate.RequiredWarmupMs, AvailableMs: warmstate.RequiredWarmupMs, MissingMs: 0, BootstrapReady: true, HandoffReady: true, LiveHealthReady: true, InputConnected: true, LastCheckpointMs: t0.Add(5 * time.Minute).UnixMilli(), BootstrapStartedAtMs: t0.UnixMilli(), BootstrapCompletedAtMs: t0.Add(time.Minute).UnixMilli(), StabilizationStartedAtMs: t0.UnixMilli(), StabilizationDeadlineMs: t0.Add(5 * time.Minute).UnixMilli()}
	ready, err := warmstate.PublishRuntimeResolved(path, s, t0.Add(5*time.Minute))
	if err != nil || !ready.Ready {
		t.Fatalf("ready setup: %+v %v", ready, err)
	}
	s.LiveHealthReady = false
	s.LastCheckpointMs = t0.Add(5*time.Minute + time.Second).UnixMilli()
	got, err := warmstate.PublishRuntimeResolved(path, s, t0.Add(5*time.Minute+time.Second))
	if err != nil || !got.Ready || got.Status != "READY" {
		t.Fatalf("stale traderui downgrade: %+v %v", got, err)
	}
}
