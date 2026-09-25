package uiapi

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"binance_trader/internal/live/warmstate"
)

func TestOperationalEntryBlockersAndKillSwitch(t *testing.T) {
	s := testServer(t)
	read := func() []EntryBlocker {
		t.Helper()
		response := perform(s.Handler(), http.MethodGet, "/api/entry-blockers?environment=TESTNET", "", nil)
		var body struct {
			Blockers []EntryBlocker `json:"blockers"`
		}
		if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &body) != nil {
			t.Fatalf("response=%s", response.Body.String())
		}
		return body.Blockers
	}
	contains := func(rows []EntryBlocker, code string) bool {
		for _, row := range rows {
			if row.Code == code {
				return true
			}
		}
		return false
	}
	if !contains(read(), "FEATURE_WARMUP") {
		t.Fatal("missing warmup blocker")
	}
	if !contains(read(), "MARKET_DATA_STALE") {
		t.Fatal("missing stale market blocker")
	}
	if response := perform(s.Handler(), http.MethodPost, "/api/auto-trading/emergency-stop?environment=TESTNET", s.csrf, nil); response.Code != http.StatusOK {
		t.Fatal(response.Body.String())
	}
	if !contains(read(), "KILL_SWITCH_ACTIVE") {
		t.Fatal("kill switch did not block new entry")
	}
	if response := perform(s.Handler(), http.MethodPost, "/api/auto-trading/start?environment=TESTNET", s.csrf, map[string]string{"model_profile_id": "btc-feature-v2-production-v1"}); response.Code != http.StatusConflict {
		t.Fatal("auto start accepted")
	}
}

func TestCLIAndUIReadinessUseSameStatusService(t *testing.T) {
	s := testServer(t)
	path := filepath.Join(t.TempDir(), "capture-state.json")
	checkpoint := map[string]any{"RequiredWarmupMs": 14_400_000, "AvailableContiguousHistoryMs": 14_398_000, "WarmupReady": false, "LastUpdatedMs": time.Now().Add(-24 * time.Hour).UnixMilli()}
	body, _ := json.Marshal(checkpoint)
	if err := os.WriteFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
	s.warmupPath = path
	cli := warmstate.Read(path, time.Now())
	response := perform(s.Handler(), http.MethodGet, "/api/status", "", nil)
	var ui struct {
		Warmup struct {
			Status    string `json:"status"`
			Required  int64  `json:"required_ms"`
			Available int64  `json:"available_ms"`
			Missing   int64  `json:"missing_ms"`
			Ready     bool   `json:"ready"`
		} `json:"warmup"`
	}
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &ui) != nil {
		t.Fatal("UI status decode failed")
	}
	if ui.Warmup.Status != cli.Status || ui.Warmup.Required != cli.RequiredMs || ui.Warmup.Available != cli.AvailableMs || ui.Warmup.Missing != cli.MissingMs || ui.Warmup.Ready != cli.Ready || ui.Warmup.Ready {
		t.Fatalf("CLI/UI readiness mismatch: cli=%+v ui=%+v", cli, ui.Warmup)
	}
}

func TestBlockerSinceTransitionsAndMultipleReasons(t *testing.T) {
	s := testServer(t)
	clock := time.Unix(1_800_000_000, 0).UTC()
	s.now = func() time.Time { return clock }
	find := func(code string) (EntryBlocker, bool) {
		for _, b := range s.entryBlockers(TradingEnvironmentTestnet) {
			if b.Code == code {
				return b, true
			}
		}
		return EntryBlocker{}, false
	}
	first, ok := find("MARKET_DATA_STALE")
	if !ok || !first.Active {
		t.Fatal("missing market blocker")
	}
	if _, ok := find("FEATURE_WARMUP"); !ok {
		t.Fatal("multiple blockers hidden")
	}
	clock = clock.Add(time.Second)
	second, _ := find("MARKET_DATA_STALE")
	if !second.Since.Equal(first.Since) {
		t.Fatal("active blocker Since changed")
	}
	clock = clock.Add(time.Second)
	s.SetMarket(Market{Connected: true, UpdatedAt: clock})
	s.mu.Lock()
	for _, kind := range []string{"futures", "spot", "mark"} {
		s.marketSourceUpdated[kind] = clock
	}
	s.mu.Unlock()
	if _, ok := find("MARKET_DATA_STALE"); ok {
		t.Fatal("cleared blocker remains active")
	}
	clock = clock.Add(11 * time.Second)
	third, ok := find("MARKET_DATA_STALE")
	if !ok || !third.Since.Equal(clock) {
		t.Fatal("reactivated blocker Since not reset")
	}
}
