package session

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	live "binance_trader/internal/live/binance"
	"binance_trader/internal/live/runtimefeature"
	"binance_trader/internal/market"
)

func TestLastAggTradeIDSkipsTrailingNoTradeBars(t *testing.T) {
	rows := []market.SecondBar{{LastAggTradeID: 41}, {LastAggTradeID: 42}, {LastAggTradeID: 0}, {LastAggTradeID: 0}}
	if got := lastAggTradeID(rows); got != 42 {
		t.Fatalf("lastAggTradeID=%d want=42", got)
	}
	if got := lastAggTradeID(nil); got != 0 {
		t.Fatalf("empty lastAggTradeID=%d want=0", got)
	}
}

func TestIncompleteSnapshotDoesNotOverwriteExistingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "current.json")
	const existing = "valid-existing-snapshot"
	if err := os.WriteFile(path, []byte(existing), 0600); err != nil {
		t.Fatal(err)
	}
	if err := saveSnapshot(runtimefeature.New("", time.Now(), nil), path); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != existing {
		t.Fatalf("existing snapshot overwritten: %q", body)
	}
}

func TestFinalizeResultComputesAllDeltas(t *testing.T) {
	baseline := runtimefeature.Stats{ExternalUpdates: 10, FuturesBars: 20, SpotBars: 30, FeatureDecisions: 40, EligibleDecisions: 50, FutureObservations: 1, DuplicateEvents: 2, ReverseEvents: 3, IDGaps: 4}
	final := runtimefeature.Stats{ExternalUpdates: 15, FuturesBars: 27, SpotBars: 39, FeatureDecisions: 51, EligibleDecisions: 63, FutureObservations: 2, DuplicateEvents: 4, ReverseEvents: 6, IDGaps: 8}
	var result Result
	finalizeResult(&result, baseline, final, 1234*time.Millisecond)
	if result.LiveDurationMs != 1234 || result.ExternalUpdates != 5 || result.FuturesBars != 7 || result.SpotBars != 9 || result.FeatureDecisions != 11 || result.EligibleDecisions != 13 || result.FutureObservations != 1 || result.DuplicateEvents != 2 || result.ReverseEvents != 3 || result.IDGaps != 4 {
		t.Fatalf("unexpected finalized result: %+v", result)
	}
}

func TestMergeHandoffRemovesBufferedOverlap(t *testing.T) {
	catchup := []live.CaptureEvent{{Source: "futures_aggTrade", ID: 10}, {Source: "spot_aggTrade", ID: 20}}
	buffered := []live.CaptureEvent{{Source: "futures_aggTrade", ID: 10}, {Source: "spot_aggTrade", ID: 20}, {Source: "mark_price", Raw: json.RawMessage(`{}`)}}
	got := mergeHandoff(catchup, buffered)
	if len(got) != 3 {
		t.Fatalf("merged events=%d want=3", len(got))
	}
	counts := map[string]int{}
	for _, event := range got {
		counts[event.Source]++
	}
	if counts["futures_aggTrade"] != 1 || counts["spot_aggTrade"] != 1 || counts["mark_price"] != 1 {
		t.Fatalf("unexpected merged counts: %#v", counts)
	}
}
