package runtimefeature

import (
	"testing"

	live "binance_trader/internal/live/binance"
)

func TestKlineCompletionRuleMatchesProductionParser(t *testing.T) {
	row := live.ExternalObservation{Dataset: "mark", SourceTimestampMs: 1_000, CloseTimeMs: 60_999, ReceiveTimestampMs: 60_999, Raw: []byte(`[1000,"1","1","1","1","1",60999]`)}
	if KlineObservationComplete(row) {
		t.Fatal("incomplete kline accepted")
	}
	parsed, err := parseExternal([]live.ExternalObservation{row})
	if err != nil || len(parsed) != 0 {
		t.Fatalf("production parser mismatch rows=%d err=%v", len(parsed), err)
	}
	row.ReceiveTimestampMs = 61_000
	if !KlineObservationComplete(row) {
		t.Fatal("completed kline rejected")
	}
	parsed, err = parseExternal([]live.ExternalObservation{row})
	if err != nil || len(parsed) != 1 {
		t.Fatalf("completed production parser mismatch rows=%d err=%v", len(parsed), err)
	}
}
