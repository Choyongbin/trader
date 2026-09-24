package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"binance_trader/internal/market"
)

func TestBridgeNoTradeSecondsAcrossCheckpointBoundary(t *testing.T) {
	rows := map[int64]market.SecondBar{
		1_000: {TimestampMs: 1_000, Close: 100, LastAggTradeID: 10, HasTrade: true},
		3_000: {TimestampMs: 3_000, Open: 101, FirstAggTradeID: 11, LastAggTradeID: 11, HasTrade: true},
	}
	bridgeNoTradeSeconds(rows)
	bar, ok := rows[2_000]
	if !ok || bar.HasTrade || bar.Close != 100 || bar.VWAP != 100 {
		t.Fatalf("bridged bar=%+v exists=%t", bar, ok)
	}
}

func TestBridgeNoTradeSecondsRejectsUnprovenGap(t *testing.T) {
	rows := map[int64]market.SecondBar{
		1_000: {TimestampMs: 1_000, Close: 100, LastAggTradeID: 10, HasTrade: true},
		3_000: {TimestampMs: 3_000, FirstAggTradeID: 12, LastAggTradeID: 12, HasTrade: true},
	}
	bridgeNoTradeSeconds(rows)
	if _, ok := rows[2_000]; ok {
		t.Fatal("unproven gap was bridged")
	}
}

func TestAuditWarmupStoreMergesContinuousSessions(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "sessions"), 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(name string, session warmupSession) {
		t.Helper()
		body, err := json.Marshal(session)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(root, "sessions", name), body, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	first := market.SecondBar{TimestampMs: 1_000, Close: 100, LastAggTradeID: 10, HasTrade: true}
	last := market.SecondBar{TimestampMs: 3_000, Open: 101, Close: 101, FirstAggTradeID: 11, LastAggTradeID: 11, HasTrade: true}
	write("session-1-2.json", warmupSession{StartedAtMs: 1, EndedAtMs: 2, FuturesBars: []market.SecondBar{first}, SpotBars: []market.SecondBar{first}})
	write("session-2-3.json", warmupSession{StartedAtMs: 2, EndedAtMs: 3, FuturesBars: []market.SecondBar{last}, SpotBars: []market.SecondBar{last}})
	state, err := auditWarmupStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if state.FuturesBars != 3 || state.SpotBars != 3 || state.CommonGapCount != 0 || state.AvailableContiguousHistoryMs != 3_000 {
		t.Fatalf("state=%+v", state)
	}
}
