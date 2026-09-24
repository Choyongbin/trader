package main

import (
	"math"
	"testing"
)

func TestFundingDirectionAndBoundary(t *testing.T) {
	points := []fundingPoint{{Timestamp: 100, Rate: .001, Mark: 100, MarkTimestamp: 99}}
	for _, tc := range []struct {
		side string
		rate float64
		want float64
	}{{"LONG", .001, -.001}, {"SHORT", .001, .001}, {"LONG", -.001, .001}, {"SHORT", -.001, -.001}} {
		points[0].Rate = tc.rate
		rows, report, err := applyFunding("SYNTHETIC", []accountingTrade{{Side: tc.side, EntryTimestampMs: 0, ExitTimestampMs: 200, EntryPrice: 100}}, points)
		if err != nil || report.FundingEventsApplied != 1 || math.Abs(rows[0].FundingReturn-tc.want) > 1e-15 {
			t.Fatalf("side=%s rate=%g got=%g report=%+v err=%v", tc.side, tc.rate, rows[0].FundingReturn, report, err)
		}
	}
	for _, ts := range []int64{0, 200} {
		points[0].Timestamp = ts
		rows, report, err := applyFunding("BOUNDARY", []accountingTrade{{Side: "LONG", EntryTimestampMs: 0, ExitTimestampMs: 200, EntryPrice: 100}}, points)
		if err != nil || rows[0].FundingReturn != 0 || report.FundingEventsApplied != 0 {
			t.Fatalf("boundary %d was applied", ts)
		}
	}
}

func TestRiskSizingAndPnLNoLeverageMultiplier(t *testing.T) {
	p := makeRiskPolicy()
	for _, c := range p.Candidates {
		if !c.Valid || c.Leverage < 1 || c.Leverage > 5 || c.MarginFraction > .25 {
			t.Fatalf("invalid candidate risk: %+v", c)
		}
	}
	trade := accountingTrade{TradeID: "x", CandidateID: "tp50_sl25_h900", Side: "SHORT", EntryTimestampMs: 1, ExitTimestampMs: 2, EntryPrice: 100, GrossReturn: .01, NetReturnExFunding: .01}
	r := walletReplay("SYNTHETIC", "BASELINE", []accountingTrade{trade}, p, .0025, 1, 1, true)
	if len(r.Records) != 1 {
		t.Fatal("missing record")
	}
	want := r.Records[0].Notional * .01
	if math.Abs(r.Records[0].TradePnL-want) > 1e-12 {
		t.Fatalf("leverage double-counted: got=%g want=%g", r.Records[0].TradePnL, want)
	}
}
