package main

import (
	"binance_trader/internal/external/metricsv2"
	"testing"
)

func TestSelectorsDoNotUseFutureStartInterval(t *testing.T) {
	p := []takerPoint{{RawTimestampMs: metricsv2.TakerChangeAtMs, IntervalStartMs: metricsv2.TakerChangeAtMs, IntervalEndMs: metricsv2.TakerChangeAtMs + metricsv2.IntervalMs, EarliestAvailableMs: metricsv2.TakerChangeAtMs + metricsv2.IntervalMs + metricsv2.SafetyLagMs, Value: 2}}
	decision := metricsv2.TakerChangeAtMs + metricsv2.SafetyLagMs
	if selectLegacy(p, decision) == nil {
		t.Fatal("legacy fixture should select")
	}
	if selectCorrected(p, decision, 0) != nil {
		t.Fatal("corrected selector used incomplete interval")
	}
	if selectCorrected(p, p[0].EarliestAvailableMs, 0) == nil {
		t.Fatal("corrected boundary not selected")
	}
}

func TestFreshnessUsesIntervalEnd(t *testing.T) {
	p := &takerPoint{RawTimestampMs: 1_000_000, IntervalEndMs: 1_300_000, Value: 1}
	ref := &takerPoint{RawTimestampMs: 700_000, IntervalEndMs: 1_000_000, Value: 1}
	if !takerEligible(p, ref, 1_300_000+metricsv2.FreshnessLimitMs, false) {
		t.Fatal("fresh boundary rejected")
	}
	if takerEligible(p, ref, 1_300_001+metricsv2.FreshnessLimitMs, false) {
		t.Fatal("stale row accepted")
	}
}

func TestAuditRejectsDuplicateAndValueMutation(t *testing.T) {
	source := []v1Row{{Timestamp: 300_000, OpenInterest: "1", OpenInterestValue: "2", TopTraderAccountRatio: "3", TopTraderPositionRatio: "4", GlobalRatio: "5", TakerRatio: "6"}}
	expanded, err := metricsv2.Expand(toSource(source[0]))
	if err != nil {
		t.Fatal(err)
	}
	pass := auditMonth("2024-01", source, expanded)
	if pass.DuplicateKeys != 0 || pass.ValueMismatches != 0 {
		t.Fatalf("pass=%+v", pass)
	}
	bad := append([]metricsv2.Observation(nil), expanded...)
	bad[0].Value = "changed"
	bad = append(bad, bad[0])
	fail := auditMonth("2024-01", source, bad)
	if fail.DuplicateKeys == 0 || fail.ValueMismatches == 0 {
		t.Fatalf("fail=%+v", fail)
	}
}

func TestIntervalGapAndOverlapAreExplicit(t *testing.T) {
	previous := takerPoint{IntervalEndMs: 1_000}
	if got := intervalDelta(previous, takerPoint{IntervalStartMs: 1_300}); got != 300 {
		t.Fatalf("gap=%d", got)
	}
	if got := intervalDelta(previous, takerPoint{IntervalStartMs: 900}); got != -100 {
		t.Fatalf("overlap=%d", got)
	}
}
