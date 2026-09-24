package mainbarrier

import (
	"binance_trader/internal/market"
	"testing"
)

func TestV2EntryBasedExpiryAcceptsHitAfterOldExpiry(t *testing.T) {
	var got BarrierOutcomeV2
	e, _ := NewEngineV2(ConfigV2{StartDecisionTimestampMs: 1000, EndDecisionTimestampMs: 1000, DecisionIntervalMs: 5000, MaxEntryWaitMs: 1000, MaxExitReferenceWaitMs: 10, MaxHorizonSeconds: 1}, func(x BarrierOutcomeV2) error { got = x; return nil })
	for _, x := range []market.AggTrade{tr(1, 1500, 100), tr(2, 2200, 100.1), tr(3, 2500, 100)} {
		if er := e.Add(x); er != nil {
			t.Fatal(er)
		}
	}
	if er := e.Flush(); er != nil {
		t.Fatal(er)
	}
	h, _ := got.Hit(true, 5)
	if h.AggTradeID != 2 {
		t.Fatalf("entry-based hit=%+v", h)
	}
}
func TestV2TimeoutReferenceAtTarget(t *testing.T) {
	var got BarrierOutcomeV2
	e, _ := NewEngineV2(ConfigV2{StartDecisionTimestampMs: 1000, EndDecisionTimestampMs: 1000, DecisionIntervalMs: 5000, MaxEntryWaitMs: 1000, MaxExitReferenceWaitMs: 10, MaxHorizonSeconds: 60}, func(x BarrierOutcomeV2) error { got = x; return nil })
	for _, x := range []market.AggTrade{tr(1, 1500, 100), tr(2, 61499, 101), tr(3, 61500, 102)} {
		if er := e.Add(x); er != nil {
			t.Fatal(er)
		}
	}
	if er := e.Flush(); er != nil {
		t.Fatal(er)
	}
	h, _ := got.TimeoutReference(60)
	if h.AggTradeID != 3 || h.Price != 102 {
		t.Fatalf("timeout=%+v", h)
	}
}
func TestV2TimeoutReferenceUnavailable(t *testing.T) {
	var got BarrierOutcomeV2
	e, _ := NewEngineV2(ConfigV2{StartDecisionTimestampMs: 1000, EndDecisionTimestampMs: 1000, DecisionIntervalMs: 5000, MaxEntryWaitMs: 1000, MaxExitReferenceWaitMs: 10, MaxHorizonSeconds: 60}, func(x BarrierOutcomeV2) error { got = x; return nil })
	_ = e.Add(tr(1, 1500, 100))
	_ = e.Add(tr(2, 61511, 102))
	if er := e.Flush(); er != nil {
		t.Fatal(er)
	}
	h, _ := got.TimeoutReference(60)
	if h.TimestampMs != 0 || e.Stats().TimeoutReferenceUnavailable[0] != 1 {
		t.Fatalf("timeout=%+v stats=%+v", h, e.Stats())
	}
}
