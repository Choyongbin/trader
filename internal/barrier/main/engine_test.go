package mainbarrier

import (
	"binance_trader/internal/market"
	"testing"
)

func tr(id, ms int64, p float64) market.AggTrade {
	return market.AggTrade{AggTradeID: id, TradeTimeMs: ms, Price: p}
}
func TestExecutableEntrySameMillisecondAndFirstPassage(t *testing.T) {
	var got []BarrierOutcomeV1
	e, err := NewEngine(Config{StartDecisionTimestampMs: 1000, EndDecisionTimestampMs: 1000, DecisionIntervalMs: 5000, MaxEntryWaitMs: 100, MaxHorizonSeconds: 2}, func(x BarrierOutcomeV1) error { got = append(got, x); return nil })
	if err != nil {
		t.Fatal(err)
	}
	for _, x := range []market.AggTrade{tr(1, 999, 99), tr(2, 1000, 100), tr(3, 1000, 100.06), tr(4, 1001, 99.8), tr(5, 3000, 100)} {
		if err = e.Add(x); err != nil {
			t.Fatal(err)
		}
	}
	if err = e.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].EntryAggTradeID != 2 {
		t.Fatalf("entry/result=%+v", got)
	}
	u, _ := got[0].Hit(true, 5)
	if u.AggTradeID != 3 || u.TimestampMs != 1000 {
		t.Fatalf("same-ms future hit=%+v", u)
	}
	d, _ := got[0].Hit(false, 10)
	if d.AggTradeID != 4 {
		t.Fatalf("down hit=%+v", d)
	}
}
func TestEntryUnavailable(t *testing.T) {
	var got BarrierOutcomeV1
	e, _ := NewEngine(Config{StartDecisionTimestampMs: 1000, EndDecisionTimestampMs: 1000, DecisionIntervalMs: 5000, MaxEntryWaitMs: 10, MaxHorizonSeconds: 2}, func(x BarrierOutcomeV1) error { got = x; return nil })
	_ = e.Add(tr(1, 1011, 100))
	_ = e.Add(tr(2, 3000, 100))
	_ = e.Flush()
	if got.EntryAvailable || e.Stats().EntryUnavailable != 1 {
		t.Fatalf("got=%+v stats=%+v", got, e.Stats())
	}
}
