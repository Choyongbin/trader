package mainoutcome

import (
	mainfeature "binance_trader/internal/feature/main"
	"binance_trader/internal/market"
	"math"
	"testing"
)

func TestFeatureOutcomeJoinKeyAcrossBoundary(t *testing.T) {
	featureEngine := mainfeature.NewEngine()
	featureKeys := map[int64]bool{}
	outcomeKeys := map[int64]bool{}
	outcomeEngine := NewEngine(func(o MainOutcomeV1) error { outcomeKeys[o.DecisionTimestampMs] = true; return nil })
	base := int64(1706727600000) // 2024-01-31 19:00:00 UTC; stream crosses into February.
	for i := 0; i < 28810; i++ {
		bar := ob(base+int64(i)*1000, 100, 100, 100)
		if i == 18000 || i == 18001 {
			bar.HasTrade = false
			bar.AggTradeCount = 0
			bar.BaseVolume = 0
			bar.QuoteVolume = 0
			bar.TakerBuyBaseVolume = 0
			bar.TakerBuyQuoteVolume = 0
		}
		f, err := featureEngine.Add(bar)
		if err != nil {
			t.Fatal(err)
		}
		if f != nil {
			featureKeys[f.DecisionTimestampMs] = true
		}
		if err := outcomeEngine.Add(bar); err != nil {
			t.Fatal(err)
		}
	}
	matched := 0
	for key := range featureKeys {
		if outcomeKeys[key] {
			matched++
		}
	}
	if matched == 0 {
		t.Fatal("no feature/outcome decision key joined across boundary")
	}
}

func TestTimingTerminalAndExcursions(t *testing.T) {
	var got *MainOutcomeV1
	e := NewEngine(func(o MainOutcomeV1) error {
		if o.DecisionTimestampMs == 5000 {
			x := o
			got = &x
		}
		return nil
	})
	for i := 0; i <= 14404; i++ {
		p := 100.0
		if i >= 5 && i <= 64 {
			p = 110
		}
		high, low := p, p
		if i == 10 {
			high = 112
		}
		if i == 20 {
			low = 96
		}
		if i == 65 {
			p, high, low = 1000, 1000, 1000
		}
		if err := e.Add(ob(int64(i)*1000, p, high, low)); err != nil {
			t.Fatal(err)
		}
	}
	if got == nil {
		t.Fatal("decision 5s not emitted")
	}
	if got.ReferenceEntryPrice != 100 || got.FutureHigh60s != 112 || got.FutureLow60s != 96 {
		t.Fatalf("wrong [5,65) window: %+v", got)
	}
	if math.Abs(got.MarketReturn60s-.1) > 1e-12 || math.Abs(got.ShortReturn60s+.1) > 1e-12 {
		t.Fatal("terminal returns")
	}
	if math.Abs(got.LongMFE60s-.12) > 1e-12 || math.Abs(got.LongMAE60s-.04) > 1e-12 || math.Abs(got.ShortMFE60s-.04) > 1e-12 || math.Abs(got.ShortMAE60s-.12) > 1e-12 {
		t.Fatal("MFE/MAE")
	}
}

func TestUSDMDirectionalSymmetry(t *testing.T) {
	m, l, s, _, _, lmfe, lmae, smfe, smae := values(100, 90, 105, 80)
	if !same(m, -.1) || !same(l, -.1) || !same(s, .1) || !same(lmfe, .05) || !same(lmae, .2) || !same(smfe, .2) || !same(smae, .05) {
		t.Fatalf("wrong linear outcome: %g %g %g %g %g %g %g", m, l, s, lmfe, lmae, smfe, smae)
	}
	_, _, falling, _, _, _, _, _, _ := values(100, 50, 100, 50)
	_, _, rising, _, _, _, _, _, _ := values(100, 150, 150, 100)
	if !same(falling, .5) || !same(rising, -.5) {
		t.Fatalf("inverse return regression: falling=%g rising=%g", falling, rising)
	}
}
func TestTailExclusion(t *testing.T) {
	rows := 0
	e := NewEngine(func(MainOutcomeV1) error { rows++; return nil })
	for i := 0; i < 100; i++ {
		if err := e.Add(ob(int64(i)*1000, 100, 100, 100)); err != nil {
			t.Fatal(err)
		}
	}
	e.Flush()
	if rows != 0 || e.Stats().TailSkipped == 0 {
		t.Fatalf("rows=%d stats=%+v", rows, e.Stats())
	}
}
func ob(ts int64, c, h, l float64) market.SecondBar {
	return market.SecondBar{TimestampMs: ts, Open: c, High: h, Low: l, Close: c, VWAP: c, HasTrade: true, AggTradeCount: 1, BaseVolume: 1, QuoteVolume: c, TakerBuyBaseVolume: 1, TakerBuyQuoteVolume: c}
}
