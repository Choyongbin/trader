package mainfeature

import (
	"math"
	"testing"

	"binance_trader/internal/market"
)

func TestFeatureTimingWarmupReturnsAndNoLeakage(t *testing.T) {
	e := NewEngine()
	var first *MainFeaturesV1
	for i := 0; i <= 14404; i++ {
		price := 100 + float64(i)*0.01
		f, err := e.Add(testBar(int64(i)*1000, price, 1, 0))
		if err != nil {
			t.Fatal(err)
		}
		if i < 14400 && e.Ready() {
			t.Fatalf("ready too early at %d", i)
		}
		if f != nil {
			copy := *f
			first = &copy
		}
	}
	if !e.Ready() || first == nil {
		t.Fatal("engine did not become ready/emit")
	}
	if first.DecisionTimestampMs != 14405000 {
		t.Fatalf("decision=%d", first.DecisionTimestampMs)
	}
	want := math.Log((100 + 14404*.01) / (100 + (14404-5)*.01))
	if math.Abs(first.RetLog5s-want) > 1e-12 {
		t.Fatalf("ret5=%g want=%g", first.RetLog5s, want)
	}
	// The decision at 14,405 seconds contains the bar ending at 14,405 and
	// cannot contain a future bar whose timestamp is 14,405 seconds.
	before := first.ReferenceClose
	if _, err := e.Add(testBar(14405000, 999, 1, 0)); err != nil {
		t.Fatal(err)
	}
	if before == 999 {
		t.Fatal("future close leaked into prior snapshot")
	}
}

func TestClampImbalance(t *testing.T) {
	if got := clampImbalance(1.0000000015); got != 1 {
		t.Fatalf("upper=%g", got)
	}
	if got := clampImbalance(-1.0000000015); got != -1 {
		t.Fatalf("lower=%g", got)
	}
}

func TestRollingEvictionAndImbalance(t *testing.T) {
	e := NewEngine()
	var last *MainFeaturesV1
	for i := 0; i <= 14404; i++ {
		buy, sell := 6.0, 4.0
		if i == 14404 {
			buy, sell = 0, 10
		}
		f, err := e.Add(testBar(int64(i)*1000, 100, buy, sell))
		if err != nil {
			t.Fatal(err)
		}
		if f != nil {
			last = f
		}
	}
	if last == nil {
		t.Fatal("missing snapshot")
	}
	if math.Abs(last.TakerImbalance5s+0.04) > 1e-12 {
		t.Fatalf("imbalance5=%g", last.TakerImbalance5s)
	}
	if last.BaseVolumeSum5s != 50 || last.AggTradeCountSum5s != 10 {
		t.Fatalf("rolling sum failed: %+v", last)
	}
	if last.BaseVolumeSum60s != 600 || last.BaseVolumeSum300s != 3000 || e.w(14400).base != 144000 {
		t.Fatalf("rolling eviction boundaries failed: 60=%g 300=%g 14400=%g", last.BaseVolumeSum60s, last.BaseVolumeSum300s, e.w(14400).base)
	}
	if last.RetLog1s != 0 || last.RV14400s != 0 {
		t.Fatal("constant price should have zero return/rv")
	}
}

func TestMonthBoundarySyntheticDoesNotResetState(t *testing.T) {
	e := NewEngine()
	for i := 0; i <= 14400; i++ {
		if _, err := e.Add(testBar(int64(i)*1000, 100, 1, 0)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 14401; i <= 14404; i++ {
		b := testBar(int64(i)*1000, 100, 0, 0)
		b.HasTrade = false
		b.AggTradeCount = 0
		if _, err := e.Add(b); err != nil {
			t.Fatal(err)
		}
	}
	f, err := e.Add(testBar(14405000, 101, 1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if f != nil {
		t.Fatal("non-cadence bar emitted")
	}
	if !e.Ready() {
		t.Fatal("state reset")
	}
}

func testBar(ts int64, price, buy, sell float64) market.SecondBar {
	volume := buy + sell
	return market.SecondBar{TimestampMs: ts, Open: price, High: price, Low: price, Close: price, VWAP: price, BaseVolume: volume, QuoteVolume: volume * price, AggTradeCount: 2, TakerBuyBaseVolume: buy, TakerSellBaseVolume: sell, TakerBuyQuoteVolume: buy * price, TakerSellQuoteVolume: sell * price, HasTrade: true}
}
