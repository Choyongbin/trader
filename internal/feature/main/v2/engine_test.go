package featurev2

import (
	"math"
	"reflect"
	"testing"

	"binance_trader/internal/external/asof"
	mainfeature "binance_trader/internal/feature/main"
	"binance_trader/internal/market"
	maintraining "binance_trader/internal/training/main"
)

func TestRegistryV1PrefixAndAmendedCount(t *testing.T) {
	if err := ValidateRegistry(ModelFeatureColumnsV2); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ModelFeatureColumnsV2[:80], maintraining.ModelFeatureColumns) {
		t.Fatal("V1 prefix mismatch")
	}
	if len(ModelFeatureColumnsV2) != 128 {
		t.Fatalf("count=%d", len(ModelFeatureColumnsV2))
	}
}

func TestSyntheticFormulasAndV1Regression(t *testing.T) {
	in := syntheticInput()
	snapshot, reason, err := Compute(in)
	if err != nil || reason != Eligible {
		t.Fatalf("reason=%s err=%v", reason, err)
	}
	v1, err := v1Values(in.V1)
	if err != nil {
		t.Fatal(err)
	}
	for i := range v1 {
		if snapshot.Values[i] != v1[i] {
			t.Fatalf("V1 mismatch i=%d got=%g want=%g", i, snapshot.Values[i], v1[i])
		}
	}
	assertNear(t, value(snapshot, "spot_return_5s"), math.Log(400.0/395.0))
	assertNear(t, value(snapshot, "spot_taker_imbalance_30s"), .2)
	assertNear(t, value(snapshot, "spot_perp_return_gap_5s"), math.Log(400.0/395.0)-.01)
	assertNear(t, value(snapshot, "oi_change_pct_5m"), 1.0/3.0)
	assertNear(t, value(snapshot, "oi_change_pct_15m"), 2.0/3.0)
	assertNear(t, value(snapshot, "oi_change_pct_60m"), 1)
	assertNear(t, value(snapshot, "top_trader_account_ls_ratio_change_5m"), 1.0/3.0)
	assertNear(t, value(snapshot, "mark_index_spread_bps"), 200)
	assertNear(t, value(snapshot, "premium_change_5m"), .002)
	assertNear(t, value(snapshot, "last_funding_rate"), .002)
	assertNear(t, value(snapshot, "funding_age_ms"), 5000)
	assertNear(t, value(snapshot, "funding_rate_change"), .001)
	if !AllFinite(snapshot.Values[:]...) {
		t.Fatal("non-finite snapshot")
	}
}

func TestBoundaryAndInvalidCases(t *testing.T) {
	in := syntheticInput()
	t.Run("Spot t bar ignored", func(t *testing.T) {
		base, _, err := Compute(in)
		if err != nil {
			t.Fatal(err)
		}
		x := in
		x.Spot = append(append([]market.SecondBar(nil), in.Spot...), market.SecondBar{TimestampMs: in.DecisionTimestampMs, Close: 1e9, QuoteVolume: 1e9})
		got, reason, err := Compute(x)
		if err != nil || reason != Eligible || value(got, "spot_return_5s") != value(base, "spot_return_5s") {
			t.Fatalf("reason=%s err=%v", reason, err)
		}
	})
	t.Run("availability boundaries", func(t *testing.T) {
		tm := in.DecisionTimestampMs
		k := []KlineObservation{{OpenTimeMs: tm - 65000, CloseTimeMs: tm - 5001, Close: 100}}
		if x, _ := lookupKline(asof.Mark, k, tm-1); x != nil {
			t.Fatal("kline close+4999 available")
		}
		if x, _ := lookupKline(asof.Mark, k, tm); x == nil {
			t.Fatal("kline close+5000 unavailable")
		}
		m := []MetricsObservation{{TimestampMs: tm - 5000, Value: metric(1, 1, 1, 1, 1, 1)}}
		if x, _ := lookupMetrics(m, tm-1); x != nil {
			t.Fatal("metrics +4999 available")
		}
		if x, _ := lookupMetrics(m, tm); x == nil {
			t.Fatal("metrics +5000 unavailable")
		}
		f := []FundingObservation{{TimestampMs: tm - 5000, Rate: .1}}
		if x, _ := lookupFunding(f, tm-1); x != nil {
			t.Fatal("funding +4999 available")
		}
		if x, _ := lookupFunding(f, tm); x == nil {
			t.Fatal("funding +5000 unavailable")
		}
	})
	t.Run("freshness exact boundary", func(t *testing.T) {
		m := []MetricsObservation{{TimestampMs: 1000, Value: metric(1, 1, 1, 1, 1, 1)}}
		x, _ := lookupMetrics(m, 1000+asof.MetricsMaxFreshAgeMs)
		if x == nil || !x.state.Fresh {
			t.Fatal("boundary stale")
		}
		x, _ = lookupMetrics(m, 1001+asof.MetricsMaxFreshAgeMs)
		if x == nil || x.state.Fresh {
			t.Fatal("boundary+1 fresh")
		}
	})
	t.Run("historical endpoint missing", func(t *testing.T) {
		x := in
		x.Metrics = append([]MetricsObservation(nil), in.Metrics[1:]...)
		_, reason, err := Compute(x)
		if err != nil || reason != LookbackUnavailable {
			t.Fatalf("reason=%s err=%v", reason, err)
		}
	})
	t.Run("zero denominator", func(t *testing.T) {
		x := in
		x.Spot = append([]market.SecondBar(nil), in.Spot...)
		for i := len(x.Spot) - 5; i < len(x.Spot); i++ {
			x.Spot[i].QuoteVolume = 0
			x.Spot[i].TakerBuyQuoteVolume = 0
			x.Spot[i].TakerSellQuoteVolume = 0
		}
		_, reason, err := Compute(x)
		if err != nil || reason != NonFiniteFeature {
			t.Fatalf("reason=%s err=%v", reason, err)
		}
	})
	t.Run("non-positive metrics operand", func(t *testing.T) {
		x := in
		x.Metrics = append([]MetricsObservation(nil), in.Metrics...)
		x.Metrics[len(x.Metrics)-1].Value.OpenInterest.Value = 0
		_, reason, err := Compute(x)
		if err != nil || reason != NonFiniteFeature {
			t.Fatalf("reason=%s err=%v", reason, err)
		}
	})
	t.Run("future observations ignored", func(t *testing.T) {
		base, _, _ := Compute(in)
		x := in
		x.Metrics = append(append([]MetricsObservation(nil), in.Metrics...), MetricsObservation{TimestampMs: in.DecisionTimestampMs + 300000, Value: metric(999, 999, 9, 9, 9, 9)})
		got, reason, err := Compute(x)
		if err != nil || reason != Eligible || value(got, "oi_change_pct_5m") != value(base, "oi_change_pct_5m") {
			t.Fatalf("reason=%s err=%v", reason, err)
		}
	})
}

func TestActualAugustKlineGapProduces24StaleDecisions(t *testing.T) {
	const (
		gapBefore = int64(1723456860000)
		gapAfter  = int64(1723457040000)
	)
	rows := []KlineObservation{
		{OpenTimeMs: gapBefore, CloseTimeMs: gapBefore + 59999, Close: 100},
		{OpenTimeMs: gapAfter, CloseTimeMs: gapAfter + 59999, Close: 101},
	}
	var stale int
	for decision := gapBefore + 65000; decision <= gapAfter+65000; decision += 5000 {
		x := selectKline(rows, decision)
		if x != nil && !x.state.Fresh {
			stale++
		}
	}
	if stale != 24 {
		t.Fatalf("KLINE_STALE=%d want=24", stale)
	}
}

func syntheticInput() ComputeInput {
	const decision int64 = 40_000_000
	v1 := mainfeature.MainFeaturesV1{DecisionTimestampMs: decision, ReferenceClose: 100, RetLog5s: .01, RetLog30s: .02, RetLog60s: .03, RetLog300s: .04, RetLog900s: .05, TakerImbalance5s: .1, TakerImbalance30s: .1, TakerImbalance60s: .1, QuoteVolumeSum30s: 200, QuoteVolumeSum300s: 2000}
	spot := make([]market.SecondBar, 0, 301)
	for i := int64(0); i <= 300; i++ {
		spot = append(spot, market.SecondBar{TimestampMs: decision - 301000 + i*1000, Open: 100 + float64(i), High: 101 + float64(i), Low: 99 + float64(i), Close: 100 + float64(i), QuoteVolume: 10, TakerBuyQuoteVolume: 6, TakerSellQuoteVolume: 4, AggTradeCount: 2})
	}
	metrics := []MetricsObservation{
		{decision - 3605000, metric(100, 1000, 1, 2, 3, 4)},
		{decision - 905000, metric(120, 1200, 1.2, 2.2, 3.2, 4.2)},
		{decision - 305000, metric(150, 1500, 1.5, 2.5, 3.5, 4.5)},
		{decision - 5000, metric(200, 2000, 2, 3, 4, 5)},
	}
	kline := func(values ...float64) []KlineObservation {
		times := []int64{decision - 3665000, decision - 965000, decision - 365000, decision - 65000}
		out := make([]KlineObservation, len(times))
		for i, ts := range times {
			out[i] = KlineObservation{OpenTimeMs: ts, CloseTimeMs: ts + 59999, Close: values[i]}
		}
		return out
	}
	return ComputeInput{DecisionTimestampMs: decision, HistoryStartTimestampMs: decision - RequiredWarmupMs, V1: v1, Spot: spot, Metrics: metrics, Mark: kline(99, 100, 101, 102), Index: kline(100, 100, 100, 100), Premium: kline(-.003, -.002, -.001, .001), Funding: []FundingObservation{{decision - 28805000, .001}, {decision - 5000, .002}}}
}

func metric(oi, oiv, a, p, g, t float64) MetricsValue {
	return MetricsValue{OptionalFloat{oi, true}, OptionalFloat{oiv, true}, OptionalFloat{a, true}, OptionalFloat{p, true}, OptionalFloat{g, true}, OptionalFloat{t, true}}
}
func value(snapshot Snapshot, name string) float64 {
	for i, n := range ModelFeatureColumnsV2 {
		if n == name {
			return snapshot.Values[i]
		}
	}
	panic(name)
}
func assertNear(t *testing.T, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-12 {
		t.Fatalf("got=%g want=%g", got, want)
	}
}
