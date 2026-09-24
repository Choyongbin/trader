package tradespeclabel

import (
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"binance_trader/internal/tradelabel"
)

func TestAccumulatorCompleteStatistics(t *testing.T) {
	temporaryReturns := filepath.Join(t.TempDir(), "returns.bin")
	a, err := NewAccumulator(temporaryReturns)
	if err != nil {
		t.Fatal(err)
	}
	a.Purged()
	rows := []Row{
		{LabelValid: true, NetProfitableExFunding: true, TradeResult: tradelabel.TPFirst, GrossMarketReturn: .0100, FeeCostReturn: -.0010, ModeledSlippageCostReturn: -.0005, NetReturnExFunding: .0085},
		{LabelValid: true, TradeResult: tradelabel.SLFirst, GrossMarketReturn: -.0200, FeeCostReturn: -.0010, ModeledSlippageCostReturn: -.0005, NetReturnExFunding: -.0215},
		{LabelValid: true, NetProfitableExFunding: true, TradeResult: tradelabel.Timeout, GrossMarketReturn: .0060, FeeCostReturn: -.0010, ModeledSlippageCostReturn: -.0005, NetReturnExFunding: .0045},
		// Grossly profitable, but fees and slippage make it a net loss.
		{LabelValid: true, TradeResult: tradelabel.Timeout, GrossMarketReturn: .0010, FeeCostReturn: -.0010, ModeledSlippageCostReturn: -.0005, NetReturnExFunding: -.0015},
		{LabelValid: false, TradeResult: tradelabel.EntryReferenceUnavailable},
		{LabelValid: false, TradeResult: tradelabel.ExitReferenceUnavailable},
	}
	for _, row := range rows {
		if err := a.Add(row); err != nil {
			t.Fatal(err)
		}
	}
	got, err := a.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	if got.RawDecisionCount != 7 || got.PurgedCount != 1 || got.IncludedCount != 6 || got.LabelValidCount != 4 || got.LabelInvalidCount != 2 {
		t.Fatalf("unexpected decision counts: %+v", got)
	}
	if got.PositiveCount != 2 || got.NegativeCount != 2 || got.NetProfitableCount != 2 {
		t.Fatalf("invalid labels must not be negatives: %+v", got)
	}
	if got.TPFirstCount != 1 || got.SLFirstCount != 1 || got.TimeoutCount != 2 || got.EntryReferenceUnavailableCount != 1 || got.ExitReferenceUnavailableCount != 1 {
		t.Fatalf("unexpected result-status counts: %+v", got)
	}
	if got.GrossProfitableCount != 3 || got.GrossProfitableToNetUnprofitableCount != 1 {
		t.Fatalf("cost-flip statistics are wrong: %+v", got)
	}
	assertClose(t, got.ValidRate, 4.0/7.0)
	assertClose(t, got.PositiveRate, .5)
	assertClose(t, got.WinRate, .5)
	assertClose(t, got.MeanGrossMarketReturn, -.00075)
	assertClose(t, got.MeanFeeCostReturn, -.001)
	assertClose(t, got.MeanModeledSlippageCostReturn, -.0005)
	assertClose(t, got.MeanNetReturnExFunding, -.0025)
	assertClose(t, got.MedianNetReturnExFunding, .0015)
	if _, err := os.Stat(temporaryReturns); !os.IsNotExist(err) {
		t.Fatalf("temporary net-return stream was not removed: %v", err)
	}
}

func TestAccumulatorRejectsNonFiniteValidEconomics(t *testing.T) {
	for _, value := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		var a Accumulator
		err := a.Add(Row{LabelValid: true, TradeResult: tradelabel.TPFirst, GrossMarketReturn: value})
		if err == nil {
			t.Fatalf("expected non-finite economics rejection for %v", value)
		}
	}
}

func TestMedianMatchesPhase9AP50(t *testing.T) {
	cases := []struct {
		name string
		in   []float64
		want float64
	}{
		{name: "empty", in: nil, want: 0},
		{name: "odd", in: []float64{9, 1, 4}, want: 4},
		{name: "even", in: []float64{8, 2, 6, 4}, want: 5},
		{name: "duplicates", in: []float64{3, 1, 3, 1}, want: 2},
		{name: "mixed", in: []float64{.25, -.5, 0, 2.5, -.25, 1}, want: .125},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := append([]float64(nil), tc.in...)
			assertClose(t, Median(tc.in), tc.want)
			assertClose(t, Median(tc.in), tc.want)
			if !reflect.DeepEqual(tc.in, before) {
				t.Fatalf("Median mutated its input: got %v want %v", tc.in, before)
			}
		})
	}
}

func assertClose(t *testing.T, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-12 {
		t.Fatalf("got %.17g want %.17g", got, want)
	}
}
