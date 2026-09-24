package mainbaseline

import (
	"math"
	"testing"
)

func closeTo(a, b float64) bool { return math.Abs(a-b) < 1e-12 }

func TestAlwaysNegativeAndPositiveMetrics(t *testing.T) {
	targets := []bool{true, false, true, false}
	for _, tc := range []struct {
		prediction     bool
		tp, fp, tn, fn int64
	}{{false, 0, 0, 2, 2}, {true, 2, 2, 0, 0}} {
		var m Classification
		for _, target := range targets {
			ObserveClassification(&m, target, tc.prediction)
		}
		FinishClassification(&m)
		if m.TP != tc.tp || m.FP != tc.fp || m.TN != tc.tn || m.FN != tc.fn || !closeTo(m.Accuracy, .5) || !closeTo(m.BalancedAccuracy, .5) {
			t.Fatalf("unexpected metrics: %+v", m)
		}
	}
}

func TestKnownMetrics(t *testing.T) {
	m := Classification{TP: 3, FP: 1, TN: 4, FN: 2}
	FinishClassification(&m)
	if !closeTo(m.Accuracy, .7) || !closeTo(m.Precision, .75) || !closeTo(m.Recall, .6) || !closeTo(m.Specificity, .8) || !closeTo(m.BalancedAccuracy, .7) || !closeTo(m.F1, 2.0/3.0) || !closeTo(m.MCC, 10/math.Sqrt(600)) {
		t.Fatalf("unexpected metrics: %+v", m)
	}
}

func TestProbabilityMetricsAndClipping(t *testing.T) {
	p := []float64{.8, .3}
	y := []bool{true, false}
	if !closeTo(Brier(p, y), .065) || !closeTo(LogLoss(p, y), -(math.Log(.8)+math.Log(.7))/2) {
		t.Fatal("probability metric mismatch")
	}
	if math.IsInf(LogLoss([]float64{0, 1}, []bool{true, false}), 0) {
		t.Fatal("log loss clipping failed")
	}
}

func TestMCCDoesNotOverflowAtDatasetScale(t *testing.T) {
	m := Classification{TP: 1_500_000, FP: 1_500_000, TN: 1_500_000, FN: 1_500_000}
	FinishClassification(&m)
	if math.IsNaN(m.MCC) || math.IsInf(m.MCC, 0) || m.MCC != 0 {
		t.Fatalf("invalid large-sample MCC: %v", m.MCC)
	}
}

func TestRankingMetricsWithTies(t *testing.T) {
	roc, ap := RankingMetrics([]float64{.9, .5}, []float64{.5, .1}, true)
	if !closeTo(roc, .875) || !closeTo(ap, 5.0/6.0) {
		t.Fatalf("roc=%f ap=%f", roc, ap)
	}
}
