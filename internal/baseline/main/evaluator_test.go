package mainbaseline

import (
	"math"
	"testing"

	mainsplit "binance_trader/internal/split/main"
)

func TestTrainPrevalenceIsFixedAcrossEvaluationData(t *testing.T) {
	e, err := NewEvaluator("LONG", "VALIDATION", .3)
	if err != nil {
		t.Fatal(err)
	}
	features := make([]float64, 80)
	for _, target := range []bool{true, true, false, false} {
		e.Observe(mainsplit.Sample{Features: features, Target: target})
	}
	for _, result := range e.Finish() {
		if result.BaselineName == TrainPrevalence {
			// Validation prevalence is .5, but predictions and probability metrics use fixed TRAIN p=.3.
			if result.Classification.TP != 0 || result.Classification.FN != 2 || !closeTo(result.BrierScore.Value, .29) || !closeTo(result.LogLoss.Value, -(math.Log(.3)+math.Log(.7))/2) {
				t.Fatalf("prevalence leakage or metric mismatch: %+v", result)
			}
			return
		}
	}
	t.Fatal("prevalence result missing")
}
