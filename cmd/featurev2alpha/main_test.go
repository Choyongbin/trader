package main

import (
	"math"
	"testing"
	"time"
)

func TestClassificationPerfectRanking(t *testing.T) {
	rows := []prediction{{Probability: .9, Positive: true}, {Probability: .8, Positive: true}, {Probability: .2}, {Probability: .1}}
	m := classification(rows)
	if math.Abs(m.ROCAUC-1) > 1e-12 || math.Abs(m.AveragePrecision-1) > 1e-12 || math.Abs(m.PositiveRate-.5) > 1e-12 {
		t.Fatalf("metrics=%+v", m)
	}
}

func TestRegressionExact(t *testing.T) {
	rows := []prediction{{PredictedReturn: 1, ActualReturn: 1}, {PredictedReturn: 2, ActualReturn: 2}, {PredictedReturn: 3, ActualReturn: 3}}
	m := regression(rows)
	if m.MAE != 0 || m.RMSE != 0 || math.Abs(m.PearsonCorrelation-1) > 1e-12 {
		t.Fatalf("metrics=%+v", m)
	}
}

func TestBucketStatistics(t *testing.T) {
	rows := []prediction{{Probability: .4, ActualReturn: -1}, {Probability: .9, ActualReturn: 3, Positive: true}, {Probability: .8, ActualReturn: 1, Positive: true}, {Probability: .1, ActualReturn: -2}}
	buckets, _ := economics(rows, func(v prediction) float64 { return v.Probability })
	if buckets[0].Count != 2 || buckets[0].MeanActualReturn != 2 || buckets[0].MedianActualReturn != 2 || buckets[0].PositiveRate != 1 {
		t.Fatalf("bucket=%+v", buckets[0])
	}
}

func TestLinearTrainerLearnsDirection(t *testing.T) {
	trainer := newLinearTrainer(1)
	trainer.batchSize = 2
	for i := 0; i < 200; i++ {
		x := []float64{float64(i%5) - 2}
		trainer.observe(x, 2*x[0])
	}
	trainer.finish()
	if trainer.weights[0] <= 0 {
		t.Fatalf("weight=%g", trainer.weights[0])
	}
}

func TestFixedAnchorThinning(t *testing.T) {
	rows := []prediction{{Timestamp: 5000}, {Timestamp: 905000}, {Timestamp: 1805000}, {Timestamp: 3605000}}
	if got := len(filterByInterval(rows, 15*time.Minute)); got != 4 {
		t.Fatalf("15m rows=%d", got)
	}
	if got := len(filterByInterval(rows, 60*time.Minute)); got != 2 {
		t.Fatalf("60m rows=%d", got)
	}
}

func TestPercentileThreshold(t *testing.T) {
	values := []float64{1, 5, 3, 2, 4}
	if got := percentileThreshold(values, .4); got != 4 {
		t.Fatalf("threshold=%g", got)
	}
}
