package logistic

import (
	"math"
	"path/filepath"
	"testing"
)

func TestSigmoidStable(t *testing.T) {
	for _, z := range []float64{-1e6, 0, 1e6} {
		p := Sigmoid(z)
		if math.IsNaN(p) || math.IsInf(p, 0) || p < 0 || p > 1 {
			t.Fatalf("bad sigmoid %g", p)
		}
	}
}
func TestScalerPopulationVariance(t *testing.T) {
	s := NewScaler(1)
	for _, x := range []float64{1, 2, 3} {
		_ = s.Observe([]float64{x})
	}
	s.Finish()
	if math.Abs(s.Mean[0]-2) > 1e-12 || math.Abs(s.Std[0]-math.Sqrt(2.0/3)) > 1e-12 {
		t.Fatalf("bad scaler %+v", s)
	}
	z := make([]float64, 1)
	_ = s.TransformInto(z, []float64{2})
	if z[0] != 0 {
		t.Fatal("bad z")
	}
}
func TestArtifactRoundTripAndRegistryGuard(t *testing.T) {
	names := []string{"x"}
	a := Artifact{FeatureNames: names, FeatureCount: 1, FeatureRegistryHash: FeatureRegistryHash(names), Scaler: Scaler{Mean: []float64{0}, Std: []float64{1}}, Weights: []float64{2}}
	p1, _ := a.Predict([]float64{.5}, names)
	path := filepath.Join(t.TempDir(), "m.json")
	if err := WriteArtifact(path, a); err != nil {
		t.Fatal(err)
	}
	b, err := ReadArtifact(path)
	if err != nil {
		t.Fatal(err)
	}
	p2, err := b.Predict([]float64{.5}, names)
	if err != nil || p1 != p2 {
		t.Fatalf("round trip %v %v %v", p1, p2, err)
	}
	if _, err = b.Predict([]float64{.5}, []string{"y"}); err == nil {
		t.Fatal("registry mismatch accepted")
	}
}
func TestBCEAndL2ExcludeIntercept(t *testing.T) {
	c := OptimizerConfig{Name: "adam", BatchSize: 2, LearningRate: 0, Beta1: .9, Beta2: .999, Epsilon: 1e-8}
	tr := NewTrainer(1, .5, c)
	tr.Weights[0] = 2
	tr.Intercept = 3
	tr.Observe([]float64{0}, true)
	tr.Observe([]float64{0}, false)
	objective, _ := tr.FinishEpoch()
	expected := (Softplus(3)-3+Softplus(3))/2 + 1
	if math.Abs(objective-expected) > 1e-12 {
		t.Fatalf("objective %g want %g", objective, expected)
	}
}
func TestDeterministicLearnableFit(t *testing.T) {
	fit := func() *Trainer {
		c := OptimizerConfig{Name: "adam", BatchSize: 4, LearningRate: .05, Beta1: .9, Beta2: .999, Epsilon: 1e-8}
		tr := NewTrainer(1, 1e-4, c)
		for epoch := 0; epoch < 20; epoch++ {
			for i := -50; i <= 50; i++ {
				x := float64(i) / 10
				tr.Observe([]float64{x}, x > 0)
			}
			tr.FinishEpoch()
		}
		return tr
	}
	a, b := fit(), fit()
	if a.Intercept != b.Intercept || a.Weights[0] != b.Weights[0] || a.Weights[0] < 1 {
		t.Fatalf("non-deterministic/unlearned: %v %v", a.Weights, b.Weights)
	}
}
