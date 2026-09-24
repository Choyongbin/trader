package calibration

import (
	"math"
	"path/filepath"
	"testing"
)

func TestPlattIdentity(t *testing.T) {
	m := PlattModel{A: 1, B: 0}
	for _, p := range []float64{.1, .5, .9} {
		if math.Abs(m.Calibrate(p)-p) > 1e-12 {
			t.Fatal("identity failed")
		}
	}
}
func TestPlattLearnable(t *testing.T) {
	raw := []float64{.01, .05, .1, .2, .8, .9, .95, .99}
	y := []bool{false, false, false, true, false, true, true, true}
	m, err := FitPlatt(raw, y)
	if err != nil {
		t.Fatal(err)
	}
	before, after := plattLoss(raw, y, 1, 0), plattLoss(raw, y, m.A, m.B)
	if after >= before {
		t.Fatalf("Platt did not improve %g >= %g", after, before)
	}
}
func TestIsotonicTiesMonotonicAndBounds(t *testing.T) {
	m, err := FitIsotonic([]float64{.2, .2, .4, .6, .8}, []bool{false, true, false, true, true})
	if err != nil {
		t.Fatal(err)
	}
	previous := 0.0
	for _, p := range []float64{0, .2, .3, .4, .6, .8, 1} {
		v := m.Calibrate(p)
		if v < previous || v < 0 || v > 1 {
			t.Fatal("isotonic invalid")
		}
		previous = v
	}
	if m.Calibrate(.2) != m.Calibrate(.2) || m.Calibrate(0) != m.Values[0] || m.Calibrate(1) != m.Values[len(m.Values)-1] {
		t.Fatal("tie/clamp failed")
	}
}
func TestReliability(t *testing.T) {
	r := ReliabilityMetrics([]float64{.1, .2, .8, .9}, []bool{false, false, true, true})
	if r.EqualWidth[1].Count != 1 || r.EqualWidth[8].ActualPositiveRate != 1 || r.ECE <= 0 {
		t.Fatalf("bad reliability %+v", r)
	}
}
func TestSourceHashRoundTrip(t *testing.T) {
	a := Artifact{SelectedMethod: Platt, SourceModelSHA256: "abc", Platt: PlattModel{A: 1, B: 0}}
	p := filepath.Join(t.TempDir(), "c.json")
	if err := WriteArtifact(p, a); err != nil {
		t.Fatal(err)
	}
	b, err := ReadArtifact(p)
	if err != nil {
		t.Fatal(err)
	}
	x, err := b.Calibrate(.3, "abc")
	if err != nil || math.Abs(x-.3) > 1e-12 {
		t.Fatal("round trip")
	}
	if _, err = b.Calibrate(.3, "wrong"); err == nil {
		t.Fatal("hash mismatch accepted")
	}
}
