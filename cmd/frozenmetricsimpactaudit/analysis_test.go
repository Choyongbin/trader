package main

import (
	"math"
	"testing"
)

func TestNoFutureTaker(t *testing.T) {
	p := []takerPoint{{EffectiveMs: 305000, IntervalEndMs: 300000, Value: 2}, {EffectiveMs: 605000, IntervalEndMs: 600000, Value: 4}}
	if _, ok := selectPoint(p, 304999); ok {
		t.Fatal("future was selected")
	}
	if _, ok := selectPoint(p, 305000); !ok {
		t.Fatal("available at boundary")
	}
	if _, ok := selectPoint(p, 905001); !ok {
		t.Fatal("latest available fresh")
	}
	if _, ok := selectPoint(p, 1200001); ok {
		t.Fatal("stale selected")
	}
	lv, ch, ok := correctedTaker(p, 605000)
	if !ok || lv != 4 || ch != 1 {
		t.Fatalf("got %f %f %v", lv, ch, ok)
	}
}
func TestScoresOnlyTakerAndAblation(t *testing.T) {
	m := linearModel{Name: "mock", Mean: make([]float64, 128), Std: make([]float64, 128), ClassWeights: make([]float64, 128), ReturnWeights: make([]float64, 128), ReturnScale: 1000}
	for i := range m.Std {
		m.Std[i] = 1
	}
	m.ClassWeights[114] = 1
	m.ReturnWeights[115] = 2
	m.ClassWeights[100] = 2
	if err := m.validate(); err != nil {
		t.Fatal(err)
	}
	var v [128]float64
	v[114] = 1
	v[115] = 2
	v[100] = 2
	p, a, r, err := m.scores(v, 2, 4)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(p.Base-sigmoid(5)) > 1e-12 || math.Abs(p.Corrected-sigmoid(6)) > 1e-12 {
		t.Fatal("wrong score")
	}
	if math.Abs(a-sigmoid(1)) > 1e-12 || math.Abs(r-p.BaseReturn) > 1e-12 {
		t.Fatal("bad ablation")
	}
	if math.Abs(p.CorrectedReturn-.008) > 1e-12 {
		t.Fatalf("return %.8f", p.CorrectedReturn)
	}
}
func TestRankOverlap(t *testing.T) {
	a := []float64{9, 8, 7, 6, 5, 4, 3, 2, 1, 0}
	b := []float64{9, 7, 8, 6, 5, 4, 3, 2, 1, 0}
	if topOverlap(a, b, .2) != .5 {
		t.Fatal("top overlap wrong")
	}
	if math.Abs(percentileSorted([]float64{1, 2, 3, 4}, .5)-2.5) > 1e-12 {
		t.Fatal("median")
	}
}

func TestHypotheticalDelayDoesNotRelaxFreshness(t *testing.T) {
	points := []takerPoint{{EffectiveMs: 305_000, IntervalEndMs: 300_000, Value: 2}}
	if _, ok := selectOffsetPoint(points, 605_000, 300_000); !ok {
		t.Fatal("305s-old value should be available and fresh")
	}
	if _, ok := selectOffsetPoint(points, 605_000, 300_001); ok {
		t.Fatal("not yet assumed published")
	}
	if _, ok := selectOffsetPoint(points, 900_001, 300_000); ok {
		t.Fatal("staleness incorrectly measured against shifted anchor")
	}
}
