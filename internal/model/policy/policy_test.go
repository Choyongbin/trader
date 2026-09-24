package policy

import "testing"

func TestDecide(t *testing.T) {
	a := Artifact{Long: SidePolicy{true, .6}, Short: SidePolicy{true, .7}}
	cases := []struct {
		l, s float64
		want Decision
	}{{.1, .2, NoTrade}, {.6, .2, Long}, {.2, .7, Short}, {.8, .75, Long}, {.65, .9, Short}, {.7, .8, Short}}
	for _, c := range cases {
		got, e := a.Decide(c.l, c.s)
		if e != nil || got != c.want {
			t.Fatalf("%v %v: %v %v", c.l, c.s, got, e)
		}
	}
	a.Long.Enabled = false
	if got, _ := a.Decide(.9, .2); got != NoTrade {
		t.Fatal(got)
	}
}
func TestExactEqualMargin(t *testing.T) {
	a := Artifact{Long: SidePolicy{true, .625}, Short: SidePolicy{true, .75}}
	if got, _ := a.Decide(.75, .875); got != NoTrade {
		t.Fatal(got)
	}
}
func TestQuantileTiesAndInvalid(t *testing.T) {
	o := []Observation{{Probability: .9, Valid: true, Net: 1, Profitable: true}, {Probability: .8, Valid: false}, {Probability: .8, Valid: true, Net: -1}, {Probability: .1, Valid: true}}
	ts := CandidateThresholds(o, []float64{.25, .5, .75})
	if len(ts) != 2 || ts[1].Threshold != .8 {
		t.Fatalf("%+v", ts)
	}
	s1, _, _ := Evaluate(o, .8)
	if s1.Signals != 3 || s1.Valid != 2 || s1.Invalid != 1 || s1.Profitable != 1 {
		t.Fatalf("%+v", s1)
	}
}
func TestPurgeBoundary(t *testing.T) {
	r := Range{0, 100}
	if IncludedInSelection(90, r, 10) {
		t.Fatal("equality must purge")
	}
	if !IncludedInSelection(89, r, 10) {
		t.Fatal("prior row must remain")
	}
}
func TestDeterminism(t *testing.T) {
	o := []Observation{{Probability: .8, Valid: true, Net: .1}, {Probability: .9, Valid: true, Net: .2}}
	a := BuildCandidates(o, []float64{.5, 1}, 1, 0)
	b := BuildCandidates(o, []float64{.5, 1}, 1, 0)
	if len(a) != len(b) || a[0].Threshold != b[0].Threshold {
		t.Fatal("not deterministic")
	}
}
