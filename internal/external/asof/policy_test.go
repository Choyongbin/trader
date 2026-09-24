package asof

import "testing"

func TestDecisionBoundaries(t *testing.T) {
	spot := []Observation[int]{{Value: 4, SourceTimestampMs: 4_000}, {Value: 5, SourceTimestampMs: 5_000}}
	v, e := Lookup(Spot, spot, 5_000)
	if e != nil || !v.Available || v.Value != 4 {
		t.Fatalf("spot=%+v err=%v", v, e)
	}
	k := []Observation[int]{{Value: 1, SourceTimestampMs: 240_000, CloseTimeMs: 299_999}}
	for _, d := range []int64{300_000, 304_999} {
		v, _ = Lookup(Mark, k, d)
		if v.Available {
			t.Fatalf("kline available at %d", d)
		}
	}
	v, _ = Lookup(Mark, k, 305_000)
	if !v.Available {
		t.Fatal("kline unavailable")
	}
	for _, s := range []Source{Metrics, Funding} {
		o := []Observation[int]{{Value: 1, SourceTimestampMs: 300_000}}
		v, _ = Lookup(s, o, 300_000)
		if v.Available {
			t.Fatalf("%s same timestamp", s)
		}
		v, _ = Lookup(s, o, 305_000)
		if !v.Available {
			t.Fatalf("%s lag boundary", s)
		}
	}
}
func TestFreshnessAndMissingNeverUsesFuture(t *testing.T) {
	for _, x := range []struct {
		s   Source
		max int64
	}{{Mark, KlineMaxFreshAgeMs}, {Metrics, MetricsMaxFreshAgeMs}, {Funding, FundingMaxFreshAgeMs}} {
		o := []Observation[int]{{Value: 1, SourceTimestampMs: 0}}
		v, _ := Lookup(x.s, o, x.max)
		if !v.Fresh {
			t.Fatalf("%s exact stale", x.s)
		}
		v, _ = Lookup(x.s, o, x.max+1)
		if v.Fresh {
			t.Fatalf("%s +1 fresh", x.s)
		}
	}
	o := []Observation[int]{{Value: 12, SourceTimestampMs: 0}, {Value: 20, SourceTimestampMs: 600_000}}
	v, _ := Lookup(Metrics, o, 305_000)
	if !v.Available || v.Value != 12 {
		t.Fatalf("future gap lookup %+v", v)
	}
	k := []Observation[int]{{Value: 1, SourceTimestampMs: 0, CloseTimeMs: 59_999}, {Value: 3, SourceTimestampMs: 120_000, CloseTimeMs: 179_999}}
	v, _ = Lookup(Premium, k, 125_000)
	if v.Value != 1 {
		t.Fatalf("future missing minute %+v", v)
	}
}
func TestOptionalAndLiveReceiveGuard(t *testing.T) {
	v, e := Lookup(Metrics, []Observation[int]{{Value: 1, SourceTimestampMs: 100, ReceiveTimestampMs: 10_000}}, 9_999)
	if e != nil || v.Available {
		t.Fatalf("received future %+v %v", v, e)
	}
	v, e = Lookup(Metrics, []Observation[int]{{Value: 1, SourceTimestampMs: 100, ReceiveTimestampMs: 10_000}}, 10_000)
	if e != nil || !v.Available || v.AvailabilityAgeMs != 0 {
		t.Fatalf("receive boundary %+v %v", v, e)
	}
	v, _ = Lookup(Spot, []Observation[int](nil), 5_000)
	if v.Available || v.Fresh {
		t.Fatal("missing represented as available")
	}
}
