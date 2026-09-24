package featurev2

import (
	"math"
	"reflect"
	"strings"
	"testing"

	"binance_trader/internal/external/asof"
)

func freshInput() Input {
	t := int64(1_000_000)
	s := SourceState{Available: true, Fresh: true, EffectiveAvailableAtMs: t}
	return Input{DecisionTimestampMs: t, Spot: s, Metrics: s, Mark: s, Index: s, Premium: s, Funding: s, WarmupReady: true, LookbackReady: true, FeaturesFinite: true}
}

func TestEligibilityReasons(t *testing.T) {
	tests := []struct {
		name string
		want Reason
		set  func(*Input)
	}{
		{"all fresh", Eligible, func(*Input) {}},
		{"spot missing", SpotUnavailable, func(x *Input) { x.Spot.Available = false }},
		{"metrics unavailable", MetricsUnavailable, func(x *Input) { x.Metrics.Available = false }},
		{"metrics stale", MetricsStale, func(x *Input) { x.Metrics.Fresh = false }},
		{"kline stale", KlineStale, func(x *Input) { x.Index.Fresh = false }},
		{"funding stale", FundingStale, func(x *Input) { x.Funding.Fresh = false }},
		{"lookback unavailable", LookbackUnavailable, func(x *Input) { x.LookbackReady = false }},
		{"non finite", NonFiniteFeature, func(x *Input) { x.FeaturesFinite = false }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			x := freshInput()
			tc.set(&x)
			got, err := Evaluate(x)
			if err != nil || got.Reason != tc.want || got.Eligible != (tc.want == Eligible) {
				t.Fatalf("got=%+v err=%v want=%s", got, err, tc.want)
			}
		})
	}
}

func TestFreshnessBoundary(t *testing.T) {
	o := asof.Observation[int]{SourceTimestampMs: 1000}
	atBoundary, err := asof.Lookup(asof.Metrics, []asof.Observation[int]{o}, 1000+asof.MetricsMaxFreshAgeMs)
	if err != nil || !atBoundary.Fresh {
		t.Fatalf("boundary got=%+v err=%v", atBoundary, err)
	}
	after, err := asof.Lookup(asof.Metrics, []asof.Observation[int]{o}, 1001+asof.MetricsMaxFreshAgeMs)
	if err != nil || after.Fresh {
		t.Fatalf("boundary+1 got=%+v err=%v", after, err)
	}
}

func TestFutureObservationRejected(t *testing.T) {
	x := freshInput()
	x.Spot.EffectiveAvailableAtMs = x.DecisionTimestampMs + 1
	got, err := Evaluate(x)
	if err == nil || got.Reason != FutureObservation {
		t.Fatalf("got=%+v err=%v", got, err)
	}
}

func TestNoSentinelAndLabelIndependent(t *testing.T) {
	if AllFinite(0, math.NaN()) || AllFinite(math.Inf(1)) {
		t.Fatal("non-finite accepted")
	}
	typ := reflect.TypeOf(Input{})
	for i := 0; i < typ.NumField(); i++ {
		name := strings.ToLower(typ.Field(i).Name)
		for _, forbidden := range []string{"label", "target", "return", "trade", "barrier", "candidate"} {
			if strings.Contains(name, forbidden) {
				t.Fatalf("label-dependent field %s", typ.Field(i).Name)
			}
		}
	}
	if V2MaxLookbackMs > RequiredWarmupMs || RequiredWarmupMs != V1WarmupMs {
		t.Fatal("warmup mismatch")
	}
}
