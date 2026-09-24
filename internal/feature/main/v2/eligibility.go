package featurev2

import (
	"fmt"
	"math"
)

const (
	EligibilityPolicyVersion int64 = 1
	V1WarmupMs               int64 = 14_400_000
	V2MaxLookbackMs          int64 = 3_600_000
	RequiredWarmupMs               = V1WarmupMs
)

type Reason string

const (
	Eligible            Reason = "ELIGIBLE"
	SpotUnavailable     Reason = "SPOT_UNAVAILABLE"
	MetricsUnavailable  Reason = "METRICS_UNAVAILABLE"
	MetricsStale        Reason = "METRICS_STALE"
	KlineUnavailable    Reason = "KLINE_UNAVAILABLE"
	KlineStale          Reason = "KLINE_STALE"
	FundingUnavailable  Reason = "FUNDING_UNAVAILABLE"
	FundingStale        Reason = "FUNDING_STALE"
	LookbackUnavailable Reason = "LOOKBACK_UNAVAILABLE"
	NonFiniteFeature    Reason = "NON_FINITE_FEATURE"
	FutureObservation   Reason = "FUTURE_OBSERVATION"
)

type SourceState struct {
	Available              bool
	Fresh                  bool
	EffectiveAvailableAtMs int64
}

type Input struct {
	DecisionTimestampMs int64
	Spot                SourceState
	Metrics             SourceState
	Mark                SourceState
	Index               SourceState
	Premium             SourceState
	Funding             SourceState
	WarmupReady         bool
	LookbackReady       bool
	FeaturesFinite      bool
}

type Result struct {
	Eligible bool
	Reason   Reason
}

// Evaluate is deliberately label-independent: Input contains source timing and
// feature validity only, and there is no numeric imputation path.
func Evaluate(in Input) (Result, error) {
	if in.DecisionTimestampMs < 0 {
		return Result{}, fmt.Errorf("negative decision timestamp")
	}
	for _, x := range []SourceState{in.Spot, in.Metrics, in.Mark, in.Index, in.Premium, in.Funding} {
		if x.Available && x.EffectiveAvailableAtMs > in.DecisionTimestampMs {
			return Result{Reason: FutureObservation}, fmt.Errorf("future observation: effective=%d decision=%d", x.EffectiveAvailableAtMs, in.DecisionTimestampMs)
		}
	}
	if !in.WarmupReady || !in.LookbackReady {
		return Result{Reason: LookbackUnavailable}, nil
	}
	if !in.Spot.Available {
		return Result{Reason: SpotUnavailable}, nil
	}
	if !in.Metrics.Available {
		return Result{Reason: MetricsUnavailable}, nil
	}
	if !in.Metrics.Fresh {
		return Result{Reason: MetricsStale}, nil
	}
	if !in.Mark.Available || !in.Index.Available || !in.Premium.Available {
		return Result{Reason: KlineUnavailable}, nil
	}
	if !in.Mark.Fresh || !in.Index.Fresh || !in.Premium.Fresh {
		return Result{Reason: KlineStale}, nil
	}
	if !in.Funding.Available {
		return Result{Reason: FundingUnavailable}, nil
	}
	if !in.Funding.Fresh {
		return Result{Reason: FundingStale}, nil
	}
	if !in.FeaturesFinite {
		return Result{Reason: NonFiniteFeature}, nil
	}
	return Result{Eligible: true, Reason: Eligible}, nil
}

func AllFinite(values ...float64) bool {
	for _, value := range values {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return false
		}
	}
	return true
}
