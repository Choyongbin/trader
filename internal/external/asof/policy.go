package asof

import (
	"fmt"
	"sort"
)

const (
	PolicyVersion              = 1
	ExternalSafetyLagMs  int64 = 5_000
	KlineMaxFreshAgeMs   int64 = 120_000
	MetricsMaxFreshAgeMs int64 = 600_000
	FundingMaxFreshAgeMs int64 = 57_600_000
)

type Source string

const (
	Spot    Source = "spot_1s"
	Mark    Source = "mark_1m"
	Index   Source = "index_1m"
	Premium Source = "premium_1m"
	Metrics Source = "metrics_5m"
	Funding Source = "funding"
)

type Observation[T any] struct {
	Value              T
	SourceTimestampMs  int64
	CloseTimeMs        int64
	ReceiveTimestampMs int64
}
type AsOfValue[T any] struct {
	Value                  T
	SourceTimestampMs      int64
	EffectiveAvailableAtMs int64
	AgeMs                  int64
	AvailabilityAgeMs      int64
	Available              bool
	Fresh                  bool
}

func EffectiveAvailableAt[T any](source Source, o Observation[T]) (int64, error) {
	var effective int64
	switch source {
	case Spot:
		effective = o.SourceTimestampMs + 1_000
	case Mark, Index, Premium:
		boundary := o.SourceTimestampMs + 60_000
		if o.CloseTimeMs != 0 {
			boundary = o.CloseTimeMs + 1
		}
		effective = boundary + ExternalSafetyLagMs
	case Metrics, Funding:
		effective = o.SourceTimestampMs + ExternalSafetyLagMs
	default:
		return 0, fmt.Errorf("unknown source %q", source)
	}
	if o.ReceiveTimestampMs > effective {
		effective = o.ReceiveTimestampMs
	}
	return effective, nil
}

func Lookup[T any](source Source, observations []Observation[T], decisionTimestampMs int64) (AsOfValue[T], error) {
	var zero AsOfValue[T]
	if decisionTimestampMs < 0 {
		return zero, fmt.Errorf("negative decision timestamp")
	}
	for i := 1; i < len(observations); i++ {
		if observations[i].SourceTimestampMs <= observations[i-1].SourceTimestampMs {
			return zero, fmt.Errorf("observations not strictly ordered")
		}
	}
	if source == Spot {
		target := decisionTimestampMs - 1_000
		i := sort.Search(len(observations), func(i int) bool { return observations[i].SourceTimestampMs >= target })
		if i == len(observations) || observations[i].SourceTimestampMs != target {
			return zero, nil
		}
		return selected(source, observations[i], decisionTimestampMs)
	}
	for i := len(observations) - 1; i >= 0; i-- {
		e, err := EffectiveAvailableAt(source, observations[i])
		if err != nil {
			return zero, err
		}
		if e <= decisionTimestampMs {
			return selected(source, observations[i], decisionTimestampMs)
		}
	}
	return zero, nil
}
func selected[T any](source Source, o Observation[T], decision int64) (AsOfValue[T], error) {
	e, err := EffectiveAvailableAt(source, o)
	if err != nil {
		return AsOfValue[T]{}, err
	}
	age := decision - o.SourceTimestampMs
	if age < 0 || e > decision {
		return AsOfValue[T]{}, fmt.Errorf("future observation selected")
	}
	max := int64(0)
	switch source {
	case Spot:
		max = 1_000
	case Mark, Index, Premium:
		max = KlineMaxFreshAgeMs
	case Metrics:
		max = MetricsMaxFreshAgeMs
	case Funding:
		max = FundingMaxFreshAgeMs
	}
	return AsOfValue[T]{Value: o.Value, SourceTimestampMs: o.SourceTimestampMs, EffectiveAvailableAtMs: e, AgeMs: age, AvailabilityAgeMs: decision - e, Available: true, Fresh: age <= max}, nil
}

// LiveUsableAt enforces historical/live parity by waiting for both source availability and local receipt.
func LiveUsableAt[T any](source Source, o Observation[T]) (int64, error) {
	return EffectiveAvailableAt(source, o)
}
