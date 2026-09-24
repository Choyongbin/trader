package tradespeclabel

import (
	"fmt"
	"math"
	"strings"

	mainsplit "binance_trader/internal/split/main"
)

const (
	// Phase 9A and G1A use the same sequential summation and exact sorted p50.
	// This only absorbs floating-point accumulation and JSON serialization noise.
	ReproductionAbsTolerance = 1e-12
	ReproductionRelTolerance = 1e-9
)

var phase9BV1RequiredMetrics = Phase9ARequiredMetrics{
	ValidCount:               true,
	PositiveCount:            true,
	WinRate:                  true,
	MeanNetReturnExFunding:   true,
	MedianNetReturnExFunding: true,
}

// ActualPartitionStatistics is the G1A partition output with the identity and
// Phase 9A provenance required for strict reproduction comparison.
type ActualPartitionStatistics struct {
	Candidate                 Phase9ACandidate
	Partition                 mainsplit.Partition
	Statistics                Statistics
	SourcePhase9AReportSHA256 string
}

// CandidateStatistics groups all materialized partitions for one candidate.
// It is keyed by partition and therefore never relies on report array order.
type CandidateStatistics struct {
	Candidate                 Phase9ACandidate
	Partitions                map[mainsplit.Partition]Statistics
	SourcePhase9AReportSHA256 string
}

type MetricComparison struct {
	Name               string
	Required           bool
	Available          bool
	Passed             bool
	Actual             float64
	Expected           float64
	AbsoluteDifference float64
	AbsoluteTolerance  float64
	RelativeTolerance  float64
	Diagnostic         string
}

type ReproductionResult struct {
	Candidate Phase9ACandidate
	Partition mainsplit.Partition
	Passed    bool
	Metrics   []MetricComparison
}

type CandidateReproductionResult struct {
	Candidate  Phase9ACandidate
	Passed     bool
	Partitions map[mainsplit.Partition]ReproductionResult
}

// AlmostEqual applies the explicit absolute-or-relative floating comparison
// policy and rejects every non-finite input.
func AlmostEqual(actual, expected, absTolerance, relTolerance float64) bool {
	if math.IsNaN(actual) || math.IsNaN(expected) || math.IsInf(actual, 0) || math.IsInf(expected, 0) {
		return false
	}
	difference := math.Abs(actual - expected)
	if difference <= absTolerance {
		return true
	}
	return difference <= relTolerance*math.Max(math.Abs(actual), math.Abs(expected))
}

// FindPhase9AReference maps by native ID, side, and complete spec, not slice
// position. It rejects any ambiguous or incomplete identity.
func FindPhase9AReference(report Phase9AReport, candidate Phase9ACandidate) (Phase9AReference, error) {
	var found *Phase9AReference
	for index := range report.Frozen {
		reference := &report.Frozen[index]
		if reference.Candidate.SourceCandidateID == candidate.SourceCandidateID && reference.Candidate.Side == candidate.Side {
			if err := validateCandidateIdentity(candidate, reference.Candidate); err != nil {
				return Phase9AReference{}, err
			}
			if found != nil {
				return Phase9AReference{}, fmt.Errorf("ambiguous Phase 9A reference for candidate %s", candidate.SourceCandidateID)
			}
			found = reference
		}
	}
	if found == nil {
		return Phase9AReference{}, fmt.Errorf("Phase 9A reference not found for candidate %s", candidate.SourceCandidateID)
	}
	return *found, nil
}

func ComparePartitionReproduction(actual ActualPartitionStatistics, reference Phase9AReference, expectedPartition mainsplit.Partition, expectedReportSHA256 string) (ReproductionResult, error) {
	result := ReproductionResult{Candidate: actual.Candidate, Partition: actual.Partition}
	if actual.Partition != expectedPartition {
		return result, fmt.Errorf("reproduction identity mismatch: candidate=%s side=%s tp=%d sl=%d horizon=%d partition actual=%s expected=%s", actual.Candidate.SourceCandidateID, actual.Candidate.Side, actual.Candidate.TPBps, actual.Candidate.SLBps, actual.Candidate.HorizonSeconds, actual.Partition, expectedPartition)
	}
	if err := validateCandidateIdentity(actual.Candidate, reference.Candidate); err != nil {
		return result, err
	}
	if err := ValidateSHA256(expectedReportSHA256, actual.SourcePhase9AReportSHA256); err != nil {
		return result, fmt.Errorf("reproduction provenance mismatch: candidate=%s partition=%s: %w", actual.Candidate.SourceCandidateID, actual.Partition, err)
	}
	referencePartition, exists := reference.Partitions[actual.Partition]
	if !exists {
		return result, fmt.Errorf("reproduction reference partition missing: candidate=%s partition=%s", actual.Candidate.SourceCandidateID, actual.Partition)
	}

	requirements := comparatorRequirements()
	result.Metrics = []MetricComparison{
		compareCount("valid_count", requirements.ValidCount, actual.Statistics.LabelValidCount, referencePartition.ValidCount),
		compareCount("positive_count", requirements.PositiveCount, actual.Statistics.PositiveCount, referencePartition.PositiveCount),
		compareFloat("win_rate", requirements.WinRate, actual.Statistics.WinRate, referencePartition.WinRate),
		compareFloat("mean_net_return_ex_funding", requirements.MeanNetReturnExFunding, actual.Statistics.MeanNetReturnExFunding, referencePartition.MeanNetReturnExFunding),
		compareFloat("median_net_return_ex_funding", requirements.MedianNetReturnExFunding, actual.Statistics.MedianNetReturnExFunding, referencePartition.MedianNetReturnExFunding),
	}
	result.Passed = true
	for _, metric := range result.Metrics {
		if !metric.Passed {
			result.Passed = false
		}
	}
	if !result.Passed {
		return result, reproductionError(result)
	}
	return result, nil
}

func comparatorRequirements() Phase9ARequiredMetrics {
	// G1B verified every field below in all five frozen candidates and all
	// TRAIN/VALIDATION/TEST references of the current Phase 9B V1 report.
	// Generic parser references still preserve nil, but V1 reproduction never
	// silently skips any of these five authoritative metrics.
	return phase9BV1RequiredMetrics
}

func CompareCandidateReproduction(actual CandidateStatistics, reference Phase9AReference, expectedReportSHA256 string) (CandidateReproductionResult, error) {
	result := CandidateReproductionResult{Candidate: actual.Candidate, Partitions: map[mainsplit.Partition]ReproductionResult{}}
	if err := validateCandidateIdentity(actual.Candidate, reference.Candidate); err != nil {
		return result, err
	}
	for _, partition := range []mainsplit.Partition{mainsplit.Train, mainsplit.Validation, mainsplit.Test} {
		statistics, exists := actual.Partitions[partition]
		if !exists {
			return result, fmt.Errorf("reproduction actual partition missing: candidate=%s partition=%s", actual.Candidate.SourceCandidateID, partition)
		}
		partitionResult, err := ComparePartitionReproduction(ActualPartitionStatistics{Candidate: actual.Candidate, Partition: partition, Statistics: statistics, SourcePhase9AReportSHA256: actual.SourcePhase9AReportSHA256}, reference, partition, expectedReportSHA256)
		result.Partitions[partition] = partitionResult
		if err != nil {
			return result, err
		}
	}
	for partition := range actual.Partitions {
		if partition != mainsplit.Train && partition != mainsplit.Validation && partition != mainsplit.Test {
			return result, fmt.Errorf("reproduction unexpected partition: candidate=%s partition=%s", actual.Candidate.SourceCandidateID, partition)
		}
	}
	result.Passed = true
	return result, nil
}

func compareCount(name string, required bool, actual int64, expected *int64) MetricComparison {
	metric := MetricComparison{Name: name, Required: required, Actual: float64(actual), AbsoluteTolerance: 0, RelativeTolerance: 0}
	if expected == nil {
		metric.Available = false
		metric.Passed = !required
		if required {
			metric.Diagnostic = "required reference metric is NOT_AVAILABLE"
		} else {
			metric.Diagnostic = "NOT_AVAILABLE"
		}
		return metric
	}
	metric.Available = true
	metric.Expected = float64(*expected)
	metric.AbsoluteDifference = math.Abs(metric.Actual - metric.Expected)
	metric.Passed = actual == *expected
	if !metric.Passed {
		metric.Diagnostic = "exact integer count mismatch"
	}
	return metric
}

func compareFloat(name string, required bool, actual float64, expected *float64) MetricComparison {
	metric := MetricComparison{Name: name, Required: required, Actual: actual, AbsoluteTolerance: ReproductionAbsTolerance, RelativeTolerance: ReproductionRelTolerance}
	if expected == nil {
		metric.Available = false
		metric.Passed = !required
		if required {
			metric.Diagnostic = "required reference metric is NOT_AVAILABLE"
		} else {
			metric.Diagnostic = "NOT_AVAILABLE"
		}
		return metric
	}
	metric.Available = true
	metric.Expected = *expected
	metric.AbsoluteDifference = math.Abs(actual - *expected)
	if math.IsNaN(actual) || math.IsNaN(*expected) || math.IsInf(actual, 0) || math.IsInf(*expected, 0) {
		metric.Diagnostic = "non-finite value"
		return metric
	}
	metric.Passed = AlmostEqual(actual, *expected, ReproductionAbsTolerance, ReproductionRelTolerance)
	if !metric.Passed {
		metric.Diagnostic = "floating tolerance exceeded"
	}
	return metric
}

func validateCandidateIdentity(actual, reference Phase9ACandidate) error {
	if actual.SourceCandidateID != reference.SourceCandidateID || actual.Side != reference.Side || actual.TPBps != reference.TPBps || actual.SLBps != reference.SLBps || actual.HorizonSeconds != reference.HorizonSeconds {
		return fmt.Errorf("reproduction identity mismatch: actual={id=%s side=%s tp=%d sl=%d horizon=%d} reference={id=%s side=%s tp=%d sl=%d horizon=%d}", actual.SourceCandidateID, actual.Side, actual.TPBps, actual.SLBps, actual.HorizonSeconds, reference.SourceCandidateID, reference.Side, reference.TPBps, reference.SLBps, reference.HorizonSeconds)
	}
	return nil
}

func reproductionError(result ReproductionResult) error {
	parts := make([]string, 0)
	for _, metric := range result.Metrics {
		if metric.Passed {
			continue
		}
		parts = append(parts, fmt.Sprintf("candidate=%s side=%s tp=%d sl=%d horizon=%d partition=%s metric=%s expected=%.17g actual=%.17g diff=%.17g abs_tol=%.17g rel_tol=%.17g detail=%s", result.Candidate.SourceCandidateID, result.Candidate.Side, result.Candidate.TPBps, result.Candidate.SLBps, result.Candidate.HorizonSeconds, result.Partition, metric.Name, metric.Expected, metric.Actual, metric.AbsoluteDifference, metric.AbsoluteTolerance, metric.RelativeTolerance, metric.Diagnostic))
	}
	return fmt.Errorf("reproduction comparison failed: %s", strings.Join(parts, "; "))
}
