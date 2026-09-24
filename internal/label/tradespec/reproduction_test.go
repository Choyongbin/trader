package tradespeclabel

import (
	"math"
	"path/filepath"
	"strings"
	"testing"

	mainsplit "binance_trader/internal/split/main"
	"binance_trader/internal/tradelabel"
)

func TestComparePartitionReproductionExactG1AIntegration(t *testing.T) {
	actual, reference, hash := reproductionFixture()
	result, err := ComparePartitionReproduction(actual, reference, mainsplit.Train, hash)
	if err != nil || !result.Passed {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if len(result.Metrics) != 5 {
		t.Fatalf("metric count=%d", len(result.Metrics))
	}
}

func TestComparePartitionReproductionMismatches(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ActualPartitionStatistics, *Phase9AReference)
		needle string
	}{
		{name: "valid count", mutate: func(actual *ActualPartitionStatistics, _ *Phase9AReference) { actual.Statistics.LabelValidCount++ }, needle: "metric=valid_count"},
		{name: "positive count", mutate: func(actual *ActualPartitionStatistics, _ *Phase9AReference) { actual.Statistics.PositiveCount++ }, needle: "metric=positive_count"},
		{name: "win rate", mutate: func(actual *ActualPartitionStatistics, _ *Phase9AReference) { actual.Statistics.WinRate += .01 }, needle: "metric=win_rate"},
		{name: "mean net", mutate: func(actual *ActualPartitionStatistics, _ *Phase9AReference) {
			actual.Statistics.MeanNetReturnExFunding += .01
		}, needle: "metric=mean_net_return_ex_funding"},
		{name: "median", mutate: func(actual *ActualPartitionStatistics, _ *Phase9AReference) {
			actual.Statistics.MedianNetReturnExFunding += .01
		}, needle: "metric=median_net_return_ex_funding"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, reference, hash := reproductionFixture()
			test.mutate(&actual, &reference)
			result, err := ComparePartitionReproduction(actual, reference, mainsplit.Train, hash)
			if err == nil || result.Passed || !strings.Contains(err.Error(), test.needle) || !strings.Contains(err.Error(), "candidate=tp50_sl25_h900") || !strings.Contains(err.Error(), "abs_tol=") {
				t.Fatalf("result=%+v err=%v", result, err)
			}
		})
	}
}

func TestComparePartitionReproductionTolerance(t *testing.T) {
	actual, reference, hash := reproductionFixture()
	actual.Statistics.MeanNetReturnExFunding += ReproductionAbsTolerance
	if _, err := ComparePartitionReproduction(actual, reference, mainsplit.Train, hash); err != nil {
		t.Fatalf("within absolute tolerance: %v", err)
	}

	actual, reference, hash = reproductionFixture()
	actual.Statistics.MeanNetReturnExFunding = 100 + 5e-8
	expected := 100.0
	reference.Partitions[mainsplit.Train] = withMean(reference.Partitions[mainsplit.Train], &expected)
	if _, err := ComparePartitionReproduction(actual, reference, mainsplit.Train, hash); err != nil {
		t.Fatalf("within relative tolerance: %v", err)
	}
}

func TestComparePartitionReproductionRejectsNonFinite(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ActualPartitionStatistics, *Phase9AReference)
	}{
		{name: "actual NaN", mutate: func(actual *ActualPartitionStatistics, _ *Phase9AReference) {
			actual.Statistics.MeanNetReturnExFunding = math.NaN()
		}},
		{name: "reference NaN", mutate: func(_ *ActualPartitionStatistics, reference *Phase9AReference) {
			value := math.NaN()
			reference.Partitions[mainsplit.Train] = withMean(reference.Partitions[mainsplit.Train], &value)
		}},
		{name: "actual positive infinity", mutate: func(actual *ActualPartitionStatistics, _ *Phase9AReference) {
			actual.Statistics.MeanNetReturnExFunding = math.Inf(1)
		}},
		{name: "reference negative infinity", mutate: func(_ *ActualPartitionStatistics, reference *Phase9AReference) {
			value := math.Inf(-1)
			reference.Partitions[mainsplit.Train] = withMean(reference.Partitions[mainsplit.Train], &value)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, reference, hash := reproductionFixture()
			test.mutate(&actual, &reference)
			_, err := ComparePartitionReproduction(actual, reference, mainsplit.Train, hash)
			if err == nil || !strings.Contains(err.Error(), "non-finite") {
				t.Fatalf("expected non-finite failure, got %v", err)
			}
		})
	}
}

func TestComparePartitionReproductionIdentityAndProvenance(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ActualPartitionStatistics)
		want   string
	}{
		{name: "candidate ID", mutate: func(actual *ActualPartitionStatistics) { actual.Candidate.SourceCandidateID = "different" }, want: "identity mismatch"},
		{name: "side", mutate: func(actual *ActualPartitionStatistics) { actual.Candidate.Side = tradelabel.Long }, want: "identity mismatch"},
		{name: "TP", mutate: func(actual *ActualPartitionStatistics) { actual.Candidate.TPBps++ }, want: "identity mismatch"},
		{name: "SL", mutate: func(actual *ActualPartitionStatistics) { actual.Candidate.SLBps++ }, want: "identity mismatch"},
		{name: "horizon", mutate: func(actual *ActualPartitionStatistics) { actual.Candidate.HorizonSeconds++ }, want: "identity mismatch"},
		{name: "partition", mutate: func(actual *ActualPartitionStatistics) { actual.Partition = mainsplit.Test }, want: "partition actual=TEST expected=TRAIN"},
		{name: "report SHA-256", mutate: func(actual *ActualPartitionStatistics) { actual.SourcePhase9AReportSHA256 = strings.Repeat("b", 64) }, want: "provenance mismatch"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, reference, hash := reproductionFixture()
			test.mutate(&actual)
			if _, err := ComparePartitionReproduction(actual, reference, mainsplit.Train, hash); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("expected %q, got %v", test.want, err)
			}
		})
	}
}

func TestComparePartitionReproductionRequiredMetricUnavailable(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Phase9APartitionReference)
		metric string
	}{
		{name: "valid count", mutate: func(reference *Phase9APartitionReference) { reference.ValidCount = nil }, metric: "valid_count"},
		{name: "positive count", mutate: func(reference *Phase9APartitionReference) { reference.PositiveCount = nil }, metric: "positive_count"},
		{name: "win rate", mutate: func(reference *Phase9APartitionReference) { reference.WinRate = nil }, metric: "win_rate"},
		{name: "mean net", mutate: func(reference *Phase9APartitionReference) { reference.MeanNetReturnExFunding = nil }, metric: "mean_net_return_ex_funding"},
		{name: "median", mutate: func(reference *Phase9APartitionReference) { reference.MedianNetReturnExFunding = nil }, metric: "median_net_return_ex_funding"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, reference, hash := reproductionFixture()
			partition := reference.Partitions[mainsplit.Train]
			test.mutate(&partition)
			reference.Partitions[mainsplit.Train] = partition
			result, err := ComparePartitionReproduction(actual, reference, mainsplit.Train, hash)
			if err == nil || result.Passed || metricByName(t, result, test.metric).Diagnostic != "required reference metric is NOT_AVAILABLE" {
				t.Fatalf("result=%+v err=%v", result, err)
			}
		})
	}
}

func TestCompareCandidateReproductionOrderIndependent(t *testing.T) {
	actual, reference, hash := reproductionFixture()
	reference.Candidate.SourceCandidateID = "tp75_sl25_h900"
	reference.Candidate.TPBps = 75
	other := reference
	other.Candidate.SourceCandidateID = "tp50_sl25_h900"
	other.Candidate.TPBps = 50
	report := Phase9AReport{Frozen: []Phase9AReference{other, reference}}
	found, err := FindPhase9AReference(report, reference.Candidate)
	if err != nil || found.Candidate.SourceCandidateID != reference.Candidate.SourceCandidateID {
		t.Fatalf("order-independent reference mapping failed: %+v %v", found, err)
	}

	actual.Candidate = reference.Candidate
	partitions := map[mainsplit.Partition]Statistics{}
	for _, partition := range []mainsplit.Partition{mainsplit.Train, mainsplit.Validation, mainsplit.Test} {
		partitions[partition] = statisticsFromReference(reference.Partitions[mainsplit.Train])
		reference.Partitions[partition] = reference.Partitions[mainsplit.Train]
	}
	result, err := CompareCandidateReproduction(CandidateStatistics{Candidate: actual.Candidate, Partitions: partitions, SourcePhase9AReportSHA256: hash}, reference, hash)
	if err != nil || !result.Passed || len(result.Partitions) != 3 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestRealPhase9AReferenceComparatorSmoke(t *testing.T) {
	path := filepath.Join("..", "..", "..", "data", "reports", "research", "tradespec", "main", "v1", "BTCUSDT-tradespec-v1.json")
	report, err := LoadPhase9AReport(path)
	if err != nil || len(report.Frozen) != 5 {
		t.Fatalf("report=%+v err=%v", report, err)
	}
	for _, reference := range report.Frozen {
		actual := CandidateStatistics{Candidate: reference.Candidate, SourcePhase9AReportSHA256: report.SourceSHA256, Partitions: map[mainsplit.Partition]Statistics{}}
		for partition, metric := range reference.Partitions {
			actual.Partitions[partition] = statisticsFromReference(metric)
		}
		if result, err := CompareCandidateReproduction(actual, reference, report.SourceSHA256); err != nil || !result.Passed {
			t.Fatalf("candidate=%s result=%+v err=%v", reference.Candidate.SourceCandidateID, result, err)
		}
	}
}

func reproductionFixture() (ActualPartitionStatistics, Phase9AReference, string) {
	valid, positive := int64(10), int64(4)
	winRate, mean, median := .4, -.001, -.0015
	candidate := Phase9ACandidate{SourceCandidateID: "tp50_sl25_h900", Side: tradelabel.Short, TPBps: 50, SLBps: 25, HorizonSeconds: 900}
	metric := Phase9APartitionReference{ValidCount: &valid, PositiveCount: &positive, WinRate: &winRate, MeanNetReturnExFunding: &mean, MedianNetReturnExFunding: &median}
	hash := strings.Repeat("a", 64)
	return ActualPartitionStatistics{Candidate: candidate, Partition: mainsplit.Train, Statistics: statisticsFromReference(metric), SourcePhase9AReportSHA256: hash}, Phase9AReference{Candidate: candidate, Partitions: map[mainsplit.Partition]Phase9APartitionReference{mainsplit.Train: metric}}, hash
}

func statisticsFromReference(reference Phase9APartitionReference) Statistics {
	statistics := Statistics{}
	if reference.ValidCount != nil {
		statistics.LabelValidCount = *reference.ValidCount
	}
	if reference.PositiveCount != nil {
		statistics.PositiveCount = *reference.PositiveCount
	}
	if reference.WinRate != nil {
		statistics.WinRate = *reference.WinRate
	}
	if reference.MeanNetReturnExFunding != nil {
		statistics.MeanNetReturnExFunding = *reference.MeanNetReturnExFunding
	}
	if reference.MedianNetReturnExFunding != nil {
		statistics.MedianNetReturnExFunding = *reference.MedianNetReturnExFunding
	}
	return statistics
}

func withMean(reference Phase9APartitionReference, mean *float64) Phase9APartitionReference {
	reference.MeanNetReturnExFunding = mean
	return reference
}

func metricByName(t *testing.T, result ReproductionResult, name string) MetricComparison {
	t.Helper()
	for _, metric := range result.Metrics {
		if metric.Name == name {
			return metric
		}
	}
	t.Fatalf("metric %s not found", name)
	return MetricComparison{}
}
