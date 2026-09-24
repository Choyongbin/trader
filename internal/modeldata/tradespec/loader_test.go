package tradespecmodeldata

import (
	"path/filepath"
	"testing"

	mainfeature "binance_trader/internal/feature/main"
	tradespeclabel "binance_trader/internal/label/tradespec"
	mainsplit "binance_trader/internal/split/main"
	maintraining "binance_trader/internal/training/main"
)

func TestStreamingExactJoinAndInvalidLabel(t *testing.T) {
	loader := testLoader([]FeatureRecord{feature(10, 1), feature(20, 2), feature(90, 3)}, map[mainsplit.Partition][]tradespeclabel.Row{
		mainsplit.Train: {{DecisionTimestampMs: 10, LabelValid: true, NetProfitableExFunding: true, NetReturnExFunding: .01}, {DecisionTimestampMs: 20, LabelValid: false}},
	})
	var samples []Sample
	n, err := loader.StreamTrain(func(sample Sample) error { samples = append(samples, sample); return nil })
	if err != nil || n != 1 || len(samples) != 1 {
		t.Fatalf("samples=%+v n=%d err=%v", samples, n, err)
	}
	if samples[0].DecisionTimestampMs != 10 || !samples[0].BinaryTarget || samples[0].NetReturnExFunding != .01 || samples[0].Features[0] != 1 {
		t.Fatalf("unexpected sample %+v", samples[0])
	}
}

func TestStreamingJoinFailures(t *testing.T) {
	tests := []struct {
		name     string
		features []FeatureRecord
		labels   []tradespeclabel.Row
	}{
		{name: "missing included label", features: []FeatureRecord{feature(10, 1)}},
		{name: "unexpected label", features: []FeatureRecord{feature(10, 1)}, labels: []tradespeclabel.Row{{DecisionTimestampMs: 5}}},
		{name: "duplicate feature", features: []FeatureRecord{feature(10, 1), feature(10, 2)}, labels: []tradespeclabel.Row{{DecisionTimestampMs: 10, LabelValid: true}}},
		{name: "reverse feature", features: []FeatureRecord{feature(20, 1), feature(10, 2)}, labels: []tradespeclabel.Row{{DecisionTimestampMs: 20, LabelValid: true}}},
		{name: "duplicate label", features: []FeatureRecord{feature(10, 1)}, labels: []tradespeclabel.Row{{DecisionTimestampMs: 10, LabelValid: true}, {DecisionTimestampMs: 10}}},
		{name: "reverse label", features: []FeatureRecord{feature(10, 1)}, labels: []tradespeclabel.Row{{DecisionTimestampMs: 10, LabelValid: true}, {DecisionTimestampMs: 5}}},
		{name: "label at purged tail", features: []FeatureRecord{feature(90, 1)}, labels: []tradespeclabel.Row{{DecisionTimestampMs: 90}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			loader := testLoader(test.features, map[mainsplit.Partition][]tradespeclabel.Row{mainsplit.Train: test.labels})
			if _, err := loader.StreamTrain(func(Sample) error { return nil }); err == nil {
				t.Fatal("expected join failure")
			}
		})
	}
}

func TestValidationAndTestTailPurge(t *testing.T) {
	loader := testLoader([]FeatureRecord{feature(110, 1), feature(190, 2), feature(210, 3), feature(290, 4)}, map[mainsplit.Partition][]tradespeclabel.Row{
		mainsplit.Validation: {{DecisionTimestampMs: 110, LabelValid: true}},
		mainsplit.Test:       {{DecisionTimestampMs: 210, LabelValid: true}},
	})
	if n, err := loader.StreamValidation(func(Sample) error { return nil }); err != nil || n != 1 {
		t.Fatalf("validation n=%d err=%v", n, err)
	}
	if n, err := loader.StreamTest(func(Sample) error { return nil }); err != nil || n != 1 {
		t.Fatalf("test n=%d err=%v", n, err)
	}
}

func TestFeatureRecordCanonicalOrderAndLeakageGuard(t *testing.T) {
	record, err := FeatureRecordFromV1(mainfeature.MainFeaturesV1{DecisionTimestampMs: 10, BarLogReturn: 1, BarRange: 2})
	if err != nil || record.Values[0] != 1 || record.Values[1] != 2 {
		t.Fatalf("record=%+v err=%v", record, err)
	}
	if len(maintraining.ModelFeatureColumns) != FeatureCount || ValidateModelFeatureColumns(maintraining.ModelFeatureColumns) != nil {
		t.Fatal("canonical feature registry is invalid")
	}
	columns := append([]string(nil), maintraining.ModelFeatureColumns...)
	columns[0] = "net_return_ex_funding"
	if ValidateModelFeatureColumns(columns) == nil {
		t.Fatal("leakage guard did not reject target column")
	}
}

func TestParquetSourcesStream(t *testing.T) {
	directory := t.TempDir()
	featurePath := filepath.Join(directory, "features.parquet")
	writer, err := mainfeature.NewWriter(featurePath, false)
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.Write(mainfeature.MainFeaturesV1{DecisionTimestampMs: 10, BarLogReturn: 7}); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	labelPath := filepath.Join(directory, "labels.parquet")
	if err = tradespeclabel.Write(labelPath, []tradespeclabel.Row{{DecisionTimestampMs: 10, LabelValid: true, NetProfitableExFunding: true, NetReturnExFunding: .02}}); err != nil {
		t.Fatal(err)
	}
	loader := NewParquetLoader(tradespeclabel.Manifest{MaxLabelDependencyMs: 10}, testSplit(), []string{featurePath}, map[mainsplit.Partition][]string{mainsplit.Train: {labelPath}})
	var sample Sample
	n, err := loader.StreamTrain(func(value Sample) error { sample = value; return nil })
	if err != nil || n != 1 || sample.Features[0] != 7 || !sample.BinaryTarget || sample.NetReturnExFunding != .02 {
		t.Fatalf("sample=%+v n=%d err=%v", sample, n, err)
	}
}

func testLoader(features []FeatureRecord, labels map[mainsplit.Partition][]tradespeclabel.Row) Loader {
	labelFactories := map[mainsplit.Partition]CandidateLabelSourceFactory{}
	for partition, rows := range labels {
		rows := append([]tradespeclabel.Row(nil), rows...)
		labelFactories[partition] = func() (CandidateLabelSource, error) { return &labelSliceSource{rows: rows}, nil }
	}
	rows := append([]FeatureRecord(nil), features...)
	return Loader{CandidateManifest: tradespeclabel.Manifest{MaxLabelDependencyMs: 10}, Split: testSplit(), Features: func() (FeatureSource, error) { return &featureSliceSource{rows: rows}, nil }, Labels: labelFactories}
}

func testSplit() mainsplit.SplitDefinition {
	return mainsplit.SplitDefinition{Train: mainsplit.Range{StartMs: 0, EndMs: 100}, Validation: mainsplit.Range{StartMs: 100, EndMs: 200}, Test: mainsplit.Range{StartMs: 200, EndMs: 300}, FinalHoldout: mainsplit.Range{StartMs: 300, EndMs: 400}}
}

func feature(timestamp int64, first float64) FeatureRecord {
	var record FeatureRecord
	record.DecisionTimestampMs = timestamp
	record.Values[0] = first
	return record
}

type featureSliceSource struct {
	rows  []FeatureRecord
	index int
}

func (s *featureSliceSource) NextFeature() (FeatureRecord, bool, error) {
	if s.index == len(s.rows) {
		return FeatureRecord{}, false, nil
	}
	row := s.rows[s.index]
	s.index++
	return row, true, nil
}
func (s *featureSliceSource) Close() error { return nil }

type labelSliceSource struct {
	rows  []tradespeclabel.Row
	index int
}

func (s *labelSliceSource) NextLabel() (tradespeclabel.Row, bool, error) {
	if s.index == len(s.rows) {
		return tradespeclabel.Row{}, false, nil
	}
	row := s.rows[s.index]
	s.index++
	return row, true, nil
}
func (s *labelSliceSource) Close() error { return nil }
