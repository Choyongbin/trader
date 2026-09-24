package mainsplit

import (
	"bytes"
	"path/filepath"
	"testing"

	mainfeature "binance_trader/internal/feature/main"
	maintraining "binance_trader/internal/training/main"
)

func TestModelFeaturesUsesRegistryOnly(t *testing.T) {
	row := maintraining.TrainingRowV1{MainFeaturesV1: mainfeature.MainFeaturesV1{DecisionTimestampMs: 123, ReferenceClose: 456}, LongNetReturnExFunding: 999}
	features := ModelFeatures(row)
	if len(features) != 80 || len(features) != len(maintraining.ModelFeatureColumns) {
		t.Fatalf("got %d model features", len(features))
	}
	for _, x := range features {
		if x == 123 || x == 456 || x == 999 {
			t.Fatal("metadata/target leaked into feature vector")
		}
	}
}

func TestSideValidityIsIndependent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rows.parquet")
	w, err := maintraining.NewWriter(path, false)
	if err != nil {
		t.Fatal(err)
	}
	row := maintraining.TrainingRowV1{MainFeaturesV1: mainfeature.MainFeaturesV1{DecisionTimestampMs: 1000}, LongLabelValid: false, ShortLabelValid: true, ShortNetProfitableExFunding: true}
	if err := w.Write(row); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	loader := Loader{Definition: SplitDefinition{Train: Range{0, 2000}}, Files: []string{path}}
	longCount, err := loader.LoadTraining(Long, func(Sample) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	shortCount, err := loader.LoadTraining(Short, func(sample Sample) error {
		if !sample.Target || len(sample.Features) != 80 {
			t.Fatalf("unexpected short sample: %+v", sample)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if longCount != 0 || shortCount != 1 {
		t.Fatalf("side counts long=%d short=%d", longCount, shortCount)
	}
	var warning bytes.Buffer
	loader.Warning = &warning
	if _, err := loader.LoadFinalHoldout(Short, func(Sample) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if warning.Len() == 0 {
		t.Fatal("holdout access warning was not emitted")
	}
}
