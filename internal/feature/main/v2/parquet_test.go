package featurev2

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFeatureRowV2ParquetRoundTripAndAtomicPublication(t *testing.T) {
	path := filepath.Join(t.TempDir(), "features.parquet")
	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	var snapshot Snapshot
	snapshot.DecisionTimestampMs = 12345
	for i := range snapshot.Values {
		snapshot.Values[i] = float64(i) + 0.25
	}
	if err := w.Write(RowFromSnapshot(snapshot)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("final visible before publish: %v", err)
	}
	if err := w.CloseTemporary(); err != nil {
		t.Fatal(err)
	}
	if err := w.Publish(); err != nil {
		t.Fatal(err)
	}
	count, err := ReadRows(path, func(row FeatureRowV2) error {
		if row.DecisionTimestampMs != snapshot.DecisionTimestampMs {
			t.Fatalf("timestamp=%d", row.DecisionTimestampMs)
		}
		if got := row.FeatureValues(); got != snapshot.Values {
			t.Fatal("feature values changed")
		}
		return nil
	})
	if err != nil || count != 1 {
		t.Fatalf("count=%d err=%v", count, err)
	}
}
