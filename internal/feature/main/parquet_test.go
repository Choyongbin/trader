package mainfeature

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestParquetRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "features.parquet")
	w, err := NewWriter(path, false)
	if err != nil {
		t.Fatal(err)
	}
	want := MainFeaturesV1{DecisionTimestampMs: 5000, ReferenceClose: 100, RetLog5s: .1, TakerImbalance5s: .2, RangePosition60s: .5}
	if err := w.Write(want); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	var got MainFeaturesV1
	n, err := Read(path, func(f MainFeaturesV1) error { got = f; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip mismatch: %+v", got)
	}
}
