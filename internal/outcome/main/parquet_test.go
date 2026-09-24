package mainoutcome

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestParquetRoundTrip(t *testing.T) {
	p := filepath.Join(t.TempDir(), "o.parquet")
	w, e := NewWriter(p, false)
	if e != nil {
		t.Fatal(e)
	}
	x := MainOutcomeV1{DecisionTimestampMs: 5000, ReferenceEntryPrice: 100, FutureHigh60s: 101, FutureLow60s: 99}
	if e = w.Write(x); e != nil {
		t.Fatal(e)
	}
	if e = w.Close(); e != nil {
		t.Fatal(e)
	}
	var y MainOutcomeV1
	n, e := Read(p, func(o MainOutcomeV1) error { y = o; return nil })
	if e != nil || n != 1 || !reflect.DeepEqual(x, y) {
		t.Fatalf("%d %v %+v", n, e, y)
	}
}
