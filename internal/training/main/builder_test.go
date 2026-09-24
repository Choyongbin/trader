package maintraining

import (
	mainbarrier "binance_trader/internal/barrier/main"
	mainfeature "binance_trader/internal/feature/main"
	mainoutcome "binance_trader/internal/outcome/main"
	"binance_trader/internal/tradelabel"
	"errors"
	"github.com/parquet-go/parquet-go"
	"os"
	"path/filepath"
	"testing"
)

func sources(t *testing.T, featureKeys, outcomeKeys, barrierKeys []int64) (string, string, string) {
	t.Helper()
	d := t.TempDir()
	fp, op, bp := filepath.Join(d, "f.parquet"), filepath.Join(d, "o.parquet"), filepath.Join(d, "b.parquet")
	fw, e := mainfeature.NewWriter(fp, false)
	if e != nil {
		t.Fatal(e)
	}
	for _, k := range featureKeys {
		if e = fw.Write(mainfeature.MainFeaturesV1{DecisionTimestampMs: k, ReferenceClose: 100}); e != nil {
			t.Fatal(e)
		}
	}
	if e = fw.Close(); e != nil {
		t.Fatal(e)
	}
	ow, e := mainoutcome.NewWriter(op, false)
	if e != nil {
		t.Fatal(e)
	}
	for _, k := range outcomeKeys {
		if e = ow.Write(mainoutcome.MainOutcomeV2{DecisionTimestampMs: k, ReferenceEntryPrice: 100}); e != nil {
			t.Fatal(e)
		}
	}
	if e = ow.Close(); e != nil {
		t.Fatal(e)
	}
	bw, e := mainbarrier.NewWriterV2(bp, false)
	if e != nil {
		t.Fatal(e)
	}
	for _, k := range barrierKeys {
		if e = bw.Write(mainbarrier.BarrierOutcomeV2{DecisionTimestampMs: k}); e != nil {
			t.Fatal(e)
		}
	}
	if e = bw.Close(); e != nil {
		t.Fatal(e)
	}
	return fp, op, bp
}
func config(f, o, b string) BuildConfig {
	return BuildConfig{FeaturePath: f, OutcomePath: o, BarrierPath: b, TradeSpec: tradelabel.TradeSpec{Side: tradelabel.Long, TPBps: 50, SLBps: 25, HorizonSeconds: 3600}}
}
func TestExactJoinPreservesInvalidLabel(t *testing.T) {
	f, o, b := sources(t, []int64{5000}, []int64{5000}, []int64{5000})
	var rows []TrainingRowV1
	st, e := Build(config(f, o, b), func(r TrainingRowV1) error { rows = append(rows, r); return nil })
	if e != nil {
		t.Fatal(e)
	}
	if st.JoinedRows != 1 || len(rows) != 1 || rows[0].LongLabelValid || rows[0].LongTradeResult != tradelabel.EntryReferenceUnavailable {
		t.Fatalf("stats=%+v rows=%+v", st, rows)
	}
}
func TestMissingKeyFails(t *testing.T) {
	f, o, b := sources(t, []int64{5000}, []int64{6000}, []int64{5000})
	if _, e := Build(config(f, o, b), func(TrainingRowV1) error { return nil }); e == nil {
		t.Fatal("expected missing-key error")
	}
}
func TestDuplicateKeyFails(t *testing.T) {
	f, o, b := sources(t, []int64{5000, 5000}, []int64{5000}, []int64{5000})
	if _, e := Build(config(f, o, b), func(TrainingRowV1) error { return nil }); e == nil {
		t.Fatal("expected duplicate-key error")
	}
}
func TestTrainingParquetRoundTrip(t *testing.T) {
	p := filepath.Join(t.TempDir(), "t.parquet")
	w, e := NewWriter(p, false)
	if e != nil {
		t.Fatal(e)
	}
	want := TrainingRowV1{MainFeaturesV1: mainfeature.MainFeaturesV1{DecisionTimestampMs: 5000, ReferenceClose: 100, RetLog5s: .1}, LongLabelValid: true, LongTradeResult: tradelabel.TPFirst}
	if e = w.Write(want); e != nil {
		t.Fatal(e)
	}
	if e = w.Close(); e != nil {
		t.Fatal(e)
	}
	var got TrainingRowV1
	n, e := Read(p, func(r TrainingRowV1) error { got = r; return nil })
	if e != nil || n != 1 || got.DecisionTimestampMs != 5000 || got.RetLog5s != .1 || got.LongTradeResult != tradelabel.TPFirst {
		t.Fatalf("n=%d got=%+v err=%v", n, got, e)
	}
	f, e := os.Open(p)
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	info, e := f.Stat()
	if e != nil {
		t.Fatal(e)
	}
	pf, e := parquet.OpenFile(f, info.Size())
	if e != nil {
		t.Fatal(e)
	}
	columns := pf.Schema().Columns()
	if len(columns) != 100 {
		t.Fatalf("physical columns=%d want 100", len(columns))
	}
	for _, path := range columns {
		if len(path) != 1 {
			t.Fatalf("nested physical column: %v", path)
		}
	}
}

func TestValidatedCloseDoesNotPublishOnFailure(t *testing.T) {
	p := filepath.Join(t.TempDir(), "bad.parquet")
	w, e := NewWriter(p, false)
	if e != nil {
		t.Fatal(e)
	}
	if e = w.Write(TrainingRowV1{}); e != nil {
		t.Fatal(e)
	}
	sentinel := errors.New("validation failed")
	if e = w.CloseValidated(func(string) error { return sentinel }); !errors.Is(e, sentinel) {
		t.Fatalf("error=%v", e)
	}
	if _, e = os.Stat(p); !errors.Is(e, os.ErrNotExist) {
		t.Fatalf("final output published: %v", e)
	}
}
