package tradespeclabel

import (
	"binance_trader/internal/tradelabel"
	"path/filepath"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	p := filepath.Join(t.TempDir(), "x.parquet")
	want := []Row{{DecisionTimestampMs: 5, LabelValid: false, TradeResult: tradelabel.EntryReferenceUnavailable}, {DecisionTimestampMs: 10, LabelValid: true, NetProfitableExFunding: true, TradeResult: tradelabel.TPFirst, NetReturnExFunding: .01}}
	if e := Write(p, want); e != nil {
		t.Fatal(e)
	}
	var got []Row
	_, e := Read(p, func(x Row) error { got = append(got, x); return nil })
	if e != nil || len(got) != len(want) || got[1].NetReturnExFunding != want[1].NetReturnExFunding || got[0].LabelValid {
		t.Fatalf("%v %+v", e, got)
	}
}
