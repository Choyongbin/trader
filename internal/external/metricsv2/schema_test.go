package metricsv2

import "testing"

func source(ts int64) SourceRow {
	return SourceRow{TimestampMs: ts, OpenInterest: "1", OpenInterestValue: "2", TopTraderAccountRatio: "3", TopTraderPositionRatio: "4", GlobalRatio: "5", TakerRatio: "6"}
}

func taker(t *testing.T, ts int64) Observation {
	t.Helper()
	rows, err := Expand(source(ts))
	if err != nil {
		t.Fatal(err)
	}
	return rows[len(rows)-1]
}

func TestTransitionSemantics(t *testing.T) {
	before := taker(t, TakerChangeAtMs-IntervalMs)
	after := taker(t, TakerChangeAtMs)
	if before.TimestampSemantic != SemanticVerifiedEnd || before.IntervalEndMs != before.SourceTimestampMs {
		t.Fatalf("before=%+v", before)
	}
	if after.TimestampSemantic != SemanticVerifiedStart || after.IntervalEndMs != after.SourceTimestampMs+IntervalMs {
		t.Fatalf("after=%+v", after)
	}
	if after.EarliestAvailableMs != after.IntervalEndMs+SafetyLagMs {
		t.Fatalf("availability=%d", after.EarliestAvailableMs)
	}
}

func TestHistoricalBlocksUntilIntervalEndAndLag(t *testing.T) {
	o := taker(t, TakerChangeAtMs)
	for _, decision := range []int64{o.SourceTimestampMs + SafetyLagMs, o.IntervalEndMs + SafetyLagMs - 1} {
		got, err := SelectHistorical([]Observation{o}, decision, 0)
		if err != nil || got != nil {
			t.Fatalf("decision=%d got=%+v err=%v", decision, got, err)
		}
	}
	got, err := SelectHistorical([]Observation{o}, o.IntervalEndMs+SafetyLagMs, 0)
	if err != nil || got == nil || got.Observation.SourceTimestampMs != o.SourceTimestampMs {
		t.Fatalf("boundary got=%+v err=%v", got, err)
	}
}

func TestLiveWaitsForReceiveTimestamp(t *testing.T) {
	o := taker(t, TakerChangeAtMs)
	receive := o.EarliestAvailableMs + 12_345
	got, err := LiveUsableAt(o, receive, 0)
	if err != nil || got != receive {
		t.Fatalf("got=%d err=%v", got, err)
	}
}

func TestUnknownFieldCannotBeCorrectedAsOf(t *testing.T) {
	rows, err := Expand(source(TakerChangeAtMs))
	if err != nil {
		t.Fatal(err)
	}
	if rows[0].SemanticVerified || rows[0].EarliestAvailableMs != 0 {
		t.Fatalf("unknown field changed: %+v", rows[0])
	}
	if _, err = HistoricalUsableAt(rows[0], 0); err == nil {
		t.Fatal("unverified field accepted")
	}
}

func TestExtraDelaySensitivity(t *testing.T) {
	o := taker(t, TakerChangeAtMs)
	e, err := HistoricalUsableAt(o, 30_000)
	if err != nil || e != o.IntervalEndMs+SafetyLagMs+30_000 {
		t.Fatalf("effective=%d err=%v", e, err)
	}
}
