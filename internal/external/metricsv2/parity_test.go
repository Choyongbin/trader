package metricsv2

import "testing"

func TestCurrentDocumentedSemanticsAreFieldSpecific(t *testing.T) {
	for _, field := range FieldOrder {
		got, err := CurrentDocumentedSemantic(field)
		if err != nil {
			t.Fatal(err)
		}
		want := SemanticDocumentedEnd
		if field == TakerRatio {
			want = SemanticDocumentedStart
		}
		if got != want {
			t.Fatalf("field=%s got=%s want=%s", field, got, want)
		}
	}
}

func TestNormalizeLiveWaitsForIntervalAndReceipt(t *testing.T) {
	ts := int64(1_800_000_000_000)
	end, err := NormalizeLive(OpenInterest, ts, ts+12_000)
	if err != nil || end.IntervalEndMs != ts || end.MinimumAvailableMs != ts+12_000 {
		t.Fatalf("end=%+v err=%v", end, err)
	}
	start, err := NormalizeLive(TakerRatio, ts-IntervalMs, ts+7_000)
	if err != nil || start.IntervalEndMs != ts || start.MinimumAvailableMs != ts+7_000 {
		t.Fatalf("start=%+v err=%v", start, err)
	}
}

func TestCompleteIntervalsDoesNotJoinEqualRawTimestamps(t *testing.T) {
	ts := int64(1_800_000_000_000)
	var wrong []TimedObservation
	for _, field := range FieldOrder {
		o, err := NormalizeLive(field, ts, ts+IntervalMs+SafetyLagMs)
		if err != nil {
			t.Fatal(err)
		}
		wrong = append(wrong, o)
	}
	if got := len(CompleteIntervals(wrong)); got != 0 {
		t.Fatalf("equal raw timestamps joined: %d", got)
	}

	var aligned []TimedObservation
	for _, field := range FieldOrder {
		raw := ts
		if field == TakerRatio {
			raw = ts - IntervalMs
		}
		o, err := NormalizeLive(field, raw, ts+SafetyLagMs)
		if err != nil {
			t.Fatal(err)
		}
		aligned = append(aligned, o)
	}
	if got := len(CompleteIntervals(aligned)); got != 1 {
		t.Fatalf("normalized interval groups=%d want=1", got)
	}
}

func TestCompleteIntervalsRejectsMissingField(t *testing.T) {
	ts := int64(1_800_000_000_000)
	var rows []TimedObservation
	for _, field := range FieldOrder[:len(FieldOrder)-1] {
		o, err := NormalizeLive(field, ts, ts+SafetyLagMs)
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, o)
	}
	if got := len(CompleteIntervals(rows)); got != 0 {
		t.Fatalf("incomplete interval accepted: %d", got)
	}
}
