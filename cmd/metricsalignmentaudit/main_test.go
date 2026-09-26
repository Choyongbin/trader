package main

import (
	"math"
	"testing"
)

func TestCandidateWindowsAreExplicit(t *testing.T) {
	got := candidatesFor(600_000)
	expected := []candidateWindow{{"END", 300_000}, {"START", 600_000}, {"PREV_END", 0}}
	if len(got) != len(expected) {
		t.Fatalf("len=%d", len(got))
	}
	for i := range got {
		if got[i] != expected[i] {
			t.Fatalf("candidate %d got=%+v want=%+v", i, got[i], expected[i])
		}
	}
}

func TestDailyAlignmentPicksMedianWinner(t *testing.T) {
	rows := []observation{
		{Day: "2024-03-03", Candidate: "END", VolumeUnit: "BASE", HistoricalRatio: 1, CalculatedRatio: 1, AbsoluteError: 0},
		{Day: "2024-03-03", Candidate: "START", VolumeUnit: "BASE", HistoricalRatio: 1, CalculatedRatio: 2, AbsoluteError: 1},
	}
	got := buildDailyAlignments(rows)
	if len(got) != 2 {
		t.Fatalf("len=%d", len(got))
	}
	for _, x := range got {
		if x.Candidate == "END" && !x.Preferred {
			t.Fatal("END should be preferred")
		}
		if x.Candidate == "START" && x.Preferred {
			t.Fatal("START should not be preferred")
		}
	}
}

func TestCompareDayRequiresExactRawValues(t *testing.T) {
	a := metricsRow{Timestamp: 1, OpenInterest: "1", OpenInterestValue: "2", TopTraderAccountRatio: "3", TopTraderPositionRatio: "4", GlobalRatio: "5", TakerRatio: "6"}
	pass := compareDay("2024-01-01", []metricsRow{a}, []metricsRow{a})
	if !pass.TimestampSetEqual || !pass.TakerValuesExact || !pass.AllMetricValuesExact {
		t.Fatalf("exact comparison failed: %+v", pass)
	}
	b := a
	b.TakerRatio = "6.0"
	fail := compareDay("2024-01-01", []metricsRow{a}, []metricsRow{b})
	if fail.TakerValuesExact || fail.AllMetricValuesExact {
		t.Fatalf("string mutation not detected: %+v", fail)
	}
}
func TestOnlyTrainValidationDates(t *testing.T) {
	if _, err := parseDates("2024-01-15,2024-10-15"); err != nil {
		t.Fatal(err)
	}
	for _, date := range []string{"2025-01-15", "2025-07-15", "2026-01-01", "not-a-date"} {
		if _, err := parseDates(date); err == nil {
			t.Fatalf("unexpected acceptance of %s", date)
		}
	}
}
func TestScoringAndQuantiles(t *testing.T) {
	vals := []observation{
		{Day: "a", HistoricalRatio: 1, CalculatedRatio: 1.001, AbsoluteError: .001, RelativeError: .001},
		{Day: "b", HistoricalRatio: 2, CalculatedRatio: 2.002, AbsoluteError: .002, RelativeError: .001},
		{Day: "b", HistoricalRatio: 3, CalculatedRatio: 3.003, AbsoluteError: .003, RelativeError: .001},
	}
	score := scoreGroup(vals, "END", "BASE")
	if score.N != 3 || score.Days != 2 || math.Abs(score.MedianAbsoluteError-.002) > 1e-10 {
		t.Fatalf("score=%+v", score)
	}
	if score.PearsonCorrelation == nil || math.Abs(*score.PearsonCorrelation-1) > 1e-9 {
		t.Fatalf("pearson=%v", score.PearsonCorrelation)
	}
}
