package main

import (
	"archive/zip"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNormalizedJoinBoundary(t *testing.T) {
	ts := []int64{changeMs - 2*intervalMs, changeMs - intervalMs, changeMs, changeMs + intervalMs, changeMs + 2*intervalMs}
	for _, mask := range []uint8{0, 31} {
		g := buildGroups(toCompleteRows(ts), mask)
		by := map[int64]uint8{}
		for _, v := range g {
			by[v.end] = v.mask
		}
		if by[changeMs] != 31 && by[changeMs] != 32 {
			t.Fatalf("change boundary should NOT have a complete interval; mask=%d got=%d", mask, by[changeMs])
		}
		if by[changeMs+intervalMs] != fullMask {
			t.Fatalf("expected corrected next full interval; mask=%d got=%d", mask, by[changeMs+intervalMs])
		}
	}
}
func TestDelayedFreshnessAndNoFuture(t *testing.T) {
	ts := []int64{}
	start := date(2024, 1, 1)
	for i := 0; i < 60; i++ {
		ts = append(ts, start+int64(i)*intervalMs)
	}
	g := buildGroups(toCompleteRows(ts), 0)
	p := partition{"SMOKE", start + 2*60*60*1000, start + 3*60*60*1000}
	a := scan(g, p, 0)
	if a.FutureSelections != 0 || a.MetricsFourLookbacksReady == 0 {
		t.Fatalf("unexpected immediate timing %+v", a)
	}
	b := scan(g, p, 600000)
	if b.FutureSelections != 0 || b.MetricsCurrentUsable != 0 || b.MetricsFourLookbacksReady != 0 {
		t.Fatalf("expected stale when earliest age 605s: %+v", b)
	}
}
func Test32StructuralCases(t *testing.T) {
	ts := []int64{date(2024, 6, 1), date(2024, 6, 1) + intervalMs, date(2024, 6, 1) + 2*intervalMs}
	for i := 0; i < 32; i++ {
		g := buildGroups(toCompleteRows(ts), uint8(i))
		if len(g) == 0 {
			t.Fatal("missing groups")
		}
		seen := map[int64]bool{}
		for _, v := range g {
			if seen[v.end] {
				t.Fatal("duplicate interval end")
			}
			seen[v.end] = true
		}
	}
}

func toCompleteRows(ts []int64) []sourceRow {
	r := make([]sourceRow, 0, len(ts))
	for _, t := range ts {
		r = append(r, sourceRow{timestamp: t, present: fullMask})
	}
	return r
}

// Regression for the 2024-06-12 raw archive: column 5
// sum_toptrader_long_short_ratio may legitimately be blank.
// The audit is timing-only; an absent value must NOT count toward a full join.
func TestMissingRawCellNeverCompletesGroup(t *testing.T) {
	dir := t.TempDir()
	fn := filepath.Join(dir, "BTCUSDT-metrics-2024-06-12.zip")
	f, err := os.Create(fn)
	if err != nil {
		t.Fatal(err)
	}
	z := zip.NewWriter(f)
	w, err := z.Create("BTCUSDT-metrics-2024-06-12.csv")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintln(w, "create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,count_long_short_ratio,sum_taker_long_short_vol_ratio")
	fmt.Fprintln(w, "2024-06-12 12:00:00.000,BTCUSDT,100,1000,1.5,,1.2,1.1")
	fmt.Fprintln(w, "2024-06-12 12:05:00.000,BTCUSDT,101,1001,1.6,1.3,1.3,1.2")
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := read2024(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.rows) != 2 || got.incompleteRows != 1 || got.missingCounts[fields[3]] != 1 {
		t.Fatalf("unexpected missing counter: %+v", got)
	}
	groups := buildGroups(got.rows, 31)
	byEnd := map[int64]uint8{}
	for _, g := range groups {
		byEnd[g.end] = g.mask
	}
	start := date(2024, 6, 12) + 12*60*60*1000
	if byEnd[start+intervalMs] != fullMask&^(1<<3) {
		t.Fatalf("missing top-position field was synthesized: mask=%b", byEnd[start+intervalMs])
	}
	if byEnd[start+2*intervalMs] != fullMask {
		t.Fatalf("complete subsequent row lost: mask=%b", byEnd[start+2*intervalMs])
	}
}

func TestNonEmptyMalformedFieldFails(t *testing.T) {
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "BTCUSDT-metrics-2024-06-12.zip"))
	if err != nil {
		t.Fatal(err)
	}
	z := zip.NewWriter(f)
	w, err := z.Create("BTCUSDT-metrics-2024-06-12.csv")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintln(w, "create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,count_long_short_ratio,sum_taker_long_short_vol_ratio")
	fmt.Fprintln(w, "2024-06-12 12:00:00.000,BTCUSDT,100,1000,1.5,NaN,1.2,1.1")
	_ = z.Close()
	_ = f.Close()
	_, err = read2024(dir, false)
	if err == nil || !strings.Contains(err.Error(), "invalid nonempty Metrics column 5") {
		t.Fatalf("expected invalid NaN failure, got %v", err)
	}
}
