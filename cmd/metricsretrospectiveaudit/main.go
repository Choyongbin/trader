// metricsretrospectiveaudit is an independent, read-only, assumption-labelled
// 2024 Metrics timing sensitivity tool. It NEVER trains models or trades.
// Run from repository root after copying cmd/metricsretrospectiveaudit into it.
package main

import (
	"archive/zip"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const intervalMs int64 = 300000
const safetyLagMs int64 = 5000
const freshAgeMs int64 = 600000
const changeMs int64 = 1709510400000 // 2024-03-04T00:00:00Z
const fullMask uint8 = 63
const unknownMask uint8 = 31 // 5 unverified CSV fields, independently toggled

type sourceRow struct {
	timestamp int64
	// Bit i is set only if raw Metrics field i has a nonempty finite number.
	// Unobserved fields must not be synthesized by a timing hypothesis.
	present uint8
}
type missingExample struct {
	Archive     string `json:"archive"`
	TimestampMs int64  `json:"timestamp_ms"`
	Field       string `json:"field"`
}
type sourceRows struct {
	rows            []sourceRow
	missingArchives []string
	missingCounts   map[string]int64
	missingExamples []missingExample
	incompleteRows  int64
}
type grouped struct {
	end    int64
	mask   uint8
	usable int64
}
type partition struct {
	name       string
	start, end int64
}
type summary struct {
	Scenario                  string `json:"scenario"`
	Hypothesis                string `json:"hypothesis"`
	ExtraDelayMs              int64  `json:"extra_delay_ms"`
	CompleteNormalizedGroups  int    `json:"complete_normalized_groups"`
	PartialNormalizedGroups   int    `json:"partial_normalized_groups"`
	GridDecisions             int64  `json:"decision_grid_count"`
	MetricsCurrentUsable      int64  `json:"metrics_current_usable_and_fresh"`
	MetricsFourLookbacksReady int64  `json:"metrics_current_5m_15m_60m_ready_and_fresh"`
	FutureSelections          int64  `json:"future_selections"`
	StaleCurrent              int64  `json:"stale_current_count"`
	UnavailableCurrent        int64  `json:"unavailable_current_count"`
}
type report struct {
	Status                          string           `json:"status"`
	Scope                           string           `json:"scope"`
	FrozenCommit                    string           `json:"frozen_commit"`
	Method                          string           `json:"method"`
	PublicationTimeCaveat           string           `json:"publication_time_caveat"`
	OriginalMetricsRows             int              `json:"original_metrics_rows"`
	IncompleteValueRows             int64            `json:"raw_rows_with_missing_field"`
	MissingRawFieldCounts           map[string]int64 `json:"missing_raw_field_counts"`
	MissingFieldExamples            []missingExample `json:"missing_raw_field_examples"`
	MissingValuePolicy              string           `json:"missing_raw_value_policy"`
	Input2024Days                   int              `json:"input_2024_days"`
	MissingArchives                 []string         `json:"missing_archives"`
	ExpectedCountMatchesPriorReport bool             `json:"expected_105281_rows_matches_phase3a"`
	Grid                            []summary        `json:"grid"`
	GroupCombinationCounts          []groupCount     `json:"group_combinations"`
	FullCorrectedAvailable          bool             `json:"full_corrected_available"`
	LegacyModelUsed                 bool             `json:"legacy_model_used"`
	TestAccessed                    bool             `json:"test_accessed"`
	FinalHoldoutAccessed            bool             `json:"final_holdout_accessed"`
}
type groupCount struct {
	AssumedStartFields []string `json:"assumed_start_fields"`
	Complete           int      `json:"complete_groups"`
	Partial            int      `json:"partial_groups"`
}

var fields = []string{"open_interest", "open_interest_value", "top_trader_account_ratio", "top_trader_position_ratio", "global_ratio"}
var parts = []partition{
	{"TRAIN", date(2024, 1, 1), date(2024, 10, 1)},
	{"VALIDATION", date(2024, 10, 1), date(2025, 1, 1)},
}

func date(y, m, d int) int64 { return time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC).UnixMilli() }
func main() {
	root := flag.String("raw-root", filepath.FromSlash("history_external/raw/binance/futures/um/daily/metrics/BTCUSDT"), "2024 original Binance daily Metrics ZIP root")
	out := flag.String("out", filepath.FromSlash("data/reports/diagnostics/metrics-conservative-sensitivity/phase3b2-v1"), "NEW output directory; existing files never overwritten")
	strict := flag.Bool("strict", true, "require all 366 original daily archives")
	flag.Parse()
	if err := run(*root, *out, *strict); err != nil {
		fmt.Fprintln(os.Stderr, "READ_ONLY_AUDIT_FAIL:", err)
		os.Exit(1)
	}
}
func run(root, out string, strict bool) error {
	if _, err := os.Stat(out); err == nil {
		return fmt.Errorf("refuse to overwrite: %s", out)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	source, err := read2024(root, strict)
	if err != nil {
		return err
	}
	if len(source.rows) == 0 {
		return errors.New("no 2024 source timestamps")
	}
	r := report{Status: "ASSUMPTION_SENSITIVITY_NOT_HISTORICAL_VERIFICATION", Scope: "2024_TRAIN_VALIDATION_ONLY", FrozenCommit: "45de18aa7e1af7bb85068a5925d5e6fd28d1054b", Method: "Unknown fields independently assumed END or START; Taker uses verified 2024-03-04 change point. Timed results simulate a corrected normalized-interval group only; NOT complete Feature/model eligibility.", PublicationTimeCaveat: "Every delay is hypothetical; Binance 2024 actual API publication/receipt timestamps are absent. None of these scenarios certifies historical-live parity.", OriginalMetricsRows: len(source.rows), IncompleteValueRows: source.incompleteRows, MissingRawFieldCounts: source.missingCounts, MissingFieldExamples: source.missingExamples, MissingValuePolicy: "A blank numeric field is absent for this timestamp only. It is never filled, never marked present, and may make normalized interval groups partial.", Input2024Days: 366 - len(source.missingArchives), MissingArchives: source.missingArchives, ExpectedCountMatchesPriorReport: len(source.rows) == 105281, Grid: []summary{}, GroupCombinationCounts: []groupCount{}, FullCorrectedAvailable: false, LegacyModelUsed: false, TestAccessed: false, FinalHoldoutAccessed: false}
	// Enumerate ALL 2^5 start/end combinations; OI and OI-value are retained
	// as distinct field-level hypotheses despite sharing an API endpoint.
	for mask := 0; mask <= int(unknownMask); mask++ {
		groups := buildGroups(source.rows, uint8(mask))
		complete, partial := 0, 0
		for _, g := range groups {
			if g.mask == fullMask {
				complete++
			} else {
				partial++
			}
		}
		startFields := []string{}
		for i, f := range fields {
			if mask&(1<<i) != 0 {
				startFields = append(startFields, f)
			}
		}
		r.GroupCombinationCounts = append(r.GroupCombinationCounts, groupCount{startFields, complete, partial})
		// Test all 32 structural group mappings, but only run the 5s-grid
		// latency scan for the all-END and all-START endmembers; other mixtures
		// have individual normalized-group completeness recorded above.
		if mask != 0 && mask != int(unknownMask) {
			continue
		}
		for _, extra := range []int64{0, 60000, 180000, 300000, 600000} {
			for _, p := range parts {
				result := scan(groups, p, extra)
				result.Scenario = fmt.Sprintf("UNVERIFIED_FIELDS_%s_%s", map[bool]string{true: "ALL_START", false: "ALL_END"}[mask == int(unknownMask)], p.name)
				result.Hypothesis = "Assume all five UNVERIFIED historical fields have the named interval semantic; Taker observed change-point is fixed. Not a factual certification."
				r.Grid = append(r.Grid, result)
			}
		}
	}
	if err = os.MkdirAll(out, 0755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(out, "metrics_timing_scenarios.json"), append(b, '\n'), 0644); err != nil {
		return err
	}
	f, err := os.Create(filepath.Join(out, "metrics_timing_scenarios.csv"))
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	_ = w.Write([]string{"scenario", "extra_delay_ms", "complete_groups", "partial_groups", "decision_grid_count", "metrics_current_fresh", "metrics_current_plus_5m_15m_60m_fresh", "future_selections", "stale_current", "unavailable_current"})
	for _, s := range r.Grid {
		_ = w.Write([]string{s.Scenario, fmt.Sprint(s.ExtraDelayMs), fmt.Sprint(s.CompleteNormalizedGroups), fmt.Sprint(s.PartialNormalizedGroups), fmt.Sprint(s.GridDecisions), fmt.Sprint(s.MetricsCurrentUsable), fmt.Sprint(s.MetricsFourLookbacksReady), fmt.Sprint(s.FutureSelections), fmt.Sprint(s.StaleCurrent), fmt.Sprint(s.UnavailableCurrent)})
	}
	w.Flush()
	closeErr := f.Close()
	if w.Error() != nil {
		return w.Error()
	}
	if closeErr != nil {
		return closeErr
	}
	fmt.Printf("READ_ONLY PASS input_rows=%d incomplete_raw_rows=%d 32 semantic combinations 20 grid scans output=%s\n", len(source.rows), source.incompleteRows, out)
	return nil
}
func read2024(root string, strict bool) (sourceRows, error) {
	out := sourceRows{rows: make([]sourceRow, 0, 105281), missingArchives: []string{}, missingCounts: make(map[string]int64)}
	seen := map[int64]bool{}
	for d := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC); d.Before(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)); d = d.AddDate(0, 0, 1) {
		name := fmt.Sprintf("BTCUSDT-metrics-%s.zip", d.Format("2006-01-02"))
		p := filepath.Join(root, name)
		z, err := zip.OpenReader(p)
		if errors.Is(err, os.ErrNotExist) {
			out.missingArchives = append(out.missingArchives, name)
			continue
		}
		if err != nil {
			return out, fmt.Errorf("%s: %w", p, err)
		}
		if len(z.File) == 0 {
			_ = z.Close()
			return out, fmt.Errorf("%s: empty ZIP", p)
		}
		var data io.ReadCloser
		for _, zf := range z.File {
			if strings.HasSuffix(strings.ToLower(zf.Name), ".csv") {
				data, err = zf.Open()
				break
			}
		}
		if data == nil || err != nil {
			_ = z.Close()
			return out, fmt.Errorf("%s: CSV missing or invalid", p)
		}
		reader := csv.NewReader(data)
		reader.FieldsPerRecord = -1
		header, e := reader.Read()
		if e != nil {
			_ = data.Close()
			_ = z.Close()
			return out, fmt.Errorf("%s: no header", p)
		}
		if len(header) < 8 {
			_ = data.Close()
			_ = z.Close()
			return out, fmt.Errorf("%s: unexpected header %v", p, header)
		}
		// Physical column offsets are used below; reject column reordering
		// rather than interpreting unrelated fields as observed metrics.
		want := []string{"sum_open_interest", "sum_open_interest_value", "count_toptrader_long_short_ratio", "sum_toptrader_long_short_ratio", "count_long_short_ratio", "sum_taker_long_short_vol_ratio"}
		for i, col := range want {
			if strings.TrimSpace(header[i+2]) != col {
				_ = data.Close()
				_ = z.Close()
				return out, fmt.Errorf("%s: Metrics header col=%d got=%q want=%q", p, i+2, header[i+2], col)
			}
		}
		for {
			x, e := reader.Read()
			if errors.Is(e, io.EOF) {
				break
			}
			if e != nil {
				_ = data.Close()
				_ = z.Close()
				return out, fmt.Errorf("%s CSV: %w", p, e)
			}
			if len(x) < 8 {
				_ = data.Close()
				_ = z.Close()
				return out, fmt.Errorf("%s: short CSV row", p)
			}
			tt, e := time.ParseInLocation("2006-01-02 15:04:05", strings.TrimSuffix(x[0], ".000"), time.UTC)
			if e != nil {
				_ = data.Close()
				_ = z.Close()
				return out, fmt.Errorf("%s timestamp %q: %w", p, x[0], e)
			}
			t := tt.UnixMilli()
			if t%intervalMs != 0 || t < date(2024, 1, 1) || t >= date(2025, 1, 1) {
				_ = data.Close()
				_ = z.Close()
				return out, fmt.Errorf("%s: timestamp outside 2024 grid %d", p, t)
			}
			if seen[t] {
				_ = data.Close()
				_ = z.Close()
				return out, fmt.Errorf("%s: duplicate %d", p, t)
			}
			seen[t] = true
			var present uint8
			for col := 2; col <= 7; col++ {
				v := strings.TrimSpace(x[col])
				if v == "" {
					field := "taker_long_short_volume_ratio"
					if col <= 6 {
						field = fields[col-2]
					}
					out.missingCounts[field]++
					if len(out.missingExamples) < 30 {
						out.missingExamples = append(out.missingExamples, missingExample{name, t, field})
					}
					continue
				}
				// Only legitimately empty numeric cells are treated as missing.
				// A malformed/non-finite nonempty value is a source-integrity error.
				f, parseErr := strconv.ParseFloat(v, 64)
				if parseErr != nil || math.IsNaN(f) || math.IsInf(f, 0) {
					_ = data.Close()
					_ = z.Close()
					return out, fmt.Errorf("%s: invalid nonempty Metrics column %d at %d (%q)", p, col, t, v)
				}
				present |= 1 << uint(col-2)
			}
			if present != fullMask {
				out.incompleteRows++
			}
			out.rows = append(out.rows, sourceRow{timestamp: t, present: present})
		}
		if e := data.Close(); e != nil {
			_ = z.Close()
			return out, e
		}
		if e := z.Close(); e != nil {
			return out, e
		}
	}
	if strict && len(out.missingArchives) > 0 {
		return out, fmt.Errorf("%d archive(s) missing; refusing partial full-year coverage, first=%s", len(out.missingArchives), out.missingArchives[0])
	}
	sort.Slice(out.rows, func(i, j int) bool { return out.rows[i].timestamp < out.rows[j].timestamp })
	return out, nil
}

// Build field-wise observations with normalized interval-end key, not raw T.
// group availability is a hypothetical end+5s+extra for all six fields.
func buildGroups(rows []sourceRow, startMask uint8) []grouped {
	byEnd := map[int64]uint8{}
	for _, row := range rows {
		t := row.timestamp
		for i := 0; i < 5; i++ {
			if row.present&(1<<uint(i)) == 0 {
				continue
			}
			end := t
			if startMask&(1<<i) != 0 {
				end += intervalMs
			}
			byEnd[end] |= (1 << uint(i))
		}
		if row.present&(1<<5) != 0 {
			end := t
			if t >= changeMs {
				end += intervalMs
			}
			byEnd[end] |= (1 << 5)
		}
	}
	groups := make([]grouped, 0, len(byEnd))
	for end, mask := range byEnd {
		groups = append(groups, grouped{end: end, mask: mask})
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].end < groups[j].end })
	return groups
}
func scan(groups []grouped, p partition, extra int64) summary {
	ready := make([]grouped, 0, len(groups))
	r := summary{ExtraDelayMs: extra, GridDecisions: (p.end - p.start) / 5000}
	for _, g := range groups {
		if g.end >= p.start && g.end < p.end {
			if g.mask == fullMask {
				r.CompleteNormalizedGroups++
			} else {
				r.PartialNormalizedGroups++
			}
		}
		if g.mask == fullMask {
			g.usable = g.end + safetyLagMs + extra
			ready = append(ready, g)
		}
	}
	// Strictly as-of lookup with four monotonic cursors; counter does not claim
	// other Spot/Kline/Funding/Feature conditions are satisfied.
	shifts := [4]int64{0, intervalMs, 3 * intervalMs, 12 * intervalMs}
	idx := [4]int{-1, -1, -1, -1}
	for d := p.start; d < p.end; d += 5000 {
		flags := [4]bool{}
		for k, shift := range shifts {
			q := d - shift
			for idx[k]+1 < len(ready) && ready[idx[k]+1].usable <= q {
				idx[k]++
			}
			if idx[k] >= 0 {
				g := ready[idx[k]]
				if g.end > q {
					r.FutureSelections++
					continue
				}
				if q-g.end <= freshAgeMs {
					flags[k] = true
				}
			}
		}
		if flags[0] {
			r.MetricsCurrentUsable++
		} else if idx[0] < 0 {
			r.UnavailableCurrent++
		} else {
			r.StaleCurrent++
		}
		if flags[0] && flags[1] && flags[2] && flags[3] {
			r.MetricsFourLookbacksReady++
		}
	}
	return r
}
