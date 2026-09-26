// metricsalignmentaudit is a read-only diagnostic: it never changes frozen
// features, policies, datasets or order-execution settings.
package main

import (
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

	"binance_trader/internal/market"
	"github.com/parquet-go/parquet-go"
)

const interval = int64(300_000)

// Match the production canonical metrics schema; do not reinterpret its timestamp.
type metricsRow struct {
	Timestamp              int64  `parquet:"timestamp"`
	OpenInterest           string `parquet:"open_interest"`
	OpenInterestValue      string `parquet:"open_interest_value"`
	TopTraderAccountRatio  string `parquet:"top_trader_account_long_short_ratio"`
	TopTraderPositionRatio string `parquet:"top_trader_position_long_short_ratio"`
	GlobalRatio            string `parquet:"global_long_short_ratio"`
	TakerRatio             string `parquet:"taker_long_short_volume_ratio"`
}

type volumeWindow struct {
	BuyBase, SellBase, BuyQuote, SellQuote float64
	Bars, TradeSeconds                     int
}

type observation struct {
	Day                        string  `json:"day"`
	HistoricalTimestampMs      int64   `json:"historical_timestamp_ms"`
	HistoricalRatio            float64 `json:"historical_ratio"`
	Candidate                  string  `json:"candidate"`
	VolumeUnit                 string  `json:"volume_unit"`
	WindowStartMs, WindowEndMs int64
	CalculatedRatio            float64 `json:"calculated_ratio"`
	AbsoluteError              float64 `json:"absolute_error"`
	RelativeError              float64 `json:"relative_error"`
	TotalVolume                float64 `json:"total_volume"`
}

type ignoredCounts struct {
	NotSampleDay           int      `json:"not_sample_day"`
	InvalidHistoricalRatio int      `json:"invalid_historical_ratio"`
	IncompleteWindow       int      `json:"incomplete_300_second_window"`
	NoSellVolume           int      `json:"zero_sell_volume"`
	MissingParquet         []string `json:"missing_parquet"`
}

type score struct {
	Candidate                   string   `json:"candidate"`
	VolumeUnit                  string   `json:"volume_unit"`
	N                           int      `json:"n"`
	MedianAbsoluteError         float64  `json:"median_absolute_error"`
	P90AbsoluteError            float64  `json:"p90_absolute_error"`
	MeanAbsoluteError           float64  `json:"mean_absolute_error"`
	FractionAbsErrorLE0001      float64  `json:"fraction_abs_error_le_0_001"`
	FractionRelativeErrorLE1Pct float64  `json:"fraction_relative_error_le_1pct"`
	PearsonCorrelation          *float64 `json:"pearson_correlation,omitempty"`
	Days                        int      `json:"days"`
}

type report struct {
	Status               string        `json:"status"`
	Scope                string        `json:"scope"`
	Notice               string        `json:"notice"`
	FrozenFilesModified  bool          `json:"frozen_files_modified"`
	SampleDays           []string      `json:"sample_days"`
	RawHistoricalMetrics int           `json:"raw_historical_metrics_rows_on_sample_days"`
	Ignored              ignoredCounts `json:"ignored"`
	Scores               []score       `json:"scores"`
	GeneratedAt          string        `json:"generated_at_utc"`
}

func main() {
	daysFlag := flag.String("days", "2024-01-15,2024-04-15,2024-07-15,2024-09-15,2024-10-15,2024-11-15,2024-12-15", "comma-separated TRAIN/VALIDATION dates (2024 only)")
	barsRoot := flag.String("bars-root", "data/derived/1s", "derived Futures 1-second parquet root")
	metricsRoot := flag.String("metrics-root", "data/external/metrics/v1/BTCUSDT", "canonical historical combined metrics parquet root")
	output := flag.String("out", "", "new report directory (auto if empty)")
	flag.Parse()
	dates, err := parseDates(*daysFlag)
	if err != nil {
		fatal(err)
	}
	if *output == "" {
		*output = filepath.Join("data", "reports", "diagnostics", "metrics-taker-alignment", time.Now().UTC().Format("20060102T150405Z"))
	}
	if _, err := os.Stat(*output); err == nil {
		fatal(fmt.Errorf("output already exists: %s", *output))
	} else if !os.IsNotExist(err) {
		fatal(err)
	}
	all := make([]observation, 0, len(dates)*280*6)
	rep := report{Status: "DIAGNOSTIC_ONLY", Scope: "TRAIN_VALIDATION_2024_ONLY", Notice: "Independent empirical alignment check; NEVER auto-realign frozen models or change live code from this report alone.", SampleDays: make([]string, 0, len(dates)), GeneratedAt: time.Now().UTC().Format(time.RFC3339)}
	for _, d := range dates {
		day := d.Format("2006-01-02")
		rep.SampleDays = append(rep.SampleDays, day)
		month := d.Format("2006-01")
		mpath := filepath.Join(*metricsRoot, fmt.Sprintf("BTCUSDT-metrics-%s.parquet", month))
		bpath := filepath.Join(*barsRoot, "BTCUSDT", "2024", fmt.Sprintf("BTCUSDT-1s-%s.parquet", month))
		if missing, err := unavailable(mpath, bpath); err != nil {
			fatal(err)
		} else if len(missing) > 0 {
			rep.Ignored.MissingParquet = append(rep.Ignored.MissingParquet, missing...)
			continue
		}
		rows, err := readHistoricalMetrics(mpath, d)
		if err != nil {
			fatal(fmt.Errorf("%s metrics: %w", day, err))
		}
		windows, err := readOneDayBars(bpath, d)
		if err != nil {
			fatal(fmt.Errorf("%s bars: %w", day, err))
		}
		rep.RawHistoricalMetrics += len(rows)
		dayValid := 0
		for _, row := range rows {
			target, err := strconv.ParseFloat(strings.TrimSpace(row.TakerRatio), 64)
			if err != nil || !finite(target) || target <= 0 {
				rep.Ignored.InvalidHistoricalRatio++
				continue
			}
			// END: CSV time T is the closing boundary [T-5m,T).
			// START: CSV time T is the opening boundary [T,T+5m).
			// PREV_END: explicit negative-control alignment [T-10m,T-5m).
			for _, candidate := range candidatesFor(row.Timestamp) {
				w, ok := windows[candidate.start]
				if !ok || w.Bars != 300 {
					rep.Ignored.IncompleteWindow++
					continue
				}
				for _, unit := range []string{"BASE", "QUOTE"} {
					buy, sell := w.BuyBase, w.SellBase
					if unit == "QUOTE" {
						buy, sell = w.BuyQuote, w.SellQuote
					}
					if sell <= 0 || !finite(buy) || !finite(sell) {
						rep.Ignored.NoSellVolume++
						continue
					}
					calc := buy / sell
					abs := math.Abs(target - calc)
					all = append(all, observation{Day: day, HistoricalTimestampMs: row.Timestamp, HistoricalRatio: target, Candidate: candidate.name, VolumeUnit: unit, WindowStartMs: candidate.start, WindowEndMs: candidate.start + interval, CalculatedRatio: calc, AbsoluteError: abs, RelativeError: abs / math.Max(math.Abs(target), 1e-12), TotalVolume: buy + sell})
					dayValid++
				}
			}
		}
		fmt.Printf("day=%s metrics=%d complete_windows=%d comparisons=%d\n", day, len(rows), len(windows), dayValid)
	}
	if len(all) == 0 {
		fatal(fmt.Errorf("no complete comparable windows; missing=%v (no reports published)", rep.Ignored.MissingParquet))
	}
	for _, unit := range []string{"BASE", "QUOTE"} {
		for _, candidate := range []string{"END", "START", "PREV_END"} {
			var group []observation
			for _, o := range all {
				if o.Candidate == candidate && o.VolumeUnit == unit {
					group = append(group, o)
				}
			}
			rep.Scores = append(rep.Scores, scoreGroup(group, candidate, unit))
		}
	}
	// Keep a diagnostic status. A numerical correlation by itself never proves
	// which timestamp the original vendor CSV used.
	if err := os.MkdirAll(*output, 0o755); err != nil {
		fatal(err)
	}
	if err := writeCSV(filepath.Join(*output, "window_comparisons.csv"), all); err != nil {
		fatal(err)
	}
	b, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		fatal(err)
	}
	b = append(b, '\n')
	if err := os.WriteFile(filepath.Join(*output, "summary.json"), b, 0o644); err != nil {
		fatal(err)
	}
	fmt.Printf("REPORT=%s rows=%d status=%s\n", *output, len(all), rep.Status)
	for _, s := range rep.Scores {
		fmt.Printf("%s %-8s n=%d median_abs=%.8f p90_abs=%.8f corr=%v\n", s.VolumeUnit, s.Candidate, s.N, s.MedianAbsoluteError, s.P90AbsoluteError, formatCorr(s.PearsonCorrelation))
	}
}

type candidateWindow struct {
	name  string
	start int64
}

func candidatesFor(timestamp int64) []candidateWindow {
	return []candidateWindow{{"END", timestamp - interval}, {"START", timestamp}, {"PREV_END", timestamp - 2*interval}}
}

func fatal(err error)       { fmt.Fprintln(os.Stderr, "AUDIT ERROR:", err); os.Exit(1) }
func finite(x float64) bool { return !math.IsNaN(x) && !math.IsInf(x, 0) }
func parseDates(s string) ([]time.Time, error) {
	seen := map[string]bool{}
	out := []time.Time{}
	for _, v := range strings.Split(s, ",") {
		v = strings.TrimSpace(v)
		t, err := time.Parse("2006-01-02", v)
		if err != nil || t.Year() != 2024 {
			return nil, fmt.Errorf("only 2024 TRAIN/VALIDATION sample days permitted, got %q", v)
		}
		if !seen[v] {
			out = append(out, t)
			seen[v] = true
		}
	}
	if len(out) == 0 {
		return nil, errors.New("no sample dates")
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Before(out[j]) })
	return out, nil
}
func unavailable(paths ...string) ([]string, error) {
	var missing []string
	for _, path := range paths {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			missing = append(missing, path)
		} else if err != nil {
			return nil, err
		}
	}
	return missing, nil
}
func readHistoricalMetrics(path string, day time.Time) ([]metricsRow, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	reader := parquet.NewGenericReader[metricsRow](f)
	defer reader.Close()
	start, end := day.UnixMilli(), day.Add(24*time.Hour).UnixMilli()
	out := []metricsRow{}
	buf := make([]metricsRow, 1024)
	for {
		n, readErr := reader.Read(buf)
		for _, row := range buf[:n] {
			if row.Timestamp >= start && row.Timestamp < end {
				out = append(out, row)
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, readErr
		}
	}
	return out, nil
}
func readOneDayBars(path string, day time.Time) (map[int64]volumeWindow, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	reader := parquet.NewGenericReader[market.SecondBar](f)
	defer reader.Close()
	start, end := day.UnixMilli(), day.Add(24*time.Hour).UnixMilli()
	result := map[int64]volumeWindow{}
	last := int64(-1)
	buf := make([]market.SecondBar, 4096)
	done := false
	for !done {
		n, readErr := reader.Read(buf)
		for _, row := range buf[:n] {
			t := row.TimestampMs
			if t >= end {
				done = true
				break
			}
			if t < start {
				continue
			}
			if t%1000 != 0 || (last >= 0 && t <= last) {
				return nil, fmt.Errorf("invalid 1s order or off-grid at %d", t)
			}
			if !finite(row.TakerBuyBaseVolume) || !finite(row.TakerSellBaseVolume) || !finite(row.TakerBuyQuoteVolume) || !finite(row.TakerSellQuoteVolume) {
				return nil, fmt.Errorf("invalid volume at %d", t)
			}
			last = t
			key := t - (t % interval)
			w := result[key]
			w.BuyBase += row.TakerBuyBaseVolume
			w.SellBase += row.TakerSellBaseVolume
			w.BuyQuote += row.TakerBuyQuoteVolume
			w.SellQuote += row.TakerSellQuoteVolume
			if row.HasTrade {
				w.TradeSeconds++
			}
			w.Bars++
			result[key] = w
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, readErr
		}
	}
	return result, nil
}
func scoreGroup(o []observation, candidate, unit string) score {
	s := score{Candidate: candidate, VolumeUnit: unit, N: len(o)}
	if len(o) == 0 {
		return s
	}
	errAbs := make([]float64, len(o))
	days := map[string]bool{}
	var sum, absMatch, relMatch, sx, sy, sxx, syy, sxy float64
	for i, x := range o {
		errAbs[i] = x.AbsoluteError
		days[x.Day] = true
		sum += x.AbsoluteError
		if x.AbsoluteError <= 0.001 {
			absMatch++
		}
		if x.RelativeError <= 0.01 {
			relMatch++
		}
		sx += x.HistoricalRatio
		sy += x.CalculatedRatio
		sxx += x.HistoricalRatio * x.HistoricalRatio
		syy += x.CalculatedRatio * x.CalculatedRatio
		sxy += x.HistoricalRatio * x.CalculatedRatio
	}
	sort.Float64s(errAbs)
	s.MedianAbsoluteError = quantile(errAbs, 0.5)
	s.P90AbsoluteError = quantile(errAbs, 0.9)
	s.MeanAbsoluteError = sum / float64(len(o))
	s.FractionAbsErrorLE0001 = absMatch / float64(len(o))
	s.FractionRelativeErrorLE1Pct = relMatch / float64(len(o))
	s.Days = len(days)
	n := float64(len(o))
	den := (n*sxx - sx*sx) * (n*syy - sy*sy)
	if den > 1e-20 {
		c := (n*sxy - sx*sy) / math.Sqrt(den)
		if finite(c) {
			s.PearsonCorrelation = &c
		}
	}
	return s
}
func quantile(x []float64, p float64) float64 {
	if len(x) == 0 {
		return 0
	}
	pos := p * float64(len(x)-1)
	lo := int(pos)
	hi := int(math.Ceil(pos))
	return x[lo] + (x[hi]-x[lo])*(pos-float64(lo))
}
func formatCorr(c *float64) string {
	if c == nil {
		return "null"
	}
	return fmt.Sprintf("%.6f", *c)
}
func writeCSV(path string, rows []observation) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	err = w.Write([]string{"day", "historical_timestamp_ms", "historical_ratio", "candidate", "volume_unit", "window_start_ms", "window_end_ms", "calculated_ratio", "absolute_error", "relative_error", "total_volume"})
	if err == nil {
		for _, o := range rows {
			err = w.Write([]string{o.Day, strconv.FormatInt(o.HistoricalTimestampMs, 10), strconv.FormatFloat(o.HistoricalRatio, 'g', 17, 64), o.Candidate, o.VolumeUnit, strconv.FormatInt(o.WindowStartMs, 10), strconv.FormatInt(o.WindowEndMs, 10), strconv.FormatFloat(o.CalculatedRatio, 'g', 17, 64), strconv.FormatFloat(o.AbsoluteError, 'g', 17, 64), strconv.FormatFloat(o.RelativeError, 'g', 17, 64), strconv.FormatFloat(o.TotalVolume, 'g', 17, 64)})
			if err != nil {
				break
			}
		}
	}
	w.Flush()
	if err == nil {
		err = w.Error()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	return err
}
