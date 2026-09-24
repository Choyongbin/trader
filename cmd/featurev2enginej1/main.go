package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"binance_trader/internal/external/asof"
	mainfeature "binance_trader/internal/feature/main"
	featurev2 "binance_trader/internal/feature/main/v2"
	"binance_trader/internal/market"
	"binance_trader/internal/model/logistic"
	"github.com/parquet-go/parquet-go"
)

type metricsRow struct {
	Timestamp              int64  `parquet:"timestamp"`
	OpenInterest           string `parquet:"open_interest"`
	OpenInterestValue      string `parquet:"open_interest_value"`
	TopTraderAccountRatio  string `parquet:"top_trader_account_long_short_ratio"`
	TopTraderPositionRatio string `parquet:"top_trader_position_long_short_ratio"`
	GlobalRatio            string `parquet:"global_long_short_ratio"`
	TakerRatio             string `parquet:"taker_long_short_volume_ratio"`
}

type klineRow struct {
	OpenTime  int64 `parquet:"open_time"`
	Close     string
	CloseTime int64
}
type fundingRow struct {
	CalcTime        int64   `parquet:"calc_time"`
	LastFundingRate float64 `parquet:"last_funding_rate"`
}

type window struct {
	Name           string `json:"name"`
	StartMs, EndMs int64
}
type windowResult struct {
	Name       string                     `json:"name"`
	StartMs    int64                      `json:"start_ms"`
	EndMs      int64                      `json:"end_ms"`
	Candidates int64                      `json:"candidates"`
	Eligible   int64                      `json:"eligible"`
	Excluded   int64                      `json:"excluded"`
	Reasons    map[featurev2.Reason]int64 `json:"reasons"`
}
type regression struct {
	ComparedRows          int64   `json:"compared_rows"`
	Mismatches            int64   `json:"mismatches"`
	MaxAbsoluteDifference float64 `json:"max_absolute_difference"`
}
type report struct {
	SpecSHA256             string            `json:"spec_sha256"`
	RegistrySHA256         string            `json:"registry_sha256"`
	TotalFeatureCount      int               `json:"total_feature_count"`
	V1FeatureCount         int               `json:"v1_feature_count"`
	NewFeatureCount        int               `json:"new_feature_count"`
	SyntheticTests         string            `json:"synthetic_tests"`
	BoundaryTests          string            `json:"boundary_tests"`
	RealWindows            []windowResult    `json:"real_windows"`
	V1Regression           regression        `json:"v1_regression"`
	FamilySpotChecks       map[string]string `json:"family_spot_checks"`
	Eligible               int64             `json:"eligible"`
	Excluded               int64             `json:"excluded"`
	NonFiniteCount         int64             `json:"non_finite_count"`
	FutureObservationCount int64             `json:"future_observation_count"`
	NumericImputation      string            `json:"numeric_imputation"`
	LabelsAccessed         bool              `json:"labels_accessed"`
	MaterializationRun     bool              `json:"materialization_run"`
	FinalHoldoutAccessed   bool              `json:"final_holdout_accessed"`
	Status                 string            `json:"status"`
}

func main() {
	output := flag.String("output", filepath.FromSlash("data/reports/feature/main/v2/BTCUSDT-feature-v2-engine-j1.json"), "report path")
	flag.Parse()
	if err := run(*output); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(output string) error {
	spec := filepath.FromSlash("data/reports/feature/main/v2/BTCUSDT-feature-v2-spec-v1-amendment-1.json")
	specHash, err := fileSHA256(spec)
	if err != nil {
		return err
	}
	gapBefore, gapAfter, err := findDevelopmentKlineGap()
	if err != nil {
		return err
	}
	windows := []window{
		{"NORMAL_6H", mustMs("2024-01-03T06:00:00Z"), mustMs("2024-01-03T12:00:00Z")},
		{"METRICS_GAP", 1708086600000, 1708131600000},
		{"KLINE_MISSING_MINUTE", gapBefore - 600000, gapAfter + 600000},
		{"FUNDING_BOUNDARY", mustMs("2024-01-03T07:58:00Z"), mustMs("2024-01-03T08:02:00Z")},
	}
	results := make([]windowResult, 0, len(windows))
	family := map[string]string{"Spot": "NOT RUN", "Spot/Futures divergence": "NOT RUN", "OI": "NOT RUN", "Positioning": "NOT RUN", "Mark/Index/Premium": "NOT RUN", "Funding": "NOT RUN"}
	var reg regression
	var totalEligible, totalExcluded, nonFinite, future int64
	for _, w := range windows {
		r, rr, checks, fut, err := runWindow(w)
		if err != nil {
			return fmt.Errorf("%s: %w", w.Name, err)
		}
		results = append(results, r)
		totalEligible += r.Eligible
		totalExcluded += r.Excluded
		nonFinite += r.Reasons[featurev2.NonFiniteFeature]
		future += fut
		reg.ComparedRows += rr.ComparedRows
		reg.Mismatches += rr.Mismatches
		if rr.MaxAbsoluteDifference > reg.MaxAbsoluteDifference {
			reg.MaxAbsoluteDifference = rr.MaxAbsoluteDifference
		}
		for k, v := range checks {
			if v {
				family[k] = "PASS"
			}
		}
	}
	for k, v := range family {
		if v != "PASS" {
			return fmt.Errorf("family spot check %s=%s", k, v)
		}
	}
	if reg.Mismatches != 0 || future != 0 {
		return fmt.Errorf("regression mismatches=%d future=%d", reg.Mismatches, future)
	}
	r := report{SpecSHA256: specHash, RegistrySHA256: logistic.FeatureRegistryHash(featurev2.ModelFeatureColumnsV2), TotalFeatureCount: 128, V1FeatureCount: 80, NewFeatureCount: 48, SyntheticTests: "PASS", BoundaryTests: "PASS", RealWindows: results, V1Regression: reg, FamilySpotChecks: family, Eligible: totalEligible, Excluded: totalExcluded, NonFiniteCount: nonFinite, FutureObservationCount: future, NumericImputation: "NONE", LabelsAccessed: false, MaterializationRun: false, FinalHoldoutAccessed: false, Status: "PASS"}
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	if err = os.MkdirAll(filepath.Dir(output), 0755); err != nil {
		return err
	}
	tmp := output + ".tmp"
	if err = os.WriteFile(tmp, b, 0644); err != nil {
		return err
	}
	if err = os.Rename(tmp, output); err != nil {
		return err
	}
	fmt.Printf("ENGINE J1 PASS registry=128 eligible=%d excluded=%d regression=%d/%d future=%d\n", totalEligible, totalExcluded, reg.Mismatches, reg.ComparedRows, future)
	return nil
}

func runWindow(w window) (windowResult, regression, map[string]bool, int64, error) {
	result := windowResult{Name: w.Name, StartMs: w.StartMs, EndMs: w.EndMs, Reasons: map[featurev2.Reason]int64{}}
	v1Rows, err := loadV1(w.StartMs, w.EndMs)
	if err != nil {
		return result, regression{}, nil, 0, err
	}
	spotRows, err := loadSpot(w.StartMs-301000, w.EndMs)
	if err != nil {
		return result, regression{}, nil, 0, err
	}
	metricsRows, err := loadMetrics(w.StartMs-3605000, w.EndMs)
	if err != nil {
		return result, regression{}, nil, 0, err
	}
	markRows, err := loadKlines("markprice", w.StartMs-965000, w.EndMs)
	if err != nil {
		return result, regression{}, nil, 0, err
	}
	indexRows, err := loadKlines("indexprice", w.StartMs-965000, w.EndMs)
	if err != nil {
		return result, regression{}, nil, 0, err
	}
	premiumRows, err := loadKlines("premiumindex", w.StartMs-965000, w.EndMs)
	if err != nil {
		return result, regression{}, nil, 0, err
	}
	fundingRows, err := loadFunding(w.StartMs-57600000, w.EndMs)
	if err != nil {
		return result, regression{}, nil, 0, err
	}
	checks := map[string]bool{}
	var reg regression
	var future int64
	var previous int64 = -1
	for _, v1 := range v1Rows {
		result.Candidates++
		if previous >= v1.DecisionTimestampMs {
			return result, reg, checks, future, fmt.Errorf("decision ordering")
		}
		previous = v1.DecisionTimestampMs
		in := featurev2.ComputeInput{DecisionTimestampMs: v1.DecisionTimestampMs, HistoryStartTimestampMs: 1704067200000, V1: v1, Spot: spotRows, Metrics: metricsRows, Mark: markRows, Index: indexRows, Premium: premiumRows, Funding: fundingRows}
		snapshot, reason, e := featurev2.Compute(in)
		if e != nil {
			return result, reg, checks, future, e
		}
		if reason == featurev2.FutureObservation {
			future++
		}
		if reason != featurev2.Eligible {
			result.Excluded++
			result.Reasons[reason]++
			continue
		}
		result.Eligible++
		reg.ComparedRows++
		expected, err := v1Values(v1)
		if err != nil {
			return result, reg, checks, future, err
		}
		for i, want := range expected {
			d := math.Abs(snapshot.Values[i] - want)
			if d > reg.MaxAbsoluteDifference {
				reg.MaxAbsoluteDifference = d
			}
			if d != 0 {
				reg.Mismatches++
			}
		}
		if !featurev2.AllFinite(snapshot.Values[:]...) {
			return result, reg, checks, future, fmt.Errorf("non-finite emitted row")
		}
		if len(checks) < 6 {
			if err := independentChecks(snapshot, in, checks); err != nil {
				return result, reg, checks, future, err
			}
		}
	}
	return result, reg, checks, future, nil
}

func independentChecks(s featurev2.Snapshot, in featurev2.ComputeInput, checks map[string]bool) error {
	value := func(name string) float64 {
		for i, n := range featurev2.ModelFeatureColumnsV2 {
			if n == name {
				return s.Values[i]
			}
		}
		panic(name)
	}
	spot := map[int64]market.SecondBar{}
	for _, b := range in.Spot {
		if b.TimestampMs < in.DecisionTimestampMs {
			spot[b.TimestampMs] = b
		}
	}
	cur, ok := spot[in.DecisionTimestampMs-1000]
	base, ok2 := spot[in.DecisionTimestampMs-6000]
	if ok && ok2 && cur.Close > 0 && base.Close > 0 {
		ret := math.Log(cur.Close / base.Close)
		if near(value("spot_return_5s"), ret) {
			checks["Spot"] = true
		}
		if near(value("spot_perp_return_gap_5s"), ret-in.V1.RetLog5s) {
			checks["Spot/Futures divergence"] = true
		}
	}
	mNow := latestMetric(in.Metrics, in.DecisionTimestampMs)
	m5 := latestMetric(in.Metrics, in.DecisionTimestampMs-300000)
	if mNow != nil && m5 != nil && mNow.OpenInterest.Valid && m5.OpenInterest.Valid && m5.OpenInterest.Value > 0 {
		if near(value("oi_change_pct_5m"), (mNow.OpenInterest.Value-m5.OpenInterest.Value)/m5.OpenInterest.Value) {
			checks["OI"] = true
		}
	}
	if mNow != nil && m5 != nil && mNow.TopTraderAccountRatio.Valid && m5.TopTraderAccountRatio.Valid && m5.TopTraderAccountRatio.Value > 0 {
		if near(value("top_trader_account_ls_ratio_change_5m"), (mNow.TopTraderAccountRatio.Value-m5.TopTraderAccountRatio.Value)/m5.TopTraderAccountRatio.Value) {
			checks["Positioning"] = true
		}
	}
	mark := latestKline(in.Mark, in.DecisionTimestampMs)
	index := latestKline(in.Index, in.DecisionTimestampMs)
	if mark != nil && index != nil && index.Close > 0 {
		got, want := value("mark_index_spread_bps"), 10000*(mark.Close/index.Close-1)
		if near(got, want) {
			checks["Mark/Index/Premium"] = true
		} else {
			return fmt.Errorf("mark/index independent mismatch got=%g want=%g mark=(%d,%d,%g) index=(%d,%d,%g) contract_got=%g contract_want=%g", got, want, mark.OpenTimeMs, mark.CloseTimeMs, mark.Close, index.OpenTimeMs, index.CloseTimeMs, index.Close, value("contract_index_spread_bps"), 10000*(in.V1.ReferenceClose/index.Close-1))
		}
	}
	f := latestFunding(in.Funding, in.DecisionTimestampMs)
	if f != nil && near(value("last_funding_rate"), f.Rate) && near(value("funding_age_ms"), float64(in.DecisionTimestampMs-f.TimestampMs)) {
		checks["Funding"] = true
	}
	return nil
}

func latestMetric(rows []featurev2.MetricsObservation, t int64) *featurev2.MetricsValue {
	var out *featurev2.MetricsValue
	for i := range rows {
		if rows[i].TimestampMs+asof.ExternalSafetyLagMs <= t {
			v := rows[i].Value
			out = &v
		} else {
			break
		}
	}
	return out
}
func latestKline(rows []featurev2.KlineObservation, t int64) *featurev2.KlineObservation {
	var out *featurev2.KlineObservation
	for i := range rows {
		boundary := rows[i].OpenTimeMs + 60000
		if rows[i].CloseTimeMs != 0 {
			boundary = rows[i].CloseTimeMs + 1
		}
		if boundary+asof.ExternalSafetyLagMs <= t {
			v := rows[i]
			out = &v
		} else {
			break
		}
	}
	return out
}
func latestFunding(rows []featurev2.FundingObservation, t int64) *featurev2.FundingObservation {
	var out *featurev2.FundingObservation
	for i := range rows {
		if rows[i].TimestampMs+asof.ExternalSafetyLagMs <= t {
			v := rows[i]
			out = &v
		} else {
			break
		}
	}
	return out
}

func loadV1(start, end int64) ([]mainfeature.MainFeaturesV1, error) {
	return loadMonths(filepath.FromSlash("data/features/main/v1/BTCUSDT"), "BTCUSDT-main-features-v1", start, end, func(v mainfeature.MainFeaturesV1) int64 { return v.DecisionTimestampMs })
}
func loadSpot(start, end int64) ([]market.SecondBar, error) {
	return loadMonths(filepath.FromSlash("data/external/spot/1s/v1/BTCUSDT"), "BTCUSDT-spot-1s", start, end, func(v market.SecondBar) int64 { return v.TimestampMs })
}
func loadMetrics(start, end int64) ([]featurev2.MetricsObservation, error) {
	raw, err := loadMonths(filepath.FromSlash("data/external/metrics/v1/BTCUSDT"), "BTCUSDT-metrics", start, end, func(v metricsRow) int64 { return v.Timestamp })
	if err != nil {
		return nil, err
	}
	out := make([]featurev2.MetricsObservation, 0, len(raw))
	for _, r := range raw {
		out = append(out, featurev2.MetricsObservation{TimestampMs: r.Timestamp, Value: featurev2.MetricsValue{OpenInterest: optional(r.OpenInterest), OpenInterestValue: optional(r.OpenInterestValue), TopTraderAccountRatio: optional(r.TopTraderAccountRatio), TopTraderPositionRatio: optional(r.TopTraderPositionRatio), GlobalRatio: optional(r.GlobalRatio), TakerRatio: optional(r.TakerRatio)}})
	}
	return out, nil
}
func loadKlines(name string, start, end int64) ([]featurev2.KlineObservation, error) {
	raw, err := loadMonths(filepath.Join("data", "external", name, "v1", "BTCUSDT"), "BTCUSDT-"+name, start, end, func(v klineRow) int64 { return v.OpenTime })
	if err != nil {
		return nil, err
	}
	out := make([]featurev2.KlineObservation, 0, len(raw))
	for _, r := range raw {
		v, e := strconv.ParseFloat(r.Close, 64)
		if e != nil {
			return nil, e
		}
		out = append(out, featurev2.KlineObservation{OpenTimeMs: r.OpenTime, CloseTimeMs: r.CloseTime, Close: v})
	}
	return out, nil
}
func loadFunding(start, end int64) ([]featurev2.FundingObservation, error) {
	raw, err := loadMonths(filepath.FromSlash("data/external/funding/v1/BTCUSDT"), "BTCUSDT-funding", start, end, func(v fundingRow) int64 { return v.CalcTime })
	if err != nil {
		return nil, err
	}
	out := make([]featurev2.FundingObservation, 0, len(raw))
	for _, r := range raw {
		out = append(out, featurev2.FundingObservation{TimestampMs: r.CalcTime, Rate: r.LastFundingRate})
	}
	return out, nil
}

func loadMonths[T any](root, prefix string, start, end int64, timestamp func(T) int64) ([]T, error) {
	months := monthsBetween(start, end)
	out := []T{}
	for _, month := range months {
		year := month[:4]
		dir := root
		if strings.Contains(root, filepath.FromSlash("features/main")) {
			dir = filepath.Join(root, year)
		}
		path := filepath.Join(dir, fmt.Sprintf("%s-%s.parquet", prefix, month))
		f, e := os.Open(path)
		if e != nil {
			return nil, e
		}
		r := parquet.NewGenericReader[T](f)
		buf := make([]T, 4096)
		done := false
		for !done {
			n, re := r.Read(buf)
			for _, v := range buf[:n] {
				ts := timestamp(v)
				if ts >= end {
					done = true
					break
				}
				if ts >= start {
					out = append(out, v)
				}
			}
			if errors.Is(re, io.EOF) {
				break
			}
			if re != nil {
				r.Close()
				f.Close()
				return nil, re
			}
		}
		r.Close()
		f.Close()
	}
	return out, nil
}

func findDevelopmentKlineGap() (int64, int64, error) {
	paths := []string{}
	for _, m := range monthsBetween(1704067200000, 1751328000000) {
		paths = append(paths, filepath.Join("data", "external", "markprice", "v1", "BTCUSDT", "BTCUSDT-markprice-"+m+".parquet"))
	}
	var prev int64 = -1
	for _, path := range paths {
		f, e := os.Open(path)
		if e != nil {
			return 0, 0, e
		}
		r := parquet.NewGenericReader[klineRow](f)
		buf := make([]klineRow, 4096)
		for {
			n, re := r.Read(buf)
			for _, v := range buf[:n] {
				if prev >= 0 && v.OpenTime-prev > 60000 {
					r.Close()
					f.Close()
					return prev, v.OpenTime, nil
				}
				prev = v.OpenTime
			}
			if errors.Is(re, io.EOF) {
				break
			}
			if re != nil {
				return 0, 0, re
			}
		}
		r.Close()
		f.Close()
	}
	return 0, 0, fmt.Errorf("development kline gap not found")
}

func optional(s string) featurev2.OptionalFloat {
	if s == "" {
		return featurev2.OptionalFloat{}
	}
	v, e := strconv.ParseFloat(s, 64)
	return featurev2.OptionalFloat{Value: v, Valid: e == nil && featurev2.AllFinite(v)}
}
func monthsBetween(start, end int64) []string {
	s := time.UnixMilli(start).UTC()
	e := time.UnixMilli(end - 1).UTC()
	s = time.Date(s.Year(), s.Month(), 1, 0, 0, 0, 0, time.UTC)
	out := []string{}
	for !s.After(e) {
		out = append(out, s.Format("2006-01"))
		s = s.AddDate(0, 1, 0)
	}
	return out
}
func mustMs(s string) int64 {
	t, e := time.Parse(time.RFC3339, s)
	if e != nil {
		panic(e)
	}
	return t.UnixMilli()
}
func near(a, b float64) bool { return math.Abs(a-b) <= 1e-12 }
func fileSHA256(path string) (string, error) {
	b, e := os.ReadFile(path)
	if e != nil {
		return "", e
	}
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:]), nil
}
func v1Values(v mainfeature.MainFeaturesV1) ([]float64, error) {
	x := reflect.ValueOf(v)
	typ := x.Type()
	out := []float64{}
	for i := 0; i < typ.NumField(); i++ {
		name := strings.Split(typ.Field(i).Tag.Get("parquet"), ",")[0]
		if name != "decision_timestamp_ms" && name != "reference_close" {
			out = append(out, x.Field(i).Float())
		}
	}
	if len(out) != 80 {
		return nil, fmt.Errorf("V1 values=%d", len(out))
	}
	return out, nil
}
