// frozenmetricsimpactaudit measures label-free inference sensitivity of frozen
// models on 2024 VALIDATION feature rows. It never writes or deploys a model.
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
	"sort"
	"strconv"
	"strings"
	"time"

	metricsv2 "binance_trader/internal/external/metricsv2"
	featurev2 "binance_trader/internal/feature/main/v2"
	"github.com/parquet-go/parquet-go"
)

const frozenExpectedHash = "a37306b80ecbf701103ab691445d07a39624ce995d84f8206d6314dae3045bef"
const modelRoot = "models/main/v2/phase9c-cde"
const metricsRoot = "data/external/metrics/v2/BTCUSDT"
const featuresRoot = "data/features/main/v2/BTCUSDT/2024"

var modelIDs = [...]string{
	"tp100_sl100_h14400", "tp75_sl50_h14400", "tp50_sl50_h14400", "tp50_sl25_h900", "tp75_sl25_h900",
}
var extraDelays = [...]int64{0, 180_000, 300_000}

type artifact struct {
	Family                    string                        `json:"family"`
	FeatureCount              int                           `json:"feature_count"`
	FeatureRegistryHash       string                        `json:"feature_registry_hash"`
	Scaler                    struct{ Mean, Std []float64 } `json:"scaler"`
	ClassificationIntercept   float64                       `json:"classification_intercept"`
	ClassificationWeights     []float64                     `json:"classification_weights"`
	RegressionInterceptScaled float64                       `json:"regression_intercept_scaled"`
	RegressionWeightsScaled   []float64                     `json:"regression_weights_scaled"`
	ReturnScale               float64                       `json:"return_scale"`
}

type scenarioAgg struct {
	Count        int64
	NotReady     int64
	ProbDelta    []float64
	ReturnDelta  []float64
	BaselineProb []float64
	ChangedProb  []float64
	CrossesHalf  int64
}

func (a *scenarioAgg) add(p pair) {
	d := math.Abs(p.Base - p.Corrected)
	a.ProbDelta = append(a.ProbDelta, d)
	a.ReturnDelta = append(a.ReturnDelta, math.Abs(p.BaseReturn-p.CorrectedReturn))
	a.BaselineProb = append(a.BaselineProb, p.Base)
	a.ChangedProb = append(a.ChangedProb, p.Corrected)
	if (p.Base >= .5) != (p.Corrected >= .5) {
		a.CrossesHalf++
	}
}
func avg(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	x := 0.0
	for _, v := range values {
		x += v
	}
	return x / float64(len(values))
}

type scenarioResult struct {
	DelayMs                           int64   `json:"extra_delay_ms"`
	FrozenSampledRows                 int64   `json:"frozen_legacy_sampled_rows"`
	CorrectedTakerReady               int64   `json:"corrected_taker_ready_rows"`
	MissingOrStale                    int64   `json:"corrected_taker_unavailable_or_stale_rows"`
	MeanAbsoluteProbabilityChange     float64 `json:"mean_absolute_probability_change"`
	P90AbsoluteProbabilityChange      float64 `json:"p90_absolute_probability_change"`
	FractionThresholdHalfFlipped      float64 `json:"fraction_probability_half_flipped"`
	TopFivePercentOverlap             float64 `json:"top_five_percent_overlap"`
	MeanAbsolutePredictedReturnChange float64 `json:"mean_absolute_predicted_return_score_change"`
}

type modelResult struct {
	Model                                                                   string           `json:"model"`
	SampledRows                                                             int64            `json:"sampled_rows"`
	MeanAbsoluteProbabilityChangeFromRemovingUnknownMetricsContribution     float64          `json:"unknown_metrics_contribution_removed_mean_abs_probability_change"`
	P90AbsoluteProbabilityChangeFromRemovingUnknownMetricsContribution      float64          `json:"unknown_metrics_contribution_removed_p90_abs_probability_change"`
	TopFivePercentOverlapUnknownMetricsContributionRemoved                  float64          `json:"unknown_metrics_contribution_removed_top_five_percent_overlap"`
	MeanAbsolutePredictedReturnChangeFromRemovingUnknownMetricsContribution float64          `json:"unknown_metrics_contribution_removed_mean_abs_predicted_return_score_change"`
	Scenarios                                                               []scenarioResult `json:"scenarios"`
}

type tracker struct {
	model              linearModel
	scenarios          map[int64]*scenarioAgg
	baseline           []float64
	unknownAblated     []float64
	unknownProbDelta   []float64
	unknownReturnDelta []float64
}

type output struct {
	Status                  string        `json:"status"`
	SourceScope             string        `json:"source_scope"`
	FrozenHash              string        `json:"frozen_feature_registry_hash"`
	Notes                   []string      `json:"interpretation_constraints"`
	SampleGranularity       string        `json:"sample_granularity"`
	Months                  []string      `json:"months"`
	TotalFeatureRowsScanned int64         `json:"total_legacy_validation_feature_rows_scanned"`
	ValidationRowsSampled   int64         `json:"validation_rows_sampled_one_per_minute"`
	CorrectedTakerPoints    int           `json:"corrected_taker_source_points"`
	Models                  []modelResult `json:"models"`
	NoTestRead              bool          `json:"test_not_accessed"`
	NoHoldoutRead           bool          `json:"final_holdout_not_accessed"`
	Orders                  int           `json:"orders_sent"`
	Retrained               bool          `json:"retrained"`
}

func loadArtifact(path, id, expectedHash string) (linearModel, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return linearModel{}, err
	}
	var a artifact
	if err = json.Unmarshal(raw, &a); err != nil {
		return linearModel{}, err
	}
	if a.FeatureCount != 128 || a.FeatureRegistryHash != expectedHash || a.Family != "LOGISTIC_LINEAR" {
		return linearModel{}, fmt.Errorf("incompatible frozen model %s: count/hash/family mismatch", id)
	}
	m := linearModel{Name: id, Mean: a.Scaler.Mean, Std: a.Scaler.Std, ClassWeights: a.ClassificationWeights,
		ReturnWeights: a.RegressionWeightsScaled, ClassIntercept: a.ClassificationIntercept,
		ReturnIntercept: a.RegressionInterceptScaled, ReturnScale: a.ReturnScale}
	return m, m.validate()
}
func readTaker(path string) ([]takerPoint, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := parquet.NewGenericReader[metricsv2.Observation](f)
	defer r.Close()
	result := make([]takerPoint, 0, 9000)
	buf := make([]metricsv2.Observation, 4096)
	for {
		n, e := r.Read(buf)
		for i := 0; i < n; i++ {
			x := buf[i]
			if x.Field != metricsv2.TakerRatio {
				continue
			}
			if !x.SemanticVerified || x.IntervalEndMs-x.IntervalStartMs != metricsv2.IntervalMs || x.EarliestAvailableMs != x.IntervalEndMs+metricsv2.SafetyLagMs {
				return nil, fmt.Errorf("invalid corrected taker metadata in %s at %d", path, x.SourceTimestampMs)
			}
			value, parseErr := strconv.ParseFloat(x.Value, 64)
			if parseErr != nil || math.IsNaN(value) || math.IsInf(value, 0) || value <= 0 {
				return nil, fmt.Errorf("invalid taker value in %s at %d", path, x.SourceTimestampMs)
			}
			result = append(result, takerPoint{EffectiveMs: x.EarliestAvailableMs, IntervalEndMs: x.IntervalEndMs, SourceTimestampMs: x.SourceTimestampMs, Value: value})
		}
		if errors.Is(e, io.EOF) {
			return result, nil
		}
		if e != nil {
			return nil, e
		}
	}
}

func appendTaker(pathList []string) ([]takerPoint, error) {
	points := make([]takerPoint, 0, 38000)
	for _, p := range pathList {
		a, err := readTaker(p)
		if err != nil {
			return nil, err
		}
		points = append(points, a...)
	}
	sort.Slice(points, func(i, j int) bool { return points[i].EffectiveMs < points[j].EffectiveMs })
	for i := 1; i < len(points); i++ {
		if points[i].EffectiveMs <= points[i-1].EffectiveMs {
			return nil, fmt.Errorf("corrected Taker availability key duplicates/reversed: %d", points[i].EffectiveMs)
		}
	}
	return points, nil
}

func main() {
	outPath := flag.String("out", "data/reports/diagnostics/frozen-metrics-impact/phase3b3-v1/result.json", "new output JSON (must not exist)")
	flag.Parse()
	if err := run(*outPath); err != nil {
		fmt.Fprintln(os.Stderr, "FROZEN_METRICS_AUDIT_FAIL:", err)
		os.Exit(1)
	}
	fmt.Println("FROZEN_METRICS_AUDIT_PASS:", *outPath)
}

func run(outPath string) error {
	if _, err := os.Stat(outPath); err == nil {
		return fmt.Errorf("refusing to overwrite %s", outPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	// No access paths outside 2024 VALIDATION and preceding corrected 2024-09 Taker source.
	names := featurev2.ModelFeatureColumnsV2
	if len(names) != featureCount {
		return fmt.Errorf("expected 128 features, got %d", len(names))
	}
	sum := sha256.Sum256([]byte(strings.Join(names, "\n")))
	hash := hex.EncodeToString(sum[:])
	if hash != frozenExpectedHash {
		return fmt.Errorf("frozen registry hash changed: %s", hash)
	}
	tr := make([]*tracker, 0, len(modelIDs))
	for _, id := range modelIDs {
		path := filepath.Join(modelRoot, id, "baseline.json")
		m, err := loadArtifact(path, id, hash)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		x := &tracker{model: m, scenarios: make(map[int64]*scenarioAgg)}
		for _, d := range extraDelays {
			x.scenarios[d] = &scenarioAgg{}
		}
		tr = append(tr, x)
	}
	metricPaths := []string{}
	for m := 9; m <= 12; m++ {
		metricPaths = append(metricPaths, filepath.Join(metricsRoot, fmt.Sprintf("BTCUSDT-metrics-v2-2024-%02d.parquet", m)))
	}
	points, err := appendTaker(metricPaths)
	if err != nil {
		return err
	}
	rep := output{Status: "LABEL_FREE_FROZEN_MODEL_SCORE_SENSITIVITY_ONLY", SourceScope: "2024_VALIDATION_OCT_DEC_ONLY",
		FrozenHash: hash, SampleGranularity: "first existing frozen feature row in each UTC minute", Months: []string{"2024-10", "2024-11", "2024-12"}, CorrectedTakerPoints: len(points), NoTestRead: true, NoHoldoutRead: true,
		Notes: []string{"Ablation of 16 unverified Metrics contributions is a counterfactual fixed-model stress test, not a trained 110/112 feature model.",
			"Corrected Taker 2-feature comparison leaves 16 unverified Metrics and all other legacy feature values untouched: it cannot certify leak-free predictions.",
			"Historical as-of uses assumed interval_end+5s+extra_delay; actual 2024 publication time is unknown.",
			"No labels, strategy thresholds, policy selections, prediction correctness or profitability are tested; 0.5 and top 5% are diagnostic probes only.",
			"Do not use this diagnostic or frozen model to resume new Demo/Mainnet entries."}}
	for month := 10; month <= 12; month++ {
		p := filepath.Join(featuresRoot, fmt.Sprintf("BTCUSDT-main-features-v2-2024-%02d.parquet", month))
		if _, err := os.Stat(p); err != nil {
			return fmt.Errorf("required validation feature parquet missing %s: %w", p, err)
		}
		lastBucket := int64(-1)
		lastTS := int64(-1)
		scanned, e := featurev2.ReadRows(p, func(row featurev2.FeatureRowV2) error {
			ts := row.DecisionTimestampMs
			utc := time.UnixMilli(ts).UTC()
			if utc.Year() != 2024 || utc.Month() != time.Month(month) {
				return fmt.Errorf("unexpected non-validation timestamp in %s: %s", p, utc)
			}
			if ts <= lastTS {
				return fmt.Errorf("duplicate/reversed feature decision in %s: %d", p, ts)
			}
			lastTS = ts
			bucket := ts / 60_000
			if bucket == lastBucket {
				return nil
			}
			lastBucket = bucket
			values := row.FeatureValues()
			var corrected [len(extraDelays)]struct {
				level, change float64
				ok            bool
			}
			for i, d := range extraDelays { // translated availability is offset uniformly; never move interval end
				// Avoid mutating shared source points for each delay.
				cur, ok1 := selectOffsetPoint(points, ts, d)
				old, ok2 := selectOffsetPoint(points, ts-300_000, d)
				if ok1 && ok2 {
					ch := (cur.Value - old.Value) / old.Value
					if !math.IsNaN(ch) && !math.IsInf(ch, 0) {
						corrected[i] = struct {
							level, change float64
							ok            bool
						}{cur.Value, ch, true}
					}
				}
			}
			for _, x := range tr {
				// Static ablation diagnostic is independent of any Taker availability assumption.
				base, abl, ablRet, e := x.model.scores(values, values[114], values[115])
				if e != nil {
					return fmt.Errorf("%s ts=%d: %w", x.model.Name, ts, e)
				}
				x.baseline = append(x.baseline, base.Base)
				x.unknownAblated = append(x.unknownAblated, abl)
				x.unknownProbDelta = append(x.unknownProbDelta, math.Abs(abl-base.Base))
				x.unknownReturnDelta = append(x.unknownReturnDelta, math.Abs(ablRet-base.BaseReturn))
				for i, d := range extraDelays {
					a := x.scenarios[d]
					a.Count++
					if !corrected[i].ok {
						a.NotReady++
						continue
					}
					result, _, _, e := x.model.scores(values, corrected[i].level, corrected[i].change)
					if e != nil {
						return fmt.Errorf("%s corrected ts=%d: %w", x.model.Name, ts, e)
					}
					a.add(result)
				}
			}
			rep.ValidationRowsSampled++
			return nil
		})
		if e != nil {
			return fmt.Errorf("%s: %w", p, e)
		}
		rep.TotalFeatureRowsScanned += scanned
	}
	if rep.ValidationRowsSampled == 0 {
		return errors.New("no sampled VALIDATION rows")
	}
	for _, x := range tr {
		m := modelResult{Model: x.model.Name, SampledRows: int64(len(x.baseline)),
			MeanAbsoluteProbabilityChangeFromRemovingUnknownMetricsContribution:     avg(x.unknownProbDelta),
			P90AbsoluteProbabilityChangeFromRemovingUnknownMetricsContribution:      percentileSorted(x.unknownProbDelta, .9),
			TopFivePercentOverlapUnknownMetricsContributionRemoved:                  topOverlap(x.baseline, x.unknownAblated, .05),
			MeanAbsolutePredictedReturnChangeFromRemovingUnknownMetricsContribution: avg(x.unknownReturnDelta)}
		for _, d := range extraDelays {
			a := x.scenarios[d]
			s := scenarioResult{DelayMs: d, FrozenSampledRows: a.Count, CorrectedTakerReady: int64(len(a.ProbDelta)), MissingOrStale: a.NotReady,
				MeanAbsoluteProbabilityChange: avg(a.ProbDelta), P90AbsoluteProbabilityChange: percentileSorted(a.ProbDelta, .9),
				MeanAbsolutePredictedReturnChange: avg(a.ReturnDelta)}
			if len(a.ProbDelta) > 0 {
				s.FractionThresholdHalfFlipped = float64(a.CrossesHalf) / float64(len(a.ProbDelta))
				s.TopFivePercentOverlap = topOverlap(a.BaselineProb, a.ChangedProb, .05)
			}
			m.Scenarios = append(m.Scenarios, s)
		}
		rep.Models = append(rep.Models, m)
	}
	// JSON and parent directory are the ONLY intended writes.
	if err := os.MkdirAll(filepath.Dir(outPath), 0755); err != nil {
		return err
	}
	f, err := os.OpenFile(outPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err = enc.Encode(rep); err != nil {
		f.Close()
		os.Remove(outPath)
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return nil
}
