package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"

	featurev2 "binance_trader/internal/feature/main/v2"
	tradespeclabel "binance_trader/internal/label/tradespec"
	"binance_trader/internal/model/logistic"
)

const alphaReportPath = "data/reports/model/main/v2/phase9c-a/BTCUSDT-feature-v2-alpha-viability.json"

type metricDelta struct {
	AUC     float64 `json:"roc_auc"`
	LogLoss float64 `json:"log_loss"`
	Brier   float64 `json:"brier"`
	AP      float64 `json:"average_precision"`
	MAE     float64 `json:"mae"`
	RMSE    float64 `json:"rmse"`
	Pearson float64 `json:"pearson_correlation"`
}

type metricComparison struct {
	Alpha20Classification classificationMetrics `json:"phase9c_a_20pct_classification"`
	FullClassification    classificationMetrics `json:"full_train_classification"`
	Alpha20Regression     regressionMetrics     `json:"phase9c_a_20pct_regression"`
	FullRegression        regressionMetrics     `json:"full_train_regression"`
	FullMinusAlpha20      metricDelta           `json:"full_minus_phase9c_a"`
}

type subsetReport struct {
	Name                      string     `json:"name"`
	Rows                      int64      `json:"rows"`
	MeanActualReturn          float64    `json:"mean_actual_net_return_ex_funding"`
	MedianActualReturn        float64    `json:"median_actual_net_return_ex_funding"`
	ClassificationPercentiles []bucket   `json:"classification_percentiles"`
	RegressionPercentiles     []bucket   `json:"regression_percentiles"`
	Combined                  []combined `json:"combined_intersections"`
}

type robustnessCandidate struct {
	CandidateID      string           `json:"candidate_id"`
	Side             string           `json:"side"`
	TPBps            int              `json:"tp_bps"`
	SLBps            int              `json:"sl_bps"`
	HorizonSeconds   int              `json:"horizon_seconds"`
	FullTrainFitRows int64            `json:"full_train_fit_rows"`
	ValidationRows   int64            `json:"validation_rows"`
	MetricComparison metricComparison `json:"metric_comparison"`
	Aggregate        subsetReport     `json:"aggregate_validation"`
	Monthly          []subsetReport   `json:"monthly_validation"`
	Thinning         []subsetReport   `json:"overlap_aware_thinning"`
	Verdict          string           `json:"robustness_verdict"`
}

type robustnessReport struct {
	Version              int                   `json:"version"`
	Symbol               string                `json:"symbol"`
	FeatureVersion       int                   `json:"feature_version"`
	FeatureCount         int                   `json:"feature_count"`
	FeatureRegistryHash  string                `json:"feature_registry_hash"`
	Phase9CAPath         string                `json:"phase9c_a_path"`
	Phase9CASHA256       string                `json:"phase9c_a_sha256"`
	TrainingSampling     string                `json:"training_sampling"`
	PercentilePolicy     string                `json:"percentile_policy"`
	ThinningAnchor       string                `json:"thinning_anchor"`
	Candidates           []robustnessCandidate `json:"candidates"`
	RobustCandidates     int                   `json:"robust_candidate_count"`
	UnstableCandidates   int                   `json:"unstable_candidate_count"`
	OverallVerdict       string                `json:"overall_verdict"`
	TestFeatureAccessed  bool                  `json:"test_feature_accessed"`
	TestLabelAccessed    bool                  `json:"test_label_accessed"`
	FinalHoldoutAccessed bool                  `json:"final_holdout_accessed"`
	NumericImputation    string                `json:"numeric_imputation"`
	ElapsedMs            int64                 `json:"elapsed_ms"`
	Complete             bool                  `json:"complete"`
}

func runRobustness(output string) error {
	started := time.Now()
	configs := candidates()
	if err := preflight(configs); err != nil {
		return err
	}
	alpha, alphaHash, err := readAlphaReport(filepath.FromSlash(alphaReportPath))
	if err != nil {
		return err
	}
	if !alpha.Complete || alpha.FeatureRegistryHash != registryHash || len(alpha.Candidates) != len(configs) {
		return fmt.Errorf("invalid Phase 9C-A report")
	}
	alphaByID := make(map[string]candidateReport, len(alpha.Candidates))
	for _, c := range alpha.Candidates {
		alphaByID[c.CandidateID] = c
	}

	trainFeatures := featurePaths(1, 9)
	validationFeatures := featurePaths(10, 12)
	scaler := logistic.NewScaler(featureCount)
	var scalerRows int64
	if err = visitFeatures(trainFeatures, func(row featurev2.FeatureRowV2) error {
		values := row.FeatureValues()
		scalerRows++
		return scaler.Observe(values[:])
	}); err != nil {
		return fmt.Errorf("TRAIN scaler: %w", err)
	}
	scaler.Finish()
	fmt.Printf("TRAIN scaler PASS rows=%d\n", scalerRows)

	states := make([]modelState, len(configs))
	optimizer := logistic.OptimizerConfig{Name: "chronological_full_minibatch_adam", Epochs: 1, BatchSize: 8192, LearningRate: .01, Beta1: .9, Beta2: .999, Epsilon: 1e-8}
	for i, c := range configs {
		states[i] = modelState{config: c, logistic: logistic.NewTrainer(featureCount, 1e-4, optimizer), linear: newLinearTrainer(featureCount)}
	}
	if err = joinedPass(trainFeatures, configs, "train", func(i int, row tradespeclabel.Row, values [featureCount]float64) error {
		if !row.LabelValid {
			return nil
		}
		states[i].joined++
		var scaled [featureCount]float64
		if transformErr := scaler.TransformInto(scaled[:], values[:]); transformErr != nil {
			return transformErr
		}
		states[i].logistic.Observe(scaled[:], row.NetProfitableExFunding)
		states[i].linear.observe(scaled[:], row.NetReturnExFunding)
		states[i].fitRows++
		return nil
	}); err != nil {
		return fmt.Errorf("full TRAIN fit: %w", err)
	}
	for i := range states {
		states[i].logistic.FinishEpoch()
		states[i].linear.finish()
		fmt.Printf("FULL TRAIN %s PASS fit=%d\n", states[i].config.ID, states[i].fitRows)
	}

	if err = joinedPass(validationFeatures, configs, "validation", func(i int, row tradespeclabel.Row, values [featureCount]float64) error {
		if !row.LabelValid {
			return nil
		}
		var scaled [featureCount]float64
		if transformErr := scaler.TransformInto(scaled[:], values[:]); transformErr != nil {
			return transformErr
		}
		logit := states[i].logistic.Intercept
		for j, value := range scaled {
			logit += states[i].logistic.Weights[j] * value
		}
		states[i].predictions = append(states[i].predictions, prediction{Timestamp: row.DecisionTimestampMs,
			Probability: logistic.Sigmoid(logit), PredictedReturn: states[i].linear.predict(scaled[:]),
			ActualReturn: row.NetReturnExFunding, Positive: row.NetProfitableExFunding})
		return nil
	}); err != nil {
		return fmt.Errorf("VALIDATION evaluation: %w", err)
	}

	r := robustnessReport{Version: 1, Symbol: "BTCUSDT", FeatureVersion: 2, FeatureCount: featureCount,
		FeatureRegistryHash: registryHash, Phase9CAPath: filepath.FromSlash(alphaReportPath), Phase9CASHA256: alphaHash,
		TrainingSampling: "ALL_VALID_TRAIN_JOINED_SAMPLES", PercentilePolicy: "FIXED_WITHIN_EVALUATION_SUBSET_50_25_10_5_2_1; reported stability subsets use 10_5_1",
		ThinningAnchor: "UNIX_EPOCH_PLUS_5000MS", NumericImputation: "NONE"}
	for i := range states {
		fullClass := classification(states[i].predictions)
		fullRegression := regression(states[i].predictions)
		prior, ok := alphaByID[states[i].config.ID]
		if !ok {
			return fmt.Errorf("missing Phase 9C-A candidate %s", states[i].config.ID)
		}
		comparison := metricComparison{Alpha20Classification: prior.Classification, FullClassification: fullClass,
			Alpha20Regression: prior.Regression, FullRegression: fullRegression,
			FullMinusAlpha20: metricDelta{AUC: fullClass.ROCAUC - prior.Classification.ROCAUC,
				LogLoss: fullClass.LogLoss - prior.Classification.LogLoss, Brier: fullClass.Brier - prior.Classification.Brier,
				AP:  fullClass.AveragePrecision - prior.Classification.AveragePrecision,
				MAE: fullRegression.MAE - prior.Regression.MAE, RMSE: fullRegression.RMSE - prior.Regression.RMSE,
				Pearson: fullRegression.PearsonCorrelation - prior.Regression.PearsonCorrelation}}
		aggregate := summarizeSubset("ALL_VALIDATION", states[i].predictions)
		monthly := make([]subsetReport, 0, 3)
		for month := 10; month <= 12; month++ {
			subset := filterPredictions(states[i].predictions, func(p prediction) bool {
				at := time.UnixMilli(p.Timestamp).UTC()
				return at.Year() == 2024 && int(at.Month()) == month
			})
			monthly = append(monthly, summarizeSubset(fmt.Sprintf("2024-%02d", month), subset))
		}
		thinning := []subsetReport{
			summarizeSubset("15_MINUTE_FIXED_ANCHOR", filterByInterval(states[i].predictions, 15*time.Minute)),
			summarizeSubset("60_MINUTE_FIXED_ANCHOR", filterByInterval(states[i].predictions, 60*time.Minute)),
		}
		candidateVerdict := robustnessVerdict(prior, comparison, aggregate, monthly, thinning)
		if candidateVerdict == "ROBUST_PROMISING" {
			r.RobustCandidates++
		} else if candidateVerdict == "PROMISING_BUT_UNSTABLE" {
			r.UnstableCandidates++
		}
		c := states[i].config
		r.Candidates = append(r.Candidates, robustnessCandidate{CandidateID: c.ID, Side: c.Side, TPBps: c.TP, SLBps: c.SL,
			HorizonSeconds: c.Horizon, FullTrainFitRows: states[i].fitRows, ValidationRows: int64(len(states[i].predictions)),
			MetricComparison: comparison, Aggregate: aggregate, Monthly: monthly, Thinning: thinning, Verdict: candidateVerdict})
		fmt.Printf("ROBUSTNESS %s PASS verdict=%s\n", c.ID, candidateVerdict)
		states[i].predictions = nil
	}
	if r.RobustCandidates >= 3 {
		r.OverallVerdict = "FEATURE_V2_ALPHA_ROBUST"
	} else if r.RobustCandidates+r.UnstableCandidates > 0 {
		r.OverallVerdict = "FEATURE_V2_ALPHA_PARTIALLY_ROBUST"
	} else {
		r.OverallVerdict = "FEATURE_V2_ALPHA_NOT_ROBUST"
	}
	r.ElapsedMs = time.Since(started).Milliseconds()
	r.Complete = true
	if err = writeJSON(output, r); err != nil {
		return err
	}
	fmt.Printf("PHASE 9C-B PASS verdict=%s elapsed=%s\n", r.OverallVerdict, time.Since(started).Round(time.Second))
	return nil
}

func readAlphaReport(path string) (report, string, error) {
	var r report
	b, err := os.ReadFile(path)
	if err != nil {
		return r, "", err
	}
	if err = json.Unmarshal(b, &r); err != nil {
		return r, "", err
	}
	sum := sha256.Sum256(b)
	return r, hex.EncodeToString(sum[:]), nil
}

func summarizeSubset(name string, rows []prediction) subsetReport {
	result := subsetReport{Name: name, Rows: int64(len(rows))}
	if len(rows) == 0 {
		return result
	}
	returns := make([]float64, len(rows))
	for i, row := range rows {
		returns[i] = row.ActualReturn
		result.MeanActualReturn += row.ActualReturn
	}
	result.MeanActualReturn /= float64(len(rows))
	result.MedianActualReturn = median(returns)
	classBuckets, classThresholds := economics(rows, func(v prediction) float64 { return v.Probability })
	returnBuckets, returnThresholds := economics(rows, func(v prediction) float64 { return v.PredictedReturn })
	for _, index := range []int{2, 3, 5} {
		result.ClassificationPercentiles = append(result.ClassificationPercentiles, classBuckets[index])
		result.RegressionPercentiles = append(result.RegressionPercentiles, returnBuckets[index])
	}
	result.Combined = []combined{
		combinedEconomics(rows, .10, classThresholds[2], returnThresholds[2]),
		combinedEconomics(rows, .05, classThresholds[3], returnThresholds[3]),
	}
	return result
}

func filterPredictions(rows []prediction, keep func(prediction) bool) []prediction {
	result := make([]prediction, 0, len(rows)/3)
	for _, row := range rows {
		if keep(row) {
			result = append(result, row)
		}
	}
	return result
}

func filterByInterval(rows []prediction, interval time.Duration) []prediction {
	intervalMs := interval.Milliseconds()
	return filterPredictions(rows, func(p prediction) bool { return (p.Timestamp-5000)%intervalMs == 0 })
}

func median(values []float64) float64 {
	sort.Float64s(values)
	middle := len(values) / 2
	if len(values)%2 == 0 {
		return (values[middle-1] + values[middle]) / 2
	}
	return values[middle]
}

func robustnessVerdict(prior candidateReport, comparison metricComparison, aggregate subsetReport, monthly, thinning []subsetReport) string {
	signalMaintained := comparison.FullClassification.ROCAUC >= .52 && comparison.FullClassification.ROCAUC >= prior.Classification.ROCAUC-.02
	aggregatePositive := hasPositiveNonTiny(aggregate)
	positiveMonths := 0
	for _, month := range monthly {
		if hasPositiveNonTiny(month) {
			positiveMonths++
		}
	}
	thinnedPositive := false
	for _, thin := range thinning {
		if hasPositiveNonTiny(thin) {
			thinnedPositive = true
		}
	}
	if signalMaintained && aggregatePositive && positiveMonths >= 2 && thinnedPositive {
		return "ROBUST_PROMISING"
	}
	if signalMaintained && aggregatePositive {
		return "PROMISING_BUT_UNSTABLE"
	}
	return "NOT_ROBUST"
}

func hasPositiveNonTiny(report subsetReport) bool {
	minimum := int64(math.Max(25, float64(report.Rows)*.005))
	for _, b := range append(append([]bucket{}, report.ClassificationPercentiles[:2]...), report.RegressionPercentiles[:2]...) {
		if b.Count >= minimum && b.MeanActualReturn > 0 {
			return true
		}
	}
	for _, b := range report.Combined {
		if b.Count >= minimum && b.MeanActualReturn > 0 {
			return true
		}
	}
	return false
}
