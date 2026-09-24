package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"

	featurev2 "binance_trader/internal/feature/main/v2"
	tradespeclabel "binance_trader/internal/label/tradespec"
	"binance_trader/internal/model/logistic"
	"github.com/parquet-go/parquet-go"
)

const (
	registryHash = "a37306b80ecbf701103ab691445d07a39624ce995d84f8206d6314dae3045bef"
	featureCount = 128
	trainStride  = int64(5)
	returnScale  = 1000.0
)

var percentiles = []float64{.50, .25, .10, .05, .02, .01}

type candidateConfig struct {
	ID, Side string
	TP, SL   int
	Horizon  int
	Root     string
}

type labelCursor struct {
	file   *os.File
	reader *parquet.GenericReader[tradespeclabel.Row]
	buf    []tradespeclabel.Row
	at, n  int
	row    tradespeclabel.Row
	has    bool
}

func newLabelCursor(path string) (*labelCursor, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	c := &labelCursor{file: f, reader: parquet.NewGenericReader[tradespeclabel.Row](f), buf: make([]tradespeclabel.Row, 4096)}
	if err = c.advance(); err != nil && !errors.Is(err, io.EOF) {
		_ = c.close()
		return nil, err
	}
	return c, nil
}

func (c *labelCursor) advance() error {
	for {
		if c.at < c.n {
			c.row = c.buf[c.at]
			c.at++
			c.has = true
			return nil
		}
		n, err := c.reader.Read(c.buf)
		c.at, c.n = 0, n
		if errors.Is(err, io.EOF) && n == 0 {
			c.has = false
			return io.EOF
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
	}
}

func (c *labelCursor) match(timestamp int64) (tradespeclabel.Row, bool, error) {
	for c.has && c.row.DecisionTimestampMs < timestamp {
		if err := c.advance(); err != nil && !errors.Is(err, io.EOF) {
			return tradespeclabel.Row{}, false, err
		}
	}
	if !c.has || c.row.DecisionTimestampMs != timestamp {
		return tradespeclabel.Row{}, false, nil
	}
	row := c.row
	if err := c.advance(); err != nil && !errors.Is(err, io.EOF) {
		return tradespeclabel.Row{}, false, err
	}
	return row, true, nil
}

func (c *labelCursor) close() error {
	err := c.reader.Close()
	if closeErr := c.file.Close(); err == nil {
		err = closeErr
	}
	return err
}

type linearTrainer struct {
	weights, grad, m, v                              []float64
	intercept, gradIntercept, mIntercept, vIntercept float64
	lambda, learningRate, beta1, beta2, epsilon      float64
	batchSize, batch, steps                          int64
}

func newLinearTrainer(features int) *linearTrainer {
	return &linearTrainer{
		weights: make([]float64, features), grad: make([]float64, features),
		m: make([]float64, features), v: make([]float64, features),
		lambda: 1e-4, learningRate: .001, beta1: .9, beta2: .999, epsilon: 1e-8, batchSize: 8192,
	}
}

func (t *linearTrainer) observe(x []float64, y float64) {
	pred := t.predictRaw(x)
	d := pred - y*returnScale
	t.gradIntercept += d
	for i := range x {
		t.grad[i] += d * x[i]
	}
	t.batch++
	if t.batch == t.batchSize {
		t.step()
	}
}

func (t *linearTrainer) step() {
	if t.batch == 0 {
		return
	}
	t.steps++
	scale := 1 / float64(t.batch)
	t.mIntercept = t.beta1*t.mIntercept + (1-t.beta1)*t.gradIntercept*scale
	t.vIntercept = t.beta2*t.vIntercept + (1-t.beta2)*math.Pow(t.gradIntercept*scale, 2)
	t.intercept -= t.learningRate * (t.mIntercept / (1 - math.Pow(t.beta1, float64(t.steps)))) / (math.Sqrt(t.vIntercept/(1-math.Pow(t.beta2, float64(t.steps)))) + t.epsilon)
	t.gradIntercept = 0
	for i := range t.weights {
		g := t.grad[i]*scale + t.lambda*t.weights[i]
		t.m[i] = t.beta1*t.m[i] + (1-t.beta1)*g
		t.v[i] = t.beta2*t.v[i] + (1-t.beta2)*g*g
		t.weights[i] -= t.learningRate * (t.m[i] / (1 - math.Pow(t.beta1, float64(t.steps)))) / (math.Sqrt(t.v[i]/(1-math.Pow(t.beta2, float64(t.steps)))) + t.epsilon)
		t.grad[i] = 0
	}
	t.batch = 0
}

func (t *linearTrainer) finish() { t.step() }
func (t *linearTrainer) predictRaw(x []float64) float64 {
	v := t.intercept
	for i := range x {
		v += t.weights[i] * x[i]
	}
	return v
}

func (t *linearTrainer) predict(x []float64) float64 { return t.predictRaw(x) / returnScale }

type prediction struct {
	Timestamp                                  int64
	Probability, PredictedReturn, ActualReturn float64
	Positive                                   bool
}

type classificationMetrics struct {
	Rows             int64   `json:"rows"`
	Positive         int64   `json:"positive"`
	PositiveRate     float64 `json:"positive_prevalence"`
	LogLoss          float64 `json:"log_loss"`
	Brier            float64 `json:"brier"`
	ROCAUC           float64 `json:"roc_auc"`
	AveragePrecision float64 `json:"average_precision"`
}

type regressionMetrics struct {
	Rows               int64   `json:"rows"`
	MAE                float64 `json:"mae"`
	RMSE               float64 `json:"rmse"`
	PearsonCorrelation float64 `json:"pearson_correlation"`
}

type bucket struct {
	Percentile         float64 `json:"top_fraction"`
	Count              int64   `json:"count"`
	Coverage           float64 `json:"coverage"`
	PositiveRate       float64 `json:"positive_rate"`
	MeanActualReturn   float64 `json:"mean_actual_net_return_ex_funding"`
	MedianActualReturn float64 `json:"median_actual_net_return_ex_funding"`
}

type combined struct {
	Percentile         float64 `json:"top_fraction_each"`
	Count              int64   `json:"count"`
	Coverage           float64 `json:"coverage"`
	PositiveRate       float64 `json:"positive_rate"`
	MeanActualReturn   float64 `json:"mean_actual_net_return_ex_funding"`
	MedianActualReturn float64 `json:"median_actual_net_return_ex_funding"`
}

type candidateReport struct {
	CandidateID                string                `json:"candidate_id"`
	Side                       string                `json:"side"`
	TPBps                      int                   `json:"tp_bps"`
	SLBps                      int                   `json:"sl_bps"`
	HorizonSeconds             int                   `json:"horizon_seconds"`
	TrainJoinedRows            int64                 `json:"train_joined_rows"`
	TrainOptimizerRows         int64                 `json:"train_optimizer_rows"`
	ValidationRows             int64                 `json:"validation_rows"`
	Classification             classificationMetrics `json:"classification"`
	Regression                 regressionMetrics     `json:"regression"`
	ClassificationPercentiles  []bucket              `json:"classification_percentiles"`
	RegressionPercentiles      []bucket              `json:"regression_percentiles"`
	Combined                   []combined            `json:"combined_intersections"`
	BestMeaningfulBucket       bucket                `json:"best_meaningful_bucket"`
	BestMeaningfulBucketSource string                `json:"best_meaningful_bucket_source"`
	Verdict                    string                `json:"verdict"`
}

type v1Comparison struct {
	Available bool   `json:"available"`
	Reason    string `json:"reason"`
}

type report struct {
	Version              int               `json:"version"`
	Symbol               string            `json:"symbol"`
	FeatureVersion       int               `json:"feature_version"`
	FeatureCount         int               `json:"feature_count"`
	FeatureRegistryHash  string            `json:"feature_registry_hash"`
	ScalerFitScope       string            `json:"scaler_fit_scope"`
	ScalerFitRows        int64             `json:"scaler_fit_rows"`
	TrainingSampling     string            `json:"training_sampling"`
	Regularization       float64           `json:"regularization"`
	Candidates           []candidateReport `json:"candidates"`
	V1Comparison         v1Comparison      `json:"v1_comparison"`
	PromisingCandidates  int               `json:"promising_candidate_count"`
	OverallVerdict       string            `json:"overall_verdict"`
	TestFeatureAccessed  bool              `json:"test_feature_accessed"`
	TestLabelAccessed    bool              `json:"test_label_accessed"`
	FinalHoldoutAccessed bool              `json:"final_holdout_accessed"`
	NumericImputation    string            `json:"numeric_imputation"`
	ElapsedMs            int64             `json:"elapsed_ms"`
	Complete             bool              `json:"complete"`
}

type modelState struct {
	config      candidateConfig
	logistic    *logistic.Trainer
	linear      *linearTrainer
	joined      int64
	fitRows     int64
	predictions []prediction
}

func main() {
	mode := flag.String("mode", "alpha", "alpha or robustness")
	output := flag.String("output", "", "report output")
	flag.Parse()
	var err error
	if *mode == "alpha" {
		path := *output
		if path == "" {
			path = filepath.FromSlash("data/reports/model/main/v2/phase9c-a/BTCUSDT-feature-v2-alpha-viability.json")
		}
		err = run(path)
	} else if *mode == "robustness" {
		path := *output
		if path == "" {
			path = filepath.FromSlash("data/reports/model/main/v2/phase9c-b/BTCUSDT-feature-v2-alpha-robustness.json")
		}
		err = runRobustness(path)
	} else if *mode == "nonlinear" {
		path := *output
		if path == "" {
			path = filepath.FromSlash("data/reports/model/main/v2/phase9c-cde/stage-c-nonlinear.json")
		}
		err = runNonlinear(path)
	} else if *mode == "policy-freeze" {
		path := *output
		if path == "" {
			path = filepath.FromSlash("data/reports/model/main/v2/phase9c-cde/stage-d-policy-freeze.json")
		}
		err = runPolicyFreeze(path)
	} else if *mode == "test-materialization-checkpoint" {
		path := *output
		if path == "" {
			path = filepath.FromSlash("data/reports/model/main/v2/phase9c-cde/stage-test-materialization.json")
		}
		err = publishTestMaterialization(path)
	} else if *mode == "test-confirmation" {
		path := *output
		if path == "" {
			path = filepath.FromSlash("data/reports/model/main/v2/phase9c-cde/stage-e-test-confirmation.json")
		}
		err = runTestConfirmation(path)
	} else if *mode == "event-backtest" {
		path := *output
		if path == "" {
			path = filepath.FromSlash("data/reports/model/main/v2/phase9c-cde/stage-event-backtest.json")
		}
		err = runEventBacktest(path)
	} else if *mode == "final-report" {
		path := *output
		if path == "" {
			path = filepath.FromSlash("data/reports/model/main/v2/phase9c-cde/BTCUSDT-phase9c-cde-final.json")
		}
		err = publishFinalCDE(path)
	} else if *mode == "production-realism" {
		path := *output
		if path == "" {
			path = filepath.FromSlash("data/reports/production/v1/BTCUSDT")
		}
		err = runProductionRealism(path)
	} else if *mode == "production-build-finalize" {
		path := *output
		if path == "" {
			path = filepath.FromSlash("data/reports/production/v1/BTCUSDT/BTCUSDT-production-realism-v1-final.json")
		}
		err = finalizeProductionBuild(path)
	} else {
		err = fmt.Errorf("unknown mode %q", *mode)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(output string) error {
	started := time.Now()
	configs := candidates()
	if err := preflight(configs); err != nil {
		return err
	}
	trainFeatures := featurePaths(1, 9)
	validationFeatures := featurePaths(10, 12)

	scaler := logistic.NewScaler(featureCount)
	var scalerRows int64
	if err := visitFeatures(trainFeatures, func(row featurev2.FeatureRowV2) error {
		values := row.FeatureValues()
		scalerRows++
		return scaler.Observe(values[:])
	}); err != nil {
		return fmt.Errorf("TRAIN scaler: %w", err)
	}
	scaler.Finish()
	fmt.Printf("TRAIN scaler PASS rows=%d\n", scalerRows)

	states := make([]modelState, len(configs))
	optimizer := logistic.OptimizerConfig{Name: "chronological_decimated_minibatch_adam", Epochs: 1, BatchSize: 8192, LearningRate: .01, Beta1: .9, Beta2: .999, Epsilon: 1e-8}
	for i, c := range configs {
		states[i] = modelState{config: c, logistic: logistic.NewTrainer(featureCount, 1e-4, optimizer), linear: newLinearTrainer(featureCount)}
	}
	if err := joinedPass(trainFeatures, configs, "train", func(i int, row tradespeclabel.Row, values [featureCount]float64) error {
		if !row.LabelValid {
			return nil
		}
		states[i].joined++
		if states[i].joined%trainStride != 0 {
			return nil
		}
		var scaled [featureCount]float64
		if err := scaler.TransformInto(scaled[:], values[:]); err != nil {
			return err
		}
		states[i].logistic.Observe(scaled[:], row.NetProfitableExFunding)
		states[i].linear.observe(scaled[:], row.NetReturnExFunding)
		states[i].fitRows++
		return nil
	}); err != nil {
		return fmt.Errorf("TRAIN fit: %w", err)
	}
	for i := range states {
		states[i].logistic.FinishEpoch()
		states[i].linear.finish()
		fmt.Printf("TRAIN %s PASS joined=%d optimizer=%d\n", states[i].config.ID, states[i].joined, states[i].fitRows)
	}

	if err := joinedPass(validationFeatures, configs, "validation", func(i int, row tradespeclabel.Row, values [featureCount]float64) error {
		if !row.LabelValid {
			return nil
		}
		var scaled [featureCount]float64
		if err := scaler.TransformInto(scaled[:], values[:]); err != nil {
			return err
		}
		logit := states[i].logistic.Intercept
		for j, value := range scaled {
			logit += states[i].logistic.Weights[j] * value
		}
		states[i].predictions = append(states[i].predictions, prediction{
			Timestamp: row.DecisionTimestampMs, Probability: logistic.Sigmoid(logit), PredictedReturn: states[i].linear.predict(scaled[:]),
			ActualReturn: row.NetReturnExFunding, Positive: row.NetProfitableExFunding,
		})
		return nil
	}); err != nil {
		return fmt.Errorf("VALIDATION evaluation: %w", err)
	}

	r := report{Version: 1, Symbol: "BTCUSDT", FeatureVersion: 2, FeatureCount: featureCount, FeatureRegistryHash: registryHash,
		ScalerFitScope: "TRAIN_FEATURE_UNIVERSE_ONLY", ScalerFitRows: scalerRows,
		TrainingSampling: "DETERMINISTIC_EVERY_5TH_JOINED_VALID_TRAIN_SAMPLE", Regularization: 1e-4,
		V1Comparison:      v1Comparison{Available: false, Reason: "Phase 8 V1 TradeSpec(tp50/sl25/h3600) does not match the five frozen Phase 9B candidates"},
		NumericImputation: "NONE"}
	weak := 0
	for i := range states {
		cr := summarize(states[i])
		if cr.Verdict == "PROMISING" {
			r.PromisingCandidates++
		} else if cr.Verdict == "WEAK_SIGNAL" {
			weak++
		}
		r.Candidates = append(r.Candidates, cr)
		fmt.Printf("VALIDATION %s PASS rows=%d verdict=%s\n", cr.CandidateID, cr.ValidationRows, cr.Verdict)
		states[i].predictions = nil
	}
	if r.PromisingCandidates > 0 {
		r.OverallVerdict = "FEATURE_V2_HAS_PROMISING_ALPHA"
	} else if weak > 0 {
		r.OverallVerdict = "FEATURE_V2_SIGNAL_IMPROVED_BUT_NOT_ECONOMIC"
	} else {
		r.OverallVerdict = "NO_MEANINGFUL_FEATURE_V2_SIGNAL"
	}
	r.ElapsedMs = time.Since(started).Milliseconds()
	r.Complete = true
	if err := writeJSON(output, r); err != nil {
		return err
	}
	fmt.Printf("PHASE 9C-A PASS verdict=%s elapsed=%s\n", r.OverallVerdict, time.Since(started).Round(time.Second))
	return nil
}

func candidates() []candidateConfig {
	base := filepath.FromSlash("data/labels/tradespec/v1/cost_synthetic_validation_v1/BTCUSDT")
	return []candidateConfig{
		{"tp100_sl100_h14400", "LONG", 100, 100, 14400, filepath.Join(base, "long", "tp100_sl100_h14400")},
		{"tp75_sl50_h14400", "LONG", 75, 50, 14400, filepath.Join(base, "long", "tp75_sl50_h14400")},
		{"tp50_sl50_h14400", "LONG", 50, 50, 14400, filepath.Join(base, "long", "tp50_sl50_h14400")},
		{"tp75_sl25_h900", "SHORT", 75, 25, 900, filepath.Join(base, "short", "tp75_sl25_h900")},
		{"tp50_sl25_h900", "SHORT", 50, 25, 900, filepath.Join(base, "short", "tp50_sl25_h900")},
	}
}

func preflight(configs []candidateConfig) error {
	if len(featurev2.ModelFeatureColumnsV2) != featureCount || logistic.FeatureRegistryHash(featurev2.ModelFeatureColumnsV2) != registryHash {
		return fmt.Errorf("Feature V2 registry mismatch")
	}
	for _, path := range append(featurePaths(1, 9), featurePaths(10, 12)...) {
		if _, err := os.Stat(path); err != nil {
			return err
		}
	}
	for _, c := range configs {
		manifest, err := tradespeclabel.ReadManifest(filepath.Join(c.Root, "manifest.json"))
		if err != nil {
			return err
		}
		if !manifest.Complete || manifest.CandidateID != c.ID || manifest.Side != c.Side || manifest.TPBps != c.TP || manifest.SLBps != c.SL || manifest.HorizonSeconds != c.Horizon || manifest.ReproductionStatus != tradespeclabel.ReproductionPassed {
			return fmt.Errorf("invalid candidate manifest %s", c.ID)
		}
		for _, partition := range []string{"train", "validation"} {
			if _, err = os.Stat(filepath.Join(c.Root, partition+".parquet")); err != nil {
				return err
			}
		}
	}
	return nil
}

func featurePaths(firstMonth, lastMonth int) []string {
	paths := make([]string, 0, lastMonth-firstMonth+1)
	for month := firstMonth; month <= lastMonth; month++ {
		paths = append(paths, filepath.Join("data", "features", "main", "v2", "BTCUSDT", "2024", fmt.Sprintf("BTCUSDT-main-features-v2-2024-%02d.parquet", month)))
	}
	return paths
}

func visitFeatures(paths []string, visit func(featurev2.FeatureRowV2) error) error {
	for _, path := range paths {
		if _, err := featurev2.ReadRows(path, visit); err != nil {
			return err
		}
	}
	return nil
}

func joinedPass(featurePaths []string, configs []candidateConfig, partition string, visit func(int, tradespeclabel.Row, [featureCount]float64) error) error {
	cursors := make([]*labelCursor, len(configs))
	for i, c := range configs {
		cursor, err := newLabelCursor(filepath.Join(c.Root, partition+".parquet"))
		if err != nil {
			for j := 0; j < i; j++ {
				_ = cursors[j].close()
			}
			return err
		}
		cursors[i] = cursor
	}
	defer func() {
		for _, cursor := range cursors {
			_ = cursor.close()
		}
	}()
	return visitFeatures(featurePaths, func(feature featurev2.FeatureRowV2) error {
		values := feature.FeatureValues()
		for i, cursor := range cursors {
			label, matched, err := cursor.match(feature.DecisionTimestampMs)
			if err != nil {
				return err
			}
			if matched {
				if err = visit(i, label, values); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func summarize(s modelState) candidateReport {
	p := s.predictions
	class := classification(p)
	reg := regression(p)
	cb, classThresholds := economics(p, func(v prediction) float64 { return v.Probability })
	rb, returnThresholds := economics(p, func(v prediction) float64 { return v.PredictedReturn })
	combinedBuckets := []combined{
		combinedEconomics(p, .10, classThresholds[2], returnThresholds[2]),
		combinedEconomics(p, .05, classThresholds[3], returnThresholds[3]),
	}
	best, source := bestMeaningful(cb, rb)
	verdict := verdict(class, best, cb, rb)
	return candidateReport{CandidateID: s.config.ID, Side: s.config.Side, TPBps: s.config.TP, SLBps: s.config.SL, HorizonSeconds: s.config.Horizon,
		TrainJoinedRows: s.joined, TrainOptimizerRows: s.fitRows, ValidationRows: int64(len(p)), Classification: class, Regression: reg,
		ClassificationPercentiles: cb, RegressionPercentiles: rb, Combined: combinedBuckets,
		BestMeaningfulBucket: best, BestMeaningfulBucketSource: source, Verdict: verdict}
}

func classification(p []prediction) classificationMetrics {
	result := classificationMetrics{Rows: int64(len(p))}
	points := make([]prediction, len(p))
	copy(points, p)
	var loss, brier float64
	for _, row := range p {
		prob := math.Max(1e-15, math.Min(1-1e-15, row.Probability))
		y := 0.0
		if row.Positive {
			y = 1
			result.Positive++
		}
		loss += -(y*math.Log(prob) + (1-y)*math.Log(1-prob))
		brier += math.Pow(prob-y, 2)
	}
	result.PositiveRate = float64(result.Positive) / float64(result.Rows)
	result.LogLoss = loss / float64(result.Rows)
	result.Brier = brier / float64(result.Rows)
	sort.Slice(points, func(i, j int) bool { return points[i].Probability < points[j].Probability })
	negative := result.Rows - result.Positive
	var rankSum float64
	for i := 0; i < len(points); {
		j := i + 1
		for j < len(points) && points[j].Probability == points[i].Probability {
			j++
		}
		averageRank := float64(i+1+j) / 2
		for k := i; k < j; k++ {
			if points[k].Positive {
				rankSum += averageRank
			}
		}
		i = j
	}
	result.ROCAUC = (rankSum - float64(result.Positive*(result.Positive+1))/2) / float64(result.Positive*negative)
	sort.Slice(points, func(i, j int) bool { return points[i].Probability > points[j].Probability })
	var seenPositive int64
	for i := 0; i < len(points); {
		j, groupPositive := i+1, int64(0)
		if points[i].Positive {
			groupPositive++
		}
		for j < len(points) && points[j].Probability == points[i].Probability {
			if points[j].Positive {
				groupPositive++
			}
			j++
		}
		seenPositive += groupPositive
		result.AveragePrecision += float64(groupPositive) / float64(result.Positive) * float64(seenPositive) / float64(j)
		i = j
	}
	return result
}

func regression(p []prediction) regressionMetrics {
	result := regressionMetrics{Rows: int64(len(p))}
	var absError, squaredError, sx, sy, sxx, syy, sxy float64
	for _, row := range p {
		d := row.PredictedReturn - row.ActualReturn
		absError += math.Abs(d)
		squaredError += d * d
		sx += row.PredictedReturn
		sy += row.ActualReturn
		sxx += row.PredictedReturn * row.PredictedReturn
		syy += row.ActualReturn * row.ActualReturn
		sxy += row.PredictedReturn * row.ActualReturn
	}
	n := float64(result.Rows)
	result.MAE = absError / n
	result.RMSE = math.Sqrt(squaredError / n)
	denominator := math.Sqrt((n*sxx - sx*sx) * (n*syy - sy*sy))
	if denominator != 0 {
		result.PearsonCorrelation = (n*sxy - sx*sy) / denominator
	}
	return result
}

func economics(p []prediction, score func(prediction) float64) ([]bucket, []float64) {
	ordered := make([]prediction, len(p))
	copy(ordered, p)
	sort.Slice(ordered, func(i, j int) bool { return score(ordered[i]) > score(ordered[j]) })
	result := make([]bucket, len(percentiles))
	thresholds := make([]float64, len(percentiles))
	for i, fraction := range percentiles {
		count := int(math.Ceil(float64(len(ordered)) * fraction))
		thresholds[i] = score(ordered[count-1])
		result[i] = bucketStats(ordered[:count], fraction)
	}
	return result, thresholds
}

func bucketStats(rows []prediction, fraction float64) bucket {
	returns := make([]float64, len(rows))
	var sum float64
	var positive int64
	for i, row := range rows {
		returns[i] = row.ActualReturn
		sum += row.ActualReturn
		if row.Positive {
			positive++
		}
	}
	sort.Float64s(returns)
	median := returns[len(returns)/2]
	if len(returns)%2 == 0 {
		median = (returns[len(returns)/2-1] + median) / 2
	}
	return bucket{Percentile: fraction, Count: int64(len(rows)), Coverage: fraction, PositiveRate: float64(positive) / float64(len(rows)), MeanActualReturn: sum / float64(len(rows)), MedianActualReturn: median}
}

func combinedEconomics(p []prediction, fraction, probabilityThreshold, returnThreshold float64) combined {
	selected := make([]prediction, 0, int(float64(len(p))*fraction))
	for _, row := range p {
		if row.Probability >= probabilityThreshold && row.PredictedReturn >= returnThreshold {
			selected = append(selected, row)
		}
	}
	if len(selected) == 0 {
		return combined{Percentile: fraction}
	}
	b := bucketStats(selected, float64(len(selected))/float64(len(p)))
	return combined{Percentile: fraction, Count: b.Count, Coverage: b.Coverage, PositiveRate: b.PositiveRate, MeanActualReturn: b.MeanActualReturn, MedianActualReturn: b.MedianActualReturn}
}

func bestMeaningful(classificationBuckets, regressionBuckets []bucket) (bucket, string) {
	best := bucket{MeanActualReturn: math.Inf(-1)}
	source := ""
	for _, set := range []struct {
		name string
		rows []bucket
	}{{"CLASSIFICATION", classificationBuckets}, {"REGRESSION", regressionBuckets}} {
		for _, b := range set.rows {
			if b.Count >= 10000 && b.MeanActualReturn > best.MeanActualReturn {
				best, source = b, set.name
			}
		}
	}
	return best, source
}

func verdict(class classificationMetrics, best bucket, classificationBuckets, regressionBuckets []bucket) string {
	positiveMeaningful := 0
	for _, set := range [][]bucket{classificationBuckets, regressionBuckets} {
		for _, b := range set {
			if b.Count >= 10000 && b.MeanActualReturn > 0 {
				positiveMeaningful++
			}
		}
	}
	rankingStrong := class.ROCAUC >= .52 || class.AveragePrecision >= class.PositiveRate+.01
	if rankingStrong && positiveMeaningful >= 2 && best.MeanActualReturn > 0 {
		return "PROMISING"
	}
	if class.ROCAUC >= .505 || class.AveragePrecision >= class.PositiveRate+.002 || best.MeanActualReturn > 0 {
		return "WEAK_SIGNAL"
	}
	return "NO_SIGNAL"
}

func writeJSON(path string, value any) error {
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	if err = os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err = os.WriteFile(tmp, b, 0644); err != nil {
		return err
	}
	if err = os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return os.Rename(tmp, path)
}
