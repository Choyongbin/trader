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

const nonlinearSampleMod = int64(47)

type treeSample struct {
	X      [featureCount]float32
	Binary bool
	Return float64
}

type decisionStump struct {
	Feature   int     `json:"feature"`
	Threshold float64 `json:"threshold"`
	Left      float64 `json:"left_value"`
	Right     float64 `json:"right_value"`
}

type boostedStumps struct {
	Family       string          `json:"family"`
	Task         string          `json:"task"`
	FeatureCount int             `json:"feature_count"`
	Initial      float64         `json:"initial"`
	LearningRate float64         `json:"learning_rate"`
	Stumps       []decisionStump `json:"stumps"`
}

func (m boostedStumps) predict(x [featureCount]float64, trees int) float64 {
	value := m.Initial
	if trees > len(m.Stumps) {
		trees = len(m.Stumps)
	}
	for _, stump := range m.Stumps[:trees] {
		leaf := stump.Right
		if x[stump.Feature] <= stump.Threshold {
			leaf = stump.Left
		}
		value += m.LearningRate * leaf
	}
	if m.Task == "classification" {
		return logistic.Sigmoid(value)
	}
	return value
}

type baselineArtifact struct {
	Family              string          `json:"family"`
	FeatureCount        int             `json:"feature_count"`
	FeatureRegistryHash string          `json:"feature_registry_hash"`
	Scaler              logistic.Scaler `json:"scaler"`
	ClassIntercept      float64         `json:"classification_intercept"`
	ClassWeights        []float64       `json:"classification_weights"`
	RegressionIntercept float64         `json:"regression_intercept_scaled"`
	RegressionWeights   []float64       `json:"regression_weights_scaled"`
	ReturnScale         float64         `json:"return_scale"`
}

func (a baselineArtifact) predict(x [featureCount]float64) (float64, float64, error) {
	var scaled [featureCount]float64
	if err := a.Scaler.TransformInto(scaled[:], x[:]); err != nil {
		return 0, 0, err
	}
	logit, ret := a.ClassIntercept, a.RegressionIntercept
	for i, value := range scaled {
		logit += a.ClassWeights[i] * value
		ret += a.RegressionWeights[i] * value
	}
	return logistic.Sigmoid(logit), ret / a.ReturnScale, nil
}

type nonlinearArtifact struct {
	Family              string        `json:"family"`
	FeatureCount        int           `json:"feature_count"`
	FeatureRegistryHash string        `json:"feature_registry_hash"`
	Classification      boostedStumps `json:"classification"`
	Regression          boostedStumps `json:"regression"`
}

func (a nonlinearArtifact) predict(x [featureCount]float64) (float64, float64) {
	return a.Classification.predict(x, len(a.Classification.Stumps)), a.Regression.predict(x, len(a.Regression.Stumps))
}

type nonlinearEvaluation struct {
	ConfigName     string                `json:"config_name"`
	Trees          int                   `json:"trees"`
	Classification classificationMetrics `json:"classification"`
	Regression     regressionMetrics     `json:"regression"`
	Aggregate      subsetReport          `json:"aggregate"`
	Monthly        []subsetReport        `json:"monthly"`
	Thinning       []subsetReport        `json:"thinning"`
}

type stageCModelRef struct {
	Family            string `json:"family"`
	Path              string `json:"path"`
	SHA256            string `json:"sha256"`
	PreprocessingHash string `json:"preprocessing_sha256"`
}

type stageCCandidate struct {
	CandidateID        string                `json:"candidate_id"`
	Side               string                `json:"side"`
	TPBps              int                   `json:"tp_bps"`
	SLBps              int                   `json:"sl_bps"`
	HorizonSeconds     int                   `json:"horizon_seconds"`
	FullTrainRows      int64                 `json:"full_train_rows"`
	NonlinearTrainRows int64                 `json:"nonlinear_train_rows"`
	ValidationRows     int64                 `json:"validation_rows"`
	Baseline           nonlinearEvaluation   `json:"baseline"`
	NonlinearConfigs   []nonlinearEvaluation `json:"nonlinear_configs"`
	ChosenConfig       string                `json:"chosen_nonlinear_config"`
	ComparisonVerdict  string                `json:"comparison_verdict"`
	SelectedFamily     string                `json:"selected_family"`
	BaselineArtifact   stageCModelRef        `json:"baseline_artifact"`
	NonlinearArtifact  stageCModelRef        `json:"nonlinear_artifact"`
}

type stageCReport struct {
	Version              int               `json:"version"`
	Stage                string            `json:"stage"`
	Status               string            `json:"status"`
	NonlinearFamily      string            `json:"nonlinear_family"`
	ConfigCount          int               `json:"config_count"`
	ConfigSemantics      []string          `json:"config_semantics"`
	TrainSampling        string            `json:"train_sampling"`
	FeatureCount         int               `json:"feature_count"`
	FeatureRegistryHash  string            `json:"feature_registry_hash"`
	Candidates           []stageCCandidate `json:"candidates"`
	TestFeatureAccessed  bool              `json:"test_feature_accessed"`
	TestLabelAccessed    bool              `json:"test_label_accessed"`
	FinalHoldoutAccessed bool              `json:"final_holdout_accessed"`
	ElapsedMs            int64             `json:"elapsed_ms"`
	Complete             bool              `json:"complete"`
}

type stageCValidation struct {
	Timestamp, ActualBits                   int64
	ActualReturn                            float64
	BaselineProbability, BaselineReturn     float64
	NonlinearAProbability, NonlinearAReturn float64
	NonlinearBProbability, NonlinearBReturn float64
}

func (v stageCValidation) positive() bool { return v.ActualBits != 0 }

func runNonlinear(output string) error {
	started := time.Now()
	if existing, err := readCompleteStageC(output); err != nil {
		return err
	} else if existing {
		fmt.Println("STAGE C RESUME PASS complete=true")
		return nil
	}
	configs := candidates()
	if err := preflight(configs); err != nil {
		return err
	}
	trainFeatures, validationFeatures := featurePaths(1, 9), featurePaths(10, 12)
	scaler := logistic.NewScaler(featureCount)
	if err := visitFeatures(trainFeatures, func(row featurev2.FeatureRowV2) error {
		values := row.FeatureValues()
		return scaler.Observe(values[:])
	}); err != nil {
		return fmt.Errorf("Stage C scaler: %w", err)
	}
	scaler.Finish()
	states := make([]modelState, len(configs))
	samples := make([][]treeSample, len(configs))
	optimizer := logistic.OptimizerConfig{Name: "chronological_full_minibatch_adam", Epochs: 1, BatchSize: 8192, LearningRate: .01, Beta1: .9, Beta2: .999, Epsilon: 1e-8}
	for i, c := range configs {
		states[i] = modelState{config: c, logistic: logistic.NewTrainer(featureCount, 1e-4, optimizer), linear: newLinearTrainer(featureCount)}
		samples[i] = make([]treeSample, 0, 100000)
	}
	if err := joinedPass(trainFeatures, configs, "train", func(i int, row tradespeclabel.Row, values [featureCount]float64) error {
		if !row.LabelValid {
			return nil
		}
		states[i].joined++
		var scaled [featureCount]float64
		if err := scaler.TransformInto(scaled[:], values[:]); err != nil {
			return err
		}
		states[i].logistic.Observe(scaled[:], row.NetProfitableExFunding)
		states[i].linear.observe(scaled[:], row.NetReturnExFunding)
		states[i].fitRows++
		if ((row.DecisionTimestampMs/5000)+int64(i)*7)%nonlinearSampleMod == 0 {
			var sample treeSample
			for j, value := range values {
				sample.X[j] = float32(value)
			}
			sample.Binary, sample.Return = row.NetProfitableExFunding, row.NetReturnExFunding
			samples[i] = append(samples[i], sample)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("Stage C TRAIN: %w", err)
	}
	baselineArtifacts := make([]baselineArtifact, len(configs))
	nonlinearArtifacts := make([]nonlinearArtifact, len(configs))
	sampleCounts := make([]int64, len(configs))
	for i := range states {
		states[i].logistic.FinishEpoch()
		states[i].linear.finish()
		baselineArtifacts[i] = baselineArtifact{Family: "LOGISTIC_LINEAR", FeatureCount: featureCount, FeatureRegistryHash: registryHash,
			Scaler: *scaler, ClassIntercept: states[i].logistic.Intercept, ClassWeights: states[i].logistic.Weights,
			RegressionIntercept: states[i].linear.intercept, RegressionWeights: states[i].linear.weights, ReturnScale: returnScale}
		classModel := trainBoostedStumps(samples[i], "classification", 64, i)
		regressionModel := trainBoostedStumps(samples[i], "regression", 64, i+17)
		nonlinearArtifacts[i] = nonlinearArtifact{Family: "GRADIENT_BOOSTED_DECISION_STUMPS", FeatureCount: featureCount,
			FeatureRegistryHash: registryHash, Classification: classModel, Regression: regressionModel}
		sampleCounts[i] = int64(len(samples[i]))
		fmt.Printf("STAGE C TRAIN %s PASS full=%d nonlinear=%d\n", configs[i].ID, states[i].fitRows, len(samples[i]))
		samples[i] = nil
	}

	validation := make([][]stageCValidation, len(configs))
	if err := joinedPass(validationFeatures, configs, "validation", func(i int, row tradespeclabel.Row, values [featureCount]float64) error {
		if !row.LabelValid {
			return nil
		}
		bp, br, err := baselineArtifacts[i].predict(values)
		if err != nil {
			return err
		}
		n := nonlinearArtifacts[i]
		positive := int64(0)
		if row.NetProfitableExFunding {
			positive = 1
		}
		validation[i] = append(validation[i], stageCValidation{Timestamp: row.DecisionTimestampMs, ActualBits: positive, ActualReturn: row.NetReturnExFunding,
			BaselineProbability: bp, BaselineReturn: br,
			NonlinearAProbability: n.Classification.predict(values, 32), NonlinearAReturn: n.Regression.predict(values, 32),
			NonlinearBProbability: n.Classification.predict(values, 64), NonlinearBReturn: n.Regression.predict(values, 64)})
		return nil
	}); err != nil {
		return fmt.Errorf("Stage C VALIDATION: %w", err)
	}

	report := stageCReport{Version: 1, Stage: "C", Status: "PASS", NonlinearFamily: "GRADIENT_BOOSTED_DECISION_STUMPS",
		ConfigCount: 2, ConfigSemantics: []string{"A: depth=1 trees=32 learning_rate=0.05 features_per_split=16", "B: depth=1 trees=64 learning_rate=0.05 features_per_split=16"},
		TrainSampling: "DETERMINISTIC_TIMESTAMP_MOD_47", FeatureCount: featureCount, FeatureRegistryHash: registryHash}
	modelRoot := filepath.FromSlash("models/main/v2/phase9c-cde")
	for i, c := range configs {
		basePred := validationPredictions(validation[i], "baseline")
		aPred := validationPredictions(validation[i], "A")
		bPred := validationPredictions(validation[i], "B")
		baseEval := evaluateModel("BASELINE", 0, basePred)
		aEval := evaluateModel("A", 32, aPred)
		bEval := evaluateModel("B", 64, bPred)
		chosen := aEval
		chosenTrees := 32
		if evaluationScore(bEval) > evaluationScore(aEval) {
			chosen, chosenTrees = bEval, 64
		}
		comparison := compareEvaluations(baseEval, chosen)
		selectedFamily := "LOGISTIC_LINEAR"
		if comparison == "NONLINEAR_WINS" {
			selectedFamily = "GRADIENT_BOOSTED_DECISION_STUMPS"
		}
		nonlinearArtifacts[i].Classification.Stumps = nonlinearArtifacts[i].Classification.Stumps[:chosenTrees]
		nonlinearArtifacts[i].Regression.Stumps = nonlinearArtifacts[i].Regression.Stumps[:chosenTrees]
		candidateRoot := filepath.Join(modelRoot, c.ID)
		basePath := filepath.Join(candidateRoot, "baseline.json")
		nonlinearPath := filepath.Join(candidateRoot, "nonlinear.json")
		baseHash, basePre, err := writeModelArtifact(basePath, baselineArtifacts[i], baselineArtifacts[i].Scaler)
		if err != nil {
			return err
		}
		nonlinearHash, nonlinearPre, err := writeModelArtifact(nonlinearPath, nonlinearArtifacts[i], "RAW_FEATURES_NO_PREPROCESSING")
		if err != nil {
			return err
		}
		report.Candidates = append(report.Candidates, stageCCandidate{CandidateID: c.ID, Side: c.Side, TPBps: c.TP, SLBps: c.SL,
			HorizonSeconds: c.Horizon, FullTrainRows: states[i].fitRows, NonlinearTrainRows: sampleCounts[i],
			ValidationRows: int64(len(validation[i])), Baseline: baseEval, NonlinearConfigs: []nonlinearEvaluation{aEval, bEval},
			ChosenConfig: chosen.ConfigName, ComparisonVerdict: comparison, SelectedFamily: selectedFamily,
			BaselineArtifact:  stageCModelRef{Family: "LOGISTIC_LINEAR", Path: basePath, SHA256: baseHash, PreprocessingHash: basePre},
			NonlinearArtifact: stageCModelRef{Family: "GRADIENT_BOOSTED_DECISION_STUMPS", Path: nonlinearPath, SHA256: nonlinearHash, PreprocessingHash: nonlinearPre}})
		fmt.Printf("STAGE C %s PASS result=%s selected=%s\n", c.ID, comparison, selectedFamily)
		validation[i], basePred, aPred, bPred = nil, nil, nil, nil
	}
	report.ElapsedMs, report.Complete = time.Since(started).Milliseconds(), true
	if err := writeJSON(output, report); err != nil {
		return err
	}
	fmt.Printf("PHASE 9C-C PASS elapsed=%s\n", time.Since(started).Round(time.Second))
	return nil
}

func trainBoostedStumps(samples []treeSample, task string, trees, seed int) boostedStumps {
	model := boostedStumps{Family: "GRADIENT_BOOSTED_DECISION_STUMPS", Task: task, FeatureCount: featureCount, LearningRate: .05}
	var sum float64
	for _, sample := range samples {
		if task == "classification" {
			if sample.Binary {
				sum++
			}
		} else {
			sum += sample.Return
		}
	}
	mean := sum / float64(len(samples))
	model.Initial = mean
	if task == "classification" {
		mean = math.Max(1e-6, math.Min(1-1e-6, mean))
		model.Initial = math.Log(mean / (1 - mean))
	}
	predictions := make([]float64, len(samples))
	for i := range predictions {
		predictions[i] = model.Initial
	}
	thresholds := quantileThresholds(samples)
	for tree := 0; tree < trees; tree++ {
		residuals := make([]float64, len(samples))
		for i, sample := range samples {
			target := sample.Return
			prediction := predictions[i]
			if task == "classification" {
				target = 0
				if sample.Binary {
					target = 1
				}
				prediction = logistic.Sigmoid(prediction)
			}
			residuals[i] = target - prediction
		}
		bestGain := math.Inf(-1)
		var best decisionStump
		for k := 0; k < 16; k++ {
			feature := (tree*17 + k*31 + seed*13) % featureCount
			cuts := thresholds[feature]
			binSums := make([]float64, len(cuts)+1)
			binCounts := make([]int, len(cuts)+1)
			var totalSum float64
			for i, sample := range samples {
				bin := sort.SearchFloat64s(cuts, float64(sample.X[feature]))
				binSums[bin] += residuals[i]
				binCounts[bin]++
				totalSum += residuals[i]
			}
			var leftSum float64
			var leftCount int
			for cutIndex, threshold := range cuts {
				leftSum += binSums[cutIndex]
				leftCount += binCounts[cutIndex]
				rightSum := totalSum - leftSum
				rightCount := len(samples) - leftCount
				if leftCount < 100 || rightCount < 100 {
					continue
				}
				gain := leftSum*leftSum/float64(leftCount) + rightSum*rightSum/float64(rightCount)
				if gain > bestGain {
					bestGain = gain
					best = decisionStump{Feature: feature, Threshold: threshold, Left: leftSum / float64(leftCount), Right: rightSum / float64(rightCount)}
				}
			}
		}
		model.Stumps = append(model.Stumps, best)
		for i, sample := range samples {
			leaf := best.Right
			if float64(sample.X[best.Feature]) <= best.Threshold {
				leaf = best.Left
			}
			predictions[i] += model.LearningRate * leaf
		}
	}
	return model
}

func quantileThresholds(samples []treeSample) [featureCount][]float64 {
	var result [featureCount][]float64
	values := make([]float64, len(samples))
	for feature := 0; feature < featureCount; feature++ {
		for i, sample := range samples {
			values[i] = float64(sample.X[feature])
		}
		sort.Float64s(values)
		for q := 1; q < 16; q++ {
			value := values[len(values)*q/16]
			if len(result[feature]) == 0 || value != result[feature][len(result[feature])-1] {
				result[feature] = append(result[feature], value)
			}
		}
	}
	return result
}

func validationPredictions(rows []stageCValidation, model string) []prediction {
	result := make([]prediction, len(rows))
	for i, row := range rows {
		p, r := row.BaselineProbability, row.BaselineReturn
		if model == "A" {
			p, r = row.NonlinearAProbability, row.NonlinearAReturn
		} else if model == "B" {
			p, r = row.NonlinearBProbability, row.NonlinearBReturn
		}
		result[i] = prediction{Timestamp: row.Timestamp, Probability: p, PredictedReturn: r, ActualReturn: row.ActualReturn, Positive: row.positive()}
	}
	return result
}

func evaluateModel(name string, trees int, predictions []prediction) nonlinearEvaluation {
	result := nonlinearEvaluation{ConfigName: name, Trees: trees, Classification: classification(predictions), Regression: regression(predictions), Aggregate: summarizeSubset("ALL_VALIDATION", predictions)}
	for month := 10; month <= 12; month++ {
		subset := filterPredictions(predictions, func(p prediction) bool {
			at := time.UnixMilli(p.Timestamp).UTC()
			return at.Year() == 2024 && int(at.Month()) == month
		})
		result.Monthly = append(result.Monthly, summarizeSubset(fmt.Sprintf("2024-%02d", month), subset))
	}
	result.Thinning = []subsetReport{
		summarizeSubset("15_MINUTE_FIXED_ANCHOR", filterByInterval(predictions, 15*time.Minute)),
		summarizeSubset("60_MINUTE_FIXED_ANCHOR", filterByInterval(predictions, 60*time.Minute)),
	}
	return result
}

func evaluationScore(e nonlinearEvaluation) float64 {
	monthlyPositive := 0
	for _, month := range e.Monthly {
		if month.Combined[1].MeanActualReturn > 0 {
			monthlyPositive++
		}
	}
	thinningPositive := 0
	for _, thin := range e.Thinning {
		if thin.Combined[1].MeanActualReturn > 0 {
			thinningPositive++
		}
	}
	return float64(monthlyPositive)*100 + float64(thinningPositive)*10 + e.Aggregate.Combined[1].MeanActualReturn*1000 + e.Aggregate.ClassificationPercentiles[1].MeanActualReturn*100 + e.Classification.ROCAUC
}

func compareEvaluations(baseline, nonlinear nonlinearEvaluation) string {
	b, n := evaluationScore(baseline), evaluationScore(nonlinear)
	if n > b+.10 && nonlinear.Classification.ROCAUC >= baseline.Classification.ROCAUC-.005 {
		return "NONLINEAR_WINS"
	}
	if b > n+.10 {
		return "BASELINE_WINS"
	}
	return "NO_CLEAR_WIN"
}

func writeModelArtifact(path string, model, preprocessing any) (string, string, error) {
	if err := writeJSON(path, model); err != nil {
		return "", "", err
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return "", "", err
	}
	sum := sha256.Sum256(b)
	pre, err := json.Marshal(preprocessing)
	if err != nil {
		return "", "", err
	}
	preSum := sha256.Sum256(pre)
	return hex.EncodeToString(sum[:]), hex.EncodeToString(preSum[:]), nil
}

func readCompleteStageC(path string) (bool, error) {
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var report stageCReport
	if err = json.Unmarshal(b, &report); err != nil {
		return false, err
	}
	if !report.Complete || report.Status != "PASS" || len(report.Candidates) != 5 {
		return false, fmt.Errorf("incomplete existing Stage C report")
	}
	return true, nil
}
