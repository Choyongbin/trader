package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	logistic "binance_trader/internal/model/logistic"
	mainsplit "binance_trader/internal/split/main"
	maintraining "binance_trader/internal/training/main"
)

var lambdas = []float64{0, 1e-6, 1e-5, 1e-4, 1e-3, 1e-2}
var optimizer = logistic.OptimizerConfig{Name: "deterministic_chronological_minibatch_adam", Epochs: 3, BatchSize: 8192, LearningRate: .01, Beta1: .9, Beta2: .999, Epsilon: 1e-8, ConvergenceTolerance: .001}

type candidate struct {
	Lambda         float64              `json:"lambda"`
	TrainObjective float64              `json:"train_objective"`
	Validation     logistic.Evaluation  `json:"validation"`
	Convergence    logistic.Convergence `json:"convergence"`
}
type coefficient struct {
	Name  string  `json:"name"`
	Value float64 `json:"value"`
}
type sideReport struct {
	Side                 string              `json:"side"`
	SelectedLambda       float64             `json:"selected_lambda"`
	ScalerFitRows        int64               `json:"scaler_fit_rows"`
	ConstantFeatureNames []string            `json:"constant_feature_names"`
	Candidates           []candidate         `json:"candidates"`
	Train                logistic.Evaluation `json:"train"`
	Validation           logistic.Evaluation `json:"validation"`
	Test                 logistic.Evaluation `json:"test"`
	TopPositive          []coefficient       `json:"top_positive_coefficients"`
	TopNegative          []coefficient       `json:"top_negative_coefficients"`
	TopAbsolute          []coefficient       `json:"top_absolute_coefficients"`
}
type report struct {
	ModelFamily          string                           `json:"model_family"`
	ModelVersion         int                              `json:"model_version"`
	SplitVersion         int                              `json:"split_version"`
	TrainingVersion      int                              `json:"training_version"`
	FeatureVersion       int                              `json:"feature_version"`
	OutcomeVersion       int                              `json:"outcome_version"`
	BarrierVersion       int                              `json:"barrier_version"`
	TradeSpec            maintraining.TradeSpecManifest   `json:"trade_spec"`
	CostProfile          maintraining.CostProfileManifest `json:"cost_profile"`
	ModelFeatureCount    int                              `json:"model_feature_count"`
	FinalHoldoutAccessed bool                             `json:"final_holdout_accessed"`
	TestUsedForSelection bool                             `json:"test_used_for_selection"`
	CostWarning          string                           `json:"cost_warning"`
	Sides                []sideReport                     `json:"sides"`
	ElapsedMs            map[string]int64                 `json:"elapsed_ms"`
	TotalElapsedMs       int64                            `json:"total_elapsed_ms"`
}

func main() {
	log.SetFlags(0)
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
func run() error {
	root := flag.String("training-root", ".\\data\\training\\main\\v1\\tp50_sl25_h3600\\cost_synthetic_validation_v1\\BTCUSDT", "training root")
	splitPath := flag.String("split-manifest", ".\\data\\manifests\\splits\\main\\v1\\BTCUSDT-split-v1.json", "split manifest")
	modelRoot := flag.String("model-root", ".\\models\\main\\logistic\\v1\\tp50_sl25_h3600\\cost_synthetic_validation_v1\\BTCUSDT", "model root")
	output := flag.String("output", ".\\data\\reports\\models\\logistic\\main\\v1\\BTCUSDT-logistic-v1.json", "report")
	flag.Parse()
	started := time.Now()
	sm, err := mainsplit.ReadManifest(*splitPath)
	if err != nil {
		return err
	}
	if !sm.FinalHoldoutSealed || sm.SplitVersion != 1 || len(sm.ModelFeatureColumns) != 80 {
		return fmt.Errorf("invalid split manifest")
	}
	files := monthlyFiles(*root)
	r := report{ModelFamily: "logistic_regression", ModelVersion: 1, SplitVersion: 1, TrainingVersion: 1, FeatureVersion: 1, OutcomeVersion: 2, BarrierVersion: 2, TradeSpec: sm.TradeSpec, CostProfile: sm.CostProfile, ModelFeatureCount: 80, FinalHoldoutAccessed: false, TestUsedForSelection: false, CostWarning: "synthetic_validation_v1; funding excluded; raw probabilities are not calibrated", ElapsedMs: map[string]int64{}}
	artifacts := make([]logistic.Artifact, 0, 2)
	for _, side := range []string{"LONG", "SHORT"} {
		sr, a, e := trainSide(side, sm, files[:9], files[9:12], r.ElapsedMs)
		if e != nil {
			return e
		}
		r.Sides = append(r.Sides, sr)
		artifacts = append(artifacts, a)
	}
	// Both side-specific lambdas are frozen before TEST is opened.
	testStarted := time.Now()
	for i := range artifacts {
		t := &logistic.Trainer{Weights: artifacts[i].Weights, Intercept: artifacts[i].Intercept}
		r.Sides[i].Test, err = evaluate(t, &artifacts[i].Scaler, artifacts[i].Side, mainsplit.Test, sm.Definition, files[12:18])
		if err != nil {
			return err
		}
		if err = logistic.WriteArtifact(filepath.Join(*modelRoot, strings.ToLower(artifacts[i].Side)+".json"), artifacts[i]); err != nil {
			return err
		}
	}
	r.ElapsedMs["selected_test_evaluation_after_both_frozen"] = time.Since(testStarted).Milliseconds()
	r.TotalElapsedMs = time.Since(started).Milliseconds()
	if err = writeJSON(*output, r); err != nil {
		return err
	}
	fmt.Printf("final_holdout_accessed=false test_used_for_selection=false total=%s\n", time.Since(started).Round(time.Millisecond))
	for _, s := range r.Sides {
		fmt.Printf("%s lambda=%g train/validation/test logloss=%.9f/%.9f/%.9f test_auc=%.6f test_ap=%.6f\n", s.Side, s.SelectedLambda, s.Train.LogLoss, s.Validation.LogLoss, s.Test.LogLoss, s.Test.ROCAUC, s.Test.AveragePrecision)
	}
	return nil
}

func trainSide(side string, sm mainsplit.Manifest, trainFiles, valFiles []string, times map[string]int64) (sideReport, logistic.Artifact, error) {
	started := time.Now()
	scaler := logistic.NewScaler(80)
	loader := mainsplit.Loader{Definition: sm.Definition, Files: trainFiles}
	_, err := loader.VisitDevelopmentPartition(mainsplit.Train, func(row maintraining.TrainingRowV1) error {
		if valid(row, side) {
			return scaler.Observe(mainsplit.ModelFeatures(row))
		}
		return nil
	})
	if err != nil {
		return sideReport{}, logistic.Artifact{}, err
	}
	constant := namesAt(sm.ModelFeatureColumns, scaler.Finish())
	times[side+"_scaler_fit"] = time.Since(started).Milliseconds()
	trainers := make([]*logistic.Trainer, len(lambdas))
	for i, l := range lambdas {
		trainers[i] = logistic.NewTrainer(80, l, optimizer)
	}
	objectives := make([]float64, len(lambdas))
	previousObjectives := make([]float64, len(lambdas))
	gradients := make([]float64, len(lambdas))
	started = time.Now()
	for epoch := 0; epoch < optimizer.Epochs; epoch++ {
		loader = mainsplit.Loader{Definition: sm.Definition, Files: trainFiles}
		_, err = loader.VisitDevelopmentPartition(mainsplit.Train, func(row maintraining.TrainingRowV1) error {
			if !valid(row, side) {
				return nil
			}
			x := mainsplit.ModelFeatures(row)
			z := make([]float64, 80)
			_ = scaler.TransformInto(z, x)
			y, _, _ := target(row, side)
			for _, t := range trainers {
				t.Observe(z, y)
			}
			return nil
		})
		if err != nil {
			return sideReport{}, logistic.Artifact{}, err
		}
		for i, t := range trainers {
			previousObjectives[i] = objectives[i]
			objectives[i], gradients[i] = t.FinishEpoch()
		}
		fmt.Printf("%s epoch=%d objectives=%v\n", side, epoch+1, objectives)
	}
	times[side+"_candidate_training"] = time.Since(started).Milliseconds()
	started = time.Now()
	validations := make([]logistic.Evaluation, len(trainers))
	for i, t := range trainers {
		validations[i], err = evaluate(t, scaler, side, mainsplit.Validation, sm.Definition, valFiles)
		if err != nil {
			return sideReport{}, logistic.Artifact{}, err
		}
	}
	times[side+"_validation"] = time.Since(started).Milliseconds()
	selected := 0
	for i := 1; i < len(validations); i++ {
		if validations[i].LogLoss < validations[selected].LogLoss-1e-12 || (math.Abs(validations[i].LogLoss-validations[selected].LogLoss) <= 1e-12 && validations[i].Brier < validations[selected].Brier) {
			selected = i
		}
	}
	cs := make([]candidate, len(trainers))
	for i := range trainers {
		change := math.Abs(previousObjectives[i] - objectives[i])
		conv := logistic.Convergence{InitialObjective: math.Log(2), FinalObjective: objectives[i], FinalObjectiveChange: change, Epochs: optimizer.Epochs, Converged: objectives[i] < math.Log(2) && change < optimizer.ConvergenceTolerance, FinalGradientNorm: gradients[i]}
		cs[i] = candidate{lambdas[i], objectives[i], validations[i], conv}
	}
	if !cs[selected].Convergence.Converged {
		return sideReport{}, logistic.Artifact{}, fmt.Errorf("selected %s model not converged: %+v", side, cs[selected].Convergence)
	}
	started = time.Now()
	trainEval, err := evaluate(trainers[selected], scaler, side, mainsplit.Train, sm.Definition, trainFiles)
	if err != nil {
		return sideReport{}, logistic.Artifact{}, err
	}
	times[side+"_frozen_train_evaluation"] = time.Since(started).Milliseconds()
	pos, neg, abs := coefficients(sm.ModelFeatureColumns, trainers[selected].Weights)
	sr := sideReport{Side: side, SelectedLambda: lambdas[selected], ScalerFitRows: scaler.Count, ConstantFeatureNames: constant, Candidates: cs, Train: trainEval, Validation: validations[selected], TopPositive: pos, TopNegative: neg, TopAbsolute: abs}
	a := logistic.Artifact{ModelFamily: "logistic_regression", ModelVersion: 1, Side: side, TrainingVersion: 1, FeatureVersion: 1, OutcomeVersion: 2, BarrierVersion: 2, SplitVersion: 1, TradeSpec: sm.TradeSpec, CostProfile: sm.CostProfile, FeatureNames: sm.ModelFeatureColumns, FeatureCount: 80, FeatureRegistryHash: logistic.FeatureRegistryHash(sm.ModelFeatureColumns), Scaler: *scaler, ConstantFeatureNames: constant, Intercept: trainers[selected].Intercept, Weights: trainers[selected].Weights, Lambda: lambdas[selected], Optimizer: optimizer, Convergence: cs[selected].Convergence, TrainRows: trainEval.Rows, ValidationRows: validations[selected].Rows, CostWarning: "synthetic_validation_v1; funding excluded; raw probability is uncalibrated"}
	return sr, a, nil
}

func evaluate(t *logistic.Trainer, s *logistic.Scaler, side string, p mainsplit.Partition, d mainsplit.SplitDefinition, files []string) (logistic.Evaluation, error) {
	loader := mainsplit.Loader{Definition: d, Files: files}
	var a logistic.EvalAccumulator
	_, err := loader.VisitDevelopmentPartition(p, func(row maintraining.TrainingRowV1) error {
		if !valid(row, side) {
			return nil
		}
		x := mainsplit.ModelFeatures(row)
		z := make([]float64, 80)
		_ = s.TransformInto(z, x)
		logit := t.Intercept
		for i := range z {
			logit += t.Weights[i] * z[i]
		}
		prob := logistic.Sigmoid(logit)
		y, net, _ := target(row, side)
		a.Observe(prob, y, net)
		return nil
	})
	return a.Finish(), err
}
func valid(r maintraining.TrainingRowV1, side string) bool {
	if side == "LONG" {
		return r.LongLabelValid
	}
	return r.ShortLabelValid
}
func target(r maintraining.TrainingRowV1, side string) (bool, float64, float64) {
	if side == "LONG" {
		return r.LongNetProfitableExFunding, r.LongNetReturnExFunding, r.LongGrossMarketReturn
	}
	return r.ShortNetProfitableExFunding, r.ShortNetReturnExFunding, r.ShortGrossMarketReturn
}
func monthlyFiles(root string) []string {
	var f []string
	for y := 2024; y <= 2025; y++ {
		for m := 1; m <= 12; m++ {
			k := fmt.Sprintf("%04d-%02d", y, m)
			f = append(f, filepath.Join(root, k[:4], fmt.Sprintf("BTCUSDT-training-v1-%s.parquet", k)))
		}
	}
	return f
}
func namesAt(names []string, indices []int) []string {
	r := make([]string, len(indices))
	for i, j := range indices {
		r[i] = names[j]
	}
	return r
}
func coefficients(names []string, w []float64) ([]coefficient, []coefficient, []coefficient) {
	all := make([]coefficient, len(w))
	for i := range w {
		all[i] = coefficient{names[i], w[i]}
	}
	pos := append([]coefficient(nil), all...)
	sort.Slice(pos, func(i, j int) bool { return pos[i].Value > pos[j].Value })
	neg := append([]coefficient(nil), all...)
	sort.Slice(neg, func(i, j int) bool { return neg[i].Value < neg[j].Value })
	abs := append([]coefficient(nil), all...)
	sort.Slice(abs, func(i, j int) bool { return math.Abs(abs[i].Value) > math.Abs(abs[j].Value) })
	return pos[:10], neg[:10], abs[:10]
}
func writeJSON(path string, v any) error {
	b, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		return e
	}
	b = append(b, '\n')
	if e = os.MkdirAll(filepath.Dir(path), 0755); e != nil {
		return e
	}
	tmp := path + ".tmp"
	if e = os.WriteFile(tmp, b, 0644); e != nil {
		return e
	}
	_ = os.Remove(path)
	return os.Rename(tmp, path)
}
