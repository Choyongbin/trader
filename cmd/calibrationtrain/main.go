package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	calibration "binance_trader/internal/model/calibration"
	logistic "binance_trader/internal/model/logistic"
	mainsplit "binance_trader/internal/split/main"
	maintraining "binance_trader/internal/training/main"
)

type observation struct {
	raw    float64
	target bool
	net    float64
}
type measured struct {
	Evaluation   logistic.Evaluation      `json:"metrics"`
	Reliability  calibration.Reliability  `json:"reliability"`
	Distribution calibration.Distribution `json:"distribution"`
}
type candidate struct {
	Method     calibration.Method `json:"method"`
	Validation measured           `json:"validation"`
}
type sideReport struct {
	Side              string                 `json:"side"`
	SourceModelSHA256 string                 `json:"source_model_sha256"`
	SelectedMethod    calibration.Method     `json:"selected_method"`
	Platt             calibration.PlattModel `json:"platt"`
	IsotonicBlocks    int                    `json:"isotonic_blocks"`
	Candidates        []candidate            `json:"candidates"`
	TestRaw           measured               `json:"test_raw"`
	TestSelected      measured               `json:"test_selected"`
	MappingExamples   map[string]float64     `json:"mapping_examples"`
}
type report struct {
	CalibrationVersion    int                `json:"calibration_version"`
	SourceLogisticVersion int                `json:"source_logistic_version"`
	SplitVersion          int                `json:"split_version"`
	FinalHoldoutAccessed  bool               `json:"final_holdout_accessed"`
	TestUsedForSelection  bool               `json:"test_used_for_selection"`
	LongSelectedMethod    calibration.Method `json:"long_selected_method"`
	ShortSelectedMethod   calibration.Method `json:"short_selected_method"`
	ReliabilitySemantics  string             `json:"reliability_semantics"`
	Sides                 []sideReport       `json:"sides"`
	ElapsedMs             map[string]int64   `json:"elapsed_ms"`
	TotalElapsedMs        int64              `json:"total_elapsed_ms"`
}

func main() {
	log.SetFlags(0)
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
func run() error {
	root := flag.String("training-root", ".\\data\\training\\main\\v1\\tp50_sl25_h3600\\cost_synthetic_validation_v1\\BTCUSDT", "training")
	splitPath := flag.String("split-manifest", ".\\data\\manifests\\splits\\main\\v1\\BTCUSDT-split-v1.json", "split")
	logisticRoot := flag.String("logistic-root", ".\\models\\main\\logistic\\v1\\tp50_sl25_h3600\\cost_synthetic_validation_v1\\BTCUSDT", "logistic")
	artifactRoot := flag.String("artifact-root", ".\\models\\main\\calibration\\v1\\tp50_sl25_h3600\\cost_synthetic_validation_v1\\BTCUSDT", "calibration")
	output := flag.String("output", ".\\data\\reports\\models\\calibration\\main\\v1\\BTCUSDT-calibration-v1.json", "report")
	flag.Parse()
	started := time.Now()
	sm, err := mainsplit.ReadManifest(*splitPath)
	if err != nil {
		return err
	}
	if !sm.FinalHoldoutSealed {
		return fmt.Errorf("holdout not sealed")
	}
	files := monthlyFiles(*root)
	r := report{CalibrationVersion: 1, SourceLogisticVersion: 1, SplitVersion: 1, FinalHoldoutAccessed: false, TestUsedForSelection: false, ReliabilitySemantics: "equal-width [0,.1),...,[.9,1] and equal-count quantile 10 bins; ECE weighted absolute gap; MCE maximum gap; count<100 warning", ElapsedMs: map[string]int64{}}
	artifacts := make([]calibration.Artifact, 0, 2)
	models := make([]logistic.Artifact, 0, 2)
	for _, side := range []string{"LONG", "SHORT"} {
		stage := time.Now()
		modelPath := filepath.Join(*logisticRoot, lower(side)+".json")
		model, err := logistic.ReadArtifact(modelPath)
		if err != nil {
			return err
		}
		if err = model.ValidateRegistry(maintraining.ModelFeatureColumns); err != nil {
			return err
		}
		hash, err := calibration.FileSHA256(modelPath)
		if err != nil {
			return err
		}
		obs, err := predictPartition(model, side, mainsplit.Validation, sm.Definition, files[9:12])
		if err != nil {
			return err
		}
		raw, targets := unpack(obs)
		platt, err := calibration.FitPlatt(raw, targets)
		if err != nil {
			return err
		}
		iso, err := calibration.FitIsotonic(raw, targets)
		if err != nil {
			return err
		}
		methods := []calibration.Method{calibration.Raw, calibration.Platt, calibration.Isotonic}
		candidates := make([]candidate, 0, 3)
		selected := 0
		for i, method := range methods {
			m := measure(obs, method, platt, iso)
			candidates = append(candidates, candidate{method, m})
			if i > 0 && (m.Evaluation.LogLoss < candidates[selected].Validation.Evaluation.LogLoss-1e-12 || (m.Evaluation.LogLoss == candidates[selected].Validation.Evaluation.LogLoss && m.Evaluation.Brier < candidates[selected].Validation.Evaluation.Brier)) {
				selected = i
			}
		}
		artifact := calibration.Artifact{CalibrationVersion: 1, Side: side, SourceModelFamily: "logistic_regression", SourceModelVersion: 1, SourceModelSHA256: hash, FeatureRegistryHash: model.FeatureRegistryHash, SplitVersion: 1, TrainingVersion: 1, FitPartition: "VALIDATION", SelectedMethod: methods[selected], Platt: platt, Isotonic: iso, CostWarning: "synthetic_validation_v1; funding excluded"}
		sr := sideReport{Side: side, SourceModelSHA256: hash, SelectedMethod: methods[selected], Platt: platt, IsotonicBlocks: iso.Blocks, Candidates: candidates, MappingExamples: map[string]float64{}}
		for _, p := range []float64{.2, .3, .4, .5, .6} {
			v, _ := artifact.Calibrate(p, hash)
			sr.MappingExamples[fmt.Sprintf("%.1f", p)] = v
		}
		r.Sides = append(r.Sides, sr)
		artifacts = append(artifacts, artifact)
		models = append(models, model)
		r.ElapsedMs[side+"_validation_fit_select"] = time.Since(stage).Milliseconds()
		fmt.Printf("%s selected=%s validation raw/platt/isotonic logloss=%.9f/%.9f/%.9f\n", side, methods[selected], candidates[0].Validation.Evaluation.LogLoss, candidates[1].Validation.Evaluation.LogLoss, candidates[2].Validation.Evaluation.LogLoss)
	}
	// Both methods are frozen before any TEST file is opened.
	for i := range artifacts {
		stage := time.Now()
		obs, err := predictPartition(models[i], artifacts[i].Side, mainsplit.Test, sm.Definition, files[12:18])
		if err != nil {
			return err
		}
		r.Sides[i].TestRaw = measure(obs, calibration.Raw, artifacts[i].Platt, artifacts[i].Isotonic)
		r.Sides[i].TestSelected = measure(obs, artifacts[i].SelectedMethod, artifacts[i].Platt, artifacts[i].Isotonic)
		r.ElapsedMs[artifacts[i].Side+"_test_raw_selected"] = time.Since(stage).Milliseconds()
		if err = calibration.WriteArtifact(filepath.Join(*artifactRoot, lower(artifacts[i].Side)+".json"), artifacts[i]); err != nil {
			return err
		}
	}
	r.LongSelectedMethod = artifacts[0].SelectedMethod
	r.ShortSelectedMethod = artifacts[1].SelectedMethod
	r.TotalElapsedMs = time.Since(started).Milliseconds()
	if err = writeJSON(*output, r); err != nil {
		return err
	}
	fmt.Printf("final_holdout_accessed=false test_used_for_selection=false total=%s\n", time.Since(started).Round(time.Millisecond))
	return nil
}
func predictPartition(model logistic.Artifact, side string, p mainsplit.Partition, d mainsplit.SplitDefinition, files []string) ([]observation, error) {
	loader := mainsplit.Loader{Definition: d, Files: files}
	result := make([]observation, 0, 1600000)
	features := make([]float64, 0, 80)
	scaled := make([]float64, 80)
	_, err := loader.VisitDevelopmentPartition(p, func(row maintraining.TrainingRowV1) error {
		valid, target, net := row.LongLabelValid, row.LongNetProfitableExFunding, row.LongNetReturnExFunding
		if side == "SHORT" {
			valid, target, net = row.ShortLabelValid, row.ShortNetProfitableExFunding, row.ShortNetReturnExFunding
		}
		if !valid {
			return nil
		}
		features = mainsplit.ModelFeaturesInto(features, row)
		if err := model.Scaler.TransformInto(scaled, features); err != nil {
			return err
		}
		z := model.Intercept
		for i := range scaled {
			z += model.Weights[i] * scaled[i]
		}
		result = append(result, observation{logistic.Sigmoid(z), target, net})
		return nil
	})
	return result, err
}
func measure(obs []observation, method calibration.Method, platt calibration.PlattModel, iso calibration.IsotonicModel) measured {
	prob := make([]float64, len(obs))
	targets := make([]bool, len(obs))
	var a logistic.EvalAccumulator
	for i, x := range obs {
		p := x.raw
		if method == calibration.Platt {
			p = platt.Calibrate(p)
		} else if method == calibration.Isotonic {
			p = iso.Calibrate(p)
		}
		prob[i], targets[i] = p, x.target
		a.Observe(p, x.target, x.net)
	}
	return measured{a.Finish(), calibration.ReliabilityMetrics(prob, targets), calibration.ProbabilityDistribution(prob)}
}
func unpack(obs []observation) ([]float64, []bool) {
	p := make([]float64, len(obs))
	y := make([]bool, len(obs))
	for i, x := range obs {
		p[i], y[i] = x.raw, x.target
	}
	return p, y
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
func lower(s string) string {
	if s == "LONG" {
		return "long"
	}
	return "short"
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
