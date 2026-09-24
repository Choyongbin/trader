package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"

	logistic "binance_trader/internal/model/logistic"
	maintraining "binance_trader/internal/training/main"
)

type reportAudit struct {
	ModelFamily          string `json:"model_family"`
	ModelVersion         int    `json:"model_version"`
	ModelFeatureCount    int    `json:"model_feature_count"`
	FinalHoldoutAccessed bool   `json:"final_holdout_accessed"`
	TestUsedForSelection bool   `json:"test_used_for_selection"`
	Sides                []struct {
		Side           string  `json:"side"`
		SelectedLambda float64 `json:"selected_lambda"`
		Candidates     []struct {
			Convergence logistic.Convergence `json:"convergence"`
		} `json:"candidates"`
		Train, Validation, Test logistic.Evaluation
	} `json:"sides"`
}

func main() {
	log.SetFlags(0)
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
func run() error {
	root := flag.String("model-root", ".\\models\\main\\logistic\\v1\\tp50_sl25_h3600\\cost_synthetic_validation_v1\\BTCUSDT", "models")
	reportPath := flag.String("report", ".\\data\\reports\\models\\logistic\\main\\v1\\BTCUSDT-logistic-v1.json", "report")
	flag.Parse()
	b, err := os.ReadFile(*reportPath)
	if err != nil {
		return err
	}
	var r reportAudit
	if err = json.Unmarshal(b, &r); err != nil {
		return err
	}
	if r.ModelFamily != "logistic_regression" || r.ModelVersion != 1 || r.ModelFeatureCount != 80 || r.FinalHoldoutAccessed || r.TestUsedForSelection || len(r.Sides) != 2 {
		return fmt.Errorf("invalid report metadata")
	}
	zero := make([]float64, 80)
	for _, side := range []string{"LONG", "SHORT"} {
		a, err := logistic.ReadArtifact(filepath.Join(*root, stringLower(side)+".json"))
		if err != nil {
			return err
		}
		if err = a.ValidateRegistry(maintraining.ModelFeatureColumns); err != nil {
			return err
		}
		if a.Side != side || a.ModelVersion != 1 || a.TrainingVersion != 1 || a.FeatureVersion != 1 || a.OutcomeVersion != 2 || a.BarrierVersion != 2 || a.SplitVersion != 1 || !a.Convergence.Converged {
			return fmt.Errorf("invalid %s artifact", side)
		}
		if bad(a.Weights) || bad(a.Scaler.Mean) || bad(a.Scaler.Std) || math.IsNaN(a.Intercept) || math.IsInf(a.Intercept, 0) {
			return fmt.Errorf("NaN/Inf in %s artifact", side)
		}
		p1, err := a.Predict(zero, maintraining.ModelFeatureColumns)
		if err != nil {
			return err
		}
		roundtrip, err := logistic.ReadArtifact(filepath.Join(*root, stringLower(side)+".json"))
		if err != nil {
			return err
		}
		p2, err := roundtrip.Predict(zero, maintraining.ModelFeatureColumns)
		if err != nil || p1 != p2 || math.IsNaN(p1) || math.IsInf(p1, 0) {
			return fmt.Errorf("%s inference round-trip failed", side)
		}
		fmt.Printf("%s feature_count=80 registry_hash=%s lambda=%g converged=true probability_roundtrip=true nan_inf=0\n", side, a.FeatureRegistryHash, a.Lambda)
	}
	fmt.Println("final_holdout_accessed=false test_used_for_selection=false audit=PASS")
	return nil
}
func bad(values []float64) bool {
	for _, x := range values {
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return true
		}
	}
	return false
}
func stringLower(s string) string {
	if s == "LONG" {
		return "long"
	}
	return "short"
}
