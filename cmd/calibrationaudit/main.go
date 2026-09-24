package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"

	calibration "binance_trader/internal/model/calibration"
	logistic "binance_trader/internal/model/logistic"
	maintraining "binance_trader/internal/training/main"
)

type reportAudit struct {
	CalibrationVersion    int                `json:"calibration_version"`
	SourceLogisticVersion int                `json:"source_logistic_version"`
	SplitVersion          int                `json:"split_version"`
	FinalHoldoutAccessed  bool               `json:"final_holdout_accessed"`
	TestUsedForSelection  bool               `json:"test_used_for_selection"`
	LongSelectedMethod    calibration.Method `json:"long_selected_method"`
	ShortSelectedMethod   calibration.Method `json:"short_selected_method"`
	Sides                 []json.RawMessage  `json:"sides"`
}

func main() {
	log.SetFlags(0)
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
func run() error {
	calRoot := flag.String("calibration-root", ".\\models\\main\\calibration\\v1\\tp50_sl25_h3600\\cost_synthetic_validation_v1\\BTCUSDT", "calibration")
	logRoot := flag.String("logistic-root", ".\\models\\main\\logistic\\v1\\tp50_sl25_h3600\\cost_synthetic_validation_v1\\BTCUSDT", "logistic")
	reportPath := flag.String("report", ".\\data\\reports\\models\\calibration\\main\\v1\\BTCUSDT-calibration-v1.json", "report")
	flag.Parse()
	b, err := os.ReadFile(*reportPath)
	if err != nil {
		return err
	}
	var r reportAudit
	if err = json.Unmarshal(b, &r); err != nil {
		return err
	}
	if r.CalibrationVersion != 1 || r.SourceLogisticVersion != 1 || r.SplitVersion != 1 || r.FinalHoldoutAccessed || r.TestUsedForSelection || len(r.Sides) != 2 {
		return fmt.Errorf("invalid calibration report")
	}
	for _, side := range []string{"LONG", "SHORT"} {
		lp := filepath.Join(*logRoot, lower(side)+".json")
		lm, err := logistic.ReadArtifact(lp)
		if err != nil {
			return err
		}
		if err = lm.ValidateRegistry(maintraining.ModelFeatureColumns); err != nil {
			return err
		}
		hash, err := calibration.FileSHA256(lp)
		if err != nil {
			return err
		}
		path := filepath.Join(*calRoot, lower(side)+".json")
		a, err := calibration.ReadArtifact(path)
		if err != nil {
			return err
		}
		if a.Side != side || a.CalibrationVersion != 1 || a.FitPartition != "VALIDATION" || a.SourceModelSHA256 != hash || a.FeatureRegistryHash != lm.FeatureRegistryHash {
			return fmt.Errorf("%s provenance mismatch", side)
		}
		if len(a.Isotonic.Thresholds) != len(a.Isotonic.Values) || len(a.Isotonic.Values) == 0 {
			return fmt.Errorf("%s invalid isotonic arrays", side)
		}
		for i := range a.Isotonic.Values {
			if math.IsNaN(a.Isotonic.Values[i]) || math.IsInf(a.Isotonic.Values[i], 0) || a.Isotonic.Values[i] < 0 || a.Isotonic.Values[i] > 1 || (i > 0 && (a.Isotonic.Values[i] < a.Isotonic.Values[i-1] || a.Isotonic.Thresholds[i] <= a.Isotonic.Thresholds[i-1])) {
				return fmt.Errorf("%s monotonicity/numeric failure", side)
			}
		}
		for _, p := range []float64{0, .2, .5, .8, 1} {
			x, err := a.Calibrate(p, hash)
			if err != nil || math.IsNaN(x) || math.IsInf(x, 0) {
				return fmt.Errorf("%s inference failed", side)
			}
			again, err := calibration.ReadArtifact(path)
			if err != nil {
				return err
			}
			y, err := again.Calibrate(p, hash)
			if err != nil || x != y {
				return fmt.Errorf("%s round trip failed", side)
			}
		}
		if _, err = a.Calibrate(.3, "wrong"); err == nil {
			return fmt.Errorf("%s source mismatch guard failed", side)
		}
		fmt.Printf("%s selected=%s source_hash=%s blocks=%d monotonicity_violations=0 nan_inf=0 roundtrip=PASS\n", side, a.SelectedMethod, hash, a.Isotonic.Blocks)
	}
	fmt.Println("final_holdout_accessed=false test_used_for_selection=false audit=PASS")
	return nil
}
func lower(s string) string {
	if s == "LONG" {
		return "long"
	}
	return "short"
}
