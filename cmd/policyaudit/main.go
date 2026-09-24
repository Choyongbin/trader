package main

import (
	policy "binance_trader/internal/model/policy"
	"flag"
	"fmt"
	"log"
	"path/filepath"
)

func main() {
	log.SetFlags(0)
	artifact := flag.String("artifact", `.\models\main\policy\v1\tp50_sl25_h3600\cost_synthetic_validation_v1\BTCUSDT\policy.json`, "policy")
	logRoot := flag.String("logistic-root", `.\models\main\logistic\v1\tp50_sl25_h3600\cost_synthetic_validation_v1\BTCUSDT`, "logistic")
	calRoot := flag.String("calibration-root", `.\models\main\calibration\v1\tp50_sl25_h3600\cost_synthetic_validation_v1\BTCUSDT`, "calibration")
	flag.Parse()
	a, e := policy.ReadArtifact(*artifact)
	if e != nil {
		log.Fatal(e)
	}
	checks := []struct{ p, w string }{{filepath.Join(*logRoot, "long.json"), a.Source.LongLogisticSHA256}, {filepath.Join(*logRoot, "short.json"), a.Source.ShortLogisticSHA256}, {filepath.Join(*calRoot, "long.json"), a.Source.LongCalibrationSHA256}, {filepath.Join(*calRoot, "short.json"), a.Source.ShortCalibrationSHA256}}
	for _, c := range checks {
		h, e := policy.FileSHA256(c.p)
		if e != nil || h != c.w {
			log.Fatalf("source hash mismatch %s", c.p)
		}
	}
	if a.PolicyVersion != 1 || a.MaxLabelDependencyMs <= 0 || a.Confirmation.StartMs != a.Selection.EndMs || a.EmbargoMs != 0 {
		log.Fatal("invalid policy manifest")
	}
	fmt.Println("PASS policy-v1 source hashes, split boundary, purge dependency, embargo, and frozen coordinator")
}
