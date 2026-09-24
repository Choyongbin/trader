package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type testMonthCheckpoint struct {
	Month             string           `json:"month"`
	Partition         string           `json:"partition"`
	RegistrySHA256    string           `json:"registry_sha256"`
	FeatureCount      int              `json:"feature_count"`
	CandidateCount    int64            `json:"candidate_count"`
	RowCount          int64            `json:"row_count"`
	Excluded          int64            `json:"excluded"`
	Reasons           map[string]int64 `json:"exclusion_reasons"`
	FilePath          string           `json:"file_path"`
	FileSize          int64            `json:"file_size"`
	NaN               int64            `json:"nan_count"`
	Inf               int64            `json:"inf_count"`
	FutureObservation int64            `json:"future_observation_count"`
	Complete          bool             `json:"complete"`
}

type fullFeatureManifest struct {
	FeatureCount           int    `json:"feature_count"`
	RegistrySHA256         string `json:"registry_sha256"`
	NaN                    int64  `json:"nan_count"`
	Inf                    int64  `json:"inf_count"`
	FutureObservationCount int64  `json:"future_observation_count"`
	V1Regression           struct {
		ComparedRows int64   `json:"compared_rows"`
		Mismatches   int64   `json:"mismatches"`
		MaxDiff      float64 `json:"max_absolute_difference"`
	} `json:"v1_regression"`
	Complete         bool   `json:"complete"`
	Scope            string `json:"scope"`
	TestMaterialized bool   `json:"test_materialized"`
}

type stageTestMaterialization struct {
	Version                  int                   `json:"version"`
	Stage                    string                `json:"stage"`
	Status                   string                `json:"status"`
	PolicyPath               string                `json:"policy_path"`
	PolicyHash               string                `json:"policy_sha256"`
	Months                   []testMonthCheckpoint `json:"months"`
	CandidateRows            int64                 `json:"candidate_rows"`
	FeatureRows              int64                 `json:"feature_rows"`
	Excluded                 int64                 `json:"excluded"`
	ExclusionReasons         map[string]int64      `json:"exclusion_reasons"`
	ParquetBytes             int64                 `json:"parquet_bytes"`
	NaN                      int64                 `json:"nan_count"`
	Inf                      int64                 `json:"inf_count"`
	FutureObservation        int64                 `json:"future_observation_count"`
	V1RegressionComparedRows int64                 `json:"v1_regression_compared_rows"`
	V1RegressionMismatch     int64                 `json:"v1_regression_mismatch"`
	V1RegressionMaxDiff      float64               `json:"v1_regression_max_absolute_difference"`
	TmpArtifacts             int                   `json:"tmp_artifacts"`
	FinalHoldoutAccessed     bool                  `json:"final_holdout_accessed"`
	Complete                 bool                  `json:"complete"`
}

func publishTestMaterialization(output string) error {
	if b, err := os.ReadFile(output); err == nil {
		var existing stageTestMaterialization
		if json.Unmarshal(b, &existing) == nil && existing.Complete && existing.Status == "PASS" {
			fmt.Println("TEST MATERIALIZATION RESUME PASS complete=true")
			return nil
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	policyPath := filepath.FromSlash("data/reports/model/main/v2/phase9c-cde/stage-d-policy-freeze.json")
	policyBytes, err := os.ReadFile(policyPath)
	if err != nil {
		return err
	}
	var policy stageDReport
	if err = json.Unmarshal(policyBytes, &policy); err != nil || !policy.Complete || !policy.PolicyFrozen || policy.TestAccessedBeforeFreeze {
		return fmt.Errorf("policy is not safely frozen")
	}
	var manifest fullFeatureManifest
	manifestPath := filepath.FromSlash("data/manifests/features/main/v2/BTCUSDT-dataset-v2.json")
	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(manifestBytes, &manifest); err != nil {
		return err
	}
	if !manifest.Complete || manifest.Scope != "FULL" || !manifest.TestMaterialized || manifest.FeatureCount != featureCount || manifest.RegistrySHA256 != registryHash || manifest.NaN != 0 || manifest.Inf != 0 || manifest.FutureObservationCount != 0 || manifest.V1Regression.Mismatches != 0 {
		return fmt.Errorf("invalid full Feature V2 manifest")
	}
	report := stageTestMaterialization{Version: 1, Stage: "TEST-M", Status: "PASS", PolicyPath: policyPath,
		PolicyHash: policy.PolicyHash, ExclusionReasons: map[string]int64{}, V1RegressionComparedRows: manifest.V1Regression.ComparedRows,
		V1RegressionMismatch: manifest.V1Regression.Mismatches, V1RegressionMaxDiff: manifest.V1Regression.MaxDiff}
	for month := 1; month <= 6; month++ {
		name := fmt.Sprintf("2025-%02d", month)
		checkpointPath := filepath.Join("data", "manifests", "features", "main", "v2", "BTCUSDT", fmt.Sprintf("BTCUSDT-main-features-v2-%s.json", name))
		b, err := os.ReadFile(checkpointPath)
		if err != nil {
			return err
		}
		var checkpoint testMonthCheckpoint
		if err = json.Unmarshal(b, &checkpoint); err != nil {
			return err
		}
		if !checkpoint.Complete || checkpoint.Month != name || checkpoint.Partition != "TEST" || checkpoint.FeatureCount != featureCount || checkpoint.RegistrySHA256 != registryHash || checkpoint.NaN != 0 || checkpoint.Inf != 0 || checkpoint.FutureObservation != 0 {
			return fmt.Errorf("invalid TEST checkpoint %s", name)
		}
		stat, err := os.Stat(checkpoint.FilePath)
		if err != nil || stat.Size() != checkpoint.FileSize {
			return fmt.Errorf("TEST parquet size mismatch %s", name)
		}
		report.Months = append(report.Months, checkpoint)
		report.CandidateRows += checkpoint.CandidateCount
		report.FeatureRows += checkpoint.RowCount
		report.Excluded += checkpoint.Excluded
		report.ParquetBytes += checkpoint.FileSize
		for reason, count := range checkpoint.Reasons {
			report.ExclusionReasons[reason] += count
		}
	}
	tmp, err := filepath.Glob(filepath.FromSlash("data/features/main/v2/BTCUSDT/2025/*.tmp"))
	if err != nil {
		return err
	}
	report.TmpArtifacts = len(tmp)
	if report.TmpArtifacts != 0 {
		return fmt.Errorf("TEST tmp artifacts=%d", report.TmpArtifacts)
	}
	report.Complete = true
	if err = writeJSON(output, report); err != nil {
		return err
	}
	fmt.Printf("TEST FEATURE V2 PASS months=6 rows=%d excluded=%d\n", report.FeatureRows, report.Excluded)
	return nil
}
