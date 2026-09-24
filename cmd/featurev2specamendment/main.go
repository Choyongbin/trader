package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"binance_trader/internal/external/asof"
	"binance_trader/internal/feature/specv2"
	"binance_trader/internal/model/logistic"
	maintraining "binance_trader/internal/training/main"
)

type report struct {
	SpecVersion                    int              `json:"spec_version"`
	AmendmentVersion               int              `json:"amendment_version"`
	Identity                       string           `json:"identity"`
	Symbol                         string           `json:"symbol"`
	GeneratedAtUTC                 string           `json:"generated_at_utc"`
	BaseSpecPath                   string           `json:"base_spec_path"`
	BaseSpecSHA256                 string           `json:"base_spec_sha256"`
	V1FeatureRegistryHash          string           `json:"v1_feature_registry_hash"`
	ExternalAsOfPolicyVersion      int              `json:"external_asof_policy_version"`
	ExternalAsOfPolicySHA256       string           `json:"external_asof_policy_sha256"`
	AmendedFeatures                []string         `json:"amended_features"`
	AmendmentReason                string           `json:"amendment_reason"`
	ProposedFeatures               []specv2.Feature `json:"proposed_features"`
	KeepCount                      int              `json:"keep_count"`
	DropCount                      int              `json:"drop_count"`
	DeferCount                     int              `json:"defer_count"`
	ExpectedTotalModelFeatureCount int              `json:"expected_total_model_feature_count"`
	ReplacementFeaturesAdded       int              `json:"replacement_features_added"`
	FeatureV2Materialization       string           `json:"feature_v2_materialization"`
	FinalHoldoutAccessed           bool             `json:"final_holdout_accessed"`
	Status                         string           `json:"status"`
}

func main() {
	output := flag.String("output", filepath.FromSlash("data/reports/feature/main/v2/BTCUSDT-feature-v2-spec-v1-amendment-1.json"), "report path")
	flag.Parse()
	basePath := filepath.FromSlash("data/reports/feature/main/v2/BTCUSDT-feature-v2-spec-v1.json")
	policyPath := filepath.FromSlash("data/reports/external/join-policy/v1/BTCUSDT-external-join-policy-v1.json")
	baseHash, err := fileSHA256(basePath)
	if err != nil {
		fatal(err)
	}
	policyHash, err := fileSHA256(policyPath)
	if err != nil {
		fatal(err)
	}
	features := specv2.Amended()
	if err := specv2.Validate(features, maintraining.ModelFeatureColumns); err != nil {
		fatal(err)
	}
	keep := specv2.Count(features, specv2.Keep)
	r := report{
		SpecVersion: specv2.SpecVersion, AmendmentVersion: specv2.AmendmentVersion,
		Identity: "FEATURE V2 SPEC V1 AMENDMENT 1", Symbol: "BTCUSDT", GeneratedAtUTC: time.Now().UTC().Format(time.RFC3339),
		BaseSpecPath: basePath, BaseSpecSHA256: baseHash,
		V1FeatureRegistryHash:     logistic.FeatureRegistryHash(maintraining.ModelFeatureColumns),
		ExternalAsOfPolicyVersion: asof.PolicyVersion, ExternalAsOfPolicySHA256: policyHash,
		AmendedFeatures:  []string{"metrics_fresh", "kline_fresh"},
		AmendmentReason:  "CONSTANT_UNDER_STRICT_ELIGIBILITY",
		ProposedFeatures: features, KeepCount: keep, DropCount: specv2.Count(features, specv2.Drop), DeferCount: specv2.Count(features, specv2.Defer),
		ExpectedTotalModelFeatureCount: len(maintraining.ModelFeatureColumns) + keep,
		ReplacementFeaturesAdded:       0, FeatureV2Materialization: "NOT RUN", FinalHoldoutAccessed: false, Status: "FROZEN",
	}
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		fatal(err)
	}
	b = append(b, '\n')
	if err := os.MkdirAll(filepath.Dir(*output), 0o755); err != nil {
		fatal(err)
	}
	tmp := *output + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		fatal(err)
	}
	if err := os.Rename(tmp, *output); err != nil {
		fatal(err)
	}
	fmt.Printf("FEATURE V2 SPEC V1 AMENDMENT 1 = FROZEN keep=%d total=%d\n", keep, r.ExpectedTotalModelFeatureCount)
}

func fileSHA256(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:]), nil
}

func fatal(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
