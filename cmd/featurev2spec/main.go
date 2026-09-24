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
	Version                        int                 `json:"version"`
	Symbol                         string              `json:"symbol"`
	GeneratedAtUTC                 string              `json:"generated_at_utc"`
	V1ExistingFeatureCount         int                 `json:"v1_existing_feature_count"`
	V1FeatureRegistryHash          string              `json:"v1_feature_registry_hash"`
	V1ModelFeatureColumns          []string            `json:"v1_model_feature_columns"`
	ExternalAsOfPolicyVersion      int                 `json:"external_asof_policy_version"`
	ExternalAsOfPolicySHA256       string              `json:"external_asof_policy_sha256"`
	ExternalCanonicalFields        map[string][]string `json:"external_canonical_fields"`
	WindowSemantics                string              `json:"window_semantics"`
	MissingEncodingStatus          string              `json:"missing_encoding_status"`
	MissingEncodingRecommendations map[string]string   `json:"missing_encoding_recommendations"`
	ProposedFeatures               []specv2.Feature    `json:"proposed_features"`
	KeepCount                      int                 `json:"keep_count"`
	DropCount                      int                 `json:"drop_count"`
	DeferCount                     int                 `json:"defer_count"`
	ExpectedTotalModelFeatures     int                 `json:"expected_total_model_features"`
	LeakageAudit                   string              `json:"leakage_audit"`
	FeatureV2Materialization       string              `json:"feature_v2_materialization"`
	FinalHoldoutAccessed           bool                `json:"final_holdout_accessed"`
	Status                         string              `json:"status"`
}

func main() {
	output := flag.String("output", filepath.FromSlash("data/reports/feature/main/v2/BTCUSDT-feature-v2-spec-v1.json"), "report path")
	policy := flag.String("policy-report", filepath.FromSlash("data/reports/external/join-policy/v1/BTCUSDT-external-join-policy-v1.json"), "frozen policy report")
	flag.Parse()

	features := specv2.Proposed()
	if err := specv2.Validate(features, maintraining.ModelFeatureColumns); err != nil {
		fatal(err)
	}
	policyHash, err := fileSHA256(*policy)
	if err != nil {
		fatal(err)
	}
	keep := specv2.Count(features, specv2.Keep)
	r := report{
		Version: specv2.SpecVersion, Symbol: "BTCUSDT", GeneratedAtUTC: time.Now().UTC().Format(time.RFC3339),
		V1ExistingFeatureCount: len(maintraining.ModelFeatureColumns), V1FeatureRegistryHash: logistic.FeatureRegistryHash(maintraining.ModelFeatureColumns),
		V1ModelFeatureColumns:     append([]string(nil), maintraining.ModelFeatureColumns...),
		ExternalAsOfPolicyVersion: asof.PolicyVersion, ExternalAsOfPolicySHA256: policyHash,
		ExternalCanonicalFields: map[string][]string{
			"spot_1s":               {"timestamp_ms", "open", "high", "low", "close", "base_volume", "quote_volume", "agg_trade_count", "taker_buy_base_volume", "taker_sell_base_volume", "taker_buy_quote_volume", "taker_sell_quote_volume", "vwap", "has_trade", "first_agg_trade_id", "last_agg_trade_id"},
			"metrics_5m":            {"timestamp", "open_interest", "open_interest_value", "top_trader_account_long_short_ratio", "top_trader_position_long_short_ratio", "global_long_short_ratio", "taker_long_short_volume_ratio"},
			"mark_index_premium_1m": {"open_time", "open", "high", "low", "close", "volume", "close_time", "quote_volume", "count", "taker_buy_volume", "taker_buy_quote_volume", "ignore"},
			"funding":               {"calc_time", "funding_interval_hours", "last_funding_rate"},
		},
		WindowSemantics:       "모든 rolling window는 [t-lookback,t)이며 t 이후 observation과 current incomplete interval을 포함하지 않는다",
		MissingEncodingStatus: "NOT FROZEN; 구현 단계에서 검증 후 확정하며 silent zero conversion은 금지",
		MissingEncodingRecommendations: map[string]string{
			"spot_1s":               "exact previous-second/window 결손 시 sample exclusion",
			"metrics_5m":            "미관측은 sample exclusion; stale value + age/mask",
			"mark_index_premium_1m": "미관측은 sample exclusion; stale value + age/mask",
			"funding":               "미관측은 sample exclusion; stale value + funding_age_ms",
		},
		ProposedFeatures: features, KeepCount: keep, DropCount: specv2.Count(features, specv2.Drop), DeferCount: specv2.Count(features, specv2.Defer),
		ExpectedTotalModelFeatures: len(maintraining.ModelFeatureColumns) + keep,
		LeakageAudit:               "PASS", FeatureV2Materialization: "NOT RUN", FinalHoldoutAccessed: false, Status: "FROZEN",
	}
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		fatal(err)
	}
	b = append(b, '\n')
	if err = os.MkdirAll(filepath.Dir(*output), 0o755); err != nil {
		fatal(err)
	}
	tmp := *output + ".tmp"
	if err = os.WriteFile(tmp, b, 0o644); err != nil {
		fatal(err)
	}
	if err = os.Rename(tmp, *output); err != nil {
		fatal(err)
	}
	fmt.Printf("FEATURE V2 SPEC V1 = FROZEN keep=%d total=%d report=%s\n", keep, r.ExpectedTotalModelFeatures, *output)
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
