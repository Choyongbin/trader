package featurev2

import (
	"fmt"

	"binance_trader/internal/feature/specv2"
	maintraining "binance_trader/internal/training/main"
)

const (
	V1ModelFeatureCount  = 80
	NewModelFeatureCount = 48
	ModelFeatureCountV2  = 128
)

var ModelFeatureColumnsV2 = modelFeatureColumnsV2()

func modelFeatureColumnsV2() []string {
	result := append([]string(nil), maintraining.ModelFeatureColumns...)
	for _, feature := range specv2.Amended() {
		if feature.Decision == specv2.Keep {
			result = append(result, feature.FeatureName)
		}
	}
	if err := ValidateRegistry(result); err != nil {
		panic(err)
	}
	return result
}

func ValidateRegistry(names []string) error {
	if len(maintraining.ModelFeatureColumns) != V1ModelFeatureCount || len(names) != ModelFeatureCountV2 {
		return fmt.Errorf("feature count mismatch: v1=%d v2=%d", len(maintraining.ModelFeatureColumns), len(names))
	}
	seen := make(map[string]bool, len(names))
	for i, name := range names {
		if name == "" || seen[name] {
			return fmt.Errorf("duplicate or empty feature name %q", name)
		}
		seen[name] = true
		if i < V1ModelFeatureCount && name != maintraining.ModelFeatureColumns[i] {
			return fmt.Errorf("V1 registry mismatch at %d", i)
		}
	}
	if seen["metrics_fresh"] || seen["kline_fresh"] {
		return fmt.Errorf("constant fresh mask present")
	}
	return nil
}
