package mainsplit

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	maintraining "binance_trader/internal/training/main"
)

type Manifest struct {
	SplitVersion          int                              `json:"split_version"`
	Symbol                string                           `json:"symbol"`
	SourceTrainingVersion int                              `json:"source_training_version"`
	FeatureVersion        int                              `json:"feature_version"`
	OutcomeVersion        int                              `json:"outcome_version"`
	BarrierVersion        int                              `json:"barrier_version"`
	TradeSpec             maintraining.TradeSpecManifest   `json:"trade_spec"`
	CostProfile           maintraining.CostProfileManifest `json:"cost_profile"`
	Definition            SplitDefinition                  `json:"definition"`
	PurgeRule             string                           `json:"purge_rule"`
	Partitions            map[Partition]*PartitionStats    `json:"partitions"`
	TotalCanonicalRows    int64                            `json:"total_canonical_rows"`
	OutsideRows           int64                            `json:"outside_rows"`
	ModelFeatureColumns   []string                         `json:"model_feature_columns"`
	FinalHoldoutSealed    bool                             `json:"final_holdout_sealed"`
	FinalHoldoutPolicy    string                           `json:"final_holdout_policy"`
	CostProfileWarning    string                           `json:"cost_profile_warning"`
}

func WriteManifest(path string, manifest Manifest) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	tmp := path + ".tmp"
	if err = os.WriteFile(tmp, b, 0644); err != nil {
		return err
	}
	if err = os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return os.Rename(tmp, path)
}

func ReadManifest(path string) (Manifest, error) {
	var manifest Manifest
	b, err := os.ReadFile(path)
	if err != nil {
		return manifest, err
	}
	err = json.Unmarshal(b, &manifest)
	return manifest, err
}
