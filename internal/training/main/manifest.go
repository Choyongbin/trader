package maintraining

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

type TradeSpecManifest struct {
	TPBps          int `json:"tp_bps"`
	SLBps          int `json:"sl_bps"`
	HorizonSeconds int `json:"horizon_seconds"`
}
type CostProfileManifest struct {
	Name               string  `json:"name"`
	EntryFeeRate       float64 `json:"entry_fee_rate"`
	TPExitFeeRate      float64 `json:"tp_exit_fee_rate"`
	SLExitFeeRate      float64 `json:"sl_exit_fee_rate"`
	TimeoutExitFeeRate float64 `json:"timeout_exit_fee_rate"`
	EntrySlippageBps   float64 `json:"entry_slippage_bps"`
	TPExitSlippageBps  float64 `json:"tp_exit_slippage_bps"`
	SLExitSlippageBps  float64 `json:"sl_exit_slippage_bps"`
	TimeoutSlippageBps float64 `json:"timeout_slippage_bps"`
	FundingIncluded    bool    `json:"funding_included"`
}
type Manifest struct {
	TrainingVersion          int                 `json:"training_version"`
	Symbol                   string              `json:"symbol"`
	Month                    string              `json:"month"`
	PartitionKey             string              `json:"partition_key"`
	PartitionTimezone        string              `json:"partition_timezone"`
	FeatureVersion           int                 `json:"feature_version"`
	OutcomeVersion           int                 `json:"outcome_version"`
	BarrierVersion           int                 `json:"barrier_version"`
	TradeSpec                TradeSpecManifest   `json:"trade_spec"`
	CostProfile              CostProfileManifest `json:"cost_profile"`
	RowCount                 int64               `json:"row_count"`
	LongLabelValid           int64               `json:"long_label_valid"`
	LongLabelInvalid         int64               `json:"long_label_invalid"`
	ShortLabelValid          int64               `json:"short_label_valid"`
	ShortLabelInvalid        int64               `json:"short_label_invalid"`
	LongNetProfitable        int64               `json:"long_net_profitable"`
	LongNetUnprofitable      int64               `json:"long_net_unprofitable"`
	ShortNetProfitable       int64               `json:"short_net_profitable"`
	ShortNetUnprofitable     int64               `json:"short_net_unprofitable"`
	StartDecisionTimestampMs int64               `json:"start_decision_timestamp_ms"`
	EndDecisionTimestampMs   int64               `json:"end_decision_timestamp_ms"`
	ModelFeatureColumns      []string            `json:"model_feature_columns"`
	MetadataColumns          []string            `json:"metadata_columns"`
	TargetColumns            []string            `json:"target_columns"`
	AnalysisColumns          []string            `json:"analysis_columns"`
	OutputSizeBytes          int64               `json:"output_size_bytes"`
	ElapsedMs                int64               `json:"elapsed_ms"`
	RowsPerSecond            float64             `json:"rows_per_second"`
}

func WriteManifest(path string, m Manifest) error {
	if e := os.MkdirAll(filepath.Dir(path), 0755); e != nil {
		return e
	}
	b, e := json.MarshalIndent(m, "", "  ")
	if e != nil {
		return e
	}
	b = append(b, '\n')
	tmp := path + ".tmp"
	if e = os.WriteFile(tmp, b, 0644); e != nil {
		return e
	}
	if e = os.Remove(path); e != nil && !errors.Is(e, os.ErrNotExist) {
		return e
	}
	return os.Rename(tmp, path)
}
func ReadManifest(path string) (Manifest, error) {
	var m Manifest
	b, e := os.ReadFile(path)
	if e != nil {
		return m, e
	}
	e = json.Unmarshal(b, &m)
	return m, e
}
