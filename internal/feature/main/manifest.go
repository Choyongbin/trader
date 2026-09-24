package mainfeature

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

type Manifest struct {
	Symbol                   string `json:"symbol"`
	Month                    string `json:"month"`
	PartitionKey             string `json:"partition_key"`
	PartitionTimezone        string `json:"partition_timezone"`
	FeatureVersion           int    `json:"feature_version"`
	SourceSchemaVersion      int    `json:"source_schema_version"`
	EmitIntervalSeconds      int    `json:"emit_interval_seconds"`
	RowCount                 int64  `json:"row_count"`
	StartDecisionTimestampMs int64  `json:"start_decision_timestamp_ms"`
	EndDecisionTimestampMs   int64  `json:"end_decision_timestamp_ms"`
	InputBars                int64  `json:"input_bars"`
	SyntheticBars            int64  `json:"synthetic_bars"`
	WarmupSkipped            int64  `json:"warmup_skipped"`
	InvalidFeatures          int64  `json:"invalid_features"`
	OutputSizeBytes          int64  `json:"output_size_bytes"`
	ElapsedMs                int64  `json:"elapsed_ms"`
	HistoryContextStart      string `json:"history_context_start"`
	ContinuousState          bool   `json:"continuous_state"`
	GlobalWarmup             bool   `json:"global_warmup"`
	BuildScope               string `json:"build_scope"`
}

func WriteManifest(path string, m Manifest) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0644); err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}
