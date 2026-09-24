package mainoutcome

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

type Manifest struct {
	Symbol                       string `json:"symbol"`
	Month                        string `json:"month"`
	OutcomeVersion               int    `json:"outcome_version"`
	SourceSecondBarSchemaVersion int    `json:"source_secondbar_schema_version"`
	DecisionIntervalSeconds      int    `json:"decision_interval_seconds"`
	MaxHorizonSeconds            int    `json:"max_horizon_seconds"`
	InputBars                    int64  `json:"input_bars"`
	DecisionCandidates           int64  `json:"decision_candidates"`
	RowCount                     int64  `json:"row_count"`
	StartDecisionTimestampMs     int64  `json:"start_decision_timestamp_ms"`
	EndDecisionTimestampMs       int64  `json:"end_decision_timestamp_ms"`
	TailSkipped                  int64  `json:"tail_skipped"`
	NaN                          int64  `json:"nan"`
	Inf                          int64  `json:"inf"`
	Invalid                      int64  `json:"invalid"`
	OutputSizeBytes              int64  `json:"output_size_bytes"`
	ElapsedMs                    int64  `json:"elapsed_ms"`
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
