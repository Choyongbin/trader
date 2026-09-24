package mainbarrier

import (
	"encoding/json"
	"os"
	"path/filepath"
)

type Manifest struct {
	BarrierVersion           int      `json:"barrier_version"`
	Symbol                   string   `json:"symbol"`
	Month                    string   `json:"month"`
	PartitionKey             string   `json:"partition_key"`
	PartitionTimezone        string   `json:"partition_timezone"`
	Source                   string   `json:"source"`
	EntryDelayMs             int64    `json:"entry_delay_ms"`
	MaxEntryWaitMs           int64    `json:"max_entry_wait_ms"`
	MaxHorizonSeconds        int64    `json:"max_horizon_seconds"`
	BarrierGridBps           []int    `json:"barrier_grid_bps"`
	RowCount                 int64    `json:"row_count"`
	StartDecisionTimestampMs int64    `json:"start_decision_timestamp_ms"`
	EndDecisionTimestampMs   int64    `json:"end_decision_timestamp_ms"`
	EntryAvailable           int64    `json:"entry_available"`
	EntryUnavailable         int64    `json:"entry_unavailable"`
	SourceFiles              []string `json:"source_files"`
	SourceTrades             int64    `json:"source_trades"`
	OutputSizeBytes          int64    `json:"output_size_bytes"`
	ElapsedMs                int64    `json:"elapsed_ms"`
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
	return os.WriteFile(path, b, 0644)
}
