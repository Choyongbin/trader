package mainbarrier

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

type ManifestV2 struct {
	BarrierVersion              int      `json:"barrier_version"`
	Symbol                      string   `json:"symbol"`
	Month                       string   `json:"month"`
	PartitionKey                string   `json:"partition_key"`
	PartitionTimezone           string   `json:"partition_timezone"`
	Source                      string   `json:"source"`
	TriggerPriceSource          string   `json:"trigger_price_source"`
	EntryPriceSemantics         string   `json:"entry_price_semantics"`
	BarrierSemantics            string   `json:"barrier_semantics"`
	HorizonOrigin               string   `json:"horizon_origin"`
	FundingIncluded             bool     `json:"funding_included"`
	EntryDelayMs                int64    `json:"entry_delay_ms"`
	MaxEntryWaitMs              int64    `json:"max_entry_wait_ms"`
	MaxExitReferenceWaitMs      int64    `json:"max_exit_reference_wait_ms"`
	MaxHorizonSeconds           int64    `json:"max_horizon_seconds"`
	BarrierGridBps              []int    `json:"barrier_grid_bps"`
	TimeoutHorizonsSeconds      []int    `json:"timeout_horizons_seconds"`
	RowCount                    int64    `json:"row_count"`
	StartDecisionTimestampMs    int64    `json:"start_decision_timestamp_ms"`
	EndDecisionTimestampMs      int64    `json:"end_decision_timestamp_ms"`
	EntryReferenceAvailable     int64    `json:"entry_reference_available"`
	EntryReferenceUnavailable   int64    `json:"entry_reference_unavailable"`
	TimeoutReferenceAvailable   [7]int64 `json:"timeout_reference_available"`
	TimeoutReferenceUnavailable [7]int64 `json:"timeout_reference_unavailable"`
	SourceFiles                 []string `json:"source_files"`
	SourceTrades                int64    `json:"source_trades"`
	OutputSizeBytes             int64    `json:"output_size_bytes"`
	ElapsedMs                   int64    `json:"elapsed_ms"`
}

func WriteManifestV2(path string, m ManifestV2) error {
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
