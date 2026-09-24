package warmstate

import (
	"encoding/json"
	"os"
	"time"
)

const RequiredWarmupMs int64 = 14_400_000

// Status is the shared CLI/UI interpretation of a persisted warmup checkpoint.
// A historical READY checkpoint is never evidence of current live continuity.
type Status struct {
	Status                     string         `json:"status"`
	RequiredMs                 int64          `json:"required_ms"`
	AvailableMs                int64          `json:"available_ms"`
	MissingMs                  int64          `json:"missing_ms"`
	ProgressPercent            float64        `json:"progress_percent"`
	Ready                      bool           `json:"ready"`
	LastCheckpointMs           int64          `json:"last_checkpoint_ms"`
	FuturesBars                int            `json:"futures_bars"`
	SpotBars                   int            `json:"spot_bars"`
	FuturesGaps                int            `json:"futures_gaps"`
	SpotGaps                   int            `json:"spot_gaps"`
	SessionCount               int            `json:"session_count"`
	ExternalObservations       int            `json:"external_observations"`
	ExternalSourceCounts       map[string]int `json:"external_source_counts,omitempty"`
	InputConnected             bool           `json:"input_connected"`
	CurrentCaptureStartMs      int64          `json:"current_capture_start_ms"`
	LastEventTimeMs            int64          `json:"last_event_time_ms"`
	LastReceiveTimeMs          int64          `json:"last_receive_time_ms"`
	FutureObservations         int64          `json:"future_observations"`
	Source                     string         `json:"source,omitempty"`
	BootstrapState             string         `json:"bootstrap_state,omitempty"`
	BootstrapReady             bool           `json:"bootstrap_ready"`
	HandoffReady               bool           `json:"handoff_ready"`
	StabilizationReady         bool           `json:"stabilization_ready"`
	StabilizationElapsed       bool           `json:"stabilization_elapsed"`
	StabilizationMs            int64          `json:"stabilization_ms,omitempty"`
	StabilizationTargetMs      int64          `json:"stabilization_target_ms,omitempty"`
	BootstrapBlocker           string         `json:"bootstrap_blocker,omitempty"`
	BlockerSource              string         `json:"blocker_source,omitempty"`
	FuturesHistoryMs           int64          `json:"futures_history_ms,omitempty"`
	SpotHistoryMs              int64          `json:"spot_history_ms,omitempty"`
	BootstrapStartedAtMs       int64          `json:"bootstrap_started_at_ms,omitempty"`
	BootstrapCompletedAtMs     int64          `json:"bootstrap_completed_at_ms,omitempty"`
	HandoffStartedAtMs         int64          `json:"handoff_started_at_ms,omitempty"`
	HandoffCompletedAtMs       int64          `json:"handoff_completed_at_ms,omitempty"`
	StabilizationStartedAtMs   int64          `json:"stabilization_started_at_ms,omitempty"`
	StabilizationDeadlineMs    int64          `json:"stabilization_deadline_ms,omitempty"`
	StabilizationCompletedAtMs int64          `json:"stabilization_completed_at_ms,omitempty"`
	StateUpdatedAtMs           int64          `json:"state_updated_at_ms,omitempty"`
	LiveHealthReady            bool           `json:"live_health_ready"`
}

func Read(path string, now time.Time) Status {
	result := Status{Status: "IDLE", RequiredMs: RequiredWarmupMs, MissingMs: RequiredWarmupMs}
	body, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return result
	}
	if err != nil {
		result.Status = "ERROR"
		return result
	}
	var raw struct {
		RequiredWarmupMs, AvailableContiguousHistoryMs, LastUpdatedMs              int64
		WarmupReady                                                                bool
		Sessions                                                                   []json.RawMessage
		FuturesBars, SpotBars, FuturesGapCount, SpotGapCount, ExternalObservations int
		ExternalSourceCounts                                                       map[string]int
	}
	if json.Unmarshal(body, &raw) != nil || raw.RequiredWarmupMs != RequiredWarmupMs || raw.LastUpdatedMs <= 0 {
		result.Status = "ERROR"
		return result
	}
	result.LastCheckpointMs = raw.LastUpdatedMs
	result.AvailableMs = raw.AvailableContiguousHistoryMs
	result.MissingMs = RequiredWarmupMs - result.AvailableMs
	if result.MissingMs < 0 {
		result.MissingMs = 0
	}
	result.ProgressPercent = 100 * float64(result.AvailableMs) / float64(RequiredWarmupMs)
	if result.ProgressPercent > 100 {
		result.ProgressPercent = 100
	}
	result.FuturesBars, result.SpotBars = raw.FuturesBars, raw.SpotBars
	result.FuturesGaps, result.SpotGaps = raw.FuturesGapCount, raw.SpotGapCount
	result.ExternalObservations, result.ExternalSourceCounts = raw.ExternalObservations, raw.ExternalSourceCounts
	result.SessionCount = len(raw.Sessions)
	if now.UnixMilli()-raw.LastUpdatedMs > 120_000 || now.UnixMilli() < raw.LastUpdatedMs {
		result.Status = "GAP"
		return result
	}
	if raw.WarmupReady && result.MissingMs == 0 {
		result.Status, result.Ready = "READY", true
	} else {
		result.Status = "WARMING_UP"
	}
	return result
}
