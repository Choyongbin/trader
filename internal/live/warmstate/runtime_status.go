package warmstate

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const RuntimeStatusPath = "data/live_state/BTCUSDT/v1/runtime-status.json"

type runtimeStatusRecord struct {
	Version             int    `json:"version"`
	FeatureRegistryHash string `json:"feature_registry_hash"`
	Status              Status `json:"status"`
}

const readinessRecordVersion = 3

// ResolveReadiness is the single STABILIZING -> READY state machine used by
// publishers, CLI readers, and UI readers. The injected clock keeps the
// transition deterministic in tests.
func ResolveReadiness(status Status, now time.Time) Status {
	status.Source = "readiness_service"
	status.StateUpdatedAtMs = now.UnixMilli()
	lifecycleStatus := status.Status == "STABILIZING" || status.Status == "READY" || status.Status == "BLOCKED"
	if lifecycleStatus && status.BootstrapReady && status.HandoffReady && status.StabilizationStartedAtMs > 0 {
		if status.StabilizationStartedAtMs > 0 {
			status.StabilizationMs = now.UnixMilli() - status.StabilizationStartedAtMs
			if status.StabilizationMs < 0 {
				status.StabilizationMs = 0
			}
		}
		status.StabilizationElapsed = status.StabilizationDeadlineMs > 0 && now.UnixMilli() >= status.StabilizationDeadlineMs
		status.StabilizationReady = status.StabilizationElapsed
		if !status.StabilizationElapsed {
			status.Status, status.BootstrapState, status.Ready = "STABILIZING", "STABILIZING", false
			status.BootstrapBlocker, status.BlockerSource = "", ""
			return status
		}
		status.BootstrapState = "READY"
		if status.StabilizationCompletedAtMs == 0 {
			status.StabilizationCompletedAtMs = now.UnixMilli()
		}
		health := status.LiveHealthReady && status.AvailableMs >= status.RequiredMs && status.MissingMs == 0 && status.FutureObservations == 0 && status.FuturesGaps == 0 && status.SpotGaps == 0
		if health {
			status.Status, status.Ready = "READY", true
			status.BootstrapBlocker, status.BlockerSource = "", ""
		} else {
			status.Status, status.Ready = "BLOCKED", false
			if status.BootstrapBlocker == "" {
				status.BootstrapBlocker = "REQUIRED_SOURCE_STALE"
			}
		}
	}
	return status
}

// ReadCurrent is the single readiness core used by CLI, UI, and the Auto gate.
// A stale runtime record is never allowed to make an old capture READY.
func ReadCurrent(capturePath, runtimePath string, now time.Time) Status {
	legacy := Read(capturePath, now)
	body, err := os.ReadFile(runtimePath)
	if err != nil {
		return legacy
	}
	var record runtimeStatusRecord
	if json.Unmarshal(body, &record) != nil || (record.Version != 2 && record.Version != readinessRecordVersion) || record.FeatureRegistryHash != FeatureRegistryHash || (record.Status.Source != "traderui_live_runtime" && record.Status.Source != "readiness_service") || record.Status.RequiredMs != RequiredWarmupMs || record.Status.LastCheckpointMs <= 0 {
		return legacy
	}
	s := record.Status
	if record.Version != readinessRecordVersion {
		s.Source = "readiness_service"
	}
	if now.UnixMilli()-s.LastCheckpointMs <= 10_000 || s.StabilizationReady {
		s = ResolveReadiness(s, now)
	}
	if s.LastCheckpointMs < legacy.LastCheckpointMs {
		return legacy
	}
	// A completed bootstrap readiness fact survives producer exit/restart. Live
	// entry safety still requires the current in-process InputConnected gate.
	if s.Status == "READY" && s.Ready && s.StabilizationReady && s.AvailableMs >= RequiredWarmupMs {
		if now.UnixMilli()-s.LastCheckpointMs > 10_000 {
			s.InputConnected = false
		}
		return s
	}
	if now.UnixMilli() < s.LastCheckpointMs || now.UnixMilli()-s.LastCheckpointMs > 10_000 || s.LastEventTimeMs > now.UnixMilli()+1000 || (s.LastReceiveTimeMs > 0 && now.UnixMilli()-s.LastReceiveTimeMs > 10_000) {
		s.Status = "STALE"
		s.Ready = false
		s.InputConnected = false
	} else if s.Ready && (s.Status != "READY" || !s.InputConnected || s.AvailableMs < RequiredWarmupMs || s.LastReceiveTimeMs <= 0 || now.UnixMilli()-s.LastReceiveTimeMs > 10_000) {
		s.Status = "STALE"
		s.Ready = false
	}
	return s
}

// PublishRuntime writes a small current-state checkpoint without replacing the
// historical warmup artifact.
func PublishRuntime(path string, status Status) error {
	_, err := PublishRuntimeResolved(path, status, time.UnixMilli(status.LastCheckpointMs))
	return err
}

func PublishRuntimeResolved(path string, status Status, now time.Time) (Status, error) {
	if (status.Source != "traderui_live_runtime" && status.Source != "readiness_service") || status.RequiredMs != RequiredWarmupMs || status.LastCheckpointMs <= 0 {
		return Status{}, fmt.Errorf("invalid runtime readiness checkpoint")
	}
	incoming := status
	status = ResolveReadiness(status, now)
	// An old local STABILIZING writer cannot downgrade a completed lifecycle.
	if body, readErr := os.ReadFile(path); readErr == nil {
		var old runtimeStatusRecord
		if json.Unmarshal(body, &old) == nil && old.FeatureRegistryHash == FeatureRegistryHash && old.Status.Status == "READY" && old.Status.Ready && (incoming.Status == "STABILIZING" || incoming.BootstrapState == "STABILIZING") && incoming.BootstrapStartedAtMs <= old.Status.BootstrapCompletedAtMs {
			old.Status.Source = "readiness_service"
			return old.Status, nil
		}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return Status{}, err
	}
	body, err := json.Marshal(runtimeStatusRecord{Version: readinessRecordVersion, FeatureRegistryHash: FeatureRegistryHash, Status: status})
	if err != nil {
		return Status{}, err
	}
	f, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*.tmp")
	if err != nil {
		return Status{}, err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if _, err = f.Write(body); err != nil {
		f.Close()
		return Status{}, err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return Status{}, err
	}
	if err = f.Close(); err != nil {
		return Status{}, err
	}
	if err = replaceRuntimeAtomic(tmp, path); err != nil {
		return Status{}, err
	}
	check, err := os.ReadFile(path)
	if err != nil {
		return Status{}, err
	}
	var saved runtimeStatusRecord
	if json.Unmarshal(check, &saved) != nil || saved.Version != readinessRecordVersion || saved.Status.Status != status.Status || saved.Status.Ready != status.Ready {
		return Status{}, fmt.Errorf("readiness reread verification failed")
	}
	return saved.Status, nil
}

// RecoverCompletedBootstrap promotes only a stale producer record backed by
// completed handoff, stabilization, and final OPS-2 evidence. GAP/error states
// are never recovered.
func RecoverCompletedBootstrap(runtimePath, reportRoot string, now time.Time) (Status, bool, error) {
	body, err := os.ReadFile(runtimePath)
	if err != nil {
		return Status{}, false, nil
	}
	var record runtimeStatusRecord
	if json.Unmarshal(body, &record) != nil || record.FeatureRegistryHash != FeatureRegistryHash {
		return Status{}, false, fmt.Errorf("invalid runtime readiness artifact")
	}
	s := record.Status
	if s.Status == "READY" && s.Ready {
		return s, false, nil
	}
	if now.UnixMilli()-s.LastCheckpointMs <= 10_000 {
		return s, false, nil
	}
	if s.Status != "STABILIZING" && !(s.Status == "BLOCKED" && s.BootstrapBlocker == "REQUIRED_SOURCE_STALE") {
		return s, false, nil
	}
	type gate struct {
		Status            string `json:"status"`
		Complete          bool   `json:"complete"`
		DurationMs        int64  `json:"duration_ms"`
		FuturesGaps       int    `json:"futures_gaps"`
		SpotGaps          int    `json:"spot_gaps"`
		FutureObservation int64  `json:"future_observation"`
		UnresolvedGaps    int    `json:"unresolved_gaps"`
	}
	read := func(name string) (gate, os.FileInfo, error) {
		p := filepath.Join(reportRoot, name)
		b, e := os.ReadFile(p)
		if e != nil {
			return gate{}, nil, e
		}
		var g gate
		if json.Unmarshal(b, &g) != nil {
			return gate{}, nil, fmt.Errorf("invalid readiness evidence %s", name)
		}
		info, e := os.Stat(p)
		return g, info, e
	}
	handoff, _, err := read("stage-g-handoff.json")
	if err != nil {
		return s, false, err
	}
	stabilization, stInfo, err := read("stage-i-stabilization.json")
	if err != nil {
		return s, false, err
	}
	final, _, err := read("BTCUSDT-live-bootstrap-v1-final.json")
	if err != nil {
		return s, false, err
	}
	if handoff.Status != "PASS" || !handoff.Complete || handoff.UnresolvedGaps != 0 || stabilization.Status != "PASS" || !stabilization.Complete || stabilization.DurationMs < 300_000 || stabilization.FuturesGaps != 0 || stabilization.SpotGaps != 0 || stabilization.FutureObservation != 0 || final.Status != "PASS" || !final.Complete {
		return s, false, nil
	}
	completed := stInfo.ModTime().UnixMilli()
	started := completed - stabilization.DurationMs
	s.Source = "readiness_service"
	s.Status = "READY"
	s.BootstrapState = "READY"
	s.Ready = true
	s.BootstrapReady = true
	s.HandoffReady = true
	s.StabilizationReady = true
	s.LiveHealthReady = true
	s.InputConnected = false
	s.BootstrapBlocker = ""
	s.StabilizationStartedAtMs = started
	s.StabilizationDeadlineMs = started + 300_000
	s.StabilizationCompletedAtMs = completed
	s.StabilizationMs = stabilization.DurationMs
	s.LastCheckpointMs = now.UnixMilli()
	s.StateUpdatedAtMs = now.UnixMilli()
	saved, err := PublishRuntimeResolved(runtimePath, s, now)
	if err != nil {
		return Status{}, false, err
	}
	return saved, true, nil
}
