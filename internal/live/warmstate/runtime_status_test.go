package warmstate

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCurrentReadinessOverridesHistoricalGapWithoutPromotingIt(t *testing.T) {
	root := t.TempDir()
	capture := filepath.Join(root, "capture-state.json")
	runtime := filepath.Join(root, "runtime-status.json")
	now := time.UnixMilli(1_800_000_000_000)
	if err := os.WriteFile(capture, []byte(`{"RequiredWarmupMs":14400000,"AvailableContiguousHistoryMs":14398000,"WarmupReady":false,"LastUpdatedMs":1799000000000}`), 0600); err != nil {
		t.Fatal(err)
	}
	s := Status{Source: "traderui_live_runtime", Status: "WARMING_UP", RequiredMs: RequiredWarmupMs, AvailableMs: 12000, MissingMs: RequiredWarmupMs - 12000, InputConnected: true, CurrentCaptureStartMs: now.UnixMilli() - 13000, LastEventTimeMs: now.UnixMilli() - 1000, LastCheckpointMs: now.UnixMilli()}
	if err := PublishRuntime(runtime, s); err != nil {
		t.Fatal(err)
	}
	got := ReadCurrent(capture, runtime, now)
	if got.Status != "WARMING_UP" || got.AvailableMs != 12000 || got.Ready {
		t.Fatalf("historical gap joined: %+v", got)
	}
	got = ReadCurrent(capture, runtime, now.Add(3*time.Minute))
	if got.Status != "STALE" || got.Ready {
		t.Fatalf("stale runtime ready: %+v", got)
	}
	s.Status = "READY"
	s.Ready = true
	s.AvailableMs = RequiredWarmupMs
	s.MissingMs = 0
	s.InputConnected = false
	s.LastCheckpointMs = now.UnixMilli() + 1
	if err := PublishRuntime(runtime, s); err != nil {
		t.Fatal(err)
	}
	got = ReadCurrent(capture, runtime, now.Add(time.Millisecond))
	if got.Ready || got.Status != "STALE" {
		t.Fatalf("disconnected runtime promoted: %+v", got)
	}
}

func TestSourceTimestampLagDoesNotImplyReceiveStaleness(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "runtime-status.json")
	now := time.UnixMilli(1_800_000_000_000)
	s := Status{Source: "traderui_live_runtime", Status: "WARMING_UP", RequiredMs: RequiredWarmupMs, AvailableMs: 1000, MissingMs: RequiredWarmupMs - 1000, InputConnected: true, LastEventTimeMs: now.UnixMilli() - 15_000, LastReceiveTimeMs: now.UnixMilli() - 100, LastCheckpointMs: now.UnixMilli()}
	if err := PublishRuntime(path, s); err != nil {
		t.Fatal(err)
	}
	got := ReadCurrent(filepath.Join(root, "absent.json"), path, now)
	if got.Status != "WARMING_UP" || !got.InputConnected {
		t.Fatalf("source-time lag mistaken for disconnection: %+v", got)
	}
	s.LastReceiveTimeMs = now.UnixMilli() - 11_000
	if err := PublishRuntime(path, s); err != nil {
		t.Fatal(err)
	}
	got = ReadCurrent(filepath.Join(root, "absent.json"), path, now)
	if got.Status != "STALE" || got.InputConnected {
		t.Fatalf("stale receive accepted: %+v", got)
	}
}

func TestReadinessServiceStabilizationPersistReloadAndRestart(t *testing.T) {
	root := t.TempDir()
	runtime := filepath.Join(root, "runtime-status.json")
	capture := filepath.Join(root, "absent.json")
	t0 := time.UnixMilli(1_800_000_000_000)
	s := Status{Source: "traderui_live_runtime", Status: "STABILIZING", BootstrapState: "STABILIZING", RequiredMs: RequiredWarmupMs, AvailableMs: RequiredWarmupMs, MissingMs: 0, Ready: false, BootstrapReady: true, HandoffReady: true, LiveHealthReady: true, InputConnected: true, LastCheckpointMs: t0.UnixMilli(), LastEventTimeMs: t0.UnixMilli() - 1000, LastReceiveTimeMs: t0.UnixMilli() - 100, StabilizationStartedAtMs: t0.UnixMilli(), StabilizationDeadlineMs: t0.Add(5 * time.Minute).UnixMilli(), StabilizationTargetMs: (5 * time.Minute).Milliseconds()}
	s.LastCheckpointMs = t0.Add(4*time.Minute + 59*time.Second).UnixMilli()
	got, err := PublishRuntimeResolved(runtime, s, t0.Add(4*time.Minute+59*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "STABILIZING" || got.Ready {
		t.Fatalf("early transition: %+v", got)
	}
	s.LastCheckpointMs = t0.Add(5 * time.Minute).UnixMilli()
	got, err = PublishRuntimeResolved(runtime, s, t0.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "READY" || !got.Ready || got.StabilizationCompletedAtMs != t0.Add(5*time.Minute).UnixMilli() {
		t.Fatalf("ready transition: %+v", got)
	}
	reloaded := ReadCurrent(capture, runtime, t0.Add(time.Hour))
	if reloaded.Status != "READY" || !reloaded.Ready || reloaded.Source != "readiness_service" || reloaded.InputConnected {
		t.Fatalf("restart persistence: %+v", reloaded)
	}
}

func TestReadinessSeparatesStabilizationElapsedFromOperationalHealth(t *testing.T) {
	t0 := time.UnixMilli(1_800_000_000_000)
	base := Status{Source: "traderui_live_runtime", Status: "STABILIZING", BootstrapState: "STABILIZING", RequiredMs: RequiredWarmupMs, AvailableMs: RequiredWarmupMs, MissingMs: 0, BootstrapReady: true, HandoffReady: true, LiveHealthReady: true, InputConnected: true, StabilizationStartedAtMs: t0.UnixMilli(), StabilizationDeadlineMs: t0.Add(5 * time.Minute).UnixMilli(), StabilizationTargetMs: 300_000}

	early := ResolveReadiness(base, t0.Add(299999*time.Millisecond))
	if early.Status != "STABILIZING" || early.Ready || early.StabilizationReady || early.StabilizationElapsed {
		t.Fatalf("299999ms: %+v", early)
	}

	ready := ResolveReadiness(base, t0.Add(300000*time.Millisecond))
	if ready.Status != "READY" || !ready.Ready || !ready.StabilizationReady || !ready.StabilizationElapsed || ready.BootstrapState != "READY" {
		t.Fatalf("300000ms: %+v", ready)
	}

	staleInput := base
	staleInput.LiveHealthReady = false
	staleInput.BootstrapBlocker = "REQUIRED_SOURCE_STALE"
	staleInput.BlockerSource = "metrics_taker"
	blocked := ResolveReadiness(staleInput, t0.Add(371224*time.Millisecond))
	if blocked.Status != "BLOCKED" || blocked.Ready || !blocked.StabilizationReady || !blocked.StabilizationElapsed || blocked.BootstrapState != "READY" || blocked.BootstrapBlocker != "REQUIRED_SOURCE_STALE" || blocked.BlockerSource != "metrics_taker" {
		t.Fatalf("elapsed stale: %+v", blocked)
	}

	blocked.LiveHealthReady = true
	recovered := ResolveReadiness(blocked, t0.Add(371225*time.Millisecond))
	if recovered.Status != "READY" || !recovered.Ready || recovered.StabilizationStartedAtMs != t0.UnixMilli() || recovered.StabilizationMs != 371225 {
		t.Fatalf("blocked to ready restarted stabilization: %+v", recovered)
	}

	recovered.LiveHealthReady = false
	recovered.BootstrapBlocker = "REQUIRED_SOURCE_STALE"
	recovered.BlockerSource = "metrics_taker"
	blockedAgain := ResolveReadiness(recovered, t0.Add(371226*time.Millisecond))
	if blockedAgain.Status != "BLOCKED" || blockedAgain.Ready || blockedAgain.Status == "STABILIZING" {
		t.Fatalf("ready to stale: %+v", blockedAgain)
	}

	blockedAgain.LiveHealthReady = true
	recoveredAgain := ResolveReadiness(blockedAgain, t0.Add(371227*time.Millisecond))
	if recoveredAgain.Status != "READY" || !recoveredAgain.Ready || recoveredAgain.Status == "STABILIZING" || recoveredAgain.StabilizationStartedAtMs != t0.UnixMilli() {
		t.Fatalf("second recovery: %+v", recoveredAgain)
	}
}

func TestReadinessServiceRejectsStaleTraderUIDowngradeAndAllowsGap(t *testing.T) {
	root := t.TempDir()
	runtime := filepath.Join(root, "runtime-status.json")
	t0 := time.UnixMilli(1_800_000_000_000)
	ready := Status{Source: "traderui_live_runtime", Status: "STABILIZING", BootstrapState: "STABILIZING", RequiredMs: RequiredWarmupMs, AvailableMs: RequiredWarmupMs, MissingMs: 0, BootstrapReady: true, HandoffReady: true, LiveHealthReady: true, InputConnected: true, LastCheckpointMs: t0.Add(5 * time.Minute).UnixMilli(), BootstrapStartedAtMs: t0.UnixMilli(), BootstrapCompletedAtMs: t0.Add(time.Minute).UnixMilli(), StabilizationStartedAtMs: t0.UnixMilli(), StabilizationDeadlineMs: t0.Add(5 * time.Minute).UnixMilli()}
	persisted, err := PublishRuntimeResolved(runtime, ready, t0.Add(5*time.Minute))
	if err != nil || !persisted.Ready {
		t.Fatalf("ready setup: %+v %v", persisted, err)
	}
	old := ready
	old.LastCheckpointMs = t0.Add(5*time.Minute + time.Second).UnixMilli()
	old.LiveHealthReady = false
	persisted, err = PublishRuntimeResolved(runtime, old, t0.Add(5*time.Minute+time.Second))
	if err != nil || !persisted.Ready || persisted.Status != "READY" {
		t.Fatalf("stale overwrite accepted: %+v %v", persisted, err)
	}
	gap := persisted
	gap.Source = "traderui_live_runtime"
	gap.Status = "GAP"
	gap.BootstrapState = "BLOCKED"
	gap.Ready = false
	gap.InputConnected = false
	gap.LastCheckpointMs = t0.Add(5*time.Minute + 2*time.Second).UnixMilli()
	persisted, err = PublishRuntimeResolved(runtime, gap, t0.Add(5*time.Minute+2*time.Second))
	if err != nil || persisted.Status != "GAP" || persisted.Ready {
		t.Fatalf("real gap rejected: %+v %v", persisted, err)
	}
}

func TestExitedBeforeDeadlineDoesNotSelfPromote(t *testing.T) {
	root := t.TempDir()
	runtime := filepath.Join(root, "runtime-status.json")
	t0 := time.UnixMilli(1_800_000_000_000)
	s := Status{Source: "traderui_live_runtime", Status: "STABILIZING", BootstrapState: "STABILIZING", RequiredMs: RequiredWarmupMs, AvailableMs: RequiredWarmupMs, BootstrapReady: true, HandoffReady: true, LiveHealthReady: true, InputConnected: true, LastCheckpointMs: t0.UnixMilli(), StabilizationStartedAtMs: t0.UnixMilli(), StabilizationDeadlineMs: t0.Add(5 * time.Minute).UnixMilli()}
	if _, err := PublishRuntimeResolved(runtime, s, t0); err != nil {
		t.Fatal(err)
	}
	got := ReadCurrent(filepath.Join(root, "absent.json"), runtime, t0.Add(6*time.Minute))
	if got.Ready || got.Status == "READY" {
		t.Fatalf("exit fabricated completion: %+v", got)
	}
}

func TestRecoverCompletedBootstrapEvidenceWithoutNetwork(t *testing.T) {
	root := t.TempDir()
	runtime := filepath.Join(root, "runtime-status.json")
	reports := filepath.Join(root, "reports")
	if err := os.MkdirAll(reports, 0755); err != nil {
		t.Fatal(err)
	}
	t0 := time.UnixMilli(1_800_000_000_000)
	blocked := Status{Source: "traderui_live_runtime", Status: "BLOCKED", BootstrapState: "READY", BootstrapBlocker: "REQUIRED_SOURCE_STALE", RequiredMs: RequiredWarmupMs, AvailableMs: RequiredWarmupMs, MissingMs: 0, BootstrapReady: true, HandoffReady: true, StabilizationReady: true, LastCheckpointMs: t0.UnixMilli()}
	if err := PublishRuntime(runtime, blocked); err != nil {
		t.Fatal(err)
	}
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(reports, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("stage-g-handoff.json", `{"status":"PASS","complete":true,"unresolved_gaps":0}`)
	write("stage-i-stabilization.json", `{"status":"PASS","complete":true,"duration_ms":301000,"futures_gaps":0,"spot_gaps":0,"future_observation":0}`)
	write("BTCUSDT-live-bootstrap-v1-final.json", `{"status":"PASS","complete":true}`)
	got, recovered, err := RecoverCompletedBootstrap(runtime, reports, t0.Add(time.Minute))
	if err != nil || !recovered || !got.Ready || got.Status != "READY" {
		t.Fatalf("recovery: %+v %t %v", got, recovered, err)
	}
	reloaded := ReadCurrent(filepath.Join(root, "absent.json"), runtime, t0.Add(2*time.Hour))
	if !reloaded.Ready || reloaded.Status != "READY" {
		t.Fatalf("reloaded recovery: %+v", reloaded)
	}
	gap := got
	gap.Source = "traderui_live_runtime"
	gap.Status = "GAP"
	gap.Ready = false
	gap.BootstrapState = "BLOCKED"
	gap.LastCheckpointMs = t0.Add(3 * time.Hour).UnixMilli()
	if _, err = PublishRuntimeResolved(runtime, gap, t0.Add(3*time.Hour)); err != nil {
		t.Fatal(err)
	}
	_, recovered, err = RecoverCompletedBootstrap(runtime, reports, t0.Add(4*time.Hour))
	if err != nil || recovered {
		t.Fatalf("gap incorrectly recovered: %t %v", recovered, err)
	}
}
