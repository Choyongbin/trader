// verify_next.go is a single-file, read-only-by-default verification runner.
// Modes: analyze (offline, default), snapshot (targeted PUBLIC_ONLY integration),
//
//	full (static Go tests + targeted PUBLIC_ONLY integration).
//
// Usage from the repository root: go run .\scripts\verify_next.go -mode analyze
//
//	go run .\scripts\verify_next.go -mode snapshot
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const (
	featureHash      = "a37306b80ecbf701103ab691445d07a39624ce995d84f8206d6314dae3045bef"
	entryHash        = "4fae120d9d54a5732dbf2009950beba69773bb0a28a7c3915b616e295cd3dd31"
	riskHash         = "21e3332b5cf286dd39827973a259fbd91dde325ca067f6a067237bb6f24596f7"
	requiredWarmupMs = int64(14_400_000)
)

type shadowReport struct {
	Complete                   bool   `json:"Complete"`
	Status                     string `json:"Status"`
	StartupMode                string `json:"startup_mode"`
	SnapshotRestored           bool   `json:"snapshot_restored"`
	RestoreApplied             bool   `json:"restore_applied"`
	FullBootstrapExecuted      bool   `json:"full_bootstrap_executed"`
	DurationMs                 int64  `json:"duration_ms"`
	FeatureDecisions           int64  `json:"feature_decisions"`
	EligibleDecisions          int64  `json:"eligible_decisions"`
	FuturesBars                int64  `json:"futures_bars"`
	SpotBars                   int64  `json:"spot_bars"`
	ExternalUpdates            int64  `json:"external_updates"`
	ActualOrderSubmits         *int64 `json:"actual_order_submits"`
	DemoActualOrders           *int64 `json:"demo_actual_orders"`
	MainnetOrders              *int64 `json:"mainnet_orders"`
	MainnetPrivateCalls        *int64 `json:"mainnet_private_calls"`
	LiveOrdersSent             bool   `json:"live_orders_sent"`
	FinalHoldoutAccessed       bool   `json:"FinalHoldoutAccessed"`
	FutureObservations         int64  `json:"future_observations"`
	DuplicateEvents            int64  `json:"duplicate_events"`
	ReverseEvents              int64  `json:"reverse_events"`
	CanonicalIDGaps            int64  `json:"canonical_id_gaps"`
	FeatureRegistryHash        string `json:"feature_registry_hash"`
	EntryPolicyHash            string `json:"entry_policy_hash"`
	RiskPolicyHash             string `json:"risk_policy_hash"`
	FrozenCandidateModels      int64  `json:"frozen_candidate_models"`
	RuntimeSnapshot            string `json:"runtime_snapshot"`
	RestoreSnapshotLastEventMs int64  `json:"restore_snapshot_last_event_ms"`
	WarmupStatus               struct {
		Status                string `json:"status"`
		RequiredMs            int64  `json:"required_ms"`
		AvailableMs           int64  `json:"available_ms"`
		MissingMs             int64  `json:"missing_ms"`
		Ready                 bool   `json:"ready"`
		CurrentCaptureStartMs int64  `json:"current_capture_start_ms"`
		FuturesGaps           int64  `json:"futures_gaps"`
		SpotGaps              int64  `json:"spot_gaps"`
	} `json:"warmup_status"`
	Metrics struct {
		Decisions        int64            `json:"decisions"`
		Eligible         int64            `json:"eligible"`
		ModelEvaluations int64            `json:"model_evaluations"`
		ExclusionReasons map[string]int64 `json:"exclusion_reason_counts"`
		DecisionAudit    json.RawMessage  `json:"decision_audit"`
	} `json:"metrics"`
}

type step struct {
	Name    string `json:"name"`
	Status  string `json:"status"`
	Detail  string `json:"detail"`
	LogFile string `json:"log_file,omitempty"`
}

type summary struct {
	ToolVersion       string    `json:"tool_version"`
	StartedUTC        time.Time `json:"started_utc"`
	EndedUTC          time.Time `json:"ended_utc"`
	Mode              string    `json:"mode"`
	Repository        string    `json:"repository"`
	ResultDirectory   string    `json:"result_directory"`
	Status            string    `json:"status"`
	Classification    string    `json:"classification"`
	Explanation       []string  `json:"explanation"`
	InitialReport     string    `json:"initial_report,omitempty"`
	RestoreReport     string    `json:"restore_report,omitempty"`
	Steps             []step    `json:"steps"`
	RealDemoOrders    string    `json:"real_demo_orders"`
	RealMainnetOrders string    `json:"real_mainnet_orders"`
}

type runner struct {
	cfg config
	sum summary
	log *os.File
	ctx context.Context
}

type config struct {
	root            string
	mode            string
	initial         string
	restore         string
	minutes         int
	race            bool
	skipSourceGuard bool
	outputBase      string
}

func main() { os.Exit(run()) }

func run() int {
	var c config
	flag.StringVar(&c.root, "root", ".", "repository root (default: current directory)")
	flag.StringVar(&c.mode, "mode", "analyze", "analyze | snapshot | full")
	flag.StringVar(&c.initial, "initial", "", "initial stage-g-shadow-session.json (analyze only)")
	flag.StringVar(&c.restore, "restore", "", "restore stage-g-shadow-session.json (analyze only)")
	flag.IntVar(&c.minutes, "minutes", 10, "live observation minutes for snapshot/full (2..60)")
	flag.BoolVar(&c.race, "race", false, "also attempt race detector in full mode")
	flag.BoolVar(&c.skipSourceGuard, "skip-source-guard", false, "override conservative source guard after independent code review")
	flag.StringVar(&c.outputBase, "out", "", "optional parent report directory")
	flag.Parse()
	c.mode = strings.ToLower(strings.TrimSpace(c.mode))
	root, err := filepath.Abs(c.root)
	if err != nil {
		fmt.Fprintln(os.Stderr, "root path:", err)
		return 2
	}
	c.root = root
	if c.mode != "analyze" && c.mode != "snapshot" && c.mode != "full" {
		fmt.Fprintln(os.Stderr, "invalid -mode; use analyze, snapshot, or full")
		return 2
	}
	if (c.initial == "") != (c.restore == "") {
		fmt.Fprintln(os.Stderr, "specify both -initial and -restore, or neither")
		return 2
	}
	if c.minutes < 2 || c.minutes > 60 {
		fmt.Fprintln(os.Stderr, "-minutes must be between 2 and 60")
		return 2
	}
	if c.mode != "analyze" && c.initial != "" {
		fmt.Fprintln(os.Stderr, "-initial/-restore are only valid with -mode analyze")
		return 2
	}
	base := filepath.Join(root, "data", "reports", "verification")
	if c.outputBase != "" {
		base, err = filepath.Abs(c.outputBase)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
	}
	// Create and announce the report directory BEFORE any project/tool checks.
	dir := filepath.Join(base, fmt.Sprintf("local-%s-%d", time.Now().UTC().Format("20060102T150405Z"), os.Getpid()))
	if err = os.MkdirAll(dir, 0700); err != nil {
		fmt.Fprintln(os.Stderr, "[REPORT DIRECTORY ERROR]", dir, err)
		return 2
	}
	fmt.Println("[REPORT CREATED]", dir)
	logfile, err := os.OpenFile(filepath.Join(dir, "runner.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		fmt.Fprintln(os.Stderr, "[REPORT LOG ERROR]", err)
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	r := &runner{cfg: c, log: logfile, ctx: ctx, sum: summary{ToolVersion: "1.0", StartedUTC: time.Now().UTC(), Mode: c.mode, Repository: root, ResultDirectory: dir, Status: "RUNNING", Classification: "NOT_YET_VERIFIED", RealDemoOrders: "NOT_CHECKED", RealMainnetOrders: "NOT_CHECKED"}}
	r.note("Start: mode=%s root=%s", c.mode, root)
	r.flush() // An initial report is present even if preflight fails.
	var workErr error
	switch c.mode {
	case "analyze":
		workErr = r.analyzeExisting()
	case "snapshot", "full":
		workErr = r.verifyLive(c.mode == "full")
	}
	if workErr != nil {
		// A process error can never be overwritten by a PASS JSON record.
		r.sum.Status = "FAIL"
		if r.sum.Classification == "NOT_YET_VERIFIED" {
			r.sum.Classification = "EXECUTION_OR_INPUT_FAILURE"
		}
		r.sum.Explanation = append(r.sum.Explanation, "Execution error: "+workErr.Error())
		r.note("ERROR: %v", workErr)
	}
	if r.sum.Status == "RUNNING" {
		r.sum.Status = "INCONCLUSIVE"
	}
	r.sum.EndedUTC = time.Now().UTC()
	r.flush()
	r.note("Final status=%s classification=%s", r.sum.Status, r.sum.Classification)
	r.note("Summary: %s", filepath.Join(dir, "verification-summary.txt"))
	_ = r.log.Close()
	if r.sum.Status != "PASS" {
		return 1
	}
	return 0
}

func (r *runner) note(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	_, _ = fmt.Fprintln(os.Stdout, msg)
	if r.log != nil {
		_, _ = fmt.Fprintln(r.log, time.Now().UTC().Format(time.RFC3339), msg)
		_ = r.log.Sync()
	}
}
func (r *runner) add(name, status, detail, log string) {
	r.sum.Steps = append(r.sum.Steps, step{Name: name, Status: status, Detail: detail, LogFile: log})
	r.note("[%s] %s: %s", status, name, detail)
	r.flush()
}
func (r *runner) flush() {
	b, err := json.MarshalIndent(r.sum, "", "  ")
	if err != nil {
		r.note("summary marshal error: %v", err)
		return
	}
	b = append(b, '\n')
	_ = os.WriteFile(filepath.Join(r.sum.ResultDirectory, "verification-summary.json"), b, 0600)
	var s strings.Builder
	fmt.Fprintf(&s, "STATUS=%s\nCLASSIFICATION=%s\nMODE=%s\nREPORT_DIR=%s\nINITIAL=%s\nRESTORE=%s\nDEMO_ORDERS=%s\nMAINNET_ORDERS=%s\n", r.sum.Status, r.sum.Classification, r.sum.Mode, r.sum.ResultDirectory, r.sum.InitialReport, r.sum.RestoreReport, r.sum.RealDemoOrders, r.sum.RealMainnetOrders)
	for _, x := range r.sum.Explanation {
		fmt.Fprintf(&s, "NOTE=%s\n", x)
	}
	for _, x := range r.sum.Steps {
		fmt.Fprintf(&s, "[%s] %s %s %s\n", x.Status, x.Name, x.Detail, x.LogFile)
	}
	_ = os.WriteFile(filepath.Join(r.sum.ResultDirectory, "verification-summary.txt"), []byte(s.String()), 0600)
}
func readReport(path string) (shadowReport, error) {
	var x shadowReport
	st, err := os.Stat(path)
	if err != nil {
		return x, err
	}
	if st.Size() > 32<<20 {
		return x, fmt.Errorf("report too large (limit 32MiB): %s", path)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return x, err
	}
	if err = json.Unmarshal(b, &x); err != nil {
		return x, fmt.Errorf("invalid JSON at %s: %w", path, err)
	}
	// Missing bool fields would otherwise silently become false and create a
	// misleading order-safety or snapshot-restoration PASS.
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(b, &fields); err != nil {
		return x, err
	}
	for _, key := range []string{"FinalHoldoutAccessed", "live_orders_sent", "snapshot_restored", "restore_applied", "full_bootstrap_executed", "warmup_status", "metrics", "actual_order_submits", "demo_actual_orders", "mainnet_orders", "mainnet_private_calls"} {
		if _, ok := fields[key]; !ok {
			return x, fmt.Errorf("missing mandatory field %q at %s", key, path)
		}
	}
	if x.Status == "" || x.StartupMode == "" || x.RuntimeSnapshot == "" {
		return x, fmt.Errorf("missing required stage-g fields: %s", path)
	}
	return x, nil
}
func normalizePath(p string) string {
	return strings.ToLower(filepath.ToSlash(strings.ReplaceAll(p, "\\", "/")))
}

type reportRef struct {
	path    string
	data    shadowReport
	modtime time.Time
}

func discover(root string) (string, string, error) {
	base := filepath.Join(root, "data", "reports", "verification")
	groups := map[string][]reportRef{}
	err := filepath.WalkDir(base, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && d.Name() == "stage-g-shadow-session.json" && strings.Contains(strings.ToLower(filepath.ToSlash(path)), "snapshot-diagnostic") {
			report, e := readReport(path)
			if e != nil {
				return nil
			} // don't trust malformed old reports
			st, e := d.Info()
			if e != nil {
				return nil
			}
			key := normalizePath(report.RuntimeSnapshot)
			if key != "" {
				groups[key] = append(groups[key], reportRef{path, report, st.ModTime()})
			}
		}
		return nil
	})
	if err != nil {
		return "", "", err
	}
	var chosenInitial, chosenRestore reportRef
	have := false
	for _, refs := range groups {
		var in, re reportRef
		for _, it := range refs {
			if it.data.StartupMode == "FULL_BOOTSTRAP" && (in.path == "" || it.modtime.After(in.modtime)) {
				in = it
			}
			if it.data.StartupMode == "SNAPSHOT_RESTORE" && (re.path == "" || it.modtime.After(re.modtime)) {
				re = it
			}
		}
		if in.path != "" && re.path != "" && (!have || re.modtime.After(chosenRestore.modtime)) {
			chosenInitial, chosenRestore, have = in, re, true
		}
	}
	if !have {
		return "", "", fmt.Errorf("no matching initial/restore diagnostic pair under %s; pass -initial and -restore explicitly", base)
	}
	return chosenInitial.path, chosenRestore.path, nil
}

func requiredIdentity(x shadowReport) error {
	if x.FeatureRegistryHash != featureHash || x.EntryPolicyHash != entryHash || x.RiskPolicyHash != riskHash || x.FrozenCandidateModels != 5 {
		return fmt.Errorf("frozen feature/policy/model identity mismatch")
	}
	if x.ActualOrderSubmits == nil || x.DemoActualOrders == nil || x.MainnetOrders == nil || x.MainnetPrivateCalls == nil {
		return fmt.Errorf("missing mandatory zero-order evidence")
	}
	if *x.ActualOrderSubmits != 0 || *x.DemoActualOrders != 0 || *x.MainnetOrders != 0 || *x.MainnetPrivateCalls != 0 || x.LiveOrdersSent || x.FinalHoldoutAccessed {
		return fmt.Errorf("order, private-call, or holdout safety invariant violated")
	}
	if x.FutureObservations != 0 || x.DuplicateEvents != 0 || x.ReverseEvents != 0 || x.CanonicalIDGaps != 0 {
		return fmt.Errorf("future/duplicate/reverse/ID-gap invariant violated")
	}
	return nil
}
func (r *runner) assess(initialPath, restorePath string) error {
	r.sum.InitialReport = initialPath
	r.sum.RestoreReport = restorePath
	r.flush()
	initial, err := readReport(initialPath)
	if err != nil {
		r.add("initial-report", "FAIL", err.Error(), initialPath)
		return err
	}
	restored, err := readReport(restorePath)
	if err != nil {
		r.add("restore-report", "FAIL", err.Error(), restorePath)
		return err
	}
	if err = requiredIdentity(initial); err != nil {
		r.add("initial-safety", "FAIL", err.Error(), initialPath)
		return err
	}
	if err = requiredIdentity(restored); err != nil {
		r.add("restore-safety", "FAIL", err.Error(), restorePath)
		return err
	}
	r.sum.RealDemoOrders = "0 (both stage-g reports)"
	r.sum.RealMainnetOrders = "0 (both stage-g reports)"
	r.add("safety-and-frozen-identity", "PASS", "no orders/private calls/holdout use; frozen hashes and 5 models match; no canonical anomalies", "")
	if normalizePath(initial.RuntimeSnapshot) != normalizePath(restored.RuntimeSnapshot) {
		e := fmt.Errorf("initial and restore used different snapshot paths")
		r.add("snapshot-isolation", "FAIL", e.Error(), "")
		return e
	}
	if initial.StartupMode != "FULL_BOOTSTRAP" || !initial.FullBootstrapExecuted || !initial.Complete || initial.Status != "PASS" || initial.FeatureDecisions <= 0 || initial.EligibleDecisions <= 0 || initial.Metrics.ModelEvaluations != initial.EligibleDecisions*5 {
		r.sum.Status = "INCONCLUSIVE"
		r.sum.Classification = "INITIAL_RUNTIME_NOT_VALIDATED"
		r.sum.Explanation = append(r.sum.Explanation, "Initial runtime must show a complete full bootstrap and at least one eligible decision with 5 model evaluations each.")
		r.add("initial-live", "REVIEW", fmt.Sprintf("status=%s decisions=%d eligible=%d evaluations=%d", initial.Status, initial.FeatureDecisions, initial.EligibleDecisions, initial.Metrics.ModelEvaluations), initialPath)
		return nil
	}
	r.add("initial-live", "PASS", fmt.Sprintf("decisions=%d eligible=%d model_evaluations=%d duration_ms=%d", initial.FeatureDecisions, initial.EligibleDecisions, initial.Metrics.ModelEvaluations, initial.DurationMs), initialPath)
	if !restored.SnapshotRestored || !restored.RestoreApplied || restored.FullBootstrapExecuted || restored.StartupMode != "SNAPSHOT_RESTORE" {
		r.sum.Status = "FAIL"
		r.sum.Classification = "SNAPSHOT_NOT_RESTORED"
		r.sum.Explanation = append(r.sum.Explanation, "Restart did not apply the original snapshot; a fresh bootstrap is not a valid substitute.")
		r.add("snapshot-applied", "FAIL", fmt.Sprintf("mode=%s restored=%t applied=%t full_bootstrap=%t", restored.StartupMode, restored.SnapshotRestored, restored.RestoreApplied, restored.FullBootstrapExecuted), restorePath)
		return nil
	}
	r.add("snapshot-applied", "PASS", "matching snapshot was applied without full bootstrap", restorePath)
	if restored.FeatureDecisions > 0 && restored.EligibleDecisions > 0 && restored.Metrics.ModelEvaluations == restored.EligibleDecisions*5 && restored.Complete && restored.Status == "PASS" && restored.WarmupStatus.AvailableMs >= requiredWarmupMs {
		r.sum.Status = "PASS"
		r.sum.Classification = "SNAPSHOT_MODEL_RECOVERY_VERIFIED"
		r.sum.Explanation = append(r.sum.Explanation, fmt.Sprintf("Restored continuity survived: decisions=%d eligible=%d evaluations=%d.", restored.FeatureDecisions, restored.EligibleDecisions, restored.Metrics.ModelEvaluations))
		r.add("restore-model-recovery", "PASS", fmt.Sprintf("decisions=%d eligible=%d models=%d warmup_ms=%d", restored.FeatureDecisions, restored.EligibleDecisions, restored.Metrics.ModelEvaluations, restored.WarmupStatus.AvailableMs), restorePath)
		return nil
	}
	if restored.WarmupStatus.AvailableMs < requiredWarmupMs && restored.FeatureDecisions == 0 {
		r.sum.Status = "FAIL"
		r.sum.Classification = "RESTORE_LOST_WARM_HISTORY"
		r.sum.Explanation = append(r.sum.Explanation, fmt.Sprintf("Snapshot reported applied, but only %.1f min of 240 min warm history remained after restore; model evaluations=0. This is not explained by metrics STALE alone.", float64(restored.WarmupStatus.AvailableMs)/60000))
		if restored.WarmupStatus.CurrentCaptureStartMs > restored.RestoreSnapshotLastEventMs {
			r.sum.Explanation = append(r.sum.Explanation, fmt.Sprintf("The live capture history restarted %d seconds AFTER the restored snapshot timestamp, suggesting a post-restore runtime reset (exact code path requires latest local source/logs).", (restored.WarmupStatus.CurrentCaptureStartMs-restored.RestoreSnapshotLastEventMs)/1000))
		}
		r.add("restore-model-recovery", "FAIL", fmt.Sprintf("decisions=0 eligible=0 evaluations=%d; warmup_ms=%d/%d", restored.Metrics.ModelEvaluations, restored.WarmupStatus.AvailableMs, requiredWarmupMs), restorePath)
		return nil
	}
	if restored.WarmupStatus.AvailableMs >= requiredWarmupMs && restored.FeatureDecisions > 0 && restored.EligibleDecisions == 0 {
		r.sum.Status = "INCONCLUSIVE"
		r.sum.Classification = "RESTORE_READY_BUT_NO_ELIGIBLE_DECISION"
		r.sum.Explanation = append(r.sum.Explanation, "Warm history was retained, but no eligible decision appeared. Review recorded exclusion reasons and external source timestamps; do NOT lower freshness limits.")
		r.add("restore-model-recovery", "REVIEW", fmt.Sprintf("decisions=%d eligible=0 exclusions=%v", restored.FeatureDecisions, restored.Metrics.ExclusionReasons), restorePath)
		return nil
	}
	r.sum.Status = "FAIL"
	r.sum.Classification = "RESTORE_MODEL_RECOVERY_NOT_PROVEN"
	r.sum.Explanation = append(r.sum.Explanation, "Inspect the restore report, warmup state, and live logs; a successful SnapshotRestored flag alone is insufficient.")
	r.add("restore-model-recovery", "FAIL", fmt.Sprintf("status=%s complete=%t decisions=%d eligible=%d model_evaluations=%d warmup_ms=%d", restored.Status, restored.Complete, restored.FeatureDecisions, restored.EligibleDecisions, restored.Metrics.ModelEvaluations, restored.WarmupStatus.AvailableMs), restorePath)
	return nil
}

func (r *runner) analyzeExisting() error {
	initial, restore := r.cfg.initial, r.cfg.restore
	if initial == "" {
		var err error
		initial, restore, err = discover(r.cfg.root)
		if err != nil {
			return err
		}
	}
	r.note("Reading (no live execution): %s", initial)
	r.note("Comparing restore report: %s", restore)
	return r.assess(initial, restore)
}
func safeEnvironment(emptyCredential string) []string {
	env := make([]string, 0, len(os.Environ())+10)
	for _, v := range os.Environ() {
		key, _, _ := strings.Cut(v, "=")
		up := strings.ToUpper(key)
		if strings.HasPrefix(up, "BINANCE_") || up == "RUN_DEMO_AUTO_SMOKE" {
			continue
		}
		env = append(env, v)
	}
	return append(env, "BINANCE_ENV=PUBLIC_ONLY", "BINANCE_TESTNET_ENABLE_ORDERS=false", "BINANCE_TESTNET_ENABLE_AUTO_ORDERS=false", "BINANCE_MAINNET_ENABLE_ORDERS=false", "RUN_DEMO_AUTO_SMOKE=0", "BINANCE_CREDENTIAL_FILE="+emptyCredential)
}
func (r *runner) command(name string, timeout time.Duration, environment []string, executable string, args ...string) error {
	path := filepath.Join(r.sum.ResultDirectory, name+".log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	ctx, cancel := context.WithTimeout(r.ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, args...)
	cmd.Dir = r.cfg.root
	cmd.Env = environment
	cmd.Stdin = nil
	cmd.Stdout = io.MultiWriter(os.Stdout, f)
	cmd.Stderr = io.MultiWriter(os.Stderr, f)
	r.note("[START] %s: %s %s (timeout=%s)", name, executable, strings.Join(args, " "), timeout)
	start := time.Now()
	err = cmd.Run()
	detail := fmt.Sprintf("duration=%s", time.Since(start).Round(time.Millisecond))
	if ctx.Err() != nil {
		err = fmt.Errorf("%s: %w", name, ctx.Err())
	}
	if err != nil {
		r.add(name, "FAIL", detail+" error="+err.Error(), path)
		return err
	}
	r.add(name, "PASS", detail, path)
	return nil
}

// This is a conservative *source-level warning*, not proof of the precise reset
// call. An old implementation restores raw metrics_* keys while the streaming
// AddExternal path deduplicates the aggregate key "metrics". A replay can then
// be added to an already restored engine and trigger an uncounted reset.
func possibleRestoreMetricKeyMismatch(root string) (bool, error) {
	p := filepath.Join(root, "internal", "live", "runtimefeature", "runtime.go")
	body, err := os.ReadFile(p)
	if err != nil {
		return false, fmt.Errorf("cannot inspect local restore code %s: %w", p, err)
	}
	text := string(body)
	at := strings.Index(text, "func (r *Runtime) tryRestore(")
	if at < 0 {
		return false, fmt.Errorf("could not find local tryRestore; inspect it before running a long live test")
	}
	section := text[at:]
	if end := strings.Index(section, "func (r *Runtime) addTrade("); end >= 0 {
		section = section[:end]
	}
	risky := strings.Contains(section, "r.lastExternal[row.Dataset]") && !strings.Contains(section, `r.lastExternal["metrics"]`)
	return risky, nil
}

func (r *runner) verifyLive(full bool) error {
	// The repository is the source of truth. Do not rely on the old GitHub main.
	if _, err := os.Stat(filepath.Join(r.cfg.root, "go.mod")); err != nil {
		return fmt.Errorf("repository go.mod missing: %w", err)
	}
	code, err := os.ReadFile(filepath.Join(r.cfg.root, "cmd", "livevalidation", "main.go"))
	if err != nil {
		return fmt.Errorf("local livevalidation source missing: %w", err)
	}
	if !strings.Contains(string(code), "snapshot-path") {
		return fmt.Errorf("local livevalidation lacks -snapshot-path support; no live run started")
	}
	if !r.cfg.skipSourceGuard {
		risky, guardErr := possibleRestoreMetricKeyMismatch(r.cfg.root)
		if guardErr != nil {
			return guardErr
		}
		if risky {
			r.sum.Status = "BLOCKED"
			r.sum.Classification = "RESTORE_METRICS_KEY_MISMATCH_SUSPECTED"
			r.sum.Explanation = append(r.sum.Explanation, "Latest local tryRestore still appears to restore raw metrics_* keys instead of the aggregate metrics key. Review and add a deterministic regression test before repeating a 20-minute live run. If it was correctly fixed elsewhere, re-run with -skip-source-guard after code review.")
			r.add("restore-source-guard", "BLOCKED", "possible restored-metrics deduplication defect; no live test started", "")
			return nil
		}
		r.add("restore-source-guard", "PASS", "old raw metrics key mismatch pattern not detected in local tryRestore", "")
	}
	creds := filepath.Join(r.sum.ResultDirectory, "empty-credentials.env")
	if err = os.WriteFile(creds, []byte("# Intentionally empty for PUBLIC_ONLY verification\n"), 0600); err != nil {
		return err
	}
	env := safeEnvironment(creds)
	r.add("isolation", "PASS", "PUBLIC_ONLY; trading flags OFF; empty credential file; unique snapshot/report files", "")
	if full {
		if err = r.command("go-test-all", 25*time.Minute, env, "go", "test", "./...", "-count=1"); err != nil {
			return err
		}
		if err = r.command("go-test-auto-repeat10", 25*time.Minute, env, "go", "test", "./internal/uiapi", "./internal/live/binance", "-count=10"); err != nil {
			return err
		}
		if err = r.command("go-vet", 15*time.Minute, env, "go", "vet", "./..."); err != nil {
			return err
		}
		if err = r.command("go-build-all", 15*time.Minute, env, "go", "build", "./..."); err != nil {
			return err
		}
		if r.cfg.race {
			if runtime.GOOS == "windows" {
				// Known toolchain/CGO limitation on the user's environment; optional only.
				r.add("race-detector", "SKIP", "Windows: ensure 64-bit CGO-capable compiler before requesting race test; not assumed to be supported", "")
			} else if err = r.command("race-detector", 25*time.Minute, env, "go", "test", "-race", "./internal/live/binance", "./internal/uiapi", "-count=1"); err != nil {
				return err
			}
		}
	} else {
		if err = r.command("go-test-targeted", 15*time.Minute, env, "go", "test", "./internal/live/runtimefeature", "./internal/live/session", "./cmd/livevalidation", "-count=1"); err != nil {
			return err
		}
	}
	exeName := "livevalidation-verifier"
	if runtime.GOOS == "windows" {
		exeName += ".exe"
	}
	exe := filepath.Join(r.sum.ResultDirectory, exeName)
	if err = r.command("build-isolated-livevalidation", 15*time.Minute, env, "go", "build", "-o", exe, "./cmd/livevalidation"); err != nil {
		return err
	}
	duration := time.Duration(r.cfg.minutes) * time.Minute
	snapshot := filepath.Join(r.sum.ResultDirectory, "isolated-runtime-snapshot.json")
	initialDir := filepath.Join(r.sum.ResultDirectory, "initial")
	restoreDir := filepath.Join(r.sum.ResultDirectory, "restore")
	if err = os.MkdirAll(initialDir, 0700); err != nil {
		return err
	}
	if err = os.MkdirAll(restoreDir, 0700); err != nil {
		return err
	}
	// No test/build step may occur between initial and restore: the live snapshot
	// has strict ID and freshness continuity and can expire quickly.
	initialArgs := []string{"-mode", "stage-g", "-root", initialDir, "-duration", duration.String(), "-snapshot-path", snapshot}
	if err = r.command("initial-public-only", duration+15*time.Minute, env, exe, initialArgs...); err != nil {
		r.sum.Explanation = append(r.sum.Explanation, "Initial stage-g failed; no restore is attempted. Inspect initial-public-only.log.")
		return err
	}
	initialPath := filepath.Join(initialDir, "stage-g-shadow-session.json")
	initial, err := readReport(initialPath)
	if err != nil {
		return fmt.Errorf("initial report: %w", err)
	}
	if err = requiredIdentity(initial); err != nil {
		return fmt.Errorf("initial safety: %w", err)
	}
	if initial.DurationMs < duration.Milliseconds()-2000 {
		return fmt.Errorf("initial run too short: observed %dms, requested %dms", initial.DurationMs, duration.Milliseconds())
	}
	if initial.FeatureDecisions == 0 || initial.EligibleDecisions == 0 || initial.Metrics.ModelEvaluations == 0 {
		r.sum.Status = "INCONCLUSIVE"
		r.sum.Classification = "INITIAL_NO_ELIGIBLE_DECISIONS"
		r.sum.InitialReport = initialPath
		r.sum.Explanation = append(r.sum.Explanation, "No eligible initial decision; cannot establish a meaningful before/after comparison. No restore rerun or code change is made automatically.")
		r.add("initial-precondition", "REVIEW", fmt.Sprintf("initial decisions=%d eligible=%d evaluations=%d", initial.FeatureDecisions, initial.EligibleDecisions, initial.Metrics.ModelEvaluations), initialPath)
		return nil
	}
	// Restore immediately, before expensive analysis or unrelated commands.
	restoreArgs := []string{"-mode", "stage-g", "-root", restoreDir, "-duration", duration.String(), "-snapshot-path", snapshot}
	restoreRunErr := r.command("restore-public-only", duration+5*time.Minute, env, exe, restoreArgs...)
	restorePath := filepath.Join(restoreDir, "stage-g-shadow-session.json")
	if _, statErr := os.Stat(restorePath); statErr != nil {
		if restoreRunErr != nil {
			return fmt.Errorf("restore did not produce a report (%v): %w", restoreRunErr, statErr)
		}
		return fmt.Errorf("restore report missing: %w", statErr)
	}
	assessErr := r.assess(initialPath, restorePath)
	if assessErr != nil {
		return assessErr
	}
	if restoredReport, readErr := readReport(restorePath); readErr != nil {
		return readErr
	} else if restoredReport.DurationMs < duration.Milliseconds()-2000 {
		return fmt.Errorf("restore run too short: observed %dms, requested %dms", restoredReport.DurationMs, duration.Milliseconds())
	}
	// A nonzero stage-g exit is expected when its acceptance gate fails; the
	// structured report above supplies the real reason rather than hiding it.
	if restoreRunErr != nil && r.sum.Status == "PASS" {
		return fmt.Errorf("restore process failed even though report says PASS: %w", restoreRunErr)
	}
	return nil
}
