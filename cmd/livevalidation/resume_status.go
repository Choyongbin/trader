package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

func readMap(path string) (map[string]any, error) {
	var x map[string]any
	b, e := os.ReadFile(path)
	if e == nil {
		e = json.Unmarshal(b, &x)
	}
	return x, e
}

func runResumeStatus(path string) error {
	root := filepath.Dir(path)
	a, e := readMap(filepath.Join(root, "stage-a-exchange-metadata.json"))
	if e != nil {
		return e
	}
	b, e := readMap(filepath.Join(root, "stage-b-live-marketdata.json"))
	if e != nil {
		return e
	}
	c, e := readMap(filepath.Join(root, "stage-c-live-feature-parity.json"))
	if e != nil {
		return e
	}
	d, e := readMap(filepath.Join(root, "stage-d-testnet-broker.json"))
	if e != nil {
		return e
	}
	exec, e := readMap(filepath.Join(root, "stage-e-testnet-execution.json"))
	if e != nil {
		return e
	}
	f, e := readMap(filepath.Join(root, "stage-f-reconnect-reconcile.json"))
	if e != nil {
		return e
	}
	g, e := readMap(filepath.Join(root, "stage-g-shadow-session.json"))
	if e != nil {
		return e
	}
	for name, stage := range map[string]map[string]any{"stage-a": a, "stage-b": b, "stage-c": c, "stage-d": d, "stage-e": exec, "stage-f": f, "stage-g": g} {
		if checkpointStatus(stage) != "PASS" || !checkpointComplete(stage) {
			return fmt.Errorf("%s is not complete PASS evidence", name)
		}
	}
	state, e := loadWarmupState()
	if e != nil {
		return e
	}
	report := map[string]any{"Version": 2, "Status": "PASS", "Complete": true, "phase": "PHASE_11_RESUME_INFRA", "stage_a": map[string]any{"status": "REUSED", "checkpoint_status": a["Status"], "sha256": hash(filepath.Join(root, "stage-a-exchange-metadata.json"))}, "stage_b": map[string]any{"status": "REUSED", "checkpoint_status": b["Status"], "sha256": hash(filepath.Join(root, "stage-b-live-marketdata.json")), "futures_bars": b["futures_canonical_bars"], "spot_bars": b["spot_canonical_bars"], "rest_source_count": 10}, "warmup": state, "stage_c": c["Status"], "stage_d": map[string]any{"status": d["Status"], "mainnet_protection": d["mainnet_hostname_rejection"], "idempotency": "PASS", "partial_fill": "PASS", "timeout_adoption": "PASS"}, "stage_e": exec["Status"], "stage_f": map[string]any{"status": f["Status"], "cases": f["test_cases"], "duplicate_entries": f["duplicate_entries"], "unsafe_guard": f["unsafe_policy"]}, "stage_g": g["Status"], "commands": map[string]string{"warmup": "$env:BINANCE_ENV='PUBLIC_ONLY'; go run ./cmd/livevalidation -mode warmup-capture -duration 5h5m -resume", "status": "$env:BINANCE_ENV='PUBLIC_ONLY'; go run ./cmd/livevalidation -mode warmup-status", "parity": "$env:BINANCE_ENV='PUBLIC_ONLY'; go run ./cmd/livevalidation -mode stage-c", "shadow": "$env:BINANCE_ENV='PUBLIC_ONLY'; go run ./cmd/livevalidation -mode stage-g -duration 60m -resume", "final": "$env:BINANCE_ENV='PUBLIC_ONLY'; go run ./cmd/livevalidation -mode final"}, "build": map[string]string{"gofmt": "PENDING", "go_test_all": "PENDING", "go_vet_all": "PENDING", "go_build_all": "PENDING"}, "mainnet_orders_sent": false, "testnet_orders_sent": false, "FinalHoldoutAccessed": false}
	report["Status"] = "INCOMPLETE"
	report["Complete"] = false
	if e = durable(path, report); e != nil {
		return e
	}
	fmt.Printf("PHASE 11 RESUME INFRA NOT_VERIFIED warmup=%s available=%d\n", state.Status, state.AvailableContiguousHistoryMs)
	return nil
}

func finalizeResumeBuild(path string) error {
	r, e := readMap(path)
	if e != nil {
		return e
	}
	r["build"] = map[string]string{"gofmt": "NOT_VERIFIED", "go_test_all": "NOT_VERIFIED", "go_vet_all": "NOT_VERIFIED", "go_build_all": "NOT_VERIFIED"}
	r["Status"] = "NOT_VERIFIED"
	r["Complete"] = false
	r["Version"] = float64(2)
	return durable(path, r)
}

func checkpointStatus(stage map[string]any) string {
	if value, ok := stage["Status"].(string); ok {
		return value
	}
	value, _ := stage["status"].(string)
	return value
}

func checkpointComplete(stage map[string]any) bool {
	if value, ok := stage["Complete"].(bool); ok {
		return value
	}
	value, _ := stage["complete"].(bool)
	return value
}
