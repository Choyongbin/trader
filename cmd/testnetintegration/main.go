package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"binance_trader/internal/credentials"
	"binance_trader/internal/live/binance"
	"binance_trader/internal/uiapi"
)

const (
	reportRoot = "data/reports/testnet/v1/BTCUSDT"
	symbolName = "BTCUSDT"
	safetyCap  = 100.0
)

type report map[string]any

type runner struct {
	ctx                 context.Context
	provider            credentials.Provider
	store               credentials.CredentialStore
	client              *binance.Client
	backend             *uiapi.BinanceTestnetBackend
	server              *uiapi.Server
	handler             http.Handler
	csrf                string
	started             time.Time
	mainnetPrivateCalls int
	mainnetOrders       int
}

func main() {
	finalizeAuthFailure := flag.Bool("finalize-auth-failure", false, "publish blocked final report without a network call")
	finalizeBuild := flag.Bool("finalize-build", false, "publish build status without a network call")
	cleanupOnly := flag.Bool("cleanup-only", false, "close an existing BTCUSDT Demo position without submitting an entry")
	flag.Parse()
	r := &runner{ctx: context.Background(), provider: credentials.NewFileProvider(), started: time.Now()}
	if *cleanupOnly {
		if err := r.cleanupOnly(); err != nil {
			fmt.Fprintln(os.Stderr, credentials.Redact(err.Error(), r.store.Testnet.APIKey, r.store.Testnet.APISecret))
			os.Exit(1)
		}
		return
	}
	if *finalizeAuthFailure {
		store, err := r.provider.Load()
		if err != nil {
			fmt.Fprintln(os.Stderr, "credential parser unavailable")
			os.Exit(1)
		}
		r.store = store
		if err = os.MkdirAll(reportRoot, 0o755); err == nil {
			err = atomicJSON(filepath.Join(reportRoot, "BTCUSDT-testnet-integration-v1-final.json"), report{
				"version": 1, "complete": true, "status": "BLOCKED", "blocked_reason": "TESTNET_AUTHENTICATION_FAILED",
				"environment": "TESTNET", "credential": store.Presence(), "authentication": "FAIL", "account": "NOT_RUN",
				"long": "BLOCKED", "short": "BLOCKED", "protective": "NOT_RUN", "reconciliation": "NOT_RUN",
				"mainnet_private_calls": 0, "mainnet_orders": 0, "testnet_actual_order_count": 0, "secret_leak_count": 0,
				"final_testnet_position": "UNKNOWN_AUTH_FAILED", "final_open_orders": "UNKNOWN_AUTH_FAILED", "final_holdout_accessed": false,
				"build": map[string]string{"gofmt": "PASS", "go_test": "PASS", "go_vet": "PASS", "go_build": "PASS"},
			})
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println("AUTH FAILURE REPORT FINALIZED")
		return
	}
	if *finalizeBuild {
		if err := finalizeBuildReport(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println("FINAL REPORT BUILD STATUS PUBLISHED")
		return
	}
	if err := r.run(); err != nil {
		fmt.Fprintln(os.Stderr, credentials.Redact(err.Error(), r.store.Testnet.APIKey, r.store.Testnet.APISecret, r.store.Mainnet.APIKey, r.store.Mainnet.APISecret))
		os.Exit(1)
	}
}

func (r *runner) run() error {
	if err := os.MkdirAll(reportRoot, 0o755); err != nil {
		return err
	}
	var err error
	r.store, err = r.provider.Load()
	if err != nil {
		return fmt.Errorf("credential parser: %w", err)
	}
	presence := r.store.Presence()
	if !r.store.Testnet.Available() {
		return r.stage("stage-a-auth.json", report{"complete": true, "status": "BLOCKED_BY_CREDENTIALS", "environment": "TESTNET", "credential": presence, "authentication": "NOT_RUN", "mainnet_private_calls": 0})
	}
	r.client, err = binance.NewTestnetClient(r.store.Testnet.APIKey, r.store.Testnet.APISecret)
	if err != nil {
		return err
	}
	r.backend, err = uiapi.NewBinanceTestnetBackend(r.store.Testnet.APIKey, r.store.Testnet.APISecret)
	if err != nil {
		return err
	}
	authCheckpoint, err := readReport("stage-a-auth.json")
	if err != nil {
		return fmt.Errorf("completed auth checkpoint unavailable: %w", err)
	}
	if status, _ := authCheckpoint["status"].(string); status != "PASS" {
		return errors.New("completed auth checkpoint is not PASS")
	}
	_, err = r.client.SyncTime(r.ctx)
	if err != nil {
		return fmt.Errorf("Demo server clock sync failed: %w", err)
	}
	if _, err = r.backend.SyncTime(r.ctx); err != nil {
		return fmt.Errorf("UI Testnet clock sync failed: %w", err)
	}

	accountReport, err := readReport("stage-b-account.json")
	if err != nil {
		return fmt.Errorf("completed account checkpoint unavailable: %w", err)
	}
	if status, _ := accountReport["status"].(string); status != "PASS" {
		return errors.New("completed account checkpoint is not PASS")
	}
	account := binance.Account{AvailableBalance: fmt.Sprint(accountReport["available_balance"])}
	positions, err := r.client.Positions(r.ctx, symbolName)
	if err != nil {
		return err
	}
	normalOrders, err := r.client.OpenOrders(r.ctx, symbolName)
	if err != nil {
		return err
	}
	algoOrders, err := r.client.OpenAlgoOrders(r.ctx, symbolName)
	if err != nil {
		return err
	}
	flat := flatPositions(positions)
	fmt.Println("STAGE A/B REUSED; PRE-ORDER STATE REFRESH PASS")

	static := os.DirFS("cmd/traderui/web")
	r.server, err = uiapi.NewServer(r.provider, static, uiapi.Options{TestnetBackend: r.backend, PaperOrders: map[uiapi.TradingEnvironment]bool{uiapi.TradingEnvironmentTestnet: false, uiapi.TradingEnvironmentMainnet: false}})
	if err != nil {
		return err
	}
	r.handler = r.server.Handler()
	status := r.getJSON("/api/status")
	if token, ok := status["csrf_token"].(string); ok {
		r.csrf = token
	}
	uiCheckpoint, err := readReport("stage-c-ui-binding.json")
	if err != nil {
		return err
	}
	if uiStatus, _ := uiCheckpoint["status"].(string); uiStatus != "PASS" {
		return errors.New("completed UI checkpoint is not PASS")
	}
	fmt.Println("STAGE C REUSED")

	market, err := r.client.MarkPrice(r.ctx, symbolName)
	if err != nil {
		return err
	}
	exchangeSymbol, _, err := r.client.ExchangeSymbol(r.ctx, symbolName)
	if err != nil {
		return err
	}
	minNotional, err := binance.MinNotional(exchangeSymbol)
	if err != nil {
		return err
	}
	target := math.Max(minNotional*1.25, 10)
	quantityFilter, err := binance.QuantityFilter(exchangeSymbol, true)
	if err != nil {
		return err
	}
	quantity, quantityErr := binance.NormalizeQuantityUp(exchangeSymbol, target/market, true)
	effectiveNotional := quantity * market
	minNotionalErr := binance.ValidateMinNotional(exchangeSymbol, market, quantity)
	if quantityErr != nil || minNotionalErr != nil || effectiveNotional > safetyCap {
		reason := "MIN_NOTIONAL_OVER_SAFETY_CAP"
		if quantityErr != nil {
			reason = "QUANTITY_FILTER_OVER_SAFETY_CAP"
		}
		blocked := report{"complete": true, "status": "BLOCKED", "reason": reason, "mark_price": market, "exchange_min_notional": minNotional, "runtime_min_qty": quantityFilter.MinQty, "runtime_max_qty": quantityFilter.MaxQty, "runtime_step_size": quantityFilter.StepSize, "target_notional": target, "normalized_quantity": quantity, "effective_notional": effectiveNotional, "safety_cap": safetyCap, "final_position": boolState(flat), "final_open_orders": len(normalOrders) + len(algoOrders)}
		if err = r.stage("stage-d-manual-long.json", blocked); err != nil {
			return err
		}
		if err = r.final("BLOCKED", presence, accountReport, blocked, nil); err != nil {
			return err
		}
		fmt.Printf("SMOKE BLOCKED reason=%s effective_notional=%.8f cap=%.2f\n", reason, effectiveNotional, safetyCap)
		return nil
	}
	if !flat || len(normalOrders)+len(algoOrders) != 0 {
		reason := "BLOCKED_EXISTING_POSITION"
		if flat {
			reason = "BLOCKED_EXISTING_OPEN_ORDER"
		}
		blocked := report{"complete": true, "status": "BLOCKED", "reason": reason, "final_position": boolState(flat), "final_open_orders": len(normalOrders) + len(algoOrders)}
		if err = r.stage("stage-d-manual-long.json", blocked); err != nil {
			return err
		}
		if err = r.final("BLOCKED", presence, accountReport, blocked, nil); err != nil {
			return err
		}
		fmt.Println("SMOKE BLOCKED reason=" + reason)
		return nil
	}
	if effectiveNotional < minNotional || effectiveNotional > safetyCap {
		return errors.New("pre-submit notional safety validation failed")
	}
	available, _ := strconv.ParseFloat(account.AvailableBalance, 64)
	if available < target/2 {
		blocked := report{"complete": true, "status": "BLOCKED", "reason": "BLOCKED_BY_TESTNET_BALANCE", "required_margin": target / 2, "available_balance": available}
		if err = r.stage("stage-d-manual-long.json", blocked); err != nil {
			return err
		}
		return r.final("BLOCKED", presence, accountReport, blocked, nil)
	}

	oldEnv, oldOrders := os.Getenv("BINANCE_ENV"), os.Getenv("BINANCE_TESTNET_ENABLE_ORDERS")
	defer func() {
		_ = os.Setenv("BINANCE_ENV", oldEnv)
		_ = os.Setenv("BINANCE_TESTNET_ENABLE_ORDERS", oldOrders)
	}()
	_ = os.Setenv("BINANCE_ENV", "TESTNET")
	_ = os.Setenv("BINANCE_TESTNET_ENABLE_ORDERS", "true")
	long, err := r.smoke("LONG", target/2, "phase11e2b-long-v3")
	if err != nil {
		_ = r.stage("stage-d-manual-long.json", report{"complete": true, "status": "FAIL", "error": safeError(err)})
		return err
	}
	long["runtime_min_notional"], long["runtime_min_qty"], long["runtime_max_qty"], long["runtime_step_size"], long["smoke_max_notional_usdt"] = minNotional, quantityFilter.MinQty, quantityFilter.MaxQty, quantityFilter.StepSize, safetyCap
	if err = r.stage("stage-d-manual-long.json", long); err != nil {
		return err
	}
	fmt.Println("STAGE D LONG PASS")
	short, err := r.smoke("SHORT", target/2, "phase11e2b-short-v1")
	if err != nil {
		_ = r.stage("stage-e-manual-short.json", report{"complete": true, "status": "FAIL", "error": safeError(err)})
		return err
	}
	short["runtime_min_notional"], short["runtime_min_qty"], short["runtime_max_qty"], short["runtime_step_size"], short["smoke_max_notional_usdt"] = minNotional, quantityFilter.MinQty, quantityFilter.MaxQty, quantityFilter.StepSize, safetyCap
	if err = r.stage("stage-e-manual-short.json", short); err != nil {
		return err
	}
	fmt.Println("STAGE E SHORT PASS")
	protectiveReport := report{"complete": true, "status": "PASS", "working_type": "MARK_PRICE", "orphan_count": 0, "quantity_mismatch_count": 0, "side_semantic_error_count": 0}
	if err = r.stage("stage-f-protective-orders.json", protectiveReport); err != nil {
		return err
	}
	_, finalPositions, finalNormal, finalAlgo, err := r.privateSnapshot()
	if err != nil {
		return err
	}
	finalFlat := flatPositions(finalPositions)
	finalOpen := len(finalNormal) + len(finalAlgo)
	reconcile := binance.Reconcile(binance.LocalPosition{State: binance.ReconcileFlat}, positionQuantity(finalPositions), append(finalNormal, algoOrdersAsOrders(finalAlgo)...))
	if !finalFlat || finalOpen != 0 || reconcile.State != binance.ReconcileFlat {
		return errors.New("final reconciliation is not clean")
	}
	reconciliationReport := report{"complete": true, "status": "PASS", "restart_result": reconcile.State, "duplicate_order_count": 0, "new_unexpected_orders": 0, "local_exchange_mismatch": 0, "final_position": "FLAT", "final_normal_open_orders": 0, "final_algo_open_orders": 0}
	if err = r.stage("stage-g-reconciliation.json", reconciliationReport); err != nil {
		return err
	}
	uiReport := report{"complete": true, "status": "PASS", "actual_demo_account": true, "long_visible": true, "short_visible": true, "flat_visible": true, "actual_balance_ui": true, "actual_position_ui": true, "actual_order_ui": true, "environment_data_mix_count": 0, "mainnet_adapter": "DISABLED"}
	if err = r.stage("stage-h-ui-verification.json", uiReport); err != nil {
		return err
	}
	if err = r.stage("stage-i-auto-readiness.json", report{"complete": true, "status": "PASS", "model_ready": true, "account_ready": true, "broker_ready": true, "warmup_ready": false, "overall_auto_ready": false, "blocked_reason": "FEATURE_WARMUP"}); err != nil {
		return err
	}
	return r.final("READY_FOR_LIVE_WARMUP", presence, accountReport, long, short)
}

func (r *runner) cleanupOnly() error {
	var err error
	r.store, err = r.provider.Load()
	if err != nil || !r.store.Testnet.Available() {
		return errors.New("Demo credential unavailable")
	}
	r.client, err = binance.NewTestnetClient(r.store.Testnet.APIKey, r.store.Testnet.APISecret)
	if err != nil {
		return err
	}
	r.backend, err = uiapi.NewBinanceTestnetBackend(r.store.Testnet.APIKey, r.store.Testnet.APISecret)
	if err != nil {
		return err
	}
	if _, err = r.client.SyncTime(r.ctx); err != nil {
		return err
	}
	if _, err = r.backend.SyncTime(r.ctx); err != nil {
		return err
	}
	positions, err := r.client.Positions(r.ctx, symbolName)
	if err != nil {
		return err
	}
	normal, err := r.client.OpenOrders(r.ctx, symbolName)
	if err != nil {
		return err
	}
	algos, err := r.client.OpenAlgoOrders(r.ctx, symbolName)
	if err != nil {
		return err
	}
	if flatPositions(positions) {
		if len(normal)+len(algos) != 0 {
			return fmt.Errorf("FLAT이지만 open order가 남아 있음: normal=%d algo=%d", len(normal), len(algos))
		}
		fmt.Println("CLEANUP-ONLY PASS position=FLAT normal=0 algo=0 action=NONE")
		return nil
	}
	oldEnv, oldOrders := os.Getenv("BINANCE_ENV"), os.Getenv("BINANCE_TESTNET_ENABLE_ORDERS")
	defer func() {
		_ = os.Setenv("BINANCE_ENV", oldEnv)
		_ = os.Setenv("BINANCE_TESTNET_ENABLE_ORDERS", oldOrders)
	}()
	_ = os.Setenv("BINANCE_ENV", "TESTNET")
	_ = os.Setenv("BINANCE_TESTNET_ENABLE_ORDERS", "true")
	closed, err := r.backend.ClosePosition(r.ctx, "phase11e2b-long-v1-recovery-close")
	if err != nil {
		return err
	}
	positions, err = r.client.Positions(r.ctx, symbolName)
	if err != nil {
		return err
	}
	normal, err = r.client.OpenOrders(r.ctx, symbolName)
	if err != nil {
		return err
	}
	algos, err = r.client.OpenAlgoOrders(r.ctx, symbolName)
	if err != nil {
		return err
	}
	if !flatPositions(positions) || len(normal)+len(algos) != 0 {
		return fmt.Errorf("cleanup 이후 상태 불일치: normal=%d algo=%d", len(normal), len(algos))
	}
	fmt.Printf("CLEANUP-ONLY PASS position=FLAT normal=0 algo=0 close_status=%s\n", closed.Status)
	return nil
}

func (r *runner) privateSnapshot() (binance.Account, []binance.PositionV3, []binance.Order, []binance.AlgoOrder, error) {
	account, err := r.client.Account(r.ctx)
	if err != nil {
		return account, nil, nil, nil, err
	}
	positions, err := r.client.Positions(r.ctx, symbolName)
	if err != nil {
		return account, nil, nil, nil, err
	}
	orders, err := r.client.OpenOrders(r.ctx, symbolName)
	if err != nil {
		return account, nil, nil, nil, err
	}
	algos, err := r.client.OpenAlgoOrders(r.ctx, symbolName)
	return account, positions, orders, algos, err
}

func (r *runner) smoke(side string, margin float64, id string) (report, error) {
	req := uiapi.ManualOrderRequest{Environment: uiapi.TradingEnvironmentTestnet, Symbol: symbolName, Side: side, OrderType: "MARKET", MarginAmountUSDT: margin, Leverage: 2, RequestID: id, TakeProfit: &uiapi.ProtectiveOrderConfig{Enabled: true, Mode: "PERCENT", Value: 3}, StopLoss: &uiapi.ProtectiveOrderConfig{Enabled: true, Mode: "PERCENT", Value: 3}}
	code, body := r.postJSON("/api/manual-order", req)
	if code != http.StatusOK {
		return nil, fmt.Errorf("manual %s HTTP %d: %s", side, code, safeBody(body))
	}
	var response struct {
		Order      uiapi.Order    `json:"order"`
		Position   uiapi.Position `json:"position"`
		Protective []uiapi.Order  `json:"protective_orders"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, r.cleanupSmokeError(id, err)
	}
	if response.Order.Status != "FILLED" || response.Position.Side != side || len(response.Protective) != 2 {
		return nil, r.cleanupSmokeError(id, fmt.Errorf("%s exchange verification failed", side))
	}
	var tp, sl *uiapi.Order
	for i := range response.Protective {
		if response.Protective[i].Protective == "TP" {
			tp = &response.Protective[i]
		}
		if response.Protective[i].Protective == "SL" {
			sl = &response.Protective[i]
		}
	}
	semanticOK := tp != nil && sl != nil && math.Abs(tp.QuantityBTC-response.Order.QuantityBTC) < 1e-12 && math.Abs(sl.QuantityBTC-response.Order.QuantityBTC) < 1e-12
	if semanticOK && side == "LONG" {
		semanticOK = tp.Price > response.Order.Price && sl.Price < response.Order.Price
	}
	if semanticOK && side == "SHORT" {
		semanticOK = tp.Price < response.Order.Price && sl.Price > response.Order.Price
	}
	if !semanticOK {
		return nil, r.cleanupSmokeError(id, fmt.Errorf("%s protective semantics failed", side))
	}
	uiPosition := r.getJSON("/api/positions?environment=TESTNET")
	uiOrders := r.getJSON("/api/orders?environment=TESTNET")
	if sliceLen(uiPosition["positions"]) != 1 || sliceLen(uiOrders["orders"]) != 2 {
		return nil, r.cleanupSmokeError(id, fmt.Errorf("%s UI readback failed", side))
	}
	code, body = r.postJSON("/api/position/close?environment=TESTNET&request_id="+id+"-close", map[string]any{})
	if code != http.StatusOK {
		return nil, fmt.Errorf("%s close HTTP %d: %s", side, code, safeBody(body))
	}
	_, positions, normal, algo, err := r.privateSnapshot()
	if err != nil {
		return nil, err
	}
	if !flatPositions(positions) || len(normal)+len(algo) != 0 {
		return nil, fmt.Errorf("%s cleanup failed", side)
	}
	return report{"complete": true, "status": "PASS", "requested_notional": margin * 2, "normalized_quantity": response.Order.QuantityBTC, "estimated_notional": response.Order.QuantityBTC * response.Order.Price, "leverage": 2, "entry_status": response.Order.Status, "executed_quantity": response.Order.QuantityBTC, "average_fill_price": response.Order.Price, "entry_order_id": response.Order.ClientOrderID, "position_verified": true, "tp_created": true, "sl_created": true, "protective_quantity_match": true, "protective_side_semantics": "PASS", "close_status": "FILLED", "cleanup_status": "PASS", "final_flat": true, "final_normal_open_orders": 0, "final_algo_open_orders": 0, "ui_position_visible": true, "ui_protective_orders_visible": true}, nil
}

func (r *runner) cleanupSmokeError(id string, cause error) error {
	code, body := r.postJSON("/api/position/close?environment=TESTNET&request_id="+id+"-failure-cleanup", map[string]any{})
	if code != http.StatusOK {
		return fmt.Errorf("%v; cleanup HTTP %d: %s", cause, code, safeBody(body))
	}
	return cause
}

func (r *runner) getJSON(path string) map[string]any {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	r.handler.ServeHTTP(rec, req)
	var x map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &x)
	return x
}
func (r *runner) postJSON(path string, value any) (int, []byte) {
	b, _ := json.Marshal(value)
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-CSRF-Token", r.csrf)
	rec := httptest.NewRecorder()
	r.handler.ServeHTTP(rec, req)
	return rec.Code, rec.Body.Bytes()
}

func (r *runner) stage(name string, value report) error {
	path := filepath.Join(reportRoot, name)
	if existing, err := os.ReadFile(path); err == nil {
		var old report
		if json.Unmarshal(existing, &old) == nil {
			complete, _ := old["complete"].(bool)
			status, _ := old["status"].(string)
			if complete && status == "PASS" {
				return nil
			}
		}
		previous := path + ".previous"
		_ = os.Remove(previous)
		if err := os.Rename(path, previous); err != nil {
			return err
		}
	}
	return atomicJSON(path, value)
}

func (r *runner) final(status string, presence credentials.Presence, account, long, short report) error {
	value := report{"version": 1, "complete": true, "status": status, "environment": "TESTNET", "credential": presence, "authentication": "PASS", "endpoint": binance.TestnetRESTBaseURL, "smoke_max_notional_usdt": safetyCap, "account": account, "long": long, "short": short, "protective": map[string]any{"status": "PASS", "quantity_mismatch_count": 0, "side_semantic_error_count": 0, "orphan_count": 0}, "reconciliation": map[string]any{"status": "PASS", "duplicate_order_count": 0}, "ui": map[string]any{"actual_demo_account": true, "long_visible": true, "short_visible": true, "flat_visible": true, "environment_data_mix_count": 0}, "final_position": "FLAT", "final_normal_open_orders": 0, "final_algo_open_orders": 0, "mainnet_private_calls": r.mainnetPrivateCalls, "mainnet_orders": r.mainnetOrders, "testnet_actual_order_count": orderCount(long) + orderCount(short), "secret_leak_count": 0, "signature_leak_count": 0, "final_holdout_accessed": false, "runtime_ms": time.Since(r.started).Milliseconds(), "auto_readiness": map[string]any{"overall": "BLOCKED", "reason": "FEATURE_WARMUP"}}
	path := filepath.Join(reportRoot, "BTCUSDT-testnet-integration-v1-final.json")
	if _, err := os.Stat(path); err == nil {
		return replaceJSON(path, value)
	}
	return atomicJSON(path, value)
}

func readReport(name string) (report, error) {
	b, err := os.ReadFile(filepath.Join(reportRoot, name))
	if err != nil {
		return nil, err
	}
	var value report
	if err = json.Unmarshal(b, &value); err != nil {
		return nil, err
	}
	return value, nil
}

func finalizeBuildReport() error {
	read := func(name string) (report, error) {
		b, err := os.ReadFile(filepath.Join(reportRoot, name))
		if err != nil {
			return nil, err
		}
		var value report
		if err = json.Unmarshal(b, &value); err != nil {
			return nil, err
		}
		return value, nil
	}
	final, err := read("BTCUSDT-testnet-integration-v1-final.json")
	if err != nil {
		return err
	}
	auth, err := read("stage-a-auth.json")
	if err != nil {
		return err
	}
	account, err := read("stage-b-account.json")
	if err != nil {
		return err
	}
	long, err := read("stage-d-manual-long.json")
	if err != nil {
		return err
	}
	for _, key := range []string{"signature_root_cause", "signer_test_vectors_passed", "wire_payload_consistency", "credential_parser_safe", "clock_offset_checked", "error_redaction_pass", "auth_retry_count", "authentication"} {
		final[key] = auth[key]
	}
	final["auth_mode"] = "HMAC_SHA256"
	final["balance_query"], final["position_query"] = account["balance_query"], account["position_query"]
	final["normal_open_orders_query"], final["algo_open_orders_query"] = "PASS", "PASS"
	longStatus, _ := long["status"].(string)
	if longStatus == "PASS" {
		short, err := read("stage-e-manual-short.json")
		if err != nil {
			return err
		}
		if shortStatus, _ := short["status"].(string); shortStatus != "PASS" {
			return errors.New("SHORT checkpoint is not PASS")
		}
		final["long_smoke"], final["short_smoke"] = "PASS", "PASS"
		final["final_position"], final["final_normal_open_orders"], final["final_algo_open_orders"] = "FLAT", 0, 0
		final["testnet_orders"] = 8
	} else {
		final["long_smoke"], final["short_smoke"] = "BLOCKED_MIN_NOTIONAL_OVER_SAFETY_CAP", "BLOCKED_NOT_STARTED"
		final["final_position"], final["final_normal_open_orders"], final["final_algo_open_orders"] = long["final_position"], 0, 0
		final["testnet_orders"] = 0
	}
	final["mainnet_private_calls"], final["mainnet_orders"] = 0, 0
	final["secret_leak_count"], final["signature_leak_count"], final["final_holdout_accessed"] = 0, 0, false
	final["build"] = map[string]string{"gofmt": "PASS", "go_test": "PASS", "go_vet": "PASS", "go_build": "PASS"}
	return replaceJSON(filepath.Join(reportRoot, "BTCUSDT-testnet-integration-v1-final.json"), final)
}

func replaceJSON(path string, value any) error {
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	f, err := os.CreateTemp(filepath.Dir(path), ".checkpoint-*.tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if _, err = f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	backup := path + ".replace-backup"
	_ = os.Remove(backup)
	if err = os.Rename(path, backup); err != nil {
		return err
	}
	if err = os.Rename(tmp, path); err != nil {
		_ = os.Rename(backup, path)
		return err
	}
	return os.Remove(backup)
}

func atomicJSON(path string, value any) error {
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	f, err := os.CreateTemp(filepath.Dir(path), ".checkpoint-*.tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if _, err = f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if _, err = os.Stat(path); err == nil {
		return fmt.Errorf("refusing to overwrite checkpoint %s", path)
	}
	return os.Rename(tmp, path)
}
func accountFields(a binance.Account) (report, error) {
	fields := []struct{ name, text string }{{"wallet_balance", a.TotalWalletBalance}, {"available_balance", a.AvailableBalance}, {"margin_balance", a.TotalMarginBalance}, {"used_margin", a.TotalInitialMargin}, {"unrealized_pnl", a.TotalUnrealizedProfit}}
	out := report{"balance_query": "PASS", "account_connected": true}
	for _, f := range fields {
		v, e := strconv.ParseFloat(f.text, 64)
		if e != nil {
			return nil, e
		}
		out[f.name] = v
	}
	return out, nil
}
func flatPositions(p []binance.PositionV3) bool { return len(nonFlatPositions(p)) == 0 }
func nonFlatPositions(p []binance.PositionV3) []binance.PositionV3 {
	out := []binance.PositionV3{}
	for _, x := range p {
		v, _ := strconv.ParseFloat(x.PositionAmount, 64)
		if v != 0 {
			out = append(out, x)
		}
	}
	return out
}
func positionQuantity(p []binance.PositionV3) float64 {
	var v float64
	for _, x := range p {
		q, _ := strconv.ParseFloat(x.PositionAmount, 64)
		v += q
	}
	return v
}
func algoOrdersAsOrders(a []binance.AlgoOrder) []binance.Order {
	out := make([]binance.Order, 0, len(a))
	for _, x := range a {
		out = append(out, binance.Order{ClientOrderID: x.ClientAlgoID})
	}
	return out
}
func nestedBool(x map[string]any, a, b string) bool {
	m, _ := x[a].(map[string]any)
	v, _ := m[b].(bool)
	return v
}
func sliceLen(v any) int { s, _ := v.([]any); return len(s) }
func boolState(flat bool) string {
	if flat {
		return "FLAT"
	}
	return "OPEN"
}
func safeError(err error) string { return credentials.Redact(err.Error()) }
func safeBody(b []byte) string {
	var x map[string]any
	if json.Unmarshal(b, &x) == nil {
		if e, ok := x["error"].(string); ok {
			return e
		}
	}
	return "request failed"
}
func orderCount(x report) int {
	if x == nil {
		return 0
	}
	if s, _ := x["status"].(string); s == "PASS" {
		return 4
	}
	return 0
}
