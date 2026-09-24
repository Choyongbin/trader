package uiapi

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"testing"
	"testing/fstest"
	"time"

	"binance_trader/internal/credentials"
	"binance_trader/internal/live/autopipeline"
	"binance_trader/internal/live/binance"
)

// TestDemoAutoExecutionSmoke is opt-in because it creates real Binance Futures
// Demo orders. It never targets Mainnet. The test submits one deterministic
// ExecutionIntent through the same Server auto-execution path, verifies entry +
// TP/SL, then immediately flattens and verifies zero residual position/orders.
func TestDemoAutoExecutionSmoke(t *testing.T) {
	if os.Getenv("RUN_DEMO_AUTO_SMOKE") != "1" {
		t.Skip("set RUN_DEMO_AUTO_SMOKE=1 for explicit Binance Demo execution")
	}
	if os.Getenv("BINANCE_TESTNET_ENABLE_ORDERS") != "true" || os.Getenv("BINANCE_TESTNET_ENABLE_AUTO_ORDERS") != "true" {
		t.Fatal("both Demo order gates must be explicitly enabled")
	}
	provider := credentials.NewFileProvider()
	store, err := provider.Load()
	if err != nil || !store.Testnet.Available() {
		t.Fatal("Demo credentials unavailable")
	}
	backend, err := NewBinanceTestnetBackend(store.Testnet.APIKey, store.Testnet.APISecret)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if _, err = backend.SyncTime(ctx); err != nil {
		t.Fatal(err)
	}
	balance, positions, orders, err := backend.Refresh(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !balance.Connected || len(positions) != 0 || len(orders) != 0 {
		t.Fatalf("Demo account must start flat and clean: connected=%t positions=%d orders=%d", balance.Connected, len(positions), len(orders))
	}
	mark, err := backend.client.MarkPrice(ctx, "BTCUSDT")
	if err != nil || mark <= 0 {
		t.Fatal("Demo mark price unavailable")
	}
	symbol, _, err := backend.client.ExchangeSymbol(ctx, "BTCUSDT")
	if err != nil {
		t.Fatal(err)
	}
	quantityFilter, err := binance.QuantityFilter(symbol, true)
	if err != nil {
		t.Fatal(err)
	}
	minQty, err := strconv.ParseFloat(quantityFilter.MinQty, 64)
	if err != nil || minQty <= 0 {
		t.Fatal("Demo minimum quantity unavailable")
	}
	notional := math.Max(100, minQty*mark*1.20)
	if notional > 200 {
		t.Fatalf("Demo minimum order exceeds 200 USDT smoke safety cap: %.2f", notional)
	}

	static := fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("console")}}
	s, err := NewServer(provider, static, Options{TestnetBackend: backend})
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.states[TradingEnvironmentTestnet].AutoRunning = true
	s.states[TradingEnvironmentTestnet].AutoState = "RUNNING"
	s.mu.Unlock()

	intent := autopipeline.ExecutionIntent{
		Environment:         "TESTNET",
		Symbol:              "BTCUSDT",
		Side:                "LONG",
		CandidateID:         "DEMO_AUTO_SMOKE",
		QuantityBTC:         notional / mark,
		NotionalUSDT:        notional,
		RiskBudgetUSDT:      1,
		Leverage:            1,
		MarginUSDT:          notional,
		EntryReferencePrice: mark,
		TPPrice:             mark * 1.03,
		SLPrice:             mark * 0.97,
		HorizonSeconds:      60,
		ClientOrderID:       "demo-auto-smoke",
		ModelProfileID:      autopipeline.ProfileID,
		DecisionTimestampMs: time.Now().UnixMilli(),
	}
	execution, err := s.executeAutoIntent(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	if execution.Order.Status == "" || len(execution.Protective) != 2 {
		t.Fatalf("auto execution incomplete: order=%+v protective=%d", execution.Order, len(execution.Protective))
	}
	closeID := fmt.Sprintf("demo-auto-close-%d", time.Now().UnixMilli())
	if _, err = backend.ClosePosition(ctx, closeID); err != nil {
		t.Fatal(err)
	}
	_, finalPositions, finalOrders, err := backend.Refresh(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(finalPositions) != 0 || len(finalOrders) != 0 {
		t.Fatalf("Demo cleanup incomplete: positions=%d orders=%d", len(finalPositions), len(finalOrders))
	}
	t.Logf("PASS entry=%s protective=%d final_positions=0 final_orders=0", execution.Order.Status, len(execution.Protective))
}
