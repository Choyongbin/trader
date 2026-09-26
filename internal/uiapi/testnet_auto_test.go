package uiapi

import (
	"context"
	"errors"
	"math"
	"strconv"
	"testing"
	"time"

	"binance_trader/internal/live/autopipeline"
	"binance_trader/internal/live/binance"
)

func TestValidAutoIntent(t *testing.T) {
	base := autopipeline.ExecutionIntent{
		Environment:         "TESTNET",
		Symbol:              "BTCUSDT",
		Side:                "LONG",
		CandidateID:         "fixture",
		QuantityBTC:         .001,
		NotionalUSDT:        100,
		RiskBudgetUSDT:      1,
		Leverage:            2,
		MarginUSDT:          50,
		EntryReferencePrice: 100000,
		TPPrice:             101000,
		SLPrice:             99000,
		HorizonSeconds:      900,
		ModelProfileID:      autopipeline.ProfileID,
		DecisionTimestampMs: time.Now().UnixMilli(),
	}
	if err := validAutoIntent(base); err != nil {
		t.Fatal(err)
	}
	bad := base
	bad.ModelProfileID = "other"
	if err := validAutoIntent(bad); err == nil {
		t.Fatal("wrong model profile accepted")
	}
	bad = base
	bad.TPPrice = 99000
	if err := validAutoIntent(bad); err == nil {
		t.Fatal("invalid LONG protective prices accepted")
	}
	bad = base
	bad.Side = "SHORT"
	bad.TPPrice = 99000
	bad.SLPrice = 101000
	if err := validAutoIntent(bad); err != nil {
		t.Fatal(err)
	}
}

type autoCleanupFakeClient struct {
	positionAmount string
	algos          []binance.AlgoOrder
}

func (*autoCleanupFakeClient) SyncTime(context.Context) (int64, error) { return 0, nil }
func (*autoCleanupFakeClient) Account(context.Context) (binance.Account, error) {
	return binance.Account{TotalWalletBalance: "1000", TotalUnrealizedProfit: "0", TotalMarginBalance: "1000", TotalInitialMargin: "0", AvailableBalance: "1000"}, nil
}
func (c *autoCleanupFakeClient) Positions(context.Context, string) ([]binance.PositionV3, error) {
	if c.positionAmount == "" || c.positionAmount == "0" {
		return nil, nil
	}
	quantity, _ := strconv.ParseFloat(c.positionAmount, 64)
	return []binance.PositionV3{{Symbol: "BTCUSDT", PositionAmount: c.positionAmount, EntryPrice: "100000", MarkPrice: "100000", Notional: strconv.FormatFloat(quantity*100000, 'f', -1, 64), InitialMargin: "100", UnrealizedProfit: "0", LiquidationPrice: "50000", Leverage: "1"}}, nil
}
func (*autoCleanupFakeClient) OpenOrders(context.Context, string) ([]binance.Order, error) {
	return nil, nil
}
func (c *autoCleanupFakeClient) OpenAlgoOrders(context.Context, string) ([]binance.AlgoOrder, error) {
	return append([]binance.AlgoOrder(nil), c.algos...), nil
}
func (*autoCleanupFakeClient) PositionMode(context.Context) (binance.PositionMode, error) {
	return binance.PositionMode{}, nil
}
func (*autoCleanupFakeClient) ExchangeSymbol(context.Context, string) (binance.Symbol, int64, error) {
	return binance.Symbol{Symbol: "BTCUSDT", Status: "TRADING", BaseAsset: "BTC", QuoteAsset: "USDT", ContractType: "PERPETUAL", PricePrecision: 1, QuantityPrecision: 3, Filters: []binance.Filter{{FilterType: "PRICE_FILTER", MinPrice: "0.1", MaxPrice: "1000000", TickSize: "0.1"}, {FilterType: "LOT_SIZE", MinQty: "0.001", MaxQty: "100", StepSize: "0.001"}, {FilterType: "MIN_NOTIONAL", Notional: "5"}}}, 0, nil
}
func (*autoCleanupFakeClient) MarkPrice(context.Context, string) (float64, error) {
	return 100000, nil
}

type autoCleanupFakeBroker struct {
	client        *autoCleanupFakeClient
	scenario      string
	entry         binance.Order
	canceled      []string
	closeCalls    int
	closeSide     string
	closeQuantity string
}

func (*autoCleanupFakeBroker) SetLeverage(context.Context, string, int) error { return nil }
func (b *autoCleanupFakeBroker) MarketEntry(ctx context.Context, symbol, side, quantity, clientID string) (binance.Order, error) {
	b.entry = binance.Order{Symbol: symbol, Side: side, Type: "MARKET", ClientOrderID: clientID, Status: "FILLED", OriginalQuantity: quantity, ExecutedQuantity: quantity, AveragePrice: "100000", UpdateTime: 1_700_000_000_000}
	positionQuantity := quantity
	if side == "SELL" {
		positionQuantity = "-" + quantity
	}
	if b.scenario == "quantity_mismatch" {
		value, _ := strconv.ParseFloat(positionQuantity, 64)
		positionQuantity = strconv.FormatFloat(value*2, 'f', 3, 64)
	}
	if b.scenario == "side_mismatch" {
		value, _ := strconv.ParseFloat(positionQuantity, 64)
		positionQuantity = strconv.FormatFloat(-value, 'f', 3, 64)
	}
	b.client.positionAmount = positionQuantity
	if b.scenario == "entry_unknown" {
		return binance.Order{}, binance.ErrSubmitOutcomeUnknown
	}
	if b.scenario == "partial_fill" {
		b.entry.Status = "PARTIALLY_FILLED"
		b.entry.ExecutedQuantity = "0.0005"
		return b.entry, nil
	}
	if b.scenario == "entry_response_lost" || b.scenario == "recovery_context_expired" {
		return binance.Order{}, context.DeadlineExceeded
	}
	return b.entry, nil
}
func (b *autoCleanupFakeBroker) ReduceOnlyMarketExit(_ context.Context, _, side, quantity, _ string) (binance.Order, error) {
	b.closeCalls++
	b.closeSide = side
	b.closeQuantity = quantity
	b.client.positionAmount = "0"
	return binance.Order{Status: "FILLED", ReduceOnly: true}, nil
}
func (b *autoCleanupFakeBroker) TakeProfit(_ context.Context, symbol, side, quantity, price, clientID string) (binance.Order, error) {
	if b.scenario == "unrelated_manual" {
		b.client.algos = append(b.client.algos, binance.AlgoOrder{Symbol: symbol, ClientAlgoID: "manual-unrelated", AlgoStatus: "NEW", OrderType: "STOP_MARKET"})
	}
	if b.scenario == "tp_failure" || b.scenario == "unrelated_manual" || b.scenario == "quantity_mismatch" || b.scenario == "side_mismatch" || b.scenario == "entry_side_missing" || b.scenario == "order_quantity_mismatch" {
		return binance.Order{}, errors.New("TP rejected")
	}
	if b.scenario == "already_flat_tp_failure" {
		b.client.positionAmount = "0"
		b.client.algos = append(b.client.algos, binance.AlgoOrder{Symbol: symbol, ClientAlgoID: "manual-unrelated", Side: side, Quantity: quantity, TriggerPrice: "90000", AlgoStatus: "NEW", OrderType: "STOP_MARKET", ReduceOnly: true})
		return binance.Order{}, errors.New("TP rejected after exchange-side close")
	}
	order := binance.AlgoOrder{Symbol: symbol, Side: side, Quantity: quantity, TriggerPrice: price, ClientAlgoID: clientID, AlgoStatus: "NEW", OrderType: "TAKE_PROFIT_MARKET", ReduceOnly: true}
	b.client.algos = append(b.client.algos, order)
	if b.scenario == "tp_unknown" {
		return binance.Order{}, binance.ErrSubmitOutcomeUnknown
	}
	return binance.Order{Symbol: symbol, Side: side, Type: order.OrderType, ClientOrderID: clientID, Status: "NEW", OriginalQuantity: quantity, StopPrice: price, ReduceOnly: true}, nil
}
func (b *autoCleanupFakeBroker) ProtectiveStop(_ context.Context, symbol, side, quantity, price, clientID string) (binance.Order, error) {
	if b.scenario == "sl_failure" {
		return binance.Order{}, errors.New("SL rejected")
	}
	order := binance.AlgoOrder{Symbol: symbol, Side: side, Quantity: quantity, TriggerPrice: price, ClientAlgoID: clientID, AlgoStatus: "NEW", OrderType: "STOP_MARKET", ReduceOnly: true}
	b.client.algos = append(b.client.algos, order)
	if b.scenario == "sl_unknown" {
		return binance.Order{}, binance.ErrSubmitOutcomeUnknown
	}
	return binance.Order{Symbol: symbol, Side: side, Type: order.OrderType, ClientOrderID: clientID, Status: "NEW", OriginalQuantity: quantity, StopPrice: price, ReduceOnly: true}, nil
}
func (b *autoCleanupFakeBroker) CancelAlgo(_ context.Context, clientID string) (binance.AlgoOrder, error) {
	b.canceled = append(b.canceled, clientID)
	remaining := b.client.algos[:0]
	for _, order := range b.client.algos {
		if order.ClientAlgoID != clientID {
			remaining = append(remaining, order)
		}
	}
	b.client.algos = remaining
	return binance.AlgoOrder{ClientAlgoID: clientID, AlgoStatus: "CANCELED"}, nil
}
func (b *autoCleanupFakeBroker) GetOrder(ctx context.Context, _ string, _ string) (binance.Order, error) {
	if b.scenario == "entry_unknown" {
		return binance.Order{}, errors.New("entry lookup timeout")
	}
	if b.scenario == "recovery_context_expired" {
		<-ctx.Done()
		return binance.Order{}, ctx.Err()
	}
	entry := b.entry
	if b.scenario == "entry_side_missing" {
		entry.Side = ""
	}
	if b.scenario == "order_quantity_mismatch" {
		entry.OriginalQuantity = "0.002"
	}
	return entry, nil
}

func TestManualEntryRejectsUnparseablePositionAmount(t *testing.T) {
	for _, amount := range []string{"not-a-number", "NaN", "+Inf"} {
		t.Run(amount, func(t *testing.T) {
			client := &autoCleanupFakeClient{positionAmount: amount}
			broker := &autoCleanupFakeBroker{client: client}
			backend := &BinanceTestnetBackend{client: client, broker: broker}
			_, err := backend.SubmitManual(context.Background(), ManualOrderRequest{Symbol: "BTCUSDT", Side: "LONG", MarginAmountUSDT: 100, Leverage: 1, RequestID: "manual-invalid"})
			if err == nil || broker.entry.ClientOrderID != "" {
				t.Fatalf("invalid exchange position was not rejected before entry: err=%v entry=%+v", err, broker.entry)
			}
		})
	}
}

func TestManualCloseCancelsOnlyOwnedProtectiveOrders(t *testing.T) {
	client := &autoCleanupFakeClient{positionAmount: "0.001", algos: []binance.AlgoOrder{
		{Symbol: "BTCUSDT", ClientAlgoID: "owned-tp", Side: "SELL", Quantity: "0.001", TriggerPrice: "101000", AlgoStatus: "NEW", OrderType: "TAKE_PROFIT_MARKET", ReduceOnly: true},
		{Symbol: "BTCUSDT", ClientAlgoID: "manual-unrelated", Side: "SELL", Quantity: "0.001", TriggerPrice: "90000", AlgoStatus: "NEW", OrderType: "STOP_MARKET", ReduceOnly: true},
	}}
	broker := &autoCleanupFakeBroker{client: client}
	backend := &BinanceTestnetBackend{client: client, broker: broker}
	if _, err := backend.ClosePositionOwned(context.Background(), "close-owned", []string{"owned-tp"}); err != nil {
		t.Fatal(err)
	}
	if len(broker.canceled) != 1 || broker.canceled[0] != "owned-tp" || len(client.algos) != 1 || client.algos[0].ClientAlgoID != "manual-unrelated" {
		t.Fatalf("ownership cleanup mismatch canceled=%v remaining=%+v", broker.canceled, client.algos)
	}
}

func TestSubmitAutoUsesIndependentBoundedRecoveryContext(t *testing.T) {
	for _, side := range []string{"LONG", "SHORT"} {
		t.Run(side+"/request_timeout_recovers", func(t *testing.T) {
			client := &autoCleanupFakeClient{}
			broker := &autoCleanupFakeBroker{client: client, scenario: "entry_response_lost"}
			backend := &BinanceTestnetBackend{client: client, broker: broker, autoRecoveryTimeout: 100 * time.Millisecond}
			_, err := backend.SubmitAuto(context.Background(), autoCleanupIntent(side, 2_000_000_000_000))
			if !errors.Is(err, errAutoCleanupConfirmed) || errors.Is(err, errUnknownExecutionState) {
				t.Fatalf("separate recovery did not confirm cleanup: %v", err)
			}
			if broker.closeCalls != 1 || client.positionAmount != "0" {
				t.Fatalf("closeCalls=%d position=%s", broker.closeCalls, client.positionAmount)
			}
		})
		t.Run(side+"/recovery_timeout_stays_unknown", func(t *testing.T) {
			client := &autoCleanupFakeClient{}
			broker := &autoCleanupFakeBroker{client: client, scenario: "recovery_context_expired"}
			backend := &BinanceTestnetBackend{client: client, broker: broker, autoRecoveryTimeout: 15 * time.Millisecond}
			started := time.Now()
			_, err := backend.SubmitAuto(context.Background(), autoCleanupIntent(side, 2_000_000_000_001))
			if !errors.Is(err, errUnknownExecutionState) || errors.Is(err, errAutoCleanupConfirmed) {
				t.Fatalf("recovery timeout was not unknown: %v", err)
			}
			if elapsed := time.Since(started); elapsed < 10*time.Millisecond || elapsed > time.Second {
				t.Fatalf("recovery timeout was not bounded: %v", elapsed)
			}
			if broker.closeCalls != 0 {
				t.Fatal("uncertain recovery closed the position")
			}
		})
	}
}

func TestSubmitAutoPreValidationFailureDoesNotSendEntry(t *testing.T) {
	client := &autoCleanupFakeClient{}
	broker := &autoCleanupFakeBroker{client: client}
	backend := &BinanceTestnetBackend{client: client, broker: broker}
	intent := autoCleanupIntent("LONG", 2_100_000_000_000)
	intent.QuantityBTC = 0
	_, err := backend.SubmitAuto(context.Background(), intent)
	if !errors.Is(err, errAutoEntryNotSubmitted) || broker.entry.ClientOrderID != "" || broker.closeCalls != 0 {
		t.Fatalf("pre-validation failure reached exchange: entry=%q closeCalls=%d err=%v", broker.entry.ClientOrderID, broker.closeCalls, err)
	}
}

func autoCleanupIntent(side string, timestamp int64) autopipeline.ExecutionIntent {
	tp, sl := 101000.0, 99000.0
	if side == "SHORT" {
		tp, sl = 99000, 101000
	}
	return autopipeline.ExecutionIntent{Environment: "TESTNET", Symbol: "BTCUSDT", Side: side, CandidateID: "cleanup-fixture", QuantityBTC: .001, NotionalUSDT: 100, RiskBudgetUSDT: 1, Leverage: 1, MarginUSDT: 100, EntryReferencePrice: 100000, TPPrice: tp, SLPrice: sl, HorizonSeconds: 900, ModelProfileID: autopipeline.ProfileID, DecisionTimestampMs: timestamp}
}

func TestSubmitAutoFailureCleanupPreservesOwnership(t *testing.T) {
	scenarios := []struct {
		name          string
		unknown       bool
		shouldClose   bool
		canceledOwned int
	}{
		{"entry_unknown", true, false, 0},
		{"partial_fill", true, false, 0},
		{"tp_failure", false, true, 0},
		{"tp_unknown", false, true, 1},
		{"sl_failure", false, true, 1},
		{"sl_unknown", false, true, 2},
		{"quantity_mismatch", true, false, 0},
		{"side_mismatch", true, false, 0},
		{"entry_side_missing", true, false, 0},
		{"order_quantity_mismatch", true, false, 0},
		{"unrelated_manual", false, true, 0},
	}
	for _, side := range []string{"LONG", "SHORT"} {
		for index, scenario := range scenarios {
			t.Run(side+"/"+scenario.name, func(t *testing.T) {
				client := &autoCleanupFakeClient{}
				broker := &autoCleanupFakeBroker{client: client, scenario: scenario.name}
				backend := &BinanceTestnetBackend{client: client, broker: broker}
				intent := autoCleanupIntent(side, 1_800_000_000_000+int64(index))
				_, err := backend.SubmitAuto(context.Background(), intent)
				if err == nil {
					t.Fatal("failure fixture unexpectedly succeeded")
				}
				if errors.Is(err, errUnknownExecutionState) != scenario.unknown {
					t.Fatalf("unknown=%t want=%t err=%v", errors.Is(err, errUnknownExecutionState), scenario.unknown, err)
				}
				if errors.Is(err, errAutoCleanupConfirmed) != scenario.shouldClose {
					t.Fatalf("cleanupConfirmed=%t want=%t err=%v", errors.Is(err, errAutoCleanupConfirmed), scenario.shouldClose, err)
				}
				if (broker.closeCalls == 1) != scenario.shouldClose {
					t.Fatalf("closeCalls=%d wantClose=%t", broker.closeCalls, scenario.shouldClose)
				}
				if scenario.shouldClose {
					expectedSide := "SELL"
					if side == "SHORT" {
						expectedSide = "BUY"
					}
					if broker.closeSide != expectedSide || broker.closeQuantity != "0.001" || client.positionAmount != "0" {
						t.Fatalf("unsafe close side=%s quantity=%s position=%s", broker.closeSide, broker.closeQuantity, client.positionAmount)
					}
				}
				for _, canceled := range broker.canceled {
					if canceled == "manual-unrelated" {
						t.Fatal("unrelated manual algo order was canceled")
					}
					if canceled != binance.DeterministicClientOrderID("auto", intent.DecisionTimestampMs, intent.CandidateID, "tp", 0) && canceled != binance.DeterministicClientOrderID("auto", intent.DecisionTimestampMs, intent.CandidateID, "sl", 0) {
						t.Fatalf("non-owned algo order was canceled: %s", canceled)
					}
				}
				if len(broker.canceled) != scenario.canceledOwned {
					t.Fatalf("owned canceled=%d want=%d", len(broker.canceled), scenario.canceledOwned)
				}
				if scenario.name == "unrelated_manual" {
					found := false
					for _, order := range client.algos {
						found = found || order.ClientAlgoID == "manual-unrelated"
					}
					if !found {
						t.Fatal("unrelated manual algo order was not preserved")
					}
				}
			})
		}
	}
}

func TestDefaultAutoRecoveryTimeoutIsBounded(t *testing.T) {
	if defaultAutoFailureRecoveryTimeout != 12*time.Second {
		t.Fatalf("timeout=%v", defaultAutoFailureRecoveryTimeout)
	}
}

func TestSubmitAutoNormalEntryStillSucceeds(t *testing.T) {
	for index, side := range []string{"LONG", "SHORT"} {
		t.Run(side, func(t *testing.T) {
			client := &autoCleanupFakeClient{}
			broker := &autoCleanupFakeBroker{client: client}
			backend := &BinanceTestnetBackend{client: client, broker: broker}
			execution, err := backend.SubmitAuto(context.Background(), autoCleanupIntent(side, 1_900_000_000_000+int64(index)))
			if err != nil {
				t.Fatal(err)
			}
			if execution.Position.Side != side || math.Abs(execution.Position.QuantityBTC-.001) > 1e-12 || len(execution.Protective) != 2 || broker.closeCalls != 0 {
				t.Fatalf("execution=%+v protective=%d closeCalls=%d", execution.Position, len(execution.Protective), broker.closeCalls)
			}
		})
	}
}

func TestSubmitAutoFailureAlreadyFlatDoesNotSendExit(t *testing.T) {
	for index, side := range []string{"LONG", "SHORT"} {
		t.Run(side, func(t *testing.T) {
			client := &autoCleanupFakeClient{}
			broker := &autoCleanupFakeBroker{client: client, scenario: "already_flat_tp_failure"}
			backend := &BinanceTestnetBackend{client: client, broker: broker}
			_, err := backend.SubmitAuto(context.Background(), autoCleanupIntent(side, 2_200_000_000_000+int64(index)))
			if !errors.Is(err, errAutoCleanupConfirmed) || broker.closeCalls != 0 {
				t.Fatalf("err=%v closeCalls=%d", err, broker.closeCalls)
			}
			if len(client.algos) != 1 || client.algos[0].ClientAlgoID != "manual-unrelated" || len(broker.canceled) != 0 {
				t.Fatalf("manual order changed: algos=%+v canceled=%v", client.algos, broker.canceled)
			}
		})
	}
}
