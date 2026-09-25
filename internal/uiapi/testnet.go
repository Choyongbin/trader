package uiapi

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"binance_trader/internal/live/autopipeline"
	"binance_trader/internal/live/binance"
)

type ManualExecution struct {
	Order      Order         `json:"order"`
	Position   Position      `json:"position"`
	Protective []Order       `json:"protective_orders"`
	RawOrder   binance.Order `json:"-"`
}

type TestnetBackend interface {
	Refresh(context.Context) (Balance, []Position, []Order, error)
	SubmitManual(context.Context, ManualOrderRequest) (ManualExecution, error)
	ClosePosition(context.Context, string) (binance.Order, error)
}

// AutoTestnetBackend is deliberately separate from TestnetBackend so read/manual
// fixtures do not silently become capable of automatic order submission. Only
// the real Demo backend (and explicit tests) implement this interface.
type AutoTestnetBackend interface {
	SubmitAuto(context.Context, autopipeline.ExecutionIntent) (ManualExecution, error)
	CleanupAutoProtective(context.Context, []string) error
}

type preparedAutoTestnetBackend interface {
	AutoTestnetBackend
	PrepareAuto(context.Context, autopipeline.ExecutionIntent) (autoSubmissionPlan, error)
	SubmitPreparedAuto(context.Context, autoSubmissionPlan) (ManualExecution, error)
}

type AutoPositionCloser interface {
	CloseAutoPosition(context.Context, string, []string, string, float64) (binance.Order, error)
}

type testnetClient interface {
	SyncTime(context.Context) (int64, error)
	Account(context.Context) (binance.Account, error)
	Positions(context.Context, string) ([]binance.PositionV3, error)
	OpenOrders(context.Context, string) ([]binance.Order, error)
	OpenAlgoOrders(context.Context, string) ([]binance.AlgoOrder, error)
	PositionMode(context.Context) (binance.PositionMode, error)
	ExchangeSymbol(context.Context, string) (binance.Symbol, int64, error)
	MarkPrice(context.Context, string) (float64, error)
}

type testnetBroker interface {
	SetLeverage(context.Context, string, int) error
	MarketEntry(context.Context, string, string, string, string) (binance.Order, error)
	ReduceOnlyMarketExit(context.Context, string, string, string, string) (binance.Order, error)
	ProtectiveStop(context.Context, string, string, string, string, string) (binance.Order, error)
	TakeProfit(context.Context, string, string, string, string, string) (binance.Order, error)
	CancelAlgo(context.Context, string) (binance.AlgoOrder, error)
	GetOrder(context.Context, string, string) (binance.Order, error)
}

type BinanceTestnetBackend struct {
	client              testnetClient
	broker              testnetBroker
	autoRecoveryTimeout time.Duration
}

var (
	errAutoCleanupConfirmed  = errors.New("AUTO_CLEANUP_CONFIRMED")
	errUnknownExecutionState = errors.New("UNKNOWN_EXECUTION_STATE")
	errAutoEntryNotSubmitted = errors.New("AUTO_ENTRY_NOT_SUBMITTED")
)

const defaultAutoFailureRecoveryTimeout = 12 * time.Second

func NewBinanceTestnetBackend(apiKey, secret string) (*BinanceTestnetBackend, error) {
	client, err := binance.NewTestnetClient(apiKey, secret)
	if err != nil {
		return nil, err
	}
	broker, err := binance.NewBinanceTestnetBroker(client)
	if err != nil {
		return nil, err
	}
	return &BinanceTestnetBackend{client: client, broker: broker}, nil
}

func (b *BinanceTestnetBackend) SyncTime(ctx context.Context) (int64, error) {
	return b.client.SyncTime(ctx)
}

func parseNumber(name, text string) (float64, error) {
	v, err := strconv.ParseFloat(text, 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, fmt.Errorf("invalid %s", name)
	}
	return v, nil
}

func (b *BinanceTestnetBackend) Refresh(ctx context.Context) (Balance, []Position, []Order, error) {
	account, err := b.client.Account(ctx)
	if err != nil {
		return Balance{Status: "ACCOUNT UNAVAILABLE"}, nil, nil, err
	}
	wallet, err := parseNumber("wallet balance", account.TotalWalletBalance)
	if err != nil {
		return Balance{}, nil, nil, err
	}
	available, err := parseNumber("available balance", account.AvailableBalance)
	if err != nil {
		return Balance{}, nil, nil, err
	}
	margin, err := parseNumber("margin balance", account.TotalMarginBalance)
	if err != nil {
		return Balance{}, nil, nil, err
	}
	used, err := parseNumber("initial margin", account.TotalInitialMargin)
	if err != nil {
		return Balance{}, nil, nil, err
	}
	pnl, err := parseNumber("unrealized pnl", account.TotalUnrealizedProfit)
	if err != nil {
		return Balance{}, nil, nil, err
	}
	resultBalance := Balance{true, "CONNECTED", &wallet, &available, &margin, &used, &pnl}

	rawPositions, err := b.client.Positions(ctx, "BTCUSDT")
	if err != nil {
		return Balance{Status: "ACCOUNT UNAVAILABLE"}, nil, nil, err
	}
	positions := make([]Position, 0, 1)
	for _, raw := range rawPositions {
		quantity, e := parseNumber("position quantity", raw.PositionAmount)
		if e != nil {
			return Balance{}, nil, nil, e
		}
		if quantity == 0 {
			continue
		}
		entry, e := parseNumber("entry price", raw.EntryPrice)
		if e != nil {
			return Balance{}, nil, nil, e
		}
		mark, e := parseNumber("mark price", raw.MarkPrice)
		if e != nil {
			return Balance{}, nil, nil, e
		}
		notional, e := parseNumber("notional", raw.Notional)
		if e != nil {
			return Balance{}, nil, nil, e
		}
		positionPNL, e := parseNumber("position pnl", raw.UnrealizedProfit)
		if e != nil {
			return Balance{}, nil, nil, e
		}
		liquidation, e := parseNumber("liquidation price", raw.LiquidationPrice)
		if e != nil {
			return Balance{}, nil, nil, e
		}
		leverage64, e := strconv.ParseInt(raw.Leverage, 10, 32)
		if e != nil || leverage64 <= 0 {
			return Balance{}, nil, nil, fmt.Errorf("invalid leverage")
		}
		initialMargin := math.Abs(notional) / float64(leverage64)
		if raw.InitialMargin != "" {
			initialMargin, e = parseNumber("position margin", raw.InitialMargin)
			if e != nil {
				return Balance{}, nil, nil, e
			}
		}
		side := "LONG"
		if quantity < 0 {
			side, quantity = "SHORT", -quantity
		}
		positions = append(positions, Position{Environment: TradingEnvironmentTestnet, Symbol: raw.Symbol, Side: side, QuantityBTC: quantity, NotionalUSDT: math.Abs(notional), EntryPrice: entry, MarkPrice: mark, UnrealizedPnL: positionPNL, Leverage: int(leverage64), MarginUSDT: initialMargin, LiquidationPrice: liquidation})
	}
	rawOrders, err := b.client.OpenOrders(ctx, "BTCUSDT")
	if err != nil {
		return Balance{Status: "ACCOUNT UNAVAILABLE"}, nil, nil, err
	}
	rawAlgo, err := b.client.OpenAlgoOrders(ctx, "BTCUSDT")
	if err != nil {
		return Balance{Status: "ACCOUNT UNAVAILABLE"}, nil, nil, err
	}
	orders := make([]Order, 0, len(rawOrders)+len(rawAlgo))
	for _, raw := range rawOrders {
		orders = append(orders, uiOrder(raw, ""))
	}
	for _, raw := range rawAlgo {
		qty, _ := strconv.ParseFloat(raw.Quantity, 64)
		price, _ := strconv.ParseFloat(raw.TriggerPrice, 64)
		protective := "SL"
		if raw.OrderType == "TAKE_PROFIT_MARKET" {
			protective = "TP"
		}
		orders = append(orders, Order{Time: time.UnixMilli(raw.CreateTime).UTC(), Environment: TradingEnvironmentTestnet, Symbol: raw.Symbol, Side: exchangeSide(raw.Side), OrderType: raw.OrderType, QuantityBTC: qty, Price: price, Status: raw.AlgoStatus, ClientOrderID: raw.ClientAlgoID, Protective: protective, ReduceOnly: raw.ReduceOnly})
	}
	return resultBalance, positions, orders, nil
}

func uiOrder(raw binance.Order, protective string) Order {
	qty, _ := strconv.ParseFloat(raw.ExecutedQuantity, 64)
	if qty == 0 {
		qty, _ = strconv.ParseFloat(raw.OriginalQuantity, 64)
	}
	price, _ := strconv.ParseFloat(raw.AveragePrice, 64)
	if price == 0 {
		price, _ = strconv.ParseFloat(raw.Price, 64)
	}
	if price == 0 && protective != "" {
		price, _ = strconv.ParseFloat(raw.StopPrice, 64)
	}
	orderTime := time.Now().UTC()
	if raw.UpdateTime > 0 {
		orderTime = time.UnixMilli(raw.UpdateTime).UTC()
	} else if raw.Time > 0 {
		orderTime = time.UnixMilli(raw.Time).UTC()
	}
	return Order{Time: orderTime, Environment: TradingEnvironmentTestnet, Symbol: raw.Symbol, Side: exchangeSide(raw.Side), OrderType: raw.Type, QuantityBTC: qty, Price: price, Status: raw.Status, ClientOrderID: raw.ClientOrderID, Protective: protective, ReduceOnly: raw.ReduceOnly}
}

func exchangeSide(side string) string {
	if side == "BUY" {
		return "LONG"
	}
	return "SHORT"
}

func (b *BinanceTestnetBackend) SubmitManual(ctx context.Context, request ManualOrderRequest) (ManualExecution, error) {
	mode, err := b.client.PositionMode(ctx)
	if err != nil {
		return ManualExecution{}, err
	}
	if mode.DualSidePosition {
		return ManualExecution{}, fmt.Errorf("Hedge Mode is not supported by this safety path")
	}
	positionsRaw, err := b.client.Positions(ctx, "BTCUSDT")
	if err != nil {
		return ManualExecution{}, err
	}
	normalOrders, err := b.client.OpenOrders(ctx, "BTCUSDT")
	if err != nil {
		return ManualExecution{}, err
	}
	algoOrders, err := b.client.OpenAlgoOrders(ctx, "BTCUSDT")
	if err != nil {
		return ManualExecution{}, err
	}
	nonFlat := false
	for _, p := range positionsRaw {
		q, _ := strconv.ParseFloat(p.PositionAmount, 64)
		if q != 0 {
			nonFlat = true
		}
	}
	if nonFlat || len(normalOrders) != 0 || len(algoOrders) != 0 {
		return ManualExecution{}, fmt.Errorf("existing position or open order safety gate")
	}
	symbol, _, err := b.client.ExchangeSymbol(ctx, request.Symbol)
	if err != nil {
		return ManualExecution{}, err
	}
	mark, err := b.client.MarkPrice(ctx, request.Symbol)
	if err != nil {
		return ManualExecution{}, err
	}
	quantity, err := binance.NormalizeQuantityUp(symbol, request.MarginAmountUSDT*float64(request.Leverage)/mark, true)
	if err != nil {
		return ManualExecution{}, err
	}
	if err = binance.ValidateMinNotional(symbol, mark, quantity); err != nil {
		return ManualExecution{}, err
	}
	quantityText, err := binance.FormatQuantity(symbol, quantity, true)
	if err != nil {
		return ManualExecution{}, err
	}
	if err = b.broker.SetLeverage(ctx, request.Symbol, request.Leverage); err != nil {
		return ManualExecution{}, err
	}
	entrySide := "BUY"
	if request.Side == "SHORT" {
		entrySide = "SELL"
	}
	entry, err := b.broker.MarketEntry(ctx, request.Symbol, entrySide, quantityText, request.RequestID)
	if err != nil {
		cleanupErr := b.cleanupManualIfOpen(ctx, request.RequestID+"-uncertain-cleanup")
		if cleanupErr != nil {
			return ManualExecution{}, fmt.Errorf("entry state uncertain and cleanup failed: %v / %v", err, cleanupErr)
		}
		return ManualExecution{}, err
	}
	if entry.ExecutedQuantity == "" || entry.AveragePrice == "" || entry.AveragePrice == "0" {
		queried, queryErr := b.broker.GetOrder(ctx, request.Symbol, request.RequestID)
		if queryErr != nil {
			cleanupErr := b.cleanupManualIfOpen(ctx, request.RequestID+"-query-cleanup")
			if cleanupErr != nil {
				return ManualExecution{}, fmt.Errorf("entry fill query failed and cleanup failed: %v / %v", queryErr, cleanupErr)
			}
			return ManualExecution{}, fmt.Errorf("entry fill query failed; exchange reconciled: %w", queryErr)
		}
		entry = queried
	}
	executed, err := parseNumber("executed quantity", entry.ExecutedQuantity)
	if err != nil || executed <= 0 {
		cleanupErr := b.cleanupManualIfOpen(ctx, request.RequestID+"-fill-cleanup")
		if cleanupErr != nil {
			return ManualExecution{}, fmt.Errorf("entry fill quantity unavailable and cleanup failed: %v", cleanupErr)
		}
		return ManualExecution{}, fmt.Errorf("entry fill quantity unavailable; exchange reconciled")
	}
	fill, err := parseNumber("average fill price", entry.AveragePrice)
	if err != nil || fill <= 0 {
		cleanupErr := b.cleanupManualIfOpen(ctx, request.RequestID+"-price-cleanup")
		if cleanupErr != nil {
			return ManualExecution{}, fmt.Errorf("entry fill price unavailable and cleanup failed: %v", cleanupErr)
		}
		return ManualExecution{}, fmt.Errorf("entry fill price unavailable; exchange reconciled")
	}
	oppositeSide := "SELL"
	if request.Side == "SHORT" {
		oppositeSide = "BUY"
	}
	protective := make([]Order, 0, 2)
	create := func(cfg *ProtectiveOrderConfig, tp bool) error {
		if cfg == nil || !cfg.Enabled {
			return nil
		}
		trigger := protectivePrice(fill, request.Side, tp, *cfg)
		trigger, err = binance.NormalizePrice(symbol, trigger)
		if err != nil {
			return err
		}
		kind, id := "SL", request.RequestID+"-sl"
		if tp {
			kind, id = "TP", request.RequestID+"-tp"
		}
		var placed binance.Order
		executedText, formatErr := binance.FormatQuantity(symbol, executed, true)
		if formatErr != nil {
			return formatErr
		}
		triggerText, formatErr := binance.FormatPrice(symbol, trigger)
		if formatErr != nil {
			return formatErr
		}
		if tp {
			placed, err = b.broker.TakeProfit(ctx, request.Symbol, oppositeSide, executedText, triggerText, id)
		} else {
			placed, err = b.broker.ProtectiveStop(ctx, request.Symbol, oppositeSide, executedText, triggerText, id)
		}
		if err != nil {
			return err
		}
		protective = append(protective, uiOrder(placed, kind))
		return nil
	}
	if err = create(request.TakeProfit, true); err != nil {
		cleanupErr := b.cleanupManualOwnedPosition(ctx, request.RequestID+"-cleanup")
		if cleanupErr != nil {
			return ManualExecution{}, fmt.Errorf("TP creation failed and cleanup failed: %v / %v", err, cleanupErr)
		}
		return ManualExecution{}, fmt.Errorf("TP creation failed; entry cleaned up: %w", err)
	}
	if err = create(request.StopLoss, false); err != nil {
		cleanupErr := b.cleanupManualOwnedPosition(ctx, request.RequestID+"-cleanup")
		if cleanupErr != nil {
			return ManualExecution{}, fmt.Errorf("SL creation failed and cleanup failed: %v / %v", err, cleanupErr)
		}
		return ManualExecution{}, fmt.Errorf("SL creation failed; entry cleaned up: %w", err)
	}
	_, refreshed, _, err := b.Refresh(ctx)
	if err != nil || len(refreshed) != 1 {
		cleanupErr := b.cleanupManualIfOpen(ctx, request.RequestID+"-verify-cleanup")
		if cleanupErr != nil {
			return ManualExecution{}, fmt.Errorf("position verification failed and cleanup failed: %v", cleanupErr)
		}
		return ManualExecution{}, fmt.Errorf("position verification failed; exchange reconciled")
	}
	for _, order := range protective {
		price := order.Price
		if order.Protective == "TP" {
			refreshed[0].TakeProfit = &price
		}
		if order.Protective == "SL" {
			refreshed[0].StopLoss = &price
		}
	}
	return ManualExecution{Order: uiOrder(entry, ""), Position: refreshed[0], Protective: protective, RawOrder: entry}, nil
}

func validAutoIntent(intent autopipeline.ExecutionIntent) error {
	finitePositive := func(v float64) bool { return v > 0 && !math.IsNaN(v) && !math.IsInf(v, 0) }
	if intent.Environment != string(TradingEnvironmentTestnet) || intent.Symbol != "BTCUSDT" || (intent.Side != "LONG" && intent.Side != "SHORT") {
		return fmt.Errorf("invalid auto execution identity")
	}
	if intent.ModelProfileID != autopipeline.ProfileID || intent.CandidateID == "" || intent.DecisionTimestampMs <= 0 || intent.HorizonSeconds <= 0 || intent.Leverage < 1 || intent.Leverage > 125 {
		return fmt.Errorf("invalid auto execution policy identity")
	}
	for _, value := range []float64{intent.QuantityBTC, intent.NotionalUSDT, intent.RiskBudgetUSDT, intent.MarginUSDT, intent.EntryReferencePrice, intent.TPPrice, intent.SLPrice} {
		if !finitePositive(value) {
			return fmt.Errorf("invalid auto execution numeric input")
		}
	}
	if intent.Side == "LONG" && !(intent.TPPrice > intent.EntryReferencePrice && intent.SLPrice < intent.EntryReferencePrice) {
		return fmt.Errorf("invalid LONG protective prices")
	}
	if intent.Side == "SHORT" && !(intent.TPPrice < intent.EntryReferencePrice && intent.SLPrice > intent.EntryReferencePrice) {
		return fmt.Errorf("invalid SHORT protective prices")
	}
	return nil
}

type autoSubmissionPlan struct {
	intent       autopipeline.ExecutionIntent
	symbol       binance.Symbol
	quantity     float64
	quantityText string
	entrySide    string
	entryID      string
	tpID         string
	slID         string
}

func (b *BinanceTestnetBackend) verifyAutoExchangeFlat(ctx context.Context, symbol string) error {
	mode, err := b.client.PositionMode(ctx)
	if err != nil {
		return err
	}
	if mode.DualSidePosition {
		return fmt.Errorf("Hedge Mode is not supported by this safety path")
	}
	positions, err := b.client.Positions(ctx, symbol)
	if err != nil {
		return err
	}
	normalOrders, err := b.client.OpenOrders(ctx, symbol)
	if err != nil {
		return err
	}
	algoOrders, err := b.client.OpenAlgoOrders(ctx, symbol)
	if err != nil {
		return err
	}
	for _, position := range positions {
		quantity, parseErr := strconv.ParseFloat(position.PositionAmount, 64)
		if parseErr != nil || math.IsNaN(quantity) || math.IsInf(quantity, 0) || quantity != 0 {
			return fmt.Errorf("existing or invalid position safety gate")
		}
	}
	if len(normalOrders) != 0 || len(algoOrders) != 0 {
		return fmt.Errorf("existing position or open order safety gate")
	}
	return nil
}

// PrepareAuto performs only read-only validation and normalization. Its result
// is persisted by Server before SubmitPreparedAuto may cause an exchange side
// effect.
func (b *BinanceTestnetBackend) PrepareAuto(ctx context.Context, intent autopipeline.ExecutionIntent) (autoSubmissionPlan, error) {
	notSubmitted := func(err error) (autoSubmissionPlan, error) {
		return autoSubmissionPlan{}, fmt.Errorf("%w: %v", errAutoEntryNotSubmitted, err)
	}
	if err := validAutoIntent(intent); err != nil {
		return notSubmitted(err)
	}
	if err := b.verifyAutoExchangeFlat(ctx, intent.Symbol); err != nil {
		return notSubmitted(err)
	}
	symbol, _, err := b.client.ExchangeSymbol(ctx, intent.Symbol)
	if err != nil {
		return notSubmitted(err)
	}
	mark, err := b.client.MarkPrice(ctx, intent.Symbol)
	if err != nil {
		return notSubmitted(err)
	}
	requestedQty := intent.QuantityBTC
	if maxByCurrentMark := intent.NotionalUSDT / mark; maxByCurrentMark < requestedQty {
		requestedQty = maxByCurrentMark
	}
	quantity, err := binance.NormalizeQuantity(symbol, requestedQty, true)
	if err != nil {
		return notSubmitted(err)
	}
	if err = binance.ValidateMinNotional(symbol, mark, quantity); err != nil {
		return notSubmitted(err)
	}
	quantityText, err := binance.FormatQuantity(symbol, quantity, true)
	if err != nil {
		return notSubmitted(err)
	}
	entrySide := "BUY"
	if intent.Side == "SHORT" {
		entrySide = "SELL"
	}
	return autoSubmissionPlan{
		intent: intent, symbol: symbol, quantity: quantity, quantityText: quantityText, entrySide: entrySide,
		entryID: binance.DeterministicClientOrderID("auto", intent.DecisionTimestampMs, intent.CandidateID, "entry", 0),
		tpID:    binance.DeterministicClientOrderID("auto", intent.DecisionTimestampMs, intent.CandidateID, "tp", 0),
		slID:    binance.DeterministicClientOrderID("auto", intent.DecisionTimestampMs, intent.CandidateID, "sl", 0),
	}, nil
}

// SubmitAuto executes one frozen-policy ExecutionIntent on Binance Futures Demo.
// It reuses the same conservative exchange gates as manual execution, rounds the
// quantity down so current mark-price movement cannot increase the requested
// frozen-policy notional, and rebases TP/SL ratios to the actual average fill.
func (b *BinanceTestnetBackend) SubmitAuto(ctx context.Context, intent autopipeline.ExecutionIntent) (ManualExecution, error) {
	plan, err := b.PrepareAuto(ctx, intent)
	if err != nil {
		return ManualExecution{}, err
	}
	return b.SubmitPreparedAuto(ctx, plan)
}

func (b *BinanceTestnetBackend) SubmitPreparedAuto(ctx context.Context, plan autoSubmissionPlan) (ManualExecution, error) {
	intent, symbol := plan.intent, plan.symbol
	quantityText, entrySide := plan.quantityText, plan.entrySide
	entryID, tpID, slID := plan.entryID, plan.tpID, plan.slID
	if err := b.verifyAutoExchangeFlat(ctx, intent.Symbol); err != nil {
		return ManualExecution{}, fmt.Errorf("%w: %v", errAutoEntryNotSubmitted, err)
	}
	if err := b.broker.SetLeverage(ctx, intent.Symbol, intent.Leverage); err != nil {
		return ManualExecution{}, fmt.Errorf("%w: %v", errAutoEntryNotSubmitted, err)
	}
	ownership := autoCleanupOwnership{symbol: intent.Symbol, side: intent.Side, requestedQuantity: quantityText, entryID: entryID, tpID: tpID, slID: slID, decisionTimestampMs: intent.DecisionTimestampMs, candidateID: intent.CandidateID}
	entry, err := b.broker.MarketEntry(ctx, intent.Symbol, entrySide, quantityText, entryID)
	if err != nil {
		return ManualExecution{}, b.cleanupFailedAutoExecution(ctx, ownership, err)
	}
	if entry.ExecutedQuantity == "" || entry.AveragePrice == "" || entry.AveragePrice == "0" {
		queried, queryErr := b.broker.GetOrder(ctx, intent.Symbol, entryID)
		if queryErr != nil {
			return ManualExecution{}, b.cleanupFailedAutoExecution(ctx, ownership, fmt.Errorf("auto entry fill query failed: %w", queryErr))
		}
		entry = queried
	}
	if entry.ClientOrderID != entryID || entry.Symbol != intent.Symbol || entry.Side != entrySide || entry.Type != "MARKET" || entry.Status != "FILLED" || entry.OriginalQuantity != quantityText {
		return ManualExecution{}, b.cleanupFailedAutoExecution(ctx, ownership, fmt.Errorf("auto entry identity or fill status mismatch"))
	}
	if entry.UpdateTime <= 0 && entry.Time <= 0 {
		return ManualExecution{}, b.cleanupFailedAutoExecution(ctx, ownership, fmt.Errorf("auto entry fill timestamp unavailable"))
	}
	executed, err := parseNumber("executed quantity", entry.ExecutedQuantity)
	if err != nil || executed <= 0 {
		return ManualExecution{}, b.cleanupFailedAutoExecution(ctx, ownership, fmt.Errorf("auto entry fill quantity unavailable"))
	}
	fill, err := parseNumber("average fill price", entry.AveragePrice)
	if err != nil || fill <= 0 {
		return ManualExecution{}, b.cleanupFailedAutoExecution(ctx, ownership, fmt.Errorf("auto entry fill price unavailable"))
	}

	cleanupAfterEntry := func(cause error) (ManualExecution, error) {
		return ManualExecution{}, b.cleanupFailedAutoExecution(ctx, ownership, cause)
	}
	executedText, err := binance.FormatQuantity(symbol, executed, true)
	if err != nil {
		return cleanupAfterEntry(fmt.Errorf("auto executed quantity format failed: %w", err))
	}
	tp, err := binance.NormalizePrice(symbol, fill*(intent.TPPrice/intent.EntryReferencePrice))
	if err != nil {
		return cleanupAfterEntry(fmt.Errorf("auto TP normalization failed: %w", err))
	}
	sl, err := binance.NormalizePrice(symbol, fill*(intent.SLPrice/intent.EntryReferencePrice))
	if err != nil {
		return cleanupAfterEntry(fmt.Errorf("auto SL normalization failed: %w", err))
	}
	if intent.Side == "LONG" && !(tp > fill && sl < fill) || intent.Side == "SHORT" && !(tp < fill && sl > fill) {
		return ManualExecution{}, b.cleanupFailedAutoExecution(ctx, ownership, fmt.Errorf("auto protective price invalid"))
	}
	tpText, err := binance.FormatPrice(symbol, tp)
	if err != nil {
		return cleanupAfterEntry(fmt.Errorf("auto TP format failed: %w", err))
	}
	slText, err := binance.FormatPrice(symbol, sl)
	if err != nil {
		return cleanupAfterEntry(fmt.Errorf("auto SL format failed: %w", err))
	}
	oppositeSide := "SELL"
	if intent.Side == "SHORT" {
		oppositeSide = "BUY"
	}
	ownership.protectiveSide = oppositeSide
	ownership.protectiveQuantity = executedText
	ownership.tpTrigger = tpText
	ownership.slTrigger = slText
	placedTP, err := b.broker.TakeProfit(ctx, intent.Symbol, oppositeSide, executedText, tpText, tpID)
	if err != nil {
		return ManualExecution{}, b.cleanupFailedAutoExecution(ctx, ownership, fmt.Errorf("auto TP creation failed: %w", err))
	}
	placedSL, err := b.broker.ProtectiveStop(ctx, intent.Symbol, oppositeSide, executedText, slText, slID)
	if err != nil {
		return ManualExecution{}, b.cleanupFailedAutoExecution(ctx, ownership, fmt.Errorf("auto SL creation failed: %w", err))
	}

	_, refreshed, refreshedOrders, err := b.Refresh(ctx)
	if err != nil || len(refreshed) != 1 || refreshed[0].Side != intent.Side || math.Abs(refreshed[0].QuantityBTC-executed) > 1e-12 {
		return ManualExecution{}, b.cleanupFailedAutoExecution(ctx, ownership, fmt.Errorf("auto position verification failed"))
	}
	protectiveState := autoExecutionState{Symbol: intent.Symbol, EntrySide: intent.Side, EntryFilledQuantity: executed, TPClientAlgoID: tpID, SLClientAlgoID: slID, TPTriggerPrice: tp, SLTriggerPrice: sl, ProtectiveQuantity: executed}
	if !protectiveOrdersMatchPersisted(refreshedOrders, protectiveState) {
		return ManualExecution{}, b.cleanupFailedAutoExecution(ctx, ownership, fmt.Errorf("auto protective order verification failed"))
	}
	protective := []Order{uiOrder(placedTP, "TP"), uiOrder(placedSL, "SL")}
	tpValue, slValue := tp, sl
	refreshed[0].TakeProfit = &tpValue
	refreshed[0].StopLoss = &slValue
	return ManualExecution{Order: uiOrder(entry, ""), Position: refreshed[0], Protective: protective, RawOrder: entry}, nil
}

type autoCleanupOwnership struct {
	symbol              string
	side                string
	requestedQuantity   string
	entryID             string
	tpID                string
	slID                string
	decisionTimestampMs int64
	candidateID         string
	protectiveSide      string
	protectiveQuantity  string
	tpTrigger           string
	slTrigger           string
}

func unknownAutoExecution(cause error, reason string) error {
	return fmt.Errorf("%w: %s: %v", errUnknownExecutionState, reason, cause)
}

// cleanupFailedAutoExecution is the only cleanup path used after an automatic
// entry may have reached the exchange. It proves ownership from the frozen,
// deterministic entry ID and the exchange position before sending a reduce-only
// exit. It never delegates to the manual ClosePosition path.
func (b *BinanceTestnetBackend) cleanupFailedAutoExecution(ctx context.Context, owned autoCleanupOwnership, cause error) error {
	timeout := b.autoRecoveryTimeout
	if timeout <= 0 {
		timeout = defaultAutoFailureRecoveryTimeout
	}
	recoveryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()
	entry, err := b.broker.GetOrder(recoveryCtx, owned.symbol, owned.entryID)
	if err != nil {
		return unknownAutoExecution(cause, "entry lookup failed")
	}
	if entry.ClientOrderID != owned.entryID || entry.Symbol != owned.symbol {
		return unknownAutoExecution(cause, "entry identity mismatch")
	}
	expectedEntrySide := "BUY"
	if owned.side == "SHORT" {
		expectedEntrySide = "SELL"
	}
	if entry.Side != expectedEntrySide {
		return unknownAutoExecution(cause, "entry side mismatch")
	}
	originalQuantity, originalErr := parseNumber("owned entry original quantity", entry.OriginalQuantity)
	requestedQuantity, requestedErr := parseNumber("requested normalized quantity", owned.requestedQuantity)
	if originalErr != nil || requestedErr != nil || originalQuantity <= 0 || math.Abs(originalQuantity-requestedQuantity) > 1e-12 {
		return unknownAutoExecution(cause, "entry original quantity mismatch")
	}
	if entry.Status != "FILLED" {
		switch entry.Status {
		case "CANCELED", "EXPIRED", "REJECTED":
			positions, positionErr := b.client.Positions(recoveryCtx, owned.symbol)
			if positionErr != nil || countOpenPositions(positions) != 0 {
				return unknownAutoExecution(cause, "non-filled entry position state is uncertain")
			}
			if cleanupErr := b.cleanupOwnedProtective(recoveryCtx, owned); cleanupErr != nil {
				return unknownAutoExecution(cause, "owned protective cleanup failed")
			}
			return fmt.Errorf("%w: %v; automatic entry was not filled", errAutoCleanupConfirmed, cause)
		default:
			return unknownAutoExecution(cause, "entry fill state is uncertain")
		}
	}
	executed, err := parseNumber("owned entry executed quantity", entry.ExecutedQuantity)
	if err != nil || executed <= 0 {
		return unknownAutoExecution(cause, "entry executed quantity is unavailable")
	}
	positions, err := b.client.Positions(recoveryCtx, owned.symbol)
	if err != nil {
		return unknownAutoExecution(cause, "position lookup failed")
	}
	if countOpenPositions(positions) == 0 {
		if cleanupErr := b.cleanupOwnedProtective(recoveryCtx, owned); cleanupErr != nil {
			return unknownAutoExecution(cause, "flat-position protective cleanup failed")
		}
		positions, err = b.client.Positions(recoveryCtx, owned.symbol)
		if err != nil || countOpenPositions(positions) != 0 {
			return unknownAutoExecution(cause, "flat position could not be reconfirmed")
		}
		return fmt.Errorf("%w: %v; automatic entry is already flat", errAutoCleanupConfirmed, cause)
	}
	position, ok := exactlyOwnedPosition(positions, owned.symbol, owned.side, executed)
	if !ok {
		return unknownAutoExecution(cause, "position side or quantity mismatch")
	}
	symbol, _, err := b.client.ExchangeSymbol(recoveryCtx, owned.symbol)
	if err != nil {
		return unknownAutoExecution(cause, "exchange symbol lookup failed")
	}
	quantityText, err := binance.FormatQuantity(symbol, math.Abs(position), true)
	if err != nil {
		return unknownAutoExecution(cause, "owned position quantity formatting failed")
	}
	exitSide := "SELL"
	if owned.side == "SHORT" {
		exitSide = "BUY"
	}
	closeID := binance.DeterministicClientOrderID("auto", owned.decisionTimestampMs, owned.candidateID, "failure-cleanup", 0)
	if _, err = b.broker.ReduceOnlyMarketExit(recoveryCtx, owned.symbol, exitSide, quantityText, closeID); err != nil {
		return unknownAutoExecution(cause, "reduce-only cleanup result is uncertain")
	}
	positions, err = b.client.Positions(recoveryCtx, owned.symbol)
	if err != nil || countOpenPositions(positions) != 0 {
		return unknownAutoExecution(cause, "post-cleanup position is not confirmed flat")
	}
	if err = b.cleanupOwnedProtective(recoveryCtx, owned); err != nil {
		return unknownAutoExecution(cause, "owned protective cleanup failed")
	}
	return fmt.Errorf("%w: %v; owned automatic entry cleaned up", errAutoCleanupConfirmed, cause)
}

func (b *BinanceTestnetBackend) cleanupOwnedProtective(ctx context.Context, owned autoCleanupOwnership) error {
	algos, err := b.client.OpenAlgoOrders(ctx, owned.symbol)
	if err != nil {
		return err
	}
	type expectation struct{ orderType, trigger string }
	expected := map[string]expectation{
		owned.tpID: {orderType: "TAKE_PROFIT_MARKET", trigger: owned.tpTrigger},
		owned.slID: {orderType: "STOP_MARKET", trigger: owned.slTrigger},
	}
	for _, order := range algos {
		want, ok := expected[order.ClientAlgoID]
		if !ok {
			continue
		}
		if owned.protectiveSide == "" || owned.protectiveQuantity == "" || want.trigger == "" || order.Symbol != owned.symbol || order.Side != owned.protectiveSide || order.OrderType != want.orderType || order.Quantity != owned.protectiveQuantity || order.TriggerPrice != want.trigger || order.AlgoStatus != "NEW" || !order.ReduceOnly {
			return fmt.Errorf("owned protective identity mismatch")
		}
		if _, err = b.broker.CancelAlgo(ctx, order.ClientAlgoID); err != nil {
			return err
		}
	}
	remaining, err := b.client.OpenAlgoOrders(ctx, owned.symbol)
	if err != nil {
		return err
	}
	for _, order := range remaining {
		if order.ClientAlgoID == owned.tpID || order.ClientAlgoID == owned.slID {
			return fmt.Errorf("owned protective order cleanup incomplete")
		}
	}
	return nil
}

func countOpenPositions(positions []binance.PositionV3) int {
	count := 0
	for _, position := range positions {
		quantity, err := strconv.ParseFloat(position.PositionAmount, 64)
		if err != nil || math.IsNaN(quantity) || math.IsInf(quantity, 0) {
			return -1
		}
		if quantity != 0 {
			count++
		}
	}
	return count
}

func exactlyOwnedPosition(positions []binance.PositionV3, symbol, side string, quantity float64) (float64, bool) {
	var found float64
	count := 0
	for _, position := range positions {
		value, err := strconv.ParseFloat(position.PositionAmount, 64)
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
			return 0, false
		}
		if value == 0 {
			continue
		}
		if position.Symbol != symbol {
			return 0, false
		}
		found = value
		count++
	}
	if count != 1 || math.Abs(math.Abs(found)-quantity) > 1e-12 {
		return 0, false
	}
	if side == "LONG" && found <= 0 || side == "SHORT" && found >= 0 {
		return 0, false
	}
	return found, true
}

// CleanupAutoProtective removes only the protective orders created by the
// current automatic position. It never cancels unrelated/manual orders.
func (b *BinanceTestnetBackend) CleanupAutoProtective(ctx context.Context, clientIDs []string) error {
	owned := make(map[string]struct{}, len(clientIDs))
	for _, id := range clientIDs {
		if id != "" {
			owned[id] = struct{}{}
		}
	}
	if len(owned) == 0 {
		return nil
	}
	algos, err := b.client.OpenAlgoOrders(ctx, "BTCUSDT")
	if err != nil {
		return err
	}
	for _, order := range algos {
		if _, ok := owned[order.ClientAlgoID]; !ok {
			continue
		}
		if _, err = b.broker.CancelAlgo(ctx, order.ClientAlgoID); err != nil {
			return err
		}
	}
	remaining, err := b.client.OpenAlgoOrders(ctx, "BTCUSDT")
	if err != nil {
		return err
	}
	for _, order := range remaining {
		if _, ok := owned[order.ClientAlgoID]; ok {
			return fmt.Errorf("auto protective order cleanup incomplete")
		}
	}
	return nil
}

func (b *BinanceTestnetBackend) cleanupManualOwnedPosition(ctx context.Context, requestID string) error {
	_, err := b.ClosePosition(ctx, requestID)
	return err
}

func (b *BinanceTestnetBackend) cleanupManualIfOpen(ctx context.Context, requestID string) error {
	positions, err := b.client.Positions(ctx, "BTCUSDT")
	if err != nil {
		return err
	}
	open := 0
	for _, p := range positions {
		q, _ := strconv.ParseFloat(p.PositionAmount, 64)
		if q != 0 {
			open++
		}
	}
	if open == 0 {
		return nil
	}
	if open != 1 {
		return fmt.Errorf("unexpected position cardinality")
	}
	return b.cleanupManualOwnedPosition(ctx, requestID)
}

func (b *BinanceTestnetBackend) ClosePosition(ctx context.Context, requestID string) (binance.Order, error) {
	_, positions, _, err := b.Refresh(ctx)
	if err != nil {
		return binance.Order{}, err
	}
	if len(positions) != 1 {
		return binance.Order{}, fmt.Errorf("exactly one open position required")
	}
	p := positions[0]
	side := "SELL"
	if p.Side == "SHORT" {
		side = "BUY"
	}
	symbol, _, err := b.client.ExchangeSymbol(ctx, p.Symbol)
	if err != nil {
		return binance.Order{}, err
	}
	quantityText, err := binance.FormatQuantity(symbol, p.QuantityBTC, true)
	if err != nil {
		return binance.Order{}, err
	}
	closed, err := b.broker.ReduceOnlyMarketExit(ctx, p.Symbol, side, quantityText, requestID)
	if err != nil {
		return closed, err
	}
	algos, err := b.client.OpenAlgoOrders(ctx, p.Symbol)
	if err != nil {
		return closed, err
	}
	for _, order := range algos {
		if _, err = b.broker.CancelAlgo(ctx, order.ClientAlgoID); err != nil {
			return closed, err
		}
	}
	_, finalPositions, finalOrders, err := b.Refresh(ctx)
	if err != nil || len(finalPositions) != 0 || len(finalOrders) != 0 {
		return closed, fmt.Errorf("final exchange state is not flat and clean")
	}
	return closed, nil
}

// CloseAutoPosition closes the exchange position but cancels only protective
// algo orders owned by the automatic entry. Unrelated/manual orders remain.
func (b *BinanceTestnetBackend) CloseAutoPosition(ctx context.Context, requestID string, protectiveIDs []string, expectedSide string, expectedQuantity float64) (binance.Order, error) {
	_, positions, _, err := b.Refresh(ctx)
	if err != nil {
		return binance.Order{}, err
	}
	if len(positions) != 1 {
		return binance.Order{}, fmt.Errorf("exactly one auto position required")
	}
	p := positions[0]
	if p.Side != expectedSide || expectedQuantity <= 0 || math.Abs(p.QuantityBTC-expectedQuantity) > 1e-12 {
		return binance.Order{}, fmt.Errorf("auto position ownership mismatch")
	}
	side := "SELL"
	if p.Side == "SHORT" {
		side = "BUY"
	}
	symbol, _, err := b.client.ExchangeSymbol(ctx, p.Symbol)
	if err != nil {
		return binance.Order{}, err
	}
	quantityText, err := binance.FormatQuantity(symbol, p.QuantityBTC, true)
	if err != nil {
		return binance.Order{}, err
	}
	closed, err := b.broker.ReduceOnlyMarketExit(ctx, p.Symbol, side, quantityText, requestID)
	if err != nil {
		return closed, err
	}
	if err = b.CleanupAutoProtective(ctx, protectiveIDs); err != nil {
		return closed, err
	}
	_, finalPositions, _, err := b.Refresh(ctx)
	if err != nil || len(finalPositions) != 0 {
		return closed, fmt.Errorf("auto position is not flat")
	}
	return closed, nil
}
