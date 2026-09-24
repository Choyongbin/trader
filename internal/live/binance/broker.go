package binance

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
)

type OrderState string

const (
	LocalCreated    OrderState = "LOCAL_CREATED"
	Submitting      OrderState = "SUBMITTING"
	Acknowledged    OrderState = "ACKNOWLEDGED"
	PartiallyFilled OrderState = "PARTIALLY_FILLED"
	Filled          OrderState = "FILLED"
	CancelPending   OrderState = "CANCEL_PENDING"
	Canceled        OrderState = "CANCELED"
	Rejected        OrderState = "REJECTED"
)

type Order struct {
	Symbol           string     `json:"symbol"`
	Side             string     `json:"side"`
	Type             string     `json:"type"`
	ClientOrderID    string     `json:"clientOrderId"`
	Status           string     `json:"status"`
	OrderID          int64      `json:"orderId"`
	Price            string     `json:"price,omitempty"`
	StopPrice        string     `json:"stopPrice,omitempty"`
	OriginalQuantity string     `json:"origQty,omitempty"`
	ExecutedQuantity string     `json:"executedQty,omitempty"`
	AveragePrice     string     `json:"avgPrice,omitempty"`
	ReduceOnly       bool       `json:"reduceOnly"`
	LocalState       OrderState `json:"-"`
}

type AlgoOrder struct {
	AlgoID         int64  `json:"algoId"`
	ClientAlgoID   string `json:"clientAlgoId"`
	AlgoType       string `json:"algoType"`
	OrderType      string `json:"orderType"`
	Symbol         string `json:"symbol"`
	Side           string `json:"side"`
	PositionSide   string `json:"positionSide"`
	Quantity       string `json:"quantity"`
	AlgoStatus     string `json:"algoStatus"`
	TriggerPrice   string `json:"triggerPrice"`
	WorkingType    string `json:"workingType"`
	ReduceOnly     bool   `json:"reduceOnly"`
	ActualOrderID  string `json:"actualOrderId"`
	ActualPrice    string `json:"actualPrice"`
	ActualQuantity string `json:"actualQty"`
	CreateTime     int64  `json:"createTime"`
	UpdateTime     int64  `json:"updateTime"`
}

type Position struct {
	Symbol           string `json:"symbol"`
	PositionAmount   string `json:"positionAmt"`
	EntryPrice       string `json:"entryPrice"`
	MarkPrice        string `json:"markPrice"`
	LiquidationPrice string `json:"liquidationPrice"`
	Leverage         string `json:"leverage"`
	MarginType       string `json:"marginType"`
}

type Balance struct {
	Asset            string `json:"asset"`
	Balance          string `json:"balance"`
	AvailableBalance string `json:"availableBalance"`
}

type SignedTransport interface {
	Signed(context.Context, string, string, url.Values, any) error
}

type BinanceTestnetBroker struct {
	transport SignedTransport
	mu        sync.Mutex
	known     map[string]Order
}

func NewBinanceTestnetBroker(client *Client) (*BinanceTestnetBroker, error) {
	if client == nil {
		return nil, fmt.Errorf("nil client")
	}
	if err := ValidateTestnetBaseURL(client.BaseURL); err != nil {
		return nil, err
	}
	return &BinanceTestnetBroker{transport: client, known: map[string]Order{}}, nil
}

var ErrSubmitOutcomeUnknown = errors.New("submit outcome unknown")

func (b *BinanceTestnetBroker) Submit(ctx context.Context, order Order) (Order, error) {
	b.mu.Lock()
	if known, ok := b.known[order.ClientOrderID]; ok {
		b.mu.Unlock()
		return known, nil
	}
	b.mu.Unlock()
	order.LocalState = Submitting
	v := url.Values{"symbol": {order.Symbol}, "side": {order.Side}, "type": {order.Type}, "quantity": {order.OriginalQuantity}, "newClientOrderId": {order.ClientOrderID}, "newOrderRespType": {"RESULT"}}
	if order.ReduceOnly {
		v.Set("reduceOnly", "true")
	}
	if order.StopPrice != "" {
		v.Set("stopPrice", order.StopPrice)
	}
	var exchange Order
	if err := b.transport.Signed(ctx, http.MethodPost, "/fapi/v1/order", v, &exchange); err != nil {
		adopted, queryErr := b.GetOrder(ctx, order.Symbol, order.ClientOrderID)
		if queryErr == nil {
			b.remember(adopted)
			return adopted, nil
		}
		return order, fmt.Errorf("%w: submit=%v query=%v", ErrSubmitOutcomeUnknown, err, queryErr)
	}
	exchange.LocalState = stateFromExchange(exchange.Status)
	b.remember(exchange)
	return exchange, nil
}

func (b *BinanceTestnetBroker) remember(order Order) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.known == nil {
		b.known = map[string]Order{}
	}
	b.known[order.ClientOrderID] = order
}

func (b *BinanceTestnetBroker) MarketEntry(ctx context.Context, symbol, side, quantity, clientID string) (Order, error) {
	return b.Submit(ctx, Order{Symbol: symbol, Side: side, Type: "MARKET", OriginalQuantity: quantity, ClientOrderID: clientID})
}
func (b *BinanceTestnetBroker) ReduceOnlyMarketExit(ctx context.Context, symbol, side, quantity, clientID string) (Order, error) {
	return b.Submit(ctx, Order{Symbol: symbol, Side: side, Type: "MARKET", OriginalQuantity: quantity, ClientOrderID: clientID, ReduceOnly: true})
}
func (b *BinanceTestnetBroker) ProtectiveStop(ctx context.Context, symbol, side, quantity, stopPrice, clientID string) (Order, error) {
	x, err := b.submitAlgo(ctx, symbol, side, quantity, stopPrice, clientID, "STOP_MARKET")
	return algoAsOrder(x), err
}
func (b *BinanceTestnetBroker) TakeProfit(ctx context.Context, symbol, side, quantity, stopPrice, clientID string) (Order, error) {
	x, err := b.submitAlgo(ctx, symbol, side, quantity, stopPrice, clientID, "TAKE_PROFIT_MARKET")
	return algoAsOrder(x), err
}

func (b *BinanceTestnetBroker) submitAlgo(ctx context.Context, symbol, side, quantity, triggerPrice, clientID, orderType string) (AlgoOrder, error) {
	v := url.Values{
		"algoType": {"CONDITIONAL"}, "symbol": {symbol}, "side": {side}, "type": {orderType},
		"quantity": {quantity}, "triggerPrice": {triggerPrice}, "workingType": {"MARK_PRICE"},
		"reduceOnly": {"true"}, "clientAlgoId": {clientID}, "newOrderRespType": {"RESULT"},
	}
	var x AlgoOrder
	if err := b.transport.Signed(ctx, http.MethodPost, "/fapi/v1/algoOrder", v, &x); err != nil {
		adopted, queryErr := b.GetAlgoOrder(ctx, clientID)
		if queryErr == nil && adopted.ClientAlgoID == clientID {
			return adopted, nil
		}
		return x, fmt.Errorf("%w: algo submit=%v query=%v", ErrSubmitOutcomeUnknown, err, queryErr)
	}
	return x, nil
}

func (b *BinanceTestnetBroker) GetAlgoOrder(ctx context.Context, clientID string) (AlgoOrder, error) {
	var x AlgoOrder
	err := b.transport.Signed(ctx, http.MethodGet, "/fapi/v1/algoOrder", url.Values{"clientAlgoId": {clientID}}, &x)
	return x, err
}

func algoAsOrder(x AlgoOrder) Order {
	return Order{Symbol: x.Symbol, Side: x.Side, Type: x.OrderType, ClientOrderID: x.ClientAlgoID, Status: x.AlgoStatus, OrderID: x.AlgoID, StopPrice: x.TriggerPrice, OriginalQuantity: x.Quantity, ReduceOnly: x.ReduceOnly, LocalState: stateFromExchange(x.AlgoStatus)}
}

func (b *BinanceTestnetBroker) CancelAlgo(ctx context.Context, clientID string) (AlgoOrder, error) {
	var x AlgoOrder
	err := b.transport.Signed(ctx, http.MethodDelete, "/fapi/v1/algoOrder", url.Values{"clientAlgoId": {clientID}}, &x)
	return x, err
}
func (o Order) RemainingQuantity() (float64, error) {
	original, err := strconv.ParseFloat(o.OriginalQuantity, 64)
	if err != nil {
		return 0, err
	}
	filled, err := strconv.ParseFloat(o.ExecutedQuantity, 64)
	if err != nil {
		return 0, err
	}
	remaining := original - filled
	if remaining < 0 {
		return 0, fmt.Errorf("executed quantity exceeds original")
	}
	return remaining, nil
}

func stateFromExchange(status string) OrderState {
	switch status {
	case "NEW":
		return Acknowledged
	case "PARTIALLY_FILLED":
		return PartiallyFilled
	case "FILLED":
		return Filled
	case "CANCELED", "EXPIRED":
		return Canceled
	case "REJECTED":
		return Rejected
	default:
		return Acknowledged
	}
}

func (b *BinanceTestnetBroker) GetOrder(ctx context.Context, symbol, clientID string) (Order, error) {
	var x Order
	e := b.transport.Signed(ctx, http.MethodGet, "/fapi/v1/order", url.Values{"symbol": {symbol}, "origClientOrderId": {clientID}}, &x)
	x.LocalState = stateFromExchange(x.Status)
	return x, e
}
func (b *BinanceTestnetBroker) Cancel(ctx context.Context, symbol, clientID string) (Order, error) {
	var x Order
	e := b.transport.Signed(ctx, http.MethodDelete, "/fapi/v1/order", url.Values{"symbol": {symbol}, "origClientOrderId": {clientID}}, &x)
	x.LocalState = stateFromExchange(x.Status)
	return x, e
}
func (b *BinanceTestnetBroker) OpenOrders(ctx context.Context, symbol string) ([]Order, error) {
	var x []Order
	e := b.transport.Signed(ctx, http.MethodGet, "/fapi/v1/openOrders", url.Values{"symbol": {symbol}}, &x)
	for i := range x {
		x[i].LocalState = stateFromExchange(x[i].Status)
	}
	return x, e
}
func (b *BinanceTestnetBroker) Positions(ctx context.Context, symbol string) ([]Position, error) {
	var x []Position
	e := b.transport.Signed(ctx, http.MethodGet, "/fapi/v2/positionRisk", url.Values{"symbol": {symbol}}, &x)
	return x, e
}
func (b *BinanceTestnetBroker) Balances(ctx context.Context) ([]Balance, error) {
	var x []Balance
	e := b.transport.Signed(ctx, http.MethodGet, "/fapi/v2/balance", nil, &x)
	return x, e
}
func (b *BinanceTestnetBroker) SetLeverage(ctx context.Context, symbol string, leverage int) error {
	if leverage < 1 || leverage > 125 {
		return fmt.Errorf("invalid leverage")
	}
	var x any
	return b.transport.Signed(ctx, http.MethodPost, "/fapi/v1/leverage", url.Values{"symbol": {symbol}, "leverage": {strconv.Itoa(leverage)}}, &x)
}

type ReconcileState string

const (
	ReconcileFlat    ReconcileState = "FLAT"
	ReconcilePending ReconcileState = "PENDING_ENTRY"
	ReconcileOpen    ReconcileState = "OPEN"
	ReconcileUnknown ReconcileState = "UNKNOWN_EXCHANGE_STATE"
)

type LocalPosition struct {
	State                              ReconcileState
	Candidate, Side                    string
	DecisionTimestampMs                int64
	EntryOrderID, TPOrderID, SLOrderID string
	FilledQuantity, AveragePrice       float64
	Leverage                           int
	LastProcessedMarketTimestampMs     int64
}
type ReconcileResult struct {
	State             ReconcileState
	NewEntriesEnabled bool
	Action, Reason    string
}

func Reconcile(local LocalPosition, exchangePositionQty float64, openOrders []Order) ReconcileResult {
	exchangeOpen := exchangePositionQty != 0
	switch {
	case local.State == ReconcileFlat && !exchangeOpen && len(openOrders) == 0:
		return ReconcileResult{ReconcileFlat, true, "NONE", "states agree"}
	case local.State == ReconcilePending && exchangeOpen:
		return ReconcileResult{ReconcileOpen, true, "ADOPT_FILLED_ENTRY", "pending entry became exchange position"}
	case local.State == ReconcileOpen && exchangeOpen:
		if local.TPOrderID != "" || local.SLOrderID != "" {
			seen := map[string]bool{}
			for _, order := range openOrders {
				seen[order.ClientOrderID] = true
			}
			if !seen[local.TPOrderID] || !seen[local.SLOrderID] {
				return ReconcileResult{ReconcileUnknown, false, "DISABLE_NEW_ENTRIES", "protective order missing"}
			}
		}
		return ReconcileResult{ReconcileOpen, true, "ADOPT_EXCHANGE", "exchange position authoritative"}
	case local.State == ReconcileOpen && !exchangeOpen && len(openOrders) == 0:
		return ReconcileResult{ReconcileFlat, true, "CLOSE_LOCAL", "exchange is flat"}
	default:
		return ReconcileResult{ReconcileUnknown, false, "DISABLE_NEW_ENTRIES", "unsafe local/exchange mismatch"}
	}
}
