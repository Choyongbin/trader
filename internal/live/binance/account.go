package binance

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
)

type Account struct {
	TotalWalletBalance    string `json:"totalWalletBalance"`
	TotalUnrealizedProfit string `json:"totalUnrealizedProfit"`
	TotalMarginBalance    string `json:"totalMarginBalance"`
	TotalInitialMargin    string `json:"totalInitialMargin"`
	AvailableBalance      string `json:"availableBalance"`
}

type PositionV3 struct {
	Symbol           string `json:"symbol"`
	PositionSide     string `json:"positionSide"`
	PositionAmount   string `json:"positionAmt"`
	EntryPrice       string `json:"entryPrice"`
	MarkPrice        string `json:"markPrice"`
	Notional         string `json:"notional"`
	InitialMargin    string `json:"positionInitialMargin"`
	UnrealizedProfit string `json:"unRealizedProfit"`
	LiquidationPrice string `json:"liquidationPrice"`
	Leverage         string `json:"leverage"`
	UpdateTime       int64  `json:"updateTime"`
}

type PositionMode struct {
	DualSidePosition bool `json:"dualSidePosition"`
}

type MarkPrice struct {
	Symbol    string `json:"symbol"`
	MarkPrice string `json:"markPrice"`
}

func (c *Client) Account(ctx context.Context) (Account, error) {
	var x Account
	err := c.UserData(ctx, "/fapi/v3/account", nil, &x)
	return x, err
}

func (c *Client) Positions(ctx context.Context, symbol string) ([]PositionV3, error) {
	var x []PositionV3
	err := c.UserData(ctx, "/fapi/v2/positionRisk", url.Values{"symbol": {symbol}}, &x)
	return x, err
}

func (c *Client) OpenOrders(ctx context.Context, symbol string) ([]Order, error) {
	var x []Order
	err := c.UserData(ctx, "/fapi/v1/openOrders", url.Values{"symbol": {symbol}}, &x)
	return x, err
}

func (c *Client) OpenAlgoOrders(ctx context.Context, symbol string) ([]AlgoOrder, error) {
	var x []AlgoOrder
	err := c.UserData(ctx, "/fapi/v1/openAlgoOrders", url.Values{"symbol": {symbol}, "algoType": {"CONDITIONAL"}}, &x)
	return x, err
}

func (c *Client) PositionMode(ctx context.Context) (PositionMode, error) {
	var x PositionMode
	err := c.UserData(ctx, "/fapi/v1/positionSide/dual", nil, &x)
	return x, err
}

func (c *Client) MarkPrice(ctx context.Context, symbol string) (float64, error) {
	var x MarkPrice
	if err := c.getJSON(ctx, "/fapi/v1/premiumIndex", url.Values{"symbol": {symbol}}, &x); err != nil {
		return 0, err
	}
	value, err := strconv.ParseFloat(x.MarkPrice, 64)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("invalid mark price")
	}
	return value, nil
}

func MinNotional(s Symbol) (float64, error) {
	f, ok := filterFor(s, "MIN_NOTIONAL")
	if !ok {
		f, ok = filterFor(s, "NOTIONAL")
	}
	if !ok {
		return 0, fmt.Errorf("notional filter absent")
	}
	v, err := strconv.ParseFloat(f.Notional, 64)
	if err != nil || v <= 0 {
		return 0, fmt.Errorf("invalid notional filter")
	}
	return v, nil
}
