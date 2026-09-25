package uiapi

import "time"

type TradingEnvironment string

const (
	TradingEnvironmentTestnet TradingEnvironment = "TESTNET"
	TradingEnvironmentMainnet TradingEnvironment = "MAINNET"
)

func ParseEnvironment(value string) (TradingEnvironment, bool) {
	switch TradingEnvironment(value) {
	case TradingEnvironmentTestnet:
		return TradingEnvironmentTestnet, true
	case TradingEnvironmentMainnet:
		return TradingEnvironmentMainnet, true
	default:
		return "", false
	}
}

type ProtectiveOrderConfig struct {
	Enabled bool    `json:"enabled"`
	Mode    string  `json:"mode"`
	Value   float64 `json:"value"`
}

type ManualOrderOptions struct {
	LimitPrice         *float64 `json:"limit_price,omitempty"`
	TriggerType        string   `json:"trigger_type,omitempty"`
	ReduceOnly         bool     `json:"reduce_only,omitempty"`
	PostOnly           bool     `json:"post_only,omitempty"`
	TimeInForce        string   `json:"time_in_force,omitempty"`
	TrailingStop       *float64 `json:"trailing_stop,omitempty"`
	PartialTakeProfits []any    `json:"partial_take_profits,omitempty"`
	BreakevenStop      *float64 `json:"breakeven_stop,omitempty"`
}

type ManualOrderRequest struct {
	Environment      TradingEnvironment     `json:"environment"`
	Symbol           string                 `json:"symbol"`
	Side             string                 `json:"side"`
	OrderType        string                 `json:"order_type"`
	MarginAmountUSDT float64                `json:"margin_amount_usdt"`
	Leverage         int                    `json:"leverage"`
	TakeProfit       *ProtectiveOrderConfig `json:"take_profit,omitempty"`
	StopLoss         *ProtectiveOrderConfig `json:"stop_loss,omitempty"`
	Options          ManualOrderOptions     `json:"options"`
	RequestID        string                 `json:"request_id"`
	LiveConfirmation string                 `json:"live_confirmation,omitempty"`
}

type Balance struct {
	Connected         bool     `json:"connected"`
	Status            string   `json:"status"`
	WalletUSDT        *float64 `json:"wallet_balance_usdt"`
	AvailableUSDT     *float64 `json:"available_balance_usdt"`
	MarginUSDT        *float64 `json:"margin_balance_usdt"`
	UsedMarginUSDT    *float64 `json:"used_margin_usdt"`
	UnrealizedPnLUSDT *float64 `json:"unrealized_pnl_usdt"`
}

type Position struct {
	Environment      TradingEnvironment `json:"environment"`
	Symbol           string             `json:"symbol"`
	Side             string             `json:"side"`
	QuantityBTC      float64            `json:"quantity_btc"`
	NotionalUSDT     float64            `json:"position_notional_usdt"`
	EntryPrice       float64            `json:"entry_price"`
	MarkPrice        float64            `json:"mark_price"`
	UnrealizedPnL    float64            `json:"unrealized_pnl_usdt"`
	ROEPercent       float64            `json:"roe_percent"`
	Leverage         int                `json:"leverage"`
	MarginUSDT       float64            `json:"margin_usdt"`
	LiquidationPrice float64            `json:"liquidation_price"`
	TakeProfit       *float64           `json:"take_profit,omitempty"`
	StopLoss         *float64           `json:"stop_loss,omitempty"`
	Funding          float64            `json:"funding"`
	OpenedAt         time.Time          `json:"opened_at"`
}

type Order struct {
	Time          time.Time          `json:"time"`
	Environment   TradingEnvironment `json:"environment"`
	Symbol        string             `json:"symbol"`
	Side          string             `json:"side"`
	OrderType     string             `json:"order_type"`
	QuantityBTC   float64            `json:"quantity_btc"`
	Price         float64            `json:"price"`
	Status        string             `json:"status"`
	ClientOrderID string             `json:"client_order_id"`
	Protective    string             `json:"protective,omitempty"`
	ReduceOnly    bool               `json:"reduce_only,omitempty"`
}

type Market struct {
	FuturesPrice float64   `json:"futures_price"`
	SpotPrice    float64   `json:"spot_price"`
	MarkPrice    float64   `json:"mark_price"`
	IndexPrice   float64   `json:"index_price"`
	Premium      float64   `json:"premium"`
	UpdatedAt    time.Time `json:"updated_at"`
	Connected    bool      `json:"connected"`
}

type ModelProfile struct {
	ID                  string `json:"id"`
	Name                string `json:"name"`
	Version             string `json:"version"`
	FeatureRegistryHash string `json:"feature_registry_hash"`
	EntryPolicyHash     string `json:"entry_policy_hash"`
	RiskPolicyHash      string `json:"risk_policy_hash"`
	Status              string `json:"status"`
	CandidateCount      int    `json:"candidate_count"`
}
