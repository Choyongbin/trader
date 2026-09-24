package market

// SecondBar is a canonical one-second observation. TimestampMs is the UTC
// bucket start represented as Unix milliseconds.
type SecondBar struct {
	TimestampMs int64   `parquet:"timestamp_ms" json:"timestamp_ms"`
	Open        float64 `parquet:"open" json:"open"`
	High        float64 `parquet:"high" json:"high"`
	Low         float64 `parquet:"low" json:"low"`
	Close       float64 `parquet:"close" json:"close"`

	BaseVolume  float64 `parquet:"base_volume" json:"base_volume"`
	QuoteVolume float64 `parquet:"quote_volume" json:"quote_volume"`

	AggTradeCount int64 `parquet:"agg_trade_count" json:"agg_trade_count"`

	TakerBuyBaseVolume   float64 `parquet:"taker_buy_base_volume" json:"taker_buy_base_volume"`
	TakerSellBaseVolume  float64 `parquet:"taker_sell_base_volume" json:"taker_sell_base_volume"`
	TakerBuyQuoteVolume  float64 `parquet:"taker_buy_quote_volume" json:"taker_buy_quote_volume"`
	TakerSellQuoteVolume float64 `parquet:"taker_sell_quote_volume" json:"taker_sell_quote_volume"`

	VWAP     float64 `parquet:"vwap" json:"vwap"`
	HasTrade bool    `parquet:"has_trade" json:"has_trade"`

	FirstAggTradeID int64 `parquet:"first_agg_trade_id" json:"first_agg_trade_id"`
	LastAggTradeID  int64 `parquet:"last_agg_trade_id" json:"last_agg_trade_id"`
}
