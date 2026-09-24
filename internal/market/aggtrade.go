package market

// AggTrade is the canonical representation shared by historical and live feeds.
type AggTrade struct {
	AggTradeID   int64
	Price        float64
	Quantity     float64
	FirstTradeID int64
	LastTradeID  int64
	TradeTimeMs  int64
	BuyerMaker   bool
}

// IsTakerBuy reports whether the buyer was the taker/aggressor.
func (a AggTrade) IsTakerBuy() bool { return !a.BuyerMaker }

// IsTakerSell reports whether the seller was the taker/aggressor.
func (a AggTrade) IsTakerSell() bool { return a.BuyerMaker }
