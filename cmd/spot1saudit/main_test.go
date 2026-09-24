package main

import (
	"testing"

	"binance_trader/internal/market"
)

func TestInspectBarFailures(t *testing.T) {
	trade := market.SecondBar{TimestampMs: 1000, Open: 10, High: 10, Low: 10, Close: 10, BaseVolume: 2, QuoteVolume: 20, AggTradeCount: 1, TakerBuyBaseVolume: 2, TakerBuyQuoteVolume: 20, VWAP: 10, HasTrade: true, FirstAggTradeID: 1, LastAggTradeID: 1}
	var ok auditReport
	if err := inspectBar(trade, market.SecondBar{}, false, &ok); err != nil || ok.NumericalFailures != 0 {
		t.Fatal("valid trade rejected")
	}
	tests := []market.SecondBar{
		{TimestampMs: 2000, Open: 10, High: 10, Low: 10, Close: 9, VWAP: 10},
		{TimestampMs: 2000, Open: 10, High: 10, Low: 10, Close: 10, HasTrade: true},
		{TimestampMs: 2000, Open: 10, High: 10, Low: 10, Close: 10, BaseVolume: 2, QuoteVolume: 20, AggTradeCount: 1, TakerBuyBaseVolume: 1, TakerBuyQuoteVolume: 20, VWAP: 10, HasTrade: true, FirstAggTradeID: 2, LastAggTradeID: 2},
		{TimestampMs: 2000, Open: 10, High: 10, Low: 10, Close: 10, BaseVolume: 2, QuoteVolume: 20, AggTradeCount: 1, TakerBuyBaseVolume: 2, TakerBuyQuoteVolume: 20, VWAP: 9, HasTrade: true, FirstAggTradeID: 2, LastAggTradeID: 2},
	}
	for _, bar := range tests {
		var r auditReport
		_ = inspectBar(bar, trade, true, &r)
		if r.NumericalFailures == 0 {
			t.Fatalf("bad bar accepted: %+v", bar)
		}
	}
	var gap auditReport
	_ = inspectBar(trade, market.SecondBar{TimestampMs: -1000}, true, &gap)
	if gap.Missing != 1 {
		t.Fatal("missing second not detected")
	}
}
