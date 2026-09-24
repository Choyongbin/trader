package binance

import (
	"encoding/json"
	"testing"

	"binance_trader/internal/market"
)

func TestPublicAggTradeAndCanonicalStream(t *testing.T) {
	var bars []market.SecondBar
	stream := NewCanonicalStream(func(b market.SecondBar) error { bars = append(bars, b); return nil })
	for _, x := range []struct {
		raw string
		id  int64
	}{
		{`{"a":"10","p":"100","q":"2","T":"1700000000000000","m":false}`, 10},
		{`{"a":11,"p":"101","q":"3","T":1700000000001,"m":true}`, 11},
		{`{"a":12,"p":"102","q":"1","T":1700000002000,"m":false}`, 12},
	} {
		e, err := ParsePublicAggTrade(json.RawMessage(x.raw), "futures_aggTrade", 1700000003000)
		if err != nil || e.ID != x.id {
			t.Fatalf("parse %d: %v", x.id, err)
		}
		if err = stream.Add(e); err != nil {
			t.Fatal(err)
		}
	}
	if len(bars) != 2 || bars[0].TimestampMs != 1700000000000 || bars[0].Close != 101 || bars[0].AggTradeCount != 2 || bars[0].TakerBuyBaseVolume != 2 || bars[0].TakerSellBaseVolume != 3 || bars[1].TimestampMs != 1700000001000 || bars[1].HasTrade {
		t.Fatalf("canonical bars: %+v", bars)
	}
	duplicate, _ := ParsePublicAggTrade(json.RawMessage(`{"a":12,"p":"102","q":"1","T":1700000002000,"m":false}`), "futures_aggTrade", 1700000003000)
	if err := stream.Add(duplicate); err != nil || stream.Duplicates != 1 {
		t.Fatalf("duplicate: %v", err)
	}
	gap, _ := ParsePublicAggTrade(json.RawMessage(`{"a":14,"p":"103","q":"1","T":1700000003000,"m":false}`), "futures_aggTrade", 1700000004000)
	if err := stream.Add(gap); err == nil || stream.IDGaps != 1 {
		t.Fatal("ID gap accepted")
	}
}
