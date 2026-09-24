package binance

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"binance_trader/internal/data/secondbar"
	"binance_trader/internal/market"
)

// ParsePublicAggTrade normalizes numeric or quoted Binance integer fields.
// Spot microsecond event times are reduced to the canonical millisecond unit.
func ParsePublicAggTrade(raw json.RawMessage, source string, receiveMs int64) (CaptureEvent, error) {
	var row struct {
		ID         json.RawMessage `json:"a"`
		Price      string          `json:"p"`
		Quantity   string          `json:"q"`
		Time       json.RawMessage `json:"T"`
		BuyerMaker bool            `json:"m"`
	}
	if err := json.Unmarshal(raw, &row); err != nil {
		return CaptureEvent{}, fmt.Errorf("aggTrade JSON: %w", err)
	}
	id, err := publicInteger(row.ID)
	if err != nil || id <= 0 {
		return CaptureEvent{}, fmt.Errorf("aggTrade invalid ID")
	}
	stamp, err := publicInteger(row.Time)
	if err != nil || stamp <= 0 {
		return CaptureEvent{}, fmt.Errorf("aggTrade invalid time")
	}
	if stamp >= 1_000_000_000_000_000 {
		stamp /= 1000
	}
	price, pe := strconv.ParseFloat(row.Price, 64)
	quantity, qe := strconv.ParseFloat(row.Quantity, 64)
	if pe != nil || qe != nil || price <= 0 || quantity <= 0 || math.IsNaN(price) || math.IsInf(price, 0) || math.IsNaN(quantity) || math.IsInf(quantity, 0) {
		return CaptureEvent{}, fmt.Errorf("aggTrade invalid numeric value")
	}
	return CaptureEvent{Source: source, EventType: "aggTrade", SourceTimestampMs: stamp, ReceiveTimestampMs: receiveMs, Origin: OriginLiveReceived, ID: id, Price: price, Quantity: quantity, BuyerMaker: row.BuyerMaker}, nil
}

func publicInteger(raw json.RawMessage) (int64, error) {
	return strconv.ParseInt(strings.Trim(string(raw), `"`), 10, 64)
}

// CanonicalStream uses the same production secondbar aggregator as historical
// conversion. Duplicate IDs are ignored; reversals and missing IDs fail closed.
type CanonicalStream struct {
	aggregator                  *secondbar.Aggregator
	lastID                      int64
	lastTradeMs                 int64
	Duplicates, Reverse, IDGaps int64
}

func NewCanonicalStream(sink secondbar.BarSink) *CanonicalStream {
	return &CanonicalStream{aggregator: secondbar.NewAggregator(sink)}
}

func NewResumedCanonicalStream(sink secondbar.BarSink, lastBar market.SecondBar, lastID int64) (*CanonicalStream, error) {
	a, err := secondbar.NewResumedAggregator(sink, lastBar.TimestampMs, lastBar.Close)
	if err != nil {
		return nil, err
	}
	return &CanonicalStream{aggregator: a, lastID: lastID}, nil
}

func (s *CanonicalStream) Add(event CaptureEvent) error {
	if event.EventType != "aggTrade" || event.ID <= 0 || event.SourceTimestampMs <= 0 || event.Price <= 0 || event.Quantity <= 0 {
		return fmt.Errorf("invalid canonical event")
	}
	if s.lastID != 0 {
		if event.ID == s.lastID {
			s.Duplicates++
			return nil
		}
		if event.ID < s.lastID || (s.lastTradeMs != 0 && event.SourceTimestampMs < s.lastTradeMs) {
			s.Reverse++
			return fmt.Errorf("canonical event reversal")
		}
		if event.ID != s.lastID+1 {
			s.IDGaps++
			return fmt.Errorf("canonical aggTrade ID gap: expected=%d actual=%d", s.lastID+1, event.ID)
		}
	}
	if err := s.aggregator.Add(market.AggTrade{AggTradeID: event.ID, TradeTimeMs: event.SourceTimestampMs, Price: event.Price, Quantity: event.Quantity, BuyerMaker: event.BuyerMaker}); err != nil {
		return err
	}
	s.lastID, s.lastTradeMs = event.ID, event.SourceTimestampMs
	return nil
}

func (s *CanonicalStream) LastID() int64 { return s.lastID }
