package featurev2

import (
	"fmt"

	"binance_trader/internal/market"
)

type SpotAggregateState struct {
	Bar              market.SecondBar `json:"bar"`
	CumulativeQuote  float64          `json:"cumulative_quote"`
	CumulativeBuy    float64          `json:"cumulative_buy"`
	CumulativeSell   float64          `json:"cumulative_sell"`
	CumulativeTrades int64            `json:"cumulative_trades"`
	Sequence         int64            `json:"sequence"`
}

// StreamState preserves all accumulator and as-of source cursor values.
type StreamState struct {
	Version                            int                          `json:"version"`
	Spot                               map[int64]SpotAggregateState `json:"spot"`
	SpotQuote, SpotBuy, SpotSell       float64
	SpotTrades, SpotSequence, LastSpot int64
	Metrics                            []MetricsObservation
	Mark, Index, Premium               []KlineObservation
	Funding                            []FundingObservation
}

func (e *StreamingEngine) ExportState() StreamState {
	s := StreamState{Version: 1, Spot: make(map[int64]SpotAggregateState, len(e.spot)), SpotQuote: e.spotQuote, SpotBuy: e.spotBuy, SpotSell: e.spotSell, SpotTrades: e.spotTrades, SpotSequence: e.spotSequence, LastSpot: e.lastSpot, Metrics: append([]MetricsObservation(nil), e.metrics...), Mark: append([]KlineObservation(nil), e.mark...), Index: append([]KlineObservation(nil), e.index...), Premium: append([]KlineObservation(nil), e.premium...), Funding: append([]FundingObservation(nil), e.funding...)}
	for ts, x := range e.spot {
		s.Spot[ts] = SpotAggregateState{x.bar, x.cumulativeQuote, x.cumulativeBuy, x.cumulativeSell, x.cumulativeTrades, x.sequence}
	}
	return s
}

func RestoreStreamingEngine(state StreamState) (*StreamingEngine, error) {
	if state.Version != 1 || state.LastSpot < 0 || state.SpotSequence < 0 {
		return nil, fmt.Errorf("V2_STREAM_STATE_INVALID")
	}
	e := NewStreamingEngine()
	e.spotQuote, e.spotBuy, e.spotSell = state.SpotQuote, state.SpotBuy, state.SpotSell
	e.spotTrades, e.spotSequence, e.lastSpot = state.SpotTrades, state.SpotSequence, state.LastSpot
	for ts, x := range state.Spot {
		if ts != x.Bar.TimestampMs || ts > state.LastSpot {
			return nil, fmt.Errorf("V2_SPOT_STATE_INVALID")
		}
		e.spot[ts] = spotAggregate{x.Bar, x.CumulativeQuote, x.CumulativeBuy, x.CumulativeSell, x.CumulativeTrades, x.Sequence}
	}
	e.metrics = append([]MetricsObservation(nil), state.Metrics...)
	e.mark = append([]KlineObservation(nil), state.Mark...)
	e.index = append([]KlineObservation(nil), state.Index...)
	e.premium = append([]KlineObservation(nil), state.Premium...)
	e.funding = append([]FundingObservation(nil), state.Funding...)
	return e, nil
}
