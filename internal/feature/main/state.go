package mainfeature

import (
	"fmt"

	"binance_trader/internal/market"
)

// EngineState stores the numeric accumulators as well as their compensation
// terms. Replaying only a truncated bar window cannot preserve float bits.
type EngineState struct {
	Version         int                  `json:"version"`
	N               int64                `json:"n"`
	LastTimestampMs int64                `json:"last_timestamp_ms"`
	LastBar         market.SecondBar     `json:"last_bar"`
	Ring            []SampleState        `json:"ring"`
	Windows         map[int]RollingState `json:"windows"`
}
type SampleState struct{ Close, Base, Quote, Buy, Sell, Count, NoTrade, ReturnSq float64 }
type PointState struct {
	Seq   int64
	Value float64
}
type RollingState struct {
	Size        int
	Sums        [7]float64 // base, quote, buy, sell, count, noTrade, returnSq
	Corrections [7]float64
	Highs, Lows []PointState
}

func (e *Engine) ExportState() EngineState {
	state := EngineState{Version: 1, N: e.n, LastTimestampMs: e.lastTimestampMs, LastBar: e.lastBar, Ring: make([]SampleState, len(e.ring)), Windows: make(map[int]RollingState, len(e.windows))}
	for i, x := range e.ring {
		state.Ring[i] = SampleState{x.close, x.base, x.quote, x.buy, x.sell, x.count, x.noTrade, x.returnSq}
	}
	for size, x := range e.windows {
		s := RollingState{Size: x.size, Sums: [7]float64{x.base, x.quote, x.buy, x.sell, x.count, x.noTrade, x.returnSq}, Corrections: [7]float64{x.baseC, x.quoteC, x.buyC, x.sellC, x.countC, x.noTradeC, x.returnSqC}}
		for _, y := range x.highs {
			s.Highs = append(s.Highs, PointState{y.seq, y.value})
		}
		for _, y := range x.lows {
			s.Lows = append(s.Lows, PointState{y.seq, y.value})
		}
		state.Windows[size] = s
	}
	return state
}

func RestoreEngine(state EngineState) (*Engine, error) {
	if state.Version != 1 || state.N < 0 || len(state.Ring) != maxWindow+1 || len(state.Windows) != len(windowSizes) || (state.N > 0 && state.LastBar.TimestampMs != state.LastTimestampMs) {
		return nil, fmt.Errorf("V1_ENGINE_STATE_INVALID")
	}
	e := NewEngine()
	e.n, e.lastTimestampMs, e.lastBar = state.N, state.LastTimestampMs, state.LastBar
	for i, x := range state.Ring {
		e.ring[i] = sample{x.Close, x.Base, x.Quote, x.Buy, x.Sell, x.Count, x.NoTrade, x.ReturnSq}
	}
	for _, size := range windowSizes {
		x, ok := state.Windows[size]
		if !ok || x.Size != size {
			return nil, fmt.Errorf("V1_ENGINE_WINDOW_MISMATCH")
		}
		w := &rolling{size: size, base: x.Sums[0], quote: x.Sums[1], buy: x.Sums[2], sell: x.Sums[3], count: x.Sums[4], noTrade: x.Sums[5], returnSq: x.Sums[6], baseC: x.Corrections[0], quoteC: x.Corrections[1], buyC: x.Corrections[2], sellC: x.Corrections[3], countC: x.Corrections[4], noTradeC: x.Corrections[5], returnSqC: x.Corrections[6]}
		for _, y := range x.Highs {
			w.highs = append(w.highs, point{y.Seq, y.Value})
		}
		for _, y := range x.Lows {
			w.lows = append(w.lows, point{y.Seq, y.Value})
		}
		e.windows[size] = w
	}
	return e, nil
}
