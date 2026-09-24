package mainoutcome

import (
	"fmt"
	"math"
	"reflect"

	"binance_trader/internal/market"
)

type Sink func(MainOutcomeV1) error
type point struct {
	ts    int64
	value float64
}
type futureWindow struct {
	seconds     int64
	highs, lows []point
}
type pending struct {
	outcome   MainOutcomeV1
	completed int
}
type Stats struct {
	InputBars, DecisionCandidates, OutcomeRows, TailSkipped int64
	StartDecisionTimestampMs, EndDecisionTimestampMs        int64
}
type Engine struct {
	sink    Sink
	windows map[int]*futureWindow
	pending map[int64]*pending
	last    market.SecondBar
	hasLast bool
	stats   Stats
}

func NewEngine(sink Sink) *Engine {
	e := &Engine{sink: sink, windows: map[int]*futureWindow{}, pending: map[int64]*pending{}}
	for _, h := range Horizons {
		e.windows[h] = &futureWindow{seconds: int64(h)}
	}
	return e
}

func (e *Engine) Add(bar market.SecondBar) error {
	if e.sink == nil {
		return fmt.Errorf("outcome: nil sink")
	}
	if e.hasLast && bar.TimestampMs != e.last.TimestampMs+1000 {
		return fmt.Errorf("outcome timestamp discontinuity: %d -> %d", e.last.TimestampMs, bar.TimestampMs)
	}
	e.stats.InputBars++
	for _, h := range Horizons {
		w := e.windows[h]
		w.add(bar)
		decision := bar.TimestampMs - int64(h-1)*1000
		if decision%5000 == 0 {
			if p := e.pending[decision]; p != nil {
				setHorizon(&p.outcome, h, bar.Close, w.highs[0].value, w.lows[0].value)
				p.completed++
				if h == 14400 {
					if err := Validate(p.outcome); err != nil {
						return err
					}
					if err := e.sink(p.outcome); err != nil {
						return err
					}
					e.stats.OutcomeRows++
					if e.stats.StartDecisionTimestampMs == 0 {
						e.stats.StartDecisionTimestampMs = decision
					}
					e.stats.EndDecisionTimestampMs = decision
					delete(e.pending, decision)
				}
			}
		}
	}
	decision := bar.TimestampMs + 1000
	if decision%5000 == 0 {
		e.pending[decision] = &pending{outcome: MainOutcomeV1{DecisionTimestampMs: decision, ReferenceEntryPrice: bar.Close}}
		e.stats.DecisionCandidates++
	}
	e.last = bar
	e.hasLast = true
	return nil
}
func (e *Engine) Flush()       { e.stats.TailSkipped = int64(len(e.pending)) }
func (e *Engine) Stats() Stats { return e.stats }

func (w *futureWindow) add(b market.SecondBar) {
	cut := b.TimestampMs - (w.seconds-1)*1000
	for len(w.highs) > 0 && w.highs[len(w.highs)-1].value <= b.High {
		w.highs = w.highs[:len(w.highs)-1]
	}
	w.highs = append(w.highs, point{b.TimestampMs, b.High})
	for len(w.lows) > 0 && w.lows[len(w.lows)-1].value >= b.Low {
		w.lows = w.lows[:len(w.lows)-1]
	}
	w.lows = append(w.lows, point{b.TimestampMs, b.Low})
	if w.highs[0].ts < cut {
		w.highs = w.highs[1:]
	}
	if w.lows[0].ts < cut {
		w.lows = w.lows[1:]
	}
}

func values(entry, terminal, high, low float64) (marketRet, longRet, shortRet, futureHigh, futureLow, longMFE, longMAE, shortMFE, shortMAE float64) {
	marketRet = terminal/entry - 1
	longRet = marketRet
	shortRet = -marketRet
	futureHigh, futureLow = high, low
	longMFE = math.Max(0, high/entry-1)
	longMAE = math.Max(0, 1-low/entry)
	shortMFE = math.Max(0, 1-low/entry)
	shortMAE = math.Max(0, high/entry-1)
	return
}

func Metrics(o MainOutcomeV1, h int) (marketRet, longMFE, longMAE, shortMFE, shortMAE float64) {
	switch h {
	case 60:
		return o.MarketReturn60s, o.LongMFE60s, o.LongMAE60s, o.ShortMFE60s, o.ShortMAE60s
	case 180:
		return o.MarketReturn180s, o.LongMFE180s, o.LongMAE180s, o.ShortMFE180s, o.ShortMAE180s
	case 300:
		return o.MarketReturn300s, o.LongMFE300s, o.LongMAE300s, o.ShortMFE300s, o.ShortMAE300s
	case 900:
		return o.MarketReturn900s, o.LongMFE900s, o.LongMAE900s, o.ShortMFE900s, o.ShortMAE900s
	case 1800:
		return o.MarketReturn1800s, o.LongMFE1800s, o.LongMAE1800s, o.ShortMFE1800s, o.ShortMAE1800s
	case 3600:
		return o.MarketReturn3600s, o.LongMFE3600s, o.LongMAE3600s, o.ShortMFE3600s, o.ShortMAE3600s
	case 14400:
		return o.MarketReturn14400s, o.LongMFE14400s, o.LongMAE14400s, o.ShortMFE14400s, o.ShortMAE14400s
	}
	panic("unsupported horizon")
}
func setHorizon(o *MainOutcomeV1, h int, terminal, high, low float64) {
	switch h {
	case 60:
		o.MarketReturn60s, o.LongReturn60s, o.ShortReturn60s, o.FutureHigh60s, o.FutureLow60s, o.LongMFE60s, o.LongMAE60s, o.ShortMFE60s, o.ShortMAE60s = values(o.ReferenceEntryPrice, terminal, high, low)
	case 180:
		o.MarketReturn180s, o.LongReturn180s, o.ShortReturn180s, o.FutureHigh180s, o.FutureLow180s, o.LongMFE180s, o.LongMAE180s, o.ShortMFE180s, o.ShortMAE180s = values(o.ReferenceEntryPrice, terminal, high, low)
	case 300:
		o.MarketReturn300s, o.LongReturn300s, o.ShortReturn300s, o.FutureHigh300s, o.FutureLow300s, o.LongMFE300s, o.LongMAE300s, o.ShortMFE300s, o.ShortMAE300s = values(o.ReferenceEntryPrice, terminal, high, low)
	case 900:
		o.MarketReturn900s, o.LongReturn900s, o.ShortReturn900s, o.FutureHigh900s, o.FutureLow900s, o.LongMFE900s, o.LongMAE900s, o.ShortMFE900s, o.ShortMAE900s = values(o.ReferenceEntryPrice, terminal, high, low)
	case 1800:
		o.MarketReturn1800s, o.LongReturn1800s, o.ShortReturn1800s, o.FutureHigh1800s, o.FutureLow1800s, o.LongMFE1800s, o.LongMAE1800s, o.ShortMFE1800s, o.ShortMAE1800s = values(o.ReferenceEntryPrice, terminal, high, low)
	case 3600:
		o.MarketReturn3600s, o.LongReturn3600s, o.ShortReturn3600s, o.FutureHigh3600s, o.FutureLow3600s, o.LongMFE3600s, o.LongMAE3600s, o.ShortMFE3600s, o.ShortMAE3600s = values(o.ReferenceEntryPrice, terminal, high, low)
	case 14400:
		o.MarketReturn14400s, o.LongReturn14400s, o.ShortReturn14400s, o.FutureHigh14400s, o.FutureLow14400s, o.LongMFE14400s, o.LongMAE14400s, o.ShortMFE14400s, o.ShortMAE14400s = values(o.ReferenceEntryPrice, terminal, high, low)
	}
}

func Validate(o MainOutcomeV1) error {
	if o.DecisionTimestampMs%5000 != 0 || o.ReferenceEntryPrice <= 0 {
		return fmt.Errorf("invalid outcome key/reference")
	}
	v := reflect.ValueOf(o)
	for i := 0; i < v.NumField(); i++ {
		if v.Field(i).Kind() == reflect.Float64 {
			x := v.Field(i).Float()
			if math.IsNaN(x) || math.IsInf(x, 0) {
				return fmt.Errorf("non-finite %s", v.Type().Field(i).Name)
			}
		}
	}
	highs := []float64{o.FutureHigh60s, o.FutureHigh180s, o.FutureHigh300s, o.FutureHigh900s, o.FutureHigh1800s, o.FutureHigh3600s, o.FutureHigh14400s}
	lows := []float64{o.FutureLow60s, o.FutureLow180s, o.FutureLow300s, o.FutureLow900s, o.FutureLow1800s, o.FutureLow3600s, o.FutureLow14400s}
	mfe := []float64{o.LongMFE60s, o.LongMFE180s, o.LongMFE300s, o.LongMFE900s, o.LongMFE1800s, o.LongMFE3600s, o.LongMFE14400s}
	mae := []float64{o.LongMAE60s, o.LongMAE180s, o.LongMAE300s, o.LongMAE900s, o.LongMAE1800s, o.LongMAE3600s, o.LongMAE14400s}
	marketReturns := []float64{o.MarketReturn60s, o.MarketReturn180s, o.MarketReturn300s, o.MarketReturn900s, o.MarketReturn1800s, o.MarketReturn3600s, o.MarketReturn14400s}
	longReturns := []float64{o.LongReturn60s, o.LongReturn180s, o.LongReturn300s, o.LongReturn900s, o.LongReturn1800s, o.LongReturn3600s, o.LongReturn14400s}
	shortReturns := []float64{o.ShortReturn60s, o.ShortReturn180s, o.ShortReturn300s, o.ShortReturn900s, o.ShortReturn1800s, o.ShortReturn3600s, o.ShortReturn14400s}
	shortMFE := []float64{o.ShortMFE60s, o.ShortMFE180s, o.ShortMFE300s, o.ShortMFE900s, o.ShortMFE1800s, o.ShortMFE3600s, o.ShortMFE14400s}
	shortMAE := []float64{o.ShortMAE60s, o.ShortMAE180s, o.ShortMAE300s, o.ShortMAE900s, o.ShortMAE1800s, o.ShortMAE3600s, o.ShortMAE14400s}
	for i := range highs {
		if highs[i] <= 0 || lows[i] <= 0 || highs[i] < lows[i] || mfe[i] < 0 || mae[i] < 0 {
			return fmt.Errorf("invalid horizon %d", Horizons[i])
		}
		if i > 0 && (highs[i] < highs[i-1] || lows[i] > lows[i-1] || mfe[i]+1e-15 < mfe[i-1] || mae[i]+1e-15 < mae[i-1]) {
			return fmt.Errorf("horizon monotonicity violation at %d", Horizons[i])
		}
		if !same(longReturns[i], marketReturns[i]) || !same(shortReturns[i], -marketReturns[i]) || !same(longReturns[i]+shortReturns[i], 0) || !same(mfe[i], shortMAE[i]) || !same(mae[i], shortMFE[i]) {
			return fmt.Errorf("directional symmetry violation at %d", Horizons[i])
		}
	}
	return nil
}

func same(a, b float64) bool {
	return math.Abs(a-b) <= 1e-12*math.Max(1, math.Max(math.Abs(a), math.Abs(b)))
}
