package mainfeature

import (
	"fmt"
	"math"
	"reflect"

	"binance_trader/internal/market"
)

const maxWindow = 14400

var windowSizes = []int{1, 3, 5, 10, 30, 60, 120, 180, 300, 900, 1800, 3600, 14400}

type sample struct{ close, base, quote, buy, sell, count, noTrade, returnSq float64 }
type point struct {
	seq   int64
	value float64
}
type rolling struct {
	size                                             int
	base, quote, buy, sell, count, noTrade, returnSq float64
	baseC, quoteC, buyC, sellC                       float64
	countC, noTradeC, returnSqC                      float64
	highs, lows                                      []point
}

type Engine struct {
	windows         map[int]*rolling
	ring            []sample
	n               int64
	lastTimestampMs int64
	lastBar         market.SecondBar
}

func NewEngine() *Engine {
	e := &Engine{windows: make(map[int]*rolling), ring: make([]sample, maxWindow+1)}
	for _, size := range windowSizes {
		e.windows[size] = &rolling{size: size}
	}
	return e
}

func (e *Engine) Ready() bool { return e.n > maxWindow }

// Add updates state from one completed bar and emits only when the next logical
// decision time is aligned to the absolute UTC five-second clock.
func (e *Engine) Add(bar market.SecondBar) (*MainFeaturesV1, error) {
	if e.n > 0 && bar.TimestampMs != e.lastTimestampMs+1000 {
		return nil, fmt.Errorf("feature engine timestamp discontinuity: previous=%d current=%d", e.lastTimestampMs, bar.TimestampMs)
	}
	r1 := 0.0
	if e.n > 0 {
		r1 = safeLogRatio(bar.Close, e.lastBar.Close)
	}
	s := sample{close: bar.Close, base: bar.BaseVolume, quote: bar.QuoteVolume, buy: bar.TakerBuyBaseVolume, sell: bar.TakerSellBaseVolume, count: float64(bar.AggTradeCount), returnSq: r1 * r1}
	if !bar.HasTrade {
		s.noTrade = 1
	}
	seq := e.n
	for _, w := range e.windows {
		w.add(seq, bar, s)
		if seq >= int64(w.size) {
			w.remove(e.get(seq - int64(w.size)))
		}
	}
	e.ring[seq%int64(len(e.ring))] = s
	e.n++
	e.lastTimestampMs, e.lastBar = bar.TimestampMs, bar
	decision := bar.TimestampMs + 1000
	if !e.Ready() || decision%5000 != 0 {
		return nil, nil
	}
	f := e.snapshot(decision, bar)
	if err := Validate(f); err != nil {
		return nil, err
	}
	return &f, nil
}

func (e *Engine) get(seq int64) sample         { return e.ring[seq%int64(len(e.ring))] }
func (e *Engine) closeAgo(seconds int) float64 { return e.get(e.n - 1 - int64(seconds)).close }
func (e *Engine) w(seconds int) *rolling       { return e.windows[seconds] }

func (w *rolling) add(seq int64, bar market.SecondBar, s sample) {
	if w.size == 1 {
		w.base, w.quote, w.buy, w.sell = s.base, s.quote, s.buy, s.sell
		w.count, w.noTrade, w.returnSq = s.count, s.noTrade, s.returnSq
		w.baseC, w.quoteC, w.buyC, w.sellC, w.countC, w.noTradeC, w.returnSqC = 0, 0, 0, 0, 0, 0, 0
		w.highs, w.lows = []point{{seq, bar.High}}, []point{{seq, bar.Low}}
		return
	}
	compensated(&w.base, &w.baseC, s.base)
	compensated(&w.quote, &w.quoteC, s.quote)
	compensated(&w.buy, &w.buyC, s.buy)
	compensated(&w.sell, &w.sellC, s.sell)
	compensated(&w.count, &w.countC, s.count)
	compensated(&w.noTrade, &w.noTradeC, s.noTrade)
	compensated(&w.returnSq, &w.returnSqC, s.returnSq)
	for len(w.highs) > 0 && w.highs[len(w.highs)-1].value <= bar.High {
		w.highs = w.highs[:len(w.highs)-1]
	}
	w.highs = append(w.highs, point{seq, bar.High})
	for len(w.lows) > 0 && w.lows[len(w.lows)-1].value >= bar.Low {
		w.lows = w.lows[:len(w.lows)-1]
	}
	w.lows = append(w.lows, point{seq, bar.Low})
	oldest := seq - int64(w.size) + 1
	if len(w.highs) > 0 && w.highs[0].seq < oldest {
		w.highs = w.highs[1:]
	}
	if len(w.lows) > 0 && w.lows[0].seq < oldest {
		w.lows = w.lows[1:]
	}
}
func (w *rolling) remove(s sample) {
	if w.size == 1 {
		return
	}
	compensated(&w.base, &w.baseC, -s.base)
	compensated(&w.quote, &w.quoteC, -s.quote)
	compensated(&w.buy, &w.buyC, -s.buy)
	compensated(&w.sell, &w.sellC, -s.sell)
	compensated(&w.count, &w.countC, -s.count)
	compensated(&w.noTrade, &w.noTradeC, -s.noTrade)
	compensated(&w.returnSq, &w.returnSqC, -s.returnSq)
}

func compensated(sum, correction *float64, value float64) {
	y := value - *correction
	t := *sum + y
	*correction = (t - *sum) - y
	*sum = t
}

func (e *Engine) snapshot(decision int64, b market.SecondBar) MainFeaturesV1 {
	f := MainFeaturesV1{DecisionTimestampMs: decision, ReferenceClose: b.Close, BarLogReturn: safeLogRatio(b.Close, b.Open), BarRange: safeDiv(b.High-b.Low, b.Close), BarVWAPDeviation: safeDiv(b.Close-b.VWAP, b.VWAP)}
	f.RetLog1s = e.ret(1)
	f.RetLog3s = e.ret(3)
	f.RetLog5s = e.ret(5)
	f.RetLog10s = e.ret(10)
	f.RetLog30s = e.ret(30)
	f.RetLog60s = e.ret(60)
	f.RetLog180s = e.ret(180)
	f.RetLog300s = e.ret(300)
	f.RetLog900s = e.ret(900)
	f.RetLog1800s = e.ret(1800)
	f.RetLog3600s = e.ret(3600)
	f.RetLog14400s = e.ret(14400)
	f.RangeWidth10s, f.RangePosition10s = e.rangeFeatures(10, b.Close)
	f.RangeWidth30s, f.RangePosition30s = e.rangeFeatures(30, b.Close)
	f.RangeWidth60s, f.RangePosition60s = e.rangeFeatures(60, b.Close)
	f.RangeWidth300s, f.RangePosition300s = e.rangeFeatures(300, b.Close)
	f.RangeWidth900s, f.RangePosition900s = e.rangeFeatures(900, b.Close)
	f.RangeWidth3600s, f.RangePosition3600s = e.rangeFeatures(3600, b.Close)
	f.RangeWidth14400s, f.RangePosition14400s = e.rangeFeatures(14400, b.Close)
	f.RV10s = e.rv(10)
	f.RV30s = e.rv(30)
	f.RV60s = e.rv(60)
	f.RV300s = e.rv(300)
	f.RV900s = e.rv(900)
	f.RV3600s = e.rv(3600)
	f.RV14400s = e.rv(14400)
	f.BaseVolumeSum5s, f.QuoteVolumeSum5s, f.AggTradeCountSum5s = e.volume(5)
	f.BaseVolumeSum30s, f.QuoteVolumeSum30s, f.AggTradeCountSum30s = e.volume(30)
	f.BaseVolumeSum60s, f.QuoteVolumeSum60s, f.AggTradeCountSum60s = e.volume(60)
	f.BaseVolumeSum300s, f.QuoteVolumeSum300s, f.AggTradeCountSum300s = e.volume(300)
	f.BaseVolumeSum900s, f.QuoteVolumeSum900s, f.AggTradeCountSum900s = e.volume(900)
	f.TakerImbalance1s = e.imbalance(1)
	f.TakerImbalance5s = e.imbalance(5)
	f.TakerImbalance10s = e.imbalance(10)
	f.TakerImbalance30s = e.imbalance(30)
	f.TakerImbalance60s = e.imbalance(60)
	f.TakerImbalance300s = e.imbalance(300)
	f.TakerImbalance900s = e.imbalance(900)
	f.VWAPDeviation30s = e.vwapDeviation(30, b.Close)
	f.VWAPDeviation60s = e.vwapDeviation(60, b.Close)
	f.VWAPDeviation300s = e.vwapDeviation(300, b.Close)
	f.VWAPDeviation900s = e.vwapDeviation(900, b.Close)
	f.TradeIntensity5s = e.w(5).count / 5
	f.TradeIntensity30s = e.w(30).count / 30
	f.TradeIntensity60s = e.w(60).count / 60
	f.TradeIntensity300s = e.w(300).count / 300
	f.NoTradeRatio30s = e.w(30).noTrade / 30
	f.NoTradeRatio60s = e.w(60).noTrade / 60
	f.NoTradeRatio300s = e.w(300).noTrade / 300
	f.NoTradeRatio900s = e.w(900).noTrade / 900
	f.MomentumAccel5s = e.accel(5)
	f.MomentumAccel30s = e.accel(30)
	f.MomentumAccel60s = e.accel(60)
	f.ImbalanceChange5s = e.imbalanceChange(5)
	f.ImbalanceChange30s = e.imbalanceChange(30)
	f.ImbalanceChange60s = e.imbalanceChange(60)
	f.VolumeRatio5s60s = safeDiv(e.w(5).quote/5, e.w(60).quote/60)
	f.TradeIntensityRatio5s60s = safeDiv(e.w(5).count/5, e.w(60).count/60)
	f.VolumeRatio30s300s = safeDiv(e.w(30).quote/30, e.w(300).quote/300)
	f.TradeIntensityRatio30s300s = safeDiv(e.w(30).count/30, e.w(300).count/300)
	return f
}

func (e *Engine) ret(w int) float64 { return safeLogRatio(e.lastBar.Close, e.closeAgo(w)) }
func (e *Engine) rangeFeatures(n int, close float64) (float64, float64) {
	w := e.w(n)
	width := w.highs[0].value - w.lows[0].value
	return safeDiv(width, close), safeDiv(close-w.lows[0].value, width)
}
func (e *Engine) rv(n int) float64 { return math.Sqrt(math.Max(0, e.w(n).returnSq)) }
func (e *Engine) volume(n int) (float64, float64, float64) {
	w := e.w(n)
	if w.count == 0 {
		return 0, 0, 0
	}
	return w.base, w.quote, w.count
}
func (e *Engine) imbalance(n int) float64 {
	w := e.w(n)
	if w.count == 0 {
		return 0
	}
	return clampImbalance(safeDiv(w.buy-w.sell, w.buy+w.sell))
}
func (e *Engine) vwapDeviation(n int, close float64) float64 {
	w := e.w(n)
	if w.count == 0 {
		return 0
	}
	v := safeDiv(w.quote, w.base)
	return safeDiv(close-v, v)
}
func (e *Engine) accel(n int) float64 {
	current := safeLogRatio(e.lastBar.Close, e.closeAgo(n))
	previous := safeLogRatio(e.closeAgo(n), e.closeAgo(2*n))
	return current - previous
}
func (e *Engine) imbalanceChange(n int) float64 {
	current := e.imbalance(n)
	wide := e.w(2 * n)
	recent := e.w(n)
	prevBuy, prevSell := wide.buy-recent.buy, wide.sell-recent.sell
	return current - clampImbalance(safeDiv(prevBuy-prevSell, prevBuy+prevSell))
}
func clampImbalance(x float64) float64 {
	if x > 1 {
		return 1
	}
	if x < -1 {
		return -1
	}
	return x
}
func safeDiv(a, b float64) float64 {
	if b == 0 {
		return 0
	}
	return a / b
}
func safeLogRatio(a, b float64) float64 {
	if a <= 0 || b <= 0 {
		return 0
	}
	return math.Log(a / b)
}

func Validate(f MainFeaturesV1) error {
	v := reflect.ValueOf(f)
	typ := v.Type()
	for i := 0; i < v.NumField(); i++ {
		if v.Field(i).Kind() == reflect.Float64 {
			x := v.Field(i).Float()
			if math.IsNaN(x) || math.IsInf(x, 0) {
				return fmt.Errorf("invalid feature %s=%v at %d", typ.Field(i).Name, x, f.DecisionTimestampMs)
			}
		}
	}
	if f.DecisionTimestampMs%5000 != 0 {
		return fmt.Errorf("decision timestamp is not 5-second aligned: %d", f.DecisionTimestampMs)
	}
	return nil
}
