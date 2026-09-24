package mainbarrier

import (
	"container/heap"
	"fmt"
	"math"

	"binance_trader/internal/market"
)

type Config struct {
	StartDecisionTimestampMs int64
	EndDecisionTimestampMs   int64
	DecisionIntervalMs       int64
	EntryDelayMs             int64
	MaxEntryWaitMs           int64
	MaxHorizonSeconds        int64
}

type Stats struct {
	Trades           int64
	Decisions        int64
	EntryAvailable   int64
	EntryUnavailable int64
	Rows             int64
}

type pending struct {
	out        BarrierOutcomeV1
	expiry     int64
	active     bool
	unresolved int
}
type threshold struct {
	target float64
	up     bool
	index  int
	p      *pending
}
type thresholdHeap struct {
	a  []threshold
	up bool
}

func (h thresholdHeap) Len() int { return len(h.a) }
func (h thresholdHeap) Less(i, j int) bool {
	if h.up {
		return h.a[i].target < h.a[j].target
	}
	return h.a[i].target > h.a[j].target
}
func (h thresholdHeap) Swap(i, j int) { h.a[i], h.a[j] = h.a[j], h.a[i] }
func (h *thresholdHeap) Push(x any)   { h.a = append(h.a, x.(threshold)) }
func (h *thresholdHeap) Pop() any     { n := len(h.a); x := h.a[n-1]; h.a = h.a[:n-1]; return x }

type Engine struct {
	cfg              Config
	emit             func(BarrierOutcomeV1) error
	nextDecision     int64
	queue            []*pending
	queueHead        int
	up, down         thresholdHeap
	lastTime, lastID int64
	activeThresholds int
	stats            Stats
}

func NewEngine(cfg Config, emit func(BarrierOutcomeV1) error) (*Engine, error) {
	if cfg.DecisionIntervalMs <= 0 || cfg.StartDecisionTimestampMs <= 0 || cfg.EndDecisionTimestampMs < cfg.StartDecisionTimestampMs {
		return nil, fmt.Errorf("invalid decision range/config")
	}
	if cfg.EntryDelayMs < 0 || cfg.MaxEntryWaitMs < 0 || cfg.MaxHorizonSeconds <= 0 || cfg.MaxEntryWaitMs >= cfg.MaxHorizonSeconds*1000 {
		return nil, fmt.Errorf("invalid delay/wait/horizon")
	}
	e := &Engine{cfg: cfg, emit: emit, nextDecision: cfg.StartDecisionTimestampMs}
	e.up.up = true
	heap.Init(&e.up)
	heap.Init(&e.down)
	return e, nil
}

func (e *Engine) Add(a market.AggTrade) error {
	if a.Price <= 0 || math.IsNaN(a.Price) || math.IsInf(a.Price, 0) {
		return fmt.Errorf("invalid trade price at id %d", a.AggTradeID)
	}
	if e.stats.Trades > 0 && (a.TradeTimeMs < e.lastTime || (a.TradeTimeMs == e.lastTime && a.AggTradeID <= e.lastID)) {
		return fmt.Errorf("trade order violation (%d,%d) after (%d,%d)", a.TradeTimeMs, a.AggTradeID, e.lastTime, e.lastID)
	}
	e.lastTime, e.lastID = a.TradeTimeMs, a.AggTradeID
	e.stats.Trades++
	if err := e.expire(a.TradeTimeMs); err != nil {
		return err
	}
	e.resolve(&e.up, a)
	e.resolve(&e.down, a)
	for e.nextDecision <= e.cfg.EndDecisionTimestampMs && e.nextDecision+e.cfg.EntryDelayMs <= a.TradeTimeMs {
		d := e.nextDecision
		e.nextDecision += e.cfg.DecisionIntervalMs
		e.stats.Decisions++
		p := &pending{out: BarrierOutcomeV1{DecisionTimestampMs: d, EntryDelayMs: e.cfg.EntryDelayMs}, expiry: d + e.cfg.MaxHorizonSeconds*1000, active: true}
		ready := d + e.cfg.EntryDelayMs
		if a.TradeTimeMs-ready <= e.cfg.MaxEntryWaitMs {
			p.out.EntryAvailable = true
			p.out.EntryTradeTimestampMs = a.TradeTimeMs
			p.out.EntryAggTradeID = a.AggTradeID
			p.out.EntryReferencePrice = a.Price
			p.out.EntryWaitMs = a.TradeTimeMs - ready
			e.stats.EntryAvailable++
			p.unresolved = 2 * len(BarrierGridBps)
			e.activeThresholds += p.unresolved
			for i, b := range BarrierGridBps {
				x := float64(b) / 10000
				heap.Push(&e.up, threshold{a.Price * (1 + x), true, i, p})
				heap.Push(&e.down, threshold{a.Price * (1 - x), false, i, p})
			}
		} else {
			e.stats.EntryUnavailable++
		}
		e.queue = append(e.queue, p)
	}
	if e.stats.Trades%100000 == 0 && (e.up.Len()+e.down.Len() > e.activeThresholds*3+10000) {
		e.rebuild()
	}
	return nil
}

func (e *Engine) resolve(h *thresholdHeap, a market.AggTrade) {
	for h.Len() > 0 {
		x := h.a[0]
		if !x.p.active {
			heap.Pop(h)
			continue
		}
		cross := x.up && a.Price >= x.target || !x.up && a.Price <= x.target
		if !cross {
			return
		}
		heap.Pop(h)
		x.p.out.setHit(x.up, x.index, BarrierHit{a.TradeTimeMs, a.AggTradeID, a.Price})
		x.p.unresolved--
		e.activeThresholds--
	}
}
func (e *Engine) expire(now int64) error {
	for e.queueHead < len(e.queue) && e.queue[e.queueHead].expiry <= now {
		p := e.queue[e.queueHead]
		e.queueHead++
		p.active = false
		e.activeThresholds -= p.unresolved
		p.unresolved = 0
		if err := e.emit(p.out); err != nil {
			return err
		}
		e.stats.Rows++
	}
	if e.queueHead > 8192 && e.queueHead*2 > len(e.queue) {
		e.queue = append([]*pending(nil), e.queue[e.queueHead:]...)
		e.queueHead = 0
	}
	return nil
}
func (e *Engine) rebuild() {
	filter := func(h *thresholdHeap) {
		dst := h.a[:0]
		for _, x := range h.a {
			if x.p.active {
				dst = append(dst, x)
			}
		}
		h.a = dst
		heap.Init(h)
	}
	filter(&e.up)
	filter(&e.down)
}
func (e *Engine) Flush() error {
	if e.nextDecision <= e.cfg.EndDecisionTimestampMs {
		return fmt.Errorf("source ended before decision %d received an entry opportunity", e.nextDecision)
	}
	return e.expire(math.MaxInt64)
}
func (e *Engine) Stats() Stats { return e.stats }
func (e *Engine) Complete() bool {
	want := (e.cfg.EndDecisionTimestampMs-e.cfg.StartDecisionTimestampMs)/e.cfg.DecisionIntervalMs + 1
	return e.stats.Rows == want
}
