package mainbarrier

import (
	"binance_trader/internal/market"
	"container/heap"
	"fmt"
	"math"
)

type ConfigV2 struct{ StartDecisionTimestampMs, EndDecisionTimestampMs, DecisionIntervalMs, EntryDelayMs, MaxEntryWaitMs, MaxExitReferenceWaitMs, MaxHorizonSeconds int64 }
type StatsV2 struct {
	Trades, Decisions, EntryReferenceAvailable, EntryReferenceUnavailable, Rows int64
	TimeoutReferenceAvailable, TimeoutReferenceUnavailable                      [7]int64
}
type pending2 struct {
	out                           BarrierOutcomeV2
	barrierExpiry                 int64
	active, barrierDone, complete bool
	unresolved, timeouts          int
}
type threshold2 struct {
	target float64
	up     bool
	index  int
	p      *pending2
}
type thresholdHeap2 struct {
	a  []threshold2
	up bool
}

func (h thresholdHeap2) Len() int { return len(h.a) }
func (h thresholdHeap2) Less(i, j int) bool {
	if h.up {
		return h.a[i].target < h.a[j].target
	}
	return h.a[i].target > h.a[j].target
}
func (h thresholdHeap2) Swap(i, j int) { h.a[i], h.a[j] = h.a[j], h.a[i] }
func (h *thresholdHeap2) Push(x any)   { h.a = append(h.a, x.(threshold2)) }
func (h *thresholdHeap2) Pop() any     { n := len(h.a); x := h.a[n-1]; h.a = h.a[:n-1]; return x }

type EngineV2 struct {
	cfg              ConfigV2
	emit             func(BarrierOutcomeV2) error
	nextDecision     int64
	order            []*pending2
	orderHead        int
	expiry           []*pending2
	expiryHead       int
	timeouts         [7][]*pending2
	timeoutHeads     [7]int
	up, down         thresholdHeap2
	lastTime, lastID int64
	activeThresholds int
	stats            StatsV2
}

func NewEngineV2(c ConfigV2, emit func(BarrierOutcomeV2) error) (*EngineV2, error) {
	if c.DecisionIntervalMs <= 0 || c.StartDecisionTimestampMs <= 0 || c.EndDecisionTimestampMs < c.StartDecisionTimestampMs {
		return nil, fmt.Errorf("invalid decision range")
	}
	if c.EntryDelayMs < 0 || c.MaxEntryWaitMs < 0 || c.MaxExitReferenceWaitMs < 0 || c.MaxHorizonSeconds <= 0 {
		return nil, fmt.Errorf("invalid wait/horizon")
	}
	e := &EngineV2{cfg: c, emit: emit, nextDecision: c.StartDecisionTimestampMs}
	e.up.up = true
	heap.Init(&e.up)
	heap.Init(&e.down)
	return e, nil
}
func (e *EngineV2) Add(a market.AggTrade) error {
	if a.Price <= 0 || math.IsNaN(a.Price) || math.IsInf(a.Price, 0) {
		return fmt.Errorf("invalid trade price")
	}
	if e.stats.Trades > 0 && (a.TradeTimeMs < e.lastTime || a.TradeTimeMs == e.lastTime && a.AggTradeID <= e.lastID) {
		return fmt.Errorf("trade order violation")
	}
	e.lastTime, e.lastID = a.TradeTimeMs, a.AggTradeID
	e.stats.Trades++
	e.expire(a.TradeTimeMs)
	e.resolve(&e.up, a)
	e.resolve(&e.down, a)
	e.resolveTimeouts(a)
	for e.nextDecision <= e.cfg.EndDecisionTimestampMs && e.nextDecision+e.cfg.EntryDelayMs <= a.TradeTimeMs {
		d := e.nextDecision
		e.nextDecision += e.cfg.DecisionIntervalMs
		e.stats.Decisions++
		ready := d + e.cfg.EntryDelayMs
		p := &pending2{out: BarrierOutcomeV2{DecisionTimestampMs: d, EntryDelayMs: e.cfg.EntryDelayMs, EntryReadyTimestampMs: ready}}
		if a.TradeTimeMs-ready <= e.cfg.MaxEntryWaitMs {
			p.out.EntryReferenceAvailable = true
			p.out.EntryReferenceTimestampMs = a.TradeTimeMs
			p.out.EntryReferenceAggTradeID = a.AggTradeID
			p.out.EntryReferencePrice = a.Price
			p.out.EntryWaitMs = a.TradeTimeMs - ready
			e.stats.EntryReferenceAvailable++
			p.active = true
			p.unresolved = 2 * len(BarrierGridBps)
			for _, h := range TimeoutHorizonsSeconds {
				if int64(h) <= e.cfg.MaxHorizonSeconds {
					p.timeouts++
				}
			}
			p.barrierExpiry = a.TradeTimeMs + e.cfg.MaxHorizonSeconds*1000
			e.activeThresholds += p.unresolved
			e.expiry = append(e.expiry, p)
			for i, b := range BarrierGridBps {
				x := float64(b) / 10000
				heap.Push(&e.up, threshold2{a.Price * (1 + x), true, i, p})
				heap.Push(&e.down, threshold2{a.Price * (1 - x), false, i, p})
			}
			for i, h := range TimeoutHorizonsSeconds {
				if int64(h) <= e.cfg.MaxHorizonSeconds {
					e.timeouts[i] = append(e.timeouts[i], p)
				}
			}
		} else {
			e.stats.EntryReferenceUnavailable++
			p.complete = true
		}
		e.order = append(e.order, p)
	}
	if err := e.drain(); err != nil {
		return err
	}
	if e.stats.Trades%100000 == 0 && (e.up.Len()+e.down.Len() > e.activeThresholds*3+10000) {
		e.rebuild()
	}
	return nil
}
func (e *EngineV2) expire(now int64) {
	for e.expiryHead < len(e.expiry) && e.expiry[e.expiryHead].barrierExpiry <= now {
		p := e.expiry[e.expiryHead]
		e.expiryHead++
		p.active = false
		p.barrierDone = true
		e.activeThresholds -= p.unresolved
		p.unresolved = 0
		if p.timeouts == 0 {
			p.complete = true
		}
	}
}
func (e *EngineV2) resolve(h *thresholdHeap2, a market.AggTrade) {
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
func (e *EngineV2) resolveTimeouts(a market.AggTrade) {
	for i, h := range TimeoutHorizonsSeconds {
		q := e.timeouts[i]
		head := e.timeoutHeads[i]
		for head < len(q) {
			p := q[head]
			target := p.out.EntryReferenceTimestampMs + int64(h)*1000
			if target > a.TradeTimeMs {
				break
			}
			if a.TradeTimeMs-target <= e.cfg.MaxExitReferenceWaitMs {
				p.out.setTimeout(i, BarrierHit{a.TradeTimeMs, a.AggTradeID, a.Price})
				e.stats.TimeoutReferenceAvailable[i]++
			} else {
				e.stats.TimeoutReferenceUnavailable[i]++
			}
			p.timeouts--
			if p.timeouts == 0 && p.barrierDone {
				p.complete = true
			}
			head++
		}
		e.timeoutHeads[i] = head
	}
}
func (e *EngineV2) drain() error {
	for e.orderHead < len(e.order) && e.order[e.orderHead].complete {
		if err := e.emit(e.order[e.orderHead].out); err != nil {
			return err
		}
		e.orderHead++
		e.stats.Rows++
	}
	return nil
}
func (e *EngineV2) rebuild() {
	f := func(h *thresholdHeap2) {
		d := h.a[:0]
		for _, x := range h.a {
			if x.p.active {
				d = append(d, x)
			}
		}
		h.a = d
		heap.Init(h)
	}
	f(&e.up)
	f(&e.down)
}
func (e *EngineV2) Flush() error {
	if !e.Complete() {
		return fmt.Errorf("source ended before V2 completion: rows=%d next_decision=%d", e.stats.Rows, e.nextDecision)
	}
	return nil
}
func (e *EngineV2) Complete() bool {
	want := (e.cfg.EndDecisionTimestampMs-e.cfg.StartDecisionTimestampMs)/e.cfg.DecisionIntervalMs + 1
	return e.stats.Rows == want
}
func (e *EngineV2) Stats() StatsV2 { return e.stats }
