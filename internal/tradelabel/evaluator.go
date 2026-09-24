package tradelabel

import (
	mainbarrier "binance_trader/internal/barrier/main"
	mainoutcome "binance_trader/internal/outcome/main"
	"fmt"
	"math"
	"strings"
)

type Side string

const (
	Long  Side = "LONG"
	Short Side = "SHORT"
)

type Status string

const (
	TPFirst                   Status = "TP_FIRST"
	SLFirst                   Status = "SL_FIRST"
	Timeout                   Status = "TIMEOUT"
	EntryUnavailable          Status = "ENTRY_UNAVAILABLE"
	EntryReferenceUnavailable Status = "ENTRY_REFERENCE_UNAVAILABLE"
	ExitReferenceUnavailable  Status = "EXIT_REFERENCE_UNAVAILABLE"
)

type TradeSpec struct {
	Side           Side
	TPBps          int
	SLBps          int
	HorizonSeconds int
}

type TradeResultV2 struct {
	DecisionTimestampMs                                                             int64
	Status                                                                          Status
	LabelValid                                                                      bool
	ExitTimestampMs, ExitAggTradeID                                                 int64
	EntryReferencePrice, ExitReferencePrice                                         float64
	AdjustedEntryPrice, AdjustedExitPrice                                           float64
	GrossMarketReturn, FeeCostReturn, ModeledSlippageCostReturn, NetReturnExFunding float64
	GrossProfitable, NetProfitableExFunding                                         bool
}

// EvaluateV2 evaluates reference-price first-passage labels. The entry and
// timeout prices are aggTrade execution proxies, not reconstructed fills.
func EvaluateV2(b mainbarrier.BarrierOutcomeV2, s TradeSpec, c CostProfile) (TradeResultV2, error) {
	r := TradeResultV2{DecisionTimestampMs: b.DecisionTimestampMs, EntryReferencePrice: b.EntryReferencePrice}
	if e := ValidateSpec(s); e != nil {
		return r, e
	}
	if !b.EntryReferenceAvailable {
		r.Status = EntryReferenceUnavailable
		return r, nil
	}
	var tp, sl mainbarrier.BarrierHit
	if s.Side == Long {
		tp, _ = b.Hit(true, s.TPBps)
		sl, _ = b.Hit(false, s.SLBps)
	} else {
		tp, _ = b.Hit(false, s.TPBps)
		sl, _ = b.Hit(true, s.SLBps)
	}
	cutoff := b.EntryReferenceTimestampMs + int64(s.HorizonSeconds)*1000
	valid := func(h mainbarrier.BarrierHit) bool { return h.TimestampMs > 0 && h.TimestampMs < cutoff }
	tpv, slv := valid(tp), valid(sl)
	exitFee, exitSlip := c.TimeoutExitFeeRate, c.TimeoutSlippageBps
	if tpv && (!slv || before(tp, sl)) {
		r.Status = TPFirst
		r.ExitTimestampMs = tp.TimestampMs
		r.ExitAggTradeID = tp.AggTradeID
		r.ExitReferencePrice = tp.Price
		exitFee = c.TPExitFeeRate
		exitSlip = c.TPExitSlippageBps
	} else if slv {
		r.Status = SLFirst
		r.ExitTimestampMs = sl.TimestampMs
		r.ExitAggTradeID = sl.AggTradeID
		r.ExitReferencePrice = sl.Price
		exitFee = c.SLExitFeeRate
		exitSlip = c.SLExitSlippageBps
	} else {
		x, ok := b.TimeoutReference(s.HorizonSeconds)
		if !ok {
			return r, fmt.Errorf("unsupported timeout horizon")
		}
		if x.TimestampMs == 0 {
			r.Status = ExitReferenceUnavailable
			return r, nil
		}
		r.Status = Timeout
		r.ExitTimestampMs = x.TimestampMs
		r.ExitAggTradeID = x.AggTradeID
		r.ExitReferencePrice = x.Price
	}
	r.LabelValid = true
	dir := 1.0
	if s.Side == Short {
		dir = -1
	}
	r.GrossMarketReturn = dir * (r.ExitReferencePrice/r.EntryReferencePrice - 1)
	ef := 1 + c.EntrySlippageBps/10000
	if s.Side == Short {
		ef = 1 - c.EntrySlippageBps/10000
	}
	xf := 1 - exitSlip/10000
	if s.Side == Short {
		xf = 1 + exitSlip/10000
	}
	r.AdjustedEntryPrice = r.EntryReferencePrice * ef
	r.AdjustedExitPrice = r.ExitReferencePrice * xf
	adjusted := dir * (r.AdjustedExitPrice/r.AdjustedEntryPrice - 1)
	r.ModeledSlippageCostReturn = r.GrossMarketReturn - adjusted
	r.FeeCostReturn = c.EntryFeeRate + (r.AdjustedExitPrice/r.AdjustedEntryPrice)*exitFee
	r.NetReturnExFunding = r.GrossMarketReturn - r.ModeledSlippageCostReturn - r.FeeCostReturn
	r.GrossProfitable = r.GrossMarketReturn > 0
	r.NetProfitableExFunding = r.NetReturnExFunding > 0
	if bad(r.GrossMarketReturn) || bad(r.NetReturnExFunding) {
		return r, fmt.Errorf("non-finite V2 result")
	}
	return r, nil
}

type CostProfile struct {
	EntryFeeRate, TPExitFeeRate, SLExitFeeRate, TimeoutExitFeeRate             float64
	EntrySlippageBps, TPExitSlippageBps, SLExitSlippageBps, TimeoutSlippageBps float64
}
type TradeResult struct {
	DecisionTimestampMs                                                                 int64
	Status                                                                              Status
	ExitTimestampMs, ExitAggTradeID                                                     int64
	EntryReferencePrice, ExitReferencePrice, AdjustedEntryPrice, AdjustedExitPrice      float64
	GrossReturn, AdjustedGrossReturn, FeeCostReturn, SlippageEffect, NetReturnExFunding float64
	GrossProfitable, NetProfitableExFunding                                             bool
}

func ParseSide(s string) (Side, error) {
	x := Side(strings.ToUpper(s))
	if x != Long && x != Short {
		return "", fmt.Errorf("invalid side %q", s)
	}
	return x, nil
}
func ValidateSpec(s TradeSpec) error {
	if s.Side != Long && s.Side != Short {
		return fmt.Errorf("invalid side")
	}
	if s.HorizonSeconds <= 0 || s.HorizonSeconds > mainbarrier.MaxHorizonSeconds {
		return fmt.Errorf("unsupported horizon")
	}
	if _, ok := grid(s.TPBps); !ok {
		return fmt.Errorf("TP bps not in barrier grid")
	}
	if _, ok := grid(s.SLBps); !ok {
		return fmt.Errorf("SL bps not in barrier grid")
	}
	return nil
}
func grid(b int) (int, bool) {
	for i, x := range mainbarrier.BarrierGridBps {
		if x == b {
			return i, true
		}
	}
	return 0, false
}
func Evaluate(b mainbarrier.BarrierOutcomeV1, o mainoutcome.MainOutcomeV2, s TradeSpec, c CostProfile) (TradeResult, error) {
	r := TradeResult{DecisionTimestampMs: b.DecisionTimestampMs, EntryReferencePrice: b.EntryReferencePrice}
	if e := ValidateSpec(s); e != nil {
		return r, e
	}
	if b.DecisionTimestampMs != o.DecisionTimestampMs {
		return r, fmt.Errorf("decision mismatch")
	}
	if !b.EntryAvailable {
		r.Status = EntryUnavailable
		return r, nil
	}
	var tp, sl mainbarrier.BarrierHit
	if s.Side == Long {
		tp, _ = b.Hit(true, s.TPBps)
		sl, _ = b.Hit(false, s.SLBps)
	} else {
		tp, _ = b.Hit(false, s.TPBps)
		sl, _ = b.Hit(true, s.SLBps)
	}
	cutoff := b.DecisionTimestampMs + int64(s.HorizonSeconds)*1000
	valid := func(h mainbarrier.BarrierHit) bool { return h.TimestampMs > 0 && h.TimestampMs < cutoff }
	tpv, slv := valid(tp), valid(sl)
	exitFee, exitSlip := c.TimeoutExitFeeRate, c.TimeoutSlippageBps
	if tpv && (!slv || before(tp, sl)) {
		r.Status = TPFirst
		r.ExitTimestampMs = tp.TimestampMs
		r.ExitAggTradeID = tp.AggTradeID
		r.ExitReferencePrice = tp.Price
		exitFee = c.TPExitFeeRate
		exitSlip = c.TPExitSlippageBps
	} else if slv {
		r.Status = SLFirst
		r.ExitTimestampMs = sl.TimestampMs
		r.ExitAggTradeID = sl.AggTradeID
		r.ExitReferencePrice = sl.Price
		exitFee = c.SLExitFeeRate
		exitSlip = c.SLExitSlippageBps
	} else {
		r.Status = Timeout
		r.ExitTimestampMs = cutoff
		r.ExitReferencePrice = terminal(o, s.HorizonSeconds)
	}
	if r.ExitReferencePrice <= 0 {
		return r, fmt.Errorf("invalid exit reference price")
	}
	dir := 1.0
	if s.Side == Short {
		dir = -1
	}
	r.GrossReturn = dir * (r.ExitReferencePrice/r.EntryReferencePrice - 1)
	entryFactor := 1 + c.EntrySlippageBps/10000
	if s.Side == Short {
		entryFactor = 1 - c.EntrySlippageBps/10000
	}
	exitFactor := 1 - exitSlip/10000
	if s.Side == Short {
		exitFactor = 1 + exitSlip/10000
	}
	r.AdjustedEntryPrice = r.EntryReferencePrice * entryFactor
	r.AdjustedExitPrice = r.ExitReferencePrice * exitFactor
	r.AdjustedGrossReturn = dir * (r.AdjustedExitPrice/r.AdjustedEntryPrice - 1)
	r.SlippageEffect = r.AdjustedGrossReturn - r.GrossReturn
	r.FeeCostReturn = c.EntryFeeRate + (r.AdjustedExitPrice/r.AdjustedEntryPrice)*exitFee
	r.NetReturnExFunding = r.AdjustedGrossReturn - r.FeeCostReturn
	r.GrossProfitable = r.GrossReturn > 0
	r.NetProfitableExFunding = r.NetReturnExFunding > 0
	if bad(r.GrossReturn) || bad(r.NetReturnExFunding) {
		return r, fmt.Errorf("non-finite result")
	}
	return r, nil
}
func before(a, b mainbarrier.BarrierHit) bool {
	return a.TimestampMs < b.TimestampMs || a.TimestampMs == b.TimestampMs && a.AggTradeID < b.AggTradeID
}
func terminal(o mainoutcome.MainOutcomeV2, h int) float64 {
	var x float64
	switch h {
	case 60:
		x = o.MarketReturn60s
	case 180:
		x = o.MarketReturn180s
	case 300:
		x = o.MarketReturn300s
	case 900:
		x = o.MarketReturn900s
	case 1800:
		x = o.MarketReturn1800s
	case 3600:
		x = o.MarketReturn3600s
	case 14400:
		x = o.MarketReturn14400s
	default:
		return 0
	}
	return o.ReferenceEntryPrice * (1 + x)
}
func bad(x float64) bool { return math.IsNaN(x) || math.IsInf(x, 0) }
