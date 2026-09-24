package maintraining

import (
	"errors"
	"fmt"
	"io"
	"os"

	mainbarrier "binance_trader/internal/barrier/main"
	mainfeature "binance_trader/internal/feature/main"
	mainoutcome "binance_trader/internal/outcome/main"
	"binance_trader/internal/tradelabel"
	"github.com/parquet-go/parquet-go"
)

type BuildConfig struct {
	FeaturePath, OutcomePath, BarrierPath string
	TradeSpec                             tradelabel.TradeSpec
	CostProfile                           tradelabel.CostProfile
}
type BuildStats struct {
	FeatureRows, OutcomeRowsRead, BarrierRowsRead, JoinedRows                  int64
	StartDecisionTimestampMs, EndDecisionTimestampMs                           int64
	Long, Short                                                                SideStats
	BothProfitable, LongOnlyProfitable, ShortOnlyProfitable, NeitherProfitable int64
}
type SideStats struct {
	Valid, Invalid, TPFirst, SLFirst, Timeout, GrossProfitable, NetProfitable, GrossWinNetLoss int64
	GrossSum, FeeSum, SlippageSum, NetSum                                                      float64
}

type source[T any] struct {
	f       *os.File
	r       *parquet.GenericReader[T]
	buf     [1]T
	current T
	has     bool
	rows    int64
	key     func(T) int64
	last    int64
}

func openSource[T any](path string, key func(T) int64) (*source[T], error) {
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	return &source[T]{f: f, r: parquet.NewGenericReader[T](f), key: key}, nil
}
func (s *source[T]) close() { _ = s.r.Close(); _ = s.f.Close() }
func (s *source[T]) next() error {
	n, e := s.r.Read(s.buf[:])
	if n == 1 {
		k := s.key(s.buf[0])
		if s.rows > 0 && k <= s.last {
			return fmt.Errorf("duplicate or unordered key %d after %d", k, s.last)
		}
		s.last = k
		s.current = s.buf[0]
		s.has = true
		s.rows++
		return nil
	}
	s.has = false
	if errors.Is(e, io.EOF) {
		return nil
	}
	return e
}

func Build(c BuildConfig, emit func(TrainingRowV1) error) (BuildStats, error) {
	var st BuildStats
	if e := tradelabel.ValidateSpec(c.TradeSpec); e != nil {
		return st, e
	}
	f, e := openSource(c.FeaturePath, func(x mainfeature.MainFeaturesV1) int64 { return x.DecisionTimestampMs })
	if e != nil {
		return st, e
	}
	defer f.close()
	o, e := openSource(c.OutcomePath, func(x mainoutcome.MainOutcomeV2) int64 { return x.DecisionTimestampMs })
	if e != nil {
		return st, e
	}
	defer o.close()
	b, e := openSource(c.BarrierPath, func(x mainbarrier.BarrierOutcomeV2) int64 { return x.DecisionTimestampMs })
	if e != nil {
		return st, e
	}
	defer b.close()
	if e = f.next(); e != nil {
		return st, e
	}
	if e = o.next(); e != nil {
		return st, e
	}
	if e = b.next(); e != nil {
		return st, e
	}
	for f.has {
		k := f.current.DecisionTimestampMs
		for o.has && o.current.DecisionTimestampMs < k {
			if e = o.next(); e != nil {
				return st, e
			}
		}
		for b.has && b.current.DecisionTimestampMs < k {
			if e = b.next(); e != nil {
				return st, e
			}
		}
		if !o.has || o.current.DecisionTimestampMs != k {
			return st, fmt.Errorf("missing Outcome V2 for feature key %d", k)
		}
		if !b.has || b.current.DecisionTimestampMs != k {
			return st, fmt.Errorf("missing Barrier V2 for feature key %d", k)
		}
		long, e := tradelabel.EvaluateV2(b.current, tradelabel.TradeSpec{Side: tradelabel.Long, TPBps: c.TradeSpec.TPBps, SLBps: c.TradeSpec.SLBps, HorizonSeconds: c.TradeSpec.HorizonSeconds}, c.CostProfile)
		if e != nil {
			return st, e
		}
		short, e := tradelabel.EvaluateV2(b.current, tradelabel.TradeSpec{Side: tradelabel.Short, TPBps: c.TradeSpec.TPBps, SLBps: c.TradeSpec.SLBps, HorizonSeconds: c.TradeSpec.HorizonSeconds}, c.CostProfile)
		if e != nil {
			return st, e
		}
		r := TrainingRowV1{MainFeaturesV1: f.current, EntryReferenceAvailable: b.current.EntryReferenceAvailable, EntryWaitMs: b.current.EntryWaitMs}
		apply(&r, long, true)
		apply(&r, short, false)
		if e = emit(r); e != nil {
			return st, e
		}
		st.FeatureRows++
		st.JoinedRows++
		if st.StartDecisionTimestampMs == 0 {
			st.StartDecisionTimestampMs = k
		}
		st.EndDecisionTimestampMs = k
		accumulate(&st.Long, long)
		accumulate(&st.Short, short)
		if long.LabelValid && short.LabelValid {
			switch {
			case long.NetProfitableExFunding && short.NetProfitableExFunding:
				st.BothProfitable++
			case long.NetProfitableExFunding:
				st.LongOnlyProfitable++
			case short.NetProfitableExFunding:
				st.ShortOnlyProfitable++
			default:
				st.NeitherProfitable++
			}
		}
		if e = f.next(); e != nil {
			return st, e
		}
		if e = o.next(); e != nil {
			return st, e
		}
		if e = b.next(); e != nil {
			return st, e
		}
	}
	st.OutcomeRowsRead = o.rows
	st.BarrierRowsRead = b.rows
	return st, nil
}
func apply(r *TrainingRowV1, x tradelabel.TradeResultV2, long bool) {
	if long {
		r.LongLabelValid = x.LabelValid
		r.LongTradeResult = x.Status
		r.LongGrossMarketReturn = x.GrossMarketReturn
		r.LongFeeCostReturn = x.FeeCostReturn
		r.LongModeledSlippageCostReturn = x.ModeledSlippageCostReturn
		r.LongNetReturnExFunding = x.NetReturnExFunding
		r.LongGrossProfitable = x.GrossProfitable
		r.LongNetProfitableExFunding = x.NetProfitableExFunding
	} else {
		r.ShortLabelValid = x.LabelValid
		r.ShortTradeResult = x.Status
		r.ShortGrossMarketReturn = x.GrossMarketReturn
		r.ShortFeeCostReturn = x.FeeCostReturn
		r.ShortModeledSlippageCostReturn = x.ModeledSlippageCostReturn
		r.ShortNetReturnExFunding = x.NetReturnExFunding
		r.ShortGrossProfitable = x.GrossProfitable
		r.ShortNetProfitableExFunding = x.NetProfitableExFunding
	}
}
func accumulate(s *SideStats, x tradelabel.TradeResultV2) {
	if !x.LabelValid {
		s.Invalid++
		return
	}
	s.Valid++
	switch x.Status {
	case tradelabel.TPFirst:
		s.TPFirst++
	case tradelabel.SLFirst:
		s.SLFirst++
	case tradelabel.Timeout:
		s.Timeout++
	}
	if x.GrossProfitable {
		s.GrossProfitable++
	}
	if x.NetProfitableExFunding {
		s.NetProfitable++
	}
	if x.GrossProfitable && !x.NetProfitableExFunding {
		s.GrossWinNetLoss++
	}
	s.GrossSum += x.GrossMarketReturn
	s.FeeSum += x.FeeCostReturn
	s.SlippageSum += x.ModeledSlippageCostReturn
	s.NetSum += x.NetReturnExFunding
}
