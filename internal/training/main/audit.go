package maintraining

import (
	"binance_trader/internal/tradelabel"
	"fmt"
	"math"
	"reflect"
	"strings"
)

type AuditStats struct {
	Rows, Duplicates, OrderViolations, FeatureNaN, FeatureInf, TargetNaN, TargetInf, InvalidLabels int64
	StartDecisionTimestampMs, EndDecisionTimestampMs                                               int64
	Long, Short                                                                                    SideStats
	BothProfitable, LongOnlyProfitable, ShortOnlyProfitable, NeitherProfitable                     int64
}

func Audit(path string) (AuditStats, error) {
	var s AuditStats
	seen := map[int64]bool{}
	var prev int64
	_, e := Read(path, func(r TrainingRowV1) error {
		s.Rows++
		k := r.DecisionTimestampMs
		if seen[k] {
			s.Duplicates++
		}
		seen[k] = true
		if prev != 0 && k <= prev {
			s.OrderViolations++
		}
		prev = k
		if s.StartDecisionTimestampMs == 0 {
			s.StartDecisionTimestampMs = k
		}
		s.EndDecisionTimestampMs = k
		v := reflect.ValueOf(r.MainFeaturesV1)
		for i := 0; i < v.NumField(); i++ {
			if v.Field(i).Kind() == reflect.Float64 {
				x := v.Field(i).Float()
				if math.IsNaN(x) {
					s.FeatureNaN++
				}
				if math.IsInf(x, 0) {
					s.FeatureInf++
				}
			}
		}
		auditSide := func(valid bool, status tradelabel.Status, gross, fee, slip, net float64, gp, np bool, st *SideStats) {
			for _, x := range []float64{gross, fee, slip, net} {
				if math.IsNaN(x) {
					s.TargetNaN++
				}
				if math.IsInf(x, 0) {
					s.TargetInf++
				}
			}
			if valid {
				if status != tradelabel.TPFirst && status != tradelabel.SLFirst && status != tradelabel.Timeout {
					s.InvalidLabels++
				}
				x := tradelabel.TradeResultV2{LabelValid: true, Status: status, GrossMarketReturn: gross, FeeCostReturn: fee, ModeledSlippageCostReturn: slip, NetReturnExFunding: net, GrossProfitable: gp, NetProfitableExFunding: np}
				accumulate(st, x)
			} else {
				st.Invalid++
				if status != tradelabel.EntryReferenceUnavailable && status != tradelabel.ExitReferenceUnavailable {
					s.InvalidLabels++
				}
				if gp || np {
					s.InvalidLabels++
				}
			}
		}
		auditSide(r.LongLabelValid, r.LongTradeResult, r.LongGrossMarketReturn, r.LongFeeCostReturn, r.LongModeledSlippageCostReturn, r.LongNetReturnExFunding, r.LongGrossProfitable, r.LongNetProfitableExFunding, &s.Long)
		auditSide(r.ShortLabelValid, r.ShortTradeResult, r.ShortGrossMarketReturn, r.ShortFeeCostReturn, r.ShortModeledSlippageCostReturn, r.ShortNetReturnExFunding, r.ShortGrossProfitable, r.ShortNetProfitableExFunding, &s.Short)
		if r.LongLabelValid && r.ShortLabelValid {
			switch {
			case r.LongNetProfitableExFunding && r.ShortNetProfitableExFunding:
				s.BothProfitable++
			case r.LongNetProfitableExFunding:
				s.LongOnlyProfitable++
			case r.ShortNetProfitableExFunding:
				s.ShortOnlyProfitable++
			default:
				s.NeitherProfitable++
			}
		}
		return nil
	})
	if e != nil {
		return s, e
	}
	if s.Duplicates+s.OrderViolations+s.FeatureNaN+s.FeatureInf+s.TargetNaN+s.TargetInf+s.InvalidLabels > 0 {
		return s, fmt.Errorf("training audit failed")
	}
	for _, c := range ModelFeatureColumns {
		if strings.Contains(c, "future_") || strings.Contains(c, "market_return") || strings.Contains(c, "barrier") || strings.Contains(c, "profitable") || strings.Contains(c, "net_return") {
			return s, fmt.Errorf("leakage registry column %q", c)
		}
	}
	return s, nil
}
