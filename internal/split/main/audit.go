package mainsplit

import (
	"fmt"
	"math"
	"reflect"
	"strings"

	maintraining "binance_trader/internal/training/main"
)

type SideStats struct {
	Valid    int64   `json:"valid_rows"`
	Positive int64   `json:"positive"`
	Negative int64   `json:"negative"`
	Rate     float64 `json:"positive_rate"`
}

type PartitionStats struct {
	RawRows       int64      `json:"raw_rows"`
	PurgedRows    int64      `json:"purged_rows"`
	EmbargoedRows int64      `json:"embargoed_rows"`
	IncludedRows  int64      `json:"included_rows"`
	FirstIncluded int64      `json:"first_included_decision_ms"`
	LastIncluded  int64      `json:"last_included_decision_ms"`
	FirstPurged   int64      `json:"first_purged_decision_ms,omitempty"`
	Long          *SideStats `json:"long,omitempty"`
	Short         *SideStats `json:"short,omitempty"`
}

type AuditStats struct {
	TotalRows              int64
	OutsideRows            int64
	Duplicates             int64
	OrderViolations        int64
	FeatureNaN             int64
	FeatureInf             int64
	FeatureCountMismatches int64
	NumericNaN             int64
	NumericInf             int64
	TimestampGaps          int64
	RegistryLeakage        int64
	UnexplainedRows        int64
	Partitions             map[Partition]*PartitionStats
}

func Audit(loader Loader) (AuditStats, error) {
	s := AuditStats{Partitions: map[Partition]*PartitionStats{}}
	for _, p := range []Partition{Train, Validation, Test, FinalHoldout} {
		ps := &PartitionStats{}
		if p != FinalHoldout {
			ps.Long, ps.Short = &SideStats{}, &SideStats{}
		}
		s.Partitions[p] = ps
	}
	var previous int64
	_, err := loader.VisitCanonical(func(row maintraining.TrainingRowV1, a Assignment) error {
		s.TotalRows++
		if previous != 0 {
			if row.DecisionTimestampMs == previous {
				s.Duplicates++
			}
			if row.DecisionTimestampMs < previous {
				s.OrderViolations++
			}
			if row.DecisionTimestampMs != previous+5000 {
				s.TimestampGaps++
			}
		}
		previous = row.DecisionTimestampMs
		features := ModelFeatures(row)
		if len(features) != len(maintraining.ModelFeatureColumns) {
			s.FeatureCountMismatches++
		}
		for _, x := range features {
			if math.IsNaN(x) {
				s.FeatureNaN++
			}
			if math.IsInf(x, 0) {
				s.FeatureInf++
			}
		}
		nan, inf := numericStructuralErrors(reflect.ValueOf(row))
		s.NumericNaN += nan
		s.NumericInf += inf
		if a.Partition == Outside {
			s.OutsideRows++
			return nil
		}
		ps := s.Partitions[a.Partition]
		ps.RawRows++
		switch a.Reason {
		case Included:
			ps.IncludedRows++
			if ps.FirstIncluded == 0 {
				ps.FirstIncluded = row.DecisionTimestampMs
			}
			ps.LastIncluded = row.DecisionTimestampMs
			// Deliberately never inspect holdout targets or label distributions.
			if a.Partition != FinalHoldout {
				addSide(ps.Long, row.LongLabelValid, row.LongNetProfitableExFunding)
				addSide(ps.Short, row.ShortLabelValid, row.ShortNetProfitableExFunding)
			}
		case PurgedLabelOverlap:
			ps.PurgedRows++
			if ps.FirstPurged == 0 {
				ps.FirstPurged = row.DecisionTimestampMs
			}
		case Embargoed:
			ps.EmbargoedRows++
		default:
			s.UnexplainedRows++
		}
		return nil
	})
	if err != nil {
		return s, err
	}
	for _, ps := range s.Partitions {
		if ps.Long != nil {
			finishSide(ps.Long)
			finishSide(ps.Short)
		}
		if ps.RawRows != ps.IncludedRows+ps.PurgedRows+ps.EmbargoedRows {
			s.UnexplainedRows++
		}
	}
	for _, name := range maintraining.ModelFeatureColumns {
		if name == "decision_timestamp_ms" || name == "reference_close" || strings.Contains(name, "future_") || strings.Contains(name, "market_return") || strings.Contains(name, "net_return") || strings.Contains(name, "fee_cost") || strings.Contains(name, "slippage_cost") || strings.Contains(name, "barrier") || strings.Contains(name, "profitable") || strings.Contains(name, "label") || strings.Contains(name, "trade_result") || strings.Contains(name, "entry_") {
			s.RegistryLeakage++
		}
	}
	if s.Duplicates+s.OrderViolations+s.FeatureNaN+s.FeatureInf+s.FeatureCountMismatches+s.NumericNaN+s.NumericInf+s.TimestampGaps+s.RegistryLeakage+s.UnexplainedRows != 0 {
		return s, fmt.Errorf("split structural audit failed")
	}
	return s, nil
}

func addSide(s *SideStats, valid, positive bool) {
	if !valid {
		return
	}
	s.Valid++
	if positive {
		s.Positive++
	} else {
		s.Negative++
	}
}

func finishSide(s *SideStats) {
	if s.Valid > 0 {
		s.Rate = float64(s.Positive) / float64(s.Valid)
	}
}

func numericStructuralErrors(v reflect.Value) (int64, int64) {
	var nan, inf int64
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		if field.Kind() == reflect.Struct {
			nestedNaN, nestedInf := numericStructuralErrors(field)
			nan += nestedNaN
			inf += nestedInf
		} else if field.Kind() == reflect.Float64 {
			if math.IsNaN(field.Float()) {
				nan++
			}
			if math.IsInf(field.Float(), 0) {
				inf++
			}
		}
	}
	return nan, inf
}
