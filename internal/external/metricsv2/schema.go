// Package metricsv2 defines the corrected, field-level historical Metrics
// representation. It is independent of the frozen canonical V1 dataset.
package metricsv2

import (
	"errors"
	"fmt"
	"sort"
)

const (
	Version                     = 2
	IntervalMs            int64 = 300_000
	SafetyLagMs           int64 = 5_000
	FreshnessLimitMs      int64 = 600_000
	TakerChangeAtMs             = int64(1709510400000) // 2024-03-04T00:00:00Z
	SemanticVerifiedEnd         = "VERIFIED_END"
	SemanticVerifiedStart       = "VERIFIED_START"
	SemanticUnverified          = "UNVERIFIED"
)

const (
	OpenInterest           = "open_interest"
	OpenInterestValue      = "open_interest_value"
	TopTraderAccountRatio  = "top_trader_account_long_short_ratio"
	TopTraderPositionRatio = "top_trader_position_long_short_ratio"
	GlobalRatio            = "global_long_short_ratio"
	TakerRatio             = "taker_long_short_volume_ratio"
)

var FieldOrder = []string{OpenInterest, OpenInterestValue, TopTraderAccountRatio, TopTraderPositionRatio, GlobalRatio, TakerRatio}

type SourceRow struct {
	TimestampMs            int64
	OpenInterest           string
	OpenInterestValue      string
	TopTraderAccountRatio  string
	TopTraderPositionRatio string
	GlobalRatio            string
	TakerRatio             string
}

// Observation deliberately stores one field per row. Unknown interval
// semantics have zero interval/availability fields and cannot be selected by
// the corrected as-of functions.
type Observation struct {
	SourceTimestampMs     int64  `parquet:"source_timestamp_ms" json:"source_timestamp_ms"`
	Field                 string `parquet:"field" json:"field"`
	Value                 string `parquet:"value" json:"value"`
	IntervalStartMs       int64  `parquet:"interval_start_ms" json:"interval_start_ms"`
	IntervalEndMs         int64  `parquet:"interval_end_ms" json:"interval_end_ms"`
	EarliestAvailableMs   int64  `parquet:"earliest_available_at_ms" json:"earliest_available_at_ms"`
	TimestampSemantic     string `parquet:"timestamp_semantic" json:"timestamp_semantic"`
	SemanticVerified      bool   `parquet:"semantic_verified" json:"semantic_verified"`
	HistoricalMinimumOnly bool   `parquet:"historical_minimum_only" json:"historical_minimum_only"`
}

func Expand(row SourceRow) ([]Observation, error) {
	if row.TimestampMs <= 0 || row.TimestampMs%IntervalMs != 0 {
		return nil, fmt.Errorf("invalid source timestamp %d", row.TimestampMs)
	}
	values := map[string]string{
		OpenInterest: row.OpenInterest, OpenInterestValue: row.OpenInterestValue,
		TopTraderAccountRatio: row.TopTraderAccountRatio, TopTraderPositionRatio: row.TopTraderPositionRatio,
		GlobalRatio: row.GlobalRatio, TakerRatio: row.TakerRatio,
	}
	out := make([]Observation, 0, len(FieldOrder))
	for _, field := range FieldOrder {
		o := Observation{SourceTimestampMs: row.TimestampMs, Field: field, Value: values[field], TimestampSemantic: SemanticUnverified}
		if field == TakerRatio {
			o.SemanticVerified = true
			o.HistoricalMinimumOnly = true
			if row.TimestampMs < TakerChangeAtMs {
				o.TimestampSemantic = SemanticVerifiedEnd
				o.IntervalStartMs = row.TimestampMs - IntervalMs
				o.IntervalEndMs = row.TimestampMs
			} else {
				o.TimestampSemantic = SemanticVerifiedStart
				o.IntervalStartMs = row.TimestampMs
				o.IntervalEndMs = row.TimestampMs + IntervalMs
			}
			o.EarliestAvailableMs = o.IntervalEndMs + SafetyLagMs
		}
		out = append(out, o)
	}
	return out, nil
}

func HistoricalUsableAt(o Observation, extraDelayMs int64) (int64, error) {
	if extraDelayMs < 0 {
		return 0, errors.New("negative historical extra delay")
	}
	if !o.SemanticVerified || o.EarliestAvailableMs <= 0 {
		return 0, fmt.Errorf("field %s has unverified interval semantics", o.Field)
	}
	return o.EarliestAvailableMs + extraDelayMs, nil
}

func LiveUsableAt(o Observation, receiveTimestampMs, extraDelayMs int64) (int64, error) {
	effective, err := HistoricalUsableAt(o, extraDelayMs)
	if err != nil {
		return 0, err
	}
	if receiveTimestampMs <= 0 {
		return 0, errors.New("invalid receive timestamp")
	}
	if receiveTimestampMs > effective {
		effective = receiveTimestampMs
	}
	return effective, nil
}

type Selected struct {
	Observation Observation
	EffectiveMs int64
	AgeMs       int64
	Fresh       bool
}

func SelectHistorical(rows []Observation, decisionTimestampMs, extraDelayMs int64) (*Selected, error) {
	if decisionTimestampMs < 0 {
		return nil, errors.New("negative decision timestamp")
	}
	i := sort.Search(len(rows), func(i int) bool {
		e, err := HistoricalUsableAt(rows[i], extraDelayMs)
		return err != nil || e > decisionTimestampMs
	})
	if i == 0 {
		return nil, nil
	}
	o := rows[i-1]
	effective, err := HistoricalUsableAt(o, extraDelayMs)
	if err != nil {
		return nil, err
	}
	age := decisionTimestampMs - o.IntervalEndMs
	if age < 0 || effective > decisionTimestampMs {
		return nil, errors.New("future observation selected")
	}
	return &Selected{Observation: o, EffectiveMs: effective, AgeMs: age, Fresh: age <= FreshnessLimitMs}, nil
}
