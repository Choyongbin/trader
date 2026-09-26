package metricsv2

import (
	"errors"
	"fmt"
)

const (
	SemanticDocumentedEnd   = "DOCUMENTED_END"
	SemanticDocumentedStart = "DOCUMENTED_START"
	EvidenceCurrentDocs     = "CURRENT_DOCUMENTATION_ONLY"
	EvidenceCurrentAnd2024  = "CURRENT_DOCUMENTATION_AND_2024_EVIDENCE"
)

// TimedObservation is the live counterpart of Observation. ReceiveTimestampMs
// is an observed clock value, never synthesized for historical rows.
type TimedObservation struct {
	SourceTimestampMs    int64  `json:"source_timestamp_ms"`
	Field                string `json:"field"`
	Value                string `json:"value,omitempty"`
	IntervalStartMs      int64  `json:"interval_start_ms"`
	IntervalEndMs        int64  `json:"interval_end_ms"`
	ReceiveTimestampMs   int64  `json:"receive_timestamp_ms"`
	MinimumAvailableMs   int64  `json:"minimum_available_at_ms"`
	TimestampSemantic    string `json:"timestamp_semantic"`
	VerificationEvidence string `json:"verification_evidence"`
}

// CurrentDocumentedSemantic returns the semantic in the current Binance API
// documentation. It deliberately says nothing about historical archives.
func CurrentDocumentedSemantic(field string) (string, error) {
	switch field {
	case OpenInterest, OpenInterestValue, GlobalRatio, TopTraderAccountRatio, TopTraderPositionRatio:
		return SemanticDocumentedEnd, nil
	case TakerRatio:
		return SemanticDocumentedStart, nil
	default:
		return "", fmt.Errorf("unknown metrics field %q", field)
	}
}

// NormalizeLive uses the current documented interval convention and gates a
// value on both interval completion plus safety lag and actual receipt.
func NormalizeLive(field string, sourceTimestampMs, receiveTimestampMs int64) (TimedObservation, error) {
	if sourceTimestampMs <= 0 || sourceTimestampMs%IntervalMs != 0 {
		return TimedObservation{}, errors.New("invalid source timestamp")
	}
	if receiveTimestampMs <= 0 {
		return TimedObservation{}, errors.New("invalid receive timestamp")
	}
	semantic, err := CurrentDocumentedSemantic(field)
	if err != nil {
		return TimedObservation{}, err
	}
	o := TimedObservation{
		SourceTimestampMs:    sourceTimestampMs,
		Field:                field,
		ReceiveTimestampMs:   receiveTimestampMs,
		TimestampSemantic:    semantic,
		VerificationEvidence: EvidenceCurrentDocs,
	}
	if semantic == SemanticDocumentedEnd {
		o.IntervalStartMs = sourceTimestampMs - IntervalMs
		o.IntervalEndMs = sourceTimestampMs
	} else {
		o.IntervalStartMs = sourceTimestampMs
		o.IntervalEndMs = sourceTimestampMs + IntervalMs
		o.VerificationEvidence = EvidenceCurrentAnd2024
	}
	o.MinimumAvailableMs = o.IntervalEndMs + SafetyLagMs
	if receiveTimestampMs > o.MinimumAvailableMs {
		o.MinimumAvailableMs = receiveTimestampMs
	}
	return o, nil
}

type IntervalKey struct {
	StartMs int64
	EndMs   int64
}

// CompleteIntervals only joins fields that identify exactly the same
// normalized interval. Equal raw timestamps are intentionally irrelevant.
func CompleteIntervals(rows []TimedObservation) map[IntervalKey]map[string]TimedObservation {
	grouped := make(map[IntervalKey]map[string]TimedObservation)
	for _, row := range rows {
		key := IntervalKey{StartMs: row.IntervalStartMs, EndMs: row.IntervalEndMs}
		if grouped[key] == nil {
			grouped[key] = make(map[string]TimedObservation)
		}
		grouped[key][row.Field] = row
	}
	for key, fields := range grouped {
		for _, field := range FieldOrder {
			if _, ok := fields[field]; !ok {
				delete(grouped, key)
				break
			}
		}
	}
	return grouped
}
