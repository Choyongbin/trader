package tradespeclabel

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	mainsplit "binance_trader/internal/split/main"
	"binance_trader/internal/tradelabel"
)

// Phase9ACandidate preserves report-native identity. SourceCandidateID is the
// Phase 9A JSON row's "id" field; no candidate ID is derived by this parser.
type Phase9ACandidate struct {
	SourceCandidateID string
	Side              tradelabel.Side
	TPBps             int
	SLBps             int
	HorizonSeconds    int
}

// Phase9APartitionReference contains only Phase 9A metrics that are actually
// present. Nil denotes a missing JSON field, never a synthesized zero value.
type Phase9APartitionReference struct {
	ValidCount               *int64   // Phase 9A JSON: metrics.label_valid
	PositiveCount            *int64   // Phase 9A JSON: metrics.net_profitable_ex_funding
	WinRate                  *float64 // Phase 9A JSON: metrics.win_rate
	MeanNetReturnExFunding   *float64 // Phase 9A JSON: metrics.mean_net_return_ex_funding
	MedianNetReturnExFunding *float64 // Phase 9A JSON: metrics.median_net_return_ex_funding
}

// Phase9ARequiredMetrics is derived from the complete frozen reference set.
// A metric is required only when the authoritative report provides it for
// every frozen candidate and every supported partition.
type Phase9ARequiredMetrics struct {
	ValidCount               bool
	PositiveCount            bool
	WinRate                  bool
	MeanNetReturnExFunding   bool
	MedianNetReturnExFunding bool
}

// Phase9AReference is one frozen candidate and its authoritative Phase 9A
// TRAIN, VALIDATION, and TEST references.
type Phase9AReference struct {
	Candidate       Phase9ACandidate
	Partitions      map[mainsplit.Partition]Phase9APartitionReference
	RequiredMetrics Phase9ARequiredMetrics
}

// Phase9AReport is the reusable parsed provenance and frozen reference set.
type Phase9AReport struct {
	SourcePath   string
	SourceSHA256 string
	Frozen       []Phase9AReference
}

type phase9ASpec struct {
	TPBps          int `json:"tp_bps"`
	SLBps          int `json:"sl_bps"`
	HorizonSeconds int `json:"horizon_seconds"`
}

type phase9AMetrics struct {
	LabelValid               *int64   `json:"label_valid"`
	NetProfitableExFunding   *int64   `json:"net_profitable_ex_funding"`
	WinRate                  *float64 `json:"win_rate"`
	MeanNetReturnExFunding   *float64 `json:"mean_net_return_ex_funding"`
	MedianNetReturnExFunding *float64 `json:"median_net_return_ex_funding"`
}

type phase9ARow struct {
	ID      string         `json:"id"`
	Side    string         `json:"side"`
	Spec    *phase9ASpec   `json:"spec"`
	Metrics phase9AMetrics `json:"metrics"`
}

type phase9ARawReport struct {
	Train          []phase9ARow `json:"train"`
	Validation     []phase9ARow `json:"validation"`
	Test           []phase9ARow `json:"test"`
	LongShortlist  []string     `json:"long_shortlist_frozen"`
	ShortShortlist []string     `json:"short_shortlist_frozen"`
}

var expectedFrozenCandidates = []Phase9ACandidate{
	{Side: tradelabel.Long, TPBps: 100, SLBps: 100, HorizonSeconds: 14400},
	{Side: tradelabel.Long, TPBps: 75, SLBps: 50, HorizonSeconds: 14400},
	{Side: tradelabel.Long, TPBps: 50, SLBps: 50, HorizonSeconds: 14400},
	{Side: tradelabel.Short, TPBps: 75, SLBps: 25, HorizonSeconds: 900},
	{Side: tradelabel.Short, TPBps: 50, SLBps: 25, HorizonSeconds: 900},
}

func SHA256File(path string) (string, error) {
	bytes, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(bytes)
	return hex.EncodeToString(sum[:]), nil
}

func ValidateSHA256(expected, actual string) error {
	if len(expected) != 64 || len(actual) != 64 || expected != strings.ToLower(expected) || actual != strings.ToLower(actual) {
		return fmt.Errorf("SHA-256 must be lowercase hexadecimal with 64 characters")
	}
	if _, err := hex.DecodeString(expected); err != nil {
		return fmt.Errorf("invalid expected SHA-256: %w", err)
	}
	if _, err := hex.DecodeString(actual); err != nil {
		return fmt.Errorf("invalid actual SHA-256: %w", err)
	}
	if expected != actual {
		return fmt.Errorf("Phase 9A report SHA-256 mismatch: expected=%s actual=%s", expected, actual)
	}
	return nil
}

func LoadPhase9AReport(path string) (Phase9AReport, error) {
	bytes, err := os.ReadFile(path)
	if err != nil {
		return Phase9AReport{}, err
	}
	report, err := parsePhase9AReport(bytes, path)
	if err != nil {
		return Phase9AReport{}, err
	}
	report.SourceSHA256, err = SHA256File(path)
	if err != nil {
		return Phase9AReport{}, err
	}
	return report, nil
}

func parsePhase9AReport(bytes []byte, sourcePath string) (Phase9AReport, error) {
	var raw phase9ARawReport
	if err := json.Unmarshal(bytes, &raw); err != nil {
		return Phase9AReport{}, err
	}
	for name, rows := range map[mainsplit.Partition][]phase9ARow{
		mainsplit.Train: raw.Train, mainsplit.Validation: raw.Validation, mainsplit.Test: raw.Test,
	} {
		if err := validatePhase9ARows(name, rows); err != nil {
			return Phase9AReport{}, err
		}
	}

	frozen, err := frozenCandidates(raw.LongShortlist, raw.ShortShortlist, raw.Train)
	if err != nil {
		return Phase9AReport{}, err
	}
	if err := validateExpectedFrozenCandidates(frozen); err != nil {
		return Phase9AReport{}, err
	}

	references := make([]Phase9AReference, 0, len(frozen))
	for _, candidate := range frozen {
		partitions := map[mainsplit.Partition]Phase9APartitionReference{}
		for partition, rows := range map[mainsplit.Partition][]phase9ARow{
			mainsplit.Train: raw.Train, mainsplit.Validation: raw.Validation, mainsplit.Test: raw.Test,
		} {
			row, err := findPhase9ARow(rows, candidate)
			if err != nil {
				return Phase9AReport{}, fmt.Errorf("%s %s: %w", candidate.SourceCandidateID, partition, err)
			}
			partitions[partition] = Phase9APartitionReference{
				ValidCount:               row.Metrics.LabelValid,
				PositiveCount:            row.Metrics.NetProfitableExFunding,
				WinRate:                  row.Metrics.WinRate,
				MeanNetReturnExFunding:   row.Metrics.MeanNetReturnExFunding,
				MedianNetReturnExFunding: row.Metrics.MedianNetReturnExFunding,
			}
		}
		references = append(references, Phase9AReference{Candidate: candidate, Partitions: partitions})
	}
	requirements := phase9ARequiredMetrics(references)
	for index := range references {
		references[index].RequiredMetrics = requirements
	}

	sum := sha256.Sum256(bytes)
	return Phase9AReport{SourcePath: sourcePath, SourceSHA256: hex.EncodeToString(sum[:]), Frozen: references}, nil
}

func phase9ARequiredMetrics(references []Phase9AReference) Phase9ARequiredMetrics {
	requirements := Phase9ARequiredMetrics{ValidCount: true, PositiveCount: true, WinRate: true, MeanNetReturnExFunding: true, MedianNetReturnExFunding: true}
	for _, reference := range references {
		for _, partition := range []mainsplit.Partition{mainsplit.Train, mainsplit.Validation, mainsplit.Test} {
			metrics := reference.Partitions[partition]
			requirements.ValidCount = requirements.ValidCount && metrics.ValidCount != nil
			requirements.PositiveCount = requirements.PositiveCount && metrics.PositiveCount != nil
			requirements.WinRate = requirements.WinRate && metrics.WinRate != nil
			requirements.MeanNetReturnExFunding = requirements.MeanNetReturnExFunding && metrics.MeanNetReturnExFunding != nil
			requirements.MedianNetReturnExFunding = requirements.MedianNetReturnExFunding && metrics.MedianNetReturnExFunding != nil
		}
	}
	return requirements
}

func validatePhase9ARows(partition mainsplit.Partition, rows []phase9ARow) error {
	if len(rows) == 0 {
		return fmt.Errorf("Phase 9A %s rows are missing", partition)
	}
	seenIDs := map[string]bool{}
	seenSpecs := map[string]bool{}
	for _, row := range rows {
		candidate, err := candidateFromRow(row)
		if err != nil {
			return fmt.Errorf("Phase 9A %s row: %w", partition, err)
		}
		idKey := string(candidate.Side) + "/" + candidate.SourceCandidateID
		if seenIDs[idKey] {
			return fmt.Errorf("Phase 9A %s duplicate candidate ID %s", partition, idKey)
		}
		seenIDs[idKey] = true
		specKey := candidateSpecKey(candidate)
		if seenSpecs[specKey] {
			return fmt.Errorf("Phase 9A %s duplicate candidate spec %s", partition, specKey)
		}
		seenSpecs[specKey] = true
	}
	return nil
}

func frozenCandidates(longIDs, shortIDs []string, train []phase9ARow) ([]Phase9ACandidate, error) {
	if len(longIDs) != 3 || len(shortIDs) != 2 {
		return nil, fmt.Errorf("Phase 9A frozen candidate count must be LONG=3 SHORT=2, got LONG=%d SHORT=%d", len(longIDs), len(shortIDs))
	}
	byID := map[string]phase9ARow{}
	for _, row := range train {
		candidate, err := candidateFromRow(row)
		if err != nil {
			return nil, err
		}
		key := string(candidate.Side) + "/" + candidate.SourceCandidateID
		if _, exists := byID[key]; exists {
			return nil, fmt.Errorf("Phase 9A duplicate candidate %s", key)
		}
		byID[key] = row
	}

	result := make([]Phase9ACandidate, 0, 5)
	seen := map[string]bool{}
	for _, shortlist := range []struct {
		side tradelabel.Side
		ids  []string
	}{{tradelabel.Long, longIDs}, {tradelabel.Short, shortIDs}} {
		for _, id := range shortlist.ids {
			key := string(shortlist.side) + "/" + id
			if id == "" {
				return nil, fmt.Errorf("Phase 9A frozen candidate ID is empty")
			}
			if seen[key] {
				return nil, fmt.Errorf("Phase 9A frozen candidate is duplicated: %s", key)
			}
			seen[key] = true
			row, exists := byID[key]
			if !exists {
				return nil, fmt.Errorf("Phase 9A frozen candidate is absent from TRAIN: %s", key)
			}
			candidate, err := candidateFromRow(row)
			if err != nil {
				return nil, err
			}
			result = append(result, candidate)
		}
	}
	return result, nil
}

func candidateFromRow(row phase9ARow) (Phase9ACandidate, error) {
	if row.ID == "" {
		return Phase9ACandidate{}, fmt.Errorf("candidate id is missing")
	}
	if row.Side != string(tradelabel.Long) && row.Side != string(tradelabel.Short) {
		return Phase9ACandidate{}, fmt.Errorf("unknown candidate side %q", row.Side)
	}
	if row.Spec == nil {
		return Phase9ACandidate{}, fmt.Errorf("candidate spec is missing")
	}
	if row.Spec.TPBps <= 0 || row.Spec.SLBps <= 0 || row.Spec.HorizonSeconds <= 0 {
		return Phase9ACandidate{}, fmt.Errorf("candidate spec must have positive TP, SL, and horizon")
	}
	return Phase9ACandidate{SourceCandidateID: row.ID, Side: tradelabel.Side(row.Side), TPBps: row.Spec.TPBps, SLBps: row.Spec.SLBps, HorizonSeconds: row.Spec.HorizonSeconds}, nil
}

func findPhase9ARow(rows []phase9ARow, candidate Phase9ACandidate) (phase9ARow, error) {
	for _, row := range rows {
		if row.ID == candidate.SourceCandidateID && row.Side == string(candidate.Side) {
			actual, err := candidateFromRow(row)
			if err != nil {
				return phase9ARow{}, err
			}
			if actual.TPBps != candidate.TPBps || actual.SLBps != candidate.SLBps || actual.HorizonSeconds != candidate.HorizonSeconds {
				return phase9ARow{}, fmt.Errorf("candidate spec differs from TRAIN identity")
			}
			return row, nil
		}
	}
	return phase9ARow{}, fmt.Errorf("candidate reference is missing")
}

func validateExpectedFrozenCandidates(candidates []Phase9ACandidate) error {
	if len(candidates) != len(expectedFrozenCandidates) {
		return fmt.Errorf("unexpected frozen candidate total %d", len(candidates))
	}
	actual := make([]string, 0, len(candidates))
	expected := make([]string, 0, len(expectedFrozenCandidates))
	for _, candidate := range candidates {
		actual = append(actual, candidateSpecKey(candidate))
	}
	for _, candidate := range expectedFrozenCandidates {
		expected = append(expected, candidateSpecKey(candidate))
	}
	sort.Strings(actual)
	sort.Strings(expected)
	for i := range actual {
		if actual[i] != expected[i] {
			return fmt.Errorf("Phase 9A frozen candidate set mismatch: got=%v expected=%v", actual, expected)
		}
	}
	return nil
}

func candidateSpecKey(candidate Phase9ACandidate) string {
	return fmt.Sprintf("%s/%d/%d/%d", candidate.Side, candidate.TPBps, candidate.SLBps, candidate.HorizonSeconds)
}
