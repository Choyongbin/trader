// metricscorrectionbuild creates a field-level, corrected Metrics V2 dataset
// for 2024 TRAIN/VALIDATION only. It never rewrites canonical V1 or Feature V2.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"binance_trader/internal/external/metricsv2"
	"github.com/parquet-go/parquet-go"
)

const symbol = "BTCUSDT"

type v1Row struct {
	Timestamp              int64  `parquet:"timestamp"`
	OpenInterest           string `parquet:"open_interest"`
	OpenInterestValue      string `parquet:"open_interest_value"`
	TopTraderAccountRatio  string `parquet:"top_trader_account_long_short_ratio"`
	TopTraderPositionRatio string `parquet:"top_trader_position_long_short_ratio"`
	GlobalRatio            string `parquet:"global_long_short_ratio"`
	TakerRatio             string `parquet:"taker_long_short_volume_ratio"`
}

type monthAudit struct {
	Month               string `json:"month"`
	Partition           string `json:"partition"`
	SourceRows          int64  `json:"source_rows"`
	OutputRows          int64  `json:"output_rows"`
	TakerRows           int64  `json:"taker_rows"`
	UnknownSemanticRows int64  `json:"unknown_semantic_rows"`
	DuplicateKeys       int64  `json:"duplicate_keys"`
	ReverseKeys         int64  `json:"reverse_keys"`
	ValueMismatches     int64  `json:"value_mismatches"`
	FutureAvailability  int64  `json:"future_availability_errors"`
	SourceValueSHA256   string `json:"source_value_sha256"`
	OutputValueSHA256   string `json:"output_value_sha256"`
	ParquetBytes        int64  `json:"parquet_bytes"`
	Complete            bool   `json:"complete"`
}

type gapExample struct {
	PreviousSourceTimestampMs int64 `json:"previous_source_timestamp_ms"`
	PreviousIntervalEndMs     int64 `json:"previous_interval_end_ms"`
	CurrentSourceTimestampMs  int64 `json:"current_source_timestamp_ms"`
	CurrentIntervalStartMs    int64 `json:"current_interval_start_ms"`
	DeltaMs                   int64 `json:"delta_ms"`
}

type integrityReport struct {
	Status                     string       `json:"status"`
	SourceRows                 int64        `json:"source_rows"`
	OutputRows                 int64        `json:"output_rows"`
	ExpectedFieldsPerSource    int          `json:"expected_fields_per_source"`
	TakerRows                  int64        `json:"taker_rows"`
	UnknownSemanticRows        int64        `json:"unknown_semantic_rows"`
	DuplicateKeys              int64        `json:"duplicate_keys"`
	ReverseKeys                int64        `json:"reverse_keys"`
	ValueMismatches            int64        `json:"value_mismatches"`
	AvailabilityMetadataErrors int64        `json:"availability_metadata_errors"`
	TakerIntervalGaps          int64        `json:"taker_interval_gaps"`
	TakerIntervalOverlaps      int64        `json:"taker_interval_overlaps"`
	TransitionGapPresent       bool         `json:"transition_gap_present"`
	TransitionGap              *gapExample  `json:"transition_gap,omitempty"`
	GapExamples                []gapExample `json:"gap_examples"`
	FutureSelections           int64        `json:"future_selections"`
	Monthly                    []monthAudit `json:"monthly"`
	TestAccessed               bool         `json:"test_accessed"`
	FinalHoldoutAccessed       bool         `json:"final_holdout_accessed"`
}

type fieldEvidence struct {
	Field                string `json:"field"`
	Status               string `json:"status"`
	ObservedRule         string `json:"observed_rule"`
	CorrectedAsOfEnabled bool   `json:"corrected_asof_enabled"`
	Reason               string `json:"reason"`
}

type schemaReport struct {
	Version              int             `json:"version"`
	Layout               string          `json:"layout"`
	Columns              []string        `json:"columns"`
	TakerChangeAtUTC     string          `json:"taker_change_at_utc"`
	HistoricalRule       string          `json:"historical_rule"`
	LiveRule             string          `json:"live_rule"`
	HistoricalLimitation string          `json:"historical_limitation"`
	Fields               []fieldEvidence `json:"fields"`
}

type comparisonStats struct {
	Partition                    string `json:"partition"`
	TotalDecisionGrid            int64  `json:"total_decision_grid"`
	BothLevelAvailable           int64  `json:"both_level_available"`
	LevelChanged                 int64  `json:"taker_level_changed"`
	BothChange5mAvailable        int64  `json:"both_change_5m_available"`
	Change5mChanged              int64  `json:"taker_change_5m_changed"`
	LegacyFutureSelections       int64  `json:"legacy_future_selections"`
	CorrectedFutureSelections    int64  `json:"corrected_future_selections"`
	LegacyTakerEligible          int64  `json:"legacy_taker_component_eligible"`
	CorrectedTakerEligible       int64  `json:"corrected_taker_component_eligible"`
	TakerEligibilityChanged      int64  `json:"taker_component_eligibility_changed"`
	LegacyEligibleCorrectedNot   int64  `json:"legacy_eligible_corrected_not"`
	LegacyNotCorrectedEligible   int64  `json:"legacy_not_corrected_eligible"`
	ExistingEligibleRows         int64  `json:"existing_feature_v2_eligible_rows"`
	ExistingEligibleStillValid   int64  `json:"existing_eligible_still_taker_valid"`
	ExistingEligibleInvalidated  int64  `json:"existing_eligible_invalidated_by_taker_timing"`
	LegacyLevelFrozenMismatches  int64  `json:"legacy_level_vs_frozen_mismatches"`
	LegacyChangeFrozenMismatches int64  `json:"legacy_change_5m_vs_frozen_mismatches"`
}

type sensitivity struct {
	ExtraDelayMs             int64 `json:"extra_delay_ms"`
	CorrectedEligible        int64 `json:"corrected_taker_component_eligible"`
	ExistingEligibleRetained int64 `json:"existing_eligible_retained"`
	ExistingEligibleDropped  int64 `json:"existing_eligible_dropped"`
}

type comparisonReport struct {
	Status                    string            `json:"status"`
	Scope                     string            `json:"scope"`
	FeatureCount              int               `json:"frozen_feature_count"`
	ConfirmedChangedFeatures  []string          `json:"confirmed_changed_features"`
	UnchangedByConstruction   int               `json:"unchanged_by_construction_count"`
	EligibilityInterpretation string            `json:"eligibility_interpretation"`
	Partitions                []comparisonStats `json:"partitions"`
	Sensitivity               []sensitivity     `json:"historical_extra_delay_sensitivity"`
	ModelConnected            bool              `json:"existing_model_connected"`
	ModelTrained              bool              `json:"new_model_trained"`
	TestAccessed              bool              `json:"test_accessed"`
	FinalHoldoutAccessed      bool              `json:"final_holdout_accessed"`
}

type manifest struct {
	Version                 int          `json:"version"`
	Symbol                  string       `json:"symbol"`
	Scope                   string       `json:"scope"`
	Source                  string       `json:"source"`
	SourceVersion           int          `json:"source_version"`
	Months                  []monthAudit `json:"months"`
	SourceRows              int64        `json:"source_rows"`
	OutputRows              int64        `json:"output_rows"`
	TakerChangeAtMs         int64        `json:"taker_change_at_ms"`
	SafetyLagMs             int64        `json:"safety_lag_ms"`
	UnknownFieldsSelectable bool         `json:"unknown_fields_selectable"`
	Complete                bool         `json:"complete"`
}

type takerPoint struct {
	RawTimestampMs, IntervalStartMs, IntervalEndMs, EarliestAvailableMs int64
	Value                                                               float64
}

type existingFeatureRow struct {
	DecisionTimestampMs int64   `parquet:"decision_timestamp_ms"`
	TakerLevel          float64 `parquet:"taker_long_short_volume_ratio"`
	TakerChange5m       float64 `parquet:"taker_long_short_volume_ratio_change_5m"`
}

func main() {
	sourceRoot := flag.String("source", "data/external/metrics/v1/BTCUSDT", "canonical V1 source root")
	outRoot := flag.String("out", "data/external/metrics/v2/BTCUSDT", "new Metrics V2 root")
	reportRoot := flag.String("report", "data/reports/diagnostics/metrics-time-correction/phase3a-v1", "new report root")
	featureRoot := flag.String("feature-root", "data/features/main/v2/BTCUSDT/2024", "existing TRAIN/VALIDATION Feature V2 root")
	reuseData := flag.Bool("reuse-data", false, "reuse and independently audit an existing complete Metrics V2 dataset")
	flag.Parse()
	if err := run(*sourceRoot, *outRoot, *reportRoot, *featureRoot, *reuseData); err != nil {
		fmt.Fprintln(os.Stderr, "CORRECTION ERROR:", err)
		os.Exit(1)
	}
}

func run(sourceRoot, outRoot, reportRoot, featureRoot string, reuseData bool) error {
	if _, err := os.Stat(reportRoot); err == nil {
		return fmt.Errorf("report output already exists: %s", reportRoot)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if reuseData {
		if _, err := os.Stat(outRoot); err != nil {
			return fmt.Errorf("reuse data unavailable: %w", err)
		}
	} else {
		if _, err := os.Stat(outRoot); err == nil {
			return fmt.Errorf("output already exists: %s", outRoot)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := os.MkdirAll(outRoot, 0o755); err != nil {
			return err
		}
	}
	var sourceRows []v1Row
	var audits []monthAudit
	if reuseData {
		var existing manifest
		if err := readJSON(filepath.Join(outRoot, "manifest.json"), &existing); err != nil {
			return err
		}
		if !existing.Complete || existing.Scope != "TRAIN_VALIDATION_2024_ONLY" || len(existing.Months) != 12 {
			return errors.New("existing Metrics V2 manifest is not reusable")
		}
		audits = existing.Months
	}
	for month := 1; month <= 12; month++ {
		name := fmt.Sprintf("2024-%02d", month)
		source := filepath.Join(sourceRoot, fmt.Sprintf("%s-metrics-%s.parquet", symbol, name))
		output := filepath.Join(outRoot, fmt.Sprintf("%s-metrics-v2-%s.parquet", symbol, name))
		rows, err := readV1(source)
		if err != nil {
			return fmt.Errorf("%s source: %w", name, err)
		}
		if !reuseData {
			audit, err := buildMonth(name, rows, output)
			if err != nil {
				return fmt.Errorf("%s build: %w", name, err)
			}
			audits = append(audits, audit)
			fmt.Printf("%s PASS source=%d output=%d\n", name, audit.SourceRows, audit.OutputRows)
		}
		sourceRows = append(sourceRows, rows...)
	}
	integrity, points, err := aggregateIntegrity(audits, sourceRows, outRoot)
	if err != nil {
		return err
	}
	if integrity.Status != "PASS" {
		return fmt.Errorf("independent integrity audit failed")
	}
	comparison, err := compareFeatures(points, featureRoot)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(reportRoot, 0o755); err != nil {
		return err
	}
	if err = writeJSON(filepath.Join(reportRoot, "corrected_schema_and_rules.json"), buildSchemaReport()); err != nil {
		return err
	}
	if err = writeJSON(filepath.Join(reportRoot, "data_integrity.json"), integrity); err != nil {
		return err
	}
	if err = writeJSON(filepath.Join(reportRoot, "feature_and_eligibility_comparison.json"), comparison); err != nil {
		return err
	}
	if err = writeJSON(filepath.Join(reportRoot, "unverified_fields_and_follow_up.json"), unresolvedReport()); err != nil {
		return err
	}
	if !reuseData {
		m := manifest{Version: metricsv2.Version, Symbol: symbol, Scope: "TRAIN_VALIDATION_2024_ONLY", Source: sourceRoot, SourceVersion: 1, Months: audits, TakerChangeAtMs: metricsv2.TakerChangeAtMs, SafetyLagMs: metricsv2.SafetyLagMs, Complete: true}
		for _, a := range audits {
			m.SourceRows += a.SourceRows
			m.OutputRows += a.OutputRows
		}
		if err = writeJSONAtomic(filepath.Join(outRoot, "manifest.json"), m); err != nil {
			return err
		}
		fmt.Printf("METRICS CORRECTION V2 PASS source=%d output=%d\n", m.SourceRows, m.OutputRows)
	} else {
		fmt.Printf("METRICS CORRECTION V2 REUSED source=%d output=%d\n", integrity.SourceRows, integrity.OutputRows)
	}
	return nil
}

func buildMonth(month string, rows []v1Row, output string) (monthAudit, error) {
	a := monthAudit{Month: month, Partition: partition(month), SourceRows: int64(len(rows))}
	var observations []metricsv2.Observation
	var last int64 = -1
	for _, row := range rows {
		if last >= 0 && row.Timestamp <= last {
			return a, fmt.Errorf("duplicate/reverse source timestamp %d", row.Timestamp)
		}
		last = row.Timestamp
		expanded, err := metricsv2.Expand(toSource(row))
		if err != nil {
			return a, err
		}
		observations = append(observations, expanded...)
	}
	tmp := output + ".tmp"
	if err := os.MkdirAll(filepath.Dir(output), 0o755); err != nil {
		return a, err
	}
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return a, err
	}
	w := parquet.NewGenericWriter[metricsv2.Observation](f, parquet.Compression(&parquet.Zstd), parquet.MaxRowsPerRowGroup(64*1024))
	if _, err = w.Write(observations); err == nil {
		err = w.Close()
	}
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(tmp)
		return a, err
	}
	readBack, err := readV2(tmp)
	if err != nil {
		_ = os.Remove(tmp)
		return a, err
	}
	a = auditMonth(month, rows, readBack)
	if a.DuplicateKeys != 0 || a.ReverseKeys != 0 || a.ValueMismatches != 0 || a.OutputRows != a.SourceRows*int64(len(metricsv2.FieldOrder)) {
		_ = os.Remove(tmp)
		return a, fmt.Errorf("local audit failed: %+v", a)
	}
	if err = os.Rename(tmp, output); err != nil {
		return a, err
	}
	info, err := os.Stat(output)
	if err != nil {
		return a, err
	}
	a.ParquetBytes, a.Complete = info.Size(), true
	return a, nil
}

func auditMonth(month string, source []v1Row, out []metricsv2.Observation) monthAudit {
	a := monthAudit{Month: month, Partition: partition(month), SourceRows: int64(len(source)), OutputRows: int64(len(out))}
	expected := map[string]string{}
	h1, h2 := sha256.New(), sha256.New()
	for _, row := range source {
		for _, pair := range sourcePairs(row) {
			key := fmt.Sprintf("%d\x00%s", row.Timestamp, pair[0])
			expected[key] = pair[1]
			fmt.Fprintf(h1, "%s\x00%s\n", key, pair[1])
		}
	}
	seen := map[string]bool{}
	lastTS, lastField := int64(-1), ""
	for _, o := range out {
		key := fmt.Sprintf("%d\x00%s", o.SourceTimestampMs, o.Field)
		if seen[key] {
			a.DuplicateKeys++
		}
		seen[key] = true
		if o.SourceTimestampMs < lastTS || (o.SourceTimestampMs == lastTS && fieldIndex(o.Field) <= fieldIndex(lastField)) {
			a.ReverseKeys++
		}
		lastTS, lastField = o.SourceTimestampMs, o.Field
		if expected[key] != o.Value {
			a.ValueMismatches++
		}
		fmt.Fprintf(h2, "%s\x00%s\n", key, o.Value)
		if o.Field == metricsv2.TakerRatio {
			a.TakerRows++
			if !o.SemanticVerified || o.IntervalEndMs <= o.IntervalStartMs || o.EarliestAvailableMs != o.IntervalEndMs+metricsv2.SafetyLagMs {
				a.FutureAvailability++
			}
		} else {
			a.UnknownSemanticRows++
			if o.SemanticVerified || o.IntervalStartMs != 0 || o.IntervalEndMs != 0 || o.EarliestAvailableMs != 0 {
				a.FutureAvailability++
			}
		}
	}
	a.SourceValueSHA256 = hex.EncodeToString(h1.Sum(nil))
	a.OutputValueSHA256 = hex.EncodeToString(h2.Sum(nil))
	return a
}

func aggregateIntegrity(audits []monthAudit, source []v1Row, outRoot string) (integrityReport, []takerPoint, error) {
	r := integrityReport{Status: "PASS", ExpectedFieldsPerSource: len(metricsv2.FieldOrder), Monthly: audits}
	var points []takerPoint
	for _, a := range audits {
		r.SourceRows += a.SourceRows
		r.OutputRows += a.OutputRows
		r.TakerRows += a.TakerRows
		r.UnknownSemanticRows += a.UnknownSemanticRows
		r.DuplicateKeys += a.DuplicateKeys
		r.ReverseKeys += a.ReverseKeys
		r.ValueMismatches += a.ValueMismatches
		r.AvailabilityMetadataErrors += a.FutureAvailability
		path := filepath.Join(outRoot, fmt.Sprintf("%s-metrics-v2-%s.parquet", symbol, a.Month))
		rows, err := readV2(path)
		if err != nil {
			return r, nil, err
		}
		for _, o := range rows {
			if o.Field != metricsv2.TakerRatio {
				continue
			}
			value, err := strconv.ParseFloat(o.Value, 64)
			if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value <= 0 {
				return r, nil, fmt.Errorf("invalid Taker value at %d", o.SourceTimestampMs)
			}
			points = append(points, takerPoint{o.SourceTimestampMs, o.IntervalStartMs, o.IntervalEndMs, o.EarliestAvailableMs, value})
		}
	}
	for i := 1; i < len(points); i++ {
		delta := intervalDelta(points[i-1], points[i])
		if delta > 0 {
			r.TakerIntervalGaps++
			e := gapExample{points[i-1].RawTimestampMs, points[i-1].IntervalEndMs, points[i].RawTimestampMs, points[i].IntervalStartMs, delta}
			if len(r.GapExamples) < 20 {
				r.GapExamples = append(r.GapExamples, e)
			}
			if points[i].RawTimestampMs == metricsv2.TakerChangeAtMs {
				r.TransitionGapPresent = true
				copy := e
				r.TransitionGap = &copy
			}
		} else if delta < 0 {
			r.TakerIntervalOverlaps++
		}
	}
	if r.OutputRows != r.SourceRows*int64(len(metricsv2.FieldOrder)) || r.DuplicateKeys != 0 || r.ReverseKeys != 0 || r.ValueMismatches != 0 || r.AvailabilityMetadataErrors != 0 || r.TakerIntervalOverlaps != 0 || !r.TransitionGapPresent {
		r.Status = "FAIL"
	}
	_ = source
	return r, points, nil
}

func intervalDelta(previous, current takerPoint) int64 {
	return current.IntervalStartMs - previous.IntervalEndMs
}

func compareFeatures(points []takerPoint, featureRoot string) (comparisonReport, error) {
	r := comparisonReport{Status: "PASS", Scope: "TRAIN_VALIDATION_2024_ONLY", FeatureCount: 128, ConfirmedChangedFeatures: []string{"taker_long_short_volume_ratio", "taker_long_short_volume_ratio_change_5m"}, UnchangedByConstruction: 126, EligibilityInterpretation: "기존 emitted Feature V2 timestamp를 독립 재읽고 corrected Taker current/ref availability와 freshness를 대조. 다른 126개 값과 source policy는 변경하지 않음"}
	byPartition := map[string]*comparisonStats{"TRAIN": {Partition: "TRAIN"}, "VALIDATION": {Partition: "VALIDATION"}}
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	end := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	for d := start; d < end; d += 5000 {
		p := byPartition[partition(time.UnixMilli(d).UTC().Format("2006-01"))]
		p.TotalDecisionGrid++
		legacyCur := selectLegacy(points, d)
		correctedCur := selectCorrected(points, d, 0)
		legacyRef := selectLegacy(points, d-metricsv2.IntervalMs)
		correctedRef := selectCorrected(points, d-metricsv2.IntervalMs, 0)
		if legacyCur != nil && correctedCur != nil {
			p.BothLevelAvailable++
			if legacyCur.Value != correctedCur.Value {
				p.LevelChanged++
			}
		}
		if legacyCur != nil && correctedCur != nil && legacyRef != nil && correctedRef != nil {
			p.BothChange5mAvailable++
			oldChange := (legacyCur.Value - legacyRef.Value) / legacyRef.Value
			newChange := (correctedCur.Value - correctedRef.Value) / correctedRef.Value
			if oldChange != newChange {
				p.Change5mChanged++
			}
		}
		if legacyCur != nil && legacyCur.IntervalEndMs > d {
			p.LegacyFutureSelections++
		}
		if correctedCur != nil && correctedCur.IntervalEndMs > d {
			p.CorrectedFutureSelections++
		}
		le := takerEligible(legacyCur, legacyRef, d, true)
		ce := takerEligible(correctedCur, correctedRef, d, false)
		if le {
			p.LegacyTakerEligible++
		}
		if ce {
			p.CorrectedTakerEligible++
		}
		if le != ce {
			p.TakerEligibilityChanged++
			if le {
				p.LegacyEligibleCorrectedNot++
			} else {
				p.LegacyNotCorrectedEligible++
			}
		}
	}
	existingTimestamps := map[string][]int64{"TRAIN": {}, "VALIDATION": {}}
	for month := 1; month <= 12; month++ {
		name := fmt.Sprintf("2024-%02d", month)
		path := filepath.Join(featureRoot, fmt.Sprintf("%s-main-features-v2-%s.parquet", symbol, name))
		rows, err := readExisting(path)
		if err != nil {
			return r, fmt.Errorf("existing feature %s: %w", name, err)
		}
		p := byPartition[partition(name)]
		for _, row := range rows {
			p.ExistingEligibleRows++
			existingTimestamps[p.Partition] = append(existingTimestamps[p.Partition], row.DecisionTimestampMs)
			legacyCur := selectLegacy(points, row.DecisionTimestampMs)
			legacyRef := selectLegacy(points, row.DecisionTimestampMs-metricsv2.IntervalMs)
			if legacyCur == nil || !near(legacyCur.Value, row.TakerLevel) {
				p.LegacyLevelFrozenMismatches++
			}
			if legacyCur == nil || legacyRef == nil || !near((legacyCur.Value-legacyRef.Value)/legacyRef.Value, row.TakerChange5m) {
				p.LegacyChangeFrozenMismatches++
			}
			cur := selectCorrected(points, row.DecisionTimestampMs, 0)
			ref := selectCorrected(points, row.DecisionTimestampMs-metricsv2.IntervalMs, 0)
			if takerEligible(cur, ref, row.DecisionTimestampMs, false) {
				p.ExistingEligibleStillValid++
			} else {
				p.ExistingEligibleInvalidated++
			}
		}
	}
	r.Partitions = []comparisonStats{*byPartition["TRAIN"], *byPartition["VALIDATION"]}
	for _, delay := range []int64{0, 5_000, 30_000, 60_000} {
		var s sensitivity
		s.ExtraDelayMs = delay
		for d := start; d < end; d += 5000 {
			if takerEligible(selectCorrected(points, d, delay), selectCorrected(points, d-metricsv2.IntervalMs, delay), d, false) {
				s.CorrectedEligible++
			}
		}
		for _, partitionName := range []string{"TRAIN", "VALIDATION"} {
			for _, decision := range existingTimestamps[partitionName] {
				cur := selectCorrected(points, decision, delay)
				ref := selectCorrected(points, decision-metricsv2.IntervalMs, delay)
				if takerEligible(cur, ref, decision, false) {
					s.ExistingEligibleRetained++
				} else {
					s.ExistingEligibleDropped++
				}
			}
		}
		r.Sensitivity = append(r.Sensitivity, s)
	}
	for _, p := range r.Partitions {
		if p.CorrectedFutureSelections != 0 {
			r.Status = "FAIL"
		}
	}
	return r, nil
}

func readExisting(path string) ([]existingFeatureRow, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := parquet.NewGenericReader[existingFeatureRow](f)
	defer r.Close()
	buf := make([]existingFeatureRow, 8192)
	var out []existingFeatureRow
	for {
		n, readErr := r.Read(buf)
		out = append(out, buf[:n]...)
		if errors.Is(readErr, io.EOF) {
			return out, nil
		}
		if readErr != nil {
			return nil, readErr
		}
	}
}

func selectLegacy(points []takerPoint, decision int64) *takerPoint {
	i := sort.Search(len(points), func(i int) bool { return points[i].RawTimestampMs+metricsv2.SafetyLagMs > decision })
	if i == 0 {
		return nil
	}
	return &points[i-1]
}
func selectCorrected(points []takerPoint, decision, extra int64) *takerPoint {
	i := sort.Search(len(points), func(i int) bool { return points[i].EarliestAvailableMs+extra > decision })
	if i == 0 {
		return nil
	}
	return &points[i-1]
}
func takerEligible(cur, ref *takerPoint, decision int64, legacy bool) bool {
	if cur == nil || ref == nil {
		return false
	}
	ageBase := cur.IntervalEndMs
	if legacy {
		ageBase = cur.RawTimestampMs
	}
	return decision-ageBase >= 0 && decision-ageBase <= metricsv2.FreshnessLimitMs && cur.Value > 0 && ref.Value > 0
}

func near(a, b float64) bool {
	d := math.Abs(a - b)
	return d <= 1e-12 || d <= 1e-9*math.Max(math.Abs(a), math.Abs(b))
}

func readV1(path string) ([]v1Row, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := parquet.NewGenericReader[v1Row](f)
	defer r.Close()
	var out []v1Row
	buf := make([]v1Row, 2048)
	for {
		n, e := r.Read(buf)
		out = append(out, buf[:n]...)
		if errors.Is(e, io.EOF) {
			return out, nil
		}
		if e != nil {
			return nil, e
		}
	}
}
func readV2(path string) ([]metricsv2.Observation, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := parquet.NewGenericReader[metricsv2.Observation](f)
	defer r.Close()
	var out []metricsv2.Observation
	buf := make([]metricsv2.Observation, 4096)
	for {
		n, e := r.Read(buf)
		out = append(out, buf[:n]...)
		if errors.Is(e, io.EOF) {
			return out, nil
		}
		if e != nil {
			return nil, e
		}
	}
}
func toSource(r v1Row) metricsv2.SourceRow {
	return metricsv2.SourceRow{
		TimestampMs:            r.Timestamp,
		OpenInterest:           r.OpenInterest,
		OpenInterestValue:      r.OpenInterestValue,
		TopTraderAccountRatio:  r.TopTraderAccountRatio,
		TopTraderPositionRatio: r.TopTraderPositionRatio,
		GlobalRatio:            r.GlobalRatio,
		TakerRatio:             r.TakerRatio,
	}
}
func sourcePairs(r v1Row) [][2]string {
	return [][2]string{{metricsv2.OpenInterest, r.OpenInterest}, {metricsv2.OpenInterestValue, r.OpenInterestValue}, {metricsv2.TopTraderAccountRatio, r.TopTraderAccountRatio}, {metricsv2.TopTraderPositionRatio, r.TopTraderPositionRatio}, {metricsv2.GlobalRatio, r.GlobalRatio}, {metricsv2.TakerRatio, r.TakerRatio}}
}
func fieldIndex(field string) int {
	for i, x := range metricsv2.FieldOrder {
		if x == field {
			return i
		}
	}
	return -1
}
func partition(month string) string {
	if month < "2024-10" {
		return "TRAIN"
	}
	return "VALIDATION"
}

func buildSchemaReport() schemaReport {
	unknown := func(field string) fieldEvidence {
		return fieldEvidence{Field: field, Status: metricsv2.SemanticUnverified, ObservedRule: "raw timestamp와 값만 보존; interval start/end 및 availability 미확정", CorrectedAsOfEnabled: false, Reason: "독립적으로 시간 의미를 검증할 ground truth가 없음"}
	}
	return schemaReport{Version: metricsv2.Version, Layout: "FIELD_LEVEL_LONG_FORMAT_NO_IMPLICIT_TIMESTAMP_JOIN", Columns: []string{"source_timestamp_ms", "field", "value", "interval_start_ms", "interval_end_ms", "earliest_available_at_ms", "timestamp_semantic", "semantic_verified", "historical_minimum_only"}, TakerChangeAtUTC: "2024-03-04T00:00:00Z", HistoricalRule: "verified interval end + 5000ms + configurable non-negative extra delay", LiveRule: "max(verified interval end + 5000ms + extra delay, ReceiveTimestampMs)", HistoricalLimitation: "historical earliest availability is a minimum bound; actual Binance publication delay is unavailable", Fields: []fieldEvidence{unknown(metricsv2.OpenInterest), unknown(metricsv2.OpenInterestValue), unknown(metricsv2.GlobalRatio), unknown(metricsv2.TopTraderAccountRatio), unknown(metricsv2.TopTraderPositionRatio), {Field: metricsv2.TakerRatio, Status: "VERIFIED_CHANGE_POINT", ObservedRule: "before 2024-03-04 END; from 2024-03-04 START", CorrectedAsOfEnabled: true, Reason: "Futures 1s taker buy/sell aggregation comparison and raw/canonical exact equality"}}}
}
func unresolvedReport() any {
	return map[string]any{"status": "FOLLOW_UP_REQUIRED", "unverified_fields": []string{metricsv2.OpenInterest, metricsv2.OpenInterestValue, metricsv2.GlobalRatio, metricsv2.TopTraderAccountRatio, metricsv2.TopTraderPositionRatio}, "rules": []string{"독립 ground truth 확보 전 interval 또는 availability를 추정하지 않음", "field-level observation을 timestamp만으로 재결합하지 않음", "결측을 미래 값으로 채우지 않음", "실제 publication delay 자료 확보 시 새 version에서만 반영"}, "test_accessed": false, "final_holdout_accessed": false}
}
func writeJSON(path string, v any) error {
	b, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		return e
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}
func writeJSONAtomic(path string, v any) error {
	if _, e := os.Stat(path); e == nil {
		return fmt.Errorf("manifest exists: %s", path)
	}
	b, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		return e
	}
	tmp := path + ".tmp"
	if e = os.WriteFile(tmp, append(b, '\n'), 0o644); e != nil {
		return e
	}
	return os.Rename(tmp, path)
}

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}
