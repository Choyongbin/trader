// metricsparityaudit records field-specific current API timing evidence and
// summarizes the already-completed Phase 3A TRAIN/VALIDATION comparison.
// It does not modify canonical data, frozen features, models, or policies.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"time"

	"binance_trader/internal/external/metricsv2"
	live "binance_trader/internal/live/binance"
)

const docsURL = "https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data"

type fieldEvidence struct {
	Field                    string `json:"field"`
	CurrentDocumentation     string `json:"current_documentation_semantic"`
	CurrentDocumentationURL  string `json:"current_documentation_url"`
	Historical2024Status     string `json:"historical_2024_status"`
	HistoricalEvidence       string `json:"historical_evidence"`
	HistoricalCorrectionSafe bool   `json:"historical_correction_safe"`
}

type evidenceReport struct {
	Status                   string          `json:"status"`
	Scope                    string          `json:"scope"`
	Fields                   []fieldEvidence `json:"fields"`
	RawCanonicalSampleStatus string          `json:"raw_canonical_sample_status"`
	RawCanonicalSampleDays   int             `json:"raw_canonical_sample_days"`
	V1V2All2024ValueStatus   string          `json:"v1_v2_all_2024_value_status"`
	FullHistoricalVerified   bool            `json:"full_historical_verified"`
	TestAccessed             bool            `json:"test_accessed"`
	FinalHoldoutAccessed     bool            `json:"final_holdout_accessed"`
}

type phase3Partition struct {
	Partition                  string `json:"partition"`
	TotalDecisionGrid          int64  `json:"total_decision_grid"`
	LevelChanged               int64  `json:"taker_level_changed"`
	Change5mChanged            int64  `json:"taker_change_5m_changed"`
	LegacyFutureSelections     int64  `json:"legacy_future_selections"`
	CorrectedFutureSelections  int64  `json:"corrected_future_selections"`
	LegacyTakerEligible        int64  `json:"legacy_taker_component_eligible"`
	CorrectedTakerEligible     int64  `json:"corrected_taker_component_eligible"`
	TakerEligibilityChanged    int64  `json:"taker_component_eligibility_changed"`
	ExistingEligibleRows       int64  `json:"existing_feature_v2_eligible_rows"`
	ExistingEligibleStillValid int64  `json:"existing_eligible_still_taker_valid"`
}

type phase3Comparison struct {
	Status      string            `json:"status"`
	Partitions  []phase3Partition `json:"partitions"`
	Sensitivity []json.RawMessage `json:"historical_extra_delay_sensitivity"`
}

type modeResult struct {
	Mode       string            `json:"mode"`
	Status     string            `json:"status"`
	Partitions []phase3Partition `json:"partitions,omitempty"`
	Reason     string            `json:"reason,omitempty"`
}

type eligibilityReport struct {
	Scope                string            `json:"scope"`
	Modes                []modeResult      `json:"modes"`
	DelaySensitivity     []json.RawMessage `json:"taker_only_delay_sensitivity"`
	FeatureValueVerdict  string            `json:"feature_value_verdict"`
	EligibilityVerdict   string            `json:"eligibility_verdict"`
	ModelConnected       bool              `json:"existing_model_connected"`
	ModelTrained         bool              `json:"new_model_trained"`
	TestAccessed         bool              `json:"test_accessed"`
	FinalHoldoutAccessed bool              `json:"final_holdout_accessed"`
}

type endpointStats struct {
	Endpoint                    string      `json:"endpoint"`
	Polls                       int         `json:"polls"`
	SuccessfulPolls             int         `json:"successful_polls"`
	HTTPStatusCounts            map[int]int `json:"http_status_counts"`
	ResponseRows                int         `json:"response_rows"`
	UniqueSourceTimestamps      int         `json:"unique_source_timestamps"`
	ResponseDuplicateCount      int         `json:"response_duplicate_timestamps"`
	ResponseReverseCount        int         `json:"response_reverse_timestamps"`
	FirstObservedArrivalSamples int         `json:"first_observed_arrival_samples"`
	LeftCensoredTimestamps      int         `json:"left_censored_timestamps"`
	FirstObservedLagMinMs       int64       `json:"first_observed_lag_min_ms,omitempty"`
	FirstObservedLagMedianMs    int64       `json:"first_observed_lag_median_ms,omitempty"`
	FirstObservedLagP90Ms       int64       `json:"first_observed_lag_p90_ms,omitempty"`
	FirstObservedLagMaxMs       int64       `json:"first_observed_lag_max_ms,omitempty"`
	FirstArrivalCount           int         `json:"first_arrival_count"`
	LastArrivalCount            int         `json:"last_arrival_count"`
	LatestSourceTimestampMs     int64       `json:"latest_source_timestamp_ms,omitempty"`
	LatestIntervalStartMs       int64       `json:"latest_interval_start_ms,omitempty"`
	LatestIntervalEndMs         int64       `json:"latest_interval_end_ms,omitempty"`
	LatestReceiveTimestampMs    int64       `json:"latest_receive_timestamp_ms,omitempty"`
	Errors                      int         `json:"errors"`
}

type liveReport struct {
	Status                         string          `json:"status"`
	StartedAtUTC                   string          `json:"started_at_utc"`
	EndedAtUTC                     string          `json:"ended_at_utc"`
	RequestedDuration              string          `json:"requested_duration"`
	PollInterval                   string          `json:"poll_interval"`
	Endpoints                      []endpointStats `json:"endpoints"`
	CompleteNormalizedIntervals    int             `json:"complete_normalized_intervals"`
	CompleteIntervalSkewSamples    int             `json:"complete_interval_arrival_skew_samples"`
	ArrivalSkewMinMs               int64           `json:"arrival_skew_min_ms,omitempty"`
	ArrivalSkewMedianMs            int64           `json:"arrival_skew_median_ms,omitempty"`
	ArrivalSkewP90Ms               int64           `json:"arrival_skew_p90_ms,omitempty"`
	ArrivalSkewMaxMs               int64           `json:"arrival_skew_max_ms,omitempty"`
	ArrivalObservationPrecisionMs  int64           `json:"arrival_observation_precision_ms"`
	EqualRawTimestampJoinForbidden bool            `json:"equal_raw_timestamp_join_forbidden"`
	OrdersSent                     int             `json:"orders_sent"`
}

func main() {
	out := flag.String("out", "data/reports/diagnostics/metrics-time-parity/phase3b-v1", "new report directory")
	duration := flag.Duration("live-duration", 0, "read-only live observation duration; zero performs one poll")
	interval := flag.Duration("poll-interval", 30*time.Second, "live polling interval")
	baseURL := flag.String("base-url", "https://fapi.binance.com", "public Binance base URL")
	phase3a := flag.String("phase3a-report", "data/reports/diagnostics/metrics-time-correction/phase3a-final/feature_and_eligibility_comparison.json", "Phase 3A comparison")
	rawCanonical := flag.String("raw-canonical-report", "data/reports/diagnostics/metrics-taker-alignment/phase2-2024-change-point-v1/raw_canonical_consistency.json", "Phase 2 raw/canonical evidence")
	manifest := flag.String("metrics-v2-manifest", "data/external/metrics/v2/BTCUSDT/manifest.json", "Metrics V2 manifest")
	resummarize := flag.Bool("resummarize-live", false, "reuse checkpointed live samples and rewrite only their summary")
	flag.Parse()
	if *resummarize {
		if err := resummarizeLive(*out); err != nil {
			fmt.Fprintln(os.Stderr, "PARITY AUDIT ERROR:", err)
			os.Exit(1)
		}
		return
	}
	if err := run(*out, *duration, *interval, *baseURL, *phase3a, *rawCanonical, *manifest); err != nil {
		fmt.Fprintln(os.Stderr, "PARITY AUDIT ERROR:", err)
		os.Exit(1)
	}
}

func run(out string, duration, interval time.Duration, baseURL, phase3aPath, rawCanonicalPath, manifestPath string) error {
	if duration < 0 || interval <= 0 {
		return errors.New("invalid observation timing")
	}
	if _, err := os.Stat(out); err == nil {
		return fmt.Errorf("report directory already exists: %s", out)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	var phase3a phase3Comparison
	if err := readJSON(phase3aPath, &phase3a); err != nil {
		return fmt.Errorf("Phase 3A comparison: %w", err)
	}
	var rawCanonical struct {
		Status string `json:"status"`
		Days   []any  `json:"days"`
	}
	if err := readJSON(rawCanonicalPath, &rawCanonical); err != nil {
		return fmt.Errorf("raw/canonical evidence: %w", err)
	}
	var manifest struct {
		Complete bool `json:"complete"`
		Months   []struct {
			ValueMismatches int64 `json:"value_mismatches"`
			Complete        bool  `json:"complete"`
		} `json:"months"`
	}
	if err := readJSON(manifestPath, &manifest); err != nil {
		return fmt.Errorf("Metrics V2 manifest: %w", err)
	}
	if phase3a.Status != "PASS" || rawCanonical.Status != "PASS" || !manifest.Complete || len(manifest.Months) != 12 {
		return errors.New("required prior evidence is not complete")
	}
	for _, month := range manifest.Months {
		if !month.Complete || month.ValueMismatches != 0 {
			return errors.New("Metrics V2 value preservation evidence failed")
		}
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		return err
	}
	evidence := buildEvidence(len(rawCanonical.Days))
	if err := writeJSON(filepath.Join(out, "field_timestamp_evidence.json"), evidence); err != nil {
		return err
	}
	eligibility := buildEligibility(phase3a)
	if err := writeJSON(filepath.Join(out, "eligibility_modes.json"), eligibility); err != nil {
		return err
	}
	liveResult, err := observeLive(out, duration, interval, baseURL)
	if err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(out, "live_arrival_summary.json"), liveResult); err != nil {
		return err
	}
	samples, err := readLiveSamples(filepath.Join(out, "live_samples.jsonl"))
	if err != nil {
		return err
	}
	if err := writeNormalizedLive(filepath.Join(out, "normalized_live_observations.jsonl"), samples); err != nil {
		return err
	}
	parity := map[string]any{
		"status":                       "TAKER_ONLY_PARITY_PASS_FULL_PARITY_BLOCKED",
		"historical_receive_timestamp": "NOT_AVAILABLE_NOT_SYNTHESIZED",
		"historical_taker_rule":        "END_THROUGH_2024_03_03_START_FROM_2024_03_04",
		"live_rule":                    "max(interval_end+5000ms, receive_timestamp_ms)",
		"join_key":                     "interval_start_ms+interval_end_ms",
		"legacy_live_join":             "RAW_TIMESTAMP_JOIN_DOES_NOT_EXPRESS_COMMON_INTERVAL",
		"full_corrected_available":     false,
		"blocking_fields":              []string{metricsv2.OpenInterest, metricsv2.OpenInterestValue, metricsv2.GlobalRatio, metricsv2.TopTraderAccountRatio, metricsv2.TopTraderPositionRatio},
		"phase3c_conditions":           []string{"independent historical interval evidence for every blocking field", "publication/receipt evidence or explicit conservative availability policy", "field-level corrected feature materialization and TRAIN/VALIDATION-only validation"},
		"test_accessed":                false,
		"final_holdout_accessed":       false,
		"orders_sent":                  0,
	}
	return writeJSON(filepath.Join(out, "historical_live_parity.json"), parity)
}

func buildEvidence(sampleDays int) evidenceReport {
	fields := make([]fieldEvidence, 0, len(metricsv2.FieldOrder))
	for _, field := range metricsv2.FieldOrder {
		semantic, _ := metricsv2.CurrentDocumentedSemantic(field)
		historicalStatus := "UNVERIFIED"
		historicalEvidence := "No independent 2024 interval ground truth available; current documentation is not retroactive evidence"
		safe := false
		if field == metricsv2.TakerRatio {
			historicalStatus = "VERIFIED_TRANSITION"
			historicalEvidence = "2024 aggregate trades independently match END through 2024-03-03 and START from 2024-03-04"
			safe = true
		}
		fields = append(fields, fieldEvidence{Field: field, CurrentDocumentation: semantic, CurrentDocumentationURL: docsURL, Historical2024Status: historicalStatus, HistoricalEvidence: historicalEvidence, HistoricalCorrectionSafe: safe})
	}
	return evidenceReport{Status: "PARTIAL_VERIFICATION", Scope: "TRAIN_VALIDATION_2024_ONLY", Fields: fields, RawCanonicalSampleStatus: "PASS", RawCanonicalSampleDays: sampleDays, V1V2All2024ValueStatus: "PASS", FullHistoricalVerified: false}
}

func buildEligibility(source phase3Comparison) eligibilityReport {
	legacy := make([]phase3Partition, len(source.Partitions))
	corrected := make([]phase3Partition, len(source.Partitions))
	copy(legacy, source.Partitions)
	copy(corrected, source.Partitions)
	return eligibilityReport{
		Scope: "TRAIN_VALIDATION_2024_ONLY",
		Modes: []modeResult{
			{Mode: "LEGACY", Status: "OBSERVED_FROZEN", Partitions: legacy},
			{Mode: "TAKER_ONLY_CORRECTED", Status: "COMPUTED_PHASE3A", Partitions: corrected},
			{Mode: "FULL_CORRECTED", Status: "NOT_COMPUTABLE", Reason: "five required historical field semantics remain UNVERIFIED"},
		},
		DelaySensitivity:    source.Sensitivity,
		FeatureValueVerdict: "Taker level and 5m change differ; other 126 frozen features were not recomputed",
		EligibilityVerdict:  "TAKER_ONLY computed; unchanged eligible counts alone do not prove full leakage removal",
	}
}

func observeLive(out string, duration, interval time.Duration, baseURL string) (liveReport, error) {
	started := time.Now().UTC()
	var samples []live.MetricsProbeResult
	samplesPath := filepath.Join(out, "live_samples.jsonl")
	file, err := os.OpenFile(samplesPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return liveReport{}, err
	}
	encoder := json.NewEncoder(file)
	poll := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		results := live.ProbePublicMetrics(ctx, &http.Client{Timeout: 12 * time.Second}, baseURL)
		cancel()
		for _, result := range results {
			if err := encoder.Encode(result); err != nil {
				return err
			}
			samples = append(samples, result)
		}
		return file.Sync()
	}
	if err := poll(); err != nil {
		_ = file.Close()
		return liveReport{}, err
	}
	if duration > 0 {
		deadline := started.Add(duration)
		for {
			wait := time.Until(deadline)
			if wait <= 0 {
				break
			}
			if wait > interval {
				wait = interval
			}
			timer := time.NewTimer(wait)
			<-timer.C
			if time.Now().UTC().Before(deadline) {
				if err := poll(); err != nil {
					_ = file.Close()
					return liveReport{}, err
				}
			}
		}
	}
	if err := file.Close(); err != nil {
		return liveReport{}, err
	}
	return summarizeLive(samples, started, time.Now().UTC(), duration, interval), nil
}

func summarizeLive(samples []live.MetricsProbeResult, started, ended time.Time, duration, interval time.Duration) liveReport {
	type accumulator struct {
		stats        endpointStats
		seen         map[int64]bool
		firstReceive map[int64]int64
		intervalEnd  map[int64]int64
		leftCensored map[int64]bool
	}
	acc := map[string]*accumulator{}
	var normalized []metricsv2.TimedObservation
	type intervalArrival struct {
		byEndpoint map[string]int64
		censored   bool
	}
	arrivals := map[metricsv2.IntervalKey]*intervalArrival{}
	for _, sample := range samples {
		a := acc[sample.Endpoint]
		if a == nil {
			a = &accumulator{
				stats: endpointStats{Endpoint: sample.Endpoint, HTTPStatusCounts: map[int]int{}},
				seen:  map[int64]bool{}, firstReceive: map[int64]int64{}, intervalEnd: map[int64]int64{}, leftCensored: map[int64]bool{},
			}
			acc[sample.Endpoint] = a
		}
		firstPoll := a.stats.Polls == 0
		a.stats.Polls++
		a.stats.HTTPStatusCounts[sample.HTTPStatus]++
		if sample.Error != "" {
			a.stats.Errors++
			continue
		}
		a.stats.SuccessfulPolls++
		a.stats.ResponseRows += sample.ResponseCount
		for i, row := range sample.Rows {
			if i > 0 {
				if row.SourceTimestampMs == sample.Rows[i-1].SourceTimestampMs {
					a.stats.ResponseDuplicateCount++
				}
				if row.SourceTimestampMs < sample.Rows[i-1].SourceTimestampMs {
					a.stats.ResponseReverseCount++
				}
			}
			a.seen[row.SourceTimestampMs] = true
			if firstPoll {
				a.leftCensored[row.SourceTimestampMs] = true
			}
			for _, field := range endpointFields(sample.Endpoint) {
				timed, err := metricsv2.NormalizeLive(field, row.SourceTimestampMs, sample.ReceiveTimestampMs)
				if err != nil {
					continue
				}
				normalized = append(normalized, timed)
				if previous, ok := a.firstReceive[row.SourceTimestampMs]; !ok || sample.ReceiveTimestampMs < previous {
					a.firstReceive[row.SourceTimestampMs] = sample.ReceiveTimestampMs
					a.intervalEnd[row.SourceTimestampMs] = timed.IntervalEndMs
				}
				key := metricsv2.IntervalKey{StartMs: timed.IntervalStartMs, EndMs: timed.IntervalEndMs}
				arrival := arrivals[key]
				if arrival == nil {
					arrival = &intervalArrival{byEndpoint: map[string]int64{}}
					arrivals[key] = arrival
				}
				if firstPoll {
					arrival.censored = true
				}
				if previous, ok := arrival.byEndpoint[sample.Endpoint]; !ok || sample.ReceiveTimestampMs < previous {
					arrival.byEndpoint[sample.Endpoint] = sample.ReceiveTimestampMs
				}
				if i == len(sample.Rows)-1 {
					if timed.SourceTimestampMs >= a.stats.LatestSourceTimestampMs {
						a.stats.LatestSourceTimestampMs = timed.SourceTimestampMs
						a.stats.LatestIntervalStartMs = timed.IntervalStartMs
						a.stats.LatestIntervalEndMs = timed.IntervalEndMs
						a.stats.LatestReceiveTimestampMs = timed.ReceiveTimestampMs
					}
				}
			}
		}
	}
	var skews []int64
	for _, arrival := range arrivals {
		if arrival.censored || len(arrival.byEndpoint) != 5 {
			continue
		}
		var firstEndpoint, lastEndpoint string
		var minReceive, maxReceive int64
		for endpoint, receive := range arrival.byEndpoint {
			if minReceive == 0 || receive < minReceive {
				minReceive, firstEndpoint = receive, endpoint
			}
			if receive > maxReceive {
				maxReceive, lastEndpoint = receive, endpoint
			}
		}
		acc[firstEndpoint].stats.FirstArrivalCount++
		acc[lastEndpoint].stats.LastArrivalCount++
		skews = append(skews, maxReceive-minReceive)
	}
	sort.Slice(skews, func(i, j int) bool { return skews[i] < skews[j] })

	endpoints := make([]endpointStats, 0, len(acc))
	allSuccess := len(acc) == 5
	for _, a := range acc {
		a.stats.UniqueSourceTimestamps = len(a.seen)
		var lags []int64
		for sourceTimestamp, receiveTimestamp := range a.firstReceive {
			if a.leftCensored[sourceTimestamp] {
				continue
			}
			lags = append(lags, receiveTimestamp-a.intervalEnd[sourceTimestamp])
		}
		a.stats.LeftCensoredTimestamps = len(a.leftCensored)
		a.stats.FirstObservedArrivalSamples = len(lags)
		if len(lags) > 0 {
			sort.Slice(lags, func(i, j int) bool { return lags[i] < lags[j] })
			a.stats.FirstObservedLagMinMs = lags[0]
			a.stats.FirstObservedLagMedianMs = percentile(lags, 0.5)
			a.stats.FirstObservedLagP90Ms = percentile(lags, 0.9)
			a.stats.FirstObservedLagMaxMs = lags[len(lags)-1]
		}
		allSuccess = allSuccess && a.stats.SuccessfulPolls > 0
		endpoints = append(endpoints, a.stats)
	}
	sort.Slice(endpoints, func(i, j int) bool { return endpoints[i].Endpoint < endpoints[j].Endpoint })
	status := "PASS"
	if !allSuccess {
		status = "INSUFFICIENT_LIVE_EVIDENCE"
	}
	report := liveReport{
		Status: status, StartedAtUTC: started.Format(time.RFC3339Nano), EndedAtUTC: ended.Format(time.RFC3339Nano),
		RequestedDuration: duration.String(), PollInterval: interval.String(), Endpoints: endpoints,
		CompleteNormalizedIntervals: len(metricsv2.CompleteIntervals(normalized)), CompleteIntervalSkewSamples: len(skews),
		ArrivalObservationPrecisionMs: interval.Milliseconds(), EqualRawTimestampJoinForbidden: true, OrdersSent: 0,
	}
	if len(skews) > 0 {
		report.ArrivalSkewMinMs = skews[0]
		report.ArrivalSkewMedianMs = percentile(skews, 0.5)
		report.ArrivalSkewP90Ms = percentile(skews, 0.9)
		report.ArrivalSkewMaxMs = skews[len(skews)-1]
	}
	return report
}

func resummarizeLive(out string) error {
	var previous liveReport
	if err := readJSON(filepath.Join(out, "live_arrival_summary.json"), &previous); err != nil {
		return err
	}
	started, err := time.Parse(time.RFC3339Nano, previous.StartedAtUTC)
	if err != nil {
		return err
	}
	ended, err := time.Parse(time.RFC3339Nano, previous.EndedAtUTC)
	if err != nil {
		return err
	}
	duration, err := time.ParseDuration(previous.RequestedDuration)
	if err != nil {
		return err
	}
	interval, err := time.ParseDuration(previous.PollInterval)
	if err != nil {
		return err
	}
	samples, err := readLiveSamples(filepath.Join(out, "live_samples.jsonl"))
	if err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(out, "live_arrival_summary.json"), summarizeLive(samples, started, ended, duration, interval)); err != nil {
		return err
	}
	return writeNormalizedLive(filepath.Join(out, "normalized_live_observations.jsonl"), samples)
}

func readLiveSamples(path string) ([]live.MetricsProbeResult, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var samples []live.MetricsProbeResult
	for _, line := range bytes.Split(b, []byte{'\n'}) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var sample live.MetricsProbeResult
		if err := json.Unmarshal(line, &sample); err != nil {
			return nil, err
		}
		samples = append(samples, sample)
	}
	return samples, nil
}

func writeNormalizedLive(path string, samples []live.MetricsProbeResult) error {
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("normalized live output already exists: %s", path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	tmp := path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(file)
	for _, sample := range samples {
		if sample.Error != "" {
			continue
		}
		for _, row := range sample.Rows {
			var values map[string]json.RawMessage
			if err := json.Unmarshal(row.Raw, &values); err != nil {
				_ = file.Close()
				return err
			}
			for _, field := range endpointFields(sample.Endpoint) {
				observation, err := metricsv2.NormalizeLive(field, row.SourceTimestampMs, sample.ReceiveTimestampMs)
				if err != nil {
					_ = file.Close()
					return err
				}
				key := fieldValueKey(field)
				if err := json.Unmarshal(values[key], &observation.Value); err != nil || observation.Value == "" {
					_ = file.Close()
					return fmt.Errorf("%s missing %s", sample.Endpoint, key)
				}
				if err := encoder.Encode(observation); err != nil {
					_ = file.Close()
					return err
				}
			}
		}
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func fieldValueKey(field string) string {
	switch field {
	case metricsv2.OpenInterest:
		return "sumOpenInterest"
	case metricsv2.OpenInterestValue:
		return "sumOpenInterestValue"
	case metricsv2.TakerRatio:
		return "buySellRatio"
	default:
		return "longShortRatio"
	}
}

func endpointFields(endpoint string) []string {
	switch endpoint {
	case "metrics_oi":
		return []string{metricsv2.OpenInterest, metricsv2.OpenInterestValue}
	case "metrics_global":
		return []string{metricsv2.GlobalRatio}
	case "metrics_top_account":
		return []string{metricsv2.TopTraderAccountRatio}
	case "metrics_top_position":
		return []string{metricsv2.TopTraderPositionRatio}
	case "metrics_taker":
		return []string{metricsv2.TakerRatio}
	default:
		return nil
	}
}

func percentile(sorted []int64, fraction float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(float64(len(sorted)-1) * fraction)
	return sorted[i]
}

func readJSON(path string, value any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, value)
}

func writeJSON(path string, value any) error {
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(path, b, 0o644)
}
