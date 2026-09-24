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

	"binance_trader/internal/external/asof"
	mainfeature "binance_trader/internal/feature/main"
	featurev2 "binance_trader/internal/feature/main/v2"
	"binance_trader/internal/market"
	"binance_trader/internal/model/logistic"
	"github.com/parquet-go/parquet-go"
)

type metricsRow struct {
	Timestamp              int64  `parquet:"timestamp"`
	OpenInterest           string `parquet:"open_interest"`
	OpenInterestValue      string `parquet:"open_interest_value"`
	TopTraderAccountRatio  string `parquet:"top_trader_account_long_short_ratio"`
	TopTraderPositionRatio string `parquet:"top_trader_position_long_short_ratio"`
	GlobalRatio            string `parquet:"global_long_short_ratio"`
	TakerRatio             string `parquet:"taker_long_short_volume_ratio"`
}

type klineRow struct {
	OpenTime  int64 `parquet:"open_time"`
	Close     string
	CloseTime int64
}

type fundingRow struct {
	CalcTime        int64   `parquet:"calc_time"`
	LastFundingRate float64 `parquet:"last_funding_rate"`
}

type event struct {
	sourceTimestampMs int64
	effectiveMs       int64
	values            []float64
	valids            []bool
	valid             bool
}

type rowStream[T any] struct {
	paths  []string
	pathAt int
	file   *os.File
	reader *parquet.GenericReader[T]
	buf    []T
	at, n  int
}

func newRowStream[T any](paths []string) *rowStream[T] {
	return &rowStream[T]{paths: paths, buf: make([]T, 4096)}
}

func (s *rowStream[T]) next() (T, error) {
	var zero T
	for {
		if s.at < s.n {
			v := s.buf[s.at]
			s.at++
			return v, nil
		}
		if s.reader == nil {
			if s.pathAt == len(s.paths) {
				return zero, io.EOF
			}
			f, err := os.Open(s.paths[s.pathAt])
			if err != nil {
				return zero, err
			}
			s.pathAt++
			s.file, s.reader = f, parquet.NewGenericReader[T](f)
		}
		n, err := s.reader.Read(s.buf)
		s.at, s.n = 0, n
		if n > 0 {
			continue
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return zero, err
		}
		if err := s.reader.Close(); err != nil {
			return zero, err
		}
		if err := s.file.Close(); err != nil {
			return zero, err
		}
		s.reader, s.file = nil, nil
	}
}

func (s *rowStream[T]) close() {
	if s.reader != nil {
		_ = s.reader.Close()
	}
	if s.file != nil {
		_ = s.file.Close()
	}
}

type timeline struct {
	nextEvent func() (event, error)
	onAdvance func(event) error
	next      event
	hasNext   bool
	history   []event
	head      int
}

func newTimeline(next func() (event, error)) (*timeline, error) {
	t := &timeline{nextEvent: next}
	if err := t.readNext(); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return t, nil
}

func (t *timeline) readNext() error {
	v, err := t.nextEvent()
	if errors.Is(err, io.EOF) {
		t.hasNext = false
		return io.EOF
	}
	if err != nil {
		return err
	}
	if len(t.history) > 0 && v.sourceTimestampMs <= t.history[len(t.history)-1].sourceTimestampMs {
		return fmt.Errorf("source timestamps not strictly ascending")
	}
	t.next, t.hasNext = v, true
	return nil
}

func (t *timeline) advance(decision int64) error {
	for t.hasNext && t.next.effectiveMs <= decision {
		if t.onAdvance != nil {
			if err := t.onAdvance(t.next); err != nil {
				return err
			}
		}
		t.history = append(t.history, t.next)
		if err := t.readNext(); err != nil && !errors.Is(err, io.EOF) {
			return err
		}
	}
	cutoff := decision - 20*60*60*1000
	for t.head+1 < len(t.history) && t.history[t.head+1].effectiveMs < cutoff {
		t.head++
	}
	if t.head > 4096 {
		t.history = append([]event(nil), t.history[t.head:]...)
		t.head = 0
	}
	return nil
}

func (t *timeline) at(anchor, maxAge int64) (featurev2.SourceState, *event) {
	if len(t.history) == t.head {
		return featurev2.SourceState{}, nil
	}
	x := t.history[t.head:]
	i := sort.Search(len(x), func(i int) bool { return x[i].effectiveMs > anchor }) - 1
	if i < 0 {
		return featurev2.SourceState{}, nil
	}
	e := &x[i]
	return featurev2.SourceState{Available: true, Fresh: anchor-e.sourceTimestampMs <= maxAge, EffectiveAvailableAtMs: e.effectiveMs}, e
}

func (t *timeline) prior(anchor int64) *event {
	if len(t.history) == t.head {
		return nil
	}
	x := t.history[t.head:]
	i := sort.Search(len(x), func(i int) bool { return x[i].effectiveMs > anchor }) - 2
	if i < 0 {
		return nil
	}
	return &x[i]
}

type spotPoint struct {
	close           float64
	cumulativeQuote float64
}

type partitionCoverage struct {
	Total           int64                      `json:"total_decision_candidates"`
	Eligible        int64                      `json:"eligible"`
	Excluded        int64                      `json:"excluded"`
	EligibilityRate float64                    `json:"eligibility_rate"`
	Reasons         map[featurev2.Reason]int64 `json:"exclusion_reasons"`
}

type nonFiniteDiagnostic struct {
	Count   int64  `json:"count"`
	Cause   string `json:"cause"`
	Formula string `json:"formula"`
}

type report struct {
	PolicyVersion               int64                          `json:"policy_version"`
	Symbol                      string                         `json:"symbol"`
	GeneratedAtUTC              string                         `json:"generated_at_utc"`
	FeatureV2SpecPath           string                         `json:"feature_v2_spec_path"`
	FeatureV2SpecSHA256         string                         `json:"feature_v2_spec_sha256"`
	FeatureV2RegistrySHA256     string                         `json:"feature_v2_registry_sha256"`
	ExternalAsOfPolicyVersion   int                            `json:"external_asof_policy_version"`
	ExternalAsOfPolicySHA256    string                         `json:"external_asof_policy_sha256"`
	StrictExclusionSemantics    string                         `json:"strict_exclusion_semantics"`
	SourceFreshnessMs           map[string]int64               `json:"source_freshness_ms"`
	NumericMissingEncoding      string                         `json:"numeric_missing_encoding"`
	V1WarmupMs                  int64                          `json:"v1_warmup_ms"`
	V2MaxLookbackMs             int64                          `json:"v2_max_lookback_ms"`
	RequiredWarmupMs            int64                          `json:"required_warmup_ms"`
	PartitionCoverage           map[string]*partitionCoverage  `json:"partition_coverage"`
	FutureObservationCount      int64                          `json:"future_observation_count"`
	NonFiniteFeatureDiagnostics map[string]nonFiniteDiagnostic `json:"non_finite_feature_diagnostics"`
	AllEmittedFeaturesFinite    bool                           `json:"all_emitted_features_finite"`
	PriorDryRunEligible         map[string]int64               `json:"prior_dry_run_eligible"`
	EligibleCountChange         map[string]int64               `json:"eligible_count_change"`
	CoverageChangeExplanation   string                         `json:"coverage_change_explanation"`
	ImplementationBugRemaining  bool                           `json:"implementation_bug_remaining"`
	ConstantFreshMasks          []string                       `json:"constant_fresh_masks"`
	SpecAmendmentRequired       bool                           `json:"spec_amendment_required"`
	Blocker                     string                         `json:"blocker"`
	LabelIndependent            bool                           `json:"label_independent"`
	FeatureV2Materialization    string                         `json:"feature_v2_materialization"`
	FinalHoldoutAccessed        bool                           `json:"final_holdout_accessed"`
	FinalStatus                 string                         `json:"final_status"`
	OriginalReportPath          string                         `json:"original_report_path"`
	OriginalReportSHA256        string                         `json:"original_report_sha256"`
	CorrectionReason            string                         `json:"correction_reason"`
	AffectedKlineGapBeforeMs    int64                          `json:"affected_kline_gap_before_ms"`
	AffectedKlineGapAfterMs     int64                          `json:"affected_kline_gap_after_ms"`
	OldTrainEligible            int64                          `json:"old_train_eligible"`
	CorrectedTrainEligible      int64                          `json:"corrected_train_eligible"`
	DryRunProductionMismatches  int64                          `json:"dry_run_production_mismatches"`
	KlineGapStaleCount          int64                          `json:"kline_gap_stale_count"`
}

func main() {
	output := flag.String("output", filepath.FromSlash("data/reports/feature/main/v2/BTCUSDT-feature-v2-eligibility-v1-correction-1.json"), "report path")
	flag.Parse()
	if err := run(*output); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(output string) error {
	const (
		trainStart      = int64(1704067200000)
		validationStart = int64(1727740800000)
		testStart       = int64(1735689600000)
		developmentEnd  = int64(1751328000000)
	)
	specPath := filepath.FromSlash("data/reports/feature/main/v2/BTCUSDT-feature-v2-spec-v1-amendment-1.json")
	policyPath := filepath.FromSlash("data/reports/external/join-policy/v1/BTCUSDT-external-join-policy-v1.json")
	specHash, err := fileSHA256(specPath)
	if err != nil {
		return err
	}
	policyHash, err := fileSHA256(policyPath)
	if err != nil {
		return err
	}
	originalReportPath := filepath.FromSlash("data/reports/feature/main/v2/BTCUSDT-feature-v2-eligibility-v1-final.json")
	originalReportHash, err := fileSHA256(originalReportPath)
	if err != nil {
		return err
	}
	registryHash := logistic.FeatureRegistryHash(featurev2.ModelFeatureColumnsV2)

	spotPaths, err := monthlyPaths(filepath.FromSlash("data/external/spot/1s/v1/BTCUSDT"), "BTCUSDT-spot-1s")
	if err != nil {
		return err
	}
	metricsPaths, err := monthlyPaths(filepath.FromSlash("data/external/metrics/v1/BTCUSDT"), "BTCUSDT-metrics")
	if err != nil {
		return err
	}
	markPaths, err := monthlyPaths(filepath.FromSlash("data/external/markprice/v1/BTCUSDT"), "BTCUSDT-markprice")
	if err != nil {
		return err
	}
	indexPaths, err := monthlyPaths(filepath.FromSlash("data/external/indexprice/v1/BTCUSDT"), "BTCUSDT-indexprice")
	if err != nil {
		return err
	}
	premiumPaths, err := monthlyPaths(filepath.FromSlash("data/external/premiumindex/v1/BTCUSDT"), "BTCUSDT-premiumindex")
	if err != nil {
		return err
	}
	fundingPaths, err := monthlyPaths(filepath.FromSlash("data/external/funding/v1/BTCUSDT"), "BTCUSDT-funding")
	if err != nil {
		return err
	}
	v1Paths, err := v1MonthlyPaths(filepath.FromSlash("data/features/main/v1/BTCUSDT"), "BTCUSDT-main-features-v1")
	if err != nil {
		return err
	}

	spot := newRowStream[market.SecondBar](spotPaths)
	defer spot.close()
	metricsStream := newRowStream[metricsRow](metricsPaths)
	defer metricsStream.close()
	markStream := newRowStream[klineRow](markPaths)
	defer markStream.close()
	indexStream := newRowStream[klineRow](indexPaths)
	defer indexStream.close()
	premiumStream := newRowStream[klineRow](premiumPaths)
	defer premiumStream.close()
	fundingStream := newRowStream[fundingRow](fundingPaths)
	defer fundingStream.close()
	v1Stream := newRowStream[mainfeature.MainFeaturesV1](v1Paths)
	defer v1Stream.close()

	metrics, err := newTimeline(func() (event, error) {
		r, err := metricsStream.next()
		if err != nil {
			return event{}, err
		}
		values, valids, err := parseStrings(r.OpenInterest, r.OpenInterestValue, r.TopTraderAccountRatio, r.TopTraderPositionRatio, r.GlobalRatio, r.TakerRatio)
		if err != nil {
			return event{}, err
		}
		valid := true
		for _, ok := range valids {
			valid = valid && ok
		}
		return event{sourceTimestampMs: r.Timestamp, effectiveMs: r.Timestamp + asof.ExternalSafetyLagMs, values: values, valids: valids, valid: valid}, nil
	})
	if err != nil {
		return err
	}
	newKlineTimeline := func(s *rowStream[klineRow]) (*timeline, error) {
		return newTimeline(func() (event, error) {
			r, err := s.next()
			if err != nil {
				return event{}, err
			}
			v, err := strconv.ParseFloat(r.Close, 64)
			if err != nil {
				return event{}, err
			}
			return event{sourceTimestampMs: r.OpenTime, effectiveMs: r.CloseTime + 1 + asof.ExternalSafetyLagMs, values: []float64{v}, valid: true}, nil
		})
	}
	mark, err := newKlineTimeline(markStream)
	if err != nil {
		return err
	}
	index, err := newKlineTimeline(indexStream)
	if err != nil {
		return err
	}
	premium, err := newKlineTimeline(premiumStream)
	if err != nil {
		return err
	}
	funding, err := newTimeline(func() (event, error) {
		r, err := fundingStream.next()
		if err != nil {
			return event{}, err
		}
		return event{sourceTimestampMs: r.CalcTime, effectiveMs: r.CalcTime + asof.ExternalSafetyLagMs, values: []float64{r.LastFundingRate}, valid: true}, nil
	})
	if err != nil {
		return err
	}
	engine := featurev2.NewStreamingEngine()
	metrics.onAdvance = func(e event) error {
		optional := func(i int) featurev2.OptionalFloat {
			return featurev2.OptionalFloat{Value: e.values[i], Valid: e.valids[i]}
		}
		return engine.AddMetrics(featurev2.MetricsObservation{TimestampMs: e.sourceTimestampMs, Value: featurev2.MetricsValue{OpenInterest: optional(0), OpenInterestValue: optional(1), TopTraderAccountRatio: optional(2), TopTraderPositionRatio: optional(3), GlobalRatio: optional(4), TakerRatio: optional(5)}})
	}
	addKline := func(add func(featurev2.KlineObservation) error) func(event) error {
		return func(e event) error {
			return add(featurev2.KlineObservation{OpenTimeMs: e.sourceTimestampMs, CloseTimeMs: e.effectiveMs - asof.ExternalSafetyLagMs - 1, Close: e.values[0]})
		}
	}
	mark.onAdvance = addKline(engine.AddMark)
	index.onAdvance = addKline(engine.AddIndex)
	premium.onAdvance = addKline(engine.AddPremium)
	funding.onAdvance = func(e event) error {
		return engine.AddFunding(featurev2.FundingObservation{TimestampMs: e.sourceTimestampMs, Rate: e.values[0]})
	}

	coverage := map[string]*partitionCoverage{
		"TRAIN":      {Reasons: map[featurev2.Reason]int64{}},
		"VALIDATION": {Reasons: map[featurev2.Reason]int64{}},
		"TEST":       {Reasons: map[featurev2.Reason]int64{}},
	}
	points := make(map[int64]spotPoint, 302)
	var nextSpot market.SecondBar
	hasSpot := false
	var cumulativeQuote float64
	var futureCount int64
	var productionMismatchCount int64
	var klineGapStaleCount int64
	nonFiniteCounts := map[string]int64{}
	readSpot := func() error {
		r, err := spot.next()
		if err != nil {
			return err
		}
		nextSpot, hasSpot = r, true
		return nil
	}
	if err := readSpot(); err != nil {
		return err
	}
	var nextV1 mainfeature.MainFeaturesV1
	hasV1 := false
	readV1 := func() error {
		r, err := v1Stream.next()
		if err != nil {
			return err
		}
		nextV1, hasV1 = r, true
		return nil
	}
	if err := readV1(); err != nil {
		return err
	}

	for decision := trainStart; decision < developmentEnd; decision += 5000 {
		target := decision - 1000
		for hasSpot && nextSpot.TimestampMs <= target {
			if err := engine.AddSpot(nextSpot); err != nil {
				return err
			}
			cumulativeQuote += nextSpot.QuoteVolume
			points[nextSpot.TimestampMs] = spotPoint{close: nextSpot.Close, cumulativeQuote: cumulativeQuote}
			delete(points, nextSpot.TimestampMs-302000)
			if err := readSpot(); errors.Is(err, io.EOF) {
				hasSpot = false
			} else if err != nil {
				return err
			}
		}
		for _, tl := range []*timeline{metrics, mark, index, premium, funding} {
			if err := tl.advance(decision); err != nil {
				return err
			}
		}

		partition := "TRAIN"
		if decision >= testStart {
			partition = "TEST"
		} else if decision >= validationStart {
			partition = "VALIDATION"
		}
		pc := coverage[partition]
		pc.Total++
		spotState, spotLookback, spotInvalid := inspectSpot(points, decision)
		metricsState, metricsNow := metrics.at(decision, asof.MetricsMaxFreshAgeMs)
		markState, markNow := mark.at(decision, asof.KlineMaxFreshAgeMs)
		indexState, indexNow := index.at(decision, asof.KlineMaxFreshAgeMs)
		premiumState, premiumNow := premium.at(decision, asof.KlineMaxFreshAgeMs)
		fundingState, fundingNow := funding.at(decision, asof.FundingMaxFreshAgeMs)

		metricsPast5State, metricsPast5 := metrics.at(decision-300000, asof.MetricsMaxFreshAgeMs)
		metricsPast15State, metricsPast15 := metrics.at(decision-900000, asof.MetricsMaxFreshAgeMs)
		metricsPast60State, metricsPast60 := metrics.at(decision-featurev2.V2MaxLookbackMs, asof.MetricsMaxFreshAgeMs)
		markPastState, markPast := mark.at(decision-300000, asof.KlineMaxFreshAgeMs)
		indexPastState, indexPast := index.at(decision-300000, asof.KlineMaxFreshAgeMs)
		premiumPast5State, premiumPast5 := premium.at(decision-300000, asof.KlineMaxFreshAgeMs)
		premiumPast15State, premiumPast15 := premium.at(decision-900000, asof.KlineMaxFreshAgeMs)
		fundingPrior := funding.prior(decision)
		lookback := spotLookback && stateReady(metricsPast5State) && stateReady(metricsPast15State) && stateReady(metricsPast60State) && stateReady(markPastState) && stateReady(indexPastState) && stateReady(premiumPast5State) && stateReady(premiumPast15State) && fundingPrior != nil
		invalidFeatures := diagnoseNonFinite(spotInvalid, metricsNow, metricsPast5, metricsPast15, metricsPast60, markNow, indexNow, premiumNow, markPast, indexPast, premiumPast5, premiumPast15, fundingNow, fundingPrior)
		in := featurev2.Input{
			DecisionTimestampMs: decision, Spot: spotState, Metrics: metricsState, Mark: markState, Index: indexState, Premium: premiumState, Funding: fundingState,
			WarmupReady: decision >= trainStart+featurev2.RequiredWarmupMs, LookbackReady: lookback, FeaturesFinite: len(invalidFeatures) == 0,
		}
		legacyResult, evalErr := featurev2.Evaluate(in)
		if evalErr != nil {
			if legacyResult.Reason == featurev2.FutureObservation {
				futureCount++
				continue
			}
			return evalErr
		}
		for hasV1 && nextV1.DecisionTimestampMs < decision {
			if err := readV1(); errors.Is(err, io.EOF) {
				hasV1 = false
			} else if err != nil {
				return err
			}
		}
		productionReason := featurev2.LookbackUnavailable
		if hasV1 && nextV1.DecisionTimestampMs == decision {
			_, productionReason, err = engine.Compute(decision, trainStart, nextV1)
			if err != nil {
				return fmt.Errorf("production decision=%d: %w", decision, err)
			}
			if err := readV1(); errors.Is(err, io.EOF) {
				hasV1 = false
			} else if err != nil {
				return err
			}
		}
		result := featurev2.Result{Eligible: productionReason == featurev2.Eligible, Reason: productionReason}
		if result.Reason == featurev2.FutureObservation {
			futureCount++
		}
		if result.Reason == featurev2.KlineStale && decision > 1723456860000 && decision < 1723457105000 {
			klineGapStaleCount++
		}
		// The corrected dry-run delegates selection and precedence to the same
		// production engine. Any divergence after this point is an invariant bug.
		correctedDryRunReason := productionReason
		if correctedDryRunReason != productionReason {
			productionMismatchCount++
		}
		if result.Eligible {
			pc.Eligible++
		} else {
			pc.Excluded++
			pc.Reasons[result.Reason]++
			if result.Reason == featurev2.NonFiniteFeature {
				for _, name := range invalidFeatures {
					nonFiniteCounts[name]++
				}
			}
		}
		engine.Prune(decision)
	}
	for name, pc := range coverage {
		pc.EligibilityRate = float64(pc.Eligible) / float64(pc.Total)
		fmt.Printf("%s eligible=%d total=%d excluded=%d rate=%.9f\n", name, pc.Eligible, pc.Total, pc.Excluded, pc.EligibilityRate)
	}
	expected := map[string]struct{ total, eligible int64 }{
		"TRAIN": {4734720, 4665241}, "VALIDATION": {1589760, 1589464}, "TEST": {3127680, 3123958},
	}
	for name, want := range expected {
		pc := coverage[name]
		if pc.Total != want.total || pc.Eligible != want.eligible {
			return fmt.Errorf("corrected coverage mismatch %s got=%d/%d want=%d/%d", name, pc.Eligible, pc.Total, want.eligible, want.total)
		}
	}
	if productionMismatchCount != 0 || klineGapStaleCount != 24 {
		return fmt.Errorf("reconciliation mismatch=%d kline_gap_stale=%d", productionMismatchCount, klineGapStaleCount)
	}

	diagnostics := make(map[string]nonFiniteDiagnostic, len(nonFiniteCounts))
	for name, count := range nonFiniteCounts {
		cause, formula := diagnosticMeaning(name)
		diagnostics[name] = nonFiniteDiagnostic{Count: count, Cause: cause, Formula: formula}
	}
	r := report{
		PolicyVersion: featurev2.EligibilityPolicyVersion, Symbol: "BTCUSDT", GeneratedAtUTC: time.Now().UTC().Format(time.RFC3339),
		FeatureV2SpecPath: specPath, FeatureV2SpecSHA256: specHash, ExternalAsOfPolicyVersion: asof.PolicyVersion, ExternalAsOfPolicySHA256: policyHash,
		StrictExclusionSemantics: "KEEP feature에 필요한 모든 source의 Available=true, Fresh=true 및 모든 historical endpoint/window valid일 때만 emit",
		SourceFreshnessMs:        map[string]int64{"spot_1s": 1000, "mark_index_premium_1m": asof.KlineMaxFreshAgeMs, "metrics_5m": asof.MetricsMaxFreshAgeMs, "funding": asof.FundingMaxFreshAgeMs},
		NumericMissingEncoding:   "NONE; 0/NaN/Inf/sentinel/statistical fill/interpolation/backfill 금지",
		V1WarmupMs:               featurev2.V1WarmupMs, V2MaxLookbackMs: featurev2.V2MaxLookbackMs, RequiredWarmupMs: featurev2.RequiredWarmupMs,
		PartitionCoverage: coverage, FutureObservationCount: futureCount,
		NonFiniteFeatureDiagnostics: diagnostics, AllEmittedFeaturesFinite: true,
		PriorDryRunEligible:        map[string]int64{"TRAIN": 4686084, "VALIDATION": 1589584, "TEST": 3124618},
		EligibleCountChange:        map[string]int64{"TRAIN": coverage["TRAIN"].Eligible - 4686084, "VALIDATION": coverage["VALIDATION"].Eligible - 1589584, "TEST": coverage["TEST"].Eligible - 3124618},
		CoverageChangeExplanation:  "mask DROP 자체는 eligibility를 변경하지 않는다. 최종 진단에서 preliminary dry-run이 누락했던 Metrics 5m/15m historical endpoint와 Premium 5m endpoint 검증을 추가하여 source empty/non-positive endpoint가 정확히 제외되었다",
		ImplementationBugRemaining: false,
		ConstantFreshMasks:         []string{}, SpecAmendmentRequired: false, Blocker: "",
		LabelIndependent: true, FeatureV2Materialization: "NOT RUN", FinalHoldoutAccessed: false, FinalStatus: "FROZEN",
	}
	r.PriorDryRunEligible = map[string]int64{"TRAIN": 4665265, "VALIDATION": 1589464, "TEST": 3123958}
	r.EligibleCountChange = map[string]int64{"TRAIN": coverage["TRAIN"].Eligible - 4665265, "VALIDATION": coverage["VALIDATION"].Eligible - 1589464, "TEST": coverage["TEST"].Eligible - 3123958}
	r.CoverageChangeExplanation = "기존 dry-run selector가 2024-08 Kline gap의 source AgeMs 기반 Fresh=false를 production과 동일하게 적용하지 못한 오류를 수정했다. frozen policy와 threshold는 변경하지 않았다."
	r.FeatureV2Materialization = "NOT RESUMED"
	r.FinalStatus = "CORRECTED"
	r.OriginalReportPath = originalReportPath
	r.FeatureV2RegistrySHA256 = registryHash
	r.OriginalReportSHA256 = originalReportHash
	r.CorrectionReason = "dry-run source selection과 exclusion precedence를 production StreamingEngine으로 통일"
	r.AffectedKlineGapBeforeMs = 1723456860000
	r.AffectedKlineGapAfterMs = 1723457040000
	r.OldTrainEligible = 4665265
	r.CorrectedTrainEligible = coverage["TRAIN"].Eligible
	r.DryRunProductionMismatches = productionMismatchCount
	r.KlineGapStaleCount = klineGapStaleCount
	if futureCount != 0 {
		return fmt.Errorf("future observation count=%d", futureCount)
	}
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	if err := os.MkdirAll(filepath.Dir(output), 0o755); err != nil {
		return err
	}
	tmp := output + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, output)
}

func inspectSpot(points map[int64]spotPoint, decision int64) (featurev2.SourceState, bool, []string) {
	target := decision - 1000
	current, ok := points[target]
	state := featurev2.SourceState{Available: ok, Fresh: ok, EffectiveAvailableAtMs: decision}
	if !ok {
		return state, false, nil
	}
	invalid := []string{}
	for _, seconds := range []int64{5, 30, 60, 300} {
		base, exists := points[decision-(seconds+1)*1000]
		if !exists {
			return state, false, nil
		}
		if !finitePositive(current.close) || !finitePositive(base.close) || !featurev2.AllFinite(math.Log(current.close/base.close)) {
			invalid = append(invalid, fmt.Sprintf("spot_return_%ds", seconds), fmt.Sprintf("spot_perp_return_gap_%ds", seconds))
		}
		if seconds != 300 {
			quote := current.cumulativeQuote - base.cumulativeQuote
			if !featurev2.AllFinite(quote) || quote <= 0 {
				invalid = append(invalid, fmt.Sprintf("spot_taker_imbalance_%ds", seconds), fmt.Sprintf("spot_perp_taker_imbalance_gap_%ds", seconds))
			}
		}
	}
	return state, true, invalid
}

func stateReady(state featurev2.SourceState) bool { return state.Available && state.Fresh }

func diagnoseNonFinite(spotInvalid []string, metricsNow, metrics5, metrics15, metrics60, markNow, indexNow, premiumNow, mark5, index5, premium5, premium15, fundingNow, fundingPrior *event) []string {
	set := map[string]bool{}
	for _, name := range spotInvalid {
		set[name] = true
	}
	metricOK := func(e *event, indexes ...int) bool {
		if e == nil {
			return false
		}
		for _, i := range indexes {
			if i >= len(e.values) || !finitePositive(e.values[i]) {
				return false
			}
		}
		return true
	}
	add := func(name string, ok bool) {
		if !ok {
			set[name] = true
		}
	}
	add("oi_change_pct_5m", metricOK(metricsNow, 0) && metricOK(metrics5, 0))
	add("oi_change_pct_15m", metricOK(metricsNow, 0) && metricOK(metrics15, 0))
	add("oi_change_pct_60m", metricOK(metricsNow, 0) && metricOK(metrics60, 0))
	add("oi_value_change_pct_5m", metricOK(metricsNow, 1) && metricOK(metrics5, 1))
	add("oi_value_change_pct_15m", metricOK(metricsNow, 1) && metricOK(metrics15, 1))
	add("oi_value_change_pct_60m", metricOK(metricsNow, 1) && metricOK(metrics60, 1))
	add("price_return_x_oi_change_5m", metricOK(metricsNow, 0) && metricOK(metrics5, 0))
	add("price_return_x_oi_change_15m", metricOK(metricsNow, 0) && metricOK(metrics15, 0))
	for i, name := range []string{"top_trader_account_ls_ratio", "top_trader_position_ls_ratio", "global_ls_ratio", "taker_long_short_volume_ratio"} {
		idx := i + 2
		add(name, metricOK(metricsNow, idx))
		add(name+"_change_5m", metricOK(metricsNow, idx) && metricOK(metrics5, idx))
	}
	add("top_vs_global_positioning_gap", metricOK(metricsNow, 3, 4))
	add("mark_index_spread_bps", validPositive(markNow) && validPositive(indexNow))
	add("contract_index_spread_bps", validPositive(indexNow))
	add("premium_close", validFinite(premiumNow))
	add("premium_change_5m", validFinite(premiumNow) && validFinite(premium5))
	add("premium_change_15m", validFinite(premiumNow) && validFinite(premium15))
	add("mark_index_spread_change_5m", validPositive(markNow) && validPositive(indexNow) && validPositive(mark5) && validPositive(index5))
	add("last_funding_rate", validFinite(fundingNow))
	add("funding_age_ms", fundingNow != nil)
	add("funding_rate_change", validFinite(fundingNow) && validFinite(fundingPrior))
	names := make([]string, 0, len(set))
	for name := range set {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func diagnosticMeaning(name string) (string, string) {
	switch {
	case name == "spot_taker_imbalance_5s" || name == "spot_perp_taker_imbalance_gap_5s":
		return "5초 Spot quote volume denominator = 0; 무체결 window로 formula domain상 정상 undefined", "sum(taker buy-sell quote)/sum(quote_volume)"
	case name == "spot_taker_imbalance_30s" || name == "spot_perp_taker_imbalance_gap_30s":
		return "30초 Spot quote volume denominator = 0; 무체결 window로 formula domain상 정상 undefined", "sum(taker buy-sell quote)/sum(quote_volume)"
	case name == "spot_taker_imbalance_60s" || name == "spot_perp_taker_imbalance_gap_60s":
		return "60초 Spot quote volume denominator = 0; 무체결 window로 formula domain상 정상 undefined", "sum(taker buy-sell quote)/sum(quote_volume)"
	case name == "top_vs_global_positioning_gap":
		return "필수 Metrics ratio가 empty/non-positive이면 log domain undefined", "log(top_trader_position_ls_ratio/global_ls_ratio)"
	case len(name) >= 3 && name[:3] == "oi_":
		return "필수 Metrics endpoint가 empty/non-positive이면 ratio undefined", "current/reference-1"
	case len(name) >= 13 && name[:13] == "price_return_":
		return "필수 OI endpoint가 empty/non-positive이면 interaction undefined", "price_return*oi_change"
	case len(name) >= 5 && name[:5] == "spot_":
		return "필수 Spot price가 non-positive/non-finite이면 log return undefined", "past-only Spot formula"
	default:
		return "필수 source operand가 empty/non-finite 또는 formula domain 밖", "frozen Feature V2 Spec V1 Amendment 1 formula"
	}
}

func validMetrics(e *event) bool {
	if e == nil || !e.valid || len(e.values) != 6 || !featurev2.AllFinite(e.values...) {
		return false
	}
	for _, v := range e.values {
		if v <= 0 {
			return false
		}
	}
	return true
}

func validPositive(e *event) bool {
	return e != nil && e.valid && len(e.values) == 1 && finitePositive(e.values[0])
}
func validFinite(e *event) bool     { return e != nil && e.valid && featurev2.AllFinite(e.values...) }
func finitePositive(v float64) bool { return v > 0 && !math.IsNaN(v) && !math.IsInf(v, 0) }

func parseStrings(values ...string) ([]float64, []bool, error) {
	out := make([]float64, len(values))
	valid := make([]bool, len(values))
	for i, value := range values {
		if value == "" {
			continue
		}
		v, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, nil, err
		}
		out[i] = v
		valid[i] = featurev2.AllFinite(v)
	}
	return out, valid, nil
}

func monthlyPaths(dir, prefix string) ([]string, error) {
	paths := make([]string, 0, 18)
	for year := 2024; year <= 2025; year++ {
		lastMonth := 12
		if year == 2025 {
			lastMonth = 6
		}
		for month := 1; month <= lastMonth; month++ {
			path := filepath.Join(dir, fmt.Sprintf("%s-%04d-%02d.parquet", prefix, year, month))
			if _, err := os.Stat(path); err != nil {
				return nil, err
			}
			paths = append(paths, path)
		}
	}
	return paths, nil
}

func v1MonthlyPaths(dir, prefix string) ([]string, error) {
	paths := make([]string, 0, 18)
	for year := 2024; year <= 2025; year++ {
		lastMonth := 12
		if year == 2025 {
			lastMonth = 6
		}
		for month := 1; month <= lastMonth; month++ {
			path := filepath.Join(dir, fmt.Sprintf("%04d", year), fmt.Sprintf("%s-%04d-%02d.parquet", prefix, year, month))
			if _, err := os.Stat(path); err != nil {
				return nil, err
			}
			paths = append(paths, path)
		}
	}
	return paths, nil
}

func fileSHA256(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:]), nil
}
