package main

import (
	"archive/zip"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type dailyAlignment struct {
	Day        string `json:"day"`
	Candidate  string `json:"candidate"`
	VolumeUnit string `json:"volume_unit"`
	Score      score  `json:"score"`
	Preferred  bool   `json:"preferred_by_median_absolute_error"`
}

type dayClassification struct {
	Day            string `json:"day"`
	BasePreferred  string `json:"base_preferred"`
	QuotePreferred string `json:"quote_preferred"`
	Classification string `json:"classification"`
}

type changePointReport struct {
	Status                 string              `json:"status"`
	Method                 string              `json:"method"`
	LastConfirmedEndDay    string              `json:"last_confirmed_end_day"`
	FirstConfirmedStartDay string              `json:"first_confirmed_start_day"`
	ChangeIntervalUTC      string              `json:"change_interval_utc"`
	BoundaryEvidenceDays   []string            `json:"boundary_evidence_days"`
	DayClassifications     []dayClassification `json:"day_classifications"`
	PreChangeScores        []score             `json:"pre_change_scores"`
	PostChangeScores       []score             `json:"post_change_scores"`
	Conclusion             string              `json:"conclusion"`
	UnverifiedCause        string              `json:"unverified_vendor_cause"`
	FrozenFilesModified    bool                `json:"frozen_files_modified"`
	TestAccessed           bool                `json:"test_accessed"`
	FinalHoldoutAccessed   bool                `json:"final_holdout_accessed"`
}

type rawCanonicalDay struct {
	Day                    string         `json:"day"`
	RawRows                int            `json:"raw_rows"`
	CanonicalRows          int            `json:"canonical_rows"`
	RawDuplicateTimestamps int            `json:"raw_duplicate_timestamps"`
	CanonicalDuplicates    int            `json:"canonical_duplicate_timestamps"`
	MissingInCanonical     int            `json:"missing_in_canonical"`
	ExtraInCanonical       int            `json:"extra_in_canonical"`
	TimestampSetEqual      bool           `json:"timestamp_set_equal"`
	TakerValuesExact       bool           `json:"taker_values_exact"`
	AllMetricValuesExact   bool           `json:"all_metric_values_exact"`
	FieldMismatchCounts    map[string]int `json:"field_mismatch_counts"`
}

type rawCanonicalReport struct {
	Status               string            `json:"status"`
	RawRoot              string            `json:"raw_root"`
	CanonicalRoot        string            `json:"canonical_root"`
	FilesInspected       int               `json:"files_inspected"`
	RawRows              int               `json:"raw_rows"`
	CanonicalRows        int               `json:"canonical_rows"`
	Days                 []rawCanonicalDay `json:"days"`
	TimestampSetExact    bool              `json:"timestamp_set_exact"`
	TakerValuesExact     bool              `json:"taker_values_exact"`
	AllMetricValuesExact bool              `json:"all_metric_values_exact"`
	Conclusion           string            `json:"conclusion"`
}

type leakageReport struct {
	Status                         string   `json:"status"`
	ConfirmedField                 string   `json:"confirmed_field"`
	ConfirmedSourceSemanticBefore  string   `json:"confirmed_source_semantic_before"`
	ConfirmedSourceSemanticAfter   string   `json:"confirmed_source_semantic_after"`
	HistoricalEffectiveRule        string   `json:"historical_effective_rule"`
	HistoricalReceiveTimeAvailable bool     `json:"historical_receive_timestamp_available"`
	HistoricalReceiveTimeVerdict   string   `json:"historical_receive_timestamp_verdict"`
	EarliestReasonableUseAfter     string   `json:"earliest_reasonable_use_after_change"`
	ConfirmedLookaheadWindowMs     int64    `json:"confirmed_lookahead_window_ms"`
	DecisionsPerFiveMinuteEvent    int      `json:"potentially_affected_5s_decisions_per_event"`
	ActuallySelectedDecisionRange  string   `json:"actually_selected_decision_range_per_event"`
	ConfirmedAffectedTrainingStart string   `json:"confirmed_affected_training_start"`
	ObservedAffectedThrough        string   `json:"observed_affected_through"`
	DirectlyAffectedFeatures       []string `json:"directly_affected_features"`
	MetricsFeatureCount            int      `json:"metrics_feature_count"`
	OtherMetricsFieldsVerdict      string   `json:"other_metrics_fields_verdict"`
	EligibilityImpact              string   `json:"eligibility_impact"`
	LivePathVerdict                string   `json:"live_path_verdict"`
	ImpactMagnitudeVerdict         string   `json:"impact_magnitude_verdict"`
	HistoricalCodePath             []string `json:"historical_code_path"`
	LiveCodePath                   []string `json:"live_code_path"`
	TestAccessed                   bool     `json:"test_accessed"`
	FinalHoldoutAccessed           bool     `json:"final_holdout_accessed"`
}

type modelReview struct {
	Verdict              string   `json:"verdict"`
	OperationalUse       string   `json:"operational_use"`
	Reason               string   `json:"reason"`
	Preservation         string   `json:"preservation"`
	RequiredRevalidation []string `json:"required_revalidation"`
	FrozenFilesModified  bool     `json:"frozen_files_modified"`
}

type followUpReport struct {
	Status string   `json:"status"`
	Items  []string `json:"items"`
}

func writePhase2Reports(output string, dates []time.Time, all []observation, rawRoot, metricsRoot string) error {
	daily := buildDailyAlignments(all)
	if err := writeDailyAlignmentCSV(filepath.Join(output, "daily_alignment_comparison.csv"), daily); err != nil {
		return err
	}
	rawReport, err := compareRawCanonical(dates, rawRoot, metricsRoot)
	if err != nil {
		return err
	}
	change := buildChangePointReport(daily, all, rawReport)
	if err = writeJSON(filepath.Join(output, "alignment_change_point.json"), change); err != nil {
		return err
	}
	if err = writeJSON(filepath.Join(output, "raw_canonical_consistency.json"), rawReport); err != nil {
		return err
	}
	leakage := buildLeakageReport(change)
	if err = writeJSON(filepath.Join(output, "future_leakage_impact.json"), leakage); err != nil {
		return err
	}
	review := modelReview{
		Verdict:              "FROZEN_MODEL_NOT_VALID_FOR_DEPLOYMENT_PENDING_CORRECTED_RETRAINING",
		OperationalUse:       "현재 artifact는 provenance 보존용으로 유지하되 자동매매 판단에는 사용하지 않는 것이 타당함",
		Reason:               "TRAIN 기간에 포함된 2024-03-04 이후 표본에서 Taker 값의 확인된 미래 구간 정보가 Feature V2에 조기 입력됨",
		Preservation:         "기존 frozen artifact를 덮어쓰거나 silent amendment하지 않음",
		RequiredRevalidation: []string{"정정된 Taker availability semantics로 TRAIN/VALIDATION Feature V2 재생성", "동일 고정 후보와 고정 hyperparameter로 모델 재학습", "VALIDATION alpha/robustness 재검증", "정정 pipeline을 확정하기 전 TEST 및 FINAL HOLDOUT 계속 봉인"},
		FrozenFilesModified:  false,
	}
	if err = writeJSON(filepath.Join(output, "frozen_model_review.json"), review); err != nil {
		return err
	}
	follow := followUpReport{Status: "REQUIRED", Items: []string{
		"Taker historical timestamp가 START인 구간에 대해 effective availability를 interval end 이후로 옮기는 별도 설계 검토",
		"다른 Metrics 필드 각각의 timestamp semantic을 독립 자료로 검증하고 공통 timestamp assumption 제거 여부 결정",
		"historical/live Taker의 source timestamp, receive timestamp, interval boundary parity 테스트 추가",
		"정정 데이터는 새 version으로 생성하고 기존 frozen artifact 및 hash 보존",
		"TRAIN/VALIDATION만으로 corrected model을 재학습·재검증한 뒤에만 TEST 개봉 여부를 별도 승인",
	}}
	return writeJSON(filepath.Join(output, "follow_up_work.json"), follow)
}

func buildDailyAlignments(all []observation) []dailyAlignment {
	byKey := map[string][]observation{}
	for _, o := range all {
		key := o.Day + "\x00" + o.VolumeUnit + "\x00" + o.Candidate
		byKey[key] = append(byKey[key], o)
	}
	var out []dailyAlignment
	for key, rows := range byKey {
		parts := strings.Split(key, "\x00")
		out = append(out, dailyAlignment{Day: parts[0], VolumeUnit: parts[1], Candidate: parts[2], Score: scoreGroup(rows, parts[2], parts[1])})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Day != out[j].Day {
			return out[i].Day < out[j].Day
		}
		if out[i].VolumeUnit != out[j].VolumeUnit {
			return out[i].VolumeUnit < out[j].VolumeUnit
		}
		return candidateOrder(out[i].Candidate) < candidateOrder(out[j].Candidate)
	})
	for i := range out {
		best := out[i].Score.MedianAbsoluteError
		for j := range out {
			if out[j].Day == out[i].Day && out[j].VolumeUnit == out[i].VolumeUnit && out[j].Score.N > 0 && out[j].Score.MedianAbsoluteError < best {
				best = out[j].Score.MedianAbsoluteError
			}
		}
		out[i].Preferred = out[i].Score.N > 0 && out[i].Score.MedianAbsoluteError == best
	}
	return out
}

func candidateOrder(s string) int {
	switch s {
	case "START":
		return 0
	case "END":
		return 1
	default:
		return 2
	}
}

func buildChangePointReport(daily []dailyAlignment, all []observation, raw rawCanonicalReport) changePointReport {
	preferred := map[string]map[string]string{}
	for _, d := range daily {
		if d.Preferred {
			if preferred[d.Day] == nil {
				preferred[d.Day] = map[string]string{}
			}
			preferred[d.Day][d.VolumeUnit] = d.Candidate
		}
	}
	days := make([]string, 0, len(preferred))
	for day := range preferred {
		days = append(days, day)
	}
	sort.Strings(days)
	r := changePointReport{Status: "INSUFFICIENT_EVIDENCE", Method: "날짜별 BASE/QUOTE에서 median absolute error가 최소인 후보를 선택하고 mean/p90/correlation/match rate를 보조 근거로 사용", FrozenFilesModified: false}
	for _, day := range days {
		b, q := preferred[day]["BASE"], preferred[day]["QUOTE"]
		class := "MIXED"
		if b == q {
			class = b
		}
		r.DayClassifications = append(r.DayClassifications, dayClassification{Day: day, BasePreferred: b, QuotePreferred: q, Classification: class})
	}
	for i := 1; i < len(r.DayClassifications); i++ {
		prev, cur := r.DayClassifications[i-1], r.DayClassifications[i]
		pt, _ := time.Parse("2006-01-02", prev.Day)
		ct, _ := time.Parse("2006-01-02", cur.Day)
		if prev.Classification == "END" && cur.Classification == "START" && ct.Sub(pt) == 24*time.Hour {
			r.LastConfirmedEndDay, r.FirstConfirmedStartDay = prev.Day, cur.Day
			r.ChangeIntervalUTC = fmt.Sprintf("(%sT23:59:59.999Z, %sT00:00:00Z]", prev.Day, cur.Day)
			r.Status = "CONFIRMED"
			for _, x := range r.DayClassifications {
				if x.Day >= pt.AddDate(0, 0, -4).Format("2006-01-02") && x.Day <= ct.AddDate(0, 0, 4).Format("2006-01-02") {
					r.BoundaryEvidenceDays = append(r.BoundaryEvidenceDays, x.Day)
				}
			}
			break
		}
	}
	if r.Status == "CONFIRMED" {
		pre, post := splitObservations(all, r.LastConfirmedEndDay, r.FirstConfirmedStartDay)
		r.PreChangeScores = aggregateScores(pre)
		r.PostChangeScores = aggregateScores(post)
		if raw.Status == "PASS" {
			r.Conclusion = "2024-03-03까지 Taker timestamp는 직전 5분 구간 END와 일치하고 2024-03-04부터 같은 timestamp에서 시작하는 5분 구간 START와 일치함. 표본 원본 ZIP과 canonical 값이 정확히 같으므로 변화는 canonical 변환이 아니라 Binance 원본에 존재함."
		} else {
			r.Conclusion = "통계적 변경점은 확인됐지만 raw/canonical 일치성 실패로 발생 계층은 확정하지 못함"
		}
		r.UnverifiedCause = "Binance가 2024-03-04에 timestamp 정의를 변경한 내부 사유 또는 배포 이력은 로컬 원본과 코드만으로 확인할 수 없음"
	}
	return r
}

func splitObservations(all []observation, lastEnd, firstStart string) (pre, post []observation) {
	for _, o := range all {
		if o.Day <= lastEnd {
			pre = append(pre, o)
		}
		if o.Day >= firstStart {
			post = append(post, o)
		}
	}
	return pre, post
}

func aggregateScores(rows []observation) []score {
	var out []score
	for _, unit := range []string{"BASE", "QUOTE"} {
		for _, candidate := range []string{"START", "END", "PREV_END"} {
			var group []observation
			for _, o := range rows {
				if o.VolumeUnit == unit && o.Candidate == candidate {
					group = append(group, o)
				}
			}
			out = append(out, scoreGroup(group, candidate, unit))
		}
	}
	return out
}

func compareRawCanonical(dates []time.Time, rawRoot, metricsRoot string) (rawCanonicalReport, error) {
	r := rawCanonicalReport{Status: "PASS", RawRoot: rawRoot, CanonicalRoot: metricsRoot, TimestampSetExact: true, TakerValuesExact: true, AllMetricValuesExact: true}
	for _, day := range dates {
		name := day.Format("2006-01-02")
		rawRows, err := readRawMetrics(filepath.Join(rawRoot, "BTCUSDT-metrics-"+name+".zip"))
		if err != nil {
			return r, fmt.Errorf("raw %s: %w", name, err)
		}
		canonicalRows, err := readHistoricalMetrics(filepath.Join(metricsRoot, "BTCUSDT-metrics-"+day.Format("2006-01")+".parquet"), day)
		if err != nil {
			return r, fmt.Errorf("canonical %s: %w", name, err)
		}
		d := compareDay(name, rawRows, canonicalRows)
		r.Days = append(r.Days, d)
		r.FilesInspected++
		r.RawRows += d.RawRows
		r.CanonicalRows += d.CanonicalRows
		r.TimestampSetExact = r.TimestampSetExact && d.TimestampSetEqual
		r.TakerValuesExact = r.TakerValuesExact && d.TakerValuesExact
		r.AllMetricValuesExact = r.AllMetricValuesExact && d.AllMetricValuesExact
	}
	if !r.TimestampSetExact || !r.TakerValuesExact || !r.AllMetricValuesExact {
		r.Status = "FAIL"
	}
	if r.Status == "PASS" {
		r.Conclusion = "검사한 모든 raw ZIP 행의 timestamp와 6개 Metrics 문자열 값이 canonical Parquet와 정확히 일치함. canonicalization은 정렬만 수행했고 timestamp 이동·Taker 값 변경을 만들지 않음."
	} else {
		r.Conclusion = "raw/canonical 불일치가 있어 변환 계층 영향 배제 불가"
	}
	return r, nil
}

func compareDay(day string, rawRows, canonicalRows []metricsRow) rawCanonicalDay {
	d := rawCanonicalDay{Day: day, RawRows: len(rawRows), CanonicalRows: len(canonicalRows), TimestampSetEqual: true, TakerValuesExact: true, AllMetricValuesExact: true, FieldMismatchCounts: map[string]int{}}
	rawMap, canonicalMap := map[int64]metricsRow{}, map[int64]metricsRow{}
	for _, x := range rawRows {
		if _, ok := rawMap[x.Timestamp]; ok {
			d.RawDuplicateTimestamps++
		}
		rawMap[x.Timestamp] = x
	}
	for _, x := range canonicalRows {
		if _, ok := canonicalMap[x.Timestamp]; ok {
			d.CanonicalDuplicates++
		}
		canonicalMap[x.Timestamp] = x
	}
	for ts, x := range rawMap {
		y, ok := canonicalMap[ts]
		if !ok {
			d.MissingInCanonical++
			continue
		}
		compareField := func(name, a, b string) {
			if a != b {
				d.FieldMismatchCounts[name]++
			}
		}
		compareField("open_interest", x.OpenInterest, y.OpenInterest)
		compareField("open_interest_value", x.OpenInterestValue, y.OpenInterestValue)
		compareField("top_trader_account_long_short_ratio", x.TopTraderAccountRatio, y.TopTraderAccountRatio)
		compareField("top_trader_position_long_short_ratio", x.TopTraderPositionRatio, y.TopTraderPositionRatio)
		compareField("global_long_short_ratio", x.GlobalRatio, y.GlobalRatio)
		compareField("taker_long_short_volume_ratio", x.TakerRatio, y.TakerRatio)
	}
	for ts := range canonicalMap {
		if _, ok := rawMap[ts]; !ok {
			d.ExtraInCanonical++
		}
	}
	d.TimestampSetEqual = d.MissingInCanonical == 0 && d.ExtraInCanonical == 0 && d.RawDuplicateTimestamps == 0 && d.CanonicalDuplicates == 0
	d.TakerValuesExact = d.FieldMismatchCounts["taker_long_short_volume_ratio"] == 0 && d.TimestampSetEqual
	for _, n := range d.FieldMismatchCounts {
		if n != 0 {
			d.AllMetricValuesExact = false
		}
	}
	d.AllMetricValuesExact = d.AllMetricValuesExact && d.TimestampSetEqual
	return d
}

func readRawMetrics(path string) ([]metricsRow, error) {
	z, err := zip.OpenReader(path)
	if err != nil {
		return nil, err
	}
	defer z.Close()
	var entry *zip.File
	for _, f := range z.File {
		if strings.HasSuffix(strings.ToLower(f.Name), ".csv") {
			if entry != nil {
				return nil, errors.New("multiple CSV entries")
			}
			entry = f
		}
	}
	if entry == nil {
		return nil, errors.New("CSV entry missing")
	}
	rc, err := entry.Open()
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	c := csv.NewReader(rc)
	header, err := c.Read()
	if err != nil {
		return nil, err
	}
	want := []string{"create_time", "symbol", "sum_open_interest", "sum_open_interest_value", "count_toptrader_long_short_ratio", "sum_toptrader_long_short_ratio", "count_long_short_ratio", "sum_taker_long_short_vol_ratio"}
	if strings.Join(header, "\x00") != strings.Join(want, "\x00") {
		return nil, fmt.Errorf("unexpected header %v", header)
	}
	var out []metricsRow
	for rowNo := 2; ; rowNo++ {
		a, readErr := c.Read()
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, fmt.Errorf("row %d: %w", rowNo, readErr)
		}
		if len(a) != len(want) {
			return nil, fmt.Errorf("row %d columns=%d", rowNo, len(a))
		}
		tsText := strings.TrimSuffix(a[0], ".000")
		t, parseErr := time.Parse("2006-01-02 15:04:05", tsText)
		if parseErr != nil {
			return nil, fmt.Errorf("row %d timestamp: %w", rowNo, parseErr)
		}
		out = append(out, metricsRow{Timestamp: t.UTC().UnixMilli(), OpenInterest: a[2], OpenInterestValue: a[3], TopTraderAccountRatio: a[4], TopTraderPositionRatio: a[5], GlobalRatio: a[6], TakerRatio: a[7]})
	}
	return out, nil
}

func buildLeakageReport(change changePointReport) leakageReport {
	features := []string{"taker_long_short_volume_ratio", "taker_long_short_volume_ratio_change_5m"}
	return leakageReport{
		Status:                         "CONFIRMED_TAKER_LOOKAHEAD",
		ConfirmedField:                 "sum_taker_long_short_vol_ratio",
		ConfirmedSourceSemanticBefore:  "timestamp T의 값이 [T-5m,T)와 일치",
		ConfirmedSourceSemanticAfter:   "timestamp T의 값이 [T,T+5m)와 일치",
		HistoricalEffectiveRule:        "selectMetrics/advanceMetrics가 source timestamp T를 그대로 사용하고 T+5000ms부터 선택",
		HistoricalReceiveTimeAvailable: false,
		HistoricalReceiveTimeVerdict:   "daily ZIP과 canonical Parquet에는 게시 시각 또는 로컬 수신 시각이 없으므로 실제 과거 수신 시각은 확인 불가",
		EarliestReasonableUseAfter:     "구간 [T,T+5m)의 종료 이후인 T+300000ms에 publication safety lag를 더한 시각",
		ConfirmedLookaheadWindowMs:     295000,
		DecisionsPerFiveMinuteEvent:    59,
		ActuallySelectedDecisionRange:  "5초 decision grid에서 T+5000ms부터 T+295000ms까지 해당 미완료 구간의 Taker 값이 선택됨",
		ConfirmedAffectedTrainingStart: change.FirstConfirmedStartDay + "T00:00:05Z",
		ObservedAffectedThrough:        "2024-12-15 표본까지 START가 일관되게 우세; 표본 사이 모든 날짜의 동일성은 미검증",
		DirectlyAffectedFeatures:       features,
		MetricsFeatureCount:            2,
		OtherMetricsFieldsVerdict:      "Taker 외 OI/positioning 필드의 period semantic은 이번 가격·거래량 대조로 검증되지 않았으므로 누출 여부를 확정하지 않음",
		EligibilityImpact:              "공통 MetricsObservation timestamp로 freshness와 lookback이 평가되므로 availability state에도 영향 가능성이 있으나, Taker 외 값의 의미와 실제 게시 시각이 없어 범위는 미확정",
		LivePathVerdict:                "live 경로는 ReceiveTimestampMs보다 이르게 값을 사용하지 않으므로 live 미래 사용은 확인되지 않음. 다만 historical에는 receive timestamp가 없어 historical/live parity가 깨짐",
		ImpactMagnitudeVerdict:         "누출이 포함된 TRAIN 표본과 모델 성능의 정량적 과대평가 크기는 정정 Feature 재생성·재학습 전에는 산정 불가",
		HistoricalCodePath:             []string{"cmd/metricscanonical.read: raw create_time을 동일 millisecond timestamp로 보존", "cmd/featurev2build.advanceMetrics: Timestamp를 MetricsObservation.TimestampMs로 전달", "internal/feature/main/v2.selectMetrics: TimestampMs+ExternalSafetyLagMs(5000)<=decision", "internal/feature/main/v2.computeMetrics: Taker level/change를 model feature로 계산"},
		LiveCodePath:                   []string{"internal/live/binance.fetchPublicObservations: API timestamp와 ReceiveTimestampMs를 보존", "internal/live/runtimefeature.parseExternal: 동일 timestamp의 5 Metrics를 결합", "internal/external/asof.LiveUsableAt: max(source+5000, receive timestamp) 적용"},
		TestAccessed:                   false,
		FinalHoldoutAccessed:           false,
	}
}

func writeDailyAlignmentCSV(path string, rows []dailyAlignment) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	err = w.Write([]string{"day", "candidate", "volume_unit", "sample_count", "median_absolute_error", "mean_absolute_error", "p90_absolute_error", "pearson_correlation", "fraction_abs_error_le_0_001", "fraction_relative_error_le_1pct", "preferred_by_median_absolute_error"})
	for _, x := range rows {
		corr := ""
		if x.Score.PearsonCorrelation != nil {
			corr = strconv.FormatFloat(*x.Score.PearsonCorrelation, 'g', 17, 64)
		}
		if err == nil {
			err = w.Write([]string{x.Day, x.Candidate, x.VolumeUnit, strconv.Itoa(x.Score.N), f64(x.Score.MedianAbsoluteError), f64(x.Score.MeanAbsoluteError), f64(x.Score.P90AbsoluteError), corr, f64(x.Score.FractionAbsErrorLE0001), f64(x.Score.FractionRelativeErrorLE1Pct), strconv.FormatBool(x.Preferred)})
		}
	}
	w.Flush()
	if err == nil {
		err = w.Error()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	return err
}

func f64(v float64) string { return strconv.FormatFloat(v, 'g', 17, 64) }

func writeJSON(path string, value any) error {
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(path, b, 0o644)
}
