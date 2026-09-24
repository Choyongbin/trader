package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	featurev2 "binance_trader/internal/feature/main/v2"
	tradespeclabel "binance_trader/internal/label/tradespec"
)

type realizedSignal struct {
	Timestamp         int64
	CandidateID, Side string
	Return            float64
	Positive          bool
	TradeResult       string
	HorizonSeconds    int
}

type signalStats struct {
	Count        int64   `json:"count"`
	Coverage     float64 `json:"coverage"`
	PositiveRate float64 `json:"positive_rate"`
	MeanReturn   float64 `json:"mean_net_return_ex_funding"`
	MedianReturn float64 `json:"median_net_return_ex_funding"`
}

type testCandidateReport struct {
	CandidateID       string                `json:"candidate_id"`
	Side              string                `json:"side"`
	ValidRows         int64                 `json:"valid_rows"`
	Classification    classificationMetrics `json:"classification"`
	Regression        regressionMetrics     `json:"regression"`
	FrozenSignal      signalStats           `json:"frozen_combined_signal"`
	InvalidSignalRows int64                 `json:"invalid_signal_rows"`
}

type policyMonthReport struct {
	Month        string  `json:"month"`
	Signals      int64   `json:"signals"`
	Long         int64   `json:"long"`
	Short        int64   `json:"short"`
	PositiveRate float64 `json:"positive_rate"`
	MeanReturn   float64 `json:"mean_net_return_ex_funding"`
	MedianReturn float64 `json:"median_net_return_ex_funding"`
}

type frozenPolicyResult struct {
	TotalDecisions      int64               `json:"total_decisions"`
	SignalDecisions     int64               `json:"signal_decisions"`
	EvaluableSignals    int64               `json:"evaluable_signals"`
	UnresolvedSignals   int64               `json:"unresolved_signals"`
	NoTrade             int64               `json:"no_trade"`
	Long                int64               `json:"long"`
	Short               int64               `json:"short"`
	CandidateSelections map[string]int64    `json:"candidate_selection_counts"`
	PositiveRate        float64             `json:"positive_rate"`
	MeanReturn          float64             `json:"mean_net_return_ex_funding"`
	MedianReturn        float64             `json:"median_net_return_ex_funding"`
	Monthly             []policyMonthReport `json:"monthly"`
	Thinning15m         signalStats         `json:"thinning_15m"`
	Thinning60m         signalStats         `json:"thinning_60m"`
}

type stageEReport struct {
	Version                   int                   `json:"version"`
	Stage                     string                `json:"stage"`
	Status                    string                `json:"status"`
	PolicyPath                string                `json:"policy_path"`
	PolicyHash                string                `json:"policy_sha256"`
	MaterializationPath       string                `json:"test_materialization_path"`
	Candidates                []testCandidateReport `json:"candidates"`
	FrozenPolicy              frozenPolicyResult    `json:"frozen_policy"`
	PositiveMonths            int                   `json:"positive_months"`
	CriteriaResults           map[string]bool       `json:"confirmation_criteria_results"`
	TestVerdict               string                `json:"test_verdict"`
	ModelChangedAfterTest     bool                  `json:"model_changed_after_test"`
	ThresholdChangedAfterTest bool                  `json:"threshold_changed_after_test"`
	FutureObservationCount    int64                 `json:"future_observation_count"`
	TestFeatureAccessed       bool                  `json:"test_feature_accessed"`
	TestLabelAccessed         bool                  `json:"test_label_accessed"`
	FinalHoldoutAccessed      bool                  `json:"final_holdout_accessed"`
	ElapsedMs                 int64                 `json:"elapsed_ms"`
	Complete                  bool                  `json:"complete"`
}

func runTestConfirmation(output string) error {
	started := time.Now()
	if b, err := os.ReadFile(output); err == nil {
		var existing stageEReport
		if json.Unmarshal(b, &existing) == nil && existing.Complete && existing.Status == "PASS" {
			fmt.Printf("STAGE E RESUME PASS verdict=%s\n", existing.TestVerdict)
			return nil
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	policyPath := filepath.FromSlash("data/reports/model/main/v2/phase9c-cde/stage-d-policy-freeze.json")
	policy, err := loadFrozenPolicy(policyPath)
	if err != nil {
		return err
	}
	materializationPath := filepath.FromSlash("data/reports/model/main/v2/phase9c-cde/stage-test-materialization.json")
	var materialization stageTestMaterialization
	if err = readJSONFile(materializationPath, &materialization); err != nil || !materialization.Complete || materialization.Status != "PASS" {
		return fmt.Errorf("TEST materialization checkpoint is not complete")
	}
	configs := candidates()
	models := make([]baselineArtifact, len(configs))
	for i := range configs {
		if policy.Candidates[i].CandidateID != configs[i].ID || policy.Candidates[i].SelectedModelType != "LOGISTIC_LINEAR" {
			return fmt.Errorf("frozen candidate/model order mismatch")
		}
		if actual, hashErr := fileSHA256(policy.Candidates[i].ModelArtifactPath); hashErr != nil || actual != policy.Candidates[i].ClassificationModelSHA {
			return fmt.Errorf("frozen model hash mismatch %s", configs[i].ID)
		}
		if err = readJSONFile(policy.Candidates[i].ModelArtifactPath, &models[i]); err != nil {
			return err
		}
	}
	cursors := make([]*labelCursor, len(configs))
	for i, c := range configs {
		cursor, cursorErr := newLabelCursor(filepath.Join(c.Root, "test.parquet"))
		if cursorErr != nil {
			return cursorErr
		}
		cursors[i] = cursor
	}
	defer func() {
		for _, cursor := range cursors {
			_ = cursor.close()
		}
	}()
	candidatePredictions := make([][]prediction, len(configs))
	invalidSignals := make([]int64, len(configs))
	realized := make([]realizedSignal, 0, 150000)
	policyResult := frozenPolicyResult{CandidateSelections: map[string]int64{}}
	err = visitFeatures(testFeaturePaths(), func(feature featurev2.FeatureRowV2) error {
		policyResult.TotalDecisions++
		values := feature.FeatureValues()
		var labels [5]tradespeclabel.Row
		var matched [5]bool
		var passing [5]int
		var passingCount int
		var predictedReturns [5]float64
		for i := range configs {
			label, ok, matchErr := cursors[i].match(feature.DecisionTimestampMs)
			if matchErr != nil {
				return matchErr
			}
			labels[i], matched[i] = label, ok
			probability, predictedReturn, predictErr := models[i].predict(values)
			if predictErr != nil {
				return predictErr
			}
			predictedReturns[i] = predictedReturn
			if ok && label.LabelValid {
				candidatePredictions[i] = append(candidatePredictions[i], prediction{Timestamp: feature.DecisionTimestampMs, Probability: probability,
					PredictedReturn: predictedReturn, ActualReturn: label.NetReturnExFunding, Positive: label.NetProfitableExFunding})
			}
			if probability >= policy.Candidates[i].ClassificationThreshold && predictedReturn >= policy.Candidates[i].RegressionThreshold {
				passing[passingCount] = i
				passingCount++
				if !ok || !label.LabelValid {
					invalidSignals[i]++
				}
			}
		}
		if passingCount == 0 {
			policyResult.NoTrade++
			return nil
		}
		selected := passing[0]
		for _, i := range passing[1:passingCount] {
			if predictedReturns[i] > predictedReturns[selected] || (predictedReturns[i] == predictedReturns[selected] && configs[i].ID < configs[selected].ID) {
				selected = i
			}
		}
		policyResult.SignalDecisions++
		policyResult.CandidateSelections[configs[selected].ID]++
		if !matched[selected] || !labels[selected].LabelValid {
			policyResult.UnresolvedSignals++
			return nil
		}
		label := labels[selected]
		realized = append(realized, realizedSignal{Timestamp: feature.DecisionTimestampMs, CandidateID: configs[selected].ID, Side: configs[selected].Side,
			Return: label.NetReturnExFunding, Positive: label.NetProfitableExFunding, TradeResult: string(label.TradeResult), HorizonSeconds: configs[selected].Horizon})
		return nil
	})
	if err != nil {
		return fmt.Errorf("one-shot TEST inference: %w", err)
	}
	report := stageEReport{Version: 1, Stage: "E", Status: "PASS", PolicyPath: policyPath, PolicyHash: policy.PolicyHash,
		MaterializationPath: materializationPath, FutureObservationCount: materialization.FutureObservation,
		TestFeatureAccessed: true, TestLabelAccessed: true, CriteriaResults: map[string]bool{}}
	for i, c := range configs {
		predictions := candidatePredictions[i]
		passing := make([]prediction, 0, len(predictions)/100)
		for _, row := range predictions {
			if row.Probability >= policy.Candidates[i].ClassificationThreshold && row.PredictedReturn >= policy.Candidates[i].RegressionThreshold {
				passing = append(passing, row)
			}
		}
		report.Candidates = append(report.Candidates, testCandidateReport{CandidateID: c.ID, Side: c.Side, ValidRows: int64(len(predictions)),
			Classification: classification(predictions), Regression: regression(predictions), FrozenSignal: predictionSignalStats(passing, int64(len(predictions))),
			InvalidSignalRows: invalidSignals[i]})
		candidatePredictions[i] = nil
	}
	policyResult.EvaluableSignals = int64(len(realized))
	for _, signal := range realized {
		if signal.Side == "LONG" {
			policyResult.Long++
		} else {
			policyResult.Short++
		}
	}
	allStats := realizedStats(realized, policyResult.TotalDecisions)
	policyResult.PositiveRate, policyResult.MeanReturn, policyResult.MedianReturn = allStats.PositiveRate, allStats.MeanReturn, allStats.MedianReturn
	for month := 1; month <= 6; month++ {
		subset := filterRealized(realized, func(s realizedSignal) bool {
			at := time.UnixMilli(s.Timestamp).UTC()
			return at.Year() == 2025 && int(at.Month()) == month
		})
		stats := realizedStats(subset, int64(len(subset)))
		var long, short int64
		for _, signal := range subset {
			if signal.Side == "LONG" {
				long++
			} else {
				short++
			}
		}
		policyResult.Monthly = append(policyResult.Monthly, policyMonthReport{Month: fmt.Sprintf("2025-%02d", month), Signals: int64(len(subset)), Long: long, Short: short,
			PositiveRate: stats.PositiveRate, MeanReturn: stats.MeanReturn, MedianReturn: stats.MedianReturn})
		if stats.MeanReturn > 0 {
			report.PositiveMonths++
		}
	}
	thin15 := filterRealized(realized, func(s realizedSignal) bool { return (s.Timestamp-5000)%(15*time.Minute).Milliseconds() == 0 })
	thin60 := filterRealized(realized, func(s realizedSignal) bool { return (s.Timestamp-5000)%(60*time.Minute).Milliseconds() == 0 })
	policyResult.Thinning15m = realizedStats(thin15, policyResult.TotalDecisions)
	policyResult.Thinning60m = realizedStats(thin60, policyResult.TotalDecisions)
	report.FrozenPolicy = policyResult
	criteria := policy.ConfirmationCriteria
	report.CriteriaResults["mean_return_positive"] = policyResult.MeanReturn > 0
	report.CriteriaResults["minimum_positive_months"] = report.PositiveMonths >= criteria.MinimumPositiveMonths
	report.CriteriaResults["thinning_15m_positive"] = policyResult.Thinning15m.MeanReturn > 0
	report.CriteriaResults["thinning_60m_positive"] = policyResult.Thinning60m.MeanReturn > 0
	report.CriteriaResults["minimum_signals_overall"] = policyResult.EvaluableSignals >= criteria.MinimumSignalsOverall
	monthlyEnough := true
	for _, month := range policyResult.Monthly {
		monthlyEnough = monthlyEnough && month.Signals >= criteria.MinimumSignalsPerMonth
	}
	report.CriteriaResults["minimum_signals_per_month"] = monthlyEnough
	report.CriteriaResults["future_observation_zero"] = report.FutureObservationCount == 0
	allPass := true
	for _, pass := range report.CriteriaResults {
		allPass = allPass && pass
	}
	if allPass {
		report.TestVerdict = "TEST_CONFIRMED"
	} else if policyResult.MeanReturn > 0 && report.PositiveMonths >= 3 {
		report.TestVerdict = "TEST_PARTIAL"
	} else {
		report.TestVerdict = "TEST_FAILED"
	}
	report.ElapsedMs, report.Complete = time.Since(started).Milliseconds(), true
	if err = writeJSON(output, report); err != nil {
		return err
	}
	fmt.Printf("PHASE 9C-E PASS verdict=%s signals=%d mean=%.12g positive_months=%d\n", report.TestVerdict, policyResult.EvaluableSignals, policyResult.MeanReturn, report.PositiveMonths)
	return nil
}

func testFeaturePaths() []string {
	paths := make([]string, 0, 6)
	for month := 1; month <= 6; month++ {
		paths = append(paths, filepath.Join("data", "features", "main", "v2", "BTCUSDT", "2025", fmt.Sprintf("BTCUSDT-main-features-v2-2025-%02d.parquet", month)))
	}
	return paths
}

func loadFrozenPolicy(path string) (stageDReport, error) {
	var policy stageDReport
	if err := readJSONFile(path, &policy); err != nil {
		return policy, err
	}
	if !policy.Complete || !policy.PolicyFrozen || policy.TestAccessedBeforeFreeze || policy.PolicyHash == "" || len(policy.Candidates) != 5 {
		return policy, fmt.Errorf("invalid frozen policy")
	}
	stored := policy.PolicyHash
	policy.PolicyHash = ""
	b, err := json.Marshal(policy)
	if err != nil {
		return policy, err
	}
	sum := fmt.Sprintf("%x", sha256.Sum256(b))
	policy.PolicyHash = stored
	if sum != stored {
		return policy, fmt.Errorf("frozen policy hash mismatch")
	}
	return policy, nil
}

func predictionSignalStats(rows []prediction, universe int64) signalStats {
	returns := make([]float64, len(rows))
	var positive int64
	var sum float64
	for i, row := range rows {
		returns[i] = row.ActualReturn
		sum += row.ActualReturn
		if row.Positive {
			positive++
		}
	}
	if len(rows) == 0 {
		return signalStats{}
	}
	return signalStats{Count: int64(len(rows)), Coverage: float64(len(rows)) / float64(universe), PositiveRate: float64(positive) / float64(len(rows)),
		MeanReturn: sum / float64(len(rows)), MedianReturn: median(returns)}
}

func realizedStats(rows []realizedSignal, universe int64) signalStats {
	returns := make([]float64, len(rows))
	var positive int64
	var sum float64
	for i, row := range rows {
		returns[i] = row.Return
		sum += row.Return
		if row.Positive {
			positive++
		}
	}
	if len(rows) == 0 {
		return signalStats{}
	}
	sort.Float64s(returns)
	middle := len(returns) / 2
	medianValue := returns[middle]
	if len(returns)%2 == 0 {
		medianValue = (returns[middle-1] + medianValue) / 2
	}
	return signalStats{Count: int64(len(rows)), Coverage: float64(len(rows)) / float64(universe), PositiveRate: float64(positive) / float64(len(rows)), MeanReturn: sum / float64(len(rows)), MedianReturn: medianValue}
}

func filterRealized(rows []realizedSignal, keep func(realizedSignal) bool) []realizedSignal {
	result := make([]realizedSignal, 0, len(rows)/4)
	for _, row := range rows {
		if keep(row) {
			result = append(result, row)
		}
	}
	return result
}

func readJSONFile(path string, value any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, value)
}
