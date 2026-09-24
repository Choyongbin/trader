package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"

	featurev2 "binance_trader/internal/feature/main/v2"
)

const stageCPath = "data/reports/model/main/v2/phase9c-cde/stage-c-nonlinear.json"

type frozenCandidate struct {
	CandidateID             string  `json:"candidate_id"`
	Side                    string  `json:"side"`
	TPBps                   int     `json:"tp_bps"`
	SLBps                   int     `json:"sl_bps"`
	HorizonSeconds          int     `json:"horizon_seconds"`
	SelectedModelType       string  `json:"selected_model_type"`
	ModelArtifactPath       string  `json:"model_artifact_path"`
	ClassificationModelSHA  string  `json:"classification_model_sha256"`
	RegressionModelSHA      string  `json:"regression_model_sha256"`
	PreprocessingSHA        string  `json:"preprocessing_sha256"`
	FeatureRegistryHash     string  `json:"feature_registry_hash"`
	ThresholdPercentile     float64 `json:"threshold_percentile"`
	ClassificationThreshold float64 `json:"classification_numeric_threshold"`
	RegressionThreshold     float64 `json:"regression_numeric_threshold"`
	ValidationScoreRows     int64   `json:"validation_score_rows"`
}

type confirmationCriteria struct {
	MeanReturnPositive     bool  `json:"mean_realized_net_return_ex_funding_positive"`
	MinimumPositiveMonths  int   `json:"minimum_positive_months_of_six"`
	Thinning15mPositive    bool  `json:"thinning_15m_mean_positive"`
	Thinning60mPositive    bool  `json:"thinning_60m_mean_positive_or_warning_if_too_small"`
	MinimumSignalsOverall  int64 `json:"minimum_evaluable_signals_overall"`
	MinimumSignalsPerMonth int64 `json:"minimum_evaluable_signals_per_month"`
	FutureObservationZero  bool  `json:"future_observation_zero"`
}

type eventRules struct {
	PositionMode      string `json:"position_mode"`
	OpenSignalPolicy  string `json:"open_signal_policy"`
	WhileOpenPolicy   string `json:"while_open_policy"`
	ClosePolicy       string `json:"close_policy"`
	NotionalBasis     string `json:"notional_basis"`
	FeeSlippagePolicy string `json:"fee_slippage_policy"`
	FundingPolicy     string `json:"funding_policy"`
}

type stageDReport struct {
	Version                  int                  `json:"version"`
	Stage                    string               `json:"stage"`
	Status                   string               `json:"status"`
	StageCPath               string               `json:"stage_c_path"`
	StageCSHA256             string               `json:"stage_c_sha256"`
	FeatureCount             int                  `json:"feature_count"`
	FeatureRegistryHash      string               `json:"feature_registry_hash"`
	Candidates               []frozenCandidate    `json:"candidates"`
	EntryRule                string               `json:"entry_rule"`
	ConflictRule             string               `json:"candidate_conflict_rule"`
	TieRule                  string               `json:"tie_rule"`
	NoTradeRule              string               `json:"no_trade_rule"`
	ThinningRule             string               `json:"thinning_rule"`
	ConfirmationCriteria     confirmationCriteria `json:"test_confirmation_criteria"`
	EventRules               eventRules           `json:"event_backtest_rules"`
	ProbabilityCalibration   string               `json:"probability_calibration"`
	FrozenAt                 string               `json:"frozen_at"`
	TestAccessedBeforeFreeze bool                 `json:"test_accessed_before_freeze"`
	PolicyFrozen             bool                 `json:"POLICY_FROZEN"`
	PolicyHash               string               `json:"policy_sha256"`
	FinalHoldoutAccessed     bool                 `json:"final_holdout_accessed"`
	Complete                 bool                 `json:"complete"`
}

func runPolicyFreeze(output string) error {
	if complete, err := readCompleteStageD(output); err != nil {
		return err
	} else if complete {
		fmt.Println("STAGE D RESUME PASS POLICY_FROZEN=true")
		return nil
	}
	stageC, stageCHash, err := loadStageC(filepath.FromSlash(stageCPath))
	if err != nil {
		return err
	}
	if !stageC.Complete || stageC.Status != "PASS" || len(stageC.Candidates) != 5 {
		return fmt.Errorf("Stage C is not complete")
	}
	models := make([]baselineArtifact, len(stageC.Candidates))
	classScores := make([][]float64, len(models))
	returnScores := make([][]float64, len(models))
	for i, candidate := range stageC.Candidates {
		if candidate.SelectedFamily != "LOGISTIC_LINEAR" {
			return fmt.Errorf("unsupported selected family %s", candidate.SelectedFamily)
		}
		actualHash, err := fileSHA256(candidate.BaselineArtifact.Path)
		if err != nil || actualHash != candidate.BaselineArtifact.SHA256 {
			return fmt.Errorf("baseline artifact hash mismatch for %s", candidate.CandidateID)
		}
		b, err := os.ReadFile(candidate.BaselineArtifact.Path)
		if err != nil {
			return err
		}
		if err = json.Unmarshal(b, &models[i]); err != nil {
			return err
		}
		if models[i].FeatureCount != featureCount || models[i].FeatureRegistryHash != registryHash {
			return fmt.Errorf("model registry mismatch for %s", candidate.CandidateID)
		}
	}
	if err := visitFeatures(featurePaths(10, 12), func(row featurev2.FeatureRowV2) error {
		values := row.FeatureValues()
		for i, model := range models {
			probability, predictedReturn, err := model.predict(values)
			if err != nil {
				return err
			}
			if !finite(probability) || !finite(predictedReturn) {
				return fmt.Errorf("non-finite validation score")
			}
			classScores[i] = append(classScores[i], probability)
			returnScores[i] = append(returnScores[i], predictedReturn)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("Stage D validation inference: %w", err)
	}
	report := stageDReport{Version: 1, Stage: "D", Status: "PASS", StageCPath: filepath.FromSlash(stageCPath), StageCSHA256: stageCHash,
		FeatureCount: featureCount, FeatureRegistryHash: registryHash, EntryRule: "classification_score >= frozen_top_5pct_threshold AND regression_score >= frozen_top_5pct_threshold",
		ConflictRule: "among passing candidates choose highest predicted net return; at most one candidate per decision; LONG and SHORT cannot both enter",
		TieRule:      "lexicographically ascending candidate_id", NoTradeRule: "NO_TRADE when no candidate passes both frozen thresholds",
		ThinningRule: "UNIX_EPOCH_PLUS_5000MS fixed anchor; 15m and 60m intervals",
		ConfirmationCriteria: confirmationCriteria{MeanReturnPositive: true, MinimumPositiveMonths: 4, Thinning15mPositive: true,
			Thinning60mPositive: true, MinimumSignalsOverall: 1000, MinimumSignalsPerMonth: 100, FutureObservationZero: true},
		EventRules: eventRules{PositionMode: "SINGLE_POSITION_FLAT_OR_OPEN", OpenSignalPolicy: "apply frozen Stage D policy only while FLAT",
			WhileOpenPolicy: "ignore all new entry signals", ClosePolicy: "TP_SL_TIMEOUT first passage from frozen candidate label/barrier semantics",
			NotionalBasis: "1X_NOTIONAL_RETURN", FeeSlippagePolicy: "use label net_return_ex_funding; do not charge twice",
			FundingPolicy: "apply only if sign and event-time semantics can be independently verified; otherwise NOT_APPLIED warning"},
		ProbabilityCalibration: "NONE_RAW_RANKING_SCORE", FrozenAt: time.Now().UTC().Format(time.RFC3339Nano), PolicyFrozen: true, Complete: true}
	for i, candidate := range stageC.Candidates {
		classThreshold := percentileThreshold(classScores[i], .05)
		returnThreshold := percentileThreshold(returnScores[i], .05)
		report.Candidates = append(report.Candidates, frozenCandidate{CandidateID: candidate.CandidateID, Side: candidate.Side,
			TPBps: candidate.TPBps, SLBps: candidate.SLBps, HorizonSeconds: candidate.HorizonSeconds, SelectedModelType: candidate.SelectedFamily,
			ModelArtifactPath: candidate.BaselineArtifact.Path, ClassificationModelSHA: candidate.BaselineArtifact.SHA256,
			RegressionModelSHA: candidate.BaselineArtifact.SHA256, PreprocessingSHA: candidate.BaselineArtifact.PreprocessingHash,
			FeatureRegistryHash: registryHash, ThresholdPercentile: .05, ClassificationThreshold: classThreshold,
			RegressionThreshold: returnThreshold, ValidationScoreRows: int64(len(classScores[i]))})
		fmt.Printf("STAGE D %s threshold class=%.12g regression=%.12g\n", candidate.CandidateID, classThreshold, returnThreshold)
	}
	identity, err := json.Marshal(report)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(identity)
	report.PolicyHash = hex.EncodeToString(sum[:])
	if err = writeJSON(output, report); err != nil {
		return err
	}
	fmt.Printf("PHASE 9C-D PASS POLICY_FROZEN=true hash=%s\n", report.PolicyHash)
	return nil
}

func percentileThreshold(values []float64, fraction float64) float64 {
	copyValues := append([]float64(nil), values...)
	sort.Sort(sort.Reverse(sort.Float64Slice(copyValues)))
	index := int(math.Ceil(float64(len(copyValues))*fraction)) - 1
	return copyValues[index]
}

func finite(value float64) bool { return !math.IsNaN(value) && !math.IsInf(value, 0) }

func loadStageC(path string) (stageCReport, string, error) {
	var report stageCReport
	b, err := os.ReadFile(path)
	if err != nil {
		return report, "", err
	}
	if err = json.Unmarshal(b, &report); err != nil {
		return report, "", err
	}
	sum := sha256.Sum256(b)
	return report, hex.EncodeToString(sum[:]), nil
}

func fileSHA256(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

func readCompleteStageD(path string) (bool, error) {
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var report stageDReport
	if err = json.Unmarshal(b, &report); err != nil {
		return false, err
	}
	if !report.Complete || !report.PolicyFrozen || report.Status != "PASS" || report.TestAccessedBeforeFreeze || len(report.Candidates) != 5 {
		return false, fmt.Errorf("invalid existing Stage D report")
	}
	return true, nil
}
