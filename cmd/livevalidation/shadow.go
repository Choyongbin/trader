package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"time"

	featurev2 "binance_trader/internal/feature/main/v2"
	"binance_trader/internal/model/logistic"
)

type shadowCandidate struct {
	CandidateID             string  `json:"candidate_id"`
	Side                    string  `json:"side"`
	ModelArtifactPath       string  `json:"model_artifact_path"`
	ClassificationModelSHA  string  `json:"classification_model_sha256"`
	TPBps                   int     `json:"tp_bps"`
	SLBps                   int     `json:"sl_bps"`
	HorizonSeconds          int     `json:"horizon_seconds"`
	ClassificationThreshold float64 `json:"classification_numeric_threshold"`
	RegressionThreshold     float64 `json:"regression_numeric_threshold"`
}
type shadowPolicy struct {
	FeatureRegistryHash string            `json:"feature_registry_hash"`
	PolicyHash          string            `json:"policy_sha256"`
	PolicyFrozen        bool              `json:"POLICY_FROZEN"`
	Complete            bool              `json:"complete"`
	Candidates          []shadowCandidate `json:"candidates"`
}
type shadowModel struct {
	FeatureCount        int             `json:"feature_count"`
	FeatureRegistryHash string          `json:"feature_registry_hash"`
	Scaler              logistic.Scaler `json:"scaler"`
	ClassIntercept      float64         `json:"classification_intercept"`
	ClassWeights        []float64       `json:"classification_weights"`
	RegressionIntercept float64         `json:"regression_intercept_scaled"`
	RegressionWeights   []float64       `json:"regression_weights_scaled"`
	ReturnScale         float64         `json:"return_scale"`
}

func (m shadowModel) predict(x [featurev2.ModelFeatureCountV2]float64) (float64, float64, error) {
	var scaled [featurev2.ModelFeatureCountV2]float64
	if e := m.Scaler.TransformInto(scaled[:], x[:]); e != nil {
		return 0, 0, e
	}
	c, r := m.ClassIntercept, m.RegressionIntercept
	for i, v := range scaled {
		c += m.ClassWeights[i] * v
		r += m.RegressionWeights[i] * v
	}
	return logistic.Sigmoid(c), r / m.ReturnScale, nil
}
func sha(path string) (string, error) {
	b, e := os.ReadFile(path)
	if e != nil {
		return "", e
	}
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:]), nil
}

type shadowMetrics struct {
	DurationMs             int64   `json:"duration_ms"`
	Decisions              int     `json:"decisions"`
	Eligible               int     `json:"eligible"`
	Excluded               int     `json:"excluded"`
	NoTrade                int     `json:"NO_TRADE"`
	Signals                int     `json:"signals"`
	PaperEntries           int     `json:"paper_entries"`
	PaperExits             int     `json:"paper_exits"`
	Long                   int     `json:"LONG"`
	Short                  int     `json:"SHORT"`
	FutureObservation      int     `json:"future_observation"`
	Errors                 int     `json:"errors"`
	FeatureLatencyMeanUs   float64 `json:"feature_latency_mean_us"`
	InferenceLatencyMeanUs float64 `json:"inference_latency_mean_us"`
	PolicyLatencyMeanUs    float64 `json:"policy_latency_mean_us"`
	OpenPositionAtEnd      bool    `json:"open_position_at_end"`
	ModelEvaluations       int     `json:"model_evaluations"`
	PaperIntents           int     `json:"paper_intents"`
	BlockedEvents          int     `json:"blocked_events"`
}

type liveShadowEvaluator struct {
	policy       shadowPolicy
	models       []shadowModel
	metrics      shadowMetrics
	open         bool
	exitDeadline int64
}

func newLiveShadowEvaluator() (*liveShadowEvaluator, error) {
	policy, models, err := loadShadowInputs()
	if err != nil {
		return nil, err
	}
	return &liveShadowEvaluator{policy: policy, models: models}, nil
}

func (e *liveShadowEvaluator) Decide(snapshot featurev2.Snapshot, reason featurev2.Reason, latencyUs, _ float64) {
	e.metrics.Decisions++
	e.metrics.FeatureLatencyMeanUs += latencyUs
	if e.open && snapshot.DecisionTimestampMs >= e.exitDeadline {
		e.open = false
		e.metrics.PaperExits++
	}
	if reason != featurev2.Eligible {
		e.metrics.Excluded++
		e.metrics.BlockedEvents++
		if reason == featurev2.FutureObservation {
			e.metrics.FutureObservation++
		}
		return
	}
	e.metrics.Eligible++
	start := time.Now()
	selected := -1
	best := math.Inf(-1)
	for i, model := range e.models {
		probability, prediction, err := model.predict(snapshot.Values)
		e.metrics.ModelEvaluations++
		if err != nil {
			e.metrics.Errors++
			return
		}
		candidate := e.policy.Candidates[i]
		if probability >= candidate.ClassificationThreshold && prediction >= candidate.RegressionThreshold && (prediction > best || (prediction == best && (selected < 0 || candidate.CandidateID < e.policy.Candidates[selected].CandidateID))) {
			selected, best = i, prediction
		}
	}
	e.metrics.InferenceLatencyMeanUs += float64(time.Since(start).Microseconds())
	if selected < 0 || e.open {
		e.metrics.NoTrade++
		return
	}
	candidate := e.policy.Candidates[selected]
	if candidate.Side == "LONG" {
		e.metrics.Long++
	} else {
		e.metrics.Short++
	}
	e.metrics.Signals++
	e.metrics.PaperEntries++
	e.metrics.PaperIntents++
	e.open = true
	e.exitDeadline = snapshot.DecisionTimestampMs + int64(candidate.HorizonSeconds)*1000
}

func (e *liveShadowEvaluator) Result(duration time.Duration) shadowMetrics {
	result := e.metrics
	result.DurationMs = duration.Milliseconds()
	result.OpenPositionAtEnd = e.open
	if result.Decisions > 0 {
		result.FeatureLatencyMeanUs /= float64(result.Decisions)
	}
	if result.Eligible > 0 {
		result.InferenceLatencyMeanUs /= float64(result.Eligible)
	}
	return result
}

func loadShadowInputs() (shadowPolicy, []shadowModel, error) {
	var policy shadowPolicy
	b, e := os.ReadFile("data/reports/model/main/v2/phase9c-cde/stage-d-policy-freeze.json")
	if e != nil {
		return shadowPolicy{}, nil, e
	}
	if e = json.Unmarshal(b, &policy); e != nil {
		return shadowPolicy{}, nil, e
	}
	if !policy.Complete || !policy.PolicyFrozen || policy.PolicyHash != entryPolicyHash || policy.FeatureRegistryHash != featureRegistryHash {
		return shadowPolicy{}, nil, fmt.Errorf("frozen entry policy identity mismatch")
	}
	var risk struct {
		Hash string `json:"risk_policy_sha256"`
	}
	b, e = os.ReadFile("data/reports/production/v1/BTCUSDT/risk-policy-v1.json")
	if e != nil {
		return shadowPolicy{}, nil, e
	}
	if json.Unmarshal(b, &risk) != nil || risk.Hash != riskPolicyHash {
		return shadowPolicy{}, nil, fmt.Errorf("frozen risk policy identity mismatch")
	}
	models := make([]shadowModel, len(policy.Candidates))
	for i, c := range policy.Candidates {
		h, e := sha(c.ModelArtifactPath)
		if e != nil || h != c.ClassificationModelSHA {
			return shadowPolicy{}, nil, fmt.Errorf("model hash mismatch %s", c.CandidateID)
		}
		b, e = os.ReadFile(c.ModelArtifactPath)
		if e != nil {
			return shadowPolicy{}, nil, e
		}
		if e = json.Unmarshal(b, &models[i]); e != nil {
			return shadowPolicy{}, nil, e
		}
		if models[i].FeatureCount != 128 || models[i].FeatureRegistryHash != featureRegistryHash {
			return shadowPolicy{}, nil, fmt.Errorf("model registry mismatch")
		}
	}
	return policy, models, nil
}

func executeShadow(duration time.Duration) (shadowMetrics, error) {
	policy, models, e := loadShadowInputs()
	if e != nil {
		return shadowMetrics{}, e
	}
	data, e := loadWarmupDataset()
	if e != nil {
		return shadowMetrics{}, e
	}
	rows, e := replayFrozenFeatures(data)
	if e != nil {
		return shadowMetrics{}, e
	}
	if len(rows) == 0 {
		return shadowMetrics{}, fmt.Errorf("no shadow decisions")
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].TimestampMs < rows[j].TimestampMs })
	end := rows[len(rows)-1].TimestampMs
	start := end - duration.Milliseconds()
	barAt := map[int64]int{}
	for i, x := range data.Futures {
		barAt[x.TimestampMs] = i
	}
	r := shadowMetrics{DurationMs: duration.Milliseconds()}
	var open bool
	var side string
	var entry, tp, sl float64
	var exitDeadline int64
	for _, row := range rows {
		if row.TimestampMs < start {
			continue
		}
		r.Decisions++
		if open {
			idx, ok := barAt[row.TimestampMs-1000]
			if !ok {
				r.Errors++
				continue
			}
			bar := data.Futures[idx]
			hit := false
			if side == "LONG" {
				hit = bar.High >= tp || bar.Low <= sl
			} else {
				hit = bar.Low <= tp || bar.High >= sl
			}
			if hit || row.TimestampMs >= exitDeadline {
				open = false
				r.PaperExits++
			}
		}
		if row.Reason != featurev2.Eligible {
			r.Excluded++
			if row.Reason == featurev2.FutureObservation {
				r.FutureObservation++
			}
			continue
		}
		r.Eligible++
		if open {
			continue
		}
		inferenceStart := time.Now()
		selected := -1
		best := math.Inf(-1)
		for i, m := range models {
			p, pred, e := m.predict(row.Values)
			if e != nil {
				return r, e
			}
			c := policy.Candidates[i]
			if p >= c.ClassificationThreshold && pred >= c.RegressionThreshold && (pred > best || (pred == best && (selected < 0 || c.CandidateID < policy.Candidates[selected].CandidateID))) {
				selected, best = i, pred
			}
		}
		r.InferenceLatencyMeanUs += float64(time.Since(inferenceStart).Microseconds())
		policyStart := time.Now()
		if selected < 0 {
			r.NoTrade++
			r.PolicyLatencyMeanUs += float64(time.Since(policyStart).Microseconds())
			continue
		}
		c := policy.Candidates[selected]
		idx, ok := barAt[row.TimestampMs-1000]
		if !ok {
			r.Errors++
			continue
		}
		entry = data.Futures[idx].Close
		side = c.Side
		if side == "LONG" {
			tp = entry * (1 + float64(c.TPBps)/10000)
			sl = entry * (1 - float64(c.SLBps)/10000)
			r.Long++
		} else {
			tp = entry * (1 - float64(c.TPBps)/10000)
			sl = entry * (1 + float64(c.SLBps)/10000)
			r.Short++
		}
		exitDeadline = row.TimestampMs + int64(c.HorizonSeconds)*1000
		open = true
		r.Signals++
		r.PaperEntries++
		r.PolicyLatencyMeanUs += float64(time.Since(policyStart).Microseconds())
	}
	if r.Eligible > 0 {
		r.InferenceLatencyMeanUs /= float64(r.Eligible)
		r.PolicyLatencyMeanUs /= float64(r.Eligible)
	}
	r.OpenPositionAtEnd = open
	return r, nil
}
