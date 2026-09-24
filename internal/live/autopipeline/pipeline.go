// Package autopipeline binds frozen Feature V2 models and policies to a
// non-submitting execution-intent sink. It cannot place an exchange order.
package autopipeline

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	featurev2 "binance_trader/internal/feature/main/v2"
	"binance_trader/internal/model/logistic"
)

const (
	ProfileID   = "btc-feature-v2-production-v1"
	FeatureHash = "a37306b80ecbf701103ab691445d07a39624ce995d84f8206d6314dae3045bef"
	EntryHash   = "4fae120d9d54a5732dbf2009950beba69773bb0a28a7c3915b616e295cd3dd31"
	RiskHash    = "21e3332b5cf286dd39827973a259fbd91dde325ca067f6a067237bb6f24596f7"
)

type Candidate struct {
	CandidateID             string  `json:"candidate_id"`
	Side                    string  `json:"side"`
	TPBps                   int     `json:"tp_bps"`
	SLBps                   int     `json:"sl_bps"`
	HorizonSeconds          int     `json:"horizon_seconds"`
	ModelArtifactPath       string  `json:"model_artifact_path"`
	ClassificationModelSHA  string  `json:"classification_model_sha256"`
	ClassificationThreshold float64 `json:"classification_numeric_threshold"`
	RegressionThreshold     float64 `json:"regression_numeric_threshold"`
}
type frozenEntry struct {
	FeatureRegistryHash string      `json:"feature_registry_hash"`
	PolicyHash          string      `json:"policy_sha256"`
	Frozen              bool        `json:"POLICY_FROZEN"`
	Complete            bool        `json:"complete"`
	Candidates          []Candidate `json:"candidates"`
}
type riskCandidate struct {
	CandidateID            string
	TargetNotionalFraction float64
	Leverage               int
	MarginFraction         float64
	Valid                  bool
}
type frozenRisk struct {
	RiskPolicySHA       string `json:"risk_policy_sha256"`
	StartingEquity      float64
	RiskPerTrade        float64
	MaxNotionalFraction float64
	MaxMarginFraction   float64
	MaxLeverage         int
	Candidates          []riskCandidate
}
type model struct {
	FeatureCount        int             `json:"feature_count"`
	FeatureRegistryHash string          `json:"feature_registry_hash"`
	Scaler              logistic.Scaler `json:"scaler"`
	ClassIntercept      float64         `json:"classification_intercept"`
	ClassWeights        []float64       `json:"classification_weights"`
	RegressionIntercept float64         `json:"regression_intercept_scaled"`
	RegressionWeights   []float64       `json:"regression_weights_scaled"`
	ReturnScale         float64         `json:"return_scale"`
}
type Pipeline struct {
	entry    frozenEntry
	risk     frozenRisk
	models   []model
	riskByID map[string]riskCandidate
}

type CandidateScore struct {
	CandidateID             string  `json:"candidate_id"`
	Side                    string  `json:"side"`
	Probability             float64 `json:"classification_probability"`
	PredictedReturn         float64 `json:"predicted_return"`
	ClassificationThreshold float64 `json:"classification_threshold"`
	RegressionThreshold     float64 `json:"regression_threshold"`
	Pass                    bool    `json:"pass"`
}
type ExecutionIntent struct {
	Environment         string  `json:"environment"`
	Symbol              string  `json:"symbol"`
	Side                string  `json:"side"`
	CandidateID         string  `json:"candidate_id"`
	QuantityBTC         float64 `json:"quantity_btc"`
	NotionalUSDT        float64 `json:"notional_usdt"`
	RiskBudgetUSDT      float64 `json:"risk_budget_usdt"`
	Leverage            int     `json:"leverage"`
	MarginUSDT          float64 `json:"margin_usdt"`
	EntryReferencePrice float64 `json:"entry_reference_price"`
	TPPrice             float64 `json:"tp_price"`
	SLPrice             float64 `json:"sl_price"`
	HorizonSeconds      int     `json:"horizon_seconds"`
	ClientOrderID       string  `json:"client_order_id"`
	ModelProfileID      string  `json:"model_profile_id"`
	DecisionTimestampMs int64   `json:"decision_timestamp_ms"`
}
type Result struct {
	DecisionTimestampMs int64            `json:"decision_timestamp_ms"`
	Scores              []CandidateScore `json:"scores"`
	FinalSignal         string           `json:"final_signal"`
	Intent              *ExecutionIntent `json:"proposed_trade,omitempty"`
	InferenceLatencyUs  float64          `json:"inference_latency_us"`
	PolicyLatencyUs     float64          `json:"policy_latency_us"`
}
type IntentSink interface{ Record(ExecutionIntent) error }
type RecordingBroker struct{ Intents []ExecutionIntent }

func (b *RecordingBroker) Record(intent ExecutionIntent) error {
	if len(b.Intents) == 256 {
		copy(b.Intents, b.Intents[1:])
		b.Intents[255] = intent
		return nil
	}
	b.Intents = append(b.Intents, intent)
	return nil
}

func LoadFrozen(root string) (*Pipeline, error) {
	read := func(path string, into any) error {
		b, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			return err
		}
		return json.Unmarshal(b, into)
	}
	var entry frozenEntry
	if err := read(filepath.FromSlash("data/reports/model/main/v2/phase9c-cde/stage-d-policy-freeze.json"), &entry); err != nil {
		return nil, err
	}
	if !entry.Complete || !entry.Frozen || entry.FeatureRegistryHash != FeatureHash || entry.PolicyHash != EntryHash || len(entry.Candidates) != 5 {
		return nil, fmt.Errorf("ENTRY_POLICY_IDENTITY_MISMATCH")
	}
	var risk frozenRisk
	if err := read(filepath.FromSlash("data/reports/production/v1/BTCUSDT/risk-policy-v1.json"), &risk); err != nil {
		return nil, err
	}
	if risk.RiskPolicySHA != RiskHash || len(risk.Candidates) != 5 || risk.RiskPerTrade <= 0 {
		return nil, fmt.Errorf("RISK_POLICY_IDENTITY_MISMATCH")
	}
	p := &Pipeline{entry: entry, risk: risk, models: make([]model, len(entry.Candidates)), riskByID: map[string]riskCandidate{}}
	for _, x := range risk.Candidates {
		if !x.Valid || x.Leverage < 1 {
			return nil, fmt.Errorf("INVALID_RISK_CANDIDATE")
		}
		p.riskByID[x.CandidateID] = x
	}
	for i, c := range entry.Candidates {
		if _, ok := p.riskByID[c.CandidateID]; !ok {
			return nil, fmt.Errorf("MISSING_RISK_CANDIDATE: %s", c.CandidateID)
		}
		path := filepath.Join(root, filepath.FromSlash(strings.ReplaceAll(c.ModelArtifactPath, "\\", "/")))
		body, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		h := sha256.Sum256(body)
		if hex.EncodeToString(h[:]) != c.ClassificationModelSHA {
			return nil, fmt.Errorf("MODEL_SHA_MISMATCH: %s", c.CandidateID)
		}
		if err = json.Unmarshal(body, &p.models[i]); err != nil {
			return nil, err
		}
		m := p.models[i]
		if m.FeatureCount != 128 || m.FeatureRegistryHash != FeatureHash || len(m.ClassWeights) != 128 || len(m.RegressionWeights) != 128 || m.ReturnScale <= 0 {
			return nil, fmt.Errorf("MODEL_IDENTITY_MISMATCH: %s", c.CandidateID)
		}
	}
	return p, nil
}

func (p *Pipeline) Evaluate(features featurev2.Snapshot, equity, entryPrice float64) (Result, error) {
	if p == nil || features.DecisionTimestampMs <= 0 || !featurev2.AllFinite(features.Values[:]...) || !finitePositive(equity) || !finitePositive(entryPrice) {
		return Result{}, fmt.Errorf("INVALID_PIPELINE_INPUT")
	}
	result := Result{DecisionTimestampMs: features.DecisionTimestampMs, Scores: make([]CandidateScore, 0, 5), FinalSignal: "NO_TRADE"}
	inferenceStart := time.Now()
	for i, c := range p.entry.Candidates {
		m := p.models[i]
		var scaled [128]float64
		if err := m.Scaler.TransformInto(scaled[:], features.Values[:]); err != nil {
			return Result{}, err
		}
		logit, estimate := m.ClassIntercept, m.RegressionIntercept
		for j, v := range scaled {
			logit += m.ClassWeights[j] * v
			estimate += m.RegressionWeights[j] * v
		}
		probability, predicted := logistic.Sigmoid(logit), estimate/m.ReturnScale
		if !finite(probability) || !finite(predicted) {
			return Result{}, fmt.Errorf("NON_FINITE_PREDICTION")
		}
		result.Scores = append(result.Scores, CandidateScore{c.CandidateID, c.Side, probability, predicted, c.ClassificationThreshold, c.RegressionThreshold, false})
	}
	result.InferenceLatencyUs = float64(time.Since(inferenceStart).Nanoseconds()) / 1000
	policyStart := time.Now()
	selected, best := -1, math.Inf(-1)
	for i, c := range p.entry.Candidates {
		score := &result.Scores[i]
		score.Pass = score.Probability >= c.ClassificationThreshold && score.PredictedReturn >= c.RegressionThreshold
		if score.Pass && (score.PredictedReturn > best || (score.PredictedReturn == best && (selected < 0 || c.CandidateID < p.entry.Candidates[selected].CandidateID))) {
			selected, best = i, score.PredictedReturn
		}
	}
	if selected < 0 {
		result.PolicyLatencyUs = float64(time.Since(policyStart).Nanoseconds()) / 1000
		return result, nil
	}
	c := p.entry.Candidates[selected]
	r := p.riskByID[c.CandidateID]
	notional := equity * r.TargetNotionalFraction
	margin := notional / float64(r.Leverage)
	if notional > equity*p.risk.MaxNotionalFraction+1e-9 || margin > equity*p.risk.MaxMarginFraction+1e-9 || r.Leverage > p.risk.MaxLeverage {
		return Result{}, fmt.Errorf("FROZEN_RISK_LIMIT_REJECT")
	}
	direction := 1.0
	if c.Side == "SHORT" {
		direction = -1
	}
	intent := ExecutionIntent{Environment: "TESTNET", Symbol: "BTCUSDT", Side: c.Side, CandidateID: c.CandidateID, QuantityBTC: notional / entryPrice, NotionalUSDT: notional, RiskBudgetUSDT: equity * p.risk.RiskPerTrade, Leverage: r.Leverage, MarginUSDT: margin, EntryReferencePrice: entryPrice, TPPrice: entryPrice * (1 + direction*float64(c.TPBps)/10000), SLPrice: entryPrice * (1 - direction*float64(c.SLBps)/10000), HorizonSeconds: c.HorizonSeconds, ClientOrderID: fmt.Sprintf("dry-%s-%d", c.CandidateID, features.DecisionTimestampMs), ModelProfileID: ProfileID, DecisionTimestampMs: features.DecisionTimestampMs}
	result.FinalSignal, result.Intent = c.Side, &intent
	result.PolicyLatencyUs = float64(time.Since(policyStart).Nanoseconds()) / 1000
	return result, nil
}

func (p *Pipeline) EvaluateAndRecord(features featurev2.Snapshot, equity, entryPrice float64, sink IntentSink) (Result, error) {
	result, err := p.Evaluate(features, equity, entryPrice)
	if err != nil {
		return Result{}, err
	}
	if result.Intent != nil && sink != nil {
		if err := sink.Record(*result.Intent); err != nil {
			return Result{}, err
		}
	}
	return result, nil
}

func finite(v float64) bool         { return !math.IsNaN(v) && !math.IsInf(v, 0) }
func finitePositive(v float64) bool { return finite(v) && v > 0 }
