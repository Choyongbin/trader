package logistic

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"

	maintraining "binance_trader/internal/training/main"
)

func Sigmoid(z float64) float64 {
	if z >= 0 {
		e := math.Exp(-z)
		return 1 / (1 + e)
	}
	e := math.Exp(z)
	return e / (1 + e)
}
func Softplus(z float64) float64 {
	if z > 0 {
		return z + math.Log1p(math.Exp(-z))
	}
	return math.Log1p(math.Exp(z))
}
func FeatureRegistryHash(names []string) string {
	sum := sha256.Sum256([]byte(strings.Join(names, "\n")))
	return hex.EncodeToString(sum[:])
}

type OptimizerConfig struct {
	Name                 string  `json:"name"`
	Epochs               int     `json:"epochs"`
	BatchSize            int     `json:"batch_size"`
	LearningRate         float64 `json:"learning_rate"`
	Beta1                float64 `json:"beta1"`
	Beta2                float64 `json:"beta2"`
	Epsilon              float64 `json:"epsilon"`
	ConvergenceTolerance float64 `json:"convergence_tolerance"`
}
type Convergence struct {
	InitialObjective     float64 `json:"initial_objective"`
	FinalObjective       float64 `json:"final_objective"`
	FinalObjectiveChange float64 `json:"final_objective_change"`
	Epochs               int     `json:"epochs"`
	Converged            bool    `json:"converged"`
	FinalGradientNorm    float64 `json:"final_gradient_norm"`
}
type Artifact struct {
	ModelFamily          string                           `json:"model_family"`
	ModelVersion         int                              `json:"model_version"`
	Side                 string                           `json:"side"`
	TrainingVersion      int                              `json:"training_version"`
	FeatureVersion       int                              `json:"feature_version"`
	OutcomeVersion       int                              `json:"outcome_version"`
	BarrierVersion       int                              `json:"barrier_version"`
	SplitVersion         int                              `json:"split_version"`
	TradeSpec            maintraining.TradeSpecManifest   `json:"trade_spec"`
	CostProfile          maintraining.CostProfileManifest `json:"cost_profile"`
	FeatureNames         []string                         `json:"ordered_feature_names"`
	FeatureCount         int                              `json:"feature_count"`
	FeatureRegistryHash  string                           `json:"feature_registry_hash"`
	Scaler               Scaler                           `json:"scaler"`
	ConstantFeatureNames []string                         `json:"constant_feature_names"`
	Intercept            float64                          `json:"intercept"`
	Weights              []float64                        `json:"weights"`
	Lambda               float64                          `json:"lambda"`
	Optimizer            OptimizerConfig                  `json:"optimizer"`
	Convergence          Convergence                      `json:"convergence"`
	TrainRows            int64                            `json:"train_rows"`
	ValidationRows       int64                            `json:"validation_rows"`
	CostWarning          string                           `json:"cost_warning"`
}

func (a Artifact) ValidateRegistry(names []string) error {
	if a.FeatureCount != len(names) || len(a.Weights) != len(names) || len(a.Scaler.Mean) != len(names) || len(a.Scaler.Std) != len(names) || a.FeatureRegistryHash != FeatureRegistryHash(names) {
		return fmt.Errorf("feature registry mismatch")
	}
	for i := range names {
		if a.FeatureNames[i] != names[i] {
			return fmt.Errorf("feature order mismatch at %d", i)
		}
	}
	return nil
}
func (a Artifact) Predict(x []float64, names []string) (float64, error) {
	if err := a.ValidateRegistry(names); err != nil {
		return 0, err
	}
	z := a.Intercept
	scaled := make([]float64, len(x))
	if err := a.Scaler.TransformInto(scaled, x); err != nil {
		return 0, err
	}
	for i := range scaled {
		z += a.Weights[i] * scaled[i]
	}
	p := Sigmoid(z)
	if math.IsNaN(p) || math.IsInf(p, 0) {
		return 0, fmt.Errorf("invalid probability")
	}
	return p, nil
}
func WriteArtifact(path string, a Artifact) error {
	b, e := json.MarshalIndent(a, "", "  ")
	if e != nil {
		return e
	}
	b = append(b, '\n')
	if e = os.MkdirAll(filepath.Dir(path), 0755); e != nil {
		return e
	}
	tmp := path + ".tmp"
	if e = os.WriteFile(tmp, b, 0644); e != nil {
		return e
	}
	if e = os.Remove(path); e != nil && !errors.Is(e, os.ErrNotExist) {
		return e
	}
	return os.Rename(tmp, path)
}
func ReadArtifact(path string) (Artifact, error) {
	var a Artifact
	b, e := os.ReadFile(path)
	if e != nil {
		return a, e
	}
	e = json.Unmarshal(b, &a)
	return a, e
}
