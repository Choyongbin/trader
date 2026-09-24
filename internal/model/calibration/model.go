package calibration

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"

	logistic "binance_trader/internal/model/logistic"
)

const Version = 1
const ProbabilityEpsilon = 1e-15

type Method string

const (
	Raw      Method = "raw"
	Platt    Method = "platt"
	Isotonic Method = "isotonic"
)

type PlattModel struct {
	A                 float64 `json:"a"`
	B                 float64 `json:"b"`
	Iterations        int     `json:"iterations"`
	Converged         bool    `json:"converged"`
	FinalGradientNorm float64 `json:"final_gradient_norm"`
	Stabilization     float64 `json:"hessian_diagonal_stabilization"`
}
type IsotonicModel struct {
	Thresholds []float64 `json:"thresholds"`
	Values     []float64 `json:"mapped_probabilities"`
	TieGroups  int       `json:"tie_groups"`
	Blocks     int       `json:"blocks"`
	OutOfRange string    `json:"out_of_range"`
}
type Artifact struct {
	CalibrationVersion  int           `json:"calibration_version"`
	Side                string        `json:"side"`
	SourceModelFamily   string        `json:"source_model_family"`
	SourceModelVersion  int           `json:"source_model_version"`
	SourceModelSHA256   string        `json:"source_model_sha256"`
	FeatureRegistryHash string        `json:"feature_registry_hash"`
	SplitVersion        int           `json:"split_version"`
	TrainingVersion     int           `json:"training_version"`
	FitPartition        string        `json:"fit_partition"`
	SelectedMethod      Method        `json:"selected_method"`
	Platt               PlattModel    `json:"platt"`
	Isotonic            IsotonicModel `json:"isotonic"`
	CostWarning         string        `json:"cost_warning"`
}

func clip(p float64) float64                       { return math.Max(ProbabilityEpsilon, math.Min(1-ProbabilityEpsilon, p)) }
func rawLogit(p float64) float64                   { p = clip(p); return math.Log(p / (1 - p)) }
func (p PlattModel) Calibrate(raw float64) float64 { return logistic.Sigmoid(p.A*rawLogit(raw) + p.B) }
func (m IsotonicModel) Calibrate(raw float64) float64 {
	if len(m.Thresholds) == 0 {
		return raw
	}
	i := sort.Search(len(m.Thresholds), func(i int) bool { return m.Thresholds[i] >= raw })
	if i == len(m.Thresholds) {
		i--
	}
	return m.Values[i]
}
func (a Artifact) Calibrate(raw float64, sourceHash string) (float64, error) {
	if sourceHash != a.SourceModelSHA256 {
		return 0, fmt.Errorf("source logistic model hash mismatch")
	}
	var p float64
	switch a.SelectedMethod {
	case Raw:
		p = raw
	case Platt:
		p = a.Platt.Calibrate(raw)
	case Isotonic:
		p = a.Isotonic.Calibrate(raw)
	default:
		return 0, fmt.Errorf("unknown calibration method")
	}
	if math.IsNaN(p) || math.IsInf(p, 0) || p < 0 || p > 1 {
		return 0, fmt.Errorf("invalid calibrated probability")
	}
	return p, nil
}
func FileSHA256(path string) (string, error) {
	b, e := os.ReadFile(path)
	if e != nil {
		return "", e
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
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
