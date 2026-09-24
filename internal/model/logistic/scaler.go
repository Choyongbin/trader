package logistic

import (
	"fmt"
	"math"
)

type Scaler struct {
	Count int64     `json:"count"`
	Mean  []float64 `json:"mean"`
	Std   []float64 `json:"std"`
	m2    []float64
}

func NewScaler(features int) *Scaler {
	return &Scaler{Mean: make([]float64, features), Std: make([]float64, features), m2: make([]float64, features)}
}
func (s *Scaler) Observe(x []float64) error {
	if len(x) != len(s.Mean) {
		return fmt.Errorf("scaler feature count mismatch")
	}
	s.Count++
	for i, value := range x {
		delta := value - s.Mean[i]
		s.Mean[i] += delta / float64(s.Count)
		s.m2[i] += delta * (value - s.Mean[i])
	}
	return nil
}
func (s *Scaler) Finish() []int {
	constant := []int{}
	for i := range s.Std {
		s.Std[i] = math.Sqrt(s.m2[i] / float64(s.Count))
		if s.Std[i] <= 1e-15 {
			s.Std[i] = 0
			constant = append(constant, i)
		}
	}
	s.m2 = nil
	return constant
}
func (s Scaler) TransformInto(dst, x []float64) error {
	if len(x) != len(s.Mean) || len(dst) != len(x) {
		return fmt.Errorf("scaler feature count mismatch")
	}
	for i, value := range x {
		if s.Std[i] == 0 {
			dst[i] = 0
		} else {
			dst[i] = (value - s.Mean[i]) / s.Std[i]
		}
	}
	return nil
}
