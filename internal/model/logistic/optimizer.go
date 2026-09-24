package logistic

import "math"

type Trainer struct {
	Lambda                                           float64
	Config                                           OptimizerConfig
	Weights, grad, m, v                              []float64
	Intercept, gradIntercept, mIntercept, vIntercept float64
	steps                                            int64
	samples                                          int64
	loss                                             float64
	lastGradientNorm                                 float64
}

func NewTrainer(features int, lambda float64, config OptimizerConfig) *Trainer {
	return &Trainer{Lambda: lambda, Config: config, Weights: make([]float64, features), grad: make([]float64, features), m: make([]float64, features), v: make([]float64, features)}
}
func (t *Trainer) Observe(x []float64, y bool) {
	z := t.Intercept
	for i := range x {
		z += t.Weights[i] * x[i]
	}
	p := Sigmoid(z)
	yf := 0.0
	if y {
		yf = 1
	}
	d := p - yf
	t.loss += Softplus(z) - yf*z
	t.gradIntercept += d
	for i := range x {
		t.grad[i] += d * x[i]
	}
	t.samples++
	if t.samples%int64(t.Config.BatchSize) == 0 {
		t.Step()
	}
}
func (t *Trainer) Step() {
	batch := t.samples % int64(t.Config.BatchSize)
	if batch == 0 {
		batch = int64(t.Config.BatchSize)
	}
	if batch == 0 {
		return
	}
	t.steps++
	scale := 1 / float64(batch)
	norm := math.Pow(t.gradIntercept*scale, 2)
	t.mIntercept = t.Config.Beta1*t.mIntercept + (1-t.Config.Beta1)*t.gradIntercept*scale
	t.vIntercept = t.Config.Beta2*t.vIntercept + (1-t.Config.Beta2)*math.Pow(t.gradIntercept*scale, 2)
	mh := t.mIntercept / (1 - math.Pow(t.Config.Beta1, float64(t.steps)))
	vh := t.vIntercept / (1 - math.Pow(t.Config.Beta2, float64(t.steps)))
	t.Intercept -= t.Config.LearningRate * mh / (math.Sqrt(vh) + t.Config.Epsilon)
	t.gradIntercept = 0
	for i := range t.Weights {
		g := t.grad[i]*scale + t.Lambda*t.Weights[i]
		norm += g * g
		t.m[i] = t.Config.Beta1*t.m[i] + (1-t.Config.Beta1)*g
		t.v[i] = t.Config.Beta2*t.v[i] + (1-t.Config.Beta2)*g*g
		mh := t.m[i] / (1 - math.Pow(t.Config.Beta1, float64(t.steps)))
		vh := t.v[i] / (1 - math.Pow(t.Config.Beta2, float64(t.steps)))
		t.Weights[i] -= t.Config.LearningRate * mh / (math.Sqrt(vh) + t.Config.Epsilon)
		t.grad[i] = 0
	}
	t.lastGradientNorm = math.Sqrt(norm)
}
func (t *Trainer) FinishEpoch() (float64, float64) {
	if t.samples%int64(t.Config.BatchSize) != 0 {
		t.Step()
	}
	penalty := 0.0
	for _, w := range t.Weights {
		penalty += w * w
	}
	objective := t.loss/float64(t.samples) + t.Lambda*penalty/2
	t.samples = 0
	t.loss = 0
	return objective, t.lastGradientNorm
}
