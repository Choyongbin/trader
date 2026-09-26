package main

import (
	"errors"
	"math"
	"sort"
)

const featureCount = 128
const metricsAgeLimitMs int64 = 600_000

var unknownFeatureIndexes = [...]int{100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 116, 126}

// Corrected Taker is separate from the five unverified 2024 Metrics fields.
type takerPoint struct {
	EffectiveMs       int64
	IntervalEndMs     int64
	SourceTimestampMs int64
	Value             float64
}

// selectPoint returns only previously completed AND assumed-available information.
// A missing or stale value is not forward-filled beyond the frozen 10m age limit.
func selectPoint(points []takerPoint, anchorMs int64) (takerPoint, bool) {
	i := sort.Search(len(points), func(i int) bool { return points[i].EffectiveMs > anchorMs }) - 1
	if i < 0 {
		return takerPoint{}, false
	}
	x := points[i]
	if x.EffectiveMs > anchorMs || x.IntervalEndMs > anchorMs ||
		anchorMs-x.IntervalEndMs < 0 || anchorMs-x.IntervalEndMs > metricsAgeLimitMs ||
		x.Value <= 0 || math.IsNaN(x.Value) || math.IsInf(x.Value, 0) {
		return takerPoint{}, false
	}
	return x, true
}

func correctedTaker(points []takerPoint, decisionMs int64) (level, change float64, ok bool) {
	current, a := selectPoint(points, decisionMs)
	prior, b := selectPoint(points, decisionMs-300_000)
	if !a || !b {
		return 0, 0, false
	}
	change = (current.Value - prior.Value) / prior.Value
	return current.Value, change, !math.IsNaN(change) && !math.IsInf(change, 0)
}

type linearModel struct {
	Name                                         string
	Mean, Std                                    []float64
	ClassWeights, ReturnWeights                  []float64
	ClassIntercept, ReturnIntercept, ReturnScale float64
}

func (m linearModel) validate() error {
	if len(m.Mean) != featureCount || len(m.Std) != featureCount ||
		len(m.ClassWeights) != featureCount || len(m.ReturnWeights) != featureCount {
		return errors.New("model vector dimensions do not match 128")
	}
	if m.ReturnScale <= 0 || math.IsNaN(m.ReturnScale) {
		return errors.New("invalid return scale")
	}
	for i := 0; i < featureCount; i++ {
		if m.Std[i] < 0 || math.IsNaN(m.Std[i]) || math.IsInf(m.Std[i], 0) || math.IsNaN(m.Mean[i]) || math.IsInf(m.Mean[i], 0) {
			return errors.New("invalid scaler")
		}
		if math.IsNaN(m.ClassWeights[i]) || math.IsNaN(m.ReturnWeights[i]) {
			return errors.New("non-finite model weight")
		}
	}
	return nil
}

func sigmoid(v float64) float64 {
	if v >= 0 {
		x := math.Exp(-v)
		return 1 / (1 + x)
	}
	x := math.Exp(v)
	return x / (1 + x)
}

type pair struct {
	Base, Corrected             float64
	BaseReturn, CorrectedReturn float64
}

func (m linearModel) scores(values [featureCount]float64, correctedLevel, correctedChange float64) (p pair, ablClass, ablReturn float64, err error) {
	z, zcorrected, zablated := m.ClassIntercept, m.ClassIntercept, m.ClassIntercept
	r, rcorrected, rablated := m.ReturnIntercept, m.ReturnIntercept, m.ReturnIntercept
	for i, value := range values {
		if math.IsInf(value, 0) || math.IsNaN(value) {
			return pair{}, 0, 0, errors.New("non-finite frozen input")
		}
		sx, sy := 0.0, 0.0
		if m.Std[i] > 0 {
			sx = (value - m.Mean[i]) / m.Std[i]
			corrected := value
			if i == 114 {
				corrected = correctedLevel
			}
			if i == 115 {
				corrected = correctedChange
			}
			sy = (corrected - m.Mean[i]) / m.Std[i]
		}
		z += m.ClassWeights[i] * sx
		r += m.ReturnWeights[i] * sx
		zcorrected += m.ClassWeights[i] * sy
		rcorrected += m.ReturnWeights[i] * sy
		unknown := false
		for _, idx := range unknownFeatureIndexes {
			if idx == i {
				unknown = true
				break
			}
		}
		if !unknown {
			zablated += m.ClassWeights[i] * sx
			rablated += m.ReturnWeights[i] * sx
		}
	}
	p = pair{Base: sigmoid(z), Corrected: sigmoid(zcorrected), BaseReturn: r / m.ReturnScale, CorrectedReturn: rcorrected / m.ReturnScale}
	ablClass = sigmoid(zablated)
	ablReturn = rablated / m.ReturnScale
	if math.IsNaN(p.Base) || math.IsNaN(p.Corrected) || math.IsInf(p.BaseReturn, 0) || math.IsInf(p.CorrectedReturn, 0) {
		return pair{}, 0, 0, errors.New("non-finite model score")
	}
	return
}

type ranked struct {
	Value float64
	ID    int
}

// topOverlap measures rank fragility; not accuracy, performance, or profitability.
func topOverlap(base, changed []float64, fraction float64) float64 {
	if len(base) != len(changed) || len(base) == 0 {
		return math.NaN()
	}
	k := int(math.Ceil(float64(len(base)) * fraction))
	if k < 1 {
		k = 1
	}
	b, c := make([]ranked, len(base)), make([]ranked, len(base))
	for i := range base {
		b[i] = ranked{base[i], i}
		c[i] = ranked{changed[i], i}
	}
	order := func(a []ranked) {
		sort.Slice(a, func(i, j int) bool {
			if a[i].Value == a[j].Value {
				return a[i].ID < a[j].ID
			}
			return a[i].Value > a[j].Value
		})
	}
	order(b)
	order(c)
	top := make(map[int]bool, k)
	for _, x := range b[:k] {
		top[x.ID] = true
	}
	hits := 0
	for _, x := range c[:k] {
		if top[x.ID] {
			hits++
		}
	}
	return float64(hits) / float64(k)
}

func percentileSorted(values []float64, quantile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	v := append([]float64(nil), values...)
	sort.Float64s(v)
	pos := float64(len(v)-1) * quantile
	lo := int(pos)
	hi := int(math.Ceil(pos))
	return v[lo] + (v[hi]-v[lo])*(pos-float64(lo))
}

// selectOffsetPoint shifts *availability*, not source interval, by the hypothetical
// publication delay. Binary search relies on source points' strictly increasing availability.
func selectOffsetPoint(points []takerPoint, anchor, delay int64) (takerPoint, bool) {
	if delay < 0 {
		return takerPoint{}, false
	}
	adjustedAnchor := anchor - delay
	p, ok := selectPoint(points, adjustedAnchor)
	// Freshness must be measured against the actual decision anchor, NOT adjustedAnchor.
	if !ok || anchor-p.IntervalEndMs > metricsAgeLimitMs {
		return takerPoint{}, false
	}
	return p, true
}
