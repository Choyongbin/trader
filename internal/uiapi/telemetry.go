package uiapi

import (
	"math"
	"sort"
)

// A fixed-size ring prevents high-frequency market telemetry from growing
// with session duration. No synthetic zero samples are inserted.
type latencyRing struct {
	values     [256]float64
	next, size uint16
}

func (r *latencyRing) add(value float64) {
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
		return
	}
	r.values[r.next] = value
	r.next = (r.next + 1) % uint16(len(r.values))
	if r.size < uint16(len(r.values)) {
		r.size++
	}
}

func (r latencyRing) summary() any {
	if r.size == 0 {
		return "UNAVAILABLE"
	}
	values := append([]float64(nil), r.values[:r.size]...)
	sort.Float64s(values)
	quantile := func(p float64) float64 { return values[int(math.Ceil(p*float64(len(values))))-1] }
	return map[string]any{"samples": r.size, "p50": quantile(.50), "p95": quantile(.95), "p99": quantile(.99)}
}
