package calibration

import "sort"

type Bin struct {
	Lower, Upper       float64 `json:",omitempty"`
	Count              int64   `json:"count"`
	MeanProbability    float64 `json:"mean_probability"`
	ActualPositiveRate float64 `json:"actual_positive_rate"`
	AbsoluteGap        float64 `json:"absolute_gap"`
	LowSampleWarning   bool    `json:"low_sample_warning"`
}
type Reliability struct {
	EqualWidth              []Bin   `json:"equal_width_10_bins"`
	Quantile                []Bin   `json:"quantile_10_bins"`
	ECE                     float64 `json:"ece"`
	MCE                     float64 `json:"mce"`
	MinimumRecommendedCount int64   `json:"minimum_recommended_count"`
}

func ReliabilityMetrics(probabilities []float64, targets []bool) Reliability {
	r := Reliability{MinimumRecommendedCount: 100, EqualWidth: make([]Bin, 10)}
	for i := range r.EqualWidth {
		r.EqualWidth[i].Lower = float64(i) / 10
		r.EqualWidth[i].Upper = float64(i+1) / 10
	}
	type pair struct {
		p float64
		y bool
	}
	pairs := make([]pair, len(probabilities))
	for i, p := range probabilities {
		pairs[i] = pair{p, targets[i]}
		bin := int(p * 10)
		if bin > 9 {
			bin = 9
		}
		b := &r.EqualWidth[bin]
		b.Count++
		b.MeanProbability += p
		if targets[i] {
			b.ActualPositiveRate++
		}
	}
	finishBins(r.EqualWidth, int64(len(probabilities)), &r)
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].p < pairs[j].p })
	for bin := 0; bin < 10; bin++ {
		start := bin * len(pairs) / 10
		end := (bin + 1) * len(pairs) / 10
		b := Bin{Count: int64(end - start)}
		if end > start {
			b.Lower = pairs[start].p
			b.Upper = pairs[end-1].p
			for _, x := range pairs[start:end] {
				b.MeanProbability += x.p
				if x.y {
					b.ActualPositiveRate++
				}
			}
		}
		r.Quantile = append(r.Quantile, b)
	}
	dummy := Reliability{}
	finishBins(r.Quantile, int64(len(probabilities)), &dummy)
	return r
}
func finishBins(bins []Bin, total int64, r *Reliability) {
	for i := range bins {
		b := &bins[i]
		if b.Count == 0 {
			b.LowSampleWarning = true
			continue
		}
		b.MeanProbability /= float64(b.Count)
		b.ActualPositiveRate /= float64(b.Count)
		gap := b.MeanProbability - b.ActualPositiveRate
		if gap < 0 {
			gap = -gap
		}
		b.AbsoluteGap = gap
		b.LowSampleWarning = b.Count < 100
		r.ECE += float64(b.Count) / float64(total) * gap
		if gap > r.MCE {
			r.MCE = gap
		}
	}
}

type Distribution struct{ Min, P01, P05, P25, Median, P75, P95, P99, Max, Mean float64 }

func ProbabilityDistribution(values []float64) Distribution {
	copyValues := append([]float64(nil), values...)
	sort.Float64s(copyValues)
	q := func(p float64) float64 { return copyValues[int(p*float64(len(copyValues)-1))] }
	sum := 0.0
	for _, x := range values {
		sum += x
	}
	return Distribution{copyValues[0], q(.01), q(.05), q(.25), q(.5), q(.75), q(.95), q(.99), copyValues[len(copyValues)-1], sum / float64(len(values))}
}
