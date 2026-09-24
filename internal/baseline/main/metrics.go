package mainbaseline

import (
	"math"
	"sort"
)

const LogLossEpsilon = 1e-15

type Classification struct {
	Rows, Positive, Negative int64
	TP, FP, TN, FN           int64
	Accuracy                 float64
	BalancedAccuracy         float64
	Precision                float64
	Recall                   float64
	Specificity              float64
	F1                       float64
	MCC                      float64
}

func safeDiv(a, b float64) float64 {
	if b == 0 {
		return 0
	}
	return a / b
}

func FinishClassification(m *Classification) {
	m.Positive, m.Negative = m.TP+m.FN, m.TN+m.FP
	m.Rows = m.Positive + m.Negative
	m.Accuracy = safeDiv(float64(m.TP+m.TN), float64(m.Rows))
	m.Recall = safeDiv(float64(m.TP), float64(m.TP+m.FN))
	m.Specificity = safeDiv(float64(m.TN), float64(m.TN+m.FP))
	m.BalancedAccuracy = (m.Recall + m.Specificity) / 2
	m.Precision = safeDiv(float64(m.TP), float64(m.TP+m.FP))
	m.F1 = safeDiv(2*m.Precision*m.Recall, m.Precision+m.Recall)
	denominator := math.Sqrt(float64(m.TP+m.FP) * float64(m.TP+m.FN) * float64(m.TN+m.FP) * float64(m.TN+m.FN))
	m.MCC = safeDiv(float64(m.TP*m.TN-m.FP*m.FN), denominator)
}

func ObserveClassification(m *Classification, actual, predicted bool) {
	switch {
	case actual && predicted:
		m.TP++
	case !actual && predicted:
		m.FP++
	case !actual && !predicted:
		m.TN++
	default:
		m.FN++
	}
}

func Brier(probabilities []float64, targets []bool) float64 {
	var sum float64
	for i, p := range probabilities {
		y := 0.0
		if targets[i] {
			y = 1
		}
		d := p - y
		sum += d * d
	}
	return safeDiv(sum, float64(len(targets)))
}

func LogLoss(probabilities []float64, targets []bool) float64 {
	var sum float64
	for i, p := range probabilities {
		p = math.Max(LogLossEpsilon, math.Min(1-LogLossEpsilon, p))
		if targets[i] {
			sum -= math.Log(p)
		} else {
			sum -= math.Log(1 - p)
		}
	}
	return safeDiv(sum, float64(len(targets)))
}

// RankingMetrics uses average ranks for ROC ties and grouped thresholds for AP.
func RankingMetrics(positiveScores, negativeScores []float64, descending bool) (rocAUC, averagePrecision float64) {
	type point struct {
		score    float64
		positive bool
	}
	points := make([]point, 0, len(positiveScores)+len(negativeScores))
	for _, score := range positiveScores {
		if !descending {
			score = -score
		}
		points = append(points, point{score, true})
	}
	for _, score := range negativeScores {
		if !descending {
			score = -score
		}
		points = append(points, point{score, false})
	}
	sort.Slice(points, func(i, j int) bool { return points[i].score > points[j].score })
	p, n := float64(len(positiveScores)), float64(len(negativeScores))
	if p == 0 || n == 0 {
		return 0, 0
	}
	var tp, fp, previousTP, previousFP float64
	for i := 0; i < len(points); {
		j := i
		for j < len(points) && points[j].score == points[i].score {
			if points[j].positive {
				tp++
			} else {
				fp++
			}
			j++
		}
		rocAUC += (fp - previousFP) * (tp + previousTP) / 2
		averagePrecision += (tp - previousTP) / p * safeDiv(tp, tp+fp)
		previousTP, previousFP = tp, fp
		i = j
	}
	return rocAUC / (p * n), averagePrecision
}
