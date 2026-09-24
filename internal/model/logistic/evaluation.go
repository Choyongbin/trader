package logistic

import (
	"math"
	"sort"

	mainbaseline "binance_trader/internal/baseline/main"
)

type Evaluation struct {
	Rows, Positive, Negative int64                       `json:",omitempty"`
	PositiveRate             float64                     `json:"positive_rate"`
	LogLoss                  float64                     `json:"log_loss"`
	Brier                    float64                     `json:"brier"`
	ROCAUC                   float64                     `json:"roc_auc"`
	AveragePrecision         float64                     `json:"average_precision"`
	Classification           mainbaseline.Classification `json:"classification"`
	MeanPredictedProbability float64                     `json:"mean_predicted_probability"`
	SignalCount              int64                       `json:"signal_count"`
	SignalCoverage           float64                     `json:"signal_coverage"`
	SignalWinRate            float64                     `json:"signal_win_rate"`
	MeanSignalNetReturn      float64                     `json:"mean_signal_net_return_ex_funding"`
	MedianSignalNetReturn    float64                     `json:"median_signal_net_return_ex_funding"`
}
type EvalAccumulator struct {
	classification                                mainbaseline.Classification
	positiveScores, negativeScores, signalReturns []float64
	probabilitySum, loss, brier, signalSum        float64
	signalWins                                    int64
}

func (e *EvalAccumulator) Observe(probability float64, target bool, netReturn float64) {
	mainbaseline.ObserveClassification(&e.classification, target, probability >= .5)
	e.probabilitySum += probability
	yf := 0.0
	if target {
		yf = 1
		e.positiveScores = append(e.positiveScores, probability)
	} else {
		e.negativeScores = append(e.negativeScores, probability)
	}
	e.loss += Softplus(logitSafe(probability)) - yf*logitSafe(probability)
	d := probability - yf
	e.brier += d * d
	if probability >= .5 {
		e.signalReturns = append(e.signalReturns, netReturn)
		e.signalSum += netReturn
		if target {
			e.signalWins++
		}
	}
}
func logitSafe(p float64) float64 {
	p = math.Max(mainbaseline.LogLossEpsilon, math.Min(1-mainbaseline.LogLossEpsilon, p))
	return math.Log(p / (1 - p))
}
func (e *EvalAccumulator) Finish() Evaluation {
	mainbaseline.FinishClassification(&e.classification)
	rows := e.classification.Rows
	roc, ap := mainbaseline.RankingMetrics(e.positiveScores, e.negativeScores, true)
	result := Evaluation{Rows: rows, Positive: e.classification.Positive, Negative: e.classification.Negative, PositiveRate: float64(e.classification.Positive) / float64(rows), LogLoss: e.loss / float64(rows), Brier: e.brier / float64(rows), ROCAUC: roc, AveragePrecision: ap, Classification: e.classification, MeanPredictedProbability: e.probabilitySum / float64(rows), SignalCount: int64(len(e.signalReturns))}
	result.SignalCoverage = float64(result.SignalCount) / float64(rows)
	if result.SignalCount > 0 {
		result.SignalWinRate = float64(e.signalWins) / float64(result.SignalCount)
		result.MeanSignalNetReturn = e.signalSum / float64(result.SignalCount)
		sort.Float64s(e.signalReturns)
		mid := len(e.signalReturns) / 2
		result.MedianSignalNetReturn = e.signalReturns[mid]
		if len(e.signalReturns)%2 == 0 {
			result.MedianSignalNetReturn = (e.signalReturns[mid-1] + e.signalReturns[mid]) / 2
		}
	}
	return result
}
