package mainbaseline

import (
	"math"
	"sort"

	mainsplit "binance_trader/internal/split/main"
)

type OptionalFloat struct {
	Valid bool    `json:"valid"`
	Value float64 `json:"value,omitempty"`
}

type TradingMetrics struct {
	SignalCount              int64         `json:"signal_count"`
	SignalCoverage           float64       `json:"signal_coverage"`
	ActualNetProfitableCount int64         `json:"actual_net_profitable_count"`
	SignalWinRate            OptionalFloat `json:"signal_win_rate"`
	MeanNetReturnExFunding   OptionalFloat `json:"mean_net_return_ex_funding"`
	MedianNetReturnExFunding OptionalFloat `json:"median_net_return_ex_funding"`
	MeanGrossMarketReturn    OptionalFloat `json:"mean_gross_market_return"`
	ExpectancyInterpretation string        `json:"expectancy_interpretation"`
}

type Result struct {
	BaselineName     string         `json:"baseline_name"`
	Side             string         `json:"side"`
	Partition        string         `json:"partition"`
	Classification   Classification `json:"classification"`
	ROCAUC           OptionalFloat  `json:"roc_auc"`
	AveragePrecision OptionalFloat  `json:"average_precision"`
	BrierScore       OptionalFloat  `json:"brier_score"`
	LogLoss          OptionalFloat  `json:"log_loss"`
	Trading          TradingMetrics `json:"trading"`
}

type resultAccumulator struct {
	result           Result
	netReturns       []float64
	netSum, grossSum float64
}

type Evaluator struct {
	Side, Partition                                        string
	Prevalence                                             float64
	Ret60Index                                             int
	Taker60Index                                           int
	results                                                map[string]*resultAccumulator
	retPositive, retNegative, takerPositive, takerNegative []float64
}

func NewEvaluator(side, partition string, prevalence float64) (*Evaluator, error) {
	retIndex, err := FeatureIndex("ret_log_60s")
	if err != nil {
		return nil, err
	}
	takerIndex, err := FeatureIndex("taker_imbalance_60s")
	if err != nil {
		return nil, err
	}
	e := &Evaluator{Side: side, Partition: partition, Prevalence: prevalence, Ret60Index: retIndex, Taker60Index: takerIndex, results: map[string]*resultAccumulator{}}
	for _, name := range []string{AlwaysNegative, AlwaysPositive, TrainPrevalence, Momentum60s, MeanReversion60s, TakerFlow60s} {
		e.results[name] = &resultAccumulator{result: Result{BaselineName: name, Side: side, Partition: partition}}
	}
	return e, nil
}

func (e *Evaluator) Observe(sample mainsplit.Sample) {
	ret60, taker60 := sample.Features[e.Ret60Index], sample.Features[e.Taker60Index]
	momentum, meanReversion, taker, _ := RuleScores(e.Side, ret60, taker60)
	predictions := map[string]bool{AlwaysNegative: false, AlwaysPositive: true, TrainPrevalence: e.Prevalence > .5, Momentum60s: momentum > 0, MeanReversion60s: meanReversion > 0, TakerFlow60s: taker > 0}
	for name, prediction := range predictions {
		a := e.results[name]
		ObserveClassification(&a.result.Classification, sample.Target, prediction)
		if prediction {
			a.result.Trading.SignalCount++
			if sample.Target {
				a.result.Trading.ActualNetProfitableCount++
			}
			a.netReturns = append(a.netReturns, sample.NetReturnExFunding)
			a.netSum += sample.NetReturnExFunding
			a.grossSum += sample.GrossMarketReturn
		}
	}
	if sample.Target {
		e.retPositive = append(e.retPositive, ret60)
		e.takerPositive = append(e.takerPositive, taker60)
	} else {
		e.retNegative = append(e.retNegative, ret60)
		e.takerNegative = append(e.takerNegative, taker60)
	}
}

func (e *Evaluator) Finish() []Result {
	for _, a := range e.results {
		FinishClassification(&a.result.Classification)
		t := &a.result.Trading
		t.SignalCoverage = safeDiv(float64(t.SignalCount), float64(a.result.Classification.Rows))
		t.ExpectancyInterpretation = "average per-decision labeled trade outcome among emitted signals; not portfolio PnL"
		if t.SignalCount > 0 {
			t.SignalWinRate = OptionalFloat{true, safeDiv(float64(t.ActualNetProfitableCount), float64(t.SignalCount))}
			t.MeanNetReturnExFunding = OptionalFloat{true, a.netSum / float64(t.SignalCount)}
			t.MeanGrossMarketReturn = OptionalFloat{true, a.grossSum / float64(t.SignalCount)}
			sort.Float64s(a.netReturns)
			middle := len(a.netReturns) / 2
			median := a.netReturns[middle]
			if len(a.netReturns)%2 == 0 {
				median = (a.netReturns[middle-1] + median) / 2
			}
			t.MedianNetReturnExFunding = OptionalFloat{true, median}
		}
	}
	setRank := func(name string, positive, negative []float64, descending bool) {
		roc, ap := RankingMetrics(positive, negative, descending)
		e.results[name].result.ROCAUC = OptionalFloat{true, roc}
		e.results[name].result.AveragePrecision = OptionalFloat{true, ap}
	}
	longDirection := e.Side == "LONG"
	setRank(Momentum60s, e.retPositive, e.retNegative, longDirection)
	setRank(MeanReversion60s, e.retPositive, e.retNegative, !longDirection)
	setRank(TakerFlow60s, e.takerPositive, e.takerNegative, longDirection)
	prevalenceResult := &e.results[TrainPrevalence].result
	prevalenceResult.ROCAUC = OptionalFloat{true, .5}
	prevalenceResult.AveragePrecision = OptionalFloat{true, safeDiv(float64(prevalenceResult.Classification.Positive), float64(prevalenceResult.Classification.Rows))}
	p := math.Max(LogLossEpsilon, math.Min(1-LogLossEpsilon, e.Prevalence))
	actualRate := safeDiv(float64(prevalenceResult.Classification.Positive), float64(prevalenceResult.Classification.Rows))
	prevalenceResult.BrierScore = OptionalFloat{true, actualRate*(1-p)*(1-p) + (1-actualRate)*p*p}
	prevalenceResult.LogLoss = OptionalFloat{true, -actualRate*math.Log(p) - (1-actualRate)*math.Log(1-p)}
	result := make([]Result, 0, len(e.results))
	for _, name := range []string{AlwaysNegative, AlwaysPositive, TrainPrevalence, Momentum60s, MeanReversion60s, TakerFlow60s} {
		result = append(result, e.results[name].result)
	}
	return result
}
