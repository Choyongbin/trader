package featurev2

import (
	"fmt"
	"math"

	"binance_trader/internal/external/asof"
	mainfeature "binance_trader/internal/feature/main"
	"binance_trader/internal/feature/specv2"
	"binance_trader/internal/market"
)

type spotAggregate struct {
	bar                        market.SecondBar
	cumulativeQuote            float64
	cumulativeBuy              float64
	cumulativeSell             float64
	cumulativeTrades, sequence int64
}

// StreamingEngine is the production Feature V2 calculator. It retains only
// past source observations needed by the frozen formulas.
type StreamingEngine struct {
	spot                               map[int64]spotAggregate
	spotQuote, spotBuy, spotSell       float64
	spotTrades, spotSequence, lastSpot int64
	metrics                            []MetricsObservation
	mark, index, premium               []KlineObservation
	funding                            []FundingObservation
}

func NewStreamingEngine() *StreamingEngine {
	return &StreamingEngine{spot: make(map[int64]spotAggregate, 320), lastSpot: -1}
}

func (e *StreamingEngine) AddSpot(row market.SecondBar) error {
	if e.lastSpot >= 0 && row.TimestampMs <= e.lastSpot {
		return fmt.Errorf("spot observations not strictly ordered")
	}
	e.lastSpot = row.TimestampMs
	e.spotQuote += row.QuoteVolume
	e.spotBuy += row.TakerBuyQuoteVolume
	e.spotSell += row.TakerSellQuoteVolume
	e.spotTrades += row.AggTradeCount
	e.spotSequence++
	e.spot[row.TimestampMs] = spotAggregate{row, e.spotQuote, e.spotBuy, e.spotSell, e.spotTrades, e.spotSequence}
	return nil
}

func appendStrict[T any](rows []T, timestamp func(T) int64, row T, source string) ([]T, error) {
	if len(rows) > 0 && timestamp(row) <= timestamp(rows[len(rows)-1]) {
		return rows, fmt.Errorf("%s observations not strictly ordered", source)
	}
	return append(rows, row), nil
}

func (e *StreamingEngine) AddMetrics(row MetricsObservation) (err error) {
	e.metrics, err = appendStrict(e.metrics, func(x MetricsObservation) int64 { return x.TimestampMs }, row, "metrics")
	return err
}
func (e *StreamingEngine) AddMark(row KlineObservation) (err error) {
	e.mark, err = appendStrict(e.mark, func(x KlineObservation) int64 { return x.OpenTimeMs }, row, "mark")
	return err
}
func (e *StreamingEngine) AddIndex(row KlineObservation) (err error) {
	e.index, err = appendStrict(e.index, func(x KlineObservation) int64 { return x.OpenTimeMs }, row, "index")
	return err
}
func (e *StreamingEngine) AddPremium(row KlineObservation) (err error) {
	e.premium, err = appendStrict(e.premium, func(x KlineObservation) int64 { return x.OpenTimeMs }, row, "premium")
	return err
}
func (e *StreamingEngine) AddFunding(row FundingObservation) (err error) {
	e.funding, err = appendStrict(e.funding, func(x FundingObservation) int64 { return x.TimestampMs }, row, "funding")
	return err
}

func selectMetrics(rows []MetricsObservation, decision int64) *selected[MetricsValue] {
	for i := len(rows) - 1; i >= 0; i-- {
		if rows[i].TimestampMs+asof.ExternalSafetyLagMs <= decision {
			age := decision - rows[i].TimestampMs
			return &selected[MetricsValue]{value: rows[i].Value, state: SourceState{Available: true, Fresh: age <= asof.MetricsMaxFreshAgeMs, EffectiveAvailableAtMs: rows[i].TimestampMs + asof.ExternalSafetyLagMs}, sourceTimestampMs: rows[i].TimestampMs}
		}
	}
	return nil
}

func selectKline(rows []KlineObservation, decision int64) *selected[float64] {
	for i := len(rows) - 1; i >= 0; i-- {
		boundary := rows[i].OpenTimeMs + 60000
		if rows[i].CloseTimeMs != 0 {
			boundary = rows[i].CloseTimeMs + 1
		}
		effective := boundary + asof.ExternalSafetyLagMs
		if effective <= decision {
			age := decision - rows[i].OpenTimeMs
			return &selected[float64]{value: rows[i].Close, state: SourceState{Available: true, Fresh: age <= asof.KlineMaxFreshAgeMs, EffectiveAvailableAtMs: effective}, sourceTimestampMs: rows[i].OpenTimeMs}
		}
	}
	return nil
}

func selectFunding(rows []FundingObservation, decision int64) (current, previous *selected[float64]) {
	for i := len(rows) - 1; i >= 0; i-- {
		effective := rows[i].TimestampMs + asof.ExternalSafetyLagMs
		if effective <= decision {
			age := decision - rows[i].TimestampMs
			current = &selected[float64]{value: rows[i].Rate, state: SourceState{Available: true, Fresh: age <= asof.FundingMaxFreshAgeMs, EffectiveAvailableAtMs: effective}, sourceTimestampMs: rows[i].TimestampMs}
			if i > 0 {
				prior := rows[i-1]
				previous = &selected[float64]{value: prior.Rate, state: SourceState{Available: true, Fresh: true, EffectiveAvailableAtMs: prior.TimestampMs + asof.ExternalSafetyLagMs}, sourceTimestampMs: prior.TimestampMs}
			}
			return current, previous
		}
	}
	return nil, nil
}

func (e *StreamingEngine) computeSpot(out map[string]calculated, decision int64, v1 mainfeature.MainFeaturesV1) bool {
	current, currentOK := e.spot[decision-1000]
	windows := map[int64]struct{ ret, volume, imbalance, intensity calculated }{}
	lookbackReady := currentOK
	for _, seconds := range []int64{5, 30, 60, 300} {
		base, baseOK := e.spot[decision-(seconds+1)*1000]
		complete := currentOK && baseOK && current.sequence-base.sequence == seconds
		if !complete {
			lookbackReady = false
		}
		ret, volume, imbalance, intensity := invalid(), invalid(), invalid(), invalid()
		if complete && current.bar.Close > 0 && base.bar.Close > 0 {
			ret = valid(math.Log(current.bar.Close / base.bar.Close))
		}
		if complete {
			quote := current.cumulativeQuote - base.cumulativeQuote
			buy := current.cumulativeBuy - base.cumulativeBuy
			sell := current.cumulativeSell - base.cumulativeSell
			count := current.cumulativeTrades - base.cumulativeTrades
			if AllFinite(quote, buy, sell) {
				volume = valid(quote)
				intensity = valid(float64(count) / float64(seconds))
				if quote > 0 {
					imbalance = valid((buy - sell) / quote)
				}
			}
		}
		windows[seconds] = struct{ ret, volume, imbalance, intensity calculated }{ret, volume, imbalance, intensity}
		out[fmt.Sprintf("spot_return_%ds", seconds)] = ret
		if seconds != 300 {
			out[fmt.Sprintf("spot_taker_imbalance_%ds", seconds)] = imbalance
		}
		if seconds == 30 || seconds == 300 {
			out[fmt.Sprintf("spot_volume_%ds", seconds)] = volume
		}
	}
	out["spot_trade_intensity_30s"] = windows[30].intensity
	perpReturns := map[int64]float64{5: v1.RetLog5s, 30: v1.RetLog30s, 60: v1.RetLog60s, 300: v1.RetLog300s}
	perpImbalance := map[int64]float64{5: v1.TakerImbalance5s, 30: v1.TakerImbalance30s, 60: v1.TakerImbalance60s}
	perpVolume := map[int64]float64{30: v1.QuoteVolumeSum30s, 300: v1.QuoteVolumeSum300s}
	for seconds, value := range perpReturns {
		out[fmt.Sprintf("spot_perp_return_gap_%ds", seconds)] = binary(windows[seconds].ret, valid(value), func(a, b float64) float64 { return a - b })
	}
	for seconds, value := range perpImbalance {
		out[fmt.Sprintf("spot_perp_taker_imbalance_gap_%ds", seconds)] = binary(windows[seconds].imbalance, valid(value), func(a, b float64) float64 { return a - b })
	}
	for seconds, value := range perpVolume {
		out[fmt.Sprintf("spot_perp_volume_ratio_%ds", seconds)] = binary(windows[seconds].volume, valid(value), func(a, b float64) float64 { return math.Log1p(a) - math.Log1p(b) })
	}
	if currentOK && current.bar.Close > 0 && v1.ReferenceClose > 0 {
		out["spot_perp_spread_bps"] = valid(10000 * (current.bar.Close/v1.ReferenceClose - 1))
	} else {
		out["spot_perp_spread_bps"] = invalid()
	}
	return lookbackReady
}

func (e *StreamingEngine) Compute(decision, historyStart int64, v1row mainfeature.MainFeaturesV1) (Snapshot, Reason, error) {
	if v1row.DecisionTimestampMs != decision {
		return Snapshot{}, LookbackUnavailable, fmt.Errorf("V1 decision timestamp mismatch")
	}
	v1, err := v1Values(v1row)
	if err != nil {
		return Snapshot{}, NonFiniteFeature, err
	}
	metricsCurrent := selectMetrics(e.metrics, decision)
	metrics5 := selectMetrics(e.metrics, decision-300000)
	metrics15 := selectMetrics(e.metrics, decision-900000)
	metrics60 := selectMetrics(e.metrics, decision-3600000)
	markCurrent, indexCurrent, premiumCurrent := selectKline(e.mark, decision), selectKline(e.index, decision), selectKline(e.premium, decision)
	mark5, index5 := selectKline(e.mark, decision-300000), selectKline(e.index, decision-300000)
	premium5, premium15 := selectKline(e.premium, decision-300000), selectKline(e.premium, decision-900000)
	fundingCurrent, fundingPrior := selectFunding(e.funding, decision)
	values := make(map[string]calculated, NewModelFeatureCount)
	spotLookback := e.computeSpot(values, decision, v1row)
	computeMetrics(values, decision, metricsCurrent, metrics5, metrics15, metrics60, v1row)
	computeKlines(values, decision, v1row, markCurrent, indexCurrent, premiumCurrent, mark5, index5, premium5, premium15)
	computeFunding(values, decision, fundingCurrent, fundingPrior)
	allNewFinite := true
	for _, feature := range specv2.Amended() {
		if feature.Decision != specv2.Keep {
			continue
		}
		x, ok := values[feature.FeatureName]
		if !ok {
			return Snapshot{}, NonFiniteFeature, fmt.Errorf("feature not computed: %s", feature.FeatureName)
		}
		if !x.valid || !AllFinite(x.value) {
			allNewFinite = false
		}
	}
	lookbackReady := spotLookback && endpointReady(metrics5) && endpointReady(metrics15) && endpointReady(metrics60) && endpointReady(mark5) && endpointReady(index5) && endpointReady(premium5) && endpointReady(premium15) && fundingPrior != nil
	result, evalErr := Evaluate(Input{DecisionTimestampMs: decision, Spot: spotState(e.spot, decision), Metrics: stateOf(metricsCurrent), Mark: stateOf(markCurrent), Index: stateOf(indexCurrent), Premium: stateOf(premiumCurrent), Funding: stateOf(fundingCurrent), WarmupReady: decision-historyStart >= RequiredWarmupMs, LookbackReady: lookbackReady, FeaturesFinite: AllFinite(v1...) && allNewFinite})
	if evalErr != nil || !result.Eligible {
		return Snapshot{}, result.Reason, evalErr
	}
	s := Snapshot{DecisionTimestampMs: decision}
	copy(s.Values[:V1ModelFeatureCount], v1)
	at := V1ModelFeatureCount
	for _, feature := range specv2.Amended() {
		if feature.Decision == specv2.Keep {
			s.Values[at] = values[feature.FeatureName].value
			at++
		}
	}
	if at != ModelFeatureCountV2 || !AllFinite(s.Values[:]...) {
		return Snapshot{}, NonFiniteFeature, fmt.Errorf("invalid emitted snapshot")
	}
	return s, Eligible, nil
}

func spotState(rows map[int64]spotAggregate, decision int64) SourceState {
	_, ok := rows[decision-1000]
	return SourceState{Available: ok, Fresh: ok, EffectiveAvailableAtMs: decision}
}

// Prune keeps bounded history without changing cumulative-window semantics.
func (e *StreamingEngine) Prune(decision int64) {
	for ts := range e.spot {
		if ts < decision-301000 {
			delete(e.spot, ts)
		}
	}
	e.metrics = pruneMetrics(e.metrics, decision-3700000)
	e.mark = pruneKlines(e.mark, decision-1000000)
	e.index = pruneKlines(e.index, decision-1000000)
	e.premium = pruneKlines(e.premium, decision-1000000)
	if len(e.funding) > 3 {
		e.funding = append([]FundingObservation(nil), e.funding[len(e.funding)-3:]...)
	}
}

func pruneMetrics(rows []MetricsObservation, cutoff int64) []MetricsObservation {
	i := 0
	for i+1 < len(rows) && rows[i+1].TimestampMs < cutoff {
		i++
	}
	if i == 0 {
		return rows
	}
	return append([]MetricsObservation(nil), rows[i:]...)
}
func pruneKlines(rows []KlineObservation, cutoff int64) []KlineObservation {
	i := 0
	for i+1 < len(rows) && rows[i+1].OpenTimeMs < cutoff {
		i++
	}
	if i == 0 {
		return rows
	}
	return append([]KlineObservation(nil), rows[i:]...)
}
