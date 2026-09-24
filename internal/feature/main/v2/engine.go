package featurev2

import (
	"fmt"
	"math"
	"reflect"
	"strings"

	"binance_trader/internal/external/asof"
	mainfeature "binance_trader/internal/feature/main"
	"binance_trader/internal/market"
)

type OptionalFloat struct {
	Value float64
	Valid bool
}

type MetricsValue struct {
	OpenInterest, OpenInterestValue                                        OptionalFloat
	TopTraderAccountRatio, TopTraderPositionRatio, GlobalRatio, TakerRatio OptionalFloat
}

type MetricsObservation struct {
	TimestampMs int64
	Value       MetricsValue
}

type KlineObservation struct {
	OpenTimeMs, CloseTimeMs int64
	Close                   float64
}

type FundingObservation struct {
	TimestampMs int64
	Rate        float64
}

type ComputeInput struct {
	DecisionTimestampMs     int64
	HistoryStartTimestampMs int64
	V1                      mainfeature.MainFeaturesV1
	Spot                    []market.SecondBar
	Metrics                 []MetricsObservation
	Mark, Index, Premium    []KlineObservation
	Funding                 []FundingObservation
}

type Snapshot struct {
	DecisionTimestampMs int64
	Values              [ModelFeatureCountV2]float64
}

type calculated struct {
	value float64
	valid bool
}

func valid(value float64) calculated { return calculated{value: value, valid: AllFinite(value)} }
func invalid() calculated            { return calculated{} }

func Compute(in ComputeInput) (Snapshot, Reason, error) {
	e := NewStreamingEngine()
	for _, row := range in.Spot {
		if err := e.AddSpot(row); err != nil {
			return Snapshot{}, SpotUnavailable, err
		}
	}
	for _, row := range in.Metrics {
		if err := e.AddMetrics(row); err != nil {
			return Snapshot{}, MetricsUnavailable, err
		}
	}
	for _, row := range in.Mark {
		if err := e.AddMark(row); err != nil {
			return Snapshot{}, KlineUnavailable, err
		}
	}
	for _, row := range in.Index {
		if err := e.AddIndex(row); err != nil {
			return Snapshot{}, KlineUnavailable, err
		}
	}
	for _, row := range in.Premium {
		if err := e.AddPremium(row); err != nil {
			return Snapshot{}, KlineUnavailable, err
		}
	}
	for _, row := range in.Funding {
		if err := e.AddFunding(row); err != nil {
			return Snapshot{}, FundingUnavailable, err
		}
	}
	return e.Compute(in.DecisionTimestampMs, in.HistoryStartTimestampMs, in.V1)
}

type selected[T any] struct {
	value             T
	state             SourceState
	sourceTimestampMs int64
}

func stateOf[T any](x *selected[T]) SourceState {
	if x == nil {
		return SourceState{}
	}
	return x.state
}

func endpointReady[T any](x *selected[T]) bool { return x != nil && x.state.Available && x.state.Fresh }

func lookupSpot(rows []market.SecondBar, decision int64) (*selected[market.SecondBar], error) {
	obs := make([]asof.Observation[market.SecondBar], len(rows))
	for i, row := range rows {
		obs[i] = asof.Observation[market.SecondBar]{Value: row, SourceTimestampMs: row.TimestampMs}
	}
	x, err := asof.Lookup(asof.Spot, obs, decision)
	if err != nil {
		return nil, err
	}
	if !x.Available {
		return nil, nil
	}
	return &selected[market.SecondBar]{value: x.Value, state: SourceState{Available: true, Fresh: x.Fresh, EffectiveAvailableAtMs: x.EffectiveAvailableAtMs}, sourceTimestampMs: x.SourceTimestampMs}, nil
}

func lookupMetrics(rows []MetricsObservation, decision int64) (*selected[MetricsValue], error) {
	obs := make([]asof.Observation[MetricsValue], len(rows))
	for i, row := range rows {
		obs[i] = asof.Observation[MetricsValue]{Value: row.Value, SourceTimestampMs: row.TimestampMs}
	}
	x, err := asof.Lookup(asof.Metrics, obs, decision)
	if err != nil {
		return nil, err
	}
	if !x.Available {
		return nil, nil
	}
	return &selected[MetricsValue]{value: x.Value, state: SourceState{Available: true, Fresh: x.Fresh, EffectiveAvailableAtMs: x.EffectiveAvailableAtMs}, sourceTimestampMs: x.SourceTimestampMs}, nil
}

func lookupKline(source asof.Source, rows []KlineObservation, decision int64) (*selected[float64], error) {
	obs := make([]asof.Observation[float64], len(rows))
	for i, row := range rows {
		obs[i] = asof.Observation[float64]{Value: row.Close, SourceTimestampMs: row.OpenTimeMs, CloseTimeMs: row.CloseTimeMs}
	}
	x, err := asof.Lookup(source, obs, decision)
	if err != nil {
		return nil, err
	}
	if !x.Available {
		return nil, nil
	}
	return &selected[float64]{value: x.Value, state: SourceState{Available: true, Fresh: x.Fresh, EffectiveAvailableAtMs: x.EffectiveAvailableAtMs}, sourceTimestampMs: x.SourceTimestampMs}, nil
}

func lookupFunding(rows []FundingObservation, decision int64) (*selected[float64], error) {
	obs := make([]asof.Observation[float64], len(rows))
	for i, row := range rows {
		obs[i] = asof.Observation[float64]{Value: row.Rate, SourceTimestampMs: row.TimestampMs}
	}
	x, err := asof.Lookup(asof.Funding, obs, decision)
	if err != nil {
		return nil, err
	}
	if !x.Available {
		return nil, nil
	}
	return &selected[float64]{value: x.Value, state: SourceState{Available: true, Fresh: x.Fresh, EffectiveAvailableAtMs: x.EffectiveAvailableAtMs}, sourceTimestampMs: x.SourceTimestampMs}, nil
}

func previousFunding(rows []FundingObservation, decision int64) *selected[float64] {
	var previous, current *selected[float64]
	for _, row := range rows {
		effective := row.TimestampMs + asof.ExternalSafetyLagMs
		if effective > decision {
			break
		}
		previous = current
		current = &selected[float64]{value: row.Rate, state: SourceState{Available: true, Fresh: true, EffectiveAvailableAtMs: effective}, sourceTimestampMs: row.TimestampMs}
	}
	return previous
}

func spotWindowReady(rows []market.SecondBar, decision int64) bool {
	byTime := make(map[int64]bool, len(rows))
	for _, row := range rows {
		byTime[row.TimestampMs] = true
	}
	for ts := decision - 301000; ts < decision; ts += 1000 {
		if !byTime[ts] {
			return false
		}
	}
	return true
}

func computeSpot(out map[string]calculated, rows []market.SecondBar, decision int64, v1 mainfeature.MainFeaturesV1) {
	byTime := make(map[int64]market.SecondBar, len(rows))
	for _, row := range rows {
		if row.TimestampMs < decision {
			byTime[row.TimestampMs] = row
		}
	}
	current, currentOK := byTime[decision-1000]
	windows := map[int64]struct{ ret, volume, imbalance, intensity calculated }{}
	for _, seconds := range []int64{5, 30, 60, 300} {
		base, baseOK := byTime[decision-(seconds+1)*1000]
		ret := invalid()
		if currentOK && baseOK && current.Close > 0 && base.Close > 0 {
			ret = valid(math.Log(current.Close / base.Close))
		}
		var quote, buy, sell float64
		var count int64
		complete := true
		for ts := decision - seconds*1000; ts < decision; ts += 1000 {
			bar, ok := byTime[ts]
			if !ok {
				complete = false
				break
			}
			quote += bar.QuoteVolume
			buy += bar.TakerBuyQuoteVolume
			sell += bar.TakerSellQuoteVolume
			count += bar.AggTradeCount
		}
		volume, imbalance, intensity := invalid(), invalid(), invalid()
		if complete && AllFinite(quote, buy, sell) {
			volume = valid(quote)
			intensity = valid(float64(count) / float64(seconds))
			if quote > 0 {
				imbalance = valid((buy - sell) / quote)
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
	if currentOK && current.Close > 0 && v1.ReferenceClose > 0 {
		out["spot_perp_spread_bps"] = valid(10000 * (current.Close/v1.ReferenceClose - 1))
	} else {
		out["spot_perp_spread_bps"] = invalid()
	}
}

func computeMetrics(out map[string]calculated, decision int64, current, m5, m15, m60 *selected[MetricsValue], v1 mainfeature.MainFeaturesV1) {
	change := func(cur, ref OptionalFloat) calculated {
		if !cur.Valid || !ref.Valid || !AllFinite(cur.Value, ref.Value) || cur.Value <= 0 || ref.Value <= 0 {
			return invalid()
		}
		return valid((cur.Value - ref.Value) / ref.Value)
	}
	if current == nil {
		return
	}
	cur := current.value
	refs := map[int64]*selected[MetricsValue]{5: m5, 15: m15, 60: m60}
	for minutes, ref := range refs {
		oi, oiv := invalid(), invalid()
		if ref != nil {
			oi = change(cur.OpenInterest, ref.value.OpenInterest)
			oiv = change(cur.OpenInterestValue, ref.value.OpenInterestValue)
		}
		out[fmt.Sprintf("oi_change_pct_%dm", minutes)] = oi
		out[fmt.Sprintf("oi_value_change_pct_%dm", minutes)] = oiv
	}
	out["price_return_x_oi_change_5m"] = binary(valid(v1.RetLog300s), out["oi_change_pct_5m"], func(a, b float64) float64 { return a * b })
	out["price_return_x_oi_change_15m"] = binary(valid(v1.RetLog900s), out["oi_change_pct_15m"], func(a, b float64) float64 { return a * b })
	positioning := []struct {
		name string
		cur  OptionalFloat
		ref  func(MetricsValue) OptionalFloat
	}{
		{"top_trader_account_ls_ratio", cur.TopTraderAccountRatio, func(v MetricsValue) OptionalFloat { return v.TopTraderAccountRatio }},
		{"top_trader_position_ls_ratio", cur.TopTraderPositionRatio, func(v MetricsValue) OptionalFloat { return v.TopTraderPositionRatio }},
		{"global_ls_ratio", cur.GlobalRatio, func(v MetricsValue) OptionalFloat { return v.GlobalRatio }},
		{"taker_long_short_volume_ratio", cur.TakerRatio, func(v MetricsValue) OptionalFloat { return v.TakerRatio }},
	}
	for _, p := range positioning {
		if p.cur.Valid && AllFinite(p.cur.Value) && p.cur.Value > 0 {
			out[p.name] = valid(p.cur.Value)
		} else {
			out[p.name] = invalid()
		}
		if m5 != nil {
			out[p.name+"_change_5m"] = change(p.cur, p.ref(m5.value))
		} else {
			out[p.name+"_change_5m"] = invalid()
		}
	}
	if cur.TopTraderPositionRatio.Valid && cur.GlobalRatio.Valid && cur.TopTraderPositionRatio.Value > 0 && cur.GlobalRatio.Value > 0 {
		out["top_vs_global_positioning_gap"] = valid(math.Log(cur.TopTraderPositionRatio.Value / cur.GlobalRatio.Value))
	} else {
		out["top_vs_global_positioning_gap"] = invalid()
	}
	out["metrics_age_ms"] = valid(float64(currentAge(current, decision)))
}

func computeKlines(out map[string]calculated, decision int64, v1 mainfeature.MainFeaturesV1, mark, index, premium, mark5, index5, premium5, premium15 *selected[float64]) {
	spread := func(a, b *selected[float64]) calculated {
		if a == nil || b == nil || a.value <= 0 || b.value <= 0 {
			return invalid()
		}
		return valid(10000 * (a.value/b.value - 1))
	}
	mi := spread(mark, index)
	out["mark_index_spread_bps"] = mi
	if index != nil && index.value > 0 && v1.ReferenceClose > 0 {
		out["contract_index_spread_bps"] = valid(10000 * (v1.ReferenceClose/index.value - 1))
	} else {
		out["contract_index_spread_bps"] = invalid()
	}
	if premium != nil {
		out["premium_close"] = valid(premium.value)
	} else {
		out["premium_close"] = invalid()
	}
	if premium != nil && premium5 != nil {
		out["premium_change_5m"] = valid(premium.value - premium5.value)
	} else {
		out["premium_change_5m"] = invalid()
	}
	if premium != nil && premium15 != nil {
		out["premium_change_15m"] = valid(premium.value - premium15.value)
	} else {
		out["premium_change_15m"] = invalid()
	}
	out["mark_index_spread_change_5m"] = binary(mi, spread(mark5, index5), func(a, b float64) float64 { return a - b })
	if mark != nil && index != nil && premium != nil {
		age := max64(decision-mark.sourceTimestampMs, decision-index.sourceTimestampMs, decision-premium.sourceTimestampMs)
		out["kline_age_ms"] = valid(float64(age))
	} else {
		out["kline_age_ms"] = invalid()
	}
}

func computeFunding(out map[string]calculated, decision int64, current, prior *selected[float64]) {
	if current == nil {
		return
	}
	out["last_funding_rate"] = valid(current.value)
	out["funding_age_ms"] = valid(float64(decision - current.sourceTimestampMs))
	if prior != nil {
		out["funding_rate_change"] = valid(current.value - prior.value)
	} else {
		out["funding_rate_change"] = invalid()
	}
}

func currentAge[T any](x *selected[T], decision int64) int64 { return decision - x.sourceTimestampMs }
func max64(values ...int64) int64 {
	m := values[0]
	for _, v := range values[1:] {
		if v > m {
			m = v
		}
	}
	return m
}
func binary(a, b calculated, fn func(float64, float64) float64) calculated {
	if !a.valid || !b.valid {
		return invalid()
	}
	return valid(fn(a.value, b.value))
}

func v1Values(features mainfeature.MainFeaturesV1) ([]float64, error) {
	v := reflect.ValueOf(features)
	t := v.Type()
	result := make([]float64, 0, V1ModelFeatureCount)
	for i := 0; i < t.NumField(); i++ {
		name := strings.Split(t.Field(i).Tag.Get("parquet"), ",")[0]
		if name == "decision_timestamp_ms" || name == "reference_close" {
			continue
		}
		result = append(result, v.Field(i).Float())
	}
	if len(result) != V1ModelFeatureCount {
		return nil, fmt.Errorf("V1 values=%d", len(result))
	}
	return result, nil
}
