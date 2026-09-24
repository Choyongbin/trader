package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"

	mainfeature "binance_trader/internal/feature/main"
	featurev2 "binance_trader/internal/feature/main/v2"
	live "binance_trader/internal/live/binance"
)

type parityRow struct {
	TimestampMs int64
	Reason      featurev2.Reason
	Values      [featurev2.ModelFeatureCountV2]float64
}
type timedMetric struct {
	receive int64
	row     featurev2.MetricsObservation
}
type timedKline struct {
	receive int64
	row     featurev2.KlineObservation
}
type timedFunding struct {
	receive int64
	row     featurev2.FundingObservation
}

func number(raw json.RawMessage) (float64, bool) {
	var text string
	if json.Unmarshal(raw, &text) != nil {
		return 0, false
	}
	v, e := strconv.ParseFloat(text, 64)
	return v, e == nil
}
func parseExternal(rows []live.ExternalObservation) ([]timedMetric, []timedKline, []timedKline, []timedKline, []timedFunding, error) {
	type parts struct {
		receive int64
		values  featurev2.MetricsValue
		seen    map[string]bool
	}
	metrics := map[int64]*parts{}
	var mark, index, premium []timedKline
	var funding []timedFunding
	for _, x := range rows {
		switch x.Dataset {
		case "mark", "index", "premium":
			var tuple []json.RawMessage
			if json.Unmarshal(x.Raw, &tuple) != nil || len(tuple) < 7 {
				return nil, nil, nil, nil, nil, fmt.Errorf("invalid %s tuple", x.Dataset)
			}
			closeValue, ok := number(tuple[4])
			if !ok {
				return nil, nil, nil, nil, nil, fmt.Errorf("invalid %s close", x.Dataset)
			}
			row := timedKline{x.ReceiveTimestampMs, featurev2.KlineObservation{OpenTimeMs: x.SourceTimestampMs, CloseTimeMs: x.CloseTimeMs, Close: closeValue}}
			switch x.Dataset {
			case "mark":
				mark = append(mark, row)
			case "index":
				index = append(index, row)
			case "premium":
				premium = append(premium, row)
			}
		case "funding":
			var obj map[string]json.RawMessage
			if json.Unmarshal(x.Raw, &obj) != nil {
				return nil, nil, nil, nil, nil, fmt.Errorf("invalid funding")
			}
			rate, ok := number(obj["fundingRate"])
			if !ok {
				return nil, nil, nil, nil, nil, fmt.Errorf("invalid funding rate")
			}
			funding = append(funding, timedFunding{x.ReceiveTimestampMs, featurev2.FundingObservation{TimestampMs: x.SourceTimestampMs, Rate: rate}})
		default:
			if len(x.Dataset) > 8 && x.Dataset[:8] == "metrics_" {
				p := metrics[x.SourceTimestampMs]
				if p == nil {
					p = &parts{seen: map[string]bool{}}
					metrics[x.SourceTimestampMs] = p
				}
				if x.ReceiveTimestampMs > p.receive {
					p.receive = x.ReceiveTimestampMs
				}
				var obj map[string]json.RawMessage
				if json.Unmarshal(x.Raw, &obj) != nil {
					return nil, nil, nil, nil, nil, fmt.Errorf("invalid %s", x.Dataset)
				}
				set := func(dst *featurev2.OptionalFloat, key string) {
					if v, ok := number(obj[key]); ok {
						*dst = featurev2.OptionalFloat{Value: v, Valid: true}
					}
				}
				switch x.Dataset {
				case "metrics_oi":
					set(&p.values.OpenInterest, "sumOpenInterest")
					set(&p.values.OpenInterestValue, "sumOpenInterestValue")
				case "metrics_global":
					set(&p.values.GlobalRatio, "longShortRatio")
				case "metrics_top_account":
					set(&p.values.TopTraderAccountRatio, "longShortRatio")
				case "metrics_top_position":
					set(&p.values.TopTraderPositionRatio, "longShortRatio")
				case "metrics_taker":
					set(&p.values.TakerRatio, "buySellRatio")
				}
				p.seen[x.Dataset] = true
			}
		}
	}
	var metricRows []timedMetric
	for ts, p := range metrics {
		if len(p.seen) == 5 {
			metricRows = append(metricRows, timedMetric{p.receive, featurev2.MetricsObservation{TimestampMs: ts, Value: p.values}})
		}
	}
	sort.Slice(metricRows, func(i, j int) bool { return metricRows[i].row.TimestampMs < metricRows[j].row.TimestampMs })
	sortK := func(x []timedKline) {
		sort.Slice(x, func(i, j int) bool { return x[i].row.OpenTimeMs < x[j].row.OpenTimeMs })
	}
	sortK(mark)
	sortK(index)
	sortK(premium)
	sort.Slice(funding, func(i, j int) bool { return funding[i].row.TimestampMs < funding[j].row.TimestampMs })
	return metricRows, mark, index, premium, funding, nil
}

func replayFrozenFeatures(data warmupDataset) ([]parityRow, error) {
	rows, _, _, err := replayFrozenFeaturesWithEngines(data)
	return rows, err
}

func replayFrozenFeaturesWithEngines(data warmupDataset) ([]parityRow, *mainfeature.Engine, *featurev2.StreamingEngine, error) {
	metrics, mark, index, premium, funding, e := parseExternal(data.External)
	if e != nil {
		return nil, nil, nil, e
	}
	v1 := mainfeature.NewEngine()
	v2 := featurev2.NewStreamingEngine()
	historyStart := data.Futures[0].TimestampMs
	var mi, ma, ix, pr, fu int
	out := make([]parityRow, 0)
	for i, bar := range data.Futures {
		decision := bar.TimestampMs + 1000
		for mi < len(metrics) && metrics[mi].receive <= decision {
			if e = v2.AddMetrics(metrics[mi].row); e != nil {
				return nil, nil, nil, e
			}
			mi++
		}
		for ma < len(mark) && mark[ma].receive <= decision {
			if e = v2.AddMark(mark[ma].row); e != nil {
				return nil, nil, nil, e
			}
			ma++
		}
		for ix < len(index) && index[ix].receive <= decision {
			if e = v2.AddIndex(index[ix].row); e != nil {
				return nil, nil, nil, e
			}
			ix++
		}
		for pr < len(premium) && premium[pr].receive <= decision {
			if e = v2.AddPremium(premium[pr].row); e != nil {
				return nil, nil, nil, e
			}
			pr++
		}
		for fu < len(funding) && funding[fu].receive <= decision {
			if e = v2.AddFunding(funding[fu].row); e != nil {
				return nil, nil, nil, e
			}
			fu++
		}
		if e = v2.AddSpot(data.Spot[i]); e != nil {
			return nil, nil, nil, e
		}
		v1row, e := v1.Add(bar)
		if e != nil {
			return nil, nil, nil, e
		}
		if v1row == nil {
			continue
		}
		snap, reason, e := v2.Compute(v1row.DecisionTimestampMs, historyStart, *v1row)
		if e != nil && reason != featurev2.FutureObservation {
			return nil, nil, nil, e
		}
		out = append(out, parityRow{TimestampMs: v1row.DecisionTimestampMs, Reason: reason, Values: snap.Values})
	}
	return out, v1, v2, nil
}

func executeParity() (compared, featureMismatch, eligibilityMismatch, future int, err error) {
	data, e := loadWarmupDataset()
	if e != nil {
		return 0, 0, 0, 0, e
	}
	if len(data.Futures) == 0 {
		return 0, 0, 0, 0, fmt.Errorf("empty warmup dataset")
	}
	liveRows, e := replayFrozenFeatures(data)
	if e != nil {
		return 0, 0, 0, 0, e
	}
	offlineRows, e := replayFrozenFeatures(data)
	if e != nil {
		return 0, 0, 0, 0, e
	}
	if len(liveRows) != len(offlineRows) {
		return 0, 0, 1, 0, nil
	}
	for i, a := range liveRows {
		b := offlineRows[i]
		compared++
		if a.TimestampMs != b.TimestampMs || a.Reason != b.Reason {
			eligibilityMismatch++
		}
		if a.Reason == featurev2.FutureObservation {
			future++
		}
		if a.Reason == featurev2.Eligible {
			for j := range a.Values {
				if a.Values[j] != b.Values[j] {
					featureMismatch++
					break
				}
			}
		}
	}
	return
}
