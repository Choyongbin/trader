package runtimefeature

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"

	featurev2 "binance_trader/internal/feature/main/v2"
	live "binance_trader/internal/live/binance"
)

type externalValue struct {
	dataset             string
	sourceMs, receiveMs int64
	metric              *featurev2.MetricsObservation
	kline               *featurev2.KlineObservation
	funding             *featurev2.FundingObservation
}

func externalNumber(raw json.RawMessage) (float64, error) {
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		return 0, err
	}
	v, err := strconv.ParseFloat(text, 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, fmt.Errorf("invalid external numeric value")
	}
	return v, nil
}

// KlineObservationComplete is shared with post-run diagnostics so a candle
// discarded by production cannot later be reported as usable.
func KlineObservationComplete(x live.ExternalObservation) bool {
	return x.CloseTimeMs >= x.SourceTimestampMs && x.CloseTimeMs+1 <= x.ReceiveTimestampMs
}

// parseExternal preserves the livevalidation source fields and receive times.
// Incomplete kline candles are not published as closed observations.
func parseExternal(rows []live.ExternalObservation) ([]externalValue, error) {
	type metricParts struct {
		receive int64
		seen    map[string]bool
		value   featurev2.MetricsValue
	}
	metrics := map[int64]*metricParts{}
	var out []externalValue
	for _, x := range rows {
		if x.SourceTimestampMs <= 0 || x.ReceiveTimestampMs <= 0 || x.SourceTimestampMs > x.ReceiveTimestampMs {
			return nil, fmt.Errorf("invalid external timestamp")
		}
		switch x.Dataset {
		case "mark", "index", "premium":
			if x.CloseTimeMs < x.SourceTimestampMs {
				return nil, fmt.Errorf("invalid %s close time", x.Dataset)
			}
			if !KlineObservationComplete(x) {
				continue
			}
			var tuple []json.RawMessage
			if json.Unmarshal(x.Raw, &tuple) != nil || len(tuple) < 7 {
				return nil, fmt.Errorf("invalid %s tuple", x.Dataset)
			}
			v, err := externalNumber(tuple[4])
			if err != nil {
				return nil, fmt.Errorf("%s close: %w", x.Dataset, err)
			}
			k := featurev2.KlineObservation{OpenTimeMs: x.SourceTimestampMs, CloseTimeMs: x.CloseTimeMs, Close: v}
			out = append(out, externalValue{dataset: x.Dataset, sourceMs: x.SourceTimestampMs, receiveMs: x.ReceiveTimestampMs, kline: &k})
		case "funding":
			var obj map[string]json.RawMessage
			if json.Unmarshal(x.Raw, &obj) != nil {
				return nil, fmt.Errorf("invalid funding object")
			}
			v, err := externalNumber(obj["fundingRate"])
			if err != nil {
				return nil, fmt.Errorf("funding rate: %w", err)
			}
			f := featurev2.FundingObservation{TimestampMs: x.SourceTimestampMs, Rate: v}
			out = append(out, externalValue{dataset: x.Dataset, sourceMs: x.SourceTimestampMs, receiveMs: x.ReceiveTimestampMs, funding: &f})
		case "metrics_oi", "metrics_global", "metrics_top_account", "metrics_top_position", "metrics_taker":
			p := metrics[x.SourceTimestampMs]
			if p == nil {
				p = &metricParts{seen: map[string]bool{}}
				metrics[x.SourceTimestampMs] = p
			}
			if x.ReceiveTimestampMs > p.receive {
				p.receive = x.ReceiveTimestampMs
			}
			var obj map[string]json.RawMessage
			if json.Unmarshal(x.Raw, &obj) != nil {
				return nil, fmt.Errorf("invalid %s object", x.Dataset)
			}
			set := func(dst *featurev2.OptionalFloat, key string) error {
				v, err := externalNumber(obj[key])
				if err != nil {
					return err
				}
				*dst = featurev2.OptionalFloat{Value: v, Valid: true}
				return nil
			}
			var err error
			switch x.Dataset {
			case "metrics_oi":
				if err = set(&p.value.OpenInterest, "sumOpenInterest"); err == nil {
					err = set(&p.value.OpenInterestValue, "sumOpenInterestValue")
				}
			case "metrics_global":
				err = set(&p.value.GlobalRatio, "longShortRatio")
			case "metrics_top_account":
				err = set(&p.value.TopTraderAccountRatio, "longShortRatio")
			case "metrics_top_position":
				err = set(&p.value.TopTraderPositionRatio, "longShortRatio")
			case "metrics_taker":
				err = set(&p.value.TakerRatio, "buySellRatio")
			}
			if err != nil {
				return nil, fmt.Errorf("%s value: %w", x.Dataset, err)
			}
			p.seen[x.Dataset] = true
		}
	}
	for ts, p := range metrics {
		if len(p.seen) != 5 {
			continue
		}
		m := featurev2.MetricsObservation{TimestampMs: ts, Value: p.value}
		out = append(out, externalValue{dataset: "metrics", sourceMs: ts, receiveMs: p.receive, metric: &m})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].receiveMs == out[j].receiveMs {
			if out[i].dataset == out[j].dataset {
				return out[i].sourceMs < out[j].sourceMs
			}
			return out[i].dataset < out[j].dataset
		}
		return out[i].receiveMs < out[j].receiveMs
	})
	return out, nil
}
