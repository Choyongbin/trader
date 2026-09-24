package specv2

import (
	"fmt"
	"strings"
)

const (
	SpecVersion        = 1
	AmendmentVersion   = 1
	V1FeatureCount     = 80
	MinNewFeatureCount = 30
	MaxNewFeatureCount = 50
)

// Amended applies Feature V2 Spec V1 Amendment 1 without mutating the frozen
// base registry. Only constant freshness masks change from KEEP to DROP.
func Amended() []Feature {
	features := Proposed()
	for i := range features {
		switch features[i].FeatureName {
		case "metrics_fresh", "kline_fresh":
			features[i].Decision = Drop
			features[i].ExistingV1Overlap = ""
			features[i].NewInformationRationale = "CONSTANT_UNDER_STRICT_ELIGIBILITY: strict eligibility가 Fresh=true를 요구하므로 emitted sample에서 항상 1"
		}
	}
	return features
}

type Decision string

const (
	Keep  Decision = "KEEP"
	Drop  Decision = "DROP"
	Defer Decision = "DEFER"
)

type Feature struct {
	FeatureName             string   `json:"feature_name"`
	Source                  string   `json:"source"`
	LookbackMs              int64    `json:"lookback_ms"`
	Window                  string   `json:"window"`
	AvailabilityRule        string   `json:"availability_rule"`
	FreshnessDependency     string   `json:"freshness_dependency"`
	MissingStaleBehavior    string   `json:"missing_stale_behavior"`
	Formula                 string   `json:"formula"`
	ExistingV1Overlap       string   `json:"existing_v1_overlap"`
	NewInformationRationale string   `json:"new_information_rationale"`
	LeakageRule             string   `json:"leakage_rule"`
	Decision                Decision `json:"decision"`
}

func feature(name, source string, lookback int64, window, availability, freshness, missing, formula, rationale string) Feature {
	return Feature{
		FeatureName: name, Source: source, LookbackMs: lookback, Window: window,
		AvailabilityRule: availability, FreshnessDependency: freshness,
		MissingStaleBehavior: missing, Formula: formula,
		NewInformationRationale: rationale,
		LeakageRule:             "decision t 이후 관측 금지; frozen External AS-OF POLICY V1로 선택된 값만 사용",
		Decision:                Keep,
	}
}

const (
	spotAvailability    = "timestamp == t-1000ms인 완료된 Spot 1s bar만 사용"
	spotFreshness       = "exact previous second; 별도 stale 허용 없음"
	spotMissing         = "exact bar 또는 window 구성 bar가 없으면 sample exclusion 권고; 0 대체 금지"
	metricsAvailability = "observation timestamp + 5000ms <= t인 최신 관측만 사용"
	metricsFreshness    = "MetricsMaxFreshAgeMs=600000"
	metricsMissing      = "미관측은 sample exclusion; stale은 값 + age/mask 권고; 보간/0 대체 금지"
	klineAvailability   = "closed 1m bar close boundary + 5000ms <= t인 최신 관측만 사용"
	klineFreshness      = "KlineMaxFreshAgeMs=120000"
	klineMissing        = "미관측은 sample exclusion; stale은 값 + age/mask 권고; 보간/0 대체 금지"
	fundingAvailability = "funding timestamp + 5000ms <= t인 최신 관측만 사용"
	fundingFreshness    = "FundingMaxFreshAgeMs=57600000"
	fundingMissing      = "미관측은 sample exclusion; stale은 값 + funding_age_ms 권고; backfill/0 대체 금지"
)

// Proposed returns the frozen V2 candidate registry. It is a specification only.
func Proposed() []Feature {
	var out []Feature
	for _, w := range []int64{5, 30, 60, 300} {
		out = append(out, feature(fmt.Sprintf("spot_return_%ds", w), "spot_1s", w*1000, fmt.Sprintf("[t-%ds,t)", w), spotAvailability, spotFreshness, spotMissing,
			fmt.Sprintf("sum(log(close_s/close_(s-1))) for Spot bar timestamp s in [t-%ds,t)", w), "현물 가격발견 흐름"))
	}
	for _, w := range []int64{5, 30, 60} {
		out = append(out, feature(fmt.Sprintf("spot_taker_imbalance_%ds", w), "spot_1s", w*1000, fmt.Sprintf("[t-%ds,t)", w), spotAvailability, spotFreshness, spotMissing,
			fmt.Sprintf("sum(taker_buy_quote-taker_sell_quote)/sum(quote_volume) over [t-%ds,t); denominator 0 is unavailable", w), "현물 공격매수/매도 흐름"))
	}
	for _, w := range []int64{30, 300} {
		out = append(out, feature(fmt.Sprintf("spot_volume_%ds", w), "spot_1s", w*1000, fmt.Sprintf("[t-%ds,t)", w), spotAvailability, spotFreshness, spotMissing,
			fmt.Sprintf("sum(quote_volume) over [t-%ds,t)", w), "현물 거래량 수준"))
	}
	out = append(out, feature("spot_trade_intensity_30s", "spot_1s", 30000, "[t-30s,t)", spotAvailability, spotFreshness, spotMissing,
		"sum(trade_count)/30 over [t-30s,t)", "현물 체결 빈도"))

	for _, w := range []int64{5, 30, 60, 300} {
		out = append(out, feature(fmt.Sprintf("spot_perp_return_gap_%ds", w), "spot_1s+perp_1s", w*1000, fmt.Sprintf("[t-%ds,t)", w), spotAvailability+"; perp는 V1 past-only window", spotFreshness, spotMissing,
			fmt.Sprintf("spot_return_%ds - ret_log_%ds", w, w), "현물과 무기한 선물 가격발견 차이"))
	}
	for _, w := range []int64{5, 30, 60} {
		out = append(out, feature(fmt.Sprintf("spot_perp_taker_imbalance_gap_%ds", w), "spot_1s+perp_1s", w*1000, fmt.Sprintf("[t-%ds,t)", w), spotAvailability+"; perp는 V1 past-only window", spotFreshness, spotMissing,
			fmt.Sprintf("spot_taker_imbalance_%ds - taker_imbalance_%ds", w, w), "현물과 무기한 선물 주문흐름 차이"))
	}
	for _, w := range []int64{30, 300} {
		out = append(out, feature(fmt.Sprintf("spot_perp_volume_ratio_%ds", w), "spot_1s+perp_1s", w*1000, fmt.Sprintf("[t-%ds,t)", w), spotAvailability+"; perp는 V1 past-only window", spotFreshness, spotMissing,
			fmt.Sprintf("log1p(spot quote volume %ds)-log1p(perp quote_volume_sum_%ds)", w, w), "시장 간 상대 거래량"))
	}
	out = append(out, feature("spot_perp_spread_bps", "spot_1s+perp_1s", 1000, "[t-1s,t)", spotAvailability+"; perp reference_close는 decision t에서 확정", spotFreshness, spotMissing,
		"10000*(spot close at t-1s/reference_close-1)", "현물-무기한 선물 basis"))

	for _, x := range []struct {
		name  string
		w     int64
		field string
	}{
		{"oi_change_pct_5m", 300000, "open_interest"}, {"oi_change_pct_15m", 900000, "open_interest"}, {"oi_change_pct_60m", 3600000, "open_interest"},
		{"oi_value_change_pct_5m", 300000, "open_interest_value"}, {"oi_value_change_pct_15m", 900000, "open_interest_value"}, {"oi_value_change_pct_60m", 3600000, "open_interest_value"},
	} {
		out = append(out, feature(x.name, "metrics_5m", x.w, fmt.Sprintf("[t-%dms,t)", x.w), metricsAvailability, metricsFreshness, metricsMissing,
			fmt.Sprintf("latest(%s as-of t)/latest(%s as-of t-%dms)-1", x.field, x.field, x.w), "OI의 상대 변화"))
	}
	out = append(out,
		feature("price_return_x_oi_change_5m", "metrics_5m+perp_1s", 300000, "[t-5m,t)", metricsAvailability+"; perp는 V1 past-only window", metricsFreshness, metricsMissing, "ret_log_300s*oi_change_pct_5m", "가격-OI 상태 interaction"),
		feature("price_return_x_oi_change_15m", "metrics_5m+perp_1s", 900000, "[t-15m,t)", metricsAvailability+"; perp는 V1 past-only window", metricsFreshness, metricsMissing, "ret_log_900s*oi_change_pct_15m", "가격-OI 상태 interaction"),
	)

	positioning := []struct{ name, raw string }{
		{"top_trader_account_ls_ratio", "top_trader_account_long_short_ratio"},
		{"top_trader_position_ls_ratio", "top_trader_position_long_short_ratio"},
		{"global_ls_ratio", "global_long_short_ratio"},
		{"taker_long_short_volume_ratio", "taker_long_short_volume_ratio"},
	}
	for _, x := range positioning {
		out = append(out, feature(x.name, "metrics_5m", 0, "latest observation as-of t", metricsAvailability, metricsFreshness, metricsMissing, "latest("+x.raw+") as-of t", "선물 포지셔닝 수준"))
		out = append(out, feature(x.name+"_change_5m", "metrics_5m", 300000, "[t-5m,t)", metricsAvailability, metricsFreshness, metricsMissing,
			"latest("+x.raw+" as-of t)/latest("+x.raw+" as-of t-300000ms)-1", "선물 포지셔닝 단기 변화"))
	}
	out = append(out, feature("top_vs_global_positioning_gap", "metrics_5m", 0, "latest observation as-of t", metricsAvailability, metricsFreshness, metricsMissing,
		"log(top_trader_position_ls_ratio/global_ls_ratio)", "상위 트레이더와 전체 계정 포지셔닝 차이"))

	out = append(out,
		feature("mark_index_spread_bps", "mark_1m+index_1m", 60000, "latest closed bars as-of t", klineAvailability, klineFreshness, klineMissing, "10000*(mark_close/index_close-1)", "mark-index basis"),
		feature("contract_index_spread_bps", "perp_1s+index_1m", 60000, "latest values as-of t", klineAvailability+"; perp reference_close는 decision t에서 확정", klineFreshness, klineMissing, "10000*(reference_close/index_close-1)", "contract-index basis"),
		feature("premium_close", "premium_1m", 60000, "latest closed bar as-of t", klineAvailability, klineFreshness, klineMissing, "latest premium close as-of t", "Binance premium index 수준"),
		feature("premium_change_5m", "premium_1m", 300000, "[t-5m,t)", klineAvailability, klineFreshness, klineMissing, "premium_close(as-of t)-premium_close(as-of t-300000ms)", "premium 단기 변화"),
		feature("premium_change_15m", "premium_1m", 900000, "[t-15m,t)", klineAvailability, klineFreshness, klineMissing, "premium_close(as-of t)-premium_close(as-of t-900000ms)", "premium 중기 변화"),
		feature("mark_index_spread_change_5m", "mark_1m+index_1m", 300000, "[t-5m,t)", klineAvailability, klineFreshness, klineMissing, "mark_index_spread_bps(as-of t)-mark_index_spread_bps(as-of t-300000ms)", "basis 단기 변화"),
	)
	out = append(out,
		feature("last_funding_rate", "funding", 0, "latest event as-of t", fundingAvailability, fundingFreshness, fundingMissing, "latest(last_funding_rate) as-of t", "최근 확정 funding 상태"),
		feature("funding_age_ms", "funding", 0, "latest event as-of t", fundingAvailability, fundingFreshness, fundingMissing, "t-latest funding timestamp", "funding 관측 경과시간"),
		feature("funding_rate_change", "funding", 0, "two latest observed events as-of t", fundingAvailability, fundingFreshness, fundingMissing, "latest funding rate-previous observed funding rate", "고정 cadence를 가정하지 않는 funding 변화"),
		feature("metrics_age_ms", "metrics_5m", 0, "latest observation as-of t", metricsAvailability, metricsFreshness, metricsMissing, "t-latest metrics timestamp", "metrics freshness 상태"),
		feature("metrics_fresh", "metrics_5m", 0, "latest observation as-of t", metricsAvailability, metricsFreshness, metricsMissing, "1 if metrics AgeMs<=600000 else 0", "metrics stale 상태의 model-friendly mask"),
		feature("kline_age_ms", "mark_1m+index_1m+premium_1m", 0, "latest closed bars as-of t", klineAvailability, klineFreshness, klineMissing, "max source AgeMs across mark/index/premium", "kline group freshness 상태"),
		feature("kline_fresh", "mark_1m+index_1m+premium_1m", 0, "latest closed bars as-of t", klineAvailability, klineFreshness, klineMissing, "1 iff mark/index/premium are all available and Fresh", "kline group stale 상태의 model-friendly mask"),
	)
	return append(out, rejected()...)
}

func rejected() []Feature {
	drop := func(name, overlap, reason string) Feature {
		return Feature{FeatureName: name, Source: "perp_1s", ExistingV1Overlap: overlap, NewInformationRationale: reason, Decision: Drop}
	}
	deferFeature := func(name, source, reason string) Feature {
		return Feature{FeatureName: name, Source: source, NewInformationRationale: reason, Decision: Defer}
	}
	return []Feature{
		drop("price_vs_ema_60s_bps", "ret_log/range/momentum 계열", "기존 V1과 높은 중복이며 초기 V2에서 증분 정보 근거 부족"),
		drop("price_vs_ema_300s_bps", "ret_log/range/momentum 계열", "기존 V1과 높은 중복이며 초기 V2에서 증분 정보 근거 부족"),
		drop("price_vs_ema_900s_bps", "ret_log/range/momentum 계열", "기존 V1과 높은 중복이며 초기 V2에서 증분 정보 근거 부족"),
		drop("ema_60s_slope", "ret_log/momentum 계열", "기존 V1 momentum과 중복"),
		drop("ema_300s_slope", "ret_log/momentum 계열", "기존 V1 momentum과 중복"),
		drop("rolling_vwap_deviation", "vwap_deviation_30s/60s/300s/900s", "동일 semantic의 V1 feature 존재"),
		deferFeature("liquidation_heatmap", "unavailable", "동일 기간 canonical source 없음"),
		deferFeature("l2_order_book_state", "unavailable", "동일 기간 canonical source 없음"),
		deferFeature("options_market_state", "unavailable", "동일 기간 canonical source 없음"),
		deferFeature("next_funding_rate", "future_or_unavailable", "decision 시점 공개 provenance가 보장된 별도 source 없음"),
	}
}

func Validate(features []Feature, v1Names []string) error {
	knownSources := map[string]bool{
		"spot_1s": true, "spot_1s+perp_1s": true, "perp_1s": true,
		"metrics_5m": true, "metrics_5m+perp_1s": true,
		"mark_1m+index_1m": true, "perp_1s+index_1m": true, "premium_1m": true,
		"mark_1m+index_1m+premium_1m": true, "funding": true,
		"unavailable": true, "future_or_unavailable": true,
	}
	v1 := make(map[string]bool, len(v1Names))
	for _, name := range v1Names {
		v1[name] = true
	}
	seen := make(map[string]bool, len(features))
	keepCount := 0
	for _, f := range features {
		if f.FeatureName == "" || seen[f.FeatureName] {
			return fmt.Errorf("duplicate or empty feature name %q", f.FeatureName)
		}
		seen[f.FeatureName] = true
		if !knownSources[f.Source] {
			return fmt.Errorf("unknown source %q for %s", f.Source, f.FeatureName)
		}
		if f.LookbackMs < 0 {
			return fmt.Errorf("invalid lookback for %s", f.FeatureName)
		}
		if f.Decision != Keep {
			continue
		}
		keepCount++
		if v1[f.FeatureName] || f.ExistingV1Overlap != "" {
			return fmt.Errorf("V1 duplicate semantic for KEEP %s", f.FeatureName)
		}
		if f.Window == "" || f.Formula == "" || f.AvailabilityRule == "" || f.FreshnessDependency == "" || f.MissingStaleBehavior == "" {
			return fmt.Errorf("incomplete KEEP spec %s", f.FeatureName)
		}
		if strings.Contains(strings.ToLower(f.Window), "t+") || strings.Contains(strings.ToLower(f.Formula), "future") {
			return fmt.Errorf("future-looking feature %s", f.FeatureName)
		}
		if strings.Contains(f.Source, "spot") || strings.Contains(f.Source, "metrics") || strings.Contains(f.Source, "mark") || strings.Contains(f.Source, "index") || strings.Contains(f.Source, "premium") || f.Source == "funding" {
			if strings.TrimSpace(f.MissingStaleBehavior) == "" {
				return fmt.Errorf("external feature without missing policy %s", f.FeatureName)
			}
		}
	}
	if keepCount < MinNewFeatureCount || keepCount > MaxNewFeatureCount {
		return fmt.Errorf("KEEP count=%d outside [%d,%d]", keepCount, MinNewFeatureCount, MaxNewFeatureCount)
	}
	if len(v1Names) != V1FeatureCount {
		return fmt.Errorf("V1 feature count=%d want %d", len(v1Names), V1FeatureCount)
	}
	return nil
}

func Count(features []Feature, decision Decision) int {
	n := 0
	for _, f := range features {
		if f.Decision == decision {
			n++
		}
	}
	return n
}
