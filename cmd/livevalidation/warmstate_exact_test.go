package main

import (
	"fmt"
	"math"
	"path/filepath"
	"testing"
	"time"

	featurev2 "binance_trader/internal/feature/main/v2"
	live "binance_trader/internal/live/binance"
	"binance_trader/internal/live/warmstate"
	"binance_trader/internal/market"
)

func TestStageGReadinessReportingUsesAuthoritativeState(t *testing.T) {
	cases := []struct {
		name                string
		state               warmstate.Status
		wantStatus, wantWhy string
		wantSource          string
	}{
		{"stabilizing", warmstate.Status{Status: "STABILIZING", StabilizationElapsed: false}, "STABILIZING", "STABILIZATION_IN_PROGRESS", ""},
		{"blocked", warmstate.Status{Status: "BLOCKED", StabilizationElapsed: true, StabilizationReady: true, BootstrapBlocker: "REQUIRED_SOURCE_STALE", BlockerSource: "metrics_taker"}, "BLOCKED", "REQUIRED_SOURCE_STALE", "metrics_taker"},
		{"ready", warmstate.Status{Status: "READY", Ready: true, InputConnected: true, StabilizationReady: true}, "READY", "", ""},
		{"disconnected", warmstate.Status{Status: "READY", Ready: true, InputConnected: false, StabilizationReady: true}, "BLOCKED", "LIVE_INPUT_DISCONNECTED", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, why, source := stageGReadinessState(tc.state)
			if status != tc.wantStatus || why != tc.wantWhy || source != tc.wantSource {
				t.Fatalf("got %s/%s/%s", status, why, source)
			}
		})
	}
}

func TestWarmStateRestartFeatureV2Exact(t *testing.T) {
	const total, split = 14_450, 14_420
	now := time.Now().UTC().Truncate(time.Second)
	base := now.Add(-total * time.Second).UnixMilli()
	all := warmupDataset{Futures: make([]market.SecondBar, 0, total), Spot: make([]market.SecondBar, 0, total)}
	for i := 0; i < total; i++ {
		ts := base + int64(i)*1000
		price := 100000 + float64(i%37)*0.1
		bar := market.SecondBar{TimestampMs: ts, Open: price - 0.02, High: price + 0.05, Low: price - 0.05, Close: price, VWAP: price - 0.01, BaseVolume: 1, QuoteVolume: 1000 + float64(i%11), TakerBuyBaseVolume: .6, TakerSellBaseVolume: .4, TakerBuyQuoteVolume: 600 + float64(i%7), TakerSellQuoteVolume: 400 + float64(i%5), AggTradeCount: 3, HasTrade: true, FirstAggTradeID: int64(i*3 + 1), LastAggTradeID: int64(i*3 + 3)}
		all.Futures = append(all.Futures, bar)
		bar.Close += .2
		all.Spot = append(all.Spot, bar)
	}
	add := func(dataset string, source, close, receive int64, raw string) {
		all.External = append(all.External, live.ExternalObservation{Source: "fixture", Dataset: dataset, SourceTimestampMs: source, CloseTimeMs: close, ReceiveTimestampMs: receive, Raw: []byte(raw)})
	}
	for ts := base - 60_000; ts < base+int64(total)*1000; ts += 60_000 {
		close := ts + 59_999
		for _, dataset := range []string{"mark", "index", "premium"} {
			add(dataset, ts, close, close+1001, `[0,"1","1","1","100000","1",0]`)
		}
	}
	for ts := base - 3_600_000; ts < base+int64(total)*1000; ts += 300_000 {
		add("metrics_oi", ts, 0, ts+1000, `{"sumOpenInterest":"1000","sumOpenInterestValue":"100000"}`)
		for _, dataset := range []string{"metrics_global", "metrics_top_account", "metrics_top_position"} {
			add(dataset, ts, 0, ts+1000, `{"longShortRatio":"1.2"}`)
		}
		add("metrics_taker", ts, 0, ts+1000, `{"buySellRatio":"1.1"}`)
	}
	add("funding", base-8*3_600_000, 0, base, `{"fundingRate":"0.0001"}`)
	add("funding", base-3_600_000, 0, base, `{"fundingRate":"0.0002"}`)

	continuous, err := replayFrozenFeatures(all)
	if err != nil {
		t.Fatal(err)
	}
	boundary := all.Futures[split-1].TimestampMs + 1000
	before := warmupDataset{Futures: all.Futures[:split], Spot: all.Spot[:split]}
	for _, row := range all.External {
		if row.ReceiveTimestampMs <= boundary {
			before.External = append(before.External, row)
		}
	}
	snap, err := warmstate.NewSnapshot(before.Futures, before.Spot, before.External, now)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "restart.json")
	if _, err = warmstate.Save(path, snap); err != nil {
		t.Fatal(err)
	}
	restored, err := warmstate.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	next := all.Futures[split].TimestampMs
	if err = restored.RestoreAtTime(next, next, time.UnixMilli(next)); err != nil {
		t.Fatal(err)
	}
	if err = restored.RestoreAtTime(next+1000, next, time.UnixMilli(next+1000)); err == nil {
		t.Fatal("logical gap accepted")
	}
	if err = restored.RestoreAtTime(next, next, time.UnixMilli(next+3_600_000)); err == nil {
		t.Fatal("stale snapshot accepted")
	}
	corrupt := restored
	corrupt.FeatureRegistryHash = "wrong"
	if err = corrupt.RestoreAtTime(next, next, time.UnixMilli(next)); err == nil {
		t.Fatal("feature hash mismatch accepted")
	}
	corrupt = restored
	corrupt.Version++
	if err = corrupt.RestoreAtTime(next, next, time.UnixMilli(next)); err == nil {
		t.Fatal("version mismatch accepted")
	}
	joined := warmupDataset{Futures: append(append([]market.SecondBar(nil), restored.Futures...), all.Futures[split:]...), Spot: append(append([]market.SecondBar(nil), restored.Spot...), all.Spot[split:]...), External: append([]live.ExternalObservation(nil), restored.External...)}
	for _, row := range all.External {
		if row.ReceiveTimestampMs > boundary {
			joined.External = append(joined.External, row)
		}
	}
	restarted, err := replayFrozenFeatures(joined)
	if err != nil {
		t.Fatal(err)
	}
	if len(continuous) != len(restarted) {
		t.Fatalf("decision count mismatch %d/%d", len(continuous), len(restarted))
	}
	compared, eligible := 0, 0
	for i, a := range continuous {
		if a.TimestampMs < next {
			continue
		}
		b := restarted[i]
		compared++
		if a.TimestampMs != b.TimestampMs || a.Reason != b.Reason {
			t.Fatalf("decision/eligibility mismatch at %d: %v/%v", i, a, b)
		}
		if a.Reason == featurev2.FutureObservation {
			t.Fatal("future observation")
		}
		if a.Reason == featurev2.Eligible {
			eligible++
			for j := 0; j < featurev2.ModelFeatureCountV2; j++ {
				if math.Float64bits(a.Values[j]) != math.Float64bits(b.Values[j]) {
					t.Fatal(fmt.Sprintf("feature mismatch decision=%d index=%d", a.TimestampMs, j))
				}
				if math.IsNaN(a.Values[j]) || math.IsInf(a.Values[j], 0) {
					t.Fatal("non-finite feature")
				}
			}
		}
	}
	if compared == 0 || eligible == 0 {
		t.Fatalf("fixture did not exercise eligible resumed decisions: compared=%d eligible=%d", compared, eligible)
	}
	t.Logf("restart exact parity decisions=%d eligible=%d features=128 mismatch=0", compared, eligible)
}
