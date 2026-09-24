package runtimefeature

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	featurev2 "binance_trader/internal/feature/main/v2"
	"binance_trader/internal/live/autopipeline"
	live "binance_trader/internal/live/binance"
	"binance_trader/internal/live/warmstate"
	"binance_trader/internal/market"
)

func TestCurrentGapStartsNewCanonicalHistory(t *testing.T) {
	const base int64 = 1_800_000_000_000
	r := New("", time.UnixMilli(base), nil)
	r.Connection("futures", true)
	r.Connection("spot", true)
	for i := int64(0); i < 4; i++ {
		for _, source := range []string{"futures_aggTrade", "spot_aggTrade"} {
			e := live.CaptureEvent{Source: source, EventType: "aggTrade", SourceTimestampMs: base + i*1000, ReceiveTimestampMs: base + i*1000 + 100, ID: i + 1, Price: 100 + float64(i), Quantity: 1}
			if err := r.AddTrade(e); err != nil {
				t.Fatal(err)
			}
		}
	}
	s := r.Status(time.UnixMilli(base + 4000))
	if !s.Status.InputConnected || s.Status.Ready || s.Status.Status != "WARMING_UP" || s.FuturesBars == 0 || s.SpotBars == 0 || s.FuturesGaps != 0 || s.SpotGaps != 0 || s.IDGaps != 0 {
		t.Fatalf("cold runtime: %+v", s)
	}
	r.Connection("spot", false)
	s = r.Status(time.UnixMilli(base + 5000))
	if s.Status.Ready || s.Status.Status != "GAP" || s.SpotGaps != 1 {
		t.Fatalf("disconnect bridged: %+v", s)
	}
}

func TestExternalFutureReceiveRejected(t *testing.T) {
	r := New("", time.UnixMilli(1_800_000_000_000), nil)
	bad := live.ExternalObservation{Source: "funding", Dataset: "funding", SourceTimestampMs: 1_800_000_010_000, ReceiveTimestampMs: 1_800_000_000_000, Raw: json.RawMessage(`{"fundingRate":"0.001"}`)}
	if err := r.AddExternal([]live.ExternalObservation{bad}); err == nil || r.stats.ExternalUpdates != 0 {
		t.Fatal("future external observation accepted")
	}
}

func TestMetricSubsourceHealthUsesLatestTimestampAndFrozenFreshness(t *testing.T) {
	const now = int64(1_800_000_900_000)
	r := New("", time.UnixMilli(now), nil)
	rows := fixtureExternal(now)
	if err := r.AddExternal(rows); err != nil {
		t.Fatal(err)
	}
	stats := r.Status(time.UnixMilli(now))
	for _, name := range []string{"metrics_oi", "metrics_global", "metrics_top_account", "metrics_top_position", "metrics_taker"} {
		h, ok := stats.MetricSources[name]
		if !ok || h.Status != "FRESH" || h.FreshnessLimitMs != 600_000 || h.LastPollResult != "PASS" || h.HTTPStatus != 200 || h.ObservationCount != 4 {
			t.Fatalf("%s health: %+v", name, h)
		}
	}
	stats = r.Status(time.UnixMilli(now + 600_001))
	if stats.MetricSources["metrics_taker"].Status != "STALE" {
		t.Fatalf("frozen freshness not applied: %+v", stats.MetricSources["metrics_taker"])
	}
}

func TestCanonicalFeatureV2AndFrozenModelIntegration(t *testing.T) {
	const decision int64 = 1_800_000_005_000
	last := decision - 2000
	var emitted featurev2.Snapshot
	var emittedReason featurev2.Reason
	var entryPrice float64
	r := New("", time.UnixMilli(decision), func(s featurev2.Snapshot, reason featurev2.Reason, _ float64, price float64) {
		emitted, emittedReason, entryPrice = s, reason, price
	})
	start := last - 14399*1000
	var lastF, lastS market.SecondBar
	var futureBars, spotBars []market.SecondBar
	for i := int64(0); i < 14400; i++ {
		ts := start + i*1000
		price := 100 + float64(i)*.001
		makeBar := func(p float64) market.SecondBar {
			return market.SecondBar{TimestampMs: ts, Open: p, High: p, Low: p, Close: p, VWAP: p, BaseVolume: 1, QuoteVolume: p, AggTradeCount: 1, TakerBuyBaseVolume: 1, TakerBuyQuoteVolume: p, HasTrade: true, FirstAggTradeID: i + 1, LastAggTradeID: i + 1}
		}
		lastF = makeBar(price)
		lastS = makeBar(price + .5)
		futureBars = append(futureBars, lastF)
		spotBars = append(spotBars, lastS)
		if _, err := r.v1.Add(lastF); err != nil {
			t.Fatal(err)
		}
		if err := r.v2.AddSpot(lastS); err != nil {
			t.Fatal(err)
		}
	}
	r.v2.Prune(last)
	pre, err := warmstate.NewSnapshotWithEngines(futureBars, spotBars, nil, r.v1, r.v2, time.UnixMilli(last+500))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "live-snapshot.json")
	if _, err = warmstate.Save(path, pre); err != nil {
		t.Fatal(err)
	}
	cold := New(path, time.UnixMilli(last+60_000), nil)
	if cold.restore != nil || cold.Status(time.UnixMilli(last+60_000)).Status.Ready {
		t.Fatal("stale warm state bridged to current stream")
	}
	r = New(path, time.UnixMilli(last+500), func(s featurev2.Snapshot, reason featurev2.Reason, _ float64, price float64) {
		emitted, emittedReason, entryPrice = s, reason, price
	})
	r.Connection("futures", true)
	r.Connection("spot", true)
	if r.restore == nil {
		t.Fatal("valid warm state not offered for exact handoff")
	}
	if err := r.AddExternal(fixtureExternal(decision)); err != nil {
		t.Fatal(err)
	}
	for i := int64(0); i < 2; i++ {
		for _, source := range []string{"futures_aggTrade", "spot_aggTrade"} {
			e := live.CaptureEvent{Source: source, EventType: "aggTrade", SourceTimestampMs: last + 1000 + i*1000, ReceiveTimestampMs: last + 1100 + i*1000, ID: 14401 + i, Price: 114.4 + float64(i)*.001, Quantity: 1}
			if source == "spot_aggTrade" {
				e.Price += .5
			}
			if err := r.AddTrade(e); err != nil {
				t.Fatal(err)
			}
		}
	}
	if emittedReason != featurev2.Eligible || emitted.DecisionTimestampMs != decision || len(emitted.Values) != 128 || !featurev2.AllFinite(emitted.Values[:]...) || entryPrice <= 0 {
		t.Fatalf("feature reason=%s decision=%d price=%g", emittedReason, emitted.DecisionTimestampMs, entryPrice)
	}
	s := r.Status(time.UnixMilli(decision))
	if !s.Status.Ready || s.FutureObservations != 0 || s.EligibleDecisions != 1 || !s.RestoreAttempted || !s.RestoreApplied || s.RestoreRejectReason != "" {
		t.Fatalf("ready runtime: %+v", s)
	}
	p, err := autopipeline.LoadFrozen(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	result, err := p.Evaluate(emitted, 10000, entryPrice)
	if err != nil || len(result.Scores) != 5 {
		t.Fatalf("frozen model integration: %v %+v", err, result)
	}
}

func TestRestoreAllowsNoTradeSeconds(t *testing.T) {
	const last int64 = 1_800_100_000_000
	path := writeRestoreFixture(t, last, 100)
	r := NewWithRestoreMaxAge(path, time.UnixMilli(last+500), time.Minute, nil)
	for _, source := range []string{"futures_aggTrade", "spot_aggTrade"} {
		if err := r.AddTrade(live.CaptureEvent{Source: source, EventType: "aggTrade", SourceTimestampMs: last + 3000, ReceiveTimestampMs: last + 3100, ID: 101, Price: 101, Quantity: 1}); err != nil {
			t.Fatal(err)
		}
	}
	s := r.Status(time.UnixMilli(last + 3100))
	if !s.RestoreAttempted || !s.RestoreApplied || s.RestoreRejectReason != "" || r.gap {
		t.Fatalf("no-trade restore rejected: %+v gap=%t", s, r.gap)
	}
}

func TestRestoreRejectsTradeIDDiscontinuityWithDiagnostics(t *testing.T) {
	const last int64 = 1_800_200_000_000
	path := writeRestoreFixture(t, last, 100)
	r := NewWithRestoreMaxAge(path, time.UnixMilli(last+500), time.Minute, nil)
	for _, event := range []live.CaptureEvent{
		{Source: "futures_aggTrade", EventType: "aggTrade", SourceTimestampMs: last + 1000, ReceiveTimestampMs: last + 1100, ID: 102, Price: 101, Quantity: 1},
		{Source: "spot_aggTrade", EventType: "aggTrade", SourceTimestampMs: last + 1000, ReceiveTimestampMs: last + 1100, ID: 101, Price: 101, Quantity: 1},
	} {
		if err := r.AddTrade(event); err != nil {
			t.Fatal(err)
		}
	}
	s := r.Status(time.UnixMilli(last + 1100))
	if !s.RestoreAttempted || s.RestoreApplied || s.RestoreRejectReason != "FUTURES_TRADE_ID_DISCONTINUITY" || s.RestoreExpectedFuturesID != 101 || s.RestoreActualFuturesID != 102 {
		t.Fatalf("missing ID diagnostics: %+v", s)
	}
}

func TestRestoreRejectsTimestampNotAfterSnapshot(t *testing.T) {
	const last int64 = 1_800_300_000_000
	path := writeRestoreFixture(t, last, 100)
	r := NewWithRestoreMaxAge(path, time.UnixMilli(last+500), time.Minute, nil)
	for _, source := range []string{"futures_aggTrade", "spot_aggTrade"} {
		if err := r.AddTrade(live.CaptureEvent{Source: source, EventType: "aggTrade", SourceTimestampMs: last, ReceiveTimestampMs: last + 600, ID: 101, Price: 101, Quantity: 1}); err != nil {
			t.Fatal(err)
		}
	}
	s := r.Status(time.UnixMilli(last + 600))
	if !s.RestoreAttempted || s.RestoreApplied || s.RestoreRejectReason != "TIMESTAMP_NOT_AFTER_SNAPSHOT" {
		t.Fatalf("missing timestamp diagnostics: %+v", s)
	}
}

func TestSnapshotRejectsIncompleteWarmState(t *testing.T) {
	r := New("", time.Now(), nil)
	if _, err := r.Snapshot(time.Now()); !errors.Is(err, ErrWarmStateIncomplete) {
		t.Fatalf("Snapshot error=%v want=%v", err, ErrWarmStateIncomplete)
	}
}

func writeRestoreFixture(t *testing.T, last, lastID int64) string {
	t.Helper()
	r := New("", time.UnixMilli(last), nil)
	start := last - warmstate.RequiredWarmupMs
	futures := make([]market.SecondBar, 0, warmstate.RequiredWarmupMs/1000+1)
	spot := make([]market.SecondBar, 0, warmstate.RequiredWarmupMs/1000+1)
	for ts := start; ts <= last; ts += 1000 {
		bar := func(price float64) market.SecondBar {
			return market.SecondBar{TimestampMs: ts, Open: price, High: price, Low: price, Close: price, VWAP: price}
		}
		f, s := bar(100), bar(100.5)
		if ts == last {
			f.HasTrade, s.HasTrade = true, true
			f.AggTradeCount, s.AggTradeCount = 1, 1
			f.FirstAggTradeID, f.LastAggTradeID = lastID, lastID
			s.FirstAggTradeID, s.LastAggTradeID = lastID, lastID
		}
		futures, spot = append(futures, f), append(spot, s)
		if _, err := r.v1.Add(f); err != nil {
			t.Fatal(err)
		}
		if err := r.v2.AddSpot(s); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := warmstate.NewSnapshotWithEngines(futures, spot, nil, r.v1, r.v2, time.UnixMilli(last+100))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "snapshot.json")
	if _, err = warmstate.Save(path, snapshot); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestBootstrapSeedEmitsNoHistoricalSignalAndExactLiveHandoff(t *testing.T) {
	const decision int64 = 1_800_000_005_000
	last := decision - 2000
	start := last - warmstate.RequiredWarmupMs
	var futures, spot []market.SecondBar
	for i, ts := int64(0), start; ts <= last; i, ts = i+1, ts+1000 {
		bar := func(price float64) market.SecondBar {
			return market.SecondBar{TimestampMs: ts, Open: price, High: price, Low: price, Close: price, VWAP: price, BaseVolume: 1, QuoteVolume: price, AggTradeCount: 1, TakerBuyBaseVolume: 1, TakerBuyQuoteVolume: price, HasTrade: true, FirstAggTradeID: i + 1, LastAggTradeID: i + 1}
		}
		futures = append(futures, bar(100+float64(i)*.001))
		spot = append(spot, bar(100.5+float64(i)*.001))
	}
	fetchCompleted := last + 500
	external := fixtureExternal(decision)
	for i := range external {
		external[i].ReceiveTimestampMs = 0
		external[i].FetchCompletedAtMs = fetchCompleted
		external[i].Origin = live.OriginBootstrapHistory
	}
	emitted := 0
	r := New("", time.UnixMilli(fetchCompleted), func(featurev2.Snapshot, featurev2.Reason, float64, float64) { emitted++ })
	if err := r.SeedBootstrap(futures, spot, external, fetchCompleted); err != nil {
		t.Fatal(err)
	}
	if emitted != 0 || !r.stats.BootstrapSeeded {
		t.Fatal("bootstrap emitted historical signal")
	}
	r.EnableLiveEmission()
	r.Connection("futures", true)
	r.Connection("spot", true)
	// Buffered overlap is exact-deduplicated, then N+1 continues the stream.
	for _, source := range []string{"futures_aggTrade", "spot_aggTrade"} {
		if err := r.AddTrade(live.CaptureEvent{Source: source, EventType: "aggTrade", SourceTimestampMs: last, ReceiveTimestampMs: last + 600, Origin: live.OriginLiveReceived, ID: 14401, Price: 120, Quantity: 1}); err != nil {
			t.Fatal(err)
		}
	}
	for i := int64(1); i <= 2; i++ {
		for _, source := range []string{"futures_aggTrade", "spot_aggTrade"} {
			if err := r.AddTrade(live.CaptureEvent{Source: source, EventType: "aggTrade", SourceTimestampMs: last + i*1000, ReceiveTimestampMs: last + i*1000 + 100, Origin: live.OriginLiveReceived, ID: 14401 + i, Price: 120 + float64(i), Quantity: 1}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if emitted != 1 {
		t.Fatalf("live handoff emissions=%d", emitted)
	}
	s := r.Status(time.UnixMilli(decision))
	if s.IDGaps != 0 || s.ReverseEvents != 0 || s.FutureObservations != 0 || !s.Status.Ready {
		t.Fatalf("handoff status: %+v", s)
	}
}

func fixtureExternal(decision int64) []live.ExternalObservation {
	var rows []live.ExternalObservation
	add := func(dataset string, source, close, receive int64, raw any) {
		b, _ := json.Marshal(raw)
		rows = append(rows, live.ExternalObservation{Source: dataset, Dataset: dataset, SourceTimestampMs: source, CloseTimeMs: close, ReceiveTimestampMs: receive, Raw: b})
	}
	for i, ts := range []int64{decision - 3605000, decision - 905000, decision - 305000, decision - 5000} {
		v := float64(100 + i*20)
		add("metrics_oi", ts, 0, ts+1000, map[string]string{"sumOpenInterest": fmt.Sprint(v), "sumOpenInterestValue": fmt.Sprint(v * 100)})
		for _, x := range []struct{ name, key string }{{"metrics_global", "longShortRatio"}, {"metrics_top_account", "longShortRatio"}, {"metrics_top_position", "longShortRatio"}, {"metrics_taker", "buySellRatio"}} {
			add(x.name, ts, 0, ts+1000, map[string]string{x.key: fmt.Sprint(1 + float64(i)*.1)})
		}
	}
	for i, ts := range []int64{decision - 3665000, decision - 965000, decision - 365000, decision - 65000} {
		for _, x := range []struct {
			name  string
			value float64
		}{{"mark", 99 + float64(i)}, {"index", 100 + float64(i)*.5}, {"premium", -.003 + float64(i)*.001}} {
			add(x.name, ts, ts+59999, ts+60000, []any{ts, "0", "0", "0", fmt.Sprint(x.value), "0", ts + 59999})
		}
	}
	add("funding", decision-28805000, 0, decision-28_804_000, map[string]string{"fundingRate": "0.001"})
	add("funding", decision-5000, 0, decision-4000, map[string]string{"fundingRate": "0.002"})
	return rows
}
