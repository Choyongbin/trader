package runtimefeature

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	featurev2 "binance_trader/internal/feature/main/v2"
	live "binance_trader/internal/live/binance"
	"binance_trader/internal/live/warmstate"
	"binance_trader/internal/market"
)

// A production-size, complete and causally available snapshot: the V2 engine
// already contains the canonical observations that the raw snapshot records.
// This is the situation where the old raw metrics_* restore keys lost the
// canonical "metrics" cursor and replayed already-applied observations.
func writeRestoredExternalFixture(t *testing.T, last int64) (string, []live.ExternalObservation) {
	t.Helper()
	r := New("", time.UnixMilli(last), nil)
	const count = int64(14401)
	start := last - (count-1)*1000
	futures := make([]market.SecondBar, 0, count)
	spot := make([]market.SecondBar, 0, count)
	for i := int64(0); i < count; i++ {
		ts := start + i*1000
		price := 100.0 + float64(i)*0.001
		bar := func(p float64) market.SecondBar {
			return market.SecondBar{
				TimestampMs: ts, Open: p, High: p, Low: p, Close: p, VWAP: p,
				BaseVolume: 1, QuoteVolume: p, AggTradeCount: 1,
				TakerBuyBaseVolume: 1, TakerBuyQuoteVolume: p,
				HasTrade: true, FirstAggTradeID: i + 1, LastAggTradeID: i + 1,
			}
		}
		f, sp := bar(price), bar(price+0.5)
		futures = append(futures, f)
		spot = append(spot, sp)
		if _, err := r.v1.Add(f); err != nil {
			t.Fatal(err)
		}
		if err := r.v2.AddSpot(sp); err != nil {
			t.Fatal(err)
		}
	}
	external := fixtureExternal(last)
	parsed, err := parseExternal(external)
	if err != nil {
		t.Fatal(err)
	}
	for _, x := range parsed {
		switch x.dataset {
		case "metrics":
			err = r.v2.AddMetrics(*x.metric)
		case "mark":
			err = r.v2.AddMark(*x.kline)
		case "index":
			err = r.v2.AddIndex(*x.kline)
		case "premium":
			err = r.v2.AddPremium(*x.kline)
		case "funding":
			err = r.v2.AddFunding(*x.funding)
		}
		if err != nil {
			t.Fatalf("external %s: %v", x.dataset, err)
		}
	}
	r.v2.Prune(last)
	s, err := warmstate.NewSnapshotWithEngines(futures, spot, external, r.v1, r.v2, time.UnixMilli(last+100))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "snapshot.json")
	if _, err = warmstate.Save(path, s); err != nil {
		t.Fatal(err)
	}
	return path, external
}

func addRestoredPair(t *testing.T, r *Runtime, last, idOffset int64) {
	t.Helper()
	for _, source := range []string{"futures_aggTrade", "spot_aggTrade"} {
		price := 115.0
		if source == "spot_aggTrade" {
			price = 115.5
		}
		err := r.AddTrade(live.CaptureEvent{
			Source: source, EventType: "aggTrade", SourceTimestampMs: last + idOffset*1000,
			ReceiveTimestampMs: last + idOffset*1000 + 100,
			ID:                 14401 + idOffset, Price: price, Quantity: 1,
		})
		if err != nil {
			t.Fatalf("pair %d %s: %v", idOffset, source, err)
		}
	}
}

func TestRestoredExternalCursorsPreserveFourHourWarmupAndResumeDecisions(t *testing.T) {
	const last int64 = 1_800_500_000_000
	path, raw := writeRestoredExternalFixture(t, last)
	decisions, eligible := 0, 0
	r := NewWithRestoreMaxAge(path, time.UnixMilli(last+200), time.Minute, func(_ featurev2.Snapshot, reason featurev2.Reason, _ float64, _ float64) {
		decisions++
		if reason == featurev2.Eligible {
			eligible++
		}
	})
	r.Connection("futures", true)
	r.Connection("spot", true)
	addRestoredPair(t, r, last, 1)
	if !r.stats.RestoreApplied || r.historyStart > last-warmstate.RequiredWarmupMs {
		t.Fatalf("restored history missing: restored=%t start=%d", r.stats.RestoreApplied, r.historyStart)
	}
	want := map[string]int64{"metrics": last - 5000, "mark": last - 65000, "index": last - 65000, "premium": last - 65000, "funding": last - 5000}
	for name, ts := range want {
		if got := r.lastExternal[name]; got != ts {
			t.Errorf("restored %s cursor=%d want=%d", name, got, ts)
		}
	}
	if _, found := r.lastExternal["metrics_taker"]; found {
		t.Fatal("raw metrics_taker key must not replace canonical cursor")
	}
	// A new public poll may repeat the exact already-restored raw sources.
	if err := r.AddExternal(raw); err != nil {
		t.Fatal(err)
	}
	if len(r.external) != 0 {
		t.Fatalf("replayed %d already-restored external observations", len(r.external))
	}
	// Five new raw sources must combine into exactly one new canonical metrics observation.
	var newer []live.ExternalObservation
	for _, x := range raw {
		if !strings.HasPrefix(x.Dataset, "metrics_") || x.SourceTimestampMs != last-5000 {
			continue
		}
		x.SourceTimestampMs = last + 1000
		x.ReceiveTimestampMs = last + 1500
		newer = append(newer, x)
	}
	if len(newer) != 5 {
		t.Fatalf("fixture newer metrics parts=%d", len(newer))
	}
	if err := r.AddExternal(newer); err != nil {
		t.Fatal(err)
	}
	if len(r.external) != 1 || r.external[0].dataset != "metrics" || r.lastExternal["metrics"] != last+1000 {
		t.Fatalf("new canonical metrics not enqueued exactly once: pending=%+v cursor=%d", r.external, r.lastExternal["metrics"])
	}
	if err := r.AddExternal(newer); err != nil {
		t.Fatal(err)
	}
	if len(r.external) != 1 {
		t.Fatalf("same metrics update enqueued twice: %d", len(r.external))
	}
	// Last bar of the original snapshot is already closed; new canonical bars
	// must continue from last+1000 without resetting the four-hour history.
	for i := int64(2); i <= 11; i++ {
		addRestoredPair(t, r, last, i)
	}
	status := r.Status(time.UnixMilli(last + 11100))
	if r.stats.RuntimeResets != 0 || status.Status.AvailableMs < warmstate.RequiredWarmupMs || status.FeatureDecisions == 0 || decisions == 0 {
		t.Fatalf("lost warm history after restore: resets=%d last_reset=%q warm=%d decisions=%d status=%+v", r.stats.RuntimeResets, r.stats.LastResetReason, status.Status.AvailableMs, decisions, status)
	}
	if eligible == 0 {
		t.Fatalf("no eligible decision with complete historical fixture; status=%+v", status)
	}
	state := r.v2.ExportState()
	found := 0
	for _, m := range state.Metrics {
		if m.TimestampMs == last+1000 {
			found++
		}
	}
	if found != 1 {
		t.Fatalf("new canonical metrics added %d times; want 1", found)
	}
}

func TestExternalPollQueuedBeforeRestoreDoesNotReplayRestoredEngineState(t *testing.T) {
	const last int64 = 1_800_600_000_000
	path, raw := writeRestoredExternalFixture(t, last)
	r := NewWithRestoreMaxAge(path, time.UnixMilli(last+200), time.Minute, nil)
	r.Connection("futures", true)
	r.Connection("spot", true)
	if err := r.AddExternal(raw); err != nil {
		t.Fatal(err)
	}
	if len(r.external) == 0 {
		t.Fatal("pre-restore poll was not queued; regression fixture invalid")
	}
	addRestoredPair(t, r, last, 1)
	if !r.stats.RestoreApplied {
		t.Fatal("valid initial snapshot not restored")
	}
	if len(r.external) != 0 {
		t.Fatalf("pre-restore poll replayed after restore: %d", len(r.external))
	}
	for i := int64(2); i <= 7; i++ {
		addRestoredPair(t, r, last, i)
	}
	status := r.Status(time.UnixMilli(last + 7100))
	if status.RuntimeResets != 0 || status.Status.AvailableMs < warmstate.RequiredWarmupMs || status.FeatureDecisions == 0 {
		t.Fatalf("pre-restore overlap broke live continuation: %+v", status)
	}
}

// Every warm-state invalidation is visible in final stage-G warmup_status,
// including reset paths that do not increment canonical gap counters.
func TestRuntimeResetCauseSurvivesHistoryReset(t *testing.T) {
	r := New("", time.Now(), nil)
	r.invalidate("EXTERNAL_ENGINE_ERROR:metrics:strict ordering")
	stats := r.Status(time.Now())
	if stats.RuntimeResets != 1 || stats.Status.RuntimeResets != 1 || stats.LastResetReason != "EXTERNAL_ENGINE_ERROR:metrics:strict ordering" || stats.Status.LastResetReason != stats.LastResetReason {
		t.Fatalf("reset cause not exposed: %+v", stats)
	}
}
