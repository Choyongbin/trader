// Package runtimefeature joins completed production one-second bars with the
// frozen V1/V2 streaming engines. It never invents a feature or an order.
package runtimefeature

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"binance_trader/internal/external/asof"
	mainfeature "binance_trader/internal/feature/main"
	featurev2 "binance_trader/internal/feature/main/v2"
	live "binance_trader/internal/live/binance"
	"binance_trader/internal/live/warmstate"
	"binance_trader/internal/market"
)

type DecisionSink func(featurev2.Snapshot, featurev2.Reason, float64, float64)

var ErrWarmStateIncomplete = errors.New("WARM_STATE_INCOMPLETE")

type Stats struct {
	FuturesBars, SpotBars                                                                       int64
	FuturesGaps, SpotGaps                                                                       int64
	DuplicateEvents, ReverseEvents, IDGaps                                                      int64
	ExternalUpdates, FeatureDecisions, EligibleDecisions, NotReadyDecisions, FutureObservations int64
	RuntimeResets                                                                               int64
	LastResetReason                                                                             string
	LastDecisionMs                                                                              int64
	SourceLastMs                                                                                map[string]int64
	MetricSources                                                                               map[string]MetricSourceHealth
	Status                                                                                      warmstate.Status
	BootstrapSeeded                                                                             bool
	BootstrapFetchCompletedMs                                                                   int64
	RestoreAttempted                                                                            bool
	RestoreApplied                                                                              bool
	RestoreRejectReason                                                                         string
	RestoreExpectedFuturesID, RestoreActualFuturesID                                            int64
	RestoreExpectedSpotID, RestoreActualSpotID                                                  int64
	RestoreSnapshotLastEventMs, RestoreActualFuturesMs, RestoreActualSpotMs                     int64
}

type MetricSourceHealth struct {
	LastSourceTimestampMs  int64  `json:"last_source_timestamp_ms"`
	LastReceiveTimestampMs int64  `json:"last_receive_timestamp_ms"`
	UsableAtMs             int64  `json:"usable_at_ms"`
	CurrentAgeMs           int64  `json:"current_age_ms"`
	FreshnessLimitMs       int64  `json:"freshness_limit_ms"`
	Status                 string `json:"status"`
	ObservationCount       int64  `json:"observation_count"`
	PollingIntervalMs      int64  `json:"polling_interval_ms"`
	LastPollAtMs           int64  `json:"last_poll_at_ms"`
	LastPollResult         string `json:"last_poll_result"`
	HTTPStatus             int    `json:"http_status"`
	ResponseRows           int    `json:"response_rows"`
	ResponseNewestMs       int64  `json:"response_newest_timestamp_ms"`
	StoredObservationMs    int64  `json:"stored_observation_timestamp_ms"`
	LastError              string `json:"last_error,omitempty"`
}

type pair struct{ futures, spot *market.SecondBar }

type Runtime struct {
	futures, spot         *live.CanonicalStream
	v1                    *mainfeature.Engine
	v2                    *featurev2.StreamingEngine
	pairs                 map[int64]*pair
	external              []externalValue
	lastExternal          map[string]int64
	metricSources         map[string]MetricSourceHealth
	futuresHistory        []market.SecondBar
	spotHistory           []market.SecondBar
	externalHistory       []live.ExternalObservation
	firstEvents           []live.CaptureEvent
	restore               *warmstate.Snapshot
	historyStart, lastBar int64
	lastReceive           map[string]int64
	connected             map[string]bool
	gap                   bool
	lastReason            featurev2.Reason
	stats                 Stats
	onDecision            DecisionSink
	emissionEnabled       bool
}

// SeedBootstrap initializes rolling state without evaluating historical
// decisions. Historical rows therefore cannot publish signals or intents.
func (r *Runtime) SeedBootstrap(futures, spot []market.SecondBar, external []live.ExternalObservation, fetchCompletedMs int64) error {
	if len(futures) == 0 || len(futures) != len(spot) || fetchCompletedMs <= 0 {
		return fmt.Errorf("BOOTSTRAP_INCOMPLETE")
	}
	if futures[len(futures)-1].TimestampMs-futures[0].TimestampMs < warmstate.RequiredWarmupMs {
		return fmt.Errorf("BOOTSTRAP_INCOMPLETE")
	}
	for i := range futures {
		if futures[i].TimestampMs != spot[i].TimestampMs || (i > 0 && futures[i].TimestampMs != futures[i-1].TimestampMs+1000) {
			return fmt.Errorf("BOOTSTRAP_HANDOFF_GAP")
		}
	}
	r.reset(false)
	r.stats = Stats{}
	seedExternal := make([]live.ExternalObservation, len(external))
	copy(seedExternal, external)
	for i := range seedExternal {
		if seedExternal[i].Origin != live.OriginBootstrapHistory || seedExternal[i].FetchCompletedAtMs != fetchCompletedMs || seedExternal[i].ReceiveTimestampMs != 0 {
			return fmt.Errorf("bootstrap origin violation")
		}
		// This is the actual fetch-completion availability boundary, not an
		// invented historical network receive timestamp.
		seedExternal[i].ReceiveTimestampMs = fetchCompletedMs
	}
	parsed, err := parseExternal(seedExternal)
	if err != nil {
		return err
	}
	r.recordMetricSources(seedExternal, parsed)
	v1 := mainfeature.NewEngine()
	v2 := featurev2.NewStreamingEngine()
	for _, x := range parsed {
		var addErr error
		switch x.dataset {
		case "metrics":
			addErr = v2.AddMetrics(*x.metric)
		case "mark":
			addErr = v2.AddMark(*x.kline)
		case "index":
			addErr = v2.AddIndex(*x.kline)
		case "premium":
			addErr = v2.AddPremium(*x.kline)
		case "funding":
			addErr = v2.AddFunding(*x.funding)
		}
		if addErr != nil {
			return addErr
		}
		r.lastExternal[x.dataset] = x.sourceMs
	}
	for i := range futures {
		if err = v2.AddSpot(spot[i]); err != nil {
			return err
		}
		if _, err = v1.Add(futures[i]); err != nil {
			return err
		}
	}
	fs, err := live.NewResumedCanonicalStream(func(b market.SecondBar) error { return r.addBar("futures", b) }, futures[len(futures)-1], lastTradeID(futures))
	if err != nil {
		return err
	}
	ss, err := live.NewResumedCanonicalStream(func(b market.SecondBar) error { return r.addBar("spot", b) }, spot[len(spot)-1], lastTradeID(spot))
	if err != nil {
		return err
	}
	r.futures, r.spot, r.v1, r.v2 = fs, ss, v1, v2
	r.pairs = map[int64]*pair{}
	r.external = nil
	r.historyStart = futures[0].TimestampMs
	r.lastBar = futures[len(futures)-1].TimestampMs
	r.gap = false
	r.restore = nil
	r.firstEvents = nil
	r.stats.FuturesBars = int64(len(futures))
	r.stats.SpotBars = int64(len(spot))
	r.stats.ExternalUpdates = int64(len(parsed))
	r.stats.BootstrapSeeded = true
	r.stats.BootstrapFetchCompletedMs = fetchCompletedMs
	r.futuresHistory = append([]market.SecondBar(nil), futures...)
	r.spotHistory = append([]market.SecondBar(nil), spot...)
	r.externalHistory = append([]live.ExternalObservation(nil), seedExternal...)
	r.emissionEnabled = false
	return nil
}

func (r *Runtime) EnableLiveEmission()  { r.emissionEnabled = true }
func (r *Runtime) DisableLiveEmission() { r.emissionEnabled = false }

func New(snapshotPath string, now time.Time, sink DecisionSink) *Runtime {
	return NewWithRestoreMaxAge(snapshotPath, now, 10*time.Second, sink)
}

func NewWithRestoreMaxAge(snapshotPath string, now time.Time, maxAge time.Duration, sink DecisionSink) *Runtime {
	r := &Runtime{onDecision: sink, emissionEnabled: true, lastExternal: map[string]int64{}, metricSources: map[string]MetricSourceHealth{}, lastReceive: map[string]int64{}, connected: map[string]bool{}}
	r.reset(false)
	if snapshotPath != "" {
		if s, err := warmstate.Load(snapshotPath); err == nil && s.V1State != nil && s.V2State != nil && maxAge > 0 && now.UnixMilli()-s.LastEventTimeMs >= 0 && now.UnixMilli()-s.LastEventTimeMs <= maxAge.Milliseconds() {
			r.restore = &s
		}
	}
	return r
}

func (r *Runtime) reset(gap bool) {
	r.futures = live.NewCanonicalStream(func(b market.SecondBar) error { return r.addBar("futures", b) })
	r.spot = live.NewCanonicalStream(func(b market.SecondBar) error { return r.addBar("spot", b) })
	r.v1 = mainfeature.NewEngine()
	r.v2 = featurev2.NewStreamingEngine()
	r.pairs = map[int64]*pair{}
	r.external = nil
	r.lastExternal = map[string]int64{}
	r.metricSources = map[string]MetricSourceHealth{}
	r.futuresHistory = nil
	r.spotHistory = nil
	r.externalHistory = nil
	r.historyStart, r.lastBar = 0, 0
	r.lastReason = ""
	r.gap = gap
}

// invalidate preserves the cause of every loss of live warm state. Counters
// intentionally survive reset; a restore_applied flag alone is not readiness.
func (r *Runtime) invalidate(reason string) {
	r.stats.RuntimeResets++
	r.stats.LastResetReason = reason
	r.reset(true)
}

// Connection changes invalidate continuity. A reconnect never bridges an
// unobserved interval with synthetic no-trade bars.
func (r *Runtime) Connection(source string, connected bool) {
	if source != "futures" && source != "spot" {
		return
	}
	if !connected && r.connected[source] {
		r.restore = nil
		r.firstEvents = nil
		r.invalidate("LIVE_DISCONNECT:" + source)
		if source == "futures" {
			r.stats.FuturesGaps++
		} else {
			r.stats.SpotGaps++
		}
	}
	r.connected[source] = connected
}

func (r *Runtime) AddTrade(event live.CaptureEvent) error {
	source := ""
	switch event.Source {
	case "futures_aggTrade":
		source = "futures"
	case "spot_aggTrade":
		source = "spot"
	default:
		return fmt.Errorf("unknown trade source")
	}
	r.lastReceive[source] = event.ReceiveTimestampMs
	if r.restore != nil {
		r.firstEvents = append(r.firstEvents, event)
		first := map[string]live.CaptureEvent{}
		for _, e := range r.firstEvents {
			if _, ok := first[e.Source]; !ok {
				first[e.Source] = e
			}
		}
		if len(first) < 2 && len(r.firstEvents) < 4096 {
			return nil
		}
		buffered := r.firstEvents
		r.firstEvents = nil
		r.tryRestore(first, time.UnixMilli(event.ReceiveTimestampMs))
		for _, e := range buffered {
			if err := r.addTrade(e); err != nil {
				return err
			}
		}
		return nil
	}
	return r.addTrade(event)
}

func lastTradeID(rows []market.SecondBar) int64 {
	for i := len(rows) - 1; i >= 0; i-- {
		if rows[i].LastAggTradeID > 0 {
			return rows[i].LastAggTradeID
		}
	}
	return 0
}

func (r *Runtime) tryRestore(first map[string]live.CaptureEvent, now time.Time) {
	s := r.restore
	r.restore = nil
	if s == nil {
		return
	}
	r.stats.RestoreAttempted = true
	r.stats.RestoreSnapshotLastEventMs = s.LastEventTimeMs
	r.stats.RestoreExpectedFuturesID = lastTradeID(s.Futures) + 1
	r.stats.RestoreExpectedSpotID = lastTradeID(s.Spot) + 1
	f, fo := first["futures_aggTrade"]
	p, po := first["spot_aggTrade"]
	if fo {
		r.stats.RestoreActualFuturesID = f.ID
		r.stats.RestoreActualFuturesMs = f.SourceTimestampMs / 1000 * 1000
	}
	if po {
		r.stats.RestoreActualSpotID = p.ID
		r.stats.RestoreActualSpotMs = p.SourceTimestampMs / 1000 * 1000
	}
	reject := func(reason string) {
		r.stats.RestoreApplied = false
		r.stats.RestoreRejectReason = reason
		r.invalidate("RESTORE_REJECTED:" + reason)
	}
	if !fo || !po {
		reject("MISSING_FIRST_EVENT")
		return
	}
	if r.stats.RestoreActualFuturesMs <= s.LastEventTimeMs || r.stats.RestoreActualSpotMs <= s.LastEventTimeMs {
		reject("TIMESTAMP_NOT_AFTER_SNAPSHOT")
		return
	}
	if f.ID != r.stats.RestoreExpectedFuturesID {
		reject("FUTURES_TRADE_ID_DISCONTINUITY")
		return
	}
	if p.ID != r.stats.RestoreExpectedSpotID {
		reject("SPOT_TRADE_ID_DISCONTINUITY")
		return
	}
	restoreAt := f.SourceTimestampMs
	if p.SourceTimestampMs > restoreAt {
		restoreAt = p.SourceTimestampMs
	}
	v1, v2, err := s.RestoreEngines(s.LastEventTimeMs+1000, s.LastEventTimeMs+1000, time.UnixMilli(restoreAt))
	if err != nil {
		reject("ENGINE_RESTORE_FAILED")
		return
	}
	fs, err := live.NewResumedCanonicalStream(func(b market.SecondBar) error { return r.addBar("futures", b) }, s.Futures[len(s.Futures)-1], lastTradeID(s.Futures))
	if err != nil {
		reject("FUTURES_CANONICAL_RESUME_FAILED")
		return
	}
	ps, err := live.NewResumedCanonicalStream(func(b market.SecondBar) error { return r.addBar("spot", b) }, s.Spot[len(s.Spot)-1], lastTradeID(s.Spot))
	if err != nil {
		reject("SPOT_CANONICAL_RESUME_FAILED")
		return
	}
	r.futures, r.spot, r.v1, r.v2 = fs, ps, v1, v2
	r.futuresHistory = append([]market.SecondBar(nil), s.Futures...)
	r.spotHistory = append([]market.SecondBar(nil), s.Spot...)
	r.externalHistory = append([]live.ExternalObservation(nil), s.External...)
	r.historyStart = s.Futures[0].TimestampMs
	r.lastBar = s.LastEventTimeMs
	r.gap = false
	r.stats.BootstrapSeeded = true
	r.stats.RestoreApplied = true
	r.stats.RestoreRejectReason = ""
	// The snapshot stores five raw metrics_* sources, but parseExternal merges
	// them into one canonical "metrics" stream. Derive the watermarks from the
	// *restored V2 state*, never the raw snapshot names or the possibly newer
	// raw observations that were received but not yet applied to V2.
	cursors := map[string]int64{}
	if n := len(s.V2State.Metrics); n > 0 {
		cursors["metrics"] = s.V2State.Metrics[n-1].TimestampMs
	}
	for _, item := range []struct {
		name string
		rows []featurev2.KlineObservation
	}{
		{"mark", s.V2State.Mark},
		{"index", s.V2State.Index},
		{"premium", s.V2State.Premium},
	} {
		if n := len(item.rows); n > 0 {
			cursors[item.name] = item.rows[n-1].OpenTimeMs
		}
	}
	if n := len(s.V2State.Funding); n > 0 {
		cursors["funding"] = s.V2State.Funding[n-1].TimestampMs
	}
	// AddExternal may already have queued an initial public poll while the
	// first two exchange streams were arriving. Drop only observations already
	// inside the restored engine and retain genuinely newer queued sources.
	pending := r.external[:0]
	for _, value := range r.external {
		if value.sourceMs > cursors[value.dataset] {
			pending = append(pending, value)
		}
	}
	r.external = pending
	for name, ts := range cursors {
		if ts > r.lastExternal[name] {
			r.lastExternal[name] = ts
		}
	}
}

func (r *Runtime) addTrade(event live.CaptureEvent) error {
	var stream *live.CanonicalStream
	var source string
	if event.Source == "futures_aggTrade" {
		stream = r.futures
		source = "futures"
	} else {
		stream = r.spot
		source = "spot"
	}
	if r.stats.BootstrapSeeded && event.ID <= stream.LastID() {
		r.stats.DuplicateEvents++
		return nil
	}
	beforeD, beforeR, beforeG := stream.Duplicates, stream.Reverse, stream.IDGaps
	err := stream.Add(event)
	r.stats.DuplicateEvents += stream.Duplicates - beforeD
	r.stats.ReverseEvents += stream.Reverse - beforeR
	r.stats.IDGaps += stream.IDGaps - beforeG
	if err != nil {
		r.invalidate("CANONICAL_STREAM_ERROR:" + source + ":" + err.Error())
		if source == "futures" {
			r.stats.FuturesGaps++
		} else {
			r.stats.SpotGaps++
		}
		return err
	}
	return nil
}

func (r *Runtime) addBar(source string, b market.SecondBar) error {
	if source == "futures" {
		r.stats.FuturesBars++
	} else {
		r.stats.SpotBars++
	}
	p := r.pairs[b.TimestampMs]
	if p == nil {
		p = &pair{}
		r.pairs[b.TimestampMs] = p
	}
	copy := b
	if source == "futures" {
		p.futures = &copy
		r.futuresHistory = append(r.futuresHistory, copy)
		r.futuresHistory = pruneBars(r.futuresHistory, b.TimestampMs-warmstate.RetentionMs)
	} else {
		p.spot = &copy
		r.spotHistory = append(r.spotHistory, copy)
		r.spotHistory = pruneBars(r.spotHistory, b.TimestampMs-warmstate.RetentionMs)
	}
	r.drain()
	return nil
}

func (r *Runtime) drain() {
	for len(r.pairs) > 0 {
		var earliest int64
		for ts := range r.pairs {
			if earliest == 0 || ts < earliest {
				earliest = ts
			}
		}
		p := r.pairs[earliest]
		if p.futures == nil || p.spot == nil {
			if r.historyStart == 0 {
				for ts, other := range r.pairs {
					if ts > earliest && other.futures != nil && other.spot != nil {
						delete(r.pairs, earliest)
						r.drain()
						return
					}
				}
				if len(r.pairs) > 60 {
					delete(r.pairs, earliest)
					r.drain()
				}
				return
			}
			if len(r.pairs) > 60 {
				r.invalidate("CANONICAL_PAIR_INCOMPLETE")
				r.stats.FuturesGaps++
				r.stats.SpotGaps++
			}
			return
		}
		delete(r.pairs, earliest)
		if r.lastBar != 0 && earliest != r.lastBar+1000 {
			r.invalidate("CANONICAL_TIME_DISCONTINUITY")
			r.stats.FuturesGaps++
			r.stats.SpotGaps++
		}
		if r.historyStart == 0 {
			r.historyStart = earliest
		}
		r.lastBar = earliest
		if err := r.v2.AddSpot(*p.spot); err != nil {
			r.invalidate("SPOT_ENGINE_ERROR:" + err.Error())
			r.stats.SpotGaps++
			return
		}
		row, err := r.v1.Add(*p.futures)
		if err != nil {
			r.invalidate("FUTURES_FEATURE_ERROR:" + err.Error())
			r.stats.FuturesGaps++
			return
		}
		decision := earliest + 1000
		if decision%5000 != 0 {
			continue
		}
		for len(r.external) > 0 && r.external[0].receiveMs <= decision {
			x := r.external[0]
			r.external = r.external[1:]
			var e error
			switch x.dataset {
			case "metrics":
				e = r.v2.AddMetrics(*x.metric)
			case "mark":
				e = r.v2.AddMark(*x.kline)
			case "index":
				e = r.v2.AddIndex(*x.kline)
			case "premium":
				e = r.v2.AddPremium(*x.kline)
			case "funding":
				e = r.v2.AddFunding(*x.funding)
			}
			if e != nil {
				r.invalidate("EXTERNAL_ENGINE_ERROR:" + x.dataset + ":" + e.Error())
				return
			}
		}
		if row == nil {
			r.stats.NotReadyDecisions++
			continue
		}
		start := time.Now()
		snapshot, reason, err := r.v2.Compute(decision, r.historyStart, *row)
		latencyUs := float64(time.Since(start).Nanoseconds()) / 1000
		r.v2.Prune(decision)
		r.stats.FeatureDecisions++
		r.stats.LastDecisionMs = decision
		r.lastReason = reason
		if reason == featurev2.FutureObservation {
			r.stats.FutureObservations++
		}
		if err != nil && reason != featurev2.FutureObservation {
			r.invalidate("FEATURE_V2_ERROR:" + string(reason) + ":" + err.Error())
			return
		}
		if reason == featurev2.Eligible {
			r.stats.EligibleDecisions++
		} else {
			r.stats.NotReadyDecisions++
		}
		if r.onDecision != nil && r.emissionEnabled {
			r.onDecision(snapshot, reason, latencyUs, p.futures.Close)
		}
	}
}

func (r *Runtime) AddExternal(rows []live.ExternalObservation) error {
	parsed, err := parseExternal(rows)
	if err != nil {
		return err
	}
	r.recordMetricSources(rows, parsed)
	r.externalHistory = append(r.externalHistory, rows...)
	if r.lastBar > 0 {
		cutoff := r.lastBar - warmstate.RetentionMs - asof.FundingMaxFreshAgeMs
		at := 0
		for at < len(r.externalHistory) && r.externalHistory[at].SourceTimestampMs < cutoff {
			at++
		}
		r.externalHistory = append([]live.ExternalObservation(nil), r.externalHistory[at:]...)
	}
	for _, x := range parsed {
		if x.sourceMs <= r.lastExternal[x.dataset] {
			continue
		}
		r.lastExternal[x.dataset] = x.sourceMs
		r.external = append(r.external, x)
		r.stats.ExternalUpdates++
	}
	sort.Slice(r.external, func(i, j int) bool {
		if r.external[i].receiveMs == r.external[j].receiveMs {
			return r.external[i].sourceMs < r.external[j].sourceMs
		}
		return r.external[i].receiveMs < r.external[j].receiveMs
	})
	if len(r.external) > 128 {
		return fmt.Errorf("external observation queue overflow")
	}
	return nil
}

func pruneBars(rows []market.SecondBar, cutoff int64) []market.SecondBar {
	at := 0
	for at < len(rows) && rows[at].TimestampMs < cutoff {
		at++
	}
	return append([]market.SecondBar(nil), rows[at:]...)
}

// Snapshot returns the authoritative restart artifact for this runtime.
func (r *Runtime) Snapshot(now time.Time) (warmstate.Snapshot, error) {
	if len(r.futuresHistory) == 0 || len(r.spotHistory) == 0 {
		return warmstate.Snapshot{}, ErrWarmStateIncomplete
	}
	if r.historyStart <= 0 || r.lastBar < r.historyStart || r.lastBar-r.historyStart < warmstate.RequiredWarmupMs {
		return warmstate.Snapshot{}, ErrWarmStateIncomplete
	}
	through := func(rows []market.SecondBar) []market.SecondBar {
		end := sort.Search(len(rows), func(i int) bool { return rows[i].TimestampMs > r.lastBar })
		return rows[:end]
	}
	return warmstate.NewSnapshotWithEngines(through(r.futuresHistory), through(r.spotHistory), r.externalHistory, r.v1, r.v2, now)
}

func (r *Runtime) recordMetricSources(rows []live.ExternalObservation, parsed []externalValue) {
	responseRows := map[string]int{}
	responseNewest := map[string]int64{}
	for _, row := range rows {
		if len(row.Dataset) < len("metrics_") || row.Dataset[:len("metrics_")] != "metrics_" {
			continue
		}
		responseRows[row.Dataset]++
		if row.SourceTimestampMs > responseNewest[row.Dataset] {
			responseNewest[row.Dataset] = row.SourceTimestampMs
		}
		h := r.metricSources[row.Dataset]
		h.ObservationCount++
		h.PollingIntervalMs = 30_000
		h.LastPollResult = "PASS"
		h.HTTPStatus = 200
		h.LastError = ""
		if row.ReceiveTimestampMs > h.LastPollAtMs {
			h.LastPollAtMs = row.ReceiveTimestampMs
		}
		if row.SourceTimestampMs >= h.LastSourceTimestampMs {
			h.LastSourceTimestampMs = row.SourceTimestampMs
			h.LastReceiveTimestampMs = row.ReceiveTimestampMs
			usable, err := asof.LiveUsableAt(asof.Metrics, asof.Observation[struct{}]{SourceTimestampMs: row.SourceTimestampMs, ReceiveTimestampMs: row.ReceiveTimestampMs})
			if err == nil {
				h.UsableAtMs = usable
			}
		}
		r.metricSources[row.Dataset] = h
	}
	for dataset, count := range responseRows {
		h := r.metricSources[dataset]
		h.ResponseRows = count
		h.ResponseNewestMs = responseNewest[dataset]
		r.metricSources[dataset] = h
	}
	for _, value := range parsed {
		if value.dataset != "metrics" {
			continue
		}
		for dataset, newest := range responseNewest {
			if newest >= value.sourceMs {
				h := r.metricSources[dataset]
				if value.sourceMs > h.StoredObservationMs {
					h.StoredObservationMs = value.sourceMs
					r.metricSources[dataset] = h
				}
			}
		}
	}
}

func (r *Runtime) Status(now time.Time) Stats {
	s := r.stats
	s.SourceLastMs = make(map[string]int64, len(r.lastExternal))
	for key, value := range r.lastExternal {
		s.SourceLastMs[key] = value
	}
	s.MetricSources = make(map[string]MetricSourceHealth, len(r.metricSources))
	for key, value := range r.metricSources {
		value.FreshnessLimitMs = asof.MetricsMaxFreshAgeMs
		value.CurrentAgeMs = now.UnixMilli() - value.LastSourceTimestampMs
		value.Status = "FRESH"
		if value.LastSourceTimestampMs <= 0 {
			value.Status = "UNAVAILABLE"
		} else if value.CurrentAgeMs > value.FreshnessLimitMs {
			value.Status = "STALE"
		}
		s.MetricSources[key] = value
	}
	available := int64(0)
	if r.lastBar >= r.historyStart && r.historyStart > 0 {
		available = r.lastBar - r.historyStart
	}
	if available > warmstate.RequiredWarmupMs {
		available = warmstate.RequiredWarmupMs
	}
	status := warmstate.Status{Status: "CAPTURING", Source: "traderui_live_runtime", RequiredMs: warmstate.RequiredWarmupMs, AvailableMs: available, MissingMs: warmstate.RequiredWarmupMs - available, ProgressPercent: 100 * float64(available) / float64(warmstate.RequiredWarmupMs), InputConnected: r.connected["futures"] && r.connected["spot"], CurrentCaptureStartMs: r.historyStart, LastEventTimeMs: r.lastBar, LastCheckpointMs: now.UnixMilli(), FuturesBars: int(s.FuturesBars), SpotBars: int(s.SpotBars), FuturesGaps: int(s.FuturesGaps), SpotGaps: int(s.SpotGaps), ExternalObservations: int(s.ExternalUpdates), FutureObservations: s.FutureObservations}
	status.RuntimeResets = s.RuntimeResets
	status.LastResetReason = s.LastResetReason
	status.FuturesHistoryMs = available
	status.SpotHistoryMs = available
	status.BootstrapReady = s.BootstrapSeeded
	if r.lastReceive["futures"] > 0 && r.lastReceive["spot"] > 0 {
		status.LastReceiveTimeMs = r.lastReceive["futures"]
		if r.lastReceive["spot"] < status.LastReceiveTimeMs {
			status.LastReceiveTimeMs = r.lastReceive["spot"]
		}
	}
	status.ExternalSourceCounts = map[string]int{}
	for key := range r.lastExternal {
		status.ExternalSourceCounts[key] = 1
	}
	if r.gap && r.historyStart == 0 {
		status.Status = "GAP"
	} else if r.historyStart > 0 {
		status.Status = "WARMING_UP"
	}
	if !status.InputConnected || (r.lastReceive["futures"] > 0 && now.UnixMilli()-r.lastReceive["futures"] > 10_000) || (r.lastReceive["spot"] > 0 && now.UnixMilli()-r.lastReceive["spot"] > 10_000) {
		if r.historyStart > 0 {
			status.Status = "STALE"
		}
	}
	if available >= warmstate.RequiredWarmupMs && r.lastReason == featurev2.Eligible && status.InputConnected && status.LastReceiveTimeMs > 0 && now.UnixMilli()-status.LastReceiveTimeMs <= 10_000 {
		status.Status = "READY"
		status.Ready = true
	}
	s.Status = status
	return s
}
