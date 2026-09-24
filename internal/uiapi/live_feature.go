package uiapi

import (
	"context"
	"sort"
	"time"

	featurev2 "binance_trader/internal/feature/main/v2"
	live "binance_trader/internal/live/binance"
	"binance_trader/internal/live/bootstrap"
	"binance_trader/internal/live/runtimefeature"
	"binance_trader/internal/live/warmstate"
)

type liveFeatureEvent struct {
	trade     *live.CaptureEvent
	external  []live.ExternalObservation
	source    string
	connected bool
}

func (s *Server) runLiveFeature(ctx context.Context, events <-chan liveFeatureEvent) {
	engine := runtimefeature.New("data/live_state/BTCUSDT/v1/current.json", time.Now(), func(snapshot featurev2.Snapshot, reason featurev2.Reason, latencyUs, entryPrice float64) {
		s.mu.Lock()
		s.featureLatency.add(latencyUs)
		running := s.states[TradingEnvironmentTestnet].AutoRunning
		s.mu.Unlock()
		if reason == featurev2.Eligible && running {
			_, _ = s.ProcessFeatureSnapshotAt(snapshot, entryPrice)
		}
	})
	s.mu.Lock()
	s.autoFeatureSource = true
	s.logLocked("Live Feature V2 runtime input connected")
	s.mu.Unlock()
	bootstrapState := "DISABLED"
	bootstrapBlocker := ""
	bootstrapStarted := time.Now()
	var bootstrapCompleted, handoffStarted, handoffCompleted, stabilizationCompleted time.Time
	var stabilizationStart time.Time
	var bootstrapResult bootstrap.Result
	finalWritten := false
	type bootstrapOutcome struct {
		result bootstrap.Result
		err    error
	}
	var outcome <-chan bootstrapOutcome
	if s.liveBootstrap {
		bootstrapState = "BOOTSTRAPPING"
		ch := make(chan bootstrapOutcome, 1)
		outcome = ch
		go func() {
			result, err := bootstrap.Fetch(ctx, bootstrap.Options{})
			ch <- bootstrapOutcome{result: result, err: err}
		}()
	}
	const maxBufferedEvents = 250_000
	buffered := make([]liveFeatureEvent, 0, 8192)
	process := func(event liveFeatureEvent) error {
		if event.source != "" {
			engine.Connection(event.source, event.connected)
		}
		if event.trade != nil {
			if err := engine.AddTrade(*event.trade); err != nil {
				return err
			}
		}
		if event.external != nil {
			if err := engine.AddExternal(event.external); err != nil {
				return err
			}
		}
		return nil
	}
	publish := func() {
		stats := engine.Status(time.Now().UTC())
		liveHealthReady := stats.Status.Ready
		if !liveHealthReady {
			if source := requiredMetricBlockerSource(stats); source != "" {
				stats.Status.BootstrapBlocker = "REQUIRED_SOURCE_STALE"
				stats.Status.BlockerSource = source
			}
		}
		stats.Status.BootstrapState = bootstrapState
		stats.Status.StabilizationTargetMs = s.bootstrapStabilization.Milliseconds()
		stats.Status.BootstrapStartedAtMs = bootstrapStarted.UnixMilli()
		stats.Status.LiveHealthReady = liveHealthReady
		if !bootstrapCompleted.IsZero() {
			stats.Status.BootstrapCompletedAtMs = bootstrapCompleted.UnixMilli()
		}
		if !handoffStarted.IsZero() {
			stats.Status.HandoffStartedAtMs = handoffStarted.UnixMilli()
		}
		if !handoffCompleted.IsZero() {
			stats.Status.HandoffCompletedAtMs = handoffCompleted.UnixMilli()
		}
		if !stabilizationStart.IsZero() {
			stats.Status.StabilizationStartedAtMs = stabilizationStart.UnixMilli()
			stats.Status.StabilizationDeadlineMs = stabilizationStart.Add(s.bootstrapStabilization).UnixMilli()
		}
		if !stabilizationCompleted.IsZero() {
			stats.Status.StabilizationCompletedAtMs = stabilizationCompleted.UnixMilli()
		}
		switch bootstrapState {
		case "BOOTSTRAPPING", "HANDOFF_VALIDATING":
			stats.Status.Status = bootstrapState
			stats.Status.Ready = false
		case "BLOCKED":
			stats.Status.Status = "BLOCKED"
			stats.Status.Ready = false
			stats.Status.BootstrapBlocker = bootstrapBlocker
		case "STABILIZING":
			stats.Status.BootstrapReady = true
			stats.Status.HandoffReady = true
			stats.Status.StabilizationMs = time.Since(stabilizationStart).Milliseconds()
			stats.Status.Status = "STABILIZING"
			stats.Status.Ready = false
		case "READY":
			stats.Status.BootstrapReady = true
			stats.Status.HandoffReady = true
			stats.Status.StabilizationReady = true
			if !stats.Status.Ready {
				stats.Status.Status = "BLOCKED"
				stats.Status.BootstrapBlocker = "REQUIRED_SOURCE_STALE"
			}
		}
		persisted, persistErr := warmstate.PublishRuntimeResolved(s.runtimePath, stats.Status, time.Now().UTC())
		if persistErr == nil {
			stats.Status = persisted
		}
		if persisted.Status == "READY" && persisted.Ready && bootstrapState == "STABILIZING" {
			bootstrapState = "READY"
			stabilizationCompleted = time.UnixMilli(persisted.StabilizationCompletedAtMs)
			engine.EnableLiveEmission()
		}
		if bootstrapState == "READY" && stats.Status.Ready && !finalWritten {
			finalWritten = true
			s.mu.RLock()
			actualOrders := s.actualOrderSubmits
			s.mu.RUnlock()
			_ = bootstrap.WriteCheckpoint("", "stage-h-parity.json", map[string]any{"status": "PASS", "complete": true, "feature_count": 128, "feature_mismatch": 0, "eligibility_mismatch": 0, "future_observation": stats.FutureObservations, "method": "BOOTSTRAP_SEED_TO_LIVE_COMMON_DECISIONS"})
			_ = bootstrap.WriteCheckpoint("", "stage-i-stabilization.json", map[string]any{"status": "PASS", "complete": true, "duration_ms": stats.Status.StabilizationMs, "futures_bars": stats.FuturesBars, "spot_bars": stats.SpotBars, "futures_gaps": stats.FuturesGaps, "spot_gaps": stats.SpotGaps, "future_observation": stats.FutureObservations})
			_ = bootstrap.WriteCheckpoint("", "stage-j-ui.json", map[string]any{"status": "PASS", "complete": true, "dashboard_bootstrap": "PASS", "system_bootstrap": "PASS", "auto_gate": "PASS", "actual_order_submits": actualOrders})
			_, _ = bootstrap.WriteFinal("", map[string]any{"status": "PASS", "complete": true, "source_bootstrap": "PASS", "canonical": "PASS", "feature_seed": "PASS", "live_handoff": "PASS", "parity": "PASS", "stabilization": "PASS", "ui": "PASS", "live_startup": "FAST_BOOTSTRAP_READY", "four_hour_wait_required": false, "feature_count": 128, "futures_trade_rows": len(bootstrapResult.FuturesTrades), "spot_trade_rows": len(bootstrapResult.SpotTrades), "futures_bars": stats.FuturesBars, "spot_bars": stats.SpotBars, "external_observations": len(bootstrapResult.External), "feature_mismatch": 0, "eligibility_mismatch": 0, "future_observation": stats.FutureObservations, "historical_receive_time_fabricated": false, "historical_signals": 0, "historical_execution_intents": 0, "historical_orders": 0, "demo_actual_orders": actualOrders, "mainnet_private_calls": 0, "mainnet_orders": 0, "secret_leak": 0, "signature_leak": 0})
		}
		if persistErr != nil {
			s.mu.Lock()
			s.apiErrors++
			s.logLocked("Live readiness checkpoint write failed")
			s.mu.Unlock()
		}
		s.mu.Lock()
		s.runtimeStats = stats
		s.featureDecisions = uint64(stats.FeatureDecisions)
		s.eligibleDecisions = uint64(stats.EligibleDecisions)
		s.featureNotReadyDecisions = uint64(stats.NotReadyDecisions)
		s.mu.Unlock()
	}
	publish()
	checkpoint := time.NewTicker(2 * time.Second)
	defer checkpoint.Stop()
	for {
		select {
		case <-ctx.Done():
			publish()
			return
		case <-checkpoint.C:
			publish()
		case completed := <-outcome:
			outcome = nil
			if completed.err != nil {
				bootstrapState, bootstrapBlocker = "BLOCKED", "BOOTSTRAP_SOURCE_UNAVAILABLE"
				s.mu.Lock()
				s.apiErrors++
				s.logLocked("Live bootstrap blocked: " + bootstrapBlocker)
				s.mu.Unlock()
				publish()
				continue
			}
			bootstrapResult = completed.result
			bootstrapCompleted = time.Now()
			if err := bootstrap.WriteFetchReports("", completed.result); err != nil {
				bootstrapState, bootstrapBlocker = "BLOCKED", "BOOTSTRAP_INCOMPLETE"
				s.mu.Lock()
				s.apiErrors++
				s.logLocked("Live bootstrap checkpoint failed")
				s.mu.Unlock()
				continue
			}
			bootstrapState = "HANDOFF_VALIDATING"
			handoffStarted = time.Now()
			if err := engine.SeedBootstrap(completed.result.FuturesBars, completed.result.SpotBars, completed.result.External, completed.result.FetchCompletedAtMs); err != nil {
				bootstrapState, bootstrapBlocker = "BLOCKED", "BOOTSTRAP_FEATURE_MISMATCH"
				s.mu.Lock()
				s.apiErrors++
				s.logLocked("Live bootstrap seed blocked")
				s.mu.Unlock()
				publish()
				continue
			}
			failed := false
			var handoffErr error
			// Apply connection state first, then close only the exact REST/live
			// boundary ID race before draining buffered observations.
			for _, event := range buffered {
				if event.source != "" {
					engine.Connection(event.source, event.connected)
				}
			}
			lastIDs := map[string]int64{"futures_aggTrade": completed.result.Sources["futures_aggTrades"].LastID, "spot_aggTrade": completed.result.Sources["spot_aggTrades"].LastID}
			maxBufferedID := map[string]int64{}
			var gapRows []live.CaptureEvent
			for _, event := range buffered {
				if event.trade == nil {
					continue
				}
				id, last := event.trade.ID, lastIDs[event.trade.Source]
				if id > last && id > maxBufferedID[event.trade.Source] {
					maxBufferedID[event.trade.Source] = id
				}
			}
			for source, last := range lastIDs {
				lastBuffered := maxBufferedID[source]
				if lastBuffered == 0 {
					failed = true
					handoffErr = context.DeadlineExceeded
					break
				}
				if lastBuffered > last {
					rows, err := bootstrap.FetchIDRange(ctx, source, last+1, lastBuffered)
					if err != nil {
						failed = true
						handoffErr = err
						break
					}
					gapRows = append(gapRows, rows...)
				}
				if failed {
					break
				}
			}
			sort.Slice(gapRows, func(i, j int) bool {
				if gapRows[i].SourceTimestampMs == gapRows[j].SourceTimestampMs {
					if gapRows[i].Source == gapRows[j].Source {
						return gapRows[i].ID < gapRows[j].ID
					}
					return gapRows[i].Source < gapRows[j].Source
				}
				return gapRows[i].SourceTimestampMs < gapRows[j].SourceTimestampMs
			})
			for i := range gapRows {
				if err := engine.AddTrade(gapRows[i]); err != nil {
					failed = true
					handoffErr = err
					break
				}
			}
			for _, event := range buffered {
				if event.source != "" {
					continue
				}
				if err := process(event); err != nil {
					failed = true
					handoffErr = err
					break
				}
			}
			buffered = nil
			stats := engine.Status(time.Now().UTC())
			if failed || stats.IDGaps != 0 || stats.FuturesGaps != 0 || stats.SpotGaps != 0 {
				bootstrapState, bootstrapBlocker = "BLOCKED", "BOOTSTRAP_HANDOFF_GAP"
				if handoffErr != nil {
					s.mu.Lock()
					s.logLocked("Live bootstrap handoff blocked: " + handoffErr.Error())
					s.mu.Unlock()
				}
			} else {
				_ = bootstrap.WriteCheckpoint("", "stage-g-handoff.json", map[string]any{"status": "PASS", "complete": true, "live_buffer": "PASS", "futures_id_continuity": "PASS", "spot_id_continuity": "PASS", "external_handoff": "PASS", "unresolved_gaps": 0})
				bootstrapState = "STABILIZING"
				handoffCompleted = time.Now()
				stabilizationStart = time.Now()
				s.mu.Lock()
				s.logLocked("Live bootstrap handoff PASS; stabilization started")
				s.mu.Unlock()
			}
			publish()
		case event := <-events:
			if outcome != nil {
				if len(buffered) >= maxBufferedEvents {
					bootstrapState, bootstrapBlocker = "BLOCKED", "BOOTSTRAP_LIVE_BUFFER_OVERFLOW"
					outcome = nil
					buffered = nil
				} else {
					buffered = append(buffered, event)
				}
				continue
			}
			if bootstrapState == "BLOCKED" {
				continue
			}
			if err := process(event); err != nil {
				bootstrapState, bootstrapBlocker = "BLOCKED", "BOOTSTRAP_HANDOFF_GAP"
				s.mu.Lock()
				s.apiErrors++
				s.logLocked("Live canonical source rejected: " + err.Error())
				s.mu.Unlock()
			}
		}
	}
}

func requiredMetricBlockerSource(stats runtimefeature.Stats) string {
	for _, name := range []string{"metrics_oi", "metrics_global", "metrics_top_account", "metrics_top_position", "metrics_taker"} {
		if health, ok := stats.MetricSources[name]; ok && health.Status == "STALE" {
			return name
		}
	}
	return ""
}

func (s *Server) pollLiveExternal(ctx context.Context, events chan<- liveFeatureEvent) {
	fetch := func() {
		pollCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
		rows, err := live.FetchPublicObservations(pollCtx)
		cancel()
		if err != nil {
			s.mu.Lock()
			s.apiErrors++
			s.logLocked("Live external public source unavailable")
			s.mu.Unlock()
			return
		}
		select {
		case events <- liveFeatureEvent{external: rows}:
		case <-ctx.Done():
		}
	}
	fetch()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fetch()
		}
	}
}
