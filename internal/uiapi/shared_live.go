package uiapi

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	featurev2 "binance_trader/internal/feature/main/v2"
	live "binance_trader/internal/live/binance"
	"binance_trader/internal/live/runtimefeature"
	livesession "binance_trader/internal/live/session"
	"binance_trader/internal/live/warmstate"
)

func (s *Server) runSharedLiveRuntime(ctx context.Context) {
	s.mu.Lock()
	s.autoFeatureSource = true
	s.logLocked("Shared live runtime starting")
	s.mu.Unlock()
	decision := func(snapshot featurev2.Snapshot, reason featurev2.Reason, latencyUs, entryPrice float64) {
		s.mu.Lock()
		s.featureLatency.add(latencyUs)
		running := s.states[TradingEnvironmentTestnet].AutoRunning
		s.mu.Unlock()
		if reason == featurev2.Eligible && running {
			s.enqueueAutoDecision(snapshot, entryPrice)
		}
	}
	for ctx.Err() == nil {
		started := time.Now().UTC()
		restored := false
		startupKnown := false
		var stabilizationStarted time.Time
		status := func(stats runtimefeature.Stats) {
			now := time.Now().UTC()
			liveHealthReady := stats.Status.Ready
			stats.Status.Source = "traderui_live_runtime"
			stats.Status.BootstrapReady = true
			stats.Status.HandoffReady = true
			stats.Status.LiveHealthReady = liveHealthReady
			stats.Status.BootstrapStartedAtMs = started.UnixMilli()
			stats.Status.BootstrapCompletedAtMs = started.UnixMilli()
			stats.Status.HandoffStartedAtMs = started.UnixMilli()
			stats.Status.HandoffCompletedAtMs = started.UnixMilli()
			stats.Status.StabilizationTargetMs = s.bootstrapStabilization.Milliseconds()
			if startupKnown && restored {
				stats.Status.BootstrapState = "READY"
				stats.Status.StabilizationReady = true
				stats.Status.StabilizationElapsed = true
				if !liveHealthReady {
					stats.Status.Status = "BLOCKED"
					stats.Status.Ready = false
				}
			} else {
				if stabilizationStarted.IsZero() {
					stabilizationStarted = now
				}
				stats.Status.Status = "STABILIZING"
				stats.Status.BootstrapState = "STABILIZING"
				stats.Status.Ready = false
				stats.Status.StabilizationStartedAtMs = stabilizationStarted.UnixMilli()
				stats.Status.StabilizationDeadlineMs = stabilizationStarted.Add(s.bootstrapStabilization).UnixMilli()
			}
			if !liveHealthReady {
				stats.Status.BootstrapBlocker = "REQUIRED_SOURCE_STALE"
				stats.Status.BlockerSource = requiredMetricBlockerSource(stats)
			}
			if persisted, err := warmstate.PublishRuntimeResolved(s.runtimePath, stats.Status, now); err == nil {
				stats.Status = persisted
			}
			s.mu.Lock()
			s.runtimeStats = stats
			s.featureDecisions = uint64(stats.FeatureDecisions)
			s.eligibleDecisions = uint64(stats.EligibleDecisions)
			s.featureNotReadyDecisions = uint64(stats.NotReadyDecisions)
			s.mu.Unlock()
		}
		onStarted := func(result livesession.Result) {
			startupKnown = true
			restored = result.SnapshotRestored && result.RestoreApplied && !result.FullBootstrap
			s.mu.Lock()
			s.logLocked("Shared live runtime ready: " + result.StartupMode)
			s.mu.Unlock()
		}
		_, err := livesession.Run(ctx, livesession.Options{Duration: 0, SnapshotPath: warmstate.RuntimeSnapshotPath, OnDecision: decision, OnStatus: status, OnEvent: s.consumeSharedLiveEvent, OnStarted: onStarted})
		if ctx.Err() != nil {
			return
		}

		s.mu.Lock()
		if err != nil {
			s.apiErrors++
			s.logLocked("Shared live runtime restart after error: " + err.Error())
		} else {
			s.logLocked("Shared live runtime session completed normally; restarting")
		}
		s.mu.Unlock()

		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

func (s *Server) consumeSharedLiveEvent(event live.CaptureEvent) {
	s.mu.Lock()
	market := s.market
	s.marketMessages++
	s.marketReceiveLatency.add(float64(time.Now().UnixMilli() - event.SourceTimestampMs))
	switch event.Source {
	case "futures_aggTrade":
		s.futuresMessages++
		s.futuresRawMessages++
		market.FuturesPrice = event.Price
	case "spot_aggTrade":
		s.spotMessages++
		s.spotRawMessages++
		market.SpotPrice = event.Price
	case "mark_price":
		s.markRawMessages++
		market.MarkPrice = event.Price
		var row struct {
			Index string `json:"i"`
		}
		if json.Unmarshal(event.Raw, &row) == nil {
			market.IndexPrice, _ = strconv.ParseFloat(row.Index, 64)
			if market.IndexPrice != 0 {
				market.Premium = market.MarkPrice/market.IndexPrice - 1
			}
		}
	}
	market.UpdatedAt = time.Now().UTC()
	market.Connected = market.FuturesPrice > 0 && market.SpotPrice > 0 && market.MarkPrice > 0
	s.market = market
	if event.Source == "futures_aggTrade" {
		s.marketSourceUpdated["futures"] = market.UpdatedAt
	} else if event.Source == "spot_aggTrade" {
		s.marketSourceUpdated["spot"] = market.UpdatedAt
	} else if event.Source == "mark_price" {
		s.marketSourceUpdated["mark"] = market.UpdatedAt
	}
	s.mu.Unlock()
	s.broadcast(map[string]any{"type": "market", "market": market})
}
