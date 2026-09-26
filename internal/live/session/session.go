// Package session owns the shared public live runtime used by UI and CLI adapters.
package session

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	live "binance_trader/internal/live/binance"
	"binance_trader/internal/live/bootstrap"
	"binance_trader/internal/live/runtimefeature"
	"binance_trader/internal/live/warmstate"
	"binance_trader/internal/market"
)

type Options struct {
	Duration     time.Duration
	SnapshotPath string
	OnDecision   runtimefeature.DecisionSink
	OnStatus     func(runtimefeature.Stats)
	OnEvent      func(live.CaptureEvent)
	OnStarted    func(Result)
}

type Result struct {
	StartupMode                                                             string
	StartupMs                                                               int64
	LiveDurationMs                                                          int64
	SnapshotRestored                                                        bool
	FullBootstrap                                                           bool
	CatchupEvents                                                           int64
	FuturesMessages                                                         int64
	SpotMessages                                                            int64
	ExternalUpdates                                                         int64
	FuturesBars                                                             int64
	SpotBars                                                                int64
	FeatureDecisions                                                        int64
	EligibleDecisions                                                       int64
	FutureObservations                                                      int64
	DuplicateEvents                                                         int64
	ReverseEvents                                                           int64
	IDGaps                                                                  int64
	FinalStatus                                                             runtimefeature.Stats
	RestoreAttempted                                                        bool
	RestoreApplied                                                          bool
	RestoreRejected                                                         bool
	RestoreRejectReason                                                     string
	RestoreExpectedFuturesID, RestoreActualFuturesID                        int64
	RestoreExpectedSpotID, RestoreActualSpotID                              int64
	RestoreSnapshotLastEventMs, RestoreActualFuturesMs, RestoreActualSpotMs int64
}

type externalResult struct {
	source      string
	attemptedAt time.Time
	rows        []live.ExternalObservation
	err         error
}

func Run(ctx context.Context, options Options) (Result, error) {
	if err := validateOptions(options); err != nil {
		return Result{}, err
	}
	if ctx.Err() != nil {
		return Result{}, nil
	}
	started := time.Now()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	batches := make(chan live.CaptureResult, 512)
	captureErr := make(chan error, 1)
	go func() {
		captureErr <- live.CapturePublicBatched(runCtx, 0, 500*time.Millisecond, func(batch live.CaptureResult) error {
			if options.OnEvent != nil {
				for _, event := range batch.Events {
					options.OnEvent(event)
				}
			}
			select {
			case batches <- batch:
				return nil
			case <-runCtx.Done():
				return runCtx.Err()
			}
		})
	}()
	externals := make(chan externalResult, 8)
	go pollExternal(runCtx, externals)

	var result Result
	var engine *runtimefeature.Runtime
	var snapshot warmstate.Snapshot
	restoreOK := false
	maxCatchupAge := time.Duration(warmstate.RetentionMs-warmstate.RequiredWarmupMs) * time.Millisecond
	if s, err := warmstate.Load(options.SnapshotPath); err == nil && snapshotHasWarmCoverage(s) && time.Since(time.UnixMilli(s.LastEventTimeMs)) >= 0 && time.Since(time.UnixMilli(s.LastEventTimeMs)) <= maxCatchupAge {
		snapshot, restoreOK = s, true
	}
	buffered, health, err := collectLiveStart(runCtx, batches)
	if err != nil {
		if ctx.Err() != nil {
			return result, nil
		}
		return result, err
	}
	fullBootstrap := func(mode string) error {
		boot, fetchErr := bootstrap.Fetch(runCtx, bootstrap.Options{})
		if fetchErr != nil {
			return fetchErr
		}
		for {
			select {
			case batch := <-batches:
				buffered = append(buffered, batch.Events...)
				health = batch.Health
			default:
				goto drained
			}
		}
	drained:
		engine = runtimefeature.New("", time.Now(), options.OnDecision)
		if seedErr := engine.SeedBootstrap(boot.FuturesBars, boot.SpotBars, boot.External, boot.FetchCompletedAtMs); seedErr != nil {
			return seedErr
		}
		catchup, bridgeErr := bridge(boot.Sources["futures_aggTrades"].LastID, boot.Sources["spot_aggTrades"].LastID, buffered)
		if bridgeErr != nil {
			return bridgeErr
		}
		result.CatchupEvents += int64(len(catchup))
		engine.Connection("futures", health["futures_aggTrade"].Connected)
		engine.Connection("spot", health["spot_aggTrade"].Connected)
		if addErr := addEvents(engine, mergeHandoff(catchup, buffered)); addErr != nil {
			return addErr
		}
		result.StartupMode, result.FullBootstrap = mode, true
		return nil
	}
	if restoreOK {
		engine = runtimefeature.NewWithRestoreMaxAge(options.SnapshotPath, time.Now(), maxCatchupAge, options.OnDecision)
		engine.DisableLiveEmission()
		catchup, err := bridge(lastAggTradeID(snapshot.Futures), lastAggTradeID(snapshot.Spot), buffered)
		if err != nil {
			return result, err
		}
		result.CatchupEvents += int64(len(catchup))
		engine.Connection("futures", health["futures_aggTrade"].Connected)
		engine.Connection("spot", health["spot_aggTrade"].Connected)
		if err = addEvents(engine, mergeHandoff(catchup, buffered)); err != nil {
			return result, err
		}
		restoreStats := engine.Status(time.Now())
		copyRestoreDiagnostics(&result, restoreStats)
		if restoreStats.RestoreApplied {
			result.StartupMode, result.SnapshotRestored = "SNAPSHOT_RESTORE", true
		} else {
			result.RestoreRejected = true
			if result.RestoreRejectReason == "" {
				result.RestoreRejectReason = "RESTORE_NOT_ATTEMPTED"
			}
			if err = fullBootstrap("RESTORE_REJECTED_FULL_BOOTSTRAP"); err != nil {
				return result, fmt.Errorf("restore rejected (%s), fallback bootstrap failed: %w", result.RestoreRejectReason, err)
			}
		}
	} else if err = fullBootstrap("FULL_BOOTSTRAP"); err != nil {
		return result, err
	}
	engine.EnableLiveEmission()
	if err = saveSnapshot(engine, options.SnapshotPath); err != nil {
		return result, err
	}
	result.StartupMs = time.Since(started).Milliseconds()
	baseline := engine.Status(time.Now())
	if options.OnStarted != nil {
		options.OnStarted(result)
	}
	liveStarted := time.Now()
	finalize := func() {
		finalizeResult(&result, baseline, engine.Status(time.Now()), time.Since(liveStarted))
	}
	var deadline <-chan time.Time
	var deadlineTimer *time.Timer
	if options.Duration > 0 {
		deadlineTimer = time.NewTimer(options.Duration)
		deadline = deadlineTimer.C
		defer deadlineTimer.Stop()
	}
	snapshotTicker := time.NewTicker(30 * time.Second)
	defer snapshotTicker.Stop()
	for {
		select {
		case batch := <-batches:
			for _, event := range batch.Events {
				if event.Source == "futures_aggTrade" {
					result.FuturesMessages++
				} else if event.Source == "spot_aggTrade" {
					result.SpotMessages++
				}
			}
			if err = addEvents(engine, batch.Events); err != nil {
				finalize()
				return result, err
			}
			if options.OnStatus != nil {
				options.OnStatus(engine.Status(time.Now()))
			}
		case external := <-externals:
			engine.RecordExternalPoll(external.source, external.attemptedAt, external.err)
			if external.err == nil {
				if err = engine.AddExternal(external.rows); err != nil {
					finalize()
					return result, err
				}
			}
		case <-snapshotTicker.C:
			if err = saveSnapshot(engine, options.SnapshotPath); err != nil {
				finalize()
				return result, err
			}
		case <-deadline:
			cancel()
			if err = saveSnapshot(engine, options.SnapshotPath); err != nil {
				finalize()
				return result, err
			}
			finalize()
			return result, nil
		case err = <-captureErr:
			finalize()
			if saveErr := saveSnapshot(engine, options.SnapshotPath); saveErr != nil {
				return result, saveErr
			}
			if runCtx.Err() != nil {
				if ctx.Err() != nil {
					return result, nil
				}
				return result, runCtx.Err()
			}
			if err == nil {
				err = errors.New("EARLY_CAPTURE_TERMINATION")
			}
			return result, err
		case <-ctx.Done():
			if saveErr := saveSnapshot(engine, options.SnapshotPath); saveErr != nil {
				finalize()
				return result, saveErr
			}
			finalize()
			return result, nil
		}
	}
}

func validateOptions(options Options) error {
	if options.Duration < 0 || options.SnapshotPath == "" {
		return fmt.Errorf("invalid live session options")
	}
	return nil
}

func snapshotHasWarmCoverage(snapshot warmstate.Snapshot) bool {
	return snapshot.V1State != nil && snapshot.V2State != nil && len(snapshot.Futures) > 0 && len(snapshot.Spot) > 0 && snapshot.LastEventTimeMs-snapshot.Futures[0].TimestampMs >= warmstate.RequiredWarmupMs && snapshot.LastEventTimeMs-snapshot.Spot[0].TimestampMs >= warmstate.RequiredWarmupMs
}

func copyRestoreDiagnostics(result *Result, stats runtimefeature.Stats) {
	result.RestoreAttempted = stats.RestoreAttempted
	result.RestoreApplied = stats.RestoreApplied
	result.RestoreRejectReason = stats.RestoreRejectReason
	result.RestoreExpectedFuturesID = stats.RestoreExpectedFuturesID
	result.RestoreActualFuturesID = stats.RestoreActualFuturesID
	result.RestoreExpectedSpotID = stats.RestoreExpectedSpotID
	result.RestoreActualSpotID = stats.RestoreActualSpotID
	result.RestoreSnapshotLastEventMs = stats.RestoreSnapshotLastEventMs
	result.RestoreActualFuturesMs = stats.RestoreActualFuturesMs
	result.RestoreActualSpotMs = stats.RestoreActualSpotMs
}

func finalizeResult(result *Result, baseline, final runtimefeature.Stats, duration time.Duration) {
	result.LiveDurationMs = duration.Milliseconds()
	result.ExternalUpdates = final.ExternalUpdates - baseline.ExternalUpdates
	result.FuturesBars = final.FuturesBars - baseline.FuturesBars
	result.SpotBars = final.SpotBars - baseline.SpotBars
	result.FeatureDecisions = final.FeatureDecisions - baseline.FeatureDecisions
	result.EligibleDecisions = final.EligibleDecisions - baseline.EligibleDecisions
	result.FutureObservations = final.FutureObservations - baseline.FutureObservations
	result.DuplicateEvents = final.DuplicateEvents - baseline.DuplicateEvents
	result.ReverseEvents = final.ReverseEvents - baseline.ReverseEvents
	result.IDGaps = final.IDGaps - baseline.IDGaps
	result.FinalStatus = final
}

func lastAggTradeID(rows []market.SecondBar) int64 {
	for i := len(rows) - 1; i >= 0; i-- {
		if rows[i].LastAggTradeID > 0 {
			return rows[i].LastAggTradeID
		}
	}
	return 0
}

func mergeHandoff(catchup, buffered []live.CaptureEvent) []live.CaptureEvent {
	maxID := map[string]int64{}
	for _, event := range catchup {
		if event.ID > maxID[event.Source] {
			maxID[event.Source] = event.ID
		}
	}
	merged := make([]live.CaptureEvent, 0, len(catchup)+len(buffered))
	merged = append(merged, catchup...)
	for _, event := range buffered {
		if event.ID > 0 && event.ID <= maxID[event.Source] {
			continue
		}
		merged = append(merged, event)
	}
	sortEvents(merged)
	return merged
}

func pollExternal(ctx context.Context, out chan<- externalResult) {
	pollExternalWithFetch(ctx, out, live.FetchPublicKlineObservations, live.FetchPublicSlowObservations, 10*time.Second, 30*time.Second)
}

// pollExternalWithFetch separates cadences without changing the receive pipeline.
func pollExternalWithFetch(ctx context.Context, out chan<- externalResult, fast, slow func(context.Context) ([]live.ExternalObservation, error), fastInterval, slowInterval time.Duration) {
	var wg sync.WaitGroup
	poll := func(source string, interval, timeout time.Duration, fetcher func(context.Context) ([]live.ExternalObservation, error)) {
		defer wg.Done()
		fetch := func() bool {
			attemptedAt := time.Now().UTC()
			pollCtx, cancel := context.WithTimeout(ctx, timeout)
			rows, err := fetcher(pollCtx)
			cancel()
			select {
			case out <- externalResult{source: source, attemptedAt: attemptedAt, rows: rows, err: err}:
				return true
			case <-ctx.Done():
				return false
			}
		}
		if !fetch() {
			return
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if !fetch() {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}

	// Run the one-minute kline and five-minute metrics pollers independently.
	// A slow metrics request cannot delay the 10-second kline cadence.
	wg.Add(2)
	go poll("kline", fastInterval, 15*time.Second, fast)
	go poll("metrics_funding", slowInterval, 25*time.Second, slow)
	<-ctx.Done()
	wg.Wait()
}

func collectLiveStart(ctx context.Context, batches <-chan live.CaptureResult) ([]live.CaptureEvent, map[string]*live.SourceHealth, error) {
	var events []live.CaptureEvent
	var health map[string]*live.SourceHealth
	timer := time.NewTimer(20 * time.Second)
	defer timer.Stop()
	for {
		select {
		case batch := <-batches:
			events = append(events, batch.Events...)
			health = batch.Health
			seenF, seenS := false, false
			for _, event := range events {
				seenF = seenF || event.Source == "futures_aggTrade"
				seenS = seenS || event.Source == "spot_aggTrade"
			}
			if seenF && seenS {
				return events, health, nil
			}
		case <-timer.C:
			return nil, nil, fmt.Errorf("live source startup timeout")
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
	}
}

func bridge(lastFutures, lastSpot int64, buffered []live.CaptureEvent) ([]live.CaptureEvent, error) {
	maxID := map[string]int64{}
	for _, event := range buffered {
		if event.ID > maxID[event.Source] {
			maxID[event.Source] = event.ID
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var rows []live.CaptureEvent
	for _, item := range []struct {
		source string
		last   int64
	}{{"futures_aggTrade", lastFutures}, {"spot_aggTrade", lastSpot}} {
		if maxID[item.source] <= item.last {
			continue
		}
		for from := item.last + 1; from <= maxID[item.source]; {
			to := from + 9_999
			if to > maxID[item.source] {
				to = maxID[item.source]
			}
			got, err := bootstrap.FetchIDRange(ctx, item.source, from, to)
			if err != nil {
				return nil, fmt.Errorf("%s handoff %d..%d: %w", item.source, from, to, err)
			}
			receive := time.Now().UnixMilli()
			for i := range got {
				got[i].ReceiveTimestampMs = receive
				got[i].Origin = live.OriginLiveReceived
			}
			rows = append(rows, got...)
			from = to + 1
		}
	}
	sortEvents(rows)
	return rows, nil
}

func addEvents(engine *runtimefeature.Runtime, events []live.CaptureEvent) error {
	copyEvents := append([]live.CaptureEvent(nil), events...)
	sortEvents(copyEvents)
	for _, event := range copyEvents {
		if event.Source != "futures_aggTrade" && event.Source != "spot_aggTrade" {
			continue
		}
		if err := engine.AddTrade(event); err != nil {
			return err
		}
	}
	return nil
}

func sortEvents(events []live.CaptureEvent) {
	sort.SliceStable(events, func(i, j int) bool {
		if events[i].SourceTimestampMs == events[j].SourceTimestampMs {
			if events[i].Source == events[j].Source {
				return events[i].ID < events[j].ID
			}
			return events[i].Source < events[j].Source
		}
		return events[i].SourceTimestampMs < events[j].SourceTimestampMs
	})
}

func saveSnapshot(engine *runtimefeature.Runtime, path string) error {
	snapshot, err := engine.Snapshot(time.Now().UTC())
	if err != nil {
		if errors.Is(err, runtimefeature.ErrWarmStateIncomplete) {
			return nil
		}
		return err
	}
	_, err = warmstate.Save(path, snapshot)
	return err
}
