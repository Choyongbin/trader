package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	live "binance_trader/internal/live/binance"
	"binance_trader/internal/market"
)

const requiredWarmupMs int64 = 14_400_000

type warmupSession struct {
	Version                int
	StartedAtMs, EndedAtMs int64
	FuturesBars, SpotBars  []market.SecondBar
	External               []live.ExternalObservation
	Health                 map[string]*live.SourceHealth
	ExternalFetchError     string
}
type warmupSessionRef struct {
	Path, SHA256                                string
	StartedAtMs, EndedAtMs                      int64
	FuturesBars, SpotBars, ExternalObservations int
}
type warmupState struct {
	Version                                                                                                                                                  int
	Symbol, Status                                                                                                                                           string
	RequiredWarmupMs, AvailableContiguousHistoryMs, MissingDurationMs                                                                                        int64
	WarmupReady, ResumeValidated                                                                                                                             bool
	Sessions                                                                                                                                                 []warmupSessionRef
	FuturesBars, SpotBars, ExternalObservations, ExternalFetchErrors, DuplicateBars, ConflictingDuplicateBars, FuturesGapCount, SpotGapCount, CommonGapCount int
	ExternalSourceCounts                                                                                                                                     map[string]int
	LastUpdatedMs                                                                                                                                            int64
	Complete                                                                                                                                                 bool
}

func warmupRoot() string { return filepath.FromSlash("data/live_capture/BTCUSDT/v1") }

func runWarmupCapture(duration time.Duration, resume bool) (warmupState, error) {
	root := warmupRoot()
	if e := os.MkdirAll(filepath.Join(root, "sessions"), 0755); e != nil {
		return warmupState{}, e
	}
	if !resume {
		if _, e := os.Stat(filepath.Join(root, "capture-state.json")); e == nil {
			return warmupState{}, fmt.Errorf("existing warmup state requires -resume")
		}
	}
	canonical := newBatchCanonicalizer()
	var state warmupState
	ctx := context.Background()
	e := live.CapturePublicBatched(ctx, duration, time.Minute, func(capture live.CaptureResult) error {
		futures, err := canonical.add(capture.Events, "futures_aggTrade")
		if err != nil {
			return err
		}
		spot, err := canonical.add(capture.Events, "spot_aggTrade")
		if err != nil {
			return err
		}
		fetchCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		observations, fetchErr := live.FetchPublicObservations(fetchCtx)
		cancel()
		s := warmupSession{Version: 2, StartedAtMs: capture.StartedAtMs, EndedAtMs: capture.EndedAtMs, FuturesBars: futures, SpotBars: spot, External: observations, Health: capture.Health}
		if fetchErr != nil {
			s.ExternalFetchError = fetchErr.Error()
		}
		if len(futures) == 0 && len(spot) == 0 && len(observations) == 0 {
			return fmt.Errorf("empty warmup checkpoint")
		}
		path := filepath.Join(root, "sessions", fmt.Sprintf("session-%d-%d.json", s.StartedAtMs, s.EndedAtMs))
		if err = durable(path, s); err != nil {
			return err
		}
		state, err = auditWarmupStore(root)
		if err != nil {
			return err
		}
		state.ResumeValidated = resume || len(state.Sessions) == 1
		return durable(filepath.Join(root, "capture-state.json"), state)
	})
	if e != nil {
		return state, e
	}
	return loadWarmupState()
}

type batchCanonicalizer struct {
	carry   map[string][]live.CaptureEvent
	started map[string]bool
	lastID  map[string]int64
}

func newBatchCanonicalizer() *batchCanonicalizer {
	return &batchCanonicalizer{carry: map[string][]live.CaptureEvent{}, started: map[string]bool{}, lastID: map[string]int64{}}
}

func (c *batchCanonicalizer) add(events []live.CaptureEvent, source string) ([]market.SecondBar, error) {
	rows := c.carry[source]
	for _, event := range events {
		if event.Source == source && event.EventType == "aggTrade" && event.ID != 0 {
			rows = append(rows, event)
		}
	}
	if len(rows) == 0 {
		return nil, nil
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].SourceTimestampMs == rows[j].SourceTimestampMs {
			return rows[i].ID < rows[j].ID
		}
		return rows[i].SourceTimestampMs < rows[j].SourceTimestampMs
	})
	lastSecond := rows[len(rows)-1].SourceTimestampMs / 1000
	cut := sort.Search(len(rows), func(i int) bool { return rows[i].SourceTimestampMs/1000 >= lastSecond })
	if cut == 0 {
		c.carry[source] = rows
		return nil, nil
	}
	ready := rows[:cut]
	c.carry[source] = append([]live.CaptureEvent(nil), rows[cut:]...)
	unique := ready[:0]
	for _, row := range ready {
		if row.ID > c.lastID[source] {
			unique = append(unique, row)
			c.lastID[source] = row.ID
		}
	}
	bars, err := live.CanonicalBars(unique, source)
	if err != nil {
		return nil, err
	}
	if !c.started[source] {
		c.started[source] = true
		if len(bars) > 0 {
			bars = bars[1:]
		}
	}
	return bars, nil
}

func auditWarmupStore(root string) (warmupState, error) {
	state := warmupState{Version: 1, Symbol: "BTCUSDT", Status: "WAITING_FOR_WARMUP", RequiredWarmupMs: requiredWarmupMs, ExternalSourceCounts: map[string]int{}}
	entries, e := os.ReadDir(filepath.Join(root, "sessions"))
	if e != nil {
		return state, e
	}
	future, spot := map[int64]market.SecondBar{}, map[int64]market.SecondBar{}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		path := filepath.Join(root, "sessions", entry.Name())
		var s warmupSession
		b, e := os.ReadFile(path)
		if e != nil || json.Unmarshal(b, &s) != nil {
			return state, fmt.Errorf("invalid warmup session %s", entry.Name())
		}
		state.Sessions = append(state.Sessions, warmupSessionRef{path, hash(path), s.StartedAtMs, s.EndedAtMs, len(s.FuturesBars), len(s.SpotBars), len(s.External)})
		if s.EndedAtMs > state.LastUpdatedMs {
			state.LastUpdatedMs = s.EndedAtMs
		}
		if s.ExternalFetchError != "" {
			state.ExternalFetchErrors++
		}
		for _, x := range s.FuturesBars {
			if old, ok := future[x.TimestampMs]; ok {
				state.DuplicateBars++
				if old != x {
					state.ConflictingDuplicateBars++
				}
			} else {
				future[x.TimestampMs] = x
			}
		}
		for _, x := range s.SpotBars {
			if old, ok := spot[x.TimestampMs]; ok {
				state.DuplicateBars++
				if old != x {
					state.ConflictingDuplicateBars++
				}
			} else {
				spot[x.TimestampMs] = x
			}
		}
		seen := map[string]bool{}
		for _, x := range s.External {
			k := fmt.Sprintf("%s|%d", x.Dataset, x.SourceTimestampMs)
			if !seen[k] {
				state.ExternalSourceCounts[x.Dataset]++
				state.ExternalObservations++
				seen[k] = true
			}
		}
	}
	bridgeNoTradeSeconds(future)
	bridgeNoTradeSeconds(spot)
	sort.Slice(state.Sessions, func(i, j int) bool { return state.Sessions[i].StartedAtMs < state.Sessions[j].StartedAtMs })
	state.FuturesBars = len(future)
	state.SpotBars = len(spot)
	state.FuturesGapCount = countGaps(future)
	state.SpotGapCount = countGaps(spot)
	common := make([]int64, 0)
	for ts := range future {
		if _, ok := spot[ts]; ok {
			common = append(common, ts)
		}
	}
	sort.Slice(common, func(i, j int) bool { return common[i] < common[j] })
	var run, longest int64
	for i, ts := range common {
		if i == 0 || ts == common[i-1]+1000 {
			run += 1000
		} else {
			state.CommonGapCount++
			run = 1000
		}
		if run > longest {
			longest = run
		}
	}
	state.AvailableContiguousHistoryMs = longest
	state.MissingDurationMs = requiredWarmupMs - longest
	if state.MissingDurationMs < 0 {
		state.MissingDurationMs = 0
	}
	required := []string{"mark", "index", "premium", "funding", "metrics_oi", "metrics_global", "metrics_top_account", "metrics_top_position", "metrics_taker"}
	externalReady := true
	for _, name := range required {
		externalReady = externalReady && state.ExternalSourceCounts[name] > 0
	}
	state.WarmupReady = longest >= requiredWarmupMs && externalReady && state.ConflictingDuplicateBars == 0
	state.Complete = state.WarmupReady
	if state.WarmupReady {
		state.Status = "READY"
	}
	return state, nil
}

// bridgeNoTradeSeconds restores the canonical no-trade bars that a batch
// boundary can omit. Consecutive aggTrade IDs prove that no trade event was
// lost between the adjacent observed bars; all other gaps remain untouched.
func bridgeNoTradeSeconds(rows map[int64]market.SecondBar) {
	keys := make([]int64, 0, len(rows))
	for ts := range rows {
		keys = append(keys, ts)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	for i := 1; i < len(keys); i++ {
		previous, next := rows[keys[i-1]], rows[keys[i]]
		if next.TimestampMs <= previous.TimestampMs+1000 || previous.LastAggTradeID <= 0 || next.FirstAggTradeID != previous.LastAggTradeID+1 {
			continue
		}
		for ts := previous.TimestampMs + 1000; ts < next.TimestampMs; ts += 1000 {
			rows[ts] = market.SecondBar{TimestampMs: ts, Open: previous.Close, High: previous.Close, Low: previous.Close, Close: previous.Close, VWAP: previous.Close}
		}
	}
}
func countGaps(rows map[int64]market.SecondBar) int {
	keys := make([]int64, 0, len(rows))
	for ts := range rows {
		keys = append(keys, ts)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	n := 0
	for i := 1; i < len(keys); i++ {
		if keys[i] != keys[i-1]+1000 {
			n++
		}
	}
	return n
}
func loadWarmupState() (warmupState, error) {
	var s warmupState
	b, e := os.ReadFile(filepath.Join(warmupRoot(), "capture-state.json"))
	if e == nil {
		e = json.Unmarshal(b, &s)
	}
	return s, e
}

type warmupDataset struct {
	Futures, Spot []market.SecondBar
	External      []live.ExternalObservation
}

func loadWarmupDataset() (warmupDataset, error) {
	root := warmupRoot()
	entries, err := os.ReadDir(filepath.Join(root, "sessions"))
	if err != nil {
		return warmupDataset{}, err
	}
	futures, spot := map[int64]market.SecondBar{}, map[int64]market.SecondBar{}
	external := map[string]live.ExternalObservation{}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		var session warmupSession
		b, readErr := os.ReadFile(filepath.Join(root, "sessions", entry.Name()))
		if readErr != nil || json.Unmarshal(b, &session) != nil {
			return warmupDataset{}, fmt.Errorf("invalid session %s", entry.Name())
		}
		for _, row := range session.FuturesBars {
			if _, ok := futures[row.TimestampMs]; !ok {
				futures[row.TimestampMs] = row
			}
		}
		for _, row := range session.SpotBars {
			if _, ok := spot[row.TimestampMs]; !ok {
				spot[row.TimestampMs] = row
			}
		}
		for _, row := range session.External {
			key := fmt.Sprintf("%s|%d", row.Dataset, row.SourceTimestampMs)
			if old, ok := external[key]; !ok || row.ReceiveTimestampMs < old.ReceiveTimestampMs {
				external[key] = row
			}
		}
	}
	bridgeNoTradeSeconds(futures)
	bridgeNoTradeSeconds(spot)
	common := make([]int64, 0)
	for ts := range futures {
		if _, ok := spot[ts]; ok {
			common = append(common, ts)
		}
	}
	sort.Slice(common, func(i, j int) bool { return common[i] < common[j] })
	bestStart, bestEnd, runStart := 0, -1, 0
	for i := range common {
		if i > 0 && common[i] != common[i-1]+1000 {
			runStart = i
		}
		if bestEnd < bestStart || i-runStart > bestEnd-bestStart {
			bestStart, bestEnd = runStart, i
		}
	}
	result := warmupDataset{}
	if bestEnd >= bestStart {
		for _, ts := range common[bestStart : bestEnd+1] {
			result.Futures = append(result.Futures, futures[ts])
			result.Spot = append(result.Spot, spot[ts])
		}
	}
	for _, row := range external {
		result.External = append(result.External, row)
	}
	sort.Slice(result.External, func(i, j int) bool {
		if result.External[i].ReceiveTimestampMs == result.External[j].ReceiveTimestampMs {
			if result.External[i].Dataset == result.External[j].Dataset {
				return result.External[i].SourceTimestampMs < result.External[j].SourceTimestampMs
			}
			return result.External[i].Dataset < result.External[j].Dataset
		}
		return result.External[i].ReceiveTimestampMs < result.External[j].ReceiveTimestampMs
	})
	return result, nil
}
