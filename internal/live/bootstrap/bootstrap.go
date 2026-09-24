// Package bootstrap retrieves recent public Binance history strictly for
// seeding the frozen live feature engines. It never emits decisions or orders.
package bootstrap

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	live "binance_trader/internal/live/binance"
	"binance_trader/internal/market"
)

const (
	DefaultLookback      = 4*time.Hour + 30*time.Minute
	DefaultFundingWindow = 20 * time.Hour
	DefaultSafetyLag     = 5 * time.Second
	RequiredCoverageMs   = int64(4 * time.Hour / time.Millisecond)
	MaxRetries           = 5
)

type Options struct {
	Symbol             string
	Now                time.Time
	Lookback           time.Duration
	FundingWindow      time.Duration
	SafetyLag          time.Duration
	HTTP               *http.Client
	FuturesBase        string
	SpotBase           string
	FuturesRequestPace time.Duration
	SpotRequestPace    time.Duration
}

type SourceStat struct {
	Endpoint, Resolution              string
	Rows, Pages                       int
	FirstTimestampMs, LastTimestampMs int64
	FirstID, LastID                   int64
	FetchLatencyMs                    int64
	RequestWeight, MaxLimit           int
}

type Result struct {
	Symbol                                                          string
	StartedAtMs, FetchCompletedAtMs                                 int64
	BootstrapStartMs, BootstrapEndMs                                int64
	FuturesTrades, SpotTrades                                       []live.CaptureEvent
	FuturesBars, SpotBars                                           []market.SecondBar
	External                                                        []live.ExternalObservation
	Sources                                                         map[string]SourceStat
	Origin                                                          live.DataOrigin
	HistoricalSignals, HistoricalExecutionIntents, HistoricalOrders int64
}

func defaults(o Options) Options {
	if o.Symbol == "" {
		o.Symbol = "BTCUSDT"
	}
	if o.Lookback == 0 {
		o.Lookback = DefaultLookback
	}
	if o.FundingWindow == 0 {
		o.FundingWindow = DefaultFundingWindow
	}
	if o.SafetyLag == 0 {
		o.SafetyLag = DefaultSafetyLag
	}
	if o.HTTP == nil {
		o.HTTP = &http.Client{Timeout: 30 * time.Second}
	}
	if o.FuturesBase == "" {
		o.FuturesBase = "https://fapi.binance.com"
	}
	if o.SpotBase == "" {
		o.SpotBase = "https://api.binance.com"
	}
	if o.FuturesRequestPace == 0 {
		o.FuturesRequestPace = 500 * time.Millisecond
	}
	if o.SpotRequestPace == 0 {
		o.SpotRequestPace = 120 * time.Millisecond
	}
	return o
}

// Fetch downloads immutable public rows. All historical receive timestamps
// remain zero; FetchCompletedAtMs records only the real startup fetch boundary.
func Fetch(ctx context.Context, options Options) (Result, error) {
	o := defaults(options)
	if o.Now.IsZero() {
		var futuresClock, spotClock struct {
			ServerTime int64 `json:"serverTime"`
		}
		if err := getJSON(ctx, o.HTTP, o.FuturesBase+"/fapi/v1/time", nil, &futuresClock); err != nil {
			return Result{}, fmt.Errorf("futures server time: %w", err)
		}
		if err := getJSON(ctx, o.HTTP, o.SpotBase+"/api/v3/time", nil, &spotClock); err != nil {
			return Result{}, fmt.Errorf("spot server time: %w", err)
		}
		serverNow := min64(futuresClock.ServerTime, spotClock.ServerTime)
		if serverNow <= 0 {
			return Result{}, fmt.Errorf("invalid Binance server time")
		}
		o.Now = time.UnixMilli(serverNow).UTC()
	}
	end := o.Now.Add(-o.SafetyLag).UnixMilli()/1000*1000 - 1
	start := end - o.Lookback.Milliseconds() + 1
	r := Result{Symbol: o.Symbol, StartedAtMs: time.Now().UnixMilli(), BootstrapStartMs: start, BootstrapEndMs: end, Sources: map[string]SourceStat{}, Origin: live.OriginBootstrapHistory}
	type tradeResult struct {
		name string
		rows []live.CaptureEvent
		stat SourceStat
		err  error
	}
	tradeCh := make(chan tradeResult, 2)
	go func() {
		rows, stat, err := fetchAggTrades(ctx, o.HTTP, o.FuturesBase, "/fapi/v1/aggTrades", o.Symbol, start, end, "futures_aggTrade", 20, o.FuturesRequestPace)
		tradeCh <- tradeResult{"futures_aggTrades", rows, stat, err}
	}()
	go func() {
		rows, stat, err := fetchAggTrades(ctx, o.HTTP, o.SpotBase, "/api/v3/aggTrades", o.Symbol, start, end, "spot_aggTrade", 4, o.SpotRequestPace)
		tradeCh <- tradeResult{"spot_aggTrades", rows, stat, err}
	}()
	for range 2 {
		x := <-tradeCh
		if x.err != nil {
			return r, fmt.Errorf("%s: %w", x.name, x.err)
		}
		r.Sources[x.name] = x.stat
		if x.name == "futures_aggTrades" {
			r.FuturesTrades = x.rows
		} else {
			r.SpotTrades = x.rows
		}
	}
	ext, stats, err := fetchExternal(ctx, o, start, end)
	if err != nil {
		return r, err
	}
	for k, v := range stats {
		r.Sources[k] = v
	}
	r.FetchCompletedAtMs = time.Now().UnixMilli()
	for i := range ext {
		ext[i].FetchCompletedAtMs = r.FetchCompletedAtMs
		ext[i].Origin = live.OriginBootstrapHistory
	}
	r.External = ext
	r.FuturesBars, err = live.CanonicalBars(r.FuturesTrades, "futures_aggTrade")
	if err != nil {
		return r, err
	}
	r.SpotBars, err = live.CanonicalBars(r.SpotTrades, "spot_aggTrade")
	if err != nil {
		return r, err
	}
	if err = alignBars(&r.FuturesBars, &r.SpotBars, end/1000*1000); err != nil {
		return r, err
	}
	if err = r.Validate(); err != nil {
		return r, err
	}
	return r, nil
}

func fetchAggTrades(ctx context.Context, client *http.Client, base, path, symbol string, start, end int64, source string, weight int, pace time.Duration) ([]live.CaptureEvent, SourceStat, error) {
	stat := SourceStat{Endpoint: path, Resolution: "event", RequestWeight: weight, MaxLimit: 1000}
	begin := time.Now()
	q := url.Values{"symbol": {symbol}, "startTime": {strconv.FormatInt(start, 10)}, "endTime": {strconv.FormatInt(min64(end, start+time.Hour.Milliseconds()-1), 10)}, "limit": {"1000"}}
	var out []live.CaptureEvent
	seen := map[int64]struct{}{}
	for {
		var raw []json.RawMessage
		if err := getJSON(ctx, client, base+path, q, &raw); err != nil {
			return nil, stat, err
		}
		stat.Pages++
		if len(raw) == 0 {
			break
		}
		stop := false
		for _, body := range raw {
			e, err := live.ParsePublicAggTrade(body, source, 0)
			if err != nil {
				return nil, stat, err
			}
			e.Origin = live.OriginBootstrapHistory
			if e.SourceTimestampMs > end {
				stop = true
				break
			}
			if e.SourceTimestampMs < start {
				continue
			}
			if _, ok := seen[e.ID]; ok {
				continue
			}
			seen[e.ID] = struct{}{}
			out = append(out, e)
		}
		lastID := mustAggID(raw[len(raw)-1])
		if stop || len(raw) < 1000 || lastID <= 0 {
			break
		}
		q = url.Values{"symbol": {symbol}, "fromId": {strconv.FormatInt(lastID+1, 10)}, "limit": {"1000"}}
		if pace > 0 {
			select {
			case <-ctx.Done():
				return nil, stat, ctx.Err()
			case <-time.After(pace):
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	for i, e := range out {
		if i > 0 && (e.ID != out[i-1].ID+1 || e.SourceTimestampMs < out[i-1].SourceTimestampMs) {
			return nil, stat, fmt.Errorf("aggregate trade continuity failure at ID %d", e.ID)
		}
	}
	if len(out) == 0 {
		return nil, stat, fmt.Errorf("no aggregate trades in bootstrap range")
	}
	stat.Rows = len(out)
	stat.FirstTimestampMs = out[0].SourceTimestampMs
	stat.LastTimestampMs = out[len(out)-1].SourceTimestampMs
	stat.FirstID = out[0].ID
	stat.LastID = out[len(out)-1].ID
	stat.FetchLatencyMs = time.Since(begin).Milliseconds()
	return out, stat, nil
}

// FetchIDRange resolves a bounded bootstrap/live race using authoritative
// public REST IDs. Any remaining discontinuity fails closed.
func FetchIDRange(ctx context.Context, source string, fromID, toID int64) ([]live.CaptureEvent, error) {
	if fromID <= 0 || toID < fromID || toID-fromID+1 > 10_000 {
		return nil, fmt.Errorf("invalid handoff ID range")
	}
	base, path := "https://fapi.binance.com", "/fapi/v1/aggTrades"
	if source == "spot_aggTrade" {
		base, path = "https://api.binance.com", "/api/v3/aggTrades"
	} else if source != "futures_aggTrade" {
		return nil, fmt.Errorf("unknown handoff source")
	}
	client := &http.Client{Timeout: 30 * time.Second}
	out := make([]live.CaptureEvent, 0, toID-fromID+1)
	for next := fromID; next <= toID; {
		limit := int(toID - next + 1)
		if limit > 1000 {
			limit = 1000
		}
		q := url.Values{"symbol": {"BTCUSDT"}, "fromId": {strconv.FormatInt(next, 10)}, "limit": {strconv.Itoa(limit)}}
		var raw []json.RawMessage
		if err := getJSON(ctx, client, base+path, q, &raw); err != nil {
			return nil, err
		}
		if len(raw) == 0 {
			return nil, fmt.Errorf("handoff ID range unavailable")
		}
		before := next
		for _, body := range raw {
			e, err := live.ParsePublicAggTrade(body, source, 0)
			if err != nil {
				return nil, err
			}
			if e.ID < next {
				continue
			}
			if e.ID > toID {
				break
			}
			if e.ID != next {
				return nil, fmt.Errorf("handoff gap unresolved: expected=%d actual=%d", next, e.ID)
			}
			e.Origin = live.OriginBootstrapHistory
			e.FetchCompletedAtMs = time.Now().UnixMilli()
			out = append(out, e)
			next++
		}
		if next == before {
			return nil, fmt.Errorf("handoff ID range made no progress")
		}
	}
	return out, nil
}

func mustAggID(raw json.RawMessage) int64 {
	var x struct {
		ID json.RawMessage `json:"a"`
	}
	if json.Unmarshal(raw, &x) != nil {
		return 0
	}
	v, _ := strconv.ParseInt(strings.Trim(string(x.ID), `"`), 10, 64)
	return v
}

func fetchExternal(ctx context.Context, o Options, start, end int64) ([]live.ExternalObservation, map[string]SourceStat, error) {
	type spec struct {
		name, path, param, res string
		funding                bool
	}
	specs := []spec{
		{"mark", "/fapi/v1/markPriceKlines", "symbol", "1m", false}, {"index", "/fapi/v1/indexPriceKlines", "pair", "1m", false}, {"premium", "/fapi/v1/premiumIndexKlines", "symbol", "1m", false},
		{"metrics_oi", "/futures/data/openInterestHist", "symbol", "5m", false}, {"metrics_global", "/futures/data/globalLongShortAccountRatio", "symbol", "5m", false}, {"metrics_top_account", "/futures/data/topLongShortAccountRatio", "symbol", "5m", false}, {"metrics_top_position", "/futures/data/topLongShortPositionRatio", "symbol", "5m", false}, {"metrics_taker", "/futures/data/takerlongshortRatio", "symbol", "5m", false},
		{"funding", "/fapi/v1/fundingRate", "symbol", "event", true},
	}
	var out []live.ExternalObservation
	stats := map[string]SourceStat{}
	for _, s := range specs {
		from := start
		limit := 500
		if s.funding {
			from = end - o.FundingWindow.Milliseconds() + 1
			limit = 1000
		}
		if s.res == "1m" {
			limit = 1000
		}
		q := url.Values{s.param: {o.Symbol}, "startTime": {strconv.FormatInt(from, 10)}, "endTime": {strconv.FormatInt(end, 10)}, "limit": {strconv.Itoa(limit)}}
		if s.res == "1m" || s.res == "5m" {
			q.Set("interval", s.res)
			if s.res == "5m" {
				q.Del("interval")
				q.Set("period", "5m")
			}
		}
		begin := time.Now()
		var rows []json.RawMessage
		if err := getJSON(ctx, o.HTTP, o.FuturesBase+s.path, q, &rows); err != nil {
			return nil, stats, fmt.Errorf("%s: %w", s.name, err)
		}
		st := SourceStat{Endpoint: s.path, Resolution: s.res, Rows: len(rows), Pages: 1, MaxLimit: limit, FetchLatencyMs: time.Since(begin).Milliseconds()}
		for _, raw := range rows {
			var ts, close int64
			if s.res == "1m" {
				var tuple []json.RawMessage
				if json.Unmarshal(raw, &tuple) != nil || len(tuple) < 7 {
					return nil, stats, fmt.Errorf("%s invalid kline", s.name)
				}
				_ = json.Unmarshal(tuple[0], &ts)
				_ = json.Unmarshal(tuple[6], &close)
			} else {
				var obj map[string]json.RawMessage
				if json.Unmarshal(raw, &obj) != nil {
					return nil, stats, fmt.Errorf("%s invalid object", s.name)
				}
				key := "timestamp"
				if s.funding {
					key = "fundingTime"
				}
				_ = json.Unmarshal(obj[key], &ts)
			}
			if ts <= 0 || ts > end {
				continue
			}
			source := s.name
			if strings.HasPrefix(s.name, "metrics_") {
				source = "metrics"
			}
			out = append(out, live.ExternalObservation{Source: source, Dataset: s.name, SourceTimestampMs: ts, CloseTimeMs: close, Raw: append(json.RawMessage(nil), raw...)})
			if st.FirstTimestampMs == 0 || ts < st.FirstTimestampMs {
				st.FirstTimestampMs = ts
			}
			if ts > st.LastTimestampMs {
				st.LastTimestampMs = ts
			}
		}
		st.Rows = 0
		for _, x := range out {
			if x.Dataset == s.name {
				st.Rows++
			}
		}
		if st.Rows == 0 {
			return nil, stats, fmt.Errorf("%s returned no usable rows", s.name)
		}
		stats[s.name] = st
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SourceTimestampMs == out[j].SourceTimestampMs {
			return out[i].Dataset < out[j].Dataset
		}
		return out[i].SourceTimestampMs < out[j].SourceTimestampMs
	})
	return out, stats, nil
}

func alignBars(a, b *[]market.SecondBar, targetEnd int64) error {
	if len(*a) == 0 || len(*b) == 0 {
		return fmt.Errorf("empty canonical stream")
	}
	start := max64((*a)[0].TimestampMs, (*b)[0].TimestampMs)
	if (*a)[len(*a)-1].TimestampMs > targetEnd || (*b)[len(*b)-1].TimestampMs > targetEnd {
		return fmt.Errorf("canonical stream exceeds bootstrap boundary")
	}
	trim := func(rows []market.SecondBar) []market.SecondBar {
		i := sort.Search(len(rows), func(i int) bool { return rows[i].TimestampMs >= start })
		rows = append([]market.SecondBar(nil), rows[i:]...)
		for ts := rows[len(rows)-1].TimestampMs + 1000; ts <= targetEnd; ts += 1000 {
			close := rows[len(rows)-1].Close
			rows = append(rows, market.SecondBar{TimestampMs: ts, Open: close, High: close, Low: close, Close: close, VWAP: close})
		}
		return rows
	}
	*a = trim(*a)
	*b = trim(*b)
	if len(*a) != len(*b) || len(*a) == 0 {
		return fmt.Errorf("canonical alignment failure")
	}
	for i := range *a {
		if (*a)[i].TimestampMs != (*b)[i].TimestampMs {
			return fmt.Errorf("canonical timestamp mismatch")
		}
	}
	return nil
}

func (r Result) Validate() error {
	if r.Origin != live.OriginBootstrapHistory || r.FetchCompletedAtMs <= 0 {
		return fmt.Errorf("BOOTSTRAP_INCOMPLETE")
	}
	if len(r.FuturesBars) == 0 || len(r.FuturesBars) != len(r.SpotBars) {
		return fmt.Errorf("BOOTSTRAP_INCOMPLETE")
	}
	coverage := r.FuturesBars[len(r.FuturesBars)-1].TimestampMs - r.FuturesBars[0].TimestampMs
	if coverage < RequiredCoverageMs {
		return fmt.Errorf("BOOTSTRAP_INCOMPLETE: canonical coverage=%d", coverage)
	}
	for i := range r.FuturesBars {
		if i > 0 && (r.FuturesBars[i].TimestampMs != r.FuturesBars[i-1].TimestampMs+1000 || r.SpotBars[i].TimestampMs != r.SpotBars[i-1].TimestampMs+1000) {
			return fmt.Errorf("BOOTSTRAP_HANDOFF_GAP")
		}
	}
	for _, x := range r.External {
		if x.Origin != live.OriginBootstrapHistory || x.ReceiveTimestampMs != 0 || x.FetchCompletedAtMs != r.FetchCompletedAtMs || x.SourceTimestampMs > r.BootstrapEndMs {
			return fmt.Errorf("bootstrap origin/availability violation")
		}
	}
	return nil
}

func getJSON(ctx context.Context, client *http.Client, endpoint string, q url.Values, out any) error {
	var last error
	for attempt := 0; attempt < MaxRetries; attempt++ {
		u := endpoint + "?" + q.Encode()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err != nil {
			last = err
		} else {
			body, readErr := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
			_ = resp.Body.Close()
			if readErr != nil {
				return readErr
			}
			if resp.StatusCode/100 == 2 {
				return json.Unmarshal(body, out)
			}
			last = fmt.Errorf("Binance HTTP %d", resp.StatusCode)
			if resp.StatusCode != 429 && resp.StatusCode != 418 && resp.StatusCode >= 400 && resp.StatusCode < 500 {
				return last
			}
			if retry := resp.Header.Get("Retry-After"); retry != "" {
				if seconds, e := strconv.Atoi(retry); e == nil {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(time.Duration(seconds) * time.Second):
					}
					continue
				}
			}
		}
		delay := time.Duration(1<<attempt) * time.Second
		if delay > 15*time.Second {
			delay = 15 * time.Second
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	return last
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

var _ = finite
