package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"binance_trader/internal/data/secondbar"
	"binance_trader/internal/market"
	"github.com/gorilla/websocket"
)

const (
	FuturesPublicWebSocket = "wss://fstream.binance.com/market/ws/btcusdt@aggTrade"
	MarkPublicWebSocket    = "wss://fstream.binance.com/market/ws/btcusdt@markPrice@1s"
	SpotPublicWebSocket    = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"
)

type CaptureEvent struct {
	Source, EventType                         string
	SourceTimestampMs, ReceiveTimestampMs, ID int64
	FetchCompletedAtMs                        int64      `json:"fetch_completed_at_ms,omitempty"`
	Origin                                    DataOrigin `json:"origin,omitempty"`
	Price, Quantity                           float64
	BuyerMaker                                bool
	Raw                                       json.RawMessage `json:"raw,omitempty"`
}

type SourceHealth struct {
	Source                                                               string
	Connected                                                            bool
	Events, ReconnectCount, ParseErrors, DuplicateEvents, SequenceErrors int64
	LastSourceTimestampMs, LastReceiveTimestampMs                        int64
	LastError                                                            string
}

type CaptureResult struct {
	StartedAtMs, EndedAtMs int64
	Events                 []CaptureEvent
	Health                 map[string]*SourceHealth
}

type ExternalObservation struct {
	Source, Dataset                string
	SourceTimestampMs, CloseTimeMs int64
	ReceiveTimestampMs             int64
	FetchCompletedAtMs             int64      `json:"fetch_completed_at_ms,omitempty"`
	Origin                         DataOrigin `json:"origin,omitempty"`
	Raw                            json.RawMessage
}

type DataOrigin string

const (
	OriginBootstrapHistory DataOrigin = "BOOTSTRAP_HISTORY"
	OriginLiveReceived     DataOrigin = "LIVE_RECEIVED"
)

func FetchPublicObservations(ctx context.Context) ([]ExternalObservation, error) {
	urls := map[string]string{
		"mark":                 "https://fapi.binance.com/fapi/v1/markPriceKlines?symbol=BTCUSDT&interval=1m&limit=2",
		"index":                "https://fapi.binance.com/fapi/v1/indexPriceKlines?pair=BTCUSDT&interval=1m&limit=2",
		"premium":              "https://fapi.binance.com/fapi/v1/premiumIndexKlines?symbol=BTCUSDT&interval=1m&limit=2",
		"funding":              "https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT&limit=2",
		"metrics_oi":           "https://fapi.binance.com/futures/data/openInterestHist?symbol=BTCUSDT&period=5m&limit=2",
		"metrics_global":       "https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol=BTCUSDT&period=5m&limit=2",
		"metrics_top_account":  "https://fapi.binance.com/futures/data/topLongShortAccountRatio?symbol=BTCUSDT&period=5m&limit=2",
		"metrics_top_position": "https://fapi.binance.com/futures/data/topLongShortPositionRatio?symbol=BTCUSDT&period=5m&limit=2",
		"metrics_taker":        "https://fapi.binance.com/futures/data/takerlongshortRatio?symbol=BTCUSDT&period=5m&limit=2",
	}
	client := &http.Client{Timeout: 12 * time.Second}
	out := make([]ExternalObservation, 0, 18)
	for name, u := range urls {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		b, readErr := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
		_ = resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode/100 != 2 {
			return nil, fmt.Errorf("%s HTTP %d", name, resp.StatusCode)
		}
		recv := time.Now().UnixMilli()
		var rows []json.RawMessage
		if err = json.Unmarshal(b, &rows); err != nil {
			return nil, fmt.Errorf("%s JSON: %w", name, err)
		}
		for _, raw := range rows {
			var ts, close int64
			if name == "mark" || name == "index" || name == "premium" {
				var tuple []json.RawMessage
				if json.Unmarshal(raw, &tuple) != nil || len(tuple) < 7 {
					return nil, fmt.Errorf("%s kline tuple", name)
				}
				_ = json.Unmarshal(tuple[0], &ts)
				_ = json.Unmarshal(tuple[6], &close)
			} else {
				var obj map[string]json.RawMessage
				if json.Unmarshal(raw, &obj) != nil {
					return nil, fmt.Errorf("%s object", name)
				}
				key := "timestamp"
				if name == "funding" {
					key = "fundingTime"
				}
				_ = json.Unmarshal(obj[key], &ts)
			}
			if ts <= 0 {
				return nil, fmt.Errorf("%s timestamp", name)
			}
			source := name
			if strings.HasPrefix(name, "metrics_") {
				source = "metrics"
			}
			out = append(out, ExternalObservation{Source: source, Dataset: name, SourceTimestampMs: ts, CloseTimeMs: close, ReceiveTimestampMs: recv, Origin: OriginLiveReceived, Raw: append(json.RawMessage(nil), raw...)})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ReceiveTimestampMs == out[j].ReceiveTimestampMs {
			return out[i].Dataset < out[j].Dataset
		}
		return out[i].ReceiveTimestampMs < out[j].ReceiveTimestampMs
	})
	return out, nil
}

type combinedMessage struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}
type aggMessage struct {
	ID         int64  `json:"a"`
	Price      string `json:"p"`
	Quantity   string `json:"q"`
	Time       int64  `json:"T"`
	BuyerMaker bool   `json:"m"`
}
type markMessage struct {
	Event       string `json:"e"`
	Time        int64  `json:"E"`
	Mark        string `json:"p"`
	Index       string `json:"i"`
	Funding     string `json:"r"`
	NextFunding int64  `json:"T"`
}

func CapturePublic(ctx context.Context, duration time.Duration) (CaptureResult, error) {
	return capturePublic(ctx, duration, 0, nil)
}

// CapturePublicBatched keeps one set of websocket connections alive while
// publishing bounded event batches. The callback is serialized and receives
// cumulative source health plus only the events since the previous callback.
func CapturePublicBatched(ctx context.Context, duration, interval time.Duration, publish func(CaptureResult) error) error {
	if interval <= 0 || publish == nil {
		return fmt.Errorf("invalid batch capture configuration")
	}
	_, err := capturePublic(ctx, duration, interval, publish)
	return err
}

func capturePublic(ctx context.Context, duration, batchInterval time.Duration, publish func(CaptureResult) error) (CaptureResult, error) {
	var cancel context.CancelFunc
	if duration > 0 {
		ctx, cancel = context.WithTimeout(ctx, duration)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()
	r := CaptureResult{StartedAtMs: time.Now().UnixMilli(), Health: map[string]*SourceHealth{"futures_aggTrade": {Source: "futures_aggTrade"}, "spot_aggTrade": {Source: "spot_aggTrade"}, "mark_price": {Source: "mark_price"}}}
	var mu sync.Mutex
	var wg sync.WaitGroup
	worker := func(name, endpoint string, combined bool) {
		defer wg.Done()
		backoff := time.Second
		for ctx.Err() == nil {
			c, _, err := websocket.DefaultDialer.DialContext(ctx, endpoint, nil)
			if err != nil {
				mu.Lock()
				r.Health[name].ReconnectCount++
				r.Health[name].LastError = err.Error()
				mu.Unlock()
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
				if backoff < 8*time.Second {
					backoff *= 2
				}
				continue
			}
			mu.Lock()
			r.Health[name].Connected = true
			mu.Unlock()
			backoff = time.Second
			for ctx.Err() == nil {
				_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
				_, b, err := c.ReadMessage()
				if err != nil {
					_ = c.Close()
					mu.Lock()
					r.Health[name].Connected = false
					if ctx.Err() == nil {
						r.Health[name].ReconnectCount++
						r.Health[name].LastError = err.Error()
					}
					mu.Unlock()
					if ctx.Err() != nil {
						return
					}
					break
				}
				recv := time.Now().UnixMilli()
				raw := json.RawMessage(b)
				stream := name
				if combined {
					var wrap combinedMessage
					if json.Unmarshal(b, &wrap) != nil {
						mu.Lock()
						r.Health[name].ParseErrors++
						mu.Unlock()
						continue
					}
					raw = wrap.Data
					stream = wrap.Stream
				}
				var event CaptureEvent
				if name == "spot_aggTrade" || name == "futures_aggTrade" || stream == "btcusdt@aggTrade" {
					source := name
					if stream == "btcusdt@aggTrade" {
						source = "futures_aggTrade"
					}
					event, err = ParsePublicAggTrade(raw, source, recv)
					if err != nil {
						mu.Lock()
						r.Health[name].ParseErrors++
						r.Health[name].LastError = err.Error()
						mu.Unlock()
						continue
					}
				} else {
					var m markMessage
					if err = json.Unmarshal(raw, &m); err != nil {
						mu.Lock()
						r.Health[name].ParseErrors++
						mu.Unlock()
						continue
					}
					p, pe := strconv.ParseFloat(m.Mark, 64)
					if pe != nil {
						mu.Lock()
						r.Health[name].ParseErrors++
						mu.Unlock()
						continue
					}
					event = CaptureEvent{Source: "mark_price", EventType: "markPrice", SourceTimestampMs: m.Time, ReceiveTimestampMs: recv, Origin: OriginLiveReceived, Price: p, Raw: append(json.RawMessage(nil), raw...)}
				}
				mu.Lock()
				h := r.Health[event.Source]
				if h.LastSourceTimestampMs > event.SourceTimestampMs {
					h.SequenceErrors++
				}
				if event.ID != 0 && len(r.Events) > 0 {
					for i := len(r.Events) - 1; i >= 0 && i >= len(r.Events)-20; i-- {
						if r.Events[i].Source == event.Source && r.Events[i].ID == event.ID {
							h.DuplicateEvents++
							event.ID = 0
							break
						}
					}
				}
				if event.ID != 0 || event.EventType == "markPrice" {
					r.Events = append(r.Events, event)
					h.Events++
					h.LastSourceTimestampMs = event.SourceTimestampMs
					h.LastReceiveTimestampMs = recv
				}
				mu.Unlock()
			}
		}
	}
	wg.Add(3)
	go worker("futures_aggTrade", FuturesPublicWebSocket, false)
	go worker("mark_price", MarkPublicWebSocket, false)
	go worker("spot_aggTrade", SpotPublicWebSocket, false)
	if publish != nil {
		publishedEvents := 0
		cloneHealth := func() map[string]*SourceHealth {
			out := make(map[string]*SourceHealth, len(r.Health))
			for name, health := range r.Health {
				copy := *health
				out[name] = &copy
			}
			return out
		}
		flush := func() error {
			mu.Lock()
			batch := CaptureResult{StartedAtMs: r.StartedAtMs, EndedAtMs: time.Now().UnixMilli(), Events: r.Events, Health: cloneHealth()}
			r.Events = nil
			r.StartedAtMs = batch.EndedAtMs
			mu.Unlock()
			if len(batch.Events) == 0 {
				return nil
			}
			if err := publish(batch); err != nil {
				return err
			}
			publishedEvents += len(batch.Events)
			return nil
		}
		ticker := time.NewTicker(batchInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := flush(); err != nil {
					cancel()
					wg.Wait()
					return r, err
				}
			case <-ctx.Done():
				wg.Wait()
				if err := flush(); err != nil {
					return r, err
				}
				r.EndedAtMs = time.Now().UnixMilli()
				if publishedEvents == 0 {
					return r, fmt.Errorf("no live events captured: futures=%s spot=%s mark=%s", r.Health["futures_aggTrade"].LastError, r.Health["spot_aggTrade"].LastError, r.Health["mark_price"].LastError)
				}
				return r, nil
			}
		}
	}
	wg.Wait()
	r.EndedAtMs = time.Now().UnixMilli()
	if len(r.Events) == 0 {
		return r, fmt.Errorf("no live events captured")
	}
	return r, nil
}

func CanonicalBars(events []CaptureEvent, source string) ([]market.SecondBar, error) {
	rows := make([]CaptureEvent, 0)
	for _, e := range events {
		if e.Source == source && e.EventType == "aggTrade" && e.ID != 0 {
			rows = append(rows, e)
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].SourceTimestampMs == rows[j].SourceTimestampMs {
			return rows[i].ID < rows[j].ID
		}
		return rows[i].SourceTimestampMs < rows[j].SourceTimestampMs
	})
	bars := make([]market.SecondBar, 0)
	a := secondbar.NewAggregator(func(b market.SecondBar) error { bars = append(bars, b); return nil })
	for _, e := range rows {
		if err := a.Add(market.AggTrade{AggTradeID: e.ID, Price: e.Price, Quantity: e.Quantity, TradeTimeMs: e.SourceTimestampMs, BuyerMaker: e.BuyerMaker}); err != nil {
			return nil, err
		}
	}
	if err := a.Flush(); err != nil {
		return nil, err
	}
	return bars, nil
}

func ProbePublicSources(ctx context.Context) (map[string]int64, error) {
	urls := map[string]string{"mark_index_funding": "https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT", "open_interest": "https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT", "metrics_oi": "https://fapi.binance.com/futures/data/openInterestHist?symbol=BTCUSDT&period=5m&limit=2", "metrics_global": "https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol=BTCUSDT&period=5m&limit=2", "metrics_top_account": "https://fapi.binance.com/futures/data/topLongShortAccountRatio?symbol=BTCUSDT&period=5m&limit=2", "metrics_top_position": "https://fapi.binance.com/futures/data/topLongShortPositionRatio?symbol=BTCUSDT&period=5m&limit=2", "metrics_taker": "https://fapi.binance.com/futures/data/takerlongshortRatio?symbol=BTCUSDT&period=5m&limit=2", "mark_kline": "https://fapi.binance.com/fapi/v1/markPriceKlines?symbol=BTCUSDT&interval=1m&limit=2", "index_kline": "https://fapi.binance.com/fapi/v1/indexPriceKlines?pair=BTCUSDT&interval=1m&limit=2", "premium_kline": "https://fapi.binance.com/fapi/v1/premiumIndexKlines?symbol=BTCUSDT&interval=1m&limit=2"}
	out := map[string]int64{}
	client := &http.Client{Timeout: 12 * time.Second}
	for name, u := range urls {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		before := time.Now()
		resp, err := client.Do(req)
		if err != nil {
			return out, fmt.Errorf("%s: %w", name, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			return out, fmt.Errorf("%s HTTP %d", name, resp.StatusCode)
		}
		out[name] = time.Since(before).Milliseconds()
	}
	return out, nil
}
