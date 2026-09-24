package uiapi

import (
	"context"
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	live "binance_trader/internal/live/binance"
	"github.com/gorilla/websocket"
)

func (s *Server) RunPublicMarketFeed(ctx context.Context) {
	if s.liveFeatureRuntime {
		go s.runSharedLiveRuntime(ctx)
		return
	}
	var featureEvents chan liveFeatureEvent
	if s.liveFeatureRuntime {
		featureEvents = make(chan liveFeatureEvent, 8192)
		go s.runLiveFeature(ctx, featureEvents)
		go s.pollLiveExternal(ctx, featureEvents)
	}
	sendFeature := func(event liveFeatureEvent) {
		if featureEvents == nil {
			return
		}
		select {
		case featureEvents <- event:
		case <-ctx.Done():
		}
	}
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.broadcast(map[string]any{"type": "warmup", "warmup": s.warmup()})
			}
		}
	}()
	var feedMu sync.Mutex
	current := Market{}
	update := func(kind string, price, index float64, sourceMs int64) {
		feedMu.Lock()
		switch kind {
		case "futures":
			current.FuturesPrice = price
		case "spot":
			current.SpotPrice = price
		case "mark":
			current.MarkPrice, current.IndexPrice = price, index
			if index != 0 {
				current.Premium = price/index - 1
			}
		}
		current.UpdatedAt = time.Now().UTC()
		current.Connected = current.FuturesPrice > 0 && current.SpotPrice > 0 && current.MarkPrice > 0
		value := current
		feedMu.Unlock()
		s.setMarketSource(kind, value, sourceMs)
	}
	lifecycle := func(kind string) func(bool, bool) {
		return func(connected, reconnect bool) {
			s.mu.Lock()
			if connected {
				if reconnect {
					if kind == "futures" {
						s.futuresReconnects++
					} else if kind == "spot" {
						s.spotReconnects++
					}
				}
				s.logLocked(kind + " WS connected")
			} else {
				s.logLocked(kind + " WS disconnected")
			}
			s.mu.Unlock()
			if kind == "futures" || kind == "spot" {
				sendFeature(liveFeatureEvent{source: kind, connected: connected})
			}
		}
	}
	recordRaw := func(kind string, parsed bool) {
		s.mu.Lock()
		switch kind {
		case "futures":
			s.futuresRawMessages++
		case "spot":
			s.spotRawMessages++
		case "mark":
			s.markRawMessages++
		}
		if !parsed {
			s.publicParseErrors++
		}
		s.mu.Unlock()
	}
	go priceWorker(ctx, live.FuturesPublicWebSocket, func(raw json.RawMessage) {
		var row struct {
			Price     string          `json:"p"`
			EventTime json.RawMessage `json:"E"`
		}
		parseErr := json.Unmarshal(raw, &row)
		value, numberErr := strconv.ParseFloat(row.Price, 64)
		stamp, stampErr := parsePublicMillis(row.EventTime)
		trade, tradeErr := live.ParsePublicAggTrade(raw, "futures_aggTrade", time.Now().UnixMilli())
		valid := parseErr == nil && numberErr == nil && stampErr == nil && tradeErr == nil && stamp > 0 && value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
		recordRaw("futures", valid)
		if valid {
			update("futures", value, 0, stamp)
			sendFeature(liveFeatureEvent{trade: &trade})
		}
	}, lifecycle("futures"))
	go priceWorker(ctx, live.SpotPublicWebSocket, func(raw json.RawMessage) {
		var row struct {
			Price     string          `json:"p"`
			EventTime json.RawMessage `json:"E"`
		}
		parseErr := json.Unmarshal(raw, &row)
		value, numberErr := strconv.ParseFloat(row.Price, 64)
		stamp, stampErr := parsePublicMillis(row.EventTime)
		trade, tradeErr := live.ParsePublicAggTrade(raw, "spot_aggTrade", time.Now().UnixMilli())
		valid := parseErr == nil && numberErr == nil && stampErr == nil && tradeErr == nil && stamp > 0 && value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
		recordRaw("spot", valid)
		if valid {
			update("spot", value, 0, stamp)
			sendFeature(liveFeatureEvent{trade: &trade})
		}
	}, lifecycle("spot"))
	go priceWorker(ctx, live.MarkPublicWebSocket, func(raw json.RawMessage) {
		var row struct {
			Mark      string          `json:"p"`
			Index     string          `json:"i"`
			EventTime json.RawMessage `json:"E"`
		}
		parseErr := json.Unmarshal(raw, &row)
		mark, a := strconv.ParseFloat(row.Mark, 64)
		index, b := strconv.ParseFloat(row.Index, 64)
		stamp, stampErr := parsePublicMillis(row.EventTime)
		valid := parseErr == nil && a == nil && b == nil && stampErr == nil && stamp > 0 && mark > 0 && index > 0 && !math.IsNaN(mark) && !math.IsNaN(index) && !math.IsInf(mark, 0) && !math.IsInf(index, 0)
		recordRaw("mark", valid)
		if valid {
			update("mark", mark, index, stamp)
		}
	}, lifecycle("mark"))
}

func parsePublicMillis(raw json.RawMessage) (int64, error) {
	value := strings.Trim(string(raw), `"`)
	return strconv.ParseInt(value, 10, 64)
}

func priceWorker(ctx context.Context, endpoint string, consume func(json.RawMessage), state func(bool, bool)) {
	backoff := time.Second
	connectedBefore := false
	for ctx.Err() == nil {
		conn, _, err := websocket.DefaultDialer.DialContext(ctx, endpoint, nil)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 15*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second
		state(true, connectedBefore)
		connectedBefore = true
		for ctx.Err() == nil {
			_ = conn.SetReadDeadline(time.Now().Add(15 * time.Second))
			_, body, readErr := conn.ReadMessage()
			if readErr != nil {
				_ = conn.Close()
				state(false, false)
				break
			}
			consume(json.RawMessage(body))
		}
	}
}
