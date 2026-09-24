package bootstrap

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

func TestFetchPaginationDedupAndOrigins(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	end := now.Add(-DefaultSafetyLag).UnixMilli() - 1
	start := end - DefaultLookback.Milliseconds() + 1
	firstTrade := (start/1000 + 1) * 1000
	type agg struct {
		A int64  `json:"a"`
		P string `json:"p"`
		Q string `json:"q"`
		T int64  `json:"T"`
		M bool   `json:"m"`
	}
	var trades []agg
	for ts, id := firstTrade, int64(10_000); ts <= end; ts, id = ts+10_000, id+1 {
		trades = append(trades, agg{id, "60000", "0.01", ts, id%2 == 0})
	}
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/fapi/v1/time" || r.URL.Path == "/api/v3/time" {
			_ = json.NewEncoder(w).Encode(map[string]int64{"serverTime": now.UnixMilli()})
			return
		}
		if r.URL.Path == "/fapi/v1/aggTrades" || r.URL.Path == "/api/v3/aggTrades" {
			at := 0
			if raw := r.URL.Query().Get("fromId"); raw != "" {
				id, _ := strconv.ParseInt(raw, 10, 64)
				at = int(id - 10_000)
				if at > 0 {
					at--
				}
			} else if raw := r.URL.Query().Get("startTime"); raw != "" {
				wanted, _ := strconv.ParseInt(raw, 10, 64)
				for at < len(trades) && trades[at].T < wanted {
					at++
				}
			}
			to := at + 1000
			if to > len(trades) {
				to = len(trades)
			}
			_ = json.NewEncoder(w).Encode(trades[at:to])
			return
		}
		q := r.URL.Query()
		from, _ := strconv.ParseInt(q.Get("startTime"), 10, 64)
		until, _ := strconv.ParseInt(q.Get("endTime"), 10, 64)
		if r.URL.Path == "/fapi/v1/fundingRate" {
			_ = json.NewEncoder(w).Encode([]map[string]any{{"fundingTime": until - 8*60*60*1000, "fundingRate": "0.0001"}, {"fundingTime": until, "fundingRate": "-0.0001"}})
			return
		}
		if r.URL.Path == "/fapi/v1/markPriceKlines" || r.URL.Path == "/fapi/v1/indexPriceKlines" || r.URL.Path == "/fapi/v1/premiumIndexKlines" {
			var rows [][]any
			for ts := (from / 60000) * 60000; ts <= until; ts += 60000 {
				rows = append(rows, []any{ts, "1", "1", "1", "1", "0", ts + 59999})
			}
			_ = json.NewEncoder(w).Encode(rows)
			return
		}
		var rows []map[string]any
		for ts := (from / 300000) * 300000; ts <= until; ts += 300000 {
			row := map[string]any{"timestamp": ts, "longShortRatio": "1.1", "buySellRatio": "1.2", "sumOpenInterest": "100", "sumOpenInterestValue": "1000"}
			rows = append(rows, row)
		}
		_ = json.NewEncoder(w).Encode(rows)
	})
	server := httptest.NewServer(h)
	defer server.Close()
	r, err := Fetch(context.Background(), Options{Now: now, HTTP: server.Client(), FuturesBase: server.URL, SpotBase: server.URL, FuturesRequestPace: -1, SpotRequestPace: -1})
	if err != nil {
		t.Fatal(err)
	}
	if r.Sources["futures_aggTrades"].Pages < 2 || r.Sources["spot_aggTrades"].Pages < 2 {
		t.Fatalf("pagination not exercised: %+v", r.Sources)
	}
	if len(r.FuturesTrades) != len(trades) || len(r.SpotTrades) != len(trades) {
		t.Fatalf("overlap dedup failed: %d/%d expected %d", len(r.FuturesTrades), len(r.SpotTrades), len(trades))
	}
	if r.HistoricalSignals != 0 || r.HistoricalExecutionIntents != 0 || r.HistoricalOrders != 0 {
		t.Fatal("bootstrap execution side effect")
	}
	for _, x := range r.External {
		if x.ReceiveTimestampMs != 0 || x.FetchCompletedAtMs == 0 {
			t.Fatalf("receive time fabricated: %+v", x)
		}
	}
	if err = r.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestValidateRejectsGapAndFabricatedReceive(t *testing.T) {
	r := Result{Origin: "BOOTSTRAP_HISTORY", FetchCompletedAtMs: 1}
	if err := r.Validate(); err == nil {
		t.Fatal("incomplete result accepted")
	}
}
