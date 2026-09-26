package binance

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestProbePublicMetricsRetainsPerEndpointEvidence(t *testing.T) {
	ts := int64(1_800_000_000_000)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method=%s", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `[{"timestamp":%d},{"timestamp":%d}]`, ts-300_000, ts)
	}))
	defer server.Close()
	results := ProbePublicMetrics(context.Background(), server.Client(), server.URL)
	if len(results) != 5 {
		t.Fatalf("results=%d", len(results))
	}
	for _, result := range results {
		if result.HTTPStatus != http.StatusOK || result.ResponseCount != 2 || len(result.Rows) != 2 || result.ReceiveTimestampMs == 0 || result.Error != "" {
			t.Fatalf("result=%+v", result)
		}
	}
}

func TestProbePublicMetricsRecordsHTTPFailureWithoutHidingOtherEndpoints(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 2 {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprint(w, `[{"timestamp":1800000000000}]`)
	}))
	defer server.Close()
	results := ProbePublicMetrics(context.Background(), server.Client(), server.URL)
	if len(results) != 5 || results[1].HTTPStatus != http.StatusServiceUnavailable || results[1].Error == "" {
		t.Fatalf("results=%+v", results)
	}
	if results[4].HTTPStatus != http.StatusOK {
		t.Fatalf("later endpoint was not attempted: %+v", results[4])
	}
}
