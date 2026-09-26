package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type MetricsProbeRow struct {
	SourceTimestampMs int64           `json:"source_timestamp_ms"`
	Raw               json.RawMessage `json:"raw"`
}

type MetricsProbeResult struct {
	Endpoint           string            `json:"endpoint"`
	URL                string            `json:"url"`
	AttemptedAtMs      int64             `json:"attempted_at_ms"`
	ReceiveTimestampMs int64             `json:"receive_timestamp_ms"`
	HTTPStatus         int               `json:"http_status"`
	ResponseCount      int               `json:"response_count"`
	Rows               []MetricsProbeRow `json:"rows,omitempty"`
	Error              string            `json:"error,omitempty"`
}

var metricsProbePaths = []struct {
	name string
	path string
}{
	{"metrics_oi", "/futures/data/openInterestHist?symbol=BTCUSDT&period=5m&limit=2"},
	{"metrics_global", "/futures/data/globalLongShortAccountRatio?symbol=BTCUSDT&period=5m&limit=2"},
	{"metrics_top_account", "/futures/data/topLongShortAccountRatio?symbol=BTCUSDT&period=5m&limit=2"},
	{"metrics_top_position", "/futures/data/topLongShortPositionRatio?symbol=BTCUSDT&period=5m&limit=2"},
	{"metrics_taker", "/futures/data/takerlongshortRatio?symbol=BTCUSDT&period=5m&limit=2"},
}

// ProbePublicMetrics performs only the same five public GETs used by the slow
// poller, while retaining endpoint-level transport evidence for diagnostics.
func ProbePublicMetrics(ctx context.Context, client *http.Client, baseURL string) []MetricsProbeResult {
	if client == nil {
		client = &http.Client{Timeout: 12 * time.Second}
	}
	if baseURL == "" {
		baseURL = "https://fapi.binance.com"
	}
	baseURL = strings.TrimRight(baseURL, "/")
	results := make([]MetricsProbeResult, 0, len(metricsProbePaths))
	for _, endpoint := range metricsProbePaths {
		result := MetricsProbeResult{Endpoint: endpoint.name, URL: baseURL + endpoint.path, AttemptedAtMs: time.Now().UnixMilli()}
		if _, err := url.ParseRequestURI(result.URL); err != nil {
			result.Error = err.Error()
			results = append(results, result)
			continue
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, result.URL, nil)
		if err != nil {
			result.Error = err.Error()
			results = append(results, result)
			continue
		}
		resp, err := client.Do(req)
		if err != nil {
			result.Error = err.Error()
			results = append(results, result)
			continue
		}
		result.HTTPStatus = resp.StatusCode
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
		_ = resp.Body.Close()
		result.ReceiveTimestampMs = time.Now().UnixMilli()
		if readErr != nil {
			result.Error = readErr.Error()
			results = append(results, result)
			continue
		}
		if resp.StatusCode/100 != 2 {
			result.Error = fmt.Sprintf("HTTP %d", resp.StatusCode)
			results = append(results, result)
			continue
		}
		var rawRows []json.RawMessage
		if err = json.Unmarshal(body, &rawRows); err != nil {
			result.Error = "invalid JSON: " + err.Error()
			results = append(results, result)
			continue
		}
		result.ResponseCount = len(rawRows)
		for _, raw := range rawRows {
			var object map[string]json.RawMessage
			if err = json.Unmarshal(raw, &object); err != nil {
				result.Error = "invalid row: " + err.Error()
				break
			}
			var timestamp int64
			if err = json.Unmarshal(object["timestamp"], &timestamp); err != nil || timestamp <= 0 {
				result.Error = "invalid row timestamp"
				break
			}
			result.Rows = append(result.Rows, MetricsProbeRow{SourceTimestampMs: timestamp, Raw: append(json.RawMessage(nil), raw...)})
		}
		results = append(results, result)
	}
	return results
}
