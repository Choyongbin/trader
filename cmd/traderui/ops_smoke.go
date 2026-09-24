package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const opsSmokeFile = "data/reports/ui/v2/stage-live-runtime-smoke.json"

type smokeSystem struct {
	LiveFeatureInput   string            `json:"live_feature_input"`
	FeatureEngineState string            `json:"feature_engine_state"`
	Counters           map[string]uint64 `json:"counters"`
	Warmup             struct {
		Status         string `json:"status"`
		Ready          bool   `json:"ready"`
		InputConnected bool   `json:"input_connected"`
		AvailableMs    int64  `json:"available_ms"`
	} `json:"warmup"`
}

type smokeAuto struct {
	Signal    string `json:"signal"`
	Running   bool   `json:"running"`
	Readiness struct {
		BlockedReason string `json:"blocked_reason"`
	} `json:"readiness"`
}

type smokePortfolio struct {
	Futures struct {
		Status string `json:"status"`
	} `json:"futures"`
	Spot struct {
		Status string `json:"status"`
	} `json:"spot"`
}

type runtimeSmokeEvidence struct {
	Version                int     `json:"version"`
	Status                 string  `json:"status"`
	Complete               bool    `json:"complete"`
	DurationSec            float64 `json:"duration_sec"`
	HTTPChecks             int     `json:"http_checks"`
	HTTPFailures           int     `json:"http_failures"`
	WSMessages             int     `json:"ws_messages"`
	MarketDelta            uint64  `json:"market_delta"`
	FuturesCanonicalDelta  uint64  `json:"futures_canonical_delta"`
	SpotCanonicalDelta     uint64  `json:"spot_canonical_delta"`
	ExternalUpdatesDelta   uint64  `json:"external_updates_delta"`
	NotReadyDecisionsDelta uint64  `json:"not_ready_decisions_delta"`
	DuplicateDelta         uint64  `json:"duplicate_delta"`
	ReverseDelta           uint64  `json:"reverse_delta"`
	IDGapDelta             uint64  `json:"id_gap_delta"`
	ParseErrorDelta        uint64  `json:"parse_error_delta"`
	ActualSubmitsDelta     uint64  `json:"actual_submits_delta"`
	ActualSubmitsFinal     uint64  `json:"actual_submits_final"`
	DemoWallet             string  `json:"demo_wallet"`
	Spot                   string  `json:"spot"`
	Mainnet                string  `json:"mainnet"`
	FeatureInput           string  `json:"feature_input"`
	FeatureState           string  `json:"feature_state"`
	Signal                 string  `json:"signal"`
	BlockedReason          string  `json:"blocked_reason"`
	WarmupReady            bool    `json:"warmup_ready"`
	FutureObservations     uint64  `json:"future_observations"`
	PanicCount             int     `json:"panic_count"`
	ConcurrentWriterErrors int     `json:"concurrent_writer_errors"`
	FinalHoldoutAccessed   bool    `json:"final_holdout_accessed"`
}

func smokeGET(client *http.Client, path string, into any) error {
	resp, err := client.Get("http://127.0.0.1:8080" + path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s HTTP %d", path, resp.StatusCode)
	}
	if into == nil {
		_, err = io.Copy(io.Discard, io.LimitReader(resp.Body, 2<<20))
		return err
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 2<<20)).Decode(into)
}

func counterDelta(end, start map[string]uint64, key string) uint64 {
	if end[key] < start[key] {
		return 0
	}
	return end[key] - start[key]
}

func runRuntimeSmoke() error {
	start := time.Now()
	client := &http.Client{Timeout: 5 * time.Second}
	var first, last smokeSystem
	var auto smokeAuto
	var demo, mainnet smokePortfolio
	e := runtimeSmokeEvidence{Version: 1, Status: "FAIL"}
	if err := smokeGET(client, "/api/system", &first); err != nil {
		return err
	}
	ws, _, err := websocket.DefaultDialer.Dial("ws://127.0.0.1:8080/ws", nil)
	if err != nil {
		return err
	}
	_ = ws.SetReadDeadline(time.Now().Add(15 * time.Second))
	if _, _, err = ws.ReadMessage(); err == nil {
		e.WSMessages++
	}
	_ = ws.Close()
	for i := 0; i < 24; i++ {
		if err := smokeGET(client, "/", nil); err != nil {
			e.HTTPFailures++
		} else {
			e.HTTPChecks++
		}
		if err := smokeGET(client, "/api/system", &last); err != nil {
			e.HTTPFailures++
		} else {
			e.HTTPChecks++
		}
		if err := smokeGET(client, "/api/auto-trading?environment=TESTNET", &auto); err != nil {
			e.HTTPFailures++
		} else {
			e.HTTPChecks++
		}
		if i == 0 || i == 23 {
			if err := smokeGET(client, "/api/portfolio?environment=TESTNET", &demo); err != nil {
				e.HTTPFailures++
			} else {
				e.HTTPChecks++
			}
			if err := smokeGET(client, "/api/portfolio?environment=MAINNET", &mainnet); err != nil {
				e.HTTPFailures++
			} else {
				e.HTTPChecks++
			}
		}
		time.Sleep(5 * time.Second)
	}
	e.DurationSec = time.Since(start).Seconds()
	e.MarketDelta = counterDelta(last.Counters, first.Counters, "market_messages")
	e.FuturesCanonicalDelta = counterDelta(last.Counters, first.Counters, "canonical_futures_bars")
	e.SpotCanonicalDelta = counterDelta(last.Counters, first.Counters, "canonical_spot_bars")
	e.ExternalUpdatesDelta = counterDelta(last.Counters, first.Counters, "external_source_updates")
	e.NotReadyDecisionsDelta = counterDelta(last.Counters, first.Counters, "feature_not_ready_decisions")
	e.DuplicateDelta = counterDelta(last.Counters, first.Counters, "canonical_duplicate_events")
	e.ReverseDelta = counterDelta(last.Counters, first.Counters, "canonical_reverse_events")
	e.IDGapDelta = counterDelta(last.Counters, first.Counters, "canonical_id_gaps")
	e.ParseErrorDelta = counterDelta(last.Counters, first.Counters, "public_parse_errors")
	e.ActualSubmitsDelta = counterDelta(last.Counters, first.Counters, "actual_order_submits")
	e.ActualSubmitsFinal = last.Counters["actual_order_submits"]
	e.FutureObservations = last.Counters["future_observations"]
	e.DemoWallet, e.Spot, e.Mainnet = demo.Futures.Status, demo.Spot.Status, mainnet.Futures.Status
	e.FeatureInput, e.FeatureState = last.LiveFeatureInput, last.FeatureEngineState
	e.Signal, e.BlockedReason = auto.Signal, auto.Readiness.BlockedReason
	e.WarmupReady = last.Warmup.Ready
	var logs struct {
		Logs []string `json:"logs"`
	}
	if err := smokeGET(client, "/api/logs", &logs); err == nil {
		e.HTTPChecks++
		for _, line := range logs.Logs {
			if strings.Contains(line, "panic:") {
				e.PanicCount++
			}
			if strings.Contains(strings.ToLower(line), "concurrent write") {
				e.ConcurrentWriterErrors++
			}
		}
	} else {
		e.HTTPFailures++
	}
	e.Complete = e.DurationSec >= 120 && e.DurationSec <= 180 && e.HTTPChecks > 0 && e.HTTPFailures == 0 && e.WSMessages > 0 && e.MarketDelta > 0 && e.FuturesCanonicalDelta > 0 && e.SpotCanonicalDelta > 0 && e.ExternalUpdatesDelta > 0 && e.NotReadyDecisionsDelta > 0 && e.DuplicateDelta == 0 && e.ReverseDelta == 0 && e.IDGapDelta == 0 && e.ParseErrorDelta == 0 && e.ActualSubmitsDelta == 0 && last.Counters["actual_order_submits"] == 0 && e.FutureObservations == 0 && e.DemoWallet == "READY" && e.Spot == "UNAVAILABLE" && e.Mainnet == "NOT_CONNECTED" && e.FeatureInput == "CONNECTED" && (e.FeatureState == "WARMING_UP" || e.FeatureState == "GAP") && !e.WarmupReady && e.Signal == "NOT_READY" && e.BlockedReason == "FEATURE_WARMUP" && e.PanicCount == 0 && e.ConcurrentWriterErrors == 0 && !auto.Running
	if e.Complete {
		e.Status = "PASS"
	}
	if err := writeOpsAtomic(filepath.FromSlash(opsSmokeFile), e, false); err != nil {
		return err
	}
	fmt.Printf("UI/OPS runtime smoke %s duration=%.1fs HTTP=%d/%d WS=%d market=%d futures_bars=%d spot_bars=%d external=%d not_ready=%d orders=%d\n", e.Status, e.DurationSec, e.HTTPChecks, e.HTTPFailures, e.WSMessages, e.MarketDelta, e.FuturesCanonicalDelta, e.SpotCanonicalDelta, e.ExternalUpdatesDelta, e.NotReadyDecisionsDelta, last.Counters["actual_order_submits"])
	if !e.Complete {
		return fmt.Errorf("runtime smoke gate failed")
	}
	return nil
}
