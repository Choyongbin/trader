package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	live "binance_trader/internal/live/binance"
)

func TestSummarizeLiveDetectsAlignedIntervalsAndTransportStats(t *testing.T) {
	end := int64(1_800_000_000_000)
	receive := end + 7_000
	endpoints := []struct {
		name string
		ts   int64
	}{
		{"metrics_oi", end}, {"metrics_global", end}, {"metrics_top_account", end},
		{"metrics_top_position", end}, {"metrics_taker", end - 300_000},
	}
	var samples []live.MetricsProbeResult
	for _, endpoint := range endpoints {
		samples = append(samples, live.MetricsProbeResult{Endpoint: endpoint.name, HTTPStatus: 200, ResponseCount: 1, ReceiveTimestampMs: receive, Rows: []live.MetricsProbeRow{{SourceTimestampMs: endpoint.ts}}})
	}
	report := summarizeLive(samples, time.UnixMilli(end), time.UnixMilli(receive), 0, 30*time.Second)
	if report.Status != "PASS" || report.CompleteNormalizedIntervals != 1 || len(report.Endpoints) != 5 {
		t.Fatalf("report=%+v", report)
	}
}

func TestSummarizeLiveRejectsMissingAndRecordsReverse(t *testing.T) {
	receive := int64(1_800_000_010_000)
	samples := []live.MetricsProbeResult{{Endpoint: "metrics_oi", HTTPStatus: 200, ResponseCount: 2, ReceiveTimestampMs: receive, Rows: []live.MetricsProbeRow{{SourceTimestampMs: 1_800_000_000_000}, {SourceTimestampMs: 1_799_999_700_000}}}}
	report := summarizeLive(samples, time.Now(), time.Now(), 0, time.Second)
	if report.Status != "INSUFFICIENT_LIVE_EVIDENCE" || report.CompleteNormalizedIntervals != 0 || report.Endpoints[0].ResponseReverseCount != 1 {
		t.Fatalf("report=%+v", report)
	}
}

func TestSummarizeLiveUsesFirstObservationAndExcludesInitialRows(t *testing.T) {
	end := int64(1_800_000_000_000)
	samples := []live.MetricsProbeResult{
		{Endpoint: "metrics_oi", HTTPStatus: 200, ResponseCount: 1, ReceiveTimestampMs: end + 10_000, Rows: []live.MetricsProbeRow{{SourceTimestampMs: end}}},
		{Endpoint: "metrics_oi", HTTPStatus: 200, ResponseCount: 2, ReceiveTimestampMs: end + 330_000, Rows: []live.MetricsProbeRow{{SourceTimestampMs: end}, {SourceTimestampMs: end + 300_000}}},
		{Endpoint: "metrics_oi", HTTPStatus: 200, ResponseCount: 1, ReceiveTimestampMs: end + 360_000, Rows: []live.MetricsProbeRow{{SourceTimestampMs: end + 300_000}}},
	}
	report := summarizeLive(samples, time.Now(), time.Now(), time.Minute, 30*time.Second)
	stats := report.Endpoints[0]
	if stats.LeftCensoredTimestamps != 1 || stats.FirstObservedArrivalSamples != 1 || stats.FirstObservedLagMinMs != 30_000 || stats.FirstObservedLagMaxMs != 30_000 {
		t.Fatalf("stats=%+v", stats)
	}
}

func TestEvidenceDoesNotPromoteCurrentDocsToHistoricalProof(t *testing.T) {
	report := buildEvidence(26)
	if report.FullHistoricalVerified {
		t.Fatal("historical fields unexpectedly verified")
	}
	for _, field := range report.Fields {
		if field.Field != "taker_long_short_volume_ratio" && field.HistoricalCorrectionSafe {
			t.Fatalf("field=%+v", field)
		}
	}
}

func TestWriteNormalizedLivePreservesIndependentFieldTimingAndValue(t *testing.T) {
	ts := int64(1_800_000_000_000)
	raw := json.RawMessage(`{"timestamp":1800000000000,"sumOpenInterest":"12.5","sumOpenInterestValue":"34.5"}`)
	samples := []live.MetricsProbeResult{{Endpoint: "metrics_oi", HTTPStatus: 200, ResponseCount: 1, ReceiveTimestampMs: ts + 9_000, Rows: []live.MetricsProbeRow{{SourceTimestampMs: ts, Raw: raw}}}}
	path := filepath.Join(t.TempDir(), "normalized.jsonl")
	if err := writeNormalizedLive(path, samples); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) == 0 || !bytes.Contains(b, []byte(`"value":"12.5"`)) || !bytes.Contains(b, []byte(`"value":"34.5"`)) || !bytes.Contains(b, []byte(`"interval_end_ms":1800000000000`)) {
		t.Fatalf("normalized=%s", b)
	}
}
