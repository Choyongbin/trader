package main

import (
	"testing"

	live "binance_trader/internal/live/binance"
)

func TestSourceAuditRejectsProductionIncompleteKline(t *testing.T) {
	row := live.ExternalObservation{Dataset: "mark", SourceTimestampMs: 1_000, CloseTimeMs: 60_999, ReceiveTimestampMs: 60_999}
	if got := selectSourceAudit([]live.ExternalObservation{row}, "mark", 120_000); got.Available {
		t.Fatalf("diagnostic accepted production-discarded kline: %+v", got)
	}
	row.ReceiveTimestampMs = 61_000
	if got := selectSourceAudit([]live.ExternalObservation{row}, "mark", 120_000); !got.Available {
		t.Fatalf("diagnostic rejected completed kline: %+v", got)
	}
}
