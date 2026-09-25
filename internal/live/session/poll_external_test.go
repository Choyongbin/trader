package session

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	live "binance_trader/internal/live/binance"
)

// A slow five-minute endpoint must never block the ten-second kline poller.
func TestExternalPollingCadencesIndependent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var fastCalls, slowCalls atomic.Int32
	startedSlow := make(chan struct{})
	fast := func(context.Context) ([]live.ExternalObservation, error) {
		fastCalls.Add(1)
		return nil, nil
	}
	slow := func(ctx context.Context) ([]live.ExternalObservation, error) {
		if slowCalls.Add(1) == 1 {
			close(startedSlow)
		}
		select {
		case <-time.After(300 * time.Millisecond):
		case <-ctx.Done():
		}
		return nil, ctx.Err()
	}
	out := make(chan externalResult, 128)
	done := make(chan struct{})
	go func() {
		defer close(done)
		pollExternalWithFetch(ctx, out, fast, slow, 10*time.Millisecond, 100*time.Millisecond)
	}()
	defer cancel()
	select {
	case <-startedSlow:
	case <-time.After(time.Second):
		t.Fatal("slow poller did not start")
	}
	deadline := time.After(time.Second)
	for fastCalls.Load() < 3 {
		select {
		case <-deadline:
			t.Fatal("slow poller blocked fast poller")
		case <-time.After(5 * time.Millisecond):
		}
	}
	if slowCalls.Load() != 1 {
		t.Fatalf("slow poller was not independently rate limited: %d", slowCalls.Load())
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("poller failed to stop on cancellation")
	}
}
