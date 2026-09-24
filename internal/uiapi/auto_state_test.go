package uiapi

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"
	"time"

	"binance_trader/internal/credentials"
	featurev2 "binance_trader/internal/feature/main/v2"
)

func TestAutoExecutionStateRoundTripBlocksRestartUntilReconciled(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auto-state.json")
	static := fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("console")}}
	s, err := NewServer(fakeProvider{credentials.CredentialStore{}}, static, Options{AutoExecutionStatePath: path})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().UTC().Add(time.Hour).Truncate(time.Millisecond)
	state := autoExecutionState{CandidateID: "candidate", DecisionTimestampMs: 123, EntryClientOrderID: "entry", EntrySide: "LONG", EntryFilledQuantity: .001, EntryFillPrice: 100000, TPClientAlgoID: "tp", SLClientAlgoID: "sl", HorizonDeadlineMs: deadline.UnixMilli(), HorizonCloseRequestID: "close", ExecutionState: "POSITION_PROTECTED"}
	if err = s.persistAutoExecutionState(state); err != nil {
		t.Fatal(err)
	}
	backend := &horizonBackend{open: true}
	restarted, err := NewServer(fakeProvider{credentials.CredentialStore{}}, static, Options{AutoExecutionStatePath: path, TestnetBackend: backend})
	if err != nil {
		t.Fatal(err)
	}
	if !restarted.autoOwnedPosition || !restarted.autoRecoveryBlocked || restarted.states[TradingEnvironmentTestnet].AutoRunning || restarted.states[TradingEnvironmentTestnet].AutoState != "UNKNOWN_EXECUTION_STATE" {
		t.Fatalf("unsafe restored state: owned=%t blocked=%t running=%t state=%s", restarted.autoOwnedPosition, restarted.autoRecoveryBlocked, restarted.states[TradingEnvironmentTestnet].AutoRunning, restarted.states[TradingEnvironmentTestnet].AutoState)
	}
	if restarted.autoCloseRequestID != "close" || !restarted.autoExitDeadline.Equal(deadline) {
		t.Fatal("horizon ownership was not restored")
	}
	holding, err := restarted.reconcileAutoLifecycle(context.Background())
	if err != nil || !holding || restarted.autoRecoveryBlocked || restarted.states[TradingEnvironmentTestnet].AutoState != "POSITION_OPEN" {
		t.Fatalf("recovery reconciliation failed: holding=%t blocked=%t state=%s err=%v", holding, restarted.autoRecoveryBlocked, restarted.states[TradingEnvironmentTestnet].AutoState, err)
	}
}

func TestCorruptAutoExecutionStateBlocksEntries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auto-state.json")
	if err := os.WriteFile(path, []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	static := fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("console")}}
	s, err := NewServer(fakeProvider{credentials.CredentialStore{}}, static, Options{AutoExecutionStatePath: path})
	if err != nil {
		t.Fatal(err)
	}
	if !s.autoRecoveryBlocked || s.states[TradingEnvironmentTestnet].AutoState != "UNKNOWN_EXECUTION_STATE" {
		t.Fatal("corrupt state did not fail closed")
	}
}

func TestAutoDecisionQueueDeduplicatesAndBounds(t *testing.T) {
	s := &Server{autoDecisionQueue: make(chan queuedAutoDecision, 1), states: map[TradingEnvironment]*environmentState{TradingEnvironmentTestnet: {}}}
	s.enqueueAutoDecision(featureSnapshot(100), 1)
	s.enqueueAutoDecision(featureSnapshot(100), 1)
	s.enqueueAutoDecision(featureSnapshot(101), 1)
	if len(s.autoDecisionQueue) != 1 || s.autoQueueOverflows != 1 {
		t.Fatalf("queue=%d overflow=%d", len(s.autoDecisionQueue), s.autoQueueOverflows)
	}
}

func featureSnapshot(timestamp int64) featurev2.Snapshot {
	return featurev2.Snapshot{DecisionTimestampMs: timestamp}
}
