package uiapi

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"binance_trader/internal/credentials"
	featurev2 "binance_trader/internal/feature/main/v2"
	"binance_trader/internal/live/autopipeline"
)

type cleanupOutcomeBackend struct {
	fakeTestnetBackend
	submitErr error
}

func (b *cleanupOutcomeBackend) SubmitAuto(context.Context, autopipeline.ExecutionIntent) (ManualExecution, error) {
	b.autoSubmitCalls++
	return ManualExecution{}, b.submitErr
}

func TestAutoExecutionStateRoundTripBlocksRestartUntilReconciled(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auto-state.json")
	static := fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("console")}}
	s, err := NewServer(fakeProvider{credentials.CredentialStore{}}, static, Options{AutoExecutionStatePath: path})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().UTC().Add(time.Hour).Truncate(time.Millisecond)
	state := autoExecutionState{CandidateID: "candidate", DecisionTimestampMs: 123, EntryClientOrderID: "entry", EntrySide: "LONG", EntryRequestedQuantity: .001, EntryNormalizedQuantity: "0.001", EntryFilledQuantity: .001, EntryFillPrice: 100000, EntryFillTimestampMs: deadline.Add(-time.Hour).UnixMilli(), TPClientAlgoID: "tp", SLClientAlgoID: "sl", TPTriggerPrice: 101000, SLTriggerPrice: 99000, ProtectiveQuantity: .001, HorizonSeconds: 3600, HorizonDeadlineMs: deadline.UnixMilli(), HorizonCloseRequestID: "close", ExecutionState: "POSITION_PROTECTED"}
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

func TestIncompleteLifecycleStatesNeverTriggerAutomaticCloseAfterRestart(t *testing.T) {
	for _, executionState := range []string{"PENDING_ENTRY", "ENTRY_CONFIRMED", "PROTECTIVE_PARTIAL", "EXIT_PENDING"} {
		t.Run(executionState, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "auto-state.json")
			static := fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("console")}}
			writer, err := NewServer(fakeProvider{credentials.CredentialStore{}}, static, Options{AutoExecutionStatePath: path})
			if err != nil {
				t.Fatal(err)
			}
			state := autoExecutionState{CandidateID: "candidate", DecisionTimestampMs: 123, EntryClientOrderID: "entry", EntrySide: "LONG", EntryRequestedQuantity: .001, EntryNormalizedQuantity: "0.001", TPClientAlgoID: "tp", SLClientAlgoID: "sl", HorizonSeconds: 900, HorizonCloseRequestID: "close", ExecutionState: executionState}
			if err = writer.persistAutoExecutionState(state); err != nil {
				t.Fatal(err)
			}
			backend := &horizonBackend{open: true}
			restarted, err := NewServer(fakeProvider{credentials.CredentialStore{}}, static, Options{AutoExecutionStatePath: path, TestnetBackend: backend})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = restarted.reconcileAutoLifecycle(context.Background()); err == nil {
				t.Fatal("ambiguous state was automatically adopted")
			}
			if backend.closeCalls != 0 || !restarted.autoRecoveryBlocked || restarted.states[TradingEnvironmentTestnet].AutoState != "UNKNOWN_EXECUTION_STATE" {
				t.Fatalf("closeCalls=%d blocked=%t state=%s", backend.closeCalls, restarted.autoRecoveryBlocked, restarted.states[TradingEnvironmentTestnet].AutoState)
			}
		})
	}
}

func TestProtectedRestartRejectsProtectiveSemanticMismatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auto-state.json")
	static := fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("console")}}
	writer, err := NewServer(fakeProvider{credentials.CredentialStore{}}, static, Options{AutoExecutionStatePath: path})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	state := autoExecutionState{CandidateID: "candidate", DecisionTimestampMs: 123, EntryClientOrderID: "entry", EntrySide: "LONG", EntryRequestedQuantity: .001, EntryNormalizedQuantity: "0.001", EntryFilledQuantity: .001, EntryFillPrice: 100000, EntryFillTimestampMs: now.UnixMilli(), TPClientAlgoID: "tp", SLClientAlgoID: "sl", TPTriggerPrice: 101000, SLTriggerPrice: 99000, ProtectiveQuantity: .001, HorizonSeconds: 3600, HorizonDeadlineMs: now.Add(time.Hour).UnixMilli(), HorizonCloseRequestID: "close", ExecutionState: "POSITION_PROTECTED"}
	if err = writer.persistAutoExecutionState(state); err != nil {
		t.Fatal(err)
	}
	backend := &horizonBackend{open: true, badProtective: true}
	restarted, err := NewServer(fakeProvider{credentials.CredentialStore{}}, static, Options{AutoExecutionStatePath: path, TestnetBackend: backend})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = restarted.reconcileAutoLifecycle(context.Background()); err == nil {
		t.Fatal("invalid protective order was accepted")
	}
	if backend.closeCalls != 0 || !restarted.autoRecoveryBlocked {
		t.Fatalf("closeCalls=%d blocked=%t", backend.closeCalls, restarted.autoRecoveryBlocked)
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

func TestIncompleteCurrentAutoExecutionStateFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auto-state.json")
	data := []byte(`{"version":2,"environment":"TESTNET","symbol":"BTCUSDT","entry_side":"LONG","execution_state":"PENDING_ENTRY"}`)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	static := fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("console")}}
	s, err := NewServer(fakeProvider{credentials.CredentialStore{}}, static, Options{AutoExecutionStatePath: path})
	if err != nil {
		t.Fatal(err)
	}
	if !s.autoRecoveryBlocked || s.autoOwnedPosition || s.states[TradingEnvironmentTestnet].AutoState != "UNKNOWN_EXECUTION_STATE" {
		t.Fatalf("blocked=%t owned=%t state=%s", s.autoRecoveryBlocked, s.autoOwnedPosition, s.states[TradingEnvironmentTestnet].AutoState)
	}
}

func TestAutoExecutionStateAtomicReplaceFailurePreservesPublishedState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auto-state.json")
	static := fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("console")}}
	s, err := NewServer(fakeProvider{credentials.CredentialStore{}}, static, Options{AutoExecutionStatePath: path})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.persistConfirmedFlatAutoExecutionState(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	original := autoStateAtomicReplace
	autoStateAtomicReplace = func(string, string) error { return errors.New("injected replace failure") }
	t.Cleanup(func() { autoStateAtomicReplace = original })
	if err = s.persistAutoExecutionState(autoExecutionState{ExecutionState: "FLAT", LastReconciliationTimestampMs: 123}); err == nil {
		t.Fatal("replace failure was ignored")
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("published state changed after failed atomic replacement")
	}
	matches, err := filepath.Glob(filepath.Join(filepath.Dir(path), ".auto-execution-*.tmp"))
	if err != nil || len(matches) != 0 {
		t.Fatalf("temporary files=%v err=%v", matches, err)
	}
}

func TestConfirmedCleanupPersistsFlatAcrossRestart(t *testing.T) {
	t.Setenv("BINANCE_ENV", "TESTNET")
	t.Setenv("BINANCE_TESTNET_ENABLE_ORDERS", "true")
	t.Setenv("BINANCE_TESTNET_ENABLE_AUTO_ORDERS", "true")
	path := filepath.Join(t.TempDir(), "auto-state.json")
	static := fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("console")}}
	backend := &cleanupOutcomeBackend{submitErr: errAutoCleanupConfirmed}
	s, err := NewServer(fakeProvider{credentials.CredentialStore{Testnet: credentials.EnvironmentCredentials{APIKey: "key", APISecret: "secret"}}}, static, Options{AutoExecutionStatePath: path, TestnetBackend: backend})
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.states[TradingEnvironmentTestnet].AutoRunning = true
	s.states[TradingEnvironmentTestnet].AutoState = "RUNNING"
	s.mu.Unlock()
	_, err = s.executeAutoIntent(context.Background(), autoCleanupIntent("LONG", 2_200_000_000_000))
	if !errors.Is(err, errAutoCleanupConfirmed) {
		t.Fatalf("cleanup result=%v", err)
	}
	if s.autoRecoveryBlocked || s.failedUnknownSubmissions != 0 || s.states[TradingEnvironmentTestnet].AutoRunning || s.states[TradingEnvironmentTestnet].AutoState != "AUTO_CLEANUP_CONFIRMED" {
		t.Fatalf("blocked=%t unknown=%d running=%t state=%s", s.autoRecoveryBlocked, s.failedUnknownSubmissions, s.states[TradingEnvironmentTestnet].AutoRunning, s.states[TradingEnvironmentTestnet].AutoState)
	}
	restarted, err := NewServer(fakeProvider{credentials.CredentialStore{}}, static, Options{AutoExecutionStatePath: path})
	if err != nil {
		t.Fatal(err)
	}
	if restarted.autoRecoveryBlocked || restarted.autoOwnedPosition || restarted.autoExecutionState == nil || restarted.autoExecutionState.ExecutionState != "FLAT" || restarted.states[TradingEnvironmentTestnet].AutoRunning {
		t.Fatalf("unsafe restart: blocked=%t owned=%t state=%+v running=%t", restarted.autoRecoveryBlocked, restarted.autoOwnedPosition, restarted.autoExecutionState, restarted.states[TradingEnvironmentTestnet].AutoRunning)
	}
}

func TestConfirmedCleanupFlatPersistenceFailureRemainsUnknown(t *testing.T) {
	t.Setenv("BINANCE_ENV", "TESTNET")
	t.Setenv("BINANCE_TESTNET_ENABLE_ORDERS", "true")
	t.Setenv("BINANCE_TESTNET_ENABLE_AUTO_ORDERS", "true")
	path := filepath.Join(t.TempDir(), "auto-state.json")
	static := fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("console")}}
	backend := &cleanupOutcomeBackend{submitErr: errAutoCleanupConfirmed}
	s, err := NewServer(fakeProvider{credentials.CredentialStore{Testnet: credentials.EnvironmentCredentials{APIKey: "key", APISecret: "secret"}}}, static, Options{AutoExecutionStatePath: path, TestnetBackend: backend})
	if err != nil {
		t.Fatal(err)
	}
	original := autoStateAtomicReplace
	calls := 0
	autoStateAtomicReplace = func(from, to string) error {
		calls++
		if calls == 2 {
			return errors.New("injected FLAT publication failure")
		}
		return original(from, to)
	}
	t.Cleanup(func() { autoStateAtomicReplace = original })
	s.mu.Lock()
	s.states[TradingEnvironmentTestnet].AutoRunning = true
	s.mu.Unlock()
	_, err = s.executeAutoIntent(context.Background(), autoCleanupIntent("LONG", 2_300_000_000_000))
	if !errors.Is(err, errUnknownExecutionState) || !s.autoRecoveryBlocked || s.failedUnknownSubmissions != 1 || s.states[TradingEnvironmentTestnet].AutoState != "UNKNOWN_EXECUTION_STATE" {
		t.Fatalf("err=%v blocked=%t unknown=%d state=%s", err, s.autoRecoveryBlocked, s.failedUnknownSubmissions, s.states[TradingEnvironmentTestnet].AutoState)
	}
	data, readErr := os.ReadFile(path)
	if readErr != nil || !strings.Contains(string(data), `"execution_state": "PENDING_ENTRY"`) {
		t.Fatalf("persisted state=%s err=%v", data, readErr)
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
