package uiapi

import (
	"context"
	"io/fs"
	"net/http"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"binance_trader/internal/credentials"
	"binance_trader/internal/live/autopipeline"
	"binance_trader/internal/live/binance"
)

type blockingEntryBackend struct {
	started     chan struct{}
	release     chan struct{}
	startOnce   sync.Once
	open        atomic.Bool
	manualCalls atomic.Int32
	autoCalls   atomic.Int32
}

type preparedGateBackend struct{ submitted atomic.Int32 }

func (*preparedGateBackend) Refresh(context.Context) (Balance, []Position, []Order, error) {
	return Balance{Connected: true}, nil, nil, nil
}
func (*preparedGateBackend) SubmitManual(context.Context, ManualOrderRequest) (ManualExecution, error) {
	return ManualExecution{}, nil
}
func (*preparedGateBackend) ClosePosition(context.Context, string) (binance.Order, error) {
	return binance.Order{}, nil
}
func (*preparedGateBackend) SubmitAuto(context.Context, autopipeline.ExecutionIntent) (ManualExecution, error) {
	return ManualExecution{}, errAutoEntryNotSubmitted
}
func (*preparedGateBackend) CleanupAutoProtective(context.Context, []string) error { return nil }
func (*preparedGateBackend) PrepareAuto(_ context.Context, intent autopipeline.ExecutionIntent) (autoSubmissionPlan, error) {
	return autoSubmissionPlan{intent: intent, quantityText: "0.001", entryID: "entry", tpID: "tp", slID: "sl"}, nil
}
func (b *preparedGateBackend) SubmitPreparedAuto(context.Context, autoSubmissionPlan) (ManualExecution, error) {
	b.submitted.Add(1)
	return ManualExecution{}, errAutoEntryNotSubmitted
}

func (b *blockingEntryBackend) Refresh(context.Context) (Balance, []Position, []Order, error) {
	balance := Balance{Connected: true}
	if !b.open.Load() {
		return balance, nil, nil, nil
	}
	return balance, []Position{{Environment: TradingEnvironmentTestnet, Symbol: "BTCUSDT", Side: "LONG", QuantityBTC: .001}}, nil, nil
}
func (b *blockingEntryBackend) SubmitManual(context.Context, ManualOrderRequest) (ManualExecution, error) {
	b.manualCalls.Add(1)
	b.open.Store(true)
	b.startOnce.Do(func() { close(b.started) })
	<-b.release
	return ManualExecution{Order: Order{ClientOrderID: "manual", Status: "FILLED"}, Position: Position{Environment: TradingEnvironmentTestnet, Symbol: "BTCUSDT", Side: "LONG", QuantityBTC: .001}}, nil
}
func (*blockingEntryBackend) ClosePosition(context.Context, string) (binance.Order, error) {
	return binance.Order{}, nil
}
func (b *blockingEntryBackend) SubmitAuto(context.Context, autopipeline.ExecutionIntent) (ManualExecution, error) {
	b.autoCalls.Add(1)
	return ManualExecution{}, nil
}
func (*blockingEntryBackend) CleanupAutoProtective(context.Context, []string) error { return nil }

func newOrderGateServer(t *testing.T, backend TestnetBackend) *Server {
	t.Helper()
	t.Setenv("BINANCE_ENV", "TESTNET")
	t.Setenv("BINANCE_TESTNET_ENABLE_ORDERS", "true")
	t.Setenv("BINANCE_TESTNET_ENABLE_AUTO_ORDERS", "true")
	static := fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("console"), Mode: fs.FileMode(0o600)}}
	s, err := NewServer(fakeProvider{credentials.CredentialStore{Testnet: credentials.EnvironmentCredentials{APIKey: "key", APISecret: "secret"}}}, static, Options{TestnetBackend: backend})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func manualGateRequest(id string) ManualOrderRequest {
	return ManualOrderRequest{Environment: TradingEnvironmentTestnet, Symbol: "BTCUSDT", Side: "LONG", OrderType: "MARKET", MarginAmountUSDT: 100, Leverage: 1, RequestID: id}
}

func TestManualEntriesShareOneSubmissionBoundary(t *testing.T) {
	backend := &blockingEntryBackend{started: make(chan struct{}), release: make(chan struct{})}
	s := newOrderGateServer(t, backend)
	responses := make(chan int, 2)
	go func() {
		responses <- perform(s.Handler(), http.MethodPost, "/api/manual-order", s.csrf, manualGateRequest("one")).Code
	}()
	<-backend.started
	go func() {
		responses <- perform(s.Handler(), http.MethodPost, "/api/manual-order", s.csrf, manualGateRequest("two")).Code
	}()
	time.Sleep(25 * time.Millisecond)
	if backend.manualCalls.Load() != 1 {
		t.Fatalf("concurrent manual submit crossed boundary: %d", backend.manualCalls.Load())
	}
	close(backend.release)
	a, b := <-responses, <-responses
	if backend.manualCalls.Load() != 1 || !((a == http.StatusOK && b == http.StatusConflict) || (b == http.StatusOK && a == http.StatusConflict)) {
		t.Fatalf("manual serialization failed calls=%d responses=%d,%d", backend.manualCalls.Load(), a, b)
	}
}

func TestManualEntryBlocksConcurrentAutoEntry(t *testing.T) {
	backend := &blockingEntryBackend{started: make(chan struct{}), release: make(chan struct{})}
	s := newOrderGateServer(t, backend)
	manualDone := make(chan int, 1)
	go func() {
		manualDone <- perform(s.Handler(), http.MethodPost, "/api/manual-order", s.csrf, manualGateRequest("manual")).Code
	}()
	<-backend.started
	s.mu.Lock()
	s.states[TradingEnvironmentTestnet].AutoRunning = true
	s.mu.Unlock()
	autoDone := make(chan error, 1)
	go func() {
		_, err := s.executeAutoIntent(context.Background(), autopipeline.ExecutionIntent{Environment: "TESTNET", Symbol: "BTCUSDT", Side: "LONG"})
		autoDone <- err
	}()
	time.Sleep(25 * time.Millisecond)
	if backend.autoCalls.Load() != 0 {
		t.Fatal("auto submit crossed manual submission boundary")
	}
	close(backend.release)
	if <-manualDone != http.StatusOK || <-autoDone == nil || backend.autoCalls.Load() != 0 {
		t.Fatalf("manual-auto exclusion failed auto_calls=%d", backend.autoCalls.Load())
	}
}

func TestStopWaitsForSubmissionBoundary(t *testing.T) {
	s := testServer(t)
	s.mu.Lock()
	s.states[TradingEnvironmentTestnet].AutoRunning = true
	s.mu.Unlock()
	s.entrySubmitMu.Lock()
	done := make(chan int, 1)
	go func() {
		done <- perform(s.Handler(), http.MethodPost, "/api/auto-trading/stop?environment=TESTNET", s.csrf, nil).Code
	}()
	select {
	case <-done:
		t.Fatal("STOP returned before the active submission boundary completed")
	case <-time.After(25 * time.Millisecond):
	}
	s.entrySubmitMu.Unlock()
	if code := <-done; code != http.StatusOK {
		t.Fatalf("STOP status=%d", code)
	}
	s.mu.RLock()
	running := s.states[TradingEnvironmentTestnet].AutoRunning
	s.mu.RUnlock()
	if running {
		t.Fatal("STOP did not disable entries")
	}
}

func TestStopAtAutomaticPreparationBoundaries(t *testing.T) {
	for _, stage := range []string{"AFTER_PREPARE", "AFTER_PENDING_PERSIST", "BEFORE_ENTRY_SUBMIT"} {
		t.Run(stage, func(t *testing.T) {
			backend := &preparedGateBackend{}
			s := newOrderGateServer(t, backend)
			s.autoExecutionStatePath = filepath.Join(t.TempDir(), "execution.json")
			s.mu.Lock()
			s.states[TradingEnvironmentTestnet].AutoRunning = true
			s.mu.Unlock()
			reached := make(chan struct{})
			release := make(chan struct{})
			var once sync.Once
			s.autoExecutionHook = func(at string) {
				if at == stage {
					once.Do(func() { close(reached) })
					<-release
				}
			}
			executionDone := make(chan error, 1)
			go func() {
				_, err := s.executeAutoIntent(context.Background(), autopipeline.ExecutionIntent{Environment: "TESTNET", Symbol: "BTCUSDT", Side: "LONG", CandidateID: "candidate", DecisionTimestampMs: 123, HorizonSeconds: 900, QuantityBTC: .001})
				executionDone <- err
			}()
			<-reached
			stopDone := make(chan int, 1)
			go func() {
				stopDone <- perform(s.Handler(), http.MethodPost, "/api/auto-trading/stop?environment=TESTNET", s.csrf, nil).Code
			}()
			select {
			case <-stopDone:
				t.Fatal("STOP returned while a pre-submit boundary was active")
			case <-time.After(25 * time.Millisecond):
			}
			close(release)
			if err := <-executionDone; err == nil {
				t.Fatal("fixture submission should report not submitted")
			}
			if code := <-stopDone; code != http.StatusOK || backend.submitted.Load() != 0 {
				t.Fatalf("boundary result status=%d submits=%d", code, backend.submitted.Load())
			}
		})
	}
}
