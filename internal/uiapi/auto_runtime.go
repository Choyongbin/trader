package uiapi

import (
	"context"
	"fmt"
	"math"
	"time"

	featurev2 "binance_trader/internal/feature/main/v2"
	"binance_trader/internal/live/autopipeline"
)

type queuedAutoDecision struct {
	snapshot   featurev2.Snapshot
	entryPrice float64
}

const maxAutoDecisionAge = 15 * time.Second

func (s *Server) enqueueAutoDecision(snapshot featurev2.Snapshot, entryPrice float64) {
	s.mu.Lock()
	if snapshot.DecisionTimestampMs <= s.autoLastDecisionMs {
		s.mu.Unlock()
		return
	}
	s.autoLastDecisionMs = snapshot.DecisionTimestampMs
	s.mu.Unlock()
	select {
	case s.autoDecisionQueue <- queuedAutoDecision{snapshot: snapshot, entryPrice: entryPrice}:
	default:
		s.mu.Lock()
		s.autoQueueOverflows++
		s.logLocked("Auto decision queue overflow; signal dropped")
		s.mu.Unlock()
	}
}

func (s *Server) runAutoWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case decision := <-s.autoDecisionQueue:
			s.mu.RLock()
			running := s.states[TradingEnvironmentTestnet].AutoRunning
			s.mu.RUnlock()
			if !running {
				continue
			}
			if age := s.now().UTC().Sub(time.UnixMilli(decision.snapshot.DecisionTimestampMs)); age < 0 || age > maxAutoDecisionAge {
				s.mu.Lock()
				s.autoStaleDecisions++
				s.logLocked("Stale auto decision dropped")
				s.mu.Unlock()
				continue
			}
			_, _ = s.ProcessFeatureSnapshotAt(decision.snapshot, decision.entryPrice)
		}
	}
}

// ProcessFeatureSnapshot is retained for deterministic fixtures. Live runtime
// passes the completed canonical bar close to ProcessFeatureSnapshotAt.
func (s *Server) ProcessFeatureSnapshot(snapshot featurev2.Snapshot) (autopipeline.Result, error) {
	s.mu.RLock()
	market := s.market
	s.mu.RUnlock()
	if market.UpdatedAt.IsZero() || market.UpdatedAt.After(time.UnixMilli(snapshot.DecisionTimestampMs).Add(time.Second)) {
		return autopipeline.Result{}, fmt.Errorf("INVALID_FEATURE_OR_MARKET_TIME")
	}
	return s.ProcessFeatureSnapshotAt(snapshot, market.FuturesPrice)
}

// ProcessFeatureSnapshotAt evaluates the frozen model/risk pipeline and, only
// when both Testnet order gates are explicitly armed, submits the resulting
// ExecutionIntent to Binance Futures Demo. Mainnet is never reachable here.
func (s *Server) ProcessFeatureSnapshotAt(snapshot featurev2.Snapshot, entryPrice float64) (autopipeline.Result, error) {
	s.autoLifecycleMu.Lock()
	defer s.autoLifecycleMu.Unlock()
	s.mu.RLock()
	running := s.states[TradingEnvironmentTestnet].AutoRunning
	s.mu.RUnlock()
	if !running || s.autoPipeline == nil || s.autoIntentSink == nil {
		return autopipeline.Result{}, fmt.Errorf("AUTO_NOT_RUNNING")
	}
	if !s.autoOrdersEnabled(TradingEnvironmentTestnet) {
		s.mu.Lock()
		s.states[TradingEnvironmentTestnet].AutoRunning = false
		s.states[TradingEnvironmentTestnet].AutoState = "BLOCKED"
		s.logLocked("Auto new entry blocked: AUTO_ORDERS_DISABLED")
		s.mu.Unlock()
		return autopipeline.Result{}, fmt.Errorf("NO_NEW_ENTRY: AUTO_ORDERS_DISABLED")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	holding, err := s.reconcileAutoOwnedPosition(ctx)
	if err != nil {
		return autopipeline.Result{}, err
	}
	if holding {
		return autopipeline.Result{}, fmt.Errorf("AUTO_POSITION_OPEN")
	}

	blockers := s.entryBlockers(TradingEnvironmentTestnet)
	for _, blocker := range blockers {
		if blocker.Code == "AUTO_TRADING_RUNNING" {
			continue
		}
		if transientAutoBlocker(blocker.Code) {
			s.mu.Lock()
			if s.states[TradingEnvironmentTestnet].AutoRunning {
				s.states[TradingEnvironmentTestnet].AutoState = "BLOCKED"
			}
			s.logLocked("Auto entry temporarily blocked: " + blocker.Code)
			s.mu.Unlock()
			return autopipeline.Result{}, fmt.Errorf("NO_NEW_ENTRY: %s", blocker.Code)
		}
		s.mu.Lock()
		s.states[TradingEnvironmentTestnet].AutoRunning = false
		s.states[TradingEnvironmentTestnet].AutoState = "BLOCKED"
		s.logLocked("Auto new entry blocked: " + blocker.Code)
		s.mu.Unlock()
		return autopipeline.Result{}, fmt.Errorf("NO_NEW_ENTRY: %s", blocker.Code)
	}

	s.mu.RLock()
	account := s.states[TradingEnvironmentTestnet].Balance
	s.mu.RUnlock()
	if account.MarginUSDT == nil || snapshot.DecisionTimestampMs <= 0 || snapshot.DecisionTimestampMs > time.Now().UnixMilli()+1000 || entryPrice <= 0 || math.IsNaN(entryPrice) || math.IsInf(entryPrice, 0) {
		return autopipeline.Result{}, fmt.Errorf("INVALID_FEATURE_OR_MARKET_TIME")
	}
	started := time.Now()
	result, err := s.autoPipeline.Evaluate(snapshot, *account.MarginUSDT, entryPrice)
	elapsedUs := float64(time.Since(started).Nanoseconds()) / 1000
	if err != nil {
		s.mu.Lock()
		s.states[TradingEnvironmentTestnet].AutoRunning = false
		s.states[TradingEnvironmentTestnet].AutoState = "ERROR"
		s.apiErrors++
		s.mu.Unlock()
		return autopipeline.Result{}, err
	}

	s.mu.Lock()
	if !s.states[TradingEnvironmentTestnet].AutoRunning || s.states[TradingEnvironmentTestnet].KillSwitch {
		s.mu.Unlock()
		return autopipeline.Result{}, fmt.Errorf("AUTO_STOPPED_BEFORE_INTENT")
	}
	s.states[TradingEnvironmentTestnet].AutoState = "RUNNING"
	s.pipelineLatency.add(elapsedUs)
	s.inferenceLatency.add(result.InferenceLatencyUs)
	s.policyLatency.add(result.PolicyLatencyUs)
	switch result.FinalSignal {
	case "LONG":
		s.longSignals++
	case "SHORT":
		s.shortSignals++
	default:
		s.noTradeSignals++
	}
	s.states[TradingEnvironmentTestnet].LastAutoResult = &result
	s.mu.Unlock()

	if result.Intent == nil {
		return result, nil
	}
	if err := s.autoIntentSink.Record(*result.Intent); err != nil {
		s.mu.Lock()
		s.states[TradingEnvironmentTestnet].AutoRunning = false
		s.states[TradingEnvironmentTestnet].AutoState = "ERROR"
		s.apiErrors++
		s.mu.Unlock()
		return autopipeline.Result{}, err
	}
	s.mu.Lock()
	s.autoIntents++
	s.mu.Unlock()
	if _, err := s.executeAutoIntent(ctx, *result.Intent); err != nil {
		return result, err
	}
	return result, nil
}
