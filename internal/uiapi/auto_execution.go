package uiapi

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"binance_trader/internal/live/autopipeline"
	"binance_trader/internal/live/binance"
)

func (s *Server) autoOrdersEnabled(environment TradingEnvironment) bool {
	if environment != TradingEnvironmentTestnet || !s.ordersEnabled(environment) {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(os.Getenv("BINANCE_TESTNET_ENABLE_AUTO_ORDERS")), "true")
}

func (s *Server) autoBackend() (AutoTestnetBackend, bool) {
	backend, ok := s.testnet.(AutoTestnetBackend)
	return backend, ok
}

func transientAutoBlocker(code string) bool {
	switch code {
	case "FEATURE_WARMUP", "FEATURE_SOURCE_UNAVAILABLE", "MARKET_DATA_STALE", "FUTURES_WS_DISCONNECTED", "SPOT_WS_DISCONNECTED", "REQUIRED_SOURCE_STALE", "ACCOUNT_UNAVAILABLE", "EXCHANGE_STATE_UNKNOWN":
		return true
	default:
		return false
	}
}

// reconcileAutoOwnedPosition keeps a running automatic session single-position.
// While the owned position is open it blocks new entries without stopping the
// session. After TP/SL closes the position, only the sibling protective orders
// created by that automatic entry are canceled before the session may re-enter.
func (s *Server) reconcileAutoOwnedPosition(ctx context.Context) (bool, error) {
	s.refreshTestnetForce(ctx)
	s.mu.RLock()
	owned := s.autoOwnedPosition
	ids := append([]string(nil), s.autoProtectiveIDs...)
	positions := append([]Position(nil), s.states[TradingEnvironmentTestnet].Positions...)
	orders := append([]Order(nil), s.states[TradingEnvironmentTestnet].Orders...)
	exchangeKnown := s.states[TradingEnvironmentTestnet].ExchangeKnown
	running := s.states[TradingEnvironmentTestnet].AutoRunning
	exitDeadline := s.autoExitDeadline
	closeRequestID := s.autoCloseRequestID
	s.mu.RUnlock()
	if !owned {
		return false, nil
	}
	if !exchangeKnown {
		return true, fmt.Errorf("AUTO_EXCHANGE_STATE_UNKNOWN")
	}
	if len(positions) != 0 {
		if !exitDeadline.IsZero() && !s.now().UTC().Before(exitDeadline) {
			if closeRequestID == "" {
				return true, fmt.Errorf("AUTO_HORIZON_CLOSE_ID_MISSING")
			}
			if _, err := s.testnet.ClosePosition(ctx, closeRequestID); err != nil {
				s.mu.Lock()
				s.states[TradingEnvironmentTestnet].AutoRunning = false
				s.states[TradingEnvironmentTestnet].AutoState = "ERROR"
				s.logLocked("Auto horizon exit failed; new entries disabled")
				s.mu.Unlock()
				return true, fmt.Errorf("AUTO_HORIZON_EXIT_FAILED: %w", err)
			}
			s.refreshTestnetForce(ctx)
			s.mu.Lock()
			state := s.states[TradingEnvironmentTestnet]
			if !state.ExchangeKnown || len(state.Positions) != 0 || len(state.Orders) != 0 {
				state.AutoRunning = false
				state.AutoState = "ERROR"
				s.logLocked("Auto horizon exit reconciliation failed")
				s.mu.Unlock()
				return true, fmt.Errorf("AUTO_HORIZON_RECONCILIATION_FAILED")
			}
			s.autoOwnedPosition = false
			s.autoProtectiveIDs = nil
			s.autoExitDeadline = time.Time{}
			s.autoCloseRequestID = ""
			if state.AutoRunning {
				state.AutoState = "RUNNING"
			}
			s.logLocked("Auto holding horizon exit completed")
			s.mu.Unlock()
			return false, nil
		}
		seen := map[string]bool{}
		for _, order := range orders {
			seen[order.ClientOrderID] = true
		}
		for _, id := range ids {
			if id != "" && !seen[id] {
				s.mu.Lock()
				s.states[TradingEnvironmentTestnet].AutoRunning = false
				s.states[TradingEnvironmentTestnet].AutoState = "ERROR"
				s.logLocked("Auto protective order missing; new entries disabled")
				s.mu.Unlock()
				return true, fmt.Errorf("AUTO_PROTECTIVE_ORDER_MISSING")
			}
		}
		s.mu.Lock()
		if running {
			s.states[TradingEnvironmentTestnet].AutoState = "POSITION_OPEN"
		}
		s.mu.Unlock()
		return true, nil
	}

	backend, ok := s.autoBackend()
	if !ok {
		return true, fmt.Errorf("AUTO_BROKER_UNAVAILABLE")
	}
	if err := backend.CleanupAutoProtective(ctx, ids); err != nil {
		return true, fmt.Errorf("AUTO_PROTECTIVE_CLEANUP_FAILED: %w", err)
	}
	s.refreshTestnetForce(ctx)
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.states[TradingEnvironmentTestnet]
	if !state.ExchangeKnown || len(state.Positions) != 0 || len(state.Orders) != 0 {
		state.AutoRunning = false
		state.AutoState = "ERROR"
		s.logLocked("Auto flat-state reconciliation failed; new entries disabled")
		return true, fmt.Errorf("AUTO_FLAT_RECONCILIATION_FAILED")
	}
	s.autoOwnedPosition = false
	s.autoProtectiveIDs = nil
	s.autoExitDeadline = time.Time{}
	s.autoCloseRequestID = ""
	if state.AutoRunning {
		state.AutoState = "RUNNING"
		s.logLocked("Auto position closed and protective orders reconciled")
	}
	return false, nil
}

func (s *Server) executeAutoIntent(ctx context.Context, intent autopipeline.ExecutionIntent) (ManualExecution, error) {
	s.autoExecutionMu.Lock()
	defer s.autoExecutionMu.Unlock()

	backend, ok := s.autoBackend()
	if !ok {
		return ManualExecution{}, fmt.Errorf("AUTO_BROKER_UNAVAILABLE")
	}
	if !s.autoOrdersEnabled(TradingEnvironmentTestnet) {
		return ManualExecution{}, fmt.Errorf("AUTO_ORDERS_DISABLED")
	}
	s.refreshTestnetForce(ctx)
	s.mu.Lock()
	state := s.states[TradingEnvironmentTestnet]
	if !state.AutoRunning || state.KillSwitch {
		s.mu.Unlock()
		return ManualExecution{}, fmt.Errorf("AUTO_STOPPED_BEFORE_SUBMIT")
	}
	if !state.ExchangeKnown || !state.Balance.Connected || len(state.Positions) != 0 || len(state.Orders) != 0 || s.autoOwnedPosition {
		s.mu.Unlock()
		return ManualExecution{}, fmt.Errorf("AUTO_EXCHANGE_STATE_NOT_FLAT")
	}
	if !s.credentialAvailable(TradingEnvironmentTestnet) || !s.ordersEnabled(TradingEnvironmentTestnet) {
		s.mu.Unlock()
		return ManualExecution{}, fmt.Errorf("AUTO_ORDER_SAFETY_GATE")
	}
	state.AutoState = "SUBMITTING"
	s.actualOrderSubmits++
	s.mu.Unlock()

	execution, err := backend.SubmitAuto(ctx, intent)
	if err != nil {
		s.refreshTestnetForce(ctx)
		s.mu.Lock()
		state = s.states[TradingEnvironmentTestnet]
		state.AutoRunning = false
		state.AutoState = "ERROR"
		s.apiErrors++
		s.logLocked("Demo auto execution failed: " + err.Error())
		s.mu.Unlock()
		return ManualExecution{}, err
	}

	protectiveIDs := make([]string, 0, len(execution.Protective))
	for _, order := range execution.Protective {
		if order.ClientOrderID != "" {
			protectiveIDs = append(protectiveIDs, order.ClientOrderID)
		}
	}
	s.mu.Lock()
	state = s.states[TradingEnvironmentTestnet]
	state.Processed[execution.Order.ClientOrderID] = execution.Order
	state.Orders = append([]Order{execution.Order}, execution.Protective...)
	state.Positions = []Position{execution.Position}
	state.ExchangeKnown = true
	state.AutoState = "POSITION_OPEN"
	s.autoOwnedPosition = true
	s.autoProtectiveIDs = protectiveIDs
	s.autoExitDeadline = execution.Order.Time.Add(time.Duration(intent.HorizonSeconds) * time.Second)
	s.autoCloseRequestID = binance.DeterministicClientOrderID("auto", intent.DecisionTimestampMs, intent.CandidateID, "horizon", 0)
	s.logLocked("Demo auto entry accepted: " + intent.Side + " " + intent.CandidateID)
	s.mu.Unlock()
	return execution, nil
}

// RunAutoReconciliation monitors only positions created by the automatic Demo
// path. It does not auto-resume across process restarts and never touches
// Mainnet. The loop mainly removes the sibling TP/SL order after an exchange
// exit and keeps the next-entry gate synchronized with the exchange.
func (s *Server) RunAutoReconciliation(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.mu.RLock()
			owned := s.autoOwnedPosition
			s.mu.RUnlock()
			if !owned {
				continue
			}
			checkCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
			_, err := s.reconcileAutoOwnedPosition(checkCtx)
			cancel()
			if err != nil {
				s.mu.Lock()
				if s.states[TradingEnvironmentTestnet].AutoRunning && s.states[TradingEnvironmentTestnet].AutoState != "ERROR" {
					s.states[TradingEnvironmentTestnet].AutoState = "BLOCKED"
				}
				s.logLocked("Auto reconciliation blocked: " + err.Error())
				s.mu.Unlock()
			}
		}
	}
}
