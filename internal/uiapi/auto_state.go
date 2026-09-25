package uiapi

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const autoExecutionStateVersion = 2

var autoStateAtomicReplace = replaceAutoExecutionStateAtomic

type autoExecutionState struct {
	Version                       int                `json:"version"`
	Environment                   TradingEnvironment `json:"environment"`
	Symbol                        string             `json:"symbol"`
	CandidateID                   string             `json:"candidate_id"`
	DecisionTimestampMs           int64              `json:"decision_timestamp_ms"`
	EntryClientOrderID            string             `json:"entry_client_order_id"`
	EntrySide                     string             `json:"entry_side"`
	EntryRequestedQuantity        float64            `json:"entry_requested_quantity,omitempty"`
	EntryNormalizedQuantity       string             `json:"entry_normalized_quantity,omitempty"`
	EntryFilledQuantity           float64            `json:"entry_filled_quantity"`
	EntryFillPrice                float64            `json:"entry_fill_price"`
	EntryFillTimestampMs          int64              `json:"entry_fill_timestamp_ms"`
	TPClientAlgoID                string             `json:"tp_client_algo_id"`
	SLClientAlgoID                string             `json:"sl_client_algo_id"`
	TPTriggerPrice                float64            `json:"tp_trigger_price,omitempty"`
	SLTriggerPrice                float64            `json:"sl_trigger_price,omitempty"`
	ProtectiveQuantity            float64            `json:"protective_quantity,omitempty"`
	HorizonSeconds                int                `json:"horizon_seconds,omitempty"`
	HorizonDeadlineMs             int64              `json:"horizon_deadline_ms"`
	HorizonCloseRequestID         string             `json:"horizon_close_request_id"`
	ExecutionState                string             `json:"execution_state"`
	LastReconciliationTimestampMs int64              `json:"last_reconciliation_timestamp_ms"`
}

func (s *Server) persistAutoExecutionState(state autoExecutionState) error {
	state.Version = autoExecutionStateVersion
	state.Environment = TradingEnvironmentTestnet
	if state.Symbol == "" {
		state.Symbol = "BTCUSDT"
	}
	if state.ExecutionState != "FLAT" {
		if err := state.validateForRecovery(); err != nil {
			return fmt.Errorf("invalid auto execution state: %w", err)
		}
	}
	if s.autoExecutionStatePath == "" {
		s.autoExecutionState = &state
		return nil
	}
	encoded, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(s.autoExecutionStatePath), 0700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.autoExecutionStatePath), ".auto-execution-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	ok := false
	defer func() {
		_ = tmp.Close()
		if !ok {
			_ = os.Remove(tmpName)
		}
	}()
	if err = tmp.Chmod(0600); err == nil {
		_, err = tmp.Write(encoded)
	}
	if err == nil {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = autoStateAtomicReplace(tmpName, s.autoExecutionStatePath)
	}
	if err != nil {
		return err
	}
	ok = true
	s.autoExecutionState = &state
	return nil
}

func (s *Server) persistConfirmedFlatAutoExecutionState() error {
	return s.persistAutoExecutionState(autoExecutionState{
		ExecutionState:                "FLAT",
		LastReconciliationTimestampMs: s.now().UTC().UnixMilli(),
	})
}

func (s *Server) loadAutoExecutionState() {
	if s.autoExecutionStatePath == "" {
		return
	}
	data, err := os.ReadFile(s.autoExecutionStatePath)
	if os.IsNotExist(err) {
		return
	}
	var state autoExecutionState
	if err != nil || json.Unmarshal(data, &state) != nil || state.Version != autoExecutionStateVersion || state.Environment != TradingEnvironmentTestnet || state.Symbol != "BTCUSDT" {
		s.autoRecoveryBlocked = true
		s.states[TradingEnvironmentTestnet].AutoState = "UNKNOWN_EXECUTION_STATE"
		s.logLocked("Auto execution state is corrupt or has incompatible identity; operator intervention required")
		return
	}
	if state.ExecutionState != "FLAT" {
		if err = state.validateForRecovery(); err != nil {
			s.autoRecoveryBlocked = true
			s.states[TradingEnvironmentTestnet].AutoState = "UNKNOWN_EXECUTION_STATE"
			s.logLocked("Auto execution state is incomplete; operator intervention required")
			return
		}
	}
	s.autoExecutionState = &state
	if state.ExecutionState == "FLAT" {
		return
	}
	s.autoOwnedPosition = true
	s.autoProtectiveIDs = nonEmptyStrings(state.TPClientAlgoID, state.SLClientAlgoID)
	s.autoExitDeadline = time.UnixMilli(state.HorizonDeadlineMs).UTC()
	s.autoCloseRequestID = state.HorizonCloseRequestID
	s.states[TradingEnvironmentTestnet].AutoState = "UNKNOWN_EXECUTION_STATE"
	s.autoRecoveryBlocked = true
	s.logLocked("Persisted auto execution requires exchange reconciliation; auto start remains disabled")
}

func nonEmptyStrings(values ...string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value != "" {
			result = append(result, value)
		}
	}
	return result
}

func (s autoExecutionState) validateForRecovery() error {
	if s.EntrySide != "LONG" && s.EntrySide != "SHORT" {
		return fmt.Errorf("invalid entry side")
	}
	if s.EntryClientOrderID == "" || s.HorizonCloseRequestID == "" || s.EntryNormalizedQuantity == "" || s.HorizonSeconds <= 0 || (s.EntryFilledQuantity <= 0 && s.EntryRequestedQuantity <= 0) {
		return fmt.Errorf("incomplete entry ownership")
	}
	if s.ExecutionState == "POSITION_PROTECTED" && (s.EntryFilledQuantity <= 0 || s.EntryFillTimestampMs <= 0 || s.HorizonDeadlineMs <= 0 || s.TPClientAlgoID == "" || s.SLClientAlgoID == "" || s.TPTriggerPrice <= 0 || s.SLTriggerPrice <= 0 || s.ProtectiveQuantity <= 0) {
		return fmt.Errorf("incomplete protected position")
	}
	return nil
}
