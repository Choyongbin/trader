package uiapi

import (
	"net/http"
	"time"
)

type EntryBlocker struct {
	Code        string    `json:"code"`
	Description string    `json:"description"`
	Since       time.Time `json:"since"`
	Active      bool      `json:"active"`
}

func (s *Server) entryBlockers(environment TradingEnvironment) []EntryBlocker {
	s.mu.RLock()
	state := s.states[environment]
	accountConnected := state.Balance.Connected
	exchangeKnown := state.ExchangeKnown
	positionOpen := len(state.Positions) != 0
	ordersOpen := len(state.Orders) != 0
	killSwitch := state.KillSwitch
	autoRunning := state.AutoRunning
	market := s.market
	futuresAt, spotAt, markAt := s.marketSourceUpdated["futures"], s.marketSourceUpdated["spot"], s.marketSourceUpdated["mark"]
	brokerReady := s.testnet != nil
	s.mu.RUnlock()
	now := s.now().UTC()
	add := func(rows []EntryBlocker, code, description string) []EntryBlocker {
		return append(rows, EntryBlocker{Code: code, Description: description, Active: true})
	}
	var rows []EntryBlocker
	if environment != TradingEnvironmentTestnet {
		rows = add(rows, "BROKER_UNAVAILABLE", "Mainnet execution is disabled")
	}
	if !s.warmup()["ready"].(bool) {
		rows = add(rows, "FEATURE_WARMUP", "Continuous live feature history is not ready")
	}
	if s.autoPipeline == nil || !s.autoFeatureSource {
		rows = add(rows, "FEATURE_SOURCE_UNAVAILABLE", "Frozen model or live Feature V2 source is unavailable")
	}
	if !market.Connected || now.Sub(market.UpdatedAt) > 10*time.Second {
		rows = add(rows, "MARKET_DATA_STALE", "Public market data is disconnected or stale")
	}
	if futuresAt.IsZero() || now.Sub(futuresAt) > 10*time.Second {
		rows = add(rows, "FUTURES_WS_DISCONNECTED", "Futures market stream is unavailable or stale")
	}
	if spotAt.IsZero() || now.Sub(spotAt) > 10*time.Second {
		rows = add(rows, "SPOT_WS_DISCONNECTED", "Spot market stream is unavailable or stale")
	}
	if markAt.IsZero() || now.Sub(markAt) > 10*time.Second {
		rows = add(rows, "REQUIRED_SOURCE_STALE", "Mark market stream is unavailable or stale")
	}
	if !accountConnected {
		rows = add(rows, "ACCOUNT_UNAVAILABLE", "Futures account is not connected")
	}
	if environment == TradingEnvironmentTestnet && !brokerReady {
		rows = add(rows, "BROKER_UNAVAILABLE", "Demo broker is not available")
	}
	if environment == TradingEnvironmentTestnet && brokerReady && !exchangeKnown {
		rows = add(rows, "EXCHANGE_STATE_UNKNOWN", "Position and order state could not be verified")
	}
	if positionOpen {
		rows = add(rows, "POSITION_RECONCILIATION_REQUIRED", "An open futures position exists")
		rows = add(rows, "MANUAL_POSITION_OPEN", "Manual position blocks automatic entries")
	}
	if ordersOpen {
		rows = add(rows, "ORDER_RECONCILIATION_REQUIRED", "Open orders require reconciliation")
	}
	if killSwitch {
		rows = add(rows, "KILL_SWITCH_ACTIVE", "New entries are blocked by emergency stop")
	}
	if autoRunning {
		rows = add(rows, "AUTO_TRADING_RUNNING", "A second auto session cannot start")
	}
	if !s.ordersEnabled(environment) {
		rows = add(rows, "ORDERS_DISABLED", "Order enable flag is off")
	}
	s.mu.Lock()
	since := s.blockerSince[environment]
	if since == nil {
		since = map[string]time.Time{}
		s.blockerSince[environment] = since
	}
	active := make(map[string]bool, len(rows))
	for i := range rows {
		active[rows[i].Code] = true
		if since[rows[i].Code].IsZero() {
			since[rows[i].Code] = now
			s.logLocked("Entry blocker active: " + rows[i].Code)
			if rows[i].Code == "MARKET_DATA_STALE" || rows[i].Code == "REQUIRED_SOURCE_STALE" {
				s.sourceStaleEvents++
			}
		}
		rows[i].Since = since[rows[i].Code]
	}
	for code := range since {
		if !active[code] {
			delete(since, code)
		}
	}
	s.mu.Unlock()
	return rows
}

func (s *Server) getEntryBlockers(w http.ResponseWriter, r *http.Request) {
	environment, ok := environmentFromRequest(w, r)
	if !ok {
		return
	}
	if environment == TradingEnvironmentTestnet && s.testnet != nil {
		s.refreshTestnet(r.Context())
	}
	rows := s.entryBlockers(environment)
	if rows == nil {
		rows = []EntryBlocker{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"environment": environment, "no_new_entry": len(rows) != 0, "blockers": rows})
}
