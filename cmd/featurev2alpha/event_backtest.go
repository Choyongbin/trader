package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"time"

	mainbarrier "binance_trader/internal/barrier/main"
	featurev2 "binance_trader/internal/feature/main/v2"
	"binance_trader/internal/tradelabel"
	"github.com/parquet-go/parquet-go"
)

type barrierCursor struct {
	paths  []string
	pathAt int
	file   *os.File
	reader *parquet.GenericReader[mainbarrier.BarrierOutcomeV2]
	buf    []mainbarrier.BarrierOutcomeV2
	at, n  int
	row    mainbarrier.BarrierOutcomeV2
	has    bool
}

func newBarrierCursor(paths []string) (*barrierCursor, error) {
	cursor := &barrierCursor{paths: paths, buf: make([]mainbarrier.BarrierOutcomeV2, 2048)}
	if err := cursor.advance(); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return cursor, nil
}

func (c *barrierCursor) advance() error {
	for {
		if c.at < c.n {
			c.row = c.buf[c.at]
			c.at++
			c.has = true
			return nil
		}
		if c.reader == nil {
			if c.pathAt == len(c.paths) {
				c.has = false
				return io.EOF
			}
			f, err := os.Open(c.paths[c.pathAt])
			if err != nil {
				return err
			}
			c.pathAt++
			c.file, c.reader = f, parquet.NewGenericReader[mainbarrier.BarrierOutcomeV2](f)
		}
		n, err := c.reader.Read(c.buf)
		c.at, c.n = 0, n
		if errors.Is(err, io.EOF) {
			_ = c.reader.Close()
			_ = c.file.Close()
			c.reader, c.file = nil, nil
			if n == 0 {
				continue
			}
		} else if err != nil {
			return err
		}
	}
}

func (c *barrierCursor) match(timestamp int64) (mainbarrier.BarrierOutcomeV2, bool, error) {
	for c.has && c.row.DecisionTimestampMs < timestamp {
		if err := c.advance(); err != nil && !errors.Is(err, io.EOF) {
			return mainbarrier.BarrierOutcomeV2{}, false, err
		}
	}
	if !c.has || c.row.DecisionTimestampMs != timestamp {
		return mainbarrier.BarrierOutcomeV2{}, false, nil
	}
	row := c.row
	if err := c.advance(); err != nil && !errors.Is(err, io.EOF) {
		return mainbarrier.BarrierOutcomeV2{}, false, err
	}
	return row, true, nil
}

func (c *barrierCursor) close() {
	if c.reader != nil {
		_ = c.reader.Close()
	}
	if c.file != nil {
		_ = c.file.Close()
	}
}

type eventTrade struct {
	CandidateID string  `json:"candidate_id"`
	Side        string  `json:"side"`
	DecisionMs  int64   `json:"decision_timestamp_ms"`
	EntryMs     int64   `json:"entry_timestamp_ms"`
	ExitMs      int64   `json:"exit_timestamp_ms"`
	HoldingMs   int64   `json:"holding_ms"`
	Result      string  `json:"trade_result"`
	NetReturn   float64 `json:"net_return_ex_funding"`
}

type eventMonth struct {
	Month            string  `json:"month"`
	Trades           int64   `json:"trades"`
	WinRate          float64 `json:"win_rate"`
	CompoundedReturn float64 `json:"compounded_return_ex_funding"`
	MaxDrawdown      float64 `json:"max_drawdown"`
}

type stageEventReport struct {
	Version                   int              `json:"version"`
	Stage                     string           `json:"stage"`
	Status                    string           `json:"status"`
	PolicyHash                string           `json:"policy_sha256"`
	TestVerdict               string           `json:"test_verdict"`
	PositionMode              string           `json:"position_mode"`
	NotionalBasis             string           `json:"notional_basis"`
	Trades                    int64            `json:"trades"`
	LongTrades                int64            `json:"long_trades"`
	ShortTrades               int64            `json:"short_trades"`
	CandidateTrades           map[string]int64 `json:"candidate_trades"`
	WinRate                   float64          `json:"win_rate"`
	MeanTradeReturn           float64          `json:"mean_trade_return_ex_funding"`
	MedianTradeReturn         float64          `json:"median_trade_return_ex_funding"`
	CumulativeReturn          float64          `json:"cumulative_compounded_return_ex_funding"`
	MaxDrawdown               float64          `json:"max_drawdown"`
	AverageHoldingMs          float64          `json:"average_holding_ms"`
	MedianHoldingMs           float64          `json:"median_holding_ms"`
	ExposureRatio             float64          `json:"exposure_ratio"`
	TradesPerDay              float64          `json:"trades_per_day"`
	TPCount                   int64            `json:"tp_count"`
	SLCount                   int64            `json:"sl_count"`
	TimeoutCount              int64            `json:"timeout_count"`
	IgnoredWhileOpen          int64            `json:"decisions_ignored_while_open"`
	InvalidEntrySignals       int64            `json:"invalid_entry_signals"`
	Monthly                   []eventMonth     `json:"monthly"`
	FundingApplied            bool             `json:"funding_applied"`
	FundingWarning            string           `json:"funding_warning"`
	FeesSlippagePolicy        string           `json:"fees_slippage_policy"`
	BacktestVerdict           string           `json:"backtest_verdict"`
	ModelChangedAfterTest     bool             `json:"model_changed_after_test"`
	ThresholdChangedAfterTest bool             `json:"threshold_changed_after_test"`
	FinalHoldoutAccessed      bool             `json:"final_holdout_accessed"`
	ElapsedMs                 int64            `json:"elapsed_ms"`
	Complete                  bool             `json:"complete"`
}

func runEventBacktest(output string) error {
	started := time.Now()
	if b, err := os.ReadFile(output); err == nil {
		var existing stageEventReport
		if json.Unmarshal(b, &existing) == nil && existing.Complete && existing.Status == "PASS" {
			fmt.Printf("EVENT RESUME PASS verdict=%s\n", existing.BacktestVerdict)
			return nil
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	var confirmation stageEReport
	confirmationPath := filepath.FromSlash("data/reports/model/main/v2/phase9c-cde/stage-e-test-confirmation.json")
	if err := readJSONFile(confirmationPath, &confirmation); err != nil || !confirmation.Complete || confirmation.TestVerdict != "TEST_CONFIRMED" {
		return fmt.Errorf("event backtest requires TEST_CONFIRMED")
	}
	policyPath := filepath.FromSlash("data/reports/model/main/v2/phase9c-cde/stage-d-policy-freeze.json")
	policy, err := loadFrozenPolicy(policyPath)
	if err != nil || policy.PolicyHash != confirmation.PolicyHash {
		return fmt.Errorf("frozen policy mismatch")
	}
	configs := candidates()
	models := make([]baselineArtifact, len(configs))
	for i := range configs {
		if err = readJSONFile(policy.Candidates[i].ModelArtifactPath, &models[i]); err != nil {
			return err
		}
	}
	barriers, err := newBarrierCursor(barrierPaths())
	if err != nil {
		return err
	}
	defer barriers.close()
	cost := tradelabel.CostProfile{EntryFeeRate: .0004, TPExitFeeRate: .0004, SLExitFeeRate: .0004, TimeoutExitFeeRate: .0004,
		EntrySlippageBps: 1, TPExitSlippageBps: 1, SLExitSlippageBps: 1, TimeoutSlippageBps: 1}
	report := stageEventReport{Version: 1, Stage: "EVENT", Status: "PASS", PolicyHash: policy.PolicyHash, TestVerdict: confirmation.TestVerdict,
		PositionMode: "SINGLE_POSITION_FLAT_OR_OPEN", NotionalBasis: "1X_NOTIONAL_RETURN", CandidateTrades: map[string]int64{},
		FundingApplied: false, FundingWarning: "NOT_APPLIED: exact funding settlement notional and mark-price mapping is not preserved in candidate labels; no inferred funding adjustment",
		FeesSlippagePolicy: "TradeEvaluator V2 net_return_ex_funding used once; no double charge"}
	trades := make([]eventTrade, 0, 10000)
	var openUntil int64
	err = visitFeatures(testFeaturePaths(), func(feature featurev2.FeatureRowV2) error {
		barrier, matched, matchErr := barriers.match(feature.DecisionTimestampMs)
		if matchErr != nil {
			return matchErr
		}
		if !matched {
			return fmt.Errorf("missing Barrier V2 row at %d", feature.DecisionTimestampMs)
		}
		if feature.DecisionTimestampMs < openUntil {
			report.IgnoredWhileOpen++
			return nil
		}
		values := feature.FeatureValues()
		selected := -1
		bestReturn := math.Inf(-1)
		for i, model := range models {
			probability, predictedReturn, predictErr := model.predict(values)
			if predictErr != nil {
				return predictErr
			}
			frozen := policy.Candidates[i]
			if probability >= frozen.ClassificationThreshold && predictedReturn >= frozen.RegressionThreshold &&
				(predictedReturn > bestReturn || (predictedReturn == bestReturn && (selected < 0 || configs[i].ID < configs[selected].ID))) {
				selected, bestReturn = i, predictedReturn
			}
		}
		if selected < 0 {
			return nil
		}
		config := configs[selected]
		result, evaluateErr := tradelabel.EvaluateV2(barrier, tradelabel.TradeSpec{Side: tradelabel.Side(config.Side), TPBps: config.TP, SLBps: config.SL, HorizonSeconds: config.Horizon}, cost)
		if evaluateErr != nil {
			return evaluateErr
		}
		if !result.LabelValid {
			report.InvalidEntrySignals++
			return nil
		}
		trade := eventTrade{CandidateID: config.ID, Side: config.Side, DecisionMs: feature.DecisionTimestampMs,
			EntryMs: barrier.EntryReferenceTimestampMs, ExitMs: result.ExitTimestampMs, HoldingMs: result.ExitTimestampMs - barrier.EntryReferenceTimestampMs,
			Result: string(result.Status), NetReturn: result.NetReturnExFunding}
		if trade.HoldingMs < 0 || trade.ExitMs <= 0 {
			return fmt.Errorf("invalid holding interval at %d", feature.DecisionTimestampMs)
		}
		trades = append(trades, trade)
		openUntil = trade.ExitMs
		return nil
	})
	if err != nil {
		return fmt.Errorf("event backtest: %w", err)
	}
	finishEventReport(&report, trades)
	report.ElapsedMs, report.Complete = time.Since(started).Milliseconds(), true
	if err = writeJSON(output, report); err != nil {
		return err
	}
	fmt.Printf("EVENT BACKTEST PASS verdict=%s trades=%d cumulative=%.12g max_dd=%.12g\n", report.BacktestVerdict, report.Trades, report.CumulativeReturn, report.MaxDrawdown)
	return nil
}

func barrierPaths() []string {
	paths := make([]string, 0, 6)
	for month := 1; month <= 6; month++ {
		paths = append(paths, filepath.Join("data", "barriers", "main", "v2", "delay_0ms", "BTCUSDT", "2025", fmt.Sprintf("BTCUSDT-main-barriers-v2-2025-%02d-delay_0ms.parquet", month)))
	}
	return paths
}

func finishEventReport(report *stageEventReport, trades []eventTrade) {
	report.Trades = int64(len(trades))
	returns := make([]float64, len(trades))
	holdings := make([]float64, len(trades))
	equity, peak := 1.0, 1.0
	var wins int64
	var exposureMs int64
	for i, trade := range trades {
		returns[i], holdings[i] = trade.NetReturn, float64(trade.HoldingMs)
		report.MeanTradeReturn += trade.NetReturn
		report.AverageHoldingMs += float64(trade.HoldingMs)
		exposureMs += trade.HoldingMs
		equity *= 1 + trade.NetReturn
		if equity > peak {
			peak = equity
		}
		if drawdown := 1 - equity/peak; drawdown > report.MaxDrawdown {
			report.MaxDrawdown = drawdown
		}
		if trade.NetReturn > 0 {
			wins++
		}
		if trade.Side == "LONG" {
			report.LongTrades++
		} else {
			report.ShortTrades++
		}
		report.CandidateTrades[trade.CandidateID]++
		switch trade.Result {
		case "TP_FIRST":
			report.TPCount++
		case "SL_FIRST":
			report.SLCount++
		case "TIMEOUT":
			report.TimeoutCount++
		}
	}
	if len(trades) > 0 {
		report.WinRate = float64(wins) / float64(len(trades))
		report.MeanTradeReturn /= float64(len(trades))
		report.AverageHoldingMs /= float64(len(trades))
		report.MedianTradeReturn = median(returns)
		report.MedianHoldingMs = median(holdings)
	}
	report.CumulativeReturn = equity - 1
	testDurationMs := int64((181 * 24 * time.Hour) / time.Millisecond)
	report.ExposureRatio = float64(exposureMs) / float64(testDurationMs)
	report.TradesPerDay = float64(len(trades)) / 181
	for month := 1; month <= 6; month++ {
		monthly := make([]eventTrade, 0, len(trades)/6)
		for _, trade := range trades {
			at := time.UnixMilli(trade.EntryMs).UTC()
			if at.Year() == 2025 && int(at.Month()) == month {
				monthly = append(monthly, trade)
			}
		}
		report.Monthly = append(report.Monthly, eventMonthStats(fmt.Sprintf("2025-%02d", month), monthly))
	}
	if report.CumulativeReturn > 0 && report.MeanTradeReturn > 0 {
		report.BacktestVerdict = "BACKTEST_POSITIVE"
	} else if report.CumulativeReturn > 0 || report.MeanTradeReturn > 0 {
		report.BacktestVerdict = "BACKTEST_MIXED"
	} else {
		report.BacktestVerdict = "BACKTEST_NEGATIVE"
	}
}

func eventMonthStats(month string, trades []eventTrade) eventMonth {
	result := eventMonth{Month: month, Trades: int64(len(trades))}
	equity, peak := 1.0, 1.0
	var wins int64
	for _, trade := range trades {
		equity *= 1 + trade.NetReturn
		if equity > peak {
			peak = equity
		}
		if drawdown := 1 - equity/peak; drawdown > result.MaxDrawdown {
			result.MaxDrawdown = drawdown
		}
		if trade.NetReturn > 0 {
			wins++
		}
	}
	result.CompoundedReturn = equity - 1
	if len(trades) > 0 {
		result.WinRate = float64(wins) / float64(len(trades))
	}
	return result
}
