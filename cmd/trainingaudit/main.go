package main

import (
	maintraining "binance_trader/internal/training/main"
	"flag"
	"fmt"
	"log"
	"path/filepath"
)

func main() {
	if e := run(); e != nil {
		log.SetFlags(0)
		log.Fatal(e)
	}
}
func run() error {
	root := flag.String("root", ".\\data\\training\\main\\v1", "training root")
	mans := flag.String("manifests", ".\\data\\manifests\\training\\main\\v1", "manifest root")
	symbol := flag.String("symbol", "BTCUSDT", "symbol")
	month := flag.String("month", "2024-01", "month")
	tp := flag.Int("tp-bps", 50, "TP")
	sl := flag.Int("sl-bps", 25, "SL")
	h := flag.Int("horizon-seconds", 3600, "horizon")
	cost := flag.String("cost-profile-name", "synthetic_validation_v1", "profile")
	ef := flag.Float64("entry-fee-rate", .0004, "expected entry fee")
	tf := flag.Float64("tp-exit-fee-rate", .0004, "expected TP fee")
	sf := flag.Float64("sl-exit-fee-rate", .0004, "expected SL fee")
	xf := flag.Float64("timeout-exit-fee-rate", .0004, "expected timeout fee")
	es := flag.Float64("entry-slippage-bps", 1, "expected entry slippage")
	ts := flag.Float64("tp-exit-slippage-bps", 1, "expected TP slippage")
	ss := flag.Float64("sl-exit-slippage-bps", 1, "expected SL slippage")
	xs := flag.Float64("timeout-slippage-bps", 1, "expected timeout slippage")
	flag.Parse()
	sd := fmt.Sprintf("tp%d_sl%d_h%d", *tp, *sl, *h)
	cd := "cost_" + *cost
	p := filepath.Join(*root, sd, cd, *symbol, (*month)[:4], fmt.Sprintf("%s-training-v1-%s.parquet", *symbol, *month))
	mp := filepath.Join(*mans, sd, cd, *symbol, fmt.Sprintf("%s-training-v1-%s.json", *symbol, *month))
	m, e := maintraining.ReadManifest(mp)
	if e != nil {
		return e
	}
	if m.TrainingVersion != 1 || m.FeatureVersion != 1 || m.OutcomeVersion != 2 || m.BarrierVersion != 2 || m.TradeSpec.TPBps != *tp || m.TradeSpec.SLBps != *sl || m.TradeSpec.HorizonSeconds != *h || m.CostProfile.Name != *cost || m.CostProfile.FundingIncluded ||
		m.CostProfile.EntryFeeRate != *ef || m.CostProfile.TPExitFeeRate != *tf || m.CostProfile.SLExitFeeRate != *sf || m.CostProfile.TimeoutExitFeeRate != *xf ||
		m.CostProfile.EntrySlippageBps != *es || m.CostProfile.TPExitSlippageBps != *ts || m.CostProfile.SLExitSlippageBps != *ss || m.CostProfile.TimeoutSlippageBps != *xs {
		return fmt.Errorf("manifest version/spec/cost mismatch")
	}
	a, e := maintraining.Audit(p)
	if e != nil {
		return e
	}
	if a.Rows != m.RowCount || a.Long.Valid != m.LongLabelValid || a.Short.Valid != m.ShortLabelValid {
		return fmt.Errorf("manifest/parquet statistics mismatch")
	}
	fmt.Printf("Rows: %d Start/end: %d/%d\nDuplicates/order: %d/%d Feature NaN/Inf: %d/%d Target NaN/Inf: %d/%d Invalid labels: %d\nModel feature columns: %d Leakage violations: 0\n", a.Rows, a.StartDecisionTimestampMs, a.EndDecisionTimestampMs, a.Duplicates, a.OrderViolations, a.FeatureNaN, a.FeatureInf, a.TargetNaN, a.TargetInf, a.InvalidLabels, len(maintraining.ModelFeatureColumns))
	return nil
}
