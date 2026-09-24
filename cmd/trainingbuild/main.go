package main

import (
	mainbarrier "binance_trader/internal/barrier/main"
	mainfeature "binance_trader/internal/feature/main"
	mainoutcome "binance_trader/internal/outcome/main"
	"binance_trader/internal/tradelabel"
	maintraining "binance_trader/internal/training/main"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

func main() {
	if e := run(); e != nil {
		log.SetFlags(0)
		log.Fatal(e)
	}
}
func run() error {
	features := flag.String("features", ".\\data\\features\\main\\v1", "Feature V1 root")
	outcomes := flag.String("outcomes", ".\\data\\outcomes\\main\\v2", "Outcome V2 root")
	barriers := flag.String("barriers", ".\\data\\barriers\\main\\v2", "Barrier V2 root")
	output := flag.String("output", ".\\data\\training\\main\\v1", "training root")
	manifests := flag.String("manifests", ".\\data\\manifests\\training\\main\\v1", "manifest root")
	symbol := flag.String("symbol", "BTCUSDT", "symbol")
	month := flag.String("month", "2024-01", "UTC month")
	tp := flag.Int("tp-bps", 50, "TP bps")
	sl := flag.Int("sl-bps", 25, "SL bps")
	h := flag.Int("horizon-seconds", 3600, "entry-based horizon")
	costName := flag.String("cost-profile-name", "synthetic_validation_v1", "explicit cost profile name")
	ef := flag.Float64("entry-fee-rate", .0004, "configured entry fee")
	tf := flag.Float64("tp-exit-fee-rate", .0004, "configured TP exit fee")
	sf := flag.Float64("sl-exit-fee-rate", .0004, "configured SL exit fee")
	xf := flag.Float64("timeout-exit-fee-rate", .0004, "configured timeout exit fee")
	es := flag.Float64("entry-slippage-bps", 1, "modeled entry slippage")
	ts := flag.Float64("tp-exit-slippage-bps", 1, "modeled TP slippage")
	ss := flag.Float64("sl-exit-slippage-bps", 1, "modeled SL slippage")
	xs := flag.Float64("timeout-slippage-bps", 1, "modeled timeout slippage")
	force := flag.Bool("force", false, "replace exact output")
	flag.Parse()
	if len(*month) != 7 {
		return fmt.Errorf("invalid month")
	}
	spec := tradelabel.TradeSpec{Side: tradelabel.Long, TPBps: *tp, SLBps: *sl, HorizonSeconds: *h}
	cost := tradelabel.CostProfile{EntryFeeRate: *ef, TPExitFeeRate: *tf, SLExitFeeRate: *sf, TimeoutExitFeeRate: *xf, EntrySlippageBps: *es, TPExitSlippageBps: *ts, SLExitSlippageBps: *ss, TimeoutSlippageBps: *xs}
	specDir := fmt.Sprintf("tp%d_sl%d_h%d", *tp, *sl, *h)
	costDir := "cost_" + *costName
	year := (*month)[:4]
	fp := filepath.Join(*features, *symbol, year, fmt.Sprintf("%s-main-features-v1-%s.parquet", *symbol, *month))
	op := filepath.Join(*outcomes, *symbol, year, fmt.Sprintf("%s-main-outcomes-v2-%s.parquet", *symbol, *month))
	bp := filepath.Join(*barriers, "delay_0ms", *symbol, year, fmt.Sprintf("%s-main-barriers-v2-%s-delay_0ms.parquet", *symbol, *month))
	out := filepath.Join(*output, specDir, costDir, *symbol, year, fmt.Sprintf("%s-training-v1-%s.parquet", *symbol, *month))
	w, e := maintraining.NewWriter(out, *force)
	if e != nil {
		return e
	}
	started := time.Now()
	st, e := maintraining.Build(maintraining.BuildConfig{FeaturePath: fp, OutcomePath: op, BarrierPath: bp, TradeSpec: spec, CostProfile: cost}, w.Write)
	if e != nil {
		w.Abort()
		return e
	}
	if e = w.CloseValidated(func(p string) error {
		a, er := maintraining.Audit(p)
		if er != nil {
			return er
		}
		if a.Rows != st.JoinedRows {
			return fmt.Errorf("temporary parquet rows %d != joined %d", a.Rows, st.JoinedRows)
		}
		return nil
	}); e != nil {
		return e
	}
	info, e := os.Stat(out)
	if e != nil {
		return e
	}
	elapsed := time.Since(started)
	m := maintraining.Manifest{TrainingVersion: 1, Symbol: *symbol, Month: *month, PartitionKey: "decision_timestamp_ms", PartitionTimezone: "UTC", FeatureVersion: mainfeature.FeatureVersion, OutcomeVersion: mainoutcome.OutcomeVersion, BarrierVersion: mainbarrier.BarrierVersionV2, TradeSpec: maintraining.TradeSpecManifest{TPBps: *tp, SLBps: *sl, HorizonSeconds: *h}, CostProfile: maintraining.CostProfileManifest{Name: *costName, EntryFeeRate: *ef, TPExitFeeRate: *tf, SLExitFeeRate: *sf, TimeoutExitFeeRate: *xf, EntrySlippageBps: *es, TPExitSlippageBps: *ts, SLExitSlippageBps: *ss, TimeoutSlippageBps: *xs, FundingIncluded: false}, RowCount: st.JoinedRows, LongLabelValid: st.Long.Valid, LongLabelInvalid: st.Long.Invalid, ShortLabelValid: st.Short.Valid, ShortLabelInvalid: st.Short.Invalid, LongNetProfitable: st.Long.NetProfitable, LongNetUnprofitable: st.Long.Valid - st.Long.NetProfitable, ShortNetProfitable: st.Short.NetProfitable, ShortNetUnprofitable: st.Short.Valid - st.Short.NetProfitable, StartDecisionTimestampMs: st.StartDecisionTimestampMs, EndDecisionTimestampMs: st.EndDecisionTimestampMs, ModelFeatureColumns: maintraining.ModelFeatureColumns, MetadataColumns: maintraining.MetadataColumns, TargetColumns: maintraining.TargetColumns, AnalysisColumns: maintraining.AnalysisColumns, OutputSizeBytes: info.Size(), ElapsedMs: elapsed.Milliseconds(), RowsPerSecond: float64(st.JoinedRows) / elapsed.Seconds()}
	mp := filepath.Join(*manifests, specDir, costDir, *symbol, fmt.Sprintf("%s-training-v1-%s.json", *symbol, *month))
	if e = maintraining.WriteManifest(mp, m); e != nil {
		return e
	}
	printStats(st, info.Size(), elapsed)
	return nil
}
func printStats(st maintraining.BuildStats, size int64, elapsed time.Duration) {
	side := func(name string, s maintraining.SideStats) {
		d := float64(s.Valid)
		if d == 0 {
			d = 1
		}
		fmt.Printf("%s valid/invalid: %d/%d TP/SL/TIMEOUT: %d/%d/%d gross/net profitable: %d/%d net_win_rate=%.6f mean_gross=%.9f mean_fee=%.9f mean_slippage=%.9f mean_net=%.9f gross_win_net_loss=%d\n", name, s.Valid, s.Invalid, s.TPFirst, s.SLFirst, s.Timeout, s.GrossProfitable, s.NetProfitable, float64(s.NetProfitable)/d, s.GrossSum/d, s.FeeSum/d, s.SlippageSum/d, s.NetSum/d, s.GrossWinNetLoss)
	}
	fmt.Printf("Feature/joined/training rows: %d/%d/%d\nStart/end: %s / %s\n", st.FeatureRows, st.JoinedRows, st.JoinedRows, time.UnixMilli(st.StartDecisionTimestampMs).UTC(), time.UnixMilli(st.EndDecisionTimestampMs).UTC())
	side("LONG", st.Long)
	side("SHORT", st.Short)
	fmt.Printf("Both/long-only/short-only/neither profitable (both valid): %d/%d/%d/%d\nOutput size: %d Elapsed: %s Rows/sec: %.1f\n", st.BothProfitable, st.LongOnlyProfitable, st.ShortOnlyProfitable, st.NeitherProfitable, size, elapsed.Round(time.Millisecond), float64(st.JoinedRows)/elapsed.Seconds())
}
