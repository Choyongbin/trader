package main

import (
	mainbarrier "binance_trader/internal/barrier/main"
	"binance_trader/internal/data/aggtrade"
	"binance_trader/internal/market"
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
	history := flag.String("history", ".\\history", "raw aggTrades ZIP root")
	out := flag.String("output", ".\\data\\barriers\\main\\v1", "barrier root")
	mans := flag.String("manifests", ".\\data\\manifests\\barriers\\main\\v1", "manifest root")
	symbol := flag.String("symbol", "BTCUSDT", "symbol")
	month := flag.String("month", "2024-01", "decision UTC month")
	delay := flag.Int64("entry-delay-ms", 0, "execution delay")
	wait := flag.Int64("max-entry-wait-ms", 1000, "maximum entry wait")
	force := flag.Bool("force", false, "replace output")
	flag.Parse()
	t0, e := time.Parse("2006-01", *month)
	if e != nil {
		return fmt.Errorf("month: %w", e)
	}
	t1 := t0.AddDate(0, 1, 0)
	start := t0.UnixMilli() + 5000
	end := t1.UnixMilli() - 5000
	profile := fmt.Sprintf("delay_%dms", *delay)
	path := filepath.Join(*out, profile, *symbol, t0.Format("2006"), fmt.Sprintf("%s-main-barriers-v1-%s-%s.parquet", *symbol, *month, profile))
	w, e := mainbarrier.NewWriter(path, *force)
	if e != nil {
		return e
	}
	started := time.Now()
	engine, e := mainbarrier.NewEngine(mainbarrier.Config{StartDecisionTimestampMs: start, EndDecisionTimestampMs: end, DecisionIntervalMs: 5000, EntryDelayMs: *delay, MaxEntryWaitMs: *wait, MaxHorizonSeconds: mainbarrier.MaxHorizonSeconds}, w.Write)
	if e != nil {
		w.Abort()
		return e
	}
	files := []string{filepath.Join(*history, fmt.Sprintf("%s-aggTrades-%s.zip", *symbol, *month)), filepath.Join(*history, fmt.Sprintf("%s-aggTrades-%s.zip", *symbol, t1.Format("2006-01")))}
	var trades int64
	for _, p := range files {
		s, _, er := aggtrade.ReadArchive(p, aggtrade.ReadOptions{ProgressInterval: 5000000, Progress: func(n int64) { fmt.Printf("%s: %d rows\n", filepath.Base(p), n) }, OnTrade: func(a market.AggTrade) error {
			if er := engine.Add(a); er != nil {
				return er
			}
			if engine.Complete() {
				return aggtrade.ErrStop
			}
			return nil
		}})
		trades += s.Rows
		if er != nil {
			w.Abort()
			return er
		}
		if engine.Complete() {
			break
		}
	}
	if e = engine.Flush(); e != nil {
		w.Abort()
		return e
	}
	if e = w.Close(); e != nil {
		return e
	}
	info, e := os.Stat(path)
	if e != nil {
		return e
	}
	st := engine.Stats()
	grid := append([]int(nil), mainbarrier.BarrierGridBps[:]...)
	m := mainbarrier.Manifest{BarrierVersion: mainbarrier.BarrierVersion, Symbol: *symbol, Month: *month, PartitionKey: "decision_timestamp_ms", PartitionTimezone: "UTC", Source: "Binance USD-M Futures aggTrades", EntryDelayMs: *delay, MaxEntryWaitMs: *wait, MaxHorizonSeconds: mainbarrier.MaxHorizonSeconds, BarrierGridBps: grid, RowCount: st.Rows, StartDecisionTimestampMs: start, EndDecisionTimestampMs: end, EntryAvailable: st.EntryAvailable, EntryUnavailable: st.EntryUnavailable, SourceFiles: files, SourceTrades: trades, OutputSizeBytes: info.Size(), ElapsedMs: time.Since(started).Milliseconds()}
	mp := filepath.Join(*mans, profile, *symbol, fmt.Sprintf("%s-main-barriers-v1-%s-%s.json", *symbol, *month, profile))
	if e = mainbarrier.WriteManifest(mp, m); e != nil {
		return e
	}
	fmt.Printf("Rows: %d\nEntry available: %d\nEntry unavailable: %d\nSource trades: %d\nOutput: %s\nOutput size: %d\nElapsed: %s\n", st.Rows, st.EntryAvailable, st.EntryUnavailable, trades, path, info.Size(), time.Since(started).Round(time.Millisecond))
	return nil
}
