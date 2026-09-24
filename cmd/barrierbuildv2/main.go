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
	history := flag.String("history", ".\\history", "raw aggTrades root")
	out := flag.String("output", ".\\data\\barriers\\main\\v2", "V2 root")
	mans := flag.String("manifests", ".\\data\\manifests\\barriers\\main\\v2", "manifest root")
	symbol := flag.String("symbol", "BTCUSDT", "symbol")
	month := flag.String("month", "2024-01", "UTC decision month")
	delay := flag.Int64("entry-delay-ms", 0, "entry-reference delay")
	entryWait := flag.Int64("max-entry-wait-ms", 1000, "entry-reference wait")
	exitWait := flag.Int64("max-exit-reference-wait-ms", 1000, "timeout-reference wait")
	force := flag.Bool("force", false, "replace V2 output")
	flag.Parse()
	t0, e := time.Parse("2006-01", *month)
	if e != nil {
		return e
	}
	t1 := t0.AddDate(0, 1, 0)
	start, end := t0.UnixMilli()+5000, t1.UnixMilli()-5000
	previous := t0.AddDate(0, -1, 0).Format("2006-01")
	if _, statErr := os.Stat(filepath.Join(*history, fmt.Sprintf("%s-aggTrades-%s.zip", *symbol, previous))); statErr == nil {
		start = t0.UnixMilli()
	}
	profile := fmt.Sprintf("delay_%dms", *delay)
	path := filepath.Join(*out, profile, *symbol, t0.Format("2006"), fmt.Sprintf("%s-main-barriers-v2-%s-%s.parquet", *symbol, *month, profile))
	w, e := mainbarrier.NewWriterV2(path, *force)
	if e != nil {
		return e
	}
	started := time.Now()
	eng, e := mainbarrier.NewEngineV2(mainbarrier.ConfigV2{StartDecisionTimestampMs: start, EndDecisionTimestampMs: end, DecisionIntervalMs: 5000, EntryDelayMs: *delay, MaxEntryWaitMs: *entryWait, MaxExitReferenceWaitMs: *exitWait, MaxHorizonSeconds: mainbarrier.MaxHorizonSeconds}, w.Write)
	if e != nil {
		w.Abort()
		return e
	}
	files := []string{filepath.Join(*history, fmt.Sprintf("%s-aggTrades-%s.zip", *symbol, *month)), filepath.Join(*history, fmt.Sprintf("%s-aggTrades-%s.zip", *symbol, t1.Format("2006-01")))}
	var trades int64
	for _, p := range files {
		s, _, er := aggtrade.ReadArchive(p, aggtrade.ReadOptions{ProgressInterval: 5000000, Progress: func(n int64) { fmt.Printf("%s: %d rows\n", filepath.Base(p), n) }, OnTrade: func(a market.AggTrade) error {
			if er := eng.Add(a); er != nil {
				return er
			}
			if eng.Complete() {
				return aggtrade.ErrStop
			}
			return nil
		}})
		trades += s.Rows
		if er != nil {
			w.Abort()
			return er
		}
		if eng.Complete() {
			break
		}
	}
	if e = eng.Flush(); e != nil {
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
	st := eng.Stats()
	m := mainbarrier.ManifestV2{BarrierVersion: 2, Symbol: *symbol, Month: *month, PartitionKey: "decision_timestamp_ms", PartitionTimezone: "UTC", Source: "Binance USD-M Futures aggTrades", TriggerPriceSource: mainbarrier.TriggerPriceSourceContractPrice, EntryPriceSemantics: mainbarrier.EntryPriceSemantics, BarrierSemantics: "reference-entry-relative first passage", HorizonOrigin: "entry_reference_timestamp_ms", FundingIncluded: false, EntryDelayMs: *delay, MaxEntryWaitMs: *entryWait, MaxExitReferenceWaitMs: *exitWait, MaxHorizonSeconds: mainbarrier.MaxHorizonSeconds, BarrierGridBps: append([]int(nil), mainbarrier.BarrierGridBps[:]...), TimeoutHorizonsSeconds: append([]int(nil), mainbarrier.TimeoutHorizonsSeconds[:]...), RowCount: st.Rows, StartDecisionTimestampMs: start, EndDecisionTimestampMs: end, EntryReferenceAvailable: st.EntryReferenceAvailable, EntryReferenceUnavailable: st.EntryReferenceUnavailable, TimeoutReferenceAvailable: st.TimeoutReferenceAvailable, TimeoutReferenceUnavailable: st.TimeoutReferenceUnavailable, SourceFiles: files, SourceTrades: trades, OutputSizeBytes: info.Size(), ElapsedMs: time.Since(started).Milliseconds()}
	mp := filepath.Join(*mans, profile, *symbol, fmt.Sprintf("%s-main-barriers-v2-%s-%s.json", *symbol, *month, profile))
	if e = mainbarrier.WriteManifestV2(mp, m); e != nil {
		return e
	}
	fmt.Printf("Rows: %d\nEntry reference available/unavailable: %d / %d\nOutput: %s\nSize: %d\nElapsed: %s\n", st.Rows, st.EntryReferenceAvailable, st.EntryReferenceUnavailable, path, info.Size(), time.Since(started).Round(time.Millisecond))
	return nil
}
