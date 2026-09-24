package main

import (
	"binance_trader/internal/data/dataset"
	"binance_trader/internal/data/secondbar"
	mainoutcome "binance_trader/internal/outcome/main"
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
	input := flag.String("input", ".\\data\\derived\\1s", "secondbar root")
	sourceMan := flag.String("source-manifests", ".\\data\\manifests\\1s", "secondbar manifests")
	output := flag.String("output", ".\\data\\outcomes\\main\\v2", "outcome V2 root")
	outMan := flag.String("outcome-manifests", ".\\data\\manifests\\outcomes\\main\\v2", "outcome V2 manifests")
	symbol := flag.String("symbol", "BTCUSDT", "symbol")
	month := flag.String("month", "2024-01", "decision month")
	force := flag.Bool("force", false, "replace output")
	flag.Parse()
	started := time.Now()
	monthStart, e := time.Parse("2006-01", *month)
	if e != nil {
		return fmt.Errorf("invalid month: %w", e)
	}
	readStart := *month
	expectedStart := monthStart.UnixMilli() + 5000
	previous := monthStart.AddDate(0, -1, 0).Format("2006-01")
	previousPath := filepath.Join(*input, *symbol, previous[:4], fmt.Sprintf("%s-1s-%s.parquet", *symbol, previous))
	if _, statErr := os.Stat(previousPath); statErr == nil {
		readStart = previous
		expectedStart = monthStart.UnixMilli()
	}
	path := filepath.Join(*output, *symbol, (*month)[:4], fmt.Sprintf("%s-main-outcomes-v2-%s.parquet", *symbol, *month))
	w, e := mainoutcome.NewWriter(path, *force)
	if e != nil {
		return e
	}
	var selected int64
	engine := mainoutcome.NewEngine(func(o mainoutcome.MainOutcomeV1) error {
		if time.UnixMilli(o.DecisionTimestampMs).UTC().Format("2006-01") != *month {
			return nil
		}
		selected++
		return w.Write(o)
	})
	readEnd := nextMonth(*month)
	loaderStats, e := dataset.Stream(dataset.Options{Root: *input, ManifestRoot: *sourceMan, Symbol: *symbol, StartMonth: readStart, EndMonth: readEnd}, engine.Add)
	if e != nil {
		w.Abort()
		return e
	}
	engine.Flush()
	if e = w.Close(); e != nil {
		return e
	}
	info, e := os.Stat(path)
	if e != nil {
		return e
	}
	manifest := mainoutcome.Manifest{Symbol: *symbol, Month: *month, OutcomeVersion: mainoutcome.OutcomeVersion, SourceSecondBarSchemaVersion: secondbar.SchemaVersion, DecisionIntervalSeconds: 5, MaxHorizonSeconds: 14400, InputBars: loaderStats.OutputBars, DecisionCandidates: selected, RowCount: selected, StartDecisionTimestampMs: expectedStart, EndDecisionTimestampMs: lastDecision(*month), TailSkipped: 0, OutputSizeBytes: info.Size(), ElapsedMs: time.Since(started).Milliseconds()}
	mp := filepath.Join(*outMan, *symbol, fmt.Sprintf("%s-main-outcomes-v2-%s.json", *symbol, *month))
	if e = mainoutcome.WriteManifest(mp, manifest); e != nil {
		return e
	}
	fmt.Printf("Input bars: %d\nDecision candidates (%s): %d\nOutcome rows (%s): %d\nStart decision: %s\nEnd decision: %s\nOutput size: %d\nElapsed: %s\n", loaderStats.OutputBars, *month, selected, *month, selected, utc(expectedStart), utc(lastDecision(*month)), info.Size(), time.Since(started).Round(time.Millisecond))
	return nil
}
func nextMonth(m string) string {
	t, _ := time.Parse("2006-01", m)
	return t.AddDate(0, 1, 0).Format("2006-01")
}
func firstDecision(m string) int64 { t, _ := time.Parse("2006-01", m); return t.UnixMilli() + 5000 }
func lastDecision(m string) int64 {
	t, _ := time.Parse("2006-01", m)
	return t.AddDate(0, 1, 0).UnixMilli() - 5000
}
func utc(ms int64) string { return time.UnixMilli(ms).UTC().Format("2006-01-02 15:04:05 UTC") }
