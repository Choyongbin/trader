package main

import (
	"flag"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"time"

	"binance_trader/internal/data/secondbar"
)

func main() {
	if err := run(); err != nil {
		log.SetFlags(0)
		log.Fatal(err)
	}
}

func run() error {
	output := flag.String("output", ".\\data\\derived\\1s", "derived 1-second dataset root")
	manifests := flag.String("manifests", ".\\data\\manifests\\1s", "manifest root")
	symbol := flag.String("symbol", "BTCUSDT", "market symbol")
	start := flag.String("start", "2024-01", "first month YYYY-MM")
	end := flag.String("end", "2025-12", "last month YYYY-MM")
	flag.Parse()
	*symbol = strings.ToUpper(*symbol)
	started := time.Now()
	audit, err := secondbar.AuditDataset(*output, *manifests, *symbol, *start, *end, func(month string) { fmt.Printf("Auditing %s...\n", month) })
	if err != nil {
		return err
	}
	if fullCoverage(audit) {
		audit.ExpectedSecondBars = (audit.EndTimestampMs-audit.StartTimestampMs)/1000 + 1
		audit.MissingSecondBars = audit.ExpectedSecondBars - audit.TotalSecondBars
	}
	datasetPath := filepath.Join(*manifests, *symbol, fmt.Sprintf("dataset-%s-%s.json", (*start)[:4], (*end)[:4]))
	if err := secondbar.WriteDatasetManifest(datasetPath, audit); err != nil {
		return err
	}
	printAudit(audit, datasetPath, time.Since(started))
	return nil
}

func fullCoverage(a secondbar.DatasetManifest) bool {
	start, err := time.Parse("2006-01", a.StartMonth)
	if err != nil {
		return false
	}
	end, err := time.Parse("2006-01", a.EndMonth)
	if err != nil {
		return false
	}
	return a.StartTimestampMs == start.UnixMilli() && a.EndTimestampMs == end.AddDate(0, 1, 0).UnixMilli()-1000
}

func printAudit(a secondbar.DatasetManifest, manifestPath string, elapsed time.Duration) {
	fmt.Println("\nMONTH     RAW ROWS     SECOND BARS  TRADE SEC   NO-TRADE    SIZE BYTES   STATUS")
	for _, m := range a.Months {
		fmt.Printf("%-7s %12d %12d %10d %10d %13d   OK\n", m.Month, m.AggTradeRows, m.SecondBars, m.TradeSeconds, m.NoTradeSeconds, m.OutputSizeBytes)
	}
	fmt.Println("\nCROSS-MONTH BOUNDARIES")
	for _, b := range a.Boundaries {
		fmt.Printf("%s -> %s  timestamp_gap=%dms  id_delta=%d  price_gap=%+.8f (%+.6f%%)\n", b.PreviousMonth, b.NextMonth, b.TimestampGapMs, b.AggTradeIDDelta, b.PriceGap, b.PriceGapPercent)
	}
	line := "============================================================"
	status := "OK"
	if a.CrossMonthGaps > 0 || a.MissingSecondBars > 0 {
		status = "WARNING"
	}
	fmt.Printf("\n%s\n%s 1-SECOND DATASET\n\nPeriod                    : %s -> %s\nSource ZIP files           : %d\nTotal AggTrades            : %d\nTotal SecondBars           : %d\nExpected SecondBars        : %d\nMissing SecondBars         : %d\nTrade Seconds              : %d\nNo-Trade Seconds           : %d\nStart                      : %s\nEnd                        : %s\nCross-Month Timestamp Gaps : %d\nInvalid Months             : %d\nTotal Parquet Size         : %d bytes\nDataset Manifest           : %s\nAudit Elapsed              : %s\nStatus                     : %s\n%s\n", line, a.Symbol, a.StartMonth, a.EndMonth, a.FileCount, a.TotalAggTradeRows, a.TotalSecondBars, a.ExpectedSecondBars, a.MissingSecondBars, a.TotalTradeSeconds, a.TotalNoTradeSeconds, utc(a.StartTimestampMs), utc(a.EndTimestampMs), a.CrossMonthGaps, a.InvalidMonths, a.TotalParquetSizeBytes, manifestPath, elapsed.Round(time.Millisecond), status, line)
}

func utc(ms int64) string { return time.UnixMilli(ms).UTC().Format("2006-01-02 15:04:05 UTC") }
