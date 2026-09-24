package main

import (
	"flag"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"time"

	"binance_trader/internal/data/aggtrade"
)

const progressInterval int64 = 10_000_000

func main() {
	if err := run(); err != nil {
		log.SetFlags(0)
		log.Fatal(err)
	}
}

func run() error {
	history := flag.String("history", ".\\history", "directory containing Binance monthly aggTrades ZIP files")
	month := flag.String("month", "", "optional YYYY-MM archive filter")
	flag.Parse()
	paths, err := aggtrade.Discover(*history)
	if err != nil {
		return err
	}
	if *month != "" {
		var filtered []string
		want := "-" + *month + ".zip"
		for _, p := range paths {
			if strings.HasSuffix(filepath.Base(p), want) {
				filtered = append(filtered, p)
			}
		}
		paths = filtered
		if len(paths) == 0 {
			return fmt.Errorf("no archive found for month %s", *month)
		}
	}
	absHistory, err := filepath.Abs(*history)
	if err != nil {
		return fmt.Errorf("resolve history path: %w", err)
	}
	fmt.Printf("Found %d BTCUSDT aggTrades archive(s)\n", len(paths))
	var total aggtrade.Stats
	var errorsCount int
	var firstTime, lastTime int64
	for i, path := range paths {
		name := filepath.Base(path)
		fmt.Printf("\n[%d/%d] %s\n", i+1, len(paths), name)
		stats, header, readErr := aggtrade.ReadArchive(path, aggtrade.ReadOptions{
			ProgressInterval: progressInterval,
			Progress:         func(rows int64) { fmt.Printf("%s: %s rows processed...\n", name, comma(rows)) },
		})
		printFile(name, stats, header, readErr)
		total.Rows += stats.Rows
		total.InvalidRows += stats.InvalidRows
		if firstTime == 0 || stats.StartTimeMs > 0 && stats.StartTimeMs < firstTime {
			firstTime = stats.StartTimeMs
		}
		if stats.EndTimeMs > lastTime {
			lastTime = stats.EndTimeMs
		}
		if readErr != nil {
			errorsCount++
		}
	}
	printDataset(absHistory, len(paths), total.Rows, firstTime, lastTime, total.InvalidRows, errorsCount)
	if errorsCount > 0 {
		return fmt.Errorf("validation failed for %d archive(s)", errorsCount)
	}
	return nil
}

func printFile(name string, s aggtrade.Stats, header bool, err error) {
	line := "============================================================"
	fmt.Printf("%s\nFILE: %s\n\n", line, name)
	fmt.Printf("Header Present     : %t\nRows               : %s\n", header, comma(s.Rows))
	if s.Rows > 0 {
		fmt.Printf("First AggTrade ID  : %d\nLast AggTrade ID   : %d\n\n", s.FirstAggTradeID, s.LastAggTradeID)
		fmt.Printf("Start Time         : %s\nEnd Time           : %s\n\n", utc(s.StartTimeMs), utc(s.EndTimeMs))
		fmt.Printf("Min Price          : %.8f\nMax Price          : %.8f\n\n", s.MinPrice, s.MaxPrice)
	}
	fmt.Printf("Total Quantity     : %.8f\nTaker Buy Qty      : %.8f\nTaker Sell Qty     : %.8f\n\n", s.TotalQuantity, s.TakerBuyQuantity, s.TakerSellQuantity)
	fmt.Printf("Timestamp Reverse  : %s\nID Reverse         : %s\nID Gaps            : %s\nInvalid Rows       : %s\n", comma(s.TimestampReversals), comma(s.IDReversals), comma(s.IDGaps), comma(s.InvalidRows))
	if err != nil {
		fmt.Printf("Error              : %v\nStatus             : ERROR\n", err)
	} else {
		fmt.Println("Status             : OK")
	}
	fmt.Println(line)
}

func printDataset(path string, files int, rows, start, end, invalid int64, errs int) {
	line := "============================================================"
	fmt.Printf("\n%s\nDATASET SUMMARY\n\nHistory Path : %s\n\nZIP Files    : %d\nTotal Rows   : %s\nStart Time   : %s\nEnd Time     : %s\n\nInvalid Rows : %s\nErrors       : %d\n\n", line, path, files, comma(rows), utc(start), utc(end), comma(invalid), errs)
	if errs == 0 {
		fmt.Println("Status       : OK")
	} else {
		fmt.Println("Status       : ERROR")
	}
	fmt.Println(line)
}

func utc(ms int64) string {
	if ms <= 0 {
		return "N/A"
	}
	return time.UnixMilli(ms).UTC().Format("2006-01-02 15:04:05.000 UTC")
}

func comma(n int64) string {
	s := fmt.Sprintf("%d", n)
	start := 0
	if len(s) > 0 && s[0] == '-' {
		start = 1
	}
	for i := len(s) - 3; i > start; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}
