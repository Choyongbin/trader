package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"binance_trader/internal/data/aggtrade"
	"binance_trader/internal/data/secondbar"
)

const progressInterval int64 = 10_000_000

var monthPattern = regexp.MustCompile(`^\d{4}-(0[1-9]|1[0-2])$`)

func main() {
	if err := run(); err != nil {
		log.SetFlags(0)
		log.Fatal(err)
	}
}

func run() error {
	history := flag.String("history", ".\\history", "raw Binance archive directory")
	output := flag.String("output", ".\\data\\derived\\1s", "derived 1-second dataset root")
	manifestRoot := flag.String("manifests", ".\\data\\manifests\\1s", "manifest root")
	symbol := flag.String("symbol", "BTCUSDT", "market symbol")
	month := flag.String("month", "", "convert only YYYY-MM")
	force := flag.Bool("force", false, "replace an existing derived file")
	flag.Parse()
	*symbol = strings.ToUpper(*symbol)
	if *symbol != "BTCUSDT" {
		return fmt.Errorf("this raw reader currently supports symbol BTCUSDT, got %q", *symbol)
	}
	if *month != "" && !monthPattern.MatchString(*month) {
		return fmt.Errorf("invalid -month %q; want YYYY-MM", *month)
	}
	paths, err := aggtrade.Discover(*history)
	if err != nil {
		return err
	}
	if *month != "" {
		want := "-" + *month + ".zip"
		var filtered []string
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
	converted, skipped := 0, 0
	for i, source := range paths {
		monthValue := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(source), *symbol+"-aggTrades-"), ".zip")
		outputPath := filepath.Join(*output, *symbol, monthValue[:4], fmt.Sprintf("%s-1s-%s.parquet", *symbol, monthValue))
		manifestPath := filepath.Join(*manifestRoot, *symbol, fmt.Sprintf("%s-1s-%s.json", *symbol, monthValue))
		if _, statErr := os.Stat(outputPath); statErr == nil && !*force {
			if err := verifyExisting(outputPath, manifestPath, *symbol, monthValue); err != nil {
				return fmt.Errorf("existing %s is not verified: %w", monthValue, err)
			}
			fmt.Printf("[%d/%d] %s existing / verified -> SKIP\n", i+1, len(paths), monthValue)
			skipped++
			continue
		} else if statErr != nil && !os.IsNotExist(statErr) {
			return fmt.Errorf("stat output %s: %w", outputPath, statErr)
		}
		if err := convert(source, *output, *manifestRoot, *symbol, *force, i+1, len(paths)); err != nil {
			return err
		}
		converted++
	}
	fmt.Printf("Conversion complete: converted=%d skipped=%d failed=0\n", converted, skipped)
	return nil
}

func verifyExisting(outputPath, manifestPath, symbol, month string) error {
	m, err := secondbar.ReadManifest(manifestPath)
	if err != nil {
		return err
	}
	if m.Symbol != symbol || m.SchemaVersion != secondbar.SchemaVersion || m.Aggregation != "1s" {
		return fmt.Errorf("manifest identity/schema mismatch")
	}
	if !strings.Contains(m.OutputFile, month) {
		return fmt.Errorf("manifest output month mismatch")
	}
	s, err := secondbar.InspectParquet(outputPath)
	if err != nil {
		return err
	}
	if s.SecondBars != m.SecondBarRows || s.StartTimestampMs != m.StartTimestampMs || s.EndTimestampMs != m.EndTimestampMs {
		return fmt.Errorf("manifest/parquet statistics mismatch")
	}
	return nil
}

func convert(source, outputRoot, manifestRoot, symbol string, force bool, index, count int) error {
	started := time.Now()
	name := filepath.Base(source)
	month := strings.TrimSuffix(strings.TrimPrefix(name, symbol+"-aggTrades-"), ".zip")
	year := month[:4]
	outputName := fmt.Sprintf("%s-1s-%s.parquet", symbol, month)
	outputPath := filepath.Join(outputRoot, symbol, year, outputName)
	manifestPath := filepath.Join(manifestRoot, symbol, strings.TrimSuffix(outputName, ".parquet")+".json")
	fmt.Printf("[%d/%d] %s\n", index, count, name)
	w, err := secondbar.NewParquetWriter(outputPath, force)
	if err != nil {
		return err
	}
	a := secondbar.NewAggregator(w.Write)
	raw, _, err := aggtrade.ReadArchive(source, aggtrade.ReadOptions{ProgressInterval: progressInterval, Progress: func(rows int64) { fmt.Printf("%s aggTrades processed\n", comma(rows)) }, OnTrade: a.Add})
	if err != nil {
		w.Abort()
		return err
	}
	if err := a.Flush(); err != nil {
		w.Abort()
		return fmt.Errorf("flush final second: %w", err)
	}
	derived := a.Stats()
	if err := verifyRaw(raw, derived); err != nil {
		w.Abort()
		return fmt.Errorf("raw/derived verification: %w", err)
	}
	if err := w.Close(); err != nil {
		return err
	}
	readback, err := secondbar.InspectParquet(outputPath)
	if err != nil {
		return fmt.Errorf("verify published parquet: %w", err)
	}
	if err := compareStats(derived, readback); err != nil {
		return fmt.Errorf("parquet round-trip verification: %w", err)
	}
	info, err := os.Stat(outputPath)
	if err != nil {
		return fmt.Errorf("stat output: %w", err)
	}
	elapsed := time.Since(started)
	m := secondbar.Manifest{Symbol: symbol, SourceFile: name, OutputFile: outputName, SchemaVersion: secondbar.SchemaVersion, Aggregation: "1s", AggTradeRows: raw.Rows, SecondBarRows: derived.SecondBars, TradeSeconds: derived.TradeSeconds, NoTradeSeconds: derived.NoTradeSeconds, StartTimestampMs: derived.StartTimestampMs, EndTimestampMs: derived.EndTimestampMs, FirstAggTradeID: raw.FirstAggTradeID, LastAggTradeID: raw.LastAggTradeID, InvalidRows: raw.InvalidRows, FirstPrice: derived.FirstPrice, LastPrice: derived.LastPrice, MinPrice: derived.MinPrice, MaxPrice: derived.MaxPrice, BaseVolume: derived.BaseVolume, QuoteVolume: derived.QuoteVolume, TakerBuyVolume: derived.TakerBuyBaseVolume, TakerSellVolume: derived.TakerSellBaseVolume, OutputSizeBytes: info.Size(), ElapsedMs: elapsed.Milliseconds()}
	if err := secondbar.WriteManifest(manifestPath, m); err != nil {
		return err
	}
	printResult(name, outputName, raw, derived, info.Size(), elapsed)
	return nil
}

func verifyRaw(raw aggtrade.Stats, d secondbar.Stats) error {
	if d.SecondBars == 0 || d.TradeSeconds+d.NoTradeSeconds != d.SecondBars {
		return fmt.Errorf("invalid second counts")
	}
	if raw.MinPrice != d.MinPrice || raw.MaxPrice != d.MaxPrice {
		return fmt.Errorf("price range differs: raw %.8f..%.8f derived %.8f..%.8f", raw.MinPrice, raw.MaxPrice, d.MinPrice, d.MaxPrice)
	}
	if raw.FirstPrice != d.FirstPrice || raw.LastPrice != d.LastPrice {
		return fmt.Errorf("first/last prices differ")
	}
	if !near(raw.TotalQuantity, d.BaseVolume) || !near(raw.TakerBuyQuantity, d.TakerBuyBaseVolume) || !near(raw.TakerSellQuantity, d.TakerSellBaseVolume) {
		return fmt.Errorf("base/taker volumes differ: base delta=%g rel=%g, buy delta=%g rel=%g, sell delta=%g rel=%g", d.BaseVolume-raw.TotalQuantity, relativeError(raw.TotalQuantity, d.BaseVolume), d.TakerBuyBaseVolume-raw.TakerBuyQuantity, relativeError(raw.TakerBuyQuantity, d.TakerBuyBaseVolume), d.TakerSellBaseVolume-raw.TakerSellQuantity, relativeError(raw.TakerSellQuantity, d.TakerSellBaseVolume))
	}
	if !near(raw.TotalQuoteQuantity, d.QuoteVolume) {
		return fmt.Errorf("quote volume differs: delta=%g relative=%g", d.QuoteVolume-raw.TotalQuoteQuantity, relativeError(raw.TotalQuoteQuantity, d.QuoteVolume))
	}
	if raw.StartTimeMs/1000*1000 != d.StartTimestampMs || raw.EndTimeMs/1000*1000 != d.EndTimestampMs {
		return fmt.Errorf("timestamp range differs")
	}
	return nil
}

func compareStats(a, b secondbar.Stats) error {
	if a.SecondBars != b.SecondBars || a.TradeSeconds != b.TradeSeconds || a.NoTradeSeconds != b.NoTradeSeconds || a.StartTimestampMs != b.StartTimestampMs || a.EndTimestampMs != b.EndTimestampMs || a.FirstPrice != b.FirstPrice || a.LastPrice != b.LastPrice || a.MinPrice != b.MinPrice || a.MaxPrice != b.MaxPrice || !near(a.BaseVolume, b.BaseVolume) || !near(a.QuoteVolume, b.QuoteVolume) || !near(a.TakerBuyBaseVolume, b.TakerBuyBaseVolume) || !near(a.TakerSellBaseVolume, b.TakerSellBaseVolume) {
		return fmt.Errorf("aggregate statistics differ after readback")
	}
	return nil
}

func near(a, b float64) bool {
	return math.Abs(a-b) <= 1e-10*math.Max(1, math.Max(math.Abs(a), math.Abs(b)))
}
func relativeError(a, b float64) float64 {
	return math.Abs(a-b) / math.Max(1, math.Max(math.Abs(a), math.Abs(b)))
}
func utc(ms int64) string { return time.UnixMilli(ms).UTC().Format("2006-01-02 15:04:05 UTC") }
func comma(n int64) string {
	s := fmt.Sprintf("%d", n)
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}
func printResult(source, output string, raw aggtrade.Stats, s secondbar.Stats, size int64, elapsed time.Duration) {
	line := "============================================================"
	fmt.Printf("%s\nSOURCE\n%s\n\nAggTrades           : %s\n\nOUTPUT\n%s\n\nSecond Bars         : %s\nTrade Seconds       : %s\nNo-Trade Seconds    : %s\n\nStart               : %s\nEnd                 : %s\n\nFirst Price         : %.8f\nLast Price          : %.8f\nMin Price           : %.8f\nMax Price           : %.8f\n\nBase Volume         : %.8f\nQuote Volume        : %.8f\nTaker Buy Volume    : %.8f\nTaker Sell Volume   : %.8f\n\nRaw Base Volume     : %.8f\nBase Volume Delta   : %.12f\n\nOutput Size         : %s bytes\nElapsed             : %s\n\nStatus              : OK\n%s\n", line, source, comma(raw.Rows), output, comma(s.SecondBars), comma(s.TradeSeconds), comma(s.NoTradeSeconds), utc(s.StartTimestampMs), utc(s.EndTimestampMs), s.FirstPrice, s.LastPrice, s.MinPrice, s.MaxPrice, s.BaseVolume, s.QuoteVolume, s.TakerBuyBaseVolume, s.TakerSellBaseVolume, raw.TotalQuantity, s.BaseVolume-raw.TotalQuantity, comma(size), elapsed.Round(time.Millisecond), line)
}
