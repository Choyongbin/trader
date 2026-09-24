package main

import (
	"binance_trader/internal/data/secondbar"
	"binance_trader/internal/data/spotaggtrade"
	"binance_trader/internal/market"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type Checkpoint struct {
	Month, SourceFile, ParquetPath                                                                  string
	RawRows, FirstID, LastID, FirstTimestampMs, LastTimestampMs, OutputRows, TradeBars, NoTradeBars int64
	OutputFirstTimestampMs, OutputLastTimestampMs                                                   int64
	PreviousClose                                                                                   float64
	ParquetBytes                                                                                    int64
	Complete                                                                                        bool
}
type Manifest struct {
	Version                                                                          int
	Symbol, Source                                                                   string
	SourceFilesExpected                                                              int
	ExpectedRawRows                                                                  int64
	ActualRawRows                                                                    int64
	FirstID, LastID                                                                  int64
	FirstTimestampMs, LastTimestampMs                                                int64
	TotalBars, TradeBars, NoTradeBars                                                int64
	IDGapCount, IDDuplicateCount, IDReverseCount, TimestampReverseCount, ParseErrors int64
	TotalParquetBytes                                                                int64
	TimestampNormalization                                                           string
	Monthly                                                                          []Checkpoint
	Complete                                                                         bool
}

func checkpointPath(root, month string) string { return filepath.Join(root, month+".checkpoint.json") }
func publishCheckpoint(path string, c Checkpoint) error {
	if !c.Complete || c.Month == "" || c.SourceFile == "" || c.ParquetPath == "" || c.OutputRows <= 0 || c.LastID < c.FirstID || c.LastTimestampMs < c.FirstTimestampMs || c.OutputLastTimestampMs < c.OutputFirstTimestampMs || c.ParquetBytes <= 0 {
		return fmt.Errorf("invalid checkpoint")
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	if _, err = f.Write(b); err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	var reread Checkpoint
	b, err = os.ReadFile(tmp)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(b, &reread); err != nil {
		return err
	}
	if reread.Month != c.Month || !reread.Complete {
		return fmt.Errorf("checkpoint reread")
	}
	return os.Rename(tmp, path)
}
func verifyCheckpoint(path, month, source string) (Checkpoint, bool) {
	var c Checkpoint
	b, e := os.ReadFile(path)
	if e != nil {
		return c, false
	}
	if json.Unmarshal(b, &c) != nil || !c.Complete || c.Month != month || c.SourceFile != source || c.OutputRows <= 0 || c.LastID < c.FirstID || c.LastTimestampMs < c.FirstTimestampMs || c.ParquetBytes <= 0 {
		return c, false
	}
	s, e := os.Stat(c.ParquetPath)
	if e != nil || s.Size() != c.ParquetBytes {
		return c, false
	}
	return c, true
}

func resumeAggregator(c Checkpoint, nextID int64, sink secondbar.BarSink) (*secondbar.Aggregator, error) {
	if nextID != c.LastID+1 {
		return nil, fmt.Errorf("aggTrade ID continuity: previous=%d current=%d", c.LastID, nextID)
	}
	return secondbar.NewResumedAggregator(sink, c.LastTimestampMs/1000*1000, c.PreviousClose)
}

func runTrades(a *secondbar.Aggregator, trades []market.AggTrade) error {
	for _, trade := range trades {
		if err := a.Add(trade); err != nil {
			return err
		}
	}
	return a.Flush()
}

func main() {
	preflight := flag.Bool("preflight", false, "")
	materialize := flag.Bool("materialize", false, "")
	verifyResume := flag.Bool("verify-resume", false, "")
	raw := flag.String("raw-root", `F:\binance_trader\history_external\raw\binance\spot\monthly\aggTrades\BTCUSDT`, "")
	out := flag.String("output-root", `data/external/spot/1s/v1/BTCUSDT`, "")
	flag.Parse()
	if !*preflight && !*materialize && !*verifyResume {
		fmt.Println("production requires -materialize")
		return
	}
	for d := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC); d.Before(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)); d = d.AddDate(0, 1, 0) {
		n := fmt.Sprintf("BTCUSDT-aggTrades-%s.zip", d.Format("2006-01"))
		if _, e := os.Stat(filepath.Join(*raw, n)); e != nil {
			panic(e)
		}
		if _, e := os.Stat(filepath.Join(*raw, n+".CHECKSUM")); e != nil {
			panic(e)
		}
	}
	if e := os.MkdirAll(*out, 0755); e != nil {
		panic(e)
	}
	if *verifyResume {
		if _, err := verifyAll(*raw, *out); err != nil {
			panic(err)
		}
		fmt.Println("RESUME PASS rebuild_months=0; final_holdout_accessed=false")
		return
	}
	if *materialize {
		if err := runMaterialization(*raw, *out); err != nil {
			panic(err)
		}
		return
	}
	m := Manifest{Version: 1, Symbol: "BTCUSDT", Source: *raw, SourceFilesExpected: 24, ExpectedRawRows: 992637604}
	b, _ := json.Marshal(m)
	_ = b
	fmt.Println("PREFLIGHT PASS; full_materialization=false; final_holdout_accessed=false")
}

func verifyAll(raw, out string) ([]Checkpoint, error) {
	var cs []Checkpoint
	for d := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC); d.Before(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)); d = d.AddDate(0, 1, 0) {
		month := d.Format("2006-01")
		source := fmt.Sprintf("BTCUSDT-aggTrades-%s.zip", month)
		c, ok := verifyCheckpoint(checkpointPath(out, month), month, source)
		if !ok {
			return nil, fmt.Errorf("invalid checkpoint %s", month)
		}
		s, err := secondbar.InspectParquet(c.ParquetPath)
		if err != nil || s.SecondBars != c.OutputRows || s.StartTimestampMs != c.OutputFirstTimestampMs || s.EndTimestampMs != c.OutputLastTimestampMs {
			return nil, fmt.Errorf("invalid parquet %s", month)
		}
		cs = append(cs, c)
	}
	return cs, nil
}

func runMaterialization(raw, out string) error {
	started := time.Now()
	var prior *Checkpoint
	manifest := Manifest{Version: 1, Symbol: "BTCUSDT", Source: raw, SourceFilesExpected: 24, ExpectedRawRows: 992637604, TimestampNormalization: "2024 ms; 2025 microseconds / 1000 => TimestampMs"}
	for d := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC); d.Before(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)); d = d.AddDate(0, 1, 0) {
		month := d.Format("2006-01")
		source := fmt.Sprintf("BTCUSDT-aggTrades-%s.zip", month)
		if c, ok := verifyCheckpoint(checkpointPath(out, month), month, source); ok {
			manifest.Monthly = append(manifest.Monthly, c)
			prior = &manifest.Monthly[len(manifest.Monthly)-1]
			fmt.Printf("month=%s resume=skip\n", month)
			continue
		}
		monthStart := time.Now()
		parquetPath := filepath.Join(out, fmt.Sprintf("BTCUSDT-spot-1s-%s.parquet", month))
		w, err := secondbar.NewParquetWriter(parquetPath, false)
		if err != nil {
			return err
		}
		var a *secondbar.Aggregator
		if prior == nil {
			a = secondbar.NewAggregator(w.Write)
		} else {
			a, err = secondbar.NewResumedAggregator(w.Write, prior.LastTimestampMs/1000*1000, prior.PreviousClose)
			if err != nil {
				w.Abort()
				return err
			}
		}
		var firstID, firstTS, lastID, lastTS int64
		var lastClose float64
		n, err := spotaggtrade.ReadArchive(filepath.Join(raw, source), spotaggtrade.ReadOptions{Microseconds: d.Year() >= 2025, OnTrade: func(t market.AggTrade) error {
			if firstID == 0 {
				firstID, firstTS = t.AggTradeID, t.TradeTimeMs
			}
			if lastID != 0 && t.AggTradeID != lastID+1 {
				return fmt.Errorf("ID continuity")
			}
			if prior != nil && lastID == 0 && t.AggTradeID != prior.LastID+1 {
				return fmt.Errorf("cross-month ID continuity")
			}
			if lastTS != 0 && t.TradeTimeMs < lastTS {
				return fmt.Errorf("timestamp reverse")
			}
			lastID, lastTS, lastClose = t.AggTradeID, t.TradeTimeMs, t.Price
			return a.Add(t)
		}})
		if err != nil {
			w.Abort()
			return fmt.Errorf("%s: %w", month, err)
		}
		if err = a.Flush(); err != nil {
			w.Abort()
			return err
		}
		if err = w.Close(); err != nil {
			return err
		}
		stats, err := secondbar.InspectParquet(parquetPath)
		if err != nil {
			return err
		}
		info, _ := os.Stat(parquetPath)
		c := Checkpoint{Month: month, SourceFile: source, ParquetPath: parquetPath, RawRows: n, FirstID: firstID, LastID: lastID, FirstTimestampMs: firstTS, LastTimestampMs: lastTS, OutputRows: stats.SecondBars, TradeBars: stats.TradeSeconds, NoTradeBars: stats.NoTradeSeconds, OutputFirstTimestampMs: stats.StartTimestampMs, OutputLastTimestampMs: stats.EndTimestampMs, PreviousClose: lastClose, ParquetBytes: info.Size(), Complete: true}
		if err = publishCheckpoint(checkpointPath(out, month), c); err != nil {
			return err
		}
		manifest.Monthly = append(manifest.Monthly, c)
		prior = &manifest.Monthly[len(manifest.Monthly)-1]
		fmt.Printf("month=%s raw=%d bars=%d trade=%d no_trade=%d bytes=%d elapsed=%s\n", month, n, stats.SecondBars, stats.TradeSeconds, stats.NoTradeSeconds, info.Size(), time.Since(monthStart).Round(time.Second))
	}
	for i, c := range manifest.Monthly {
		manifest.ActualRawRows += c.RawRows
		manifest.TotalBars += c.OutputRows
		manifest.TradeBars += c.TradeBars
		manifest.NoTradeBars += c.NoTradeBars
		manifest.TotalParquetBytes += c.ParquetBytes
		if i == 0 {
			manifest.FirstID, manifest.FirstTimestampMs = c.FirstID, c.FirstTimestampMs
		}
		manifest.LastID, manifest.LastTimestampMs = c.LastID, c.LastTimestampMs
	}
	if manifest.ActualRawRows != manifest.ExpectedRawRows {
		return fmt.Errorf("raw rows: got %d want %d", manifest.ActualRawRows, manifest.ExpectedRawRows)
	}
	manifest.Complete = true
	b, _ := json.MarshalIndent(manifest, "", "  ")
	path := filepath.Join("data", "manifests", "external", "spot", "1s", "v1", "BTCUSDT.json")
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	if err := os.WriteFile(path+".tmp", b, 0644); err != nil {
		return err
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		return err
	}
	fmt.Printf("COMPLETE raw=%d months=24 elapsed=%s final_holdout_accessed=false\n", manifest.ActualRawRows, time.Since(started).Round(time.Second))
	return nil
}
