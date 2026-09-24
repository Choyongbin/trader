package main

import (
	"archive/zip"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"binance_trader/internal/market"
	"github.com/parquet-go/parquet-go"
)

const (
	expectedRows   = int64(63158400)
	expectedTrades = int64(992637604)
)

type auditReport struct {
	Files, MonthBoundaries, RawSpotChecks                    int
	Rows, Missing, Duplicate, Reverse, OffGrid               int64
	TradeBars, NoTradeBars, AggTradeCount                    int64
	FirstTimestamp, LastTimestamp, FirstID, LastID           int64
	IDGap, IDOverlap, IDReverse, NaN, Inf, NumericalFailures int64
	ManifestConsistent, RawSpotChecksPassed, Complete        bool
	ParquetBytes                                             int64
	Status                                                   string
}
type manifest struct {
	ActualRawRows, FirstID, LastID, FirstTimestampMs, LastTimestampMs int64
	TotalBars, TradeBars, NoTradeBars, TotalParquetBytes              int64
	Monthly                                                           []json.RawMessage
	Complete                                                          bool
}

func main() {
	out := `data/external/spot/1s/v1/BTCUSDT`
	raw := `F:\binance_trader\history_external\raw\binance\spot\monthly\aggTrades\BTCUSDT`
	manifestPath := `data/manifests/external/spot/1s/v1/BTCUSDT.json`
	samples := map[int64]market.SecondBar{}
	for _, month := range []string{"2024-01", "2024-04", "2024-07", "2024-12", "2025-01", "2025-06", "2025-09", "2025-12"} {
		bar, err := rawFirstSecond(filepath.Join(raw, "BTCUSDT-aggTrades-"+month+".zip"), strings.HasPrefix(month, "2025"))
		if err != nil {
			panic(err)
		}
		samples[bar.TimestampMs] = bar
	}
	paths, _ := filepath.Glob(filepath.Join(out, "*.parquet"))
	sort.Strings(paths)
	r := auditReport{Files: len(paths), RawSpotChecks: len(samples), Complete: true}
	var previous market.SecondBar
	var havePrevious bool
	var previousTradeID int64
	for fileIndex, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			panic(err)
		}
		r.ParquetBytes += info.Size()
		f, err := os.Open(path)
		if err != nil {
			panic(err)
		}
		reader := parquet.NewGenericReader[market.SecondBar](f)
		batch := make([]market.SecondBar, 4096)
		firstInFile := true
		for {
			n, readErr := reader.Read(batch)
			for _, bar := range batch[:n] {
				if err := inspectBar(bar, previous, havePrevious, &r); err != nil {
					panic(fmt.Errorf("%s row %d: %w", filepath.Base(path), r.Rows+1, err))
				}
				if firstInFile && fileIndex > 0 {
					r.MonthBoundaries++
					if !havePrevious || bar.TimestampMs != previous.TimestampMs+1000 {
						panic("month boundary discontinuity")
					}
					if !bar.HasTrade && bar.Close != previous.Close {
						panic("month boundary previous close")
					}
				}
				firstInFile = false
				if bar.HasTrade {
					if previousTradeID != 0 {
						if bar.FirstAggTradeID > previousTradeID+1 {
							r.IDGap += bar.FirstAggTradeID - previousTradeID - 1
						}
						if bar.FirstAggTradeID <= previousTradeID {
							r.IDOverlap++
							if bar.LastAggTradeID < previousTradeID {
								r.IDReverse++
							}
						}
					}
					if r.FirstID == 0 {
						r.FirstID = bar.FirstAggTradeID
					}
					r.LastID = bar.LastAggTradeID
					previousTradeID = bar.LastAggTradeID
				}
				if want, ok := samples[bar.TimestampMs]; ok {
					if !sameBar(want, bar) {
						panic(fmt.Errorf("raw spot-check mismatch timestamp=%d", bar.TimestampMs))
					}
					delete(samples, bar.TimestampMs)
				}
				previous = bar
				havePrevious = true
				r.Rows++
			}
			if errors.Is(readErr, io.EOF) {
				break
			}
			if readErr != nil {
				panic(readErr)
			}
		}
		reader.Close()
		f.Close()
	}
	if len(samples) != 0 {
		panic("raw spot-check timestamp missing")
	}
	r.RawSpotChecksPassed = true
	b, err := os.ReadFile(manifestPath)
	if err != nil {
		panic(err)
	}
	var m manifest
	if json.Unmarshal(b, &m) != nil {
		panic("manifest parse")
	}
	r.ManifestConsistent = m.Complete && len(m.Monthly) == 24 && m.ActualRawRows == r.AggTradeCount && m.TotalBars == r.Rows && m.TradeBars == r.TradeBars && m.NoTradeBars == r.NoTradeBars && m.FirstID == r.FirstID && m.LastID == r.LastID && m.FirstTimestampMs/1000*1000 == r.FirstTimestamp && m.LastTimestampMs/1000*1000 == r.LastTimestamp && m.TotalParquetBytes == r.ParquetBytes
	if r.Files != 24 || r.Rows != expectedRows || r.AggTradeCount != expectedTrades || r.Missing+r.Duplicate+r.Reverse+r.OffGrid+r.NaN+r.Inf+r.NumericalFailures+r.IDGap+r.IDOverlap+r.IDReverse != 0 || r.MonthBoundaries != 23 || !r.ManifestConsistent {
		panic(fmt.Sprintf("audit failed: %+v", r))
	}
	r.Status = "PASS"
	data, _ := json.MarshalIndent(r, "", "  ")
	reportPath := `data/reports/external/spot/1s/v1/BTCUSDT-audit-v1.json`
	os.MkdirAll(filepath.Dir(reportPath), 0755)
	if os.WriteFile(reportPath, data, 0644) != nil {
		panic("report write")
	}
	fmt.Printf("AUDIT PASS rows=%d trade=%d no_trade=%d aggtrades=%d boundaries=%d spot_checks=%d\n", r.Rows, r.TradeBars, r.NoTradeBars, r.AggTradeCount, r.MonthBoundaries, r.RawSpotChecks)
}

func inspectBar(b, previous market.SecondBar, havePrevious bool, r *auditReport) error {
	if r.Rows == 0 {
		r.FirstTimestamp = b.TimestampMs
	}
	r.LastTimestamp = b.TimestampMs
	if b.TimestampMs%1000 != 0 {
		r.OffGrid++
	}
	if havePrevious {
		d := b.TimestampMs - previous.TimestampMs
		if d == 0 {
			r.Duplicate++
		} else if d < 0 {
			r.Reverse++
		} else if d > 1000 {
			r.Missing += d/1000 - 1
		}
	}
	values := []float64{b.Open, b.High, b.Low, b.Close, b.BaseVolume, b.QuoteVolume, b.TakerBuyBaseVolume, b.TakerSellBaseVolume, b.TakerBuyQuoteVolume, b.TakerSellQuoteVolume, b.VWAP}
	for _, v := range values {
		if math.IsNaN(v) {
			r.NaN++
		}
		if math.IsInf(v, 0) {
			r.Inf++
		}
	}
	if b.HasTrade {
		r.TradeBars++
		r.AggTradeCount += b.AggTradeCount
		if b.AggTradeCount <= 0 || b.BaseVolume <= 0 || b.QuoteVolume <= 0 || b.FirstAggTradeID > b.LastAggTradeID || b.High < b.Open || b.High < b.Close || b.Low > b.Open || b.Low > b.Close || b.High < b.Low || !near(b.TakerBuyBaseVolume+b.TakerSellBaseVolume, b.BaseVolume) || !near(b.TakerBuyQuoteVolume+b.TakerSellQuoteVolume, b.QuoteVolume) || !near(b.VWAP, b.QuoteVolume/b.BaseVolume) {
			r.NumericalFailures++
		}
	} else {
		r.NoTradeBars++
		if b.AggTradeCount != 0 || b.BaseVolume != 0 || b.QuoteVolume != 0 || b.TakerBuyBaseVolume != 0 || b.TakerSellBaseVolume != 0 || b.TakerBuyQuoteVolume != 0 || b.TakerSellQuoteVolume != 0 || b.Open != b.High || b.High != b.Low || b.Low != b.Close || (havePrevious && b.Close != previous.Close) {
			r.NumericalFailures++
		}
	}
	return nil
}
func near(a, b float64) bool {
	return math.Abs(a-b) <= 1e-9*math.Max(1, math.Max(math.Abs(a), math.Abs(b)))
}
func sameBar(a, b market.SecondBar) bool {
	return a.TimestampMs == b.TimestampMs && near(a.Open, b.Open) && near(a.High, b.High) && near(a.Low, b.Low) && near(a.Close, b.Close) && near(a.BaseVolume, b.BaseVolume) && near(a.QuoteVolume, b.QuoteVolume) && near(a.VWAP, b.VWAP) && a.AggTradeCount == b.AggTradeCount && near(a.TakerBuyBaseVolume, b.TakerBuyBaseVolume) && near(a.TakerSellBaseVolume, b.TakerSellBaseVolume) && near(a.TakerBuyQuoteVolume, b.TakerBuyQuoteVolume) && near(a.TakerSellQuoteVolume, b.TakerSellQuoteVolume) && a.HasTrade == b.HasTrade && a.FirstAggTradeID == b.FirstAggTradeID && a.LastAggTradeID == b.LastAggTradeID
}
func rawFirstSecond(path string, micro bool) (market.SecondBar, error) {
	z, e := zip.OpenReader(path)
	if e != nil {
		return market.SecondBar{}, e
	}
	defer z.Close()
	rc, e := z.File[0].Open()
	if e != nil {
		return market.SecondBar{}, e
	}
	defer rc.Close()
	c := csv.NewReader(rc)
	c.FieldsPerRecord = -1
	var b market.SecondBar
	first := true
	for {
		a, e := c.Read()
		if e != nil {
			return b, e
		}
		id, _ := strconv.ParseInt(a[0], 10, 64)
		price, _ := strconv.ParseFloat(a[1], 64)
		qty, _ := strconv.ParseFloat(a[2], 64)
		ts, _ := strconv.ParseInt(a[5], 10, 64)
		maker, _ := strconv.ParseBool(a[6])
		if micro {
			ts /= 1000
		}
		bucket := ts / 1000 * 1000
		if first {
			b = market.SecondBar{TimestampMs: bucket, Open: price, High: price, Low: price, Close: price, BaseVolume: qty, QuoteVolume: price * qty, AggTradeCount: 1, VWAP: price, HasTrade: true, FirstAggTradeID: id, LastAggTradeID: id}
			first = false
			if maker {
				b.TakerSellBaseVolume = qty
				b.TakerSellQuoteVolume = price * qty
			} else {
				b.TakerBuyBaseVolume = qty
				b.TakerBuyQuoteVolume = price * qty
			}
			continue
		}
		if bucket != b.TimestampMs {
			return b, nil
		}
		q := price * qty
		if price > b.High {
			b.High = price
		}
		if price < b.Low {
			b.Low = price
		}
		b.Close = price
		b.BaseVolume += qty
		b.QuoteVolume += q
		b.AggTradeCount++
		b.LastAggTradeID = id
		if maker {
			b.TakerSellBaseVolume += qty
			b.TakerSellQuoteVolume += q
		} else {
			b.TakerBuyBaseVolume += qty
			b.TakerBuyQuoteVolume += q
		}
		b.VWAP = b.QuoteVolume / b.BaseVolume
	}
}
