package main

import (
	"binance_trader/internal/data/secondbar"
	"binance_trader/internal/market"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestCheckpointAtomicAndVerifier(t *testing.T) {
	d := t.TempDir()
	p := filepath.Join(d, "a.parquet")
	if err := os.WriteFile(p, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	c := Checkpoint{Month: "2024-01", SourceFile: "a.zip", ParquetPath: p, RawRows: 2, FirstID: 10, LastID: 11, FirstTimestampMs: 1000, LastTimestampMs: 2000, OutputRows: 1, TradeBars: 1, PreviousClose: 1, ParquetBytes: 1, Complete: true}
	cp := checkpointPath(d, c.Month)
	if err := publishCheckpoint(cp, c); err != nil {
		t.Fatal(err)
	}
	if _, ok := verifyCheckpoint(cp, c.Month, c.SourceFile); !ok {
		t.Fatal("verify")
	}
	if _, ok := verifyCheckpoint(cp, "bad", c.SourceFile); ok {
		t.Fatal("wrong month")
	}
	c.Complete = false
	if err := publishCheckpoint(filepath.Join(d, "bad"), c); err == nil {
		t.Fatal("incomplete")
	}
}

func TestFreshAndResumedMonthBoundaryEquality(t *testing.T) {
	const boundary = int64(1706745600000)
	aTrade := market.AggTrade{AggTradeID: 100, Price: 100, Quantity: 2, FirstTradeID: 100, LastTradeID: 100, TradeTimeMs: boundary - 500, BuyerMaker: true}
	bTrade := market.AggTrade{AggTradeID: 101, Price: 101, Quantity: 3, FirstTradeID: 101, LastTradeID: 101, TradeTimeMs: boundary + 2250, BuyerMaker: false}
	var fresh []market.SecondBar
	fa := secondbar.NewAggregator(func(b market.SecondBar) error {
		if b.TimestampMs >= boundary {
			fresh = append(fresh, b)
		}
		return nil
	})
	if err := fa.Add(aTrade); err != nil {
		t.Fatal(err)
	}
	if err := runTrades(fa, []market.AggTrade{bTrade}); err != nil {
		t.Fatal(err)
	}
	resumeDir := t.TempDir()
	monthAPath := filepath.Join(resumeDir, "month-a.parquet")
	monthAWriter, err := secondbar.NewParquetWriter(monthAPath, false)
	if err != nil {
		t.Fatal(err)
	}
	monthA := secondbar.NewAggregator(monthAWriter.Write)
	if err = runTrades(monthA, []market.AggTrade{aTrade}); err != nil {
		monthAWriter.Abort()
		t.Fatal(err)
	}
	if err = monthAWriter.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(monthAPath)
	if err != nil {
		t.Fatal(err)
	}
	c := Checkpoint{Month: "2024-01", SourceFile: "month-a.zip", ParquetPath: monthAPath, RawRows: 1, FirstID: 100, LastID: 100, FirstTimestampMs: aTrade.TradeTimeMs, LastTimestampMs: aTrade.TradeTimeMs, OutputRows: 1, TradeBars: 1, PreviousClose: 100, ParquetBytes: info.Size(), Complete: true}
	cp := checkpointPath(resumeDir, c.Month)
	if err = publishCheckpoint(cp, c); err != nil {
		t.Fatal(err)
	}
	c, ok := verifyCheckpoint(cp, c.Month, c.SourceFile)
	if !ok {
		t.Fatal("published checkpoint rejected")
	}
	var resumed []market.SecondBar
	ra, err := resumeAggregator(c, 101, func(b market.SecondBar) error { resumed = append(resumed, b); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err = runTrades(ra, []market.AggTrade{bTrade}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fresh, resumed) {
		t.Fatalf("fresh=%+v resumed=%+v", fresh, resumed)
	}
	if len(resumed) != 3 || resumed[0].TimestampMs != boundary || resumed[1].TimestampMs != boundary+1000 || resumed[0].HasTrade || resumed[0].Close != 100 || !resumed[2].HasTrade {
		t.Fatalf("boundary=%+v", resumed)
	}
	bad := c
	bad.PreviousClose = 99
	var wrong []market.SecondBar
	wa, err := resumeAggregator(bad, 101, func(b market.SecondBar) error { wrong = append(wrong, b); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err = runTrades(wa, []market.AggTrade{bTrade}); err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(fresh, wrong) {
		t.Fatal("corrupted previous close accepted silently")
	}
	if _, err = resumeAggregator(c, 102, func(market.SecondBar) error { return nil }); err == nil {
		t.Fatal("ID gap accepted")
	}
}
