package main

import (
	"archive/zip"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/parquet-go/parquet-go"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type Row struct {
	CalcTime             int64   `parquet:"calc_time"`
	FundingIntervalHours int64   `parquet:"funding_interval_hours"`
	LastFundingRate      float64 `parquet:"last_funding_rate"`
}
type Manifest struct {
	Version                                                                    int
	Symbol, Source                                                             string
	SourceFiles                                                                int
	Rows, First, Last, Duplicate, Reverse, NaN, Inf, MinGap, MedianGap, MaxGap int64
	ParquetFiles                                                               int
	Complete                                                                   bool
	FutureUse                                                                  string
}

func main() {
	raw := `F:\binance_trader\history_external\raw\binance\futures\um\monthly\fundingRate\BTCUSDT`
	out := `data/external/funding/v1/BTCUSDT`
	os.MkdirAll(out, 0755)
	m := Manifest{Version: 1, Symbol: "BTCUSDT", Source: "Binance futures/um/monthly/fundingRate/BTCUSDT", SourceFiles: 24, FutureUse: "decision t may use only observation timestamp <= t; no backward fill"}
	all := []Row{}
	for y := 2024; y <= 2025; y++ {
		for mo := 1; mo <= 12; mo++ {
			rows := read(filepath.Join(raw, fmt.Sprintf("BTCUSDT-fundingRate-%04d-%02d.zip", y, mo)))
			all = append(all, rows...)
			tmp := filepath.Join(out, fmt.Sprintf("BTCUSDT-funding-%04d-%02d.parquet.tmp", y, mo))
			f, _ := os.Create(tmp)
			w := parquet.NewGenericWriter[Row](f, parquet.Compression(&parquet.Zstd))
			w.Write(rows)
			w.Close()
			f.Close()
			os.Rename(tmp, strings.TrimSuffix(tmp, ".tmp"))
			m.ParquetFiles++
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].CalcTime < all[j].CalcTime })
	ds := []int64{}
	for i, r := range all {
		if i == 0 {
			m.First = r.CalcTime
		}
		m.Last = r.CalcTime
		m.Rows++
		if i > 0 {
			d := r.CalcTime - all[i-1].CalcTime
			if d <= 0 {
				panic("order")
			}
			ds = append(ds, d)
			if m.MinGap == 0 || d < m.MinGap {
				m.MinGap = d
			}
			if d > m.MaxGap {
				m.MaxGap = d
			}
		}
	}
	sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
	m.MedianGap = ds[len(ds)/2]
	m.Complete = true
	b, _ := json.MarshalIndent(m, "", "  ")
	os.WriteFile(filepath.Join(out, "manifest.json"), b, 0644)
	fmt.Printf("rows=%d first=%d last=%d min=%d median=%d max=%d\n", m.Rows, m.First, m.Last, m.MinGap, m.MedianGap, m.MaxGap)
}
func read(p string) []Row {
	z, e := zip.OpenReader(p)
	if e != nil {
		panic(e)
	}
	defer z.Close()
	r, e := z.File[0].Open()
	if e != nil {
		panic(e)
	}
	defer r.Close()
	c := csv.NewReader(r)
	c.Read()
	o := []Row{}
	for {
		a, e := c.Read()
		if e == io.EOF {
			break
		}
		if e != nil {
			panic(e)
		}
		t, e := strconv.ParseInt(a[0], 10, 64)
		if e != nil {
			panic(e)
		}
		h, e := strconv.ParseInt(a[1], 10, 64)
		if e != nil {
			panic(e)
		}
		v, e := strconv.ParseFloat(a[2], 64)
		if e != nil || math.IsNaN(v) || math.IsInf(v, 0) {
			panic("rate")
		}
		o = append(o, Row{t, h, v})
	}
	return o
}
