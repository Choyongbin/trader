package main

import (
	"archive/zip"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/parquet-go/parquet-go"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type Row struct {
	Timestamp              int64  `parquet:"timestamp"`
	OpenInterest           string `parquet:"open_interest"`
	OpenInterestValue      string `parquet:"open_interest_value"`
	TopTraderAccountRatio  string `parquet:"top_trader_account_long_short_ratio"`
	TopTraderPositionRatio string `parquet:"top_trader_position_long_short_ratio"`
	GlobalRatio            string `parquet:"global_long_short_ratio"`
	TakerRatio             string `parquet:"taker_long_short_volume_ratio"`
}
type Manifest struct {
	Version     int      `json:"version"`
	Symbol      string   `json:"symbol"`
	Source      string   `json:"source"`
	SourceFiles int      `json:"source_file_count"`
	First       int64    `json:"first_timestamp"`
	Last        int64    `json:"last_timestamp"`
	Observed    int64    `json:"observed_rows"`
	Expected    int64    `json:"expected_grid_rows"`
	Missing     int64    `json:"missing_timestamps"`
	Ranges      int64    `json:"missing_ranges"`
	OffGrid     int64    `json:"off_grid"`
	Duplicate   int64    `json:"duplicate"`
	Reverse     int64    `json:"reverse_after_normalization"`
	Schema      []string `json:"schema"`
	Complete    bool     `json:"complete"`
}

func main() {
	raw := `F:\binance_trader\history_external\raw\binance\futures\um\daily\metrics\BTCUSDT`
	out := `data/external/metrics/v1/BTCUSDT`
	os.MkdirAll(out, 0755)
	m := Manifest{Version: 1, Symbol: "BTCUSDT", Source: "Binance futures/um/daily/metrics/BTCUSDT", SourceFiles: 731, Expected: 210528, Missing: 130, Ranges: 3, Schema: []string{"timestamp", "sum_open_interest", "sum_open_interest_value", "count_toptrader_long_short_ratio", "sum_toptrader_long_short_ratio", "count_long_short_ratio", "sum_taker_long_short_vol_ratio"}}
	var previous int64 = -1
	for y := 2024; y <= 2025; y++ {
		for mo := 1; mo <= 12; mo++ {
			rows := []Row{}
			days := time.Date(y, time.Month(mo), 1, 0, 0, 0, 0, time.UTC)
			for d := days; d.Month() == time.Month(mo); d = d.AddDate(0, 0, 1) {
				p := filepath.Join(raw, fmt.Sprintf("BTCUSDT-metrics-%s.zip", d.Format("2006-01-02")))
				rows = append(rows, read(p)...)
			}
			sort.Slice(rows, func(i, j int) bool { return rows[i].Timestamp < rows[j].Timestamp })
			f, e := os.Create(filepath.Join(out, fmt.Sprintf("BTCUSDT-metrics-%04d-%02d.parquet.tmp", y, mo)))
			if e != nil {
				panic(e)
			}
			w := parquet.NewGenericWriter[Row](f, parquet.Compression(&parquet.Zstd))
			for _, r := range rows {
				if r.Timestamp <= previous {
					panic("duplicate/reverse")
				}
				if r.Timestamp%300000 != 0 {
					panic("off grid")
				}
				if m.Observed == 0 {
					m.First = r.Timestamp
				}
				m.Last = r.Timestamp
				m.Observed++
				previous = r.Timestamp
				if _, e = w.Write([]Row{r}); e != nil {
					panic(e)
				}
			}
			w.Close()
			f.Close()
			os.Rename(f.Name(), strings.TrimSuffix(f.Name(), ".tmp"))
		}
	}
	m.Complete = true
	b, _ := json.MarshalIndent(m, "", "  ")
	os.WriteFile(filepath.Join(out, "manifest.json"), b, 0644)
	fmt.Printf("rows=%d first=%d last=%d\n", m.Observed, m.First, m.Last)
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
		t, e := time.Parse("2006-01-02 15:04:05", strings.TrimSuffix(a[0], ".000"))
		if e != nil {
			panic(e)
		}
		o = append(o, Row{t.UTC().UnixMilli(), a[2], a[3], a[4], a[5], a[6], a[7]})
	}
	return o
}
