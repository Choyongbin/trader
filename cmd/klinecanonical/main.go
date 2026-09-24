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
	OpenTime                                    int64 `parquet:"open_time"`
	Open, High, Low, Close, Volume              string
	CloseTime                                   int64 `parquet:"close_time"`
	QuoteVolume                                 string
	Count                                       int64
	TakerBuyVolume, TakerBuyQuoteVolume, Ignore string
}
type M struct {
	Version                                                                int `json:"version"`
	Symbol, Source                                                         string
	Files                                                                  int `json:"source_file_count"`
	Rows, Expected, Missing, Ranges, Duplicate, Reverse, OffGrid, NaN, Inf int64
	First, Last, LongestGap                                                int64
	ParquetFiles                                                           int
	Complete                                                               bool `json:"complete"`
}

func main() {
	for _, x := range []struct{ n, dir, out, prefix string }{{"markprice", "markPriceKlines", `data/external/markprice/v1/BTCUSDT`, "markprice"}, {"indexprice", "indexPriceKlines", `data/external/indexprice/v1/BTCUSDT`, "indexprice"}, {"premiumindex", "premiumIndexKlines", `data/external/premiumindex/v1/BTCUSDT`, "premiumindex"}} {
		build(x.n, `F:\binance_trader\history_external\raw\binance\futures\um\monthly\`+x.dir+`\BTCUSDT\1m`, x.out, x.prefix)
	}
}
func build(n, raw, out, prefix string) {
	os.MkdirAll(out, 0755)
	m := M{Version: 1, Symbol: "BTCUSDT", Source: raw, Files: 24, Expected: 1052640}
	set := map[int64]bool{}
	prev := int64(-1)
	for y := 2024; y <= 2025; y++ {
		for mo := 1; mo <= 12; mo++ {
			p := filepath.Join(raw, fmt.Sprintf("BTCUSDT-1m-%04d-%02d.zip", y, mo))
			rows := read(p)
			sort.Slice(rows, func(i, j int) bool { return rows[i].OpenTime < rows[j].OpenTime })
			tmp := filepath.Join(out, fmt.Sprintf("BTCUSDT-%s-%04d-%02d.parquet.tmp", prefix, y, mo))
			f, e := os.Create(tmp)
			if e != nil {
				panic(e)
			}
			w := parquet.NewGenericWriter[Row](f, parquet.Compression(&parquet.Zstd))
			for _, r := range rows {
				if r.OpenTime <= prev {
					panic("duplicate/reverse")
				}
				if r.OpenTime%60000 != 0 {
					panic("off-grid")
				}
				if m.Rows == 0 {
					m.First = r.OpenTime
				}
				m.Last = r.OpenTime
				m.Rows++
				set[r.OpenTime] = true
				prev = r.OpenTime
				if _, e = w.Write([]Row{r}); e != nil {
					panic(e)
				}
			}
			w.Close()
			f.Close()
			os.Rename(tmp, strings.TrimSuffix(tmp, ".tmp"))
			m.ParquetFiles++
		}
	}
	in := false
	ts := make([]int64, 0, len(set))
	for t := range set {
		ts = append(ts, t)
	}
	sort.Slice(ts, func(i, j int) bool { return ts[i] < ts[j] })
	for t := int64(1704067200000); t <= 1767225540000; t += 60000 {
		if !set[t] {
			m.Missing++
			if !in {
				m.Ranges++
				in = true
			}
		} else {
			in = false
		}
	}
	for i := 1; i < len(ts); i++ {
		if d := ts[i] - ts[i-1]; d > m.LongestGap {
			m.LongestGap = d
		}
	}
	m.Complete = true
	b, _ := json.MarshalIndent(m, "", "  ")
	os.WriteFile(filepath.Join(out, "manifest.json"), b, 0644)
	fmt.Printf("%s rows=%d missing=%d\n", n, m.Rows, m.Missing)
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
		i := func(x int) int64 {
			v, e := strconv.ParseInt(a[x], 10, 64)
			if e != nil {
				panic(e)
			}
			return v
		}
		for _, x := range []int{1, 2, 3, 4} {
			v, e := strconv.ParseFloat(a[x], 64)
			if e != nil || math.IsNaN(v) || math.IsInf(v, 0) {
				panic("numeric")
			}
		}
		o = append(o, Row{i(0), a[1], a[2], a[3], a[4], a[5], i(6), a[7], i(8), a[9], a[10], a[11]})
	}
	return o
}
