package main

import (
	"archive/zip"
	"encoding/csv"
	"fmt"
	"github.com/parquet-go/parquet-go"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
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

func main() {
	out := `data/external/metrics/v1/BTCUSDT`
	raw := `F:\binance_trader\history_external\raw\binance\futures\um\daily\metrics\BTCUSDT`
	paths, _ := filepath.Glob(filepath.Join(out, "*.parquet"))
	sort.Strings(paths)
	var n, dup, rev, off, nan, inf int64
	prev := int64(-1)
	set := map[int64]bool{}
	for _, p := range paths {
		f, e := os.Open(p)
		if e != nil {
			panic(e)
		}
		rd := parquet.NewGenericReader[Row](f)
		buf := make([]Row, 2048)
		for {
			c, e := rd.Read(buf)
			for _, x := range buf[:c] {
				n++
				if x.Timestamp <= prev {
					if x.Timestamp == prev {
						dup++
					} else {
						rev++
					}
				}
				prev = x.Timestamp
				if x.Timestamp%300000 != 0 {
					off++
				}
				set[x.Timestamp] = true
				for _, v := range []string{x.OpenInterest, x.OpenInterestValue, x.TopTraderAccountRatio, x.TopTraderPositionRatio, x.GlobalRatio, x.TakerRatio} {
					if v == "" {
						continue
					}
					z, q := strconv.ParseFloat(v, 64)
					if q != nil {
						panic(q)
					}
					if math.IsNaN(z) {
						nan++
					}
					if math.IsInf(z, 0) {
						inf++
					}
				}
			}
			if e == io.EOF {
				break
			}
			if e != nil {
				panic(e)
			}
		}
		rd.Close()
		f.Close()
	}
	missing, ranges := int64(0), int64(0)
	in := false
	for t := int64(1704067200000); t <= 1767225300000; t += 300000 {
		if !set[t] {
			missing++
			if !in {
				ranges++
				in = true
			}
		} else {
			in = false
		}
	}
	rawset := map[int64]bool{}
	for d := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC); d.Before(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)); d = d.AddDate(0, 0, 1) {
		p := filepath.Join(raw, fmt.Sprintf("BTCUSDT-metrics-%s.zip", d.Format("2006-01-02")))
		z, e := zip.OpenReader(p)
		if e != nil {
			panic(e)
		}
		rc, e := z.File[0].Open()
		if e != nil {
			panic(e)
		}
		c := csv.NewReader(rc)
		c.Read()
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
			rawset[t.UTC().UnixMilli()] = true
		}
		rc.Close()
		z.Close()
	}
	if len(rawset) != len(set) {
		panic("set count mismatch")
	}
	for t := range rawset {
		if !set[t] {
			panic("raw canonical timestamp mismatch")
		}
	}
	if n != 210398 || missing != 130 || ranges != 3 || dup != 0 || rev != 0 || off != 0 || nan != 0 || inf != 0 {
		panic("audit invariant")
	}
	fmt.Printf("AUDIT PASS rows=%d first=%d last=%d missing=%d ranges=%d\n", n, func() int64 {
		a := make([]int64, 0, len(set))
		for t := range set {
			a = append(a, t)
		}
		sort.Slice(a, func(i, j int) bool { return a[i] < a[j] })
		return a[0]
	}(), prev, missing, ranges)
}
