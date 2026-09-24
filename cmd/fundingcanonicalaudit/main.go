package main

import (
	"fmt"
	"github.com/parquet-go/parquet-go"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
)

type Row struct {
	CalcTime             int64   `parquet:"calc_time"`
	FundingIntervalHours int64   `parquet:"funding_interval_hours"`
	LastFundingRate      float64 `parquet:"last_funding_rate"`
}

func main() {
	p, _ := filepath.Glob(`data/external/funding/v1/BTCUSDT/*.parquet`)
	sort.Strings(p)
	if len(p) != 24 {
		panic("files")
	}
	var n, dup, rev, nan, inf int64
	prev := int64(-1)
	for _, x := range p {
		f, _ := os.Open(x)
		r := parquet.NewGenericReader[Row](f)
		b := make([]Row, 256)
		for {
			c, e := r.Read(b)
			for _, v := range b[:c] {
				n++
				if v.CalcTime <= prev {
					if v.CalcTime == prev {
						dup++
					} else {
						rev++
					}
				}
				prev = v.CalcTime
				if math.IsNaN(v.LastFundingRate) {
					nan++
				}
				if math.IsInf(v.LastFundingRate, 0) {
					inf++
				}
			}
			if e == io.EOF {
				break
			}
			if e != nil {
				panic(e)
			}
		}
		r.Close()
		f.Close()
	}
	if n != 2193 || dup != 0 || rev != 0 || nan != 0 || inf != 0 {
		panic("audit")
	}
	fmt.Printf("AUDIT PASS files=%d rows=%d first=%d last=%d\n", len(p), n, 1704067200000, prev)
}
