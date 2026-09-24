package main

import (
	"fmt"
	"github.com/parquet-go/parquet-go"
	"io"
	"os"
	"path/filepath"
	"sort"
)

type Row struct {
	OpenTime                                    int64 `parquet:"open_time"`
	Open, High, Low, Close, Volume              string
	CloseTime                                   int64
	QuoteVolume                                 string
	Count                                       int64
	TakerBuyVolume, TakerBuyQuoteVolume, Ignore string
}

func main() {
	sets := []map[int64]bool{}
	for _, d := range []string{`data/external/markprice/v1/BTCUSDT`, `data/external/indexprice/v1/BTCUSDT`, `data/external/premiumindex/v1/BTCUSDT`} {
		p, _ := filepath.Glob(filepath.Join(d, "*.parquet"))
		sort.Strings(p)
		if len(p) != 24 {
			panic("file count")
		}
		s := map[int64]bool{}
		prev := int64(-1)
		var n int64
		for _, x := range p {
			f, e := os.Open(x)
			if e != nil {
				panic(e)
			}
			r := parquet.NewGenericReader[Row](f)
			b := make([]Row, 2048)
			for {
				c, e := r.Read(b)
				for _, v := range b[:c] {
					if v.OpenTime <= prev || v.OpenTime%60000 != 0 {
						panic("order/grid")
					}
					prev = v.OpenTime
					s[v.OpenTime] = true
					n++
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
		miss := int64(0)
		for t := int64(1704067200000); t <= 1767225540000; t += 60000 {
			if !s[t] {
				miss++
			}
		}
		if n != 1052638 || miss != 2 {
			panic("count")
		}
		fmt.Printf("AUDIT PASS %s rows=%d missing=%d\n", d, n, miss)
		sets = append(sets, s)
	}
	for i := range sets[0] {
		if !sets[1][i] || !sets[2][i] {
			panic("alignment")
		}
	}
	fmt.Println("ALIGNMENT PASS common=1052638 only=0; final_holdout_accessed=false")
}
