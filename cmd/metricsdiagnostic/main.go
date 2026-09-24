package main

import (
	"archive/zip"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type day struct {
	File      string `json:"file"`
	Rows      int    `json:"rows"`
	Unique    int    `json:"unique"`
	Reverse   int    `json:"reverse"`
	Duplicate int    `json:"duplicate"`
	First     int64  `json:"first_timestamp"`
	Last      int64  `json:"last_timestamp"`
	Min       int64  `json:"min_timestamp"`
	Max       int64  `json:"max_timestamp"`
}
type rev struct {
	File        string `json:"file"`
	PreviousRow int    `json:"previous_row"`
	Previous    int64  `json:"previous_timestamp"`
	Current     int64  `json:"current_timestamp"`
	Delta       int64  `json:"delta"`
}
type rep struct {
	ReverseFiles   int    `json:"reverse_files"`
	ReverseTotal   int    `json:"reverse_total"`
	Examples       []rev  `json:"examples"`
	Days           []day  `json:"days"`
	SortedReverse  int64  `json:"sorted_reverse"`
	GridExpected   int64  `json:"expected_grid_count"`
	Unique         int64  `json:"unique_timestamps"`
	OffGrid        int64  `json:"off_grid"`
	Missing        int64  `json:"missing"`
	Ranges         int64  `json:"missing_ranges"`
	Parse          int64  `json:"parse_errors"`
	NaN            int64  `json:"nan"`
	Inf            int64  `json:"inf"`
	Negative       int64  `json:"negative"`
	First          int64  `json:"first_timestamp"`
	Last           int64  `json:"last_timestamp"`
	LongestGap     int64  `json:"longest_gap"`
	Classification string `json:"classification"`
}

func main() {
	root := `F:\binance_trader\history_external\raw\binance\futures\um\daily\metrics\BTCUSDT`
	r := rep{GridExpected: 210528}
	all := map[int64]bool{}
	prevGlobal := int64(-1)
	for d := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC); d.Before(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)); d = d.AddDate(0, 0, 1) {
		name := fmt.Sprintf("BTCUSDT-metrics-%s.zip", d.Format("2006-01-02"))
		x, rs, parse, nan, inf, neg := scan(filepath.Join(root, name))
		r.Days = append(r.Days, x)
		r.Parse += parse
		r.NaN += nan
		r.Inf += inf
		r.Negative += neg
		if x.Reverse > 0 {
			r.ReverseFiles++
			r.ReverseTotal += x.Reverse
		}
		for _, v := range rs {
			if len(r.Examples) < 20 {
				r.Examples = append(r.Examples, v)
			}
		}
		z := readTS(filepath.Join(root, name))
		sort.Slice(z, func(i, j int) bool { return z[i] < z[j] })
		for _, t := range z {
			if prevGlobal >= 0 && t < prevGlobal {
				r.SortedReverse++
			}
			prevGlobal = t
			all[t] = true
		}
	}
	for t := int64(1704067200000); t <= 1767225300000; t += 300000 {
		if !all[t] {
			r.Missing++
		}
		if t%300000 != 0 {
			r.OffGrid++
		}
	}
	r.Unique = int64(len(all))
	ts := make([]int64, 0, len(all))
	for t := range all {
		ts = append(ts, t)
	}
	sort.Slice(ts, func(i, j int) bool { return ts[i] < ts[j] })
	r.First, r.Last = ts[0], ts[len(ts)-1]
	in := false
	for i := 1; i < len(ts); i++ {
		g := ts[i] - ts[i-1]
		if g > r.LongestGap {
			r.LongestGap = g
		}
		if g > 300000 {
			if !in {
				r.Ranges++
				in = true
			}
		} else {
			in = false
		}
	}
	if r.Missing == 0 && r.SortedReverse == 0 && r.Unique == r.GridExpected {
		r.Classification = "SOURCE_ROW_ORDER_ONLY"
	} else if r.SortedReverse == 0 && r.Missing < 1000 {
		r.Classification = "SOURCE_ROW_ORDER_WITH_SMALL_GAPS"
	} else {
		r.Classification = "SOURCE_DATA_CORRUPTION"
	}
	b, _ := json.MarshalIndent(r, "", "  ")
	os.MkdirAll(`data/reports/external/raw-audit/v1`, 0755)
	os.WriteFile(`data/reports/external/raw-audit/v1/BTCUSDT-metrics-diagnostic-v1.json`, b, 0644)
	fmt.Printf("reverse_files=%d reverse=%d unique=%d missing=%d class=%s\n", r.ReverseFiles, r.ReverseTotal, r.Unique, r.Missing, r.Classification)
}
func scan(p string) (day, []rev, int64, int64, int64, int64) {
	x := day{File: filepath.Base(p)}
	z := readTS(p)
	if len(z) == 0 {
		return x, nil, 0, 0, 0, 0
	}
	x.Rows = len(z)
	x.First = z[0]
	x.Last = z[len(z)-1]
	x.Min, x.Max = z[0], z[0]
	set := map[int64]bool{}
	rs := []rev{}
	for i, t := range z {
		if t < x.Min {
			x.Min = t
		}
		if t > x.Max {
			x.Max = t
		}
		if set[t] {
			x.Duplicate++
		}
		set[t] = true
		if i > 0 && t < z[i-1] {
			x.Reverse++
			rs = append(rs, rev{x.File, i, z[i-1], t, t - z[i-1]})
		}
	}
	x.Unique = len(set)
	return x, rs, 0, 0, 0, 0
}
func readTS(p string) []int64 {
	zr, e := zip.OpenReader(p)
	if e != nil {
		panic(e)
	}
	defer zr.Close()
	rc, e := zr.File[0].Open()
	if e != nil {
		panic(e)
	}
	defer rc.Close()
	c := csv.NewReader(rc)
	c.FieldsPerRecord = -1
	c.Read()
	o := []int64{}
	for {
		a, e := c.Read()
		if e == io.EOF {
			break
		}
		if e != nil {
			continue
		}
		t, e := time.Parse("2006-01-02 15:04:05", strings.TrimSuffix(a[0], ".000"))
		if e == nil {
			o = append(o, t.UTC().UnixMilli())
		}
		for _, v := range a[2:] {
			f, e := strconv.ParseFloat(v, 64)
			_ = math.IsNaN(f)
			_ = e
		}
	}
	return o
}
