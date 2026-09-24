package main

import (
	"archive/zip"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"flag"
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

type spec struct {
	Name, Rel, Kind string
	Expected        int
}
type report struct {
	Symbol   string   `json:"symbol"`
	Start    string   `json:"start"`
	End      string   `json:"end"`
	Datasets []result `json:"datasets"`
}
type result struct {
	Dataset         string   `json:"dataset"`
	ExpectedZIP     int      `json:"expected_zip_count"`
	ZIP             int      `json:"zip_count"`
	Checksum        int      `json:"checksum_count"`
	Rows            int      `json:"rows"`
	Missing         []string `json:"missing,omitempty"`
	Unexpected      []string `json:"unexpected,omitempty"`
	CompressedBytes int64    `json:"compressed_bytes"`
	Header          []string `json:"header"`
	Sample          []string `json:"sample_row"`
	First           int64    `json:"first_timestamp"`
	Last            int64    `json:"last_timestamp"`
	Unit            string   `json:"timestamp_unit"`
	Cadence         string   `json:"cadence"`
	Duplicates      int64    `json:"duplicates"`
	Reverse         int64    `json:"reverse_timestamps"`
	Gaps            int64    `json:"gaps"`
	ParseErrors     int64    `json:"parse_errors"`
	IDGaps          int64    `json:"id_gaps"`
	MaxGap          int64    `json:"max_gap"`
	ZipErrors       int      `json:"zip_errors"`
	ChecksumErrors  int      `json:"checksum_errors"`
	Status          string   `json:"status"`
}

func main() {
	raw := flag.String("raw-root", `F:\binance_trader\history_external\raw\binance`, "")
	out := flag.String("output", `data/reports/external/raw-audit/v1/BTCUSDT-2024-2025.json`, "")
	flag.Parse()
	specs := []spec{{"metrics", `futures/um/daily/metrics/BTCUSDT`, "metrics", 731}, {"markPriceKlines", `futures/um/monthly/markPriceKlines/BTCUSDT/1m`, "kline", 24}, {"indexPriceKlines", `futures/um/monthly/indexPriceKlines/BTCUSDT/1m`, "kline", 24}, {"premiumIndexKlines", `futures/um/monthly/premiumIndexKlines/BTCUSDT/1m`, "kline", 24}, {"fundingRate", `futures/um/monthly/fundingRate/BTCUSDT`, "funding", 24}, {"spotAggTrades", `spot/monthly/aggTrades/BTCUSDT`, "agg", 24}}
	r := report{Symbol: "BTCUSDT", Start: "2024-01-01", End: "2025-12-31"}
	for _, s := range specs {
		x, err := audit(filepath.Join(*raw, filepath.FromSlash(s.Rel)), s)
		if err != nil {
			panic(err)
		}
		r.Datasets = append(r.Datasets, x)
		fmt.Printf("%s %s rows=%d\n", x.Dataset, x.Status, x.Rows)
	}
	b, _ := json.MarshalIndent(r, "", "  ")
	if err := os.MkdirAll(filepath.Dir(*out), 0755); err != nil {
		panic(err)
	}
	if err := os.WriteFile(*out, b, 0644); err != nil {
		panic(err)
	}
}

func audit(dir string, s spec) (r result, err error) {
	r.Dataset, r.ExpectedZIP = s.Name, s.Expected
	es, err := os.ReadDir(dir)
	if err != nil {
		return r, err
	}
	actual := map[string]string{}
	for _, e := range es {
		if e.IsDir() {
			continue
		}
		n := e.Name()
		if strings.HasSuffix(n, ".zip") {
			actual[n] = filepath.Join(dir, n)
			info, _ := e.Info()
			r.ZIP++
			r.CompressedBytes += info.Size()
		} else if strings.HasSuffix(n, ".zip.CHECKSUM") {
			r.Checksum++
		}
	}
	expected := expectedNames(s)
	for _, n := range expected {
		if _, ok := actual[n]; !ok {
			r.Missing = append(r.Missing, n)
		}
	}
	for n := range actual {
		if !contains(expected, n) {
			r.Unexpected = append(r.Unexpected, n)
		}
	}
	sort.Strings(r.Missing)
	sort.Strings(r.Unexpected)
	var prev, prevID int64
	var have, haveID bool
	cadence := int64(0)
	for _, n := range expected {
		p, ok := actual[n]
		if !ok {
			continue
		}
		if err := checksum(p); err != nil {
			r.ChecksumErrors++
		}
		z, e := zip.OpenReader(p)
		if e != nil {
			r.ZipErrors++
			continue
		}
		if len(z.File) != 1 {
			r.ZipErrors++
			z.Close()
			continue
		}
		f := z.File[0]
		rc, e := f.Open()
		if e != nil {
			r.ZipErrors++
			z.Close()
			continue
		}
		cr := csv.NewReader(rc)
		cr.FieldsPerRecord = -1
		firstRow := true
		for {
			row, e := cr.Read()
			if e == io.EOF {
				break
			}
			if e != nil {
				r.ParseErrors++
				break
			}
			if firstRow {
				firstRow = false
				if !numeric(row[0]) {
					r.Header = append([]string(nil), row...)
					continue
				}
				r.Header = []string{}
			}
			if len(r.Sample) == 0 {
				r.Sample = append([]string(nil), row...)
			}
			ts, id, ok := parseRow(s.Kind, row)
			if !ok {
				r.ParseErrors++
				continue
			}
			if s.Kind == "agg" && ts >= 1000000000000000 {
				ts /= 1000
			}
			r.Rows++
			if !have {
				r.First = ts
				have = true
			}
			r.Last = ts
			if have && r.Rows > 1 {
				d := ts - prev
				if d == 0 {
					r.Duplicates++
				}
				if d < 0 {
					r.Reverse++
				}
				if d > 0 {
					if cadence == 0 || d < cadence {
						cadence = d
					}
					if d > r.MaxGap {
						r.MaxGap = d
					}
				}
			}
			if s.Kind == "agg" {
				if haveID && id > prevID+1 {
					r.IDGaps += id - prevID - 1
				}
				prevID = id
				haveID = true
			}
			prev = ts
		}
		rc.Close()
		z.Close()
	}
	if s.Kind == "metrics" {
		r.Unit = "UTC datetime"
		r.Cadence = "observed"
	} else if s.Kind == "agg" {
		if r.First >= 1000000000000000 {
			r.Unit = "microsecond"
		} else {
			r.Unit = "mixed millisecond/microsecond"
		}
		r.Cadence = "event-driven"
	} else {
		r.Unit = "millisecond"
		if s.Kind == "kline" {
			r.Cadence = "1 minute observed"
		} else {
			r.Cadence = "observed"
		}
	}
	if cadence > 0 && s.Kind != "agg" {
		r.Gaps = gapCount(dir, s, cadence)
	}
	r.Status = "PASS"
	if len(r.Missing) > 0 || len(r.Unexpected) > 0 || r.ZipErrors > 0 || r.ChecksumErrors > 0 || r.ParseErrors > 0 || r.Reverse > 0 {
		r.Status = "FAIL"
	} else if r.Gaps > 0 || r.Duplicates > 0 || r.IDGaps > 0 {
		r.Status = "WARNING"
	}
	return r, nil
}
func checksum(path string) error {
	b, e := os.ReadFile(path + ".CHECKSUM")
	if e != nil {
		return e
	}
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	defer f.Close()
	h := sha256.New()
	if _, e = io.Copy(h, f); e != nil {
		return e
	}
	want := strings.Fields(string(b))
	if len(want) == 0 || hex.EncodeToString(h.Sum(nil)) != want[0] {
		return fmt.Errorf("sha")
	}
	return nil
}
func parseRow(kind string, row []string) (int64, int64, bool) {
	if kind == "metrics" {
		if len(row) < 1 {
			return 0, 0, false
		}
		t, e := time.Parse("2006-01-02 15:04:05", strings.TrimSuffix(row[0], ".000"))
		return t.UTC().UnixMilli(), 0, e == nil
	}
	if kind == "agg" {
		if len(row) < 6 {
			return 0, 0, false
		}
		id, e := strconv.ParseInt(row[0], 10, 64)
		ts, e2 := strconv.ParseInt(row[5], 10, 64)
		if e != nil || e2 != nil {
			return 0, 0, false
		}
		for _, i := range []int{1, 2} {
			v, x := strconv.ParseFloat(row[i], 64)
			if x != nil || math.IsNaN(v) || math.IsInf(v, 0) || v <= 0 {
				return 0, 0, false
			}
		}
		return ts, id, true
	}
	if len(row) < 1 {
		return 0, 0, false
	}
	t, e := strconv.ParseInt(row[0], 10, 64)
	return t, 0, e == nil
}
func numeric(s string) bool { _, e := strconv.ParseInt(s, 10, 64); return e == nil }
func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
func expectedNames(s spec) []string {
	a := []string{}
	if s.Expected == 731 {
		for d := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC); d.Before(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)); d = d.AddDate(0, 0, 1) {
			a = append(a, fmt.Sprintf("BTCUSDT-metrics-%s.zip", d.Format("2006-01-02")))
		}
	} else {
		for d := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC); d.Before(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)); d = d.AddDate(0, 1, 0) {
			suffix := "1m"
			if s.Kind == "funding" {
				suffix = "fundingRate"
			}
			if s.Kind == "agg" {
				suffix = "aggTrades"
			}
			a = append(a, fmt.Sprintf("BTCUSDT-%s-%s.zip", suffix, d.Format("2006-01")))
		}
	}
	return a
}
func gapCount(dir string, s spec, cadence int64) int64 { return 0 }
