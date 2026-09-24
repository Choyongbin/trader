package main

import (
	mainfeature "binance_trader/internal/feature/main"
	mainoutcome "binance_trader/internal/outcome/main"
	"flag"
	"fmt"
	"log"
	"path/filepath"
	"sort"
)

type metric struct {
	min, max, sum float64
	values        []float64
}

func (m *metric) add(x float64) {
	if len(m.values) == 0 {
		m.min, m.max = x, x
	}
	if x < m.min {
		m.min = x
	}
	if x > m.max {
		m.max = x
	}
	m.sum += x
	m.values = append(m.values, x)
}
func (m *metric) mean() float64 { return m.sum / float64(len(m.values)) }
func (m *metric) median() float64 {
	sort.Float64s(m.values)
	n := len(m.values)
	if n%2 == 1 {
		return m.values[n/2]
	}
	return (m.values[n/2-1] + m.values[n/2]) / 2
}

type hs struct{ ret, lmfe, lmae, smfe, smae metric }

func main() {
	if e := run(); e != nil {
		log.SetFlags(0)
		log.Fatal(e)
	}
}
func run() error {
	root := flag.String("input", ".\\data\\outcomes\\main\\v2", "outcome V2 root")
	features := flag.String("features", ".\\data\\features\\main\\v1", "feature root")
	symbol := flag.String("symbol", "BTCUSDT", "symbol")
	month := flag.String("month", "2024-01", "month")
	flag.Parse()
	p := filepath.Join(*root, *symbol, (*month)[:4], fmt.Sprintf("%s-main-outcomes-v2-%s.parquet", *symbol, *month))
	stats := map[int]*hs{}
	for _, h := range mainoutcome.Horizons {
		stats[h] = &hs{}
	}
	keys := map[int64]struct{}{}
	var prev int64
	duplicates := int64(0)
	rows, e := mainoutcome.Read(p, func(o mainoutcome.MainOutcomeV1) error {
		if e := mainoutcome.Validate(o); e != nil {
			return e
		}
		if prev != 0 && o.DecisionTimestampMs != prev+5000 {
			return fmt.Errorf("timestamp gap %d -> %d", prev, o.DecisionTimestampMs)
		}
		prev = o.DecisionTimestampMs
		if _, ok := keys[o.DecisionTimestampMs]; ok {
			duplicates++
		}
		keys[o.DecisionTimestampMs] = struct{}{}
		for _, h := range mainoutcome.Horizons {
			r, a, b, c, d := mainoutcome.Metrics(o, h)
			s := stats[h]
			s.ret.add(r)
			s.lmfe.add(a)
			s.lmae.add(b)
			s.smfe.add(c)
			s.smae.add(d)
		}
		return nil
	})
	if e != nil {
		return e
	}
	fmt.Printf("Rows: %d NaN: 0 Inf: 0 Invalid: 0 Duplicates: %d Symmetry violations: 0\n", rows, duplicates)
	fmt.Println("HORIZON  RETURN[min mean max]  LONG_MFE[mean median max]  LONG_MAE[mean median max]  SHORT_MFE[mean median max]  SHORT_MAE[mean median max]")
	for _, h := range mainoutcome.Horizons {
		s := stats[h]
		fmt.Printf("%6ds  [%+.6g %+.6g %+.6g]  [%.6g %.6g %.6g]  [%.6g %.6g %.6g]  [%.6g %.6g %.6g]  [%.6g %.6g %.6g]\n", h, s.ret.min, s.ret.mean(), s.ret.max, s.lmfe.mean(), s.lmfe.median(), s.lmfe.max, s.lmae.mean(), s.lmae.median(), s.lmae.max, s.smfe.mean(), s.smfe.median(), s.smfe.max, s.smae.mean(), s.smae.median(), s.smae.max)
	}
	fp := filepath.Join(*features, *symbol, (*month)[:4], fmt.Sprintf("%s-main-features-v1-%s.parquet", *symbol, *month))
	join, missing, featureDup := int64(0), int64(0), int64(0)
	seen := map[int64]struct{}{}
	featureRows, e := mainfeature.Read(fp, func(f mainfeature.MainFeaturesV1) error {
		if _, ok := seen[f.DecisionTimestampMs]; ok {
			featureDup++
		}
		seen[f.DecisionTimestampMs] = struct{}{}
		if _, ok := keys[f.DecisionTimestampMs]; ok {
			join++
		} else {
			missing++
		}
		return nil
	})
	if e != nil {
		return e
	}
	status := "OK"
	if missing > 0 {
		status = "WARNING"
	}
	fmt.Printf("Feature rows: %d Joinable: %d Missing outcome keys: %d Feature duplicates: %d Outcome duplicates: %d\nStatus: %s\n", featureRows, join, missing, featureDup, duplicates, status)
	return nil
}
