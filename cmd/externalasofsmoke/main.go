package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"binance_trader/internal/external/asof"
	"binance_trader/internal/market"
	"github.com/parquet-go/parquet-go"
)

type metricRow struct {
	Timestamp int64 `parquet:"timestamp"`
}
type klineRow struct {
	OpenTime  int64 `parquet:"open_time"`
	CloseTime int64 `parquet:"close_time"`
}
type fundingRow struct {
	CalcTime        int64   `parquet:"calc_time"`
	LastFundingRate float64 `parquet:"last_funding_rate"`
}
type report struct {
	PolicyVersion                                                     int
	ExternalSafetyLagMs                                               int64
	AvailabilityRules, Freshness, MissingPolicy, HistoricalLiveParity map[string]string
	FutureLookupForbidden, InterpolationForbidden                     bool
	Examples                                                          map[string]int64
	BoundaryTests, RealSmoke, Status                                  string
}

func main() {
	examples := map[string]int64{}
	metricBefore, metricAfter := firstGap[metricRow](`data/external/metrics/v1/BTCUSDT/*.parquet`, 300_000, func(r metricRow) int64 { return r.Timestamp })
	examples["metrics_gap_before_ms"], examples["metrics_gap_after_ms"] = metricBefore.Timestamp, metricAfter.Timestamp
	mObs := []asof.Observation[int]{{Value: 1, SourceTimestampMs: metricBefore.Timestamp}, {Value: 2, SourceTimestampMs: metricAfter.Timestamp}}
	decision := metricBefore.Timestamp + 300_000 + asof.ExternalSafetyLagMs
	mv, err := asof.Lookup(asof.Metrics, mObs, decision)
	must(err == nil && mv.Available && mv.Value == 1, "metrics gap future lookup")
	markBefore, markAfter := firstGap[klineRow](`data/external/markprice/v1/BTCUSDT/*.parquet`, 60_000, func(r klineRow) int64 { return r.OpenTime })
	examples["kline_gap_before_ms"], examples["kline_gap_after_ms"] = markBefore.OpenTime, markAfter.OpenTime
	for _, item := range []struct {
		s    asof.Source
		glob string
	}{{asof.Mark, `data/external/markprice/v1/BTCUSDT/*.parquet`}, {asof.Index, `data/external/indexprice/v1/BTCUSDT/*.parquet`}, {asof.Premium, `data/external/premiumindex/v1/BTCUSDT/*.parquet`}} {
		gapBefore, gapAfter := firstGap[klineRow](item.glob, 60_000, func(r klineRow) int64 { return r.OpenTime })
		must(gapBefore.OpenTime == markBefore.OpenTime && gapAfter.OpenTime == markAfter.OpenTime, "kline gap alignment")
		r := first[klineRow](item.glob)
		v, e := asof.Lookup(item.s, []asof.Observation[int]{{Value: 1, SourceTimestampMs: r.OpenTime, CloseTimeMs: r.CloseTime}}, r.CloseTime+1+asof.ExternalSafetyLagMs)
		must(e == nil && v.Available, "kline real boundary")
	}
	f := first[fundingRow](`data/external/funding/v1/BTCUSDT/*.parquet`)
	fv, e := asof.Lookup(asof.Funding, []asof.Observation[float64]{{Value: f.LastFundingRate, SourceTimestampMs: f.CalcTime}}, f.CalcTime+asof.ExternalSafetyLagMs)
	must(e == nil && fv.Available, "funding real boundary")
	examples["funding_timestamp_ms"] = f.CalcTime
	for _, x := range []struct{ name, path string }{{"spot_2024", `data/external/spot/1s/v1/BTCUSDT/BTCUSDT-spot-1s-2024-01.parquet`}, {"spot_2025", `data/external/spot/1s/v1/BTCUSDT/BTCUSDT-spot-1s-2025-01.parquet`}} {
		b := firstPath[market.SecondBar](x.path)
		v, e := asof.Lookup(asof.Spot, []asof.Observation[float64]{{Value: b.Close, SourceTimestampMs: b.TimestampMs}}, b.TimestampMs+1000)
		must(e == nil && v.Available, x.name)
		examples[x.name+"_timestamp_ms"] = b.TimestampMs
	}
	r := report{PolicyVersion: 1, ExternalSafetyLagMs: asof.ExternalSafetyLagMs, AvailabilityRules: map[string]string{"spot": "exact decision-1000ms completed second", "mark/index/premium": "close boundary + 5000ms", "metrics": "observation + 5000ms", "funding": "event + 5000ms"}, Freshness: map[string]string{"spot": "exact previous second", "mark/index/premium": "120000ms", "metrics": "600000ms", "funding": "57600000ms"}, MissingPolicy: map[string]string{"value": "optional", "fill": "none", "stale": "returned with Fresh=false"}, HistoricalLiveParity: map[string]string{"historical": "source availability + fixed lag", "live": "max(source availability, local receive timestamp)"}, FutureLookupForbidden: true, InterpolationForbidden: true, Examples: examples, BoundaryTests: "PASS", RealSmoke: "PASS", Status: "PASS"}
	b, _ := json.MarshalIndent(r, "", "  ")
	p := `data/reports/external/join-policy/v1/BTCUSDT-external-join-policy-v1.json`
	os.MkdirAll(filepath.Dir(p), 0755)
	if err := os.WriteFile(p, b, 0644); err != nil {
		panic(err)
	}
	fmt.Printf("REAL SMOKE PASS metrics_gap=%d..%d kline_gap=%d..%d funding=%d spot_2024=%d spot_2025=%d\n", metricBefore.Timestamp, metricAfter.Timestamp, markBefore.OpenTime, markAfter.OpenTime, f.CalcTime, examples["spot_2024_timestamp_ms"], examples["spot_2025_timestamp_ms"])
}
func must(ok bool, name string) {
	if !ok {
		panic(name)
	}
}
func first[T any](glob string) T {
	paths, _ := filepath.Glob(glob)
	sort.Strings(paths)
	return firstPath[T](paths[0])
}
func firstPath[T any](path string) T {
	f, e := os.Open(path)
	if e != nil {
		panic(e)
	}
	defer f.Close()
	r := parquet.NewGenericReader[T](f)
	defer r.Close()
	b := make([]T, 1)
	n, e := r.Read(b)
	if n != 1 || (e != nil && e != io.EOF) {
		panic("first row")
	}
	return b[0]
}
func firstGap[T any](glob string, cadence int64, ts func(T) int64) (T, T) {
	paths, _ := filepath.Glob(glob)
	sort.Strings(paths)
	var prev T
	have := false
	for _, p := range paths {
		f, e := os.Open(p)
		if e != nil {
			panic(e)
		}
		r := parquet.NewGenericReader[T](f)
		b := make([]T, 2048)
		for {
			n, e := r.Read(b)
			for _, x := range b[:n] {
				if have && ts(x)-ts(prev) > cadence {
					r.Close()
					f.Close()
					return prev, x
				}
				prev = x
				have = true
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
	panic("gap not found")
}
