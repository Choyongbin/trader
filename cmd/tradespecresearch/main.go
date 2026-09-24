package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"

	mainbarrier "binance_trader/internal/barrier/main"
	mainfeature "binance_trader/internal/feature/main"
	research "binance_trader/internal/research/tradespec"
	"binance_trader/internal/tradelabel"
)

type key struct{ ID, Side string }
type row struct {
	ID                   string           `json:"id"`
	Side                 string           `json:"side"`
	Spec                 research.Spec    `json:"spec"`
	DependencyMs         int64            `json:"max_label_dependency_ms"`
	Summary              research.Summary `json:"metrics"`
	DeltaMeanNet         float64          `json:"delta_mean_net_vs_control"`
	DeltaMedianNet       float64          `json:"delta_median_net_vs_control"`
	DeltaWinRate         float64          `json:"delta_win_rate_vs_control"`
	DeltaValidRate       float64          `json:"delta_valid_rate_vs_control"`
	DeltaNonOverlap      float64          `json:"delta_non_overlap_mean_vs_control"`
	Eligible             bool             `json:"eligible"`
	Improved             bool             `json:"improved"`
	PositiveCandidate    bool             `json:"positive_expectancy_candidate"`
	ConfirmedImprovement bool             `json:"confirmed_improvement"`
	ConfirmedPositive    bool             `json:"confirmed_positive_expectancy"`
}
type report struct {
	ResearchVersion               int             `json:"research_version"`
	Symbol                        string          `json:"symbol"`
	FeatureVersion                int             `json:"feature_version"`
	BarrierVersion                int             `json:"barrier_version"`
	CostProfile                   string          `json:"cost_profile"`
	FundingIncluded               bool            `json:"funding_included"`
	TriggerPriceSource            string          `json:"trigger_price_source"`
	Grid                          []research.Spec `json:"grid"`
	GridHash                      string          `json:"grid_hash"`
	Control                       research.Spec   `json:"control_spec"`
	QuantileMethod                string          `json:"percentile_method"`
	MemoryStrategy                string          `json:"memory_strategy"`
	TempDiskStrategy              string          `json:"temporary_disk_strategy"`
	Train                         []row           `json:"train"`
	Validation                    []row           `json:"validation"`
	Test                          []row           `json:"test"`
	LongShortlist                 []string        `json:"long_shortlist_frozen"`
	ShortShortlist                []string        `json:"short_shortlist_frozen"`
	FinalHoldoutAccessed          bool            `json:"final_holdout_accessed"`
	TestUsedForCandidateSelection bool            `json:"test_used_for_candidate_selection"`
	SourceScans                   int             `json:"source_scans"`
	PeakHeapBytes                 uint64          `json:"peak_heap_bytes"`
	ElapsedMs                     int64           `json:"elapsed_ms"`
}

func main() {
	log.SetFlags(0)
	if e := run(); e != nil {
		log.Fatal(e)
	}
}
func run() error {
	featureRoot := flag.String("feature-root", `.\data\features\main\v1\BTCUSDT`, "features")
	barrierRoot := flag.String("barrier-root", `.\data\barriers\main\v2\delay_0ms\BTCUSDT`, "barriers")
	manifestRoot := flag.String("barrier-manifest-root", `.\data\manifests\barriers\main\v2\delay_0ms\BTCUSDT`, "manifests")
	temp := flag.String("temp", `.\.tmp_tradespec_research`, "temporary exact-return files")
	output := flag.String("output", `.\data\reports\research\tradespec\main\v1\BTCUSDT-tradespec-v1.json`, "report")
	config := flag.String("config", `.\data\manifests\research\tradespec\main\v1\BTCUSDT-tradespec-v1.json`, "config")
	flag.Parse()
	started := time.Now()
	m, e := readManifest(filepath.Join(*manifestRoot, "BTCUSDT-main-barriers-v2-2024-01-delay_0ms.json"))
	if e != nil {
		return e
	}
	g := research.Grid()
	if e = research.ValidateGrid(m, g); e != nil {
		return e
	}
	cost := tradelabel.CostProfile{EntryFeeRate: .0004, TPExitFeeRate: .0004, SLExitFeeRate: .0004, TimeoutExitFeeRate: .0004, EntrySlippageBps: 1, TPExitSlippageBps: 1, SLExitSlippageBps: 1, TimeoutSlippageBps: 1}
	r := report{ResearchVersion: 1, Symbol: "BTCUSDT", FeatureVersion: 1, BarrierVersion: 2, CostProfile: "synthetic_validation_v1", FundingIncluded: false, TriggerPriceSource: mainbarrier.TriggerPriceSourceContractPrice, Grid: g, GridHash: research.GridHash(g), Control: research.Control(), QuantileMethod: "linear interpolation over sorted exact valid returns (p*(n-1))", MemoryStrategy: "limited spec batch of 4 specs; exact return pairs are sorted one side-spec at a time", TempDiskStrategy: "temporary binary net/gross return pairs; removed after each finalized batch", FinalHoldoutAccessed: false, TestUsedForCandidateSelection: false}
	r.Train, e = eval("TRAIN", months(2024, 1, 9), g, nil, *featureRoot, *barrierRoot, *temp, m, cost)
	if e != nil {
		return e
	}
	r.SourceScans += 7
	r.Validation, e = eval("VALIDATION", months(2024, 10, 12), g, nil, *featureRoot, *barrierRoot, *temp, m, cost)
	if e != nil {
		return e
	}
	r.SourceScans += 7
	r.LongShortlist = shortlist(r.Train, r.Validation, "LONG")
	r.ShortShortlist = shortlist(r.Train, r.Validation, "SHORT")
	annotateSelection(r.Train, r.Validation, r.LongShortlist, r.ShortShortlist)
	// Shortlists are now frozen; only control and frozen IDs can reach TEST.
	wanted := []research.Spec{research.Control()}
	for _, id := range append(append([]string{}, r.LongShortlist...), r.ShortShortlist...) {
		for _, s := range g {
			if s.ID() == id {
				wanted = appendUnique(wanted, s)
			}
		}
	}
	allowed := map[key]bool{{research.Control().ID(), "LONG"}: true, {research.Control().ID(), "SHORT"}: true}
	for _, id := range r.LongShortlist {
		allowed[key{id, "LONG"}] = true
	}
	for _, id := range r.ShortShortlist {
		allowed[key{id, "SHORT"}] = true
	}
	r.Test, e = eval("TEST", months(2025, 1, 6), wanted, allowed, *featureRoot, *barrierRoot, *temp, m, cost)
	if e != nil {
		return e
	}
	r.SourceScans += (len(wanted) + 3) / 4
	markTest(r.Test, research.Control())
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	r.PeakHeapBytes = ms.HeapSys
	r.ElapsedMs = time.Since(started).Milliseconds()
	if e = writeJSON(*output, r); e != nil {
		return e
	}
	if e = writeJSON(*config, map[string]any{"research_version": 1, "grid": g, "grid_hash": r.GridHash, "control_spec": r.Control, "cost_profile": "synthetic_validation_v1", "funding_included": false, "final_holdout_accessed": false, "test_used_for_candidate_selection": false}); e != nil {
		return e
	}
	if e = os.RemoveAll(*temp); e != nil {
		return e
	}
	fmt.Printf("train=%d validation=%d long_shortlist=%v short_shortlist=%v test=%d elapsed=%s\n", len(r.Train), len(r.Validation), r.LongShortlist, r.ShortShortlist, len(r.Test), time.Since(started).Round(time.Second))
	return nil
}
func eval(part string, mons []string, specs []research.Spec, allowed map[key]bool, fr, br, tmp string, m mainbarrier.ManifestV2, c tradelabel.CostProfile) ([]row, error) {
	out := []row{}
	for start := 0; start < len(specs); start += 4 {
		end := start + 4
		if end > len(specs) {
			end = len(specs)
		}
		batch := specs[start:end]
		acc := map[key]*research.Metrics{}
		for _, s := range batch {
			for _, side := range []tradelabel.Side{tradelabel.Long, tradelabel.Short} {
				k := key{s.ID(), string(side)}
				if allowed != nil && !allowed[k] {
					continue
				}
				a, e := research.NewMetrics(filepath.Join(tmp, part, s.ID()+"-"+string(side)+".bin"))
				if e != nil {
					return nil, e
				}
				acc[k] = a
			}
		}
		for _, mon := range mons {
			keys, e := featureKeys(filepath.Join(fr, mon[:4], fmt.Sprintf("BTCUSDT-main-features-v1-%s.parquet", mon)))
			if e != nil {
				return nil, e
			}
			p := filepath.Join(br, mon[:4], fmt.Sprintf("BTCUSDT-main-barriers-v2-%s-delay_0ms.parquet", mon))
			_, e = mainbarrier.ReadV2(p, func(b mainbarrier.BarrierOutcomeV2) error {
				if !keys[b.DecisionTimestampMs] {
					return nil
				}
				for _, s := range batch {
					dep := research.Dependency(m, s.HorizonSeconds)
					if !included(part, b.DecisionTimestampMs, dep) {
						continue
					}
					for _, side := range []tradelabel.Side{tradelabel.Long, tradelabel.Short} {
						a := acc[key{s.ID(), string(side)}]
						if a == nil {
							continue
						}
						if e := a.Add(b, s, side, c); e != nil {
							return e
						}
					}
				}
				return nil
			})
			if e != nil {
				return nil, e
			}
		}
		for _, s := range batch {
			for _, side := range []tradelabel.Side{tradelabel.Long, tradelabel.Short} {
				a := acc[key{s.ID(), string(side)}]
				if a == nil {
					continue
				}
				z, e := a.Finish()
				if e != nil {
					return nil, e
				}
				_ = os.Remove(a.Path)
				out = append(out, row{ID: s.ID(), Side: string(side), Spec: s, DependencyMs: research.Dependency(m, s.HorizonSeconds), Summary: z})
			}
		}
	}
	addDeltas(out, research.Control())
	return out, nil
}
func featureKeys(path string) (map[int64]bool, error) {
	x := map[int64]bool{}
	_, e := mainfeature.Read(path, func(f mainfeature.MainFeaturesV1) error { x[f.DecisionTimestampMs] = true; return nil })
	return x, e
}
func included(p string, ts, dep int64) bool {
	var lo, hi int64
	switch p {
	case "TRAIN":
		lo = 1704067200000
		hi = 1727740800000
	case "VALIDATION":
		lo = 1727740800000
		hi = 1735689600000
	case "TEST":
		lo = 1735689600000
		hi = 1751328000000
	}
	return ts >= lo && ts < hi && ts+dep < hi
}
func addDeltas(x []row, c research.Spec) {
	base := map[string]research.Summary{}
	for _, v := range x {
		if v.ID == c.ID() {
			base[v.Side] = v.Summary
		}
	}
	for i := range x {
		b := base[x[i].Side]
		x[i].DeltaMeanNet = x[i].Summary.MeanNet - b.MeanNet
		x[i].DeltaMedianNet = x[i].Summary.MedianNet - b.MedianNet
		x[i].DeltaWinRate = x[i].Summary.WinRate - b.WinRate
		x[i].DeltaValidRate = x[i].Summary.ValidRate - b.ValidRate
		x[i].DeltaNonOverlap = x[i].Summary.NonOverlap.MeanNet - b.NonOverlap.MeanNet
	}
}
func shortlist(train, val []row, side string) []string {
	tm := map[string]row{}
	for _, x := range train {
		if x.Side == side {
			tm[x.ID] = x
		}
	}
	a := []row{}
	for _, x := range val {
		t := tm[x.ID]
		x.Eligible = t.Summary.ValidRate >= .9 && x.Summary.Daily.ActiveDays >= 80 && t.Summary.Daily.ActiveDays >= 200
		x.Improved = x.Eligible && t.DeltaMeanNet > 0 && x.DeltaMeanNet > 0 && x.DeltaMedianNet >= 0
		x.PositiveCandidate = x.Improved && t.Summary.MeanNet > 0 && x.Summary.MeanNet > 0 && x.Summary.NonOverlap.MeanNet > 0
		if x.Improved {
			a = append(a, x)
		}
	}
	sort.Slice(a, func(i, j int) bool {
		if a[i].Summary.MeanNet != a[j].Summary.MeanNet {
			return a[i].Summary.MeanNet > a[j].Summary.MeanNet
		}
		if a[i].Summary.LabelValid != a[j].Summary.LabelValid {
			return a[i].Summary.LabelValid > a[j].Summary.LabelValid
		}
		return a[i].ID < a[j].ID
	})
	if len(a) > 3 {
		a = a[:3]
	}
	o := []string{}
	for _, x := range a {
		o = append(o, x.ID)
	}
	return o
}
func annotateSelection(train []row, val []row, longIDs, shortIDs []string) {
	trainBy := map[string]row{}
	for _, x := range train {
		trainBy[x.Side+"/"+x.ID] = x
	}
	selected := map[string]bool{}
	for _, id := range longIDs {
		selected["LONG/"+id] = true
	}
	for _, id := range shortIDs {
		selected["SHORT/"+id] = true
	}
	for i := range val {
		t := trainBy[val[i].Side+"/"+val[i].ID]
		val[i].Eligible = t.Summary.ValidRate >= .9 && t.Summary.Daily.ActiveDays >= 200 && val[i].Summary.Daily.ActiveDays >= 80
		val[i].Improved = val[i].Eligible && t.DeltaMeanNet > 0 && val[i].DeltaMeanNet > 0 && val[i].DeltaMedianNet >= 0
		val[i].PositiveCandidate = val[i].Improved && t.Summary.MeanNet > 0 && val[i].Summary.MeanNet > 0 && val[i].Summary.NonOverlap.MeanNet > 0
		if selected[val[i].Side+"/"+val[i].ID] {
			val[i].Improved = true
		}
	}
}
func markTest(x []row, c research.Spec) {
	base := map[string]float64{}
	for _, v := range x {
		if v.ID == c.ID() {
			base[v.Side] = v.Summary.MeanNet
		}
	}
	for i := range x {
		if x[i].ID != c.ID() {
			x[i].ConfirmedImprovement = x[i].Summary.MeanNet > base[x[i].Side]
			x[i].ConfirmedPositive = x[i].Summary.MeanNet > 0 && x[i].Summary.NonOverlap.MeanNet > 0
		}
	}
}
func appendUnique(x []research.Spec, s research.Spec) []research.Spec {
	for _, v := range x {
		if v.ID() == s.ID() {
			return x
		}
	}
	return append(x, s)
}
func months(y, a, b int) []string {
	o := []string{}
	for m := a; m <= b; m++ {
		o = append(o, fmt.Sprintf("%04d-%02d", y, m))
	}
	return o
}
func readManifest(p string) (mainbarrier.ManifestV2, error) {
	var m mainbarrier.ManifestV2
	b, e := os.ReadFile(p)
	if e == nil {
		e = json.Unmarshal(b, &m)
	}
	return m, e
}
func writeJSON(p string, v any) error {
	b, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		return e
	}
	b = append(b, '\n')
	if e = os.MkdirAll(filepath.Dir(p), 0755); e != nil {
		return e
	}
	t := p + ".tmp"
	if e = os.WriteFile(t, b, 0644); e != nil {
		return e
	}
	_ = os.Remove(p)
	return os.Rename(t, p)
}
