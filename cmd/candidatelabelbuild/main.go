package main

import (
	mainbarrier "binance_trader/internal/barrier/main"
	mainfeature "binance_trader/internal/feature/main"
	labels "binance_trader/internal/label/tradespec"
	logistic "binance_trader/internal/model/logistic"
	research "binance_trader/internal/research/tradespec"
	mainsplit "binance_trader/internal/split/main"
	"binance_trader/internal/tradelabel"
	maintraining "binance_trader/internal/training/main"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

type c struct {
	id    string
	side  tradelabel.Side
	s     research.Spec
	dep   int64
	w     map[string]*labels.Writer
	stats map[string]*labels.Accumulator
}

func main() {
	if e := run(); e != nil {
		log.Fatal(e)
	}
}
func run() error {
	fr := flag.String("feature-root", `.\data\features\main\v1\BTCUSDT`, "")
	br := flag.String("barrier-root", `.\data\barriers\main\v2\delay_0ms\BTCUSDT`, "")
	out := flag.String("output-root", `.\data\labels\tradespec\v1\cost_synthetic_validation_v1\BTCUSDT`, "")
	rp := flag.String("phase9a-report", `.\data\reports\research\tradespec\main\v1\BTCUSDT-tradespec-v1.json`, "")
	manifestPath := flag.String("barrier-manifest", `.\data\manifests\barriers\main\v2\delay_0ms\BTCUSDT\BTCUSDT-main-barriers-v2-2024-01-delay_0ms.json`, "Barrier V2 source manifest")
	preflight := flag.Bool("preflight", false, "validate inputs without writing candidate artifacts")
	verifyResume := flag.Bool("verify-resume", false, "verify complete candidate artifacts without rebuilding")
	flag.Parse()
	phase9A, e := labels.LoadPhase9AReport(*rp)
	if e != nil {
		return fmt.Errorf("Phase 9A report: %w", e)
	}
	cs := []*c{}
	for _, reference := range phase9A.Frozen {
		candidate := reference.Candidate
		cs = append(cs, &c{id: candidate.SourceCandidateID, side: candidate.Side, s: research.Spec{TPBps: candidate.TPBps, SLBps: candidate.SLBps, HorizonSeconds: candidate.HorizonSeconds}, w: map[string]*labels.Writer{}, stats: map[string]*labels.Accumulator{}})
	}
	var m mainbarrier.ManifestV2
	mb, e := os.ReadFile(*manifestPath)
	if e != nil {
		return e
	}
	if e = json.Unmarshal(mb, &m); e != nil {
		return e
	}
	if m.BarrierVersion != mainbarrier.BarrierVersionV2 || m.TriggerPriceSource != mainbarrier.TriggerPriceSourceContractPrice {
		return fmt.Errorf("unexpected Barrier V2 source semantics")
	}
	for _, candidate := range cs {
		candidate.dep = research.Dependency(m, candidate.s.HorizonSeconds)
	}
	if *preflight {
		if e = runPreflight(*fr, *br, phase9A, m, cs); e != nil {
			return e
		}
		fmt.Println("PREFLIGHT PASS; final_holdout_accessed=false")
		return nil
	}
	if *verifyResume {
		registryHash := logistic.FeatureRegistryHash(maintraining.ModelFeatureColumns)
		for _, candidate := range cs {
			root := filepath.Join(*out, lower(string(candidate.side)), candidate.id)
			if e = labels.VerifyResume(root, labels.AuditExpectation{Phase9AReportSHA256: phase9A.SourceSHA256, FeatureRegistryHash: registryHash, BarrierVersion: mainbarrier.BarrierVersionV2, MaxDependencyMs: candidate.dep}); e != nil {
				return fmt.Errorf("%s resume: %w", candidate.id, e)
			}
		}
		fmt.Println("RESUME PASS; final_holdout_accessed=false")
		return nil
	}
	cost := tradelabel.CostProfile{EntryFeeRate: .0004, TPExitFeeRate: .0004, SLExitFeeRate: .0004, TimeoutExitFeeRate: .0004, EntrySlippageBps: 1, TPExitSlippageBps: 1, SLExitSlippageBps: 1, TimeoutSlippageBps: 1}
	for _, x := range cs {
		root := filepath.Join(*out, lower(string(x.side)), x.id)
		for _, p := range []string{"train", "validation", "test"} {
			w, e := labels.NewWriter(filepath.Join(root, p+".parquet"))
			if e != nil {
				return e
			}
			x.w[p] = w
			stats, e := labels.NewAccumulator(filepath.Join(root, ".statistics", p+".net.bin"))
			if e != nil {
				return e
			}
			x.stats[p] = stats
		}
	}
	for _, mon := range months() {
		keys := map[int64]bool{}
		_, e = mainfeature.Read(filepath.Join(*fr, mon[:4], fmt.Sprintf("BTCUSDT-main-features-v1-%s.parquet", mon)), func(z mainfeature.MainFeaturesV1) error { keys[z.DecisionTimestampMs] = true; return nil })
		if e != nil {
			return e
		}
		_, e = mainbarrier.ReadV2(filepath.Join(*br, mon[:4], fmt.Sprintf("BTCUSDT-main-barriers-v2-%s-delay_0ms.parquet", mon)), func(z mainbarrier.BarrierOutcomeV2) error {
			if !keys[z.DecisionTimestampMs] {
				return nil
			}
			for _, x := range cs {
				p := part(z.DecisionTimestampMs)
				if p == "" {
					continue
				}
				if z.DecisionTimestampMs+x.dep >= end(p) {
					x.stats[p].Purged()
					continue
				}
				r, e := tradelabel.EvaluateV2(z, tradelabel.TradeSpec{Side: x.side, TPBps: x.s.TPBps, SLBps: x.s.SLBps, HorizonSeconds: x.s.HorizonSeconds}, cost)
				if e != nil {
					return e
				}
				row := labels.Row{DecisionTimestampMs: z.DecisionTimestampMs, EntryReferenceAvailable: z.EntryReferenceAvailable, EntryWaitMs: z.EntryWaitMs, LabelValid: r.LabelValid, NetProfitableExFunding: r.NetProfitableExFunding, TradeResult: r.Status, GrossMarketReturn: r.GrossMarketReturn, FeeCostReturn: r.FeeCostReturn, ModeledSlippageCostReturn: r.ModeledSlippageCostReturn, NetReturnExFunding: r.NetReturnExFunding}
				if e = x.stats[p].Add(row); e != nil {
					return e
				}
				if e = x.w[p].Write(row); e != nil {
					return e
				}
			}
			return nil
		})
		if e != nil {
			return e
		}
	}
	for _, x := range cs {
		for _, w := range x.w {
			if e = w.Close(); e != nil {
				return e
			}
		}
		root := filepath.Join(*out, lower(string(x.side)), x.id)
		partitions := map[string]labels.PartitionStats{}
		actualPartitions := map[mainsplit.Partition]labels.Statistics{}
		for name, accumulator := range x.stats {
			stat, e := accumulator.Finalize()
			if e != nil {
				return fmt.Errorf("%s %s statistics: %w", x.id, name, e)
			}
			partitions[name] = labels.PartitionStats{
				RawDecisions: stat.RawDecisionCount,
				Purged:       stat.PurgedCount,
				Included:     stat.IncludedCount,
				Valid:        stat.LabelValidCount,
				Invalid:      stat.LabelInvalidCount,
				Positive:     stat.PositiveCount,
				Negative:     stat.NegativeCount,
				MeanNet:      stat.MeanNetReturnExFunding,
				MedianNet:    stat.MedianNetReturnExFunding,
				Statistics:   stat,
			}
			partition, e := splitPartition(name)
			if e != nil {
				return e
			}
			actualPartitions[partition] = stat
		}
		candidate := labels.Phase9ACandidate{SourceCandidateID: x.id, Side: x.side, TPBps: x.s.TPBps, SLBps: x.s.SLBps, HorizonSeconds: x.s.HorizonSeconds}
		reference, e := labels.FindPhase9AReference(phase9A, candidate)
		if e != nil {
			return fmt.Errorf("%s Phase 9A reference: %w", x.id, e)
		}
		if _, e = labels.CompareCandidateReproduction(labels.CandidateStatistics{Candidate: candidate, Partitions: actualPartitions, SourcePhase9AReportSHA256: phase9A.SourceSHA256}, reference, phase9A.SourceSHA256); e != nil {
			return fmt.Errorf("%s reproduction: %w", x.id, e)
		}
		manifest := labels.Manifest{CandidateLabelVersion: 1, CandidateID: x.id, Side: string(x.side), TPBps: x.s.TPBps, SLBps: x.s.SLBps, HorizonSeconds: x.s.HorizonSeconds, SourceFeatureVersion: 1, FeatureRegistryHash: logistic.FeatureRegistryHash(maintraining.ModelFeatureColumns), SourceBarrierVersion: 2, CostProfile: "synthetic_validation_v1", TriggerPriceSource: "CONTRACT_PRICE", MaxLabelDependencyMs: x.dep, Phase9AReportPath: phase9A.SourcePath, Phase9AReportSHA256: phase9A.SourceSHA256, ReproductionStatus: labels.ReproductionPassed, Partitions: partitions, Complete: false}
		if e = labels.PublishComplete(root, manifest, labels.AuditExpectation{Phase9AReportSHA256: phase9A.SourceSHA256, FeatureRegistryHash: manifest.FeatureRegistryHash, BarrierVersion: mainbarrier.BarrierVersionV2, MaxDependencyMs: x.dep}); e != nil {
			return e
		}
	}
	fmt.Println("built", len(cs), "frozen candidate label artifacts; final_holdout_accessed=false")
	return nil
}

func runPreflight(featureRoot, barrierRoot string, phase9A labels.Phase9AReport, barrier mainbarrier.ManifestV2, candidates []*c) error {
	if len(maintraining.ModelFeatureColumns) != 80 || len(phase9A.Frozen) != 5 {
		return fmt.Errorf("invalid feature registry or frozen candidates")
	}
	if _, err := os.Stat(featureRoot); err != nil {
		return err
	}
	if _, err := os.Stat(barrierRoot); err != nil {
		return err
	}
	if err := research.ValidateGrid(barrier, research.Grid()); err != nil {
		return err
	}
	for _, candidate := range candidates {
		if candidate.dep <= 0 {
			return fmt.Errorf("invalid candidate dependency %s", candidate.id)
		}
	}
	return nil
}
func lower(s string) string {
	if s == "LONG" {
		return "long"
	}
	return "short"
}
func months() []string {
	o := []string{}
	for y := 2024; y <= 2025; y++ {
		n := 12
		if y == 2025 {
			n = 6
		}
		for m := 1; m <= n; m++ {
			o = append(o, fmt.Sprintf("%04d-%02d", y, m))
		}
	}
	return o
}
func part(t int64) string {
	if t >= 1704067200000 && t < 1727740800000 {
		return "train"
	}
	if t < 1735689600000 {
		return "validation"
	}
	if t < 1751328000000 {
		return "test"
	}
	return ""
}
func end(p string) int64 {
	if p == "train" {
		return 1727740800000
	}
	if p == "validation" {
		return 1735689600000
	}
	return 1751328000000
}

func splitPartition(name string) (mainsplit.Partition, error) {
	switch name {
	case "train":
		return mainsplit.Train, nil
	case "validation":
		return mainsplit.Validation, nil
	case "test":
		return mainsplit.Test, nil
	default:
		return "", fmt.Errorf("unknown candidate-label partition %q", name)
	}
}
