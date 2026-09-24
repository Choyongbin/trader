package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"time"

	mainbaseline "binance_trader/internal/baseline/main"
	mainsplit "binance_trader/internal/split/main"
	maintraining "binance_trader/internal/training/main"
)

const baselineVersion = 1

type report struct {
	BaselineVersion      int                              `json:"baseline_version"`
	SplitVersion         int                              `json:"split_version"`
	TrainingVersion      int                              `json:"training_version"`
	FeatureVersion       int                              `json:"feature_version"`
	OutcomeVersion       int                              `json:"outcome_version"`
	BarrierVersion       int                              `json:"barrier_version"`
	TradeSpec            maintraining.TradeSpecManifest   `json:"trade_spec"`
	CostProfile          maintraining.CostProfileManifest `json:"cost_profile"`
	FinalHoldoutAccessed bool                             `json:"final_holdout_accessed"`
	Interpretation       string                           `json:"interpretation"`
	TrainPrevalence      map[string]float64               `json:"train_prevalence_probability"`
	ElapsedMs            map[string]int64                 `json:"elapsed_ms"`
	TotalElapsedMs       int64                            `json:"total_elapsed_ms"`
	PeakHeapAllocBytes   uint64                           `json:"peak_heap_alloc_bytes"`
	Results              []mainbaseline.Result            `json:"results"`
}

func main() {
	log.SetFlags(0)
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	trainingRoot := flag.String("training-root", ".\\data\\training\\main\\v1\\tp50_sl25_h3600\\cost_synthetic_validation_v1\\BTCUSDT", "canonical training root")
	splitPath := flag.String("split-manifest", ".\\data\\manifests\\splits\\main\\v1\\BTCUSDT-split-v1.json", "sealed split manifest")
	output := flag.String("output", ".\\data\\reports\\baselines\\main\\v1\\BTCUSDT-baselines-v1.json", "JSON report")
	flag.Parse()
	splitManifest, err := mainsplit.ReadManifest(*splitPath)
	if err != nil {
		return err
	}
	if splitManifest.SplitVersion != 1 || !splitManifest.FinalHoldoutSealed || splitManifest.SourceTrainingVersion != 1 || splitManifest.FeatureVersion != 1 || splitManifest.OutcomeVersion != 2 || splitManifest.BarrierVersion != 2 || splitManifest.TradeSpec.TPBps != 50 || splitManifest.TradeSpec.SLBps != 25 || splitManifest.TradeSpec.HorizonSeconds != 3600 || splitManifest.CostProfile.Name != "synthetic_validation_v1" || splitManifest.CostProfile.FundingIncluded || len(splitManifest.ModelFeatureColumns) != 80 {
		return fmt.Errorf("split/source manifest mismatch")
	}
	trainStats := splitManifest.Partitions[mainsplit.Train]
	if trainStats == nil || trainStats.Long == nil || trainStats.Short == nil {
		return fmt.Errorf("train prevalence unavailable")
	}
	prevalence := map[string]float64{"LONG": trainStats.Long.Rate, "SHORT": trainStats.Short.Rate}
	files := monthlyFiles(*trainingRoot)
	partitionFiles := map[mainsplit.Partition][]string{
		mainsplit.Train: files[:9], mainsplit.Validation: files[9:12], mainsplit.Test: files[12:18],
	}
	var peak atomic.Uint64
	done := make(chan struct{})
	go sampleMemory(&peak, done)
	started := time.Now()
	r := report{BaselineVersion: baselineVersion, SplitVersion: 1, TrainingVersion: 1, FeatureVersion: 1, OutcomeVersion: 2, BarrierVersion: 2, TradeSpec: splitManifest.TradeSpec, CostProfile: splitManifest.CostProfile, FinalHoldoutAccessed: false, Interpretation: "per-decision overlapping-horizon label baselines; not portfolio backtest or cumulative PnL", TrainPrevalence: prevalence, ElapsedMs: map[string]int64{}}
	for _, partition := range []mainsplit.Partition{mainsplit.Train, mainsplit.Validation, mainsplit.Test} {
		partitionStarted := time.Now()
		longEval, err := mainbaseline.NewEvaluator("LONG", string(partition), prevalence["LONG"])
		if err != nil {
			return err
		}
		shortEval, err := mainbaseline.NewEvaluator("SHORT", string(partition), prevalence["SHORT"])
		if err != nil {
			return err
		}
		loader := mainsplit.Loader{Definition: splitManifest.Definition, Files: partitionFiles[partition]}
		rows, err := loader.VisitDevelopmentPartition(partition, func(row maintraining.TrainingRowV1) error {
			features := mainsplit.ModelFeatures(row)
			if row.LongLabelValid {
				longEval.Observe(mainsplit.Sample{DecisionTimestampMs: row.DecisionTimestampMs, Features: features, Target: row.LongNetProfitableExFunding, NetReturnExFunding: row.LongNetReturnExFunding, GrossMarketReturn: row.LongGrossMarketReturn})
			}
			if row.ShortLabelValid {
				shortEval.Observe(mainsplit.Sample{DecisionTimestampMs: row.DecisionTimestampMs, Features: features, Target: row.ShortNetProfitableExFunding, NetReturnExFunding: row.ShortNetReturnExFunding, GrossMarketReturn: row.ShortGrossMarketReturn})
			}
			return nil
		})
		if err != nil {
			return err
		}
		if rows != trainStatsFor(splitManifest, partition).IncludedRows {
			return fmt.Errorf("%s included row mismatch", partition)
		}
		r.Results = append(r.Results, longEval.Finish()...)
		r.Results = append(r.Results, shortEval.Finish()...)
		r.ElapsedMs[string(partition)] = time.Since(partitionStarted).Milliseconds()
		fmt.Printf("%s rows=%d elapsed=%s\n", partition, rows, time.Since(partitionStarted).Round(time.Millisecond))
	}
	close(done)
	r.TotalElapsedMs = time.Since(started).Milliseconds()
	r.PeakHeapAllocBytes = peak.Load()
	if err := writeJSON(*output, r); err != nil {
		return err
	}
	printResults(r)
	fmt.Printf("final_holdout_accessed=false total_elapsed=%s peak_heap_alloc_bytes=%d\nReport: %s\n", time.Since(started).Round(time.Millisecond), r.PeakHeapAllocBytes, *output)
	return nil
}

func monthlyFiles(root string) []string {
	files := make([]string, 0, 24)
	for year := 2024; year <= 2025; year++ {
		for month := 1; month <= 12; month++ {
			key := fmt.Sprintf("%04d-%02d", year, month)
			files = append(files, filepath.Join(root, key[:4], fmt.Sprintf("BTCUSDT-training-v1-%s.parquet", key)))
		}
	}
	return files
}

func trainStatsFor(m mainsplit.Manifest, p mainsplit.Partition) *mainsplit.PartitionStats {
	return m.Partitions[p]
}

func sampleMemory(peak *atomic.Uint64, done <-chan struct{}) {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			for old := peak.Load(); m.HeapAlloc > old && !peak.CompareAndSwap(old, m.HeapAlloc); old = peak.Load() {
			}
		}
	}
}

func writeJSON(path string, value any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	tmp := path + ".tmp"
	if err = os.WriteFile(tmp, b, 0644); err != nil {
		return err
	}
	_ = os.Remove(path)
	return os.Rename(tmp, path)
}

func printResults(r report) {
	for _, x := range r.Results {
		fmt.Printf("%s %-5s %-28s acc=%.6f bal_acc=%.6f precision=%.6f recall=%.6f f1=%.6f mcc=%.6f coverage=%.6f", x.Partition, x.Side, x.BaselineName, x.Classification.Accuracy, x.Classification.BalancedAccuracy, x.Classification.Precision, x.Classification.Recall, x.Classification.F1, x.Classification.MCC, x.Trading.SignalCoverage)
		if x.Trading.SignalWinRate.Valid {
			fmt.Printf(" signal_win=%.6f mean_net=%.9f", x.Trading.SignalWinRate.Value, x.Trading.MeanNetReturnExFunding.Value)
		} else {
			fmt.Print(" signal_win=N/A mean_net=N/A")
		}
		if x.ROCAUC.Valid {
			fmt.Printf(" roc_auc=%.6f ap=%.6f", x.ROCAUC.Value, x.AveragePrecision.Value)
		}
		if x.BrierScore.Valid {
			fmt.Printf(" brier=%.6f logloss=%.6f", x.BrierScore.Value, x.LogLoss.Value)
		}
		fmt.Println()
	}
}
