package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"

	mainbarrier "binance_trader/internal/barrier/main"
	mainsplit "binance_trader/internal/split/main"
	maintraining "binance_trader/internal/training/main"
)

func main() {
	log.SetFlags(0)
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	trainingRoot := flag.String("training-root", ".\\data\\training\\main\\v1", "canonical training root")
	trainingManifests := flag.String("training-manifests", ".\\data\\manifests\\training\\main\\v1", "training manifest root")
	barrierManifests := flag.String("barrier-manifests", ".\\data\\manifests\\barriers\\main\\v2\\delay_0ms", "barrier manifest root")
	output := flag.String("output", ".\\data\\manifests\\splits\\main\\v1\\BTCUSDT-split-v1.json", "split manifest output")
	symbol := flag.String("symbol", "BTCUSDT", "symbol")
	embargoSeconds := flag.Int64("embargo-seconds", 0, "right-partition embargo seconds")
	flag.Parse()
	if *embargoSeconds < 0 {
		return fmt.Errorf("embargo-seconds must be non-negative")
	}

	specDir := "tp50_sl25_h3600"
	costDir := "cost_synthetic_validation_v1"
	var files []string
	var source maintraining.Manifest
	for year := 2024; year <= 2025; year++ {
		for month := 1; month <= 12; month++ {
			key := fmt.Sprintf("%04d-%02d", year, month)
			manifestPath := filepath.Join(*trainingManifests, specDir, costDir, *symbol, fmt.Sprintf("%s-training-v1-%s.json", *symbol, key))
			manifest, err := maintraining.ReadManifest(manifestPath)
			if err != nil {
				return fmt.Errorf("read training manifest %s: %w", key, err)
			}
			if err := validateSource(manifest, *symbol, key); err != nil {
				return err
			}
			if source.Symbol == "" {
				source = manifest
			} else if !sameSource(source, manifest) {
				return fmt.Errorf("training source configuration changes at %s", key)
			}
			path := filepath.Join(*trainingRoot, specDir, costDir, *symbol, key[:4], fmt.Sprintf("%s-training-v1-%s.parquet", *symbol, key))
			if info, err := os.Stat(path); err != nil || info.Size() == 0 {
				return fmt.Errorf("missing or empty training parquet %s", key)
			}
			files = append(files, path)
		}
	}
	sort.Strings(files)

	barrierPath := filepath.Join(*barrierManifests, *symbol, fmt.Sprintf("%s-main-barriers-v2-2024-01-delay_0ms.json", *symbol))
	barrier, err := readBarrierManifest(barrierPath)
	if err != nil {
		return err
	}
	dependency, err := mainsplit.MaxLabelDependencyMs(source.TradeSpec, barrier)
	if err != nil {
		return err
	}
	definition := mainsplit.DefaultV1(dependency, *embargoSeconds*1000)
	loader := mainsplit.Loader{Definition: definition, Files: files}
	stats, err := mainsplit.Audit(loader)
	if err != nil {
		return err
	}
	if stats.TotalRows != 12628799 || stats.OutsideRows != 0 {
		return fmt.Errorf("unexpected canonical coverage: rows=%d outside=%d", stats.TotalRows, stats.OutsideRows)
	}
	if err := auditBoundaries(definition, stats.Partitions); err != nil {
		return err
	}

	manifest := mainsplit.Manifest{
		SplitVersion:          mainsplit.SplitVersion,
		Symbol:                *symbol,
		SourceTrainingVersion: source.TrainingVersion,
		FeatureVersion:        source.FeatureVersion,
		OutcomeVersion:        source.OutcomeVersion,
		BarrierVersion:        source.BarrierVersion,
		TradeSpec:             source.TradeSpec,
		CostProfile:           source.CostProfile,
		Definition:            definition,
		PurgeRule:             "exclude left-partition row when decision_timestamp_ms + max_label_dependency_ms >= next_partition_start_ms",
		Partitions:            stats.Partitions,
		TotalCanonicalRows:    stats.TotalRows,
		OutsideRows:           stats.OutsideRows,
		ModelFeatureColumns:   append([]string(nil), maintraining.ModelFeatureColumns...),
		FinalHoldoutSealed:    true,
		FinalHoldoutPolicy:    "Final holdout must not be used for model, feature, threshold, cost, strategy, calibration, or hyperparameter selection.",
		CostProfileWarning:    "synthetic_validation_v1 is a pipeline validation profile, not verified Binance production cost.",
	}
	if err := mainsplit.WriteManifest(*output, manifest); err != nil {
		return err
	}
	printReport(definition, stats)
	fmt.Printf("Manifest: %s\n", *output)
	return nil
}

func validateSource(m maintraining.Manifest, symbol, month string) error {
	if m.Symbol != symbol || m.Month != month || m.TrainingVersion != 1 || m.FeatureVersion != 1 || m.OutcomeVersion != 2 || m.BarrierVersion != 2 || m.TradeSpec.TPBps != 50 || m.TradeSpec.SLBps != 25 || m.TradeSpec.HorizonSeconds != 3600 || m.CostProfile.Name != "synthetic_validation_v1" || m.CostProfile.FundingIncluded || len(m.ModelFeatureColumns) != 80 {
		return fmt.Errorf("training manifest source mismatch at %s", month)
	}
	for i, name := range maintraining.ModelFeatureColumns {
		if m.ModelFeatureColumns[i] != name {
			return fmt.Errorf("model feature registry mismatch at %s column %d", month, i)
		}
	}
	return nil
}

func sameSource(a, b maintraining.Manifest) bool {
	return a.TrainingVersion == b.TrainingVersion && a.FeatureVersion == b.FeatureVersion && a.OutcomeVersion == b.OutcomeVersion && a.BarrierVersion == b.BarrierVersion && a.TradeSpec == b.TradeSpec && a.CostProfile == b.CostProfile
}

func readBarrierManifest(path string) (mainbarrier.ManifestV2, error) {
	var manifest mainbarrier.ManifestV2
	b, err := os.ReadFile(path)
	if err != nil {
		return manifest, err
	}
	if err := json.Unmarshal(b, &manifest); err != nil {
		return manifest, err
	}
	if manifest.BarrierVersion != 2 || manifest.HorizonOrigin != "entry_reference_timestamp_ms" {
		return manifest, fmt.Errorf("unexpected barrier semantics")
	}
	return manifest, nil
}

func auditBoundaries(d mainsplit.SplitDefinition, stats map[mainsplit.Partition]*mainsplit.PartitionStats) error {
	for _, item := range []struct {
		left     mainsplit.Partition
		boundary int64
	}{{mainsplit.Train, d.Validation.StartMs}, {mainsplit.Validation, d.Test.StartMs}, {mainsplit.Test, d.FinalHoldout.StartMs}} {
		last := stats[item.left].LastIncluded
		if last+d.MaxLabelDependencyMs >= item.boundary {
			return fmt.Errorf("label overlap at %s boundary", item.left)
		}
	}
	return nil
}

func printReport(d mainsplit.SplitDefinition, s mainsplit.AuditStats) {
	fmt.Printf("Split V1 max_label_dependency_ms=%d embargo_ms=%d\n", d.MaxLabelDependencyMs, d.EmbargoMs)
	for _, p := range []mainsplit.Partition{mainsplit.Train, mainsplit.Validation, mainsplit.Test} {
		x := s.Partitions[p]
		fmt.Printf("%s raw=%d purged=%d embargoed=%d included=%d first=%s last=%s\n", p, x.RawRows, x.PurgedRows, x.EmbargoedRows, x.IncludedRows, stamp(x.FirstIncluded), stamp(x.LastIncluded))
		fmt.Printf("  LONG valid=%d positive=%d negative=%d rate=%.9f\n", x.Long.Valid, x.Long.Positive, x.Long.Negative, x.Long.Rate)
		fmt.Printf("  SHORT valid=%d positive=%d negative=%d rate=%.9f\n", x.Short.Valid, x.Short.Positive, x.Short.Negative, x.Short.Rate)
	}
	h := s.Partitions[mainsplit.FinalHoldout]
	fmt.Printf("FINAL_HOLDOUT raw=%d embargoed=%d included=%d first=%s last=%s structural_only=true\n", h.RawRows, h.EmbargoedRows, h.IncludedRows, stamp(h.FirstIncluded), stamp(h.LastIncluded))
	for _, item := range []struct {
		left     mainsplit.Partition
		boundary int64
	}{{mainsplit.Train, d.Validation.StartMs}, {mainsplit.Validation, d.Test.StartMs}, {mainsplit.Test, d.FinalHoldout.StartMs}} {
		x := s.Partitions[item.left]
		fmt.Printf("BOUNDARY %s boundary=%s dependency=%d last_retained=%s first_purged=%s no_overlap=%t purged=%d\n", item.left, stamp(item.boundary), d.MaxLabelDependencyMs, stamp(x.LastIncluded), stamp(x.FirstPurged), x.LastIncluded+d.MaxLabelDependencyMs < item.boundary, x.PurgedRows)
	}
	fmt.Printf("STRUCTURAL duplicates=%d ordering=%d timestamp_gaps=%d numeric_nan=%d numeric_inf=%d feature_count_mismatch=%d unexplained=%d outside=%d overlap=0 future_leakage_columns=%d\n", s.Duplicates, s.OrderViolations, s.TimestampGaps, s.NumericNaN, s.NumericInf, s.FeatureCountMismatches, s.UnexplainedRows, s.OutsideRows, s.RegistryLeakage)
	fmt.Println("WARNING: synthetic_validation_v1 is not verified Binance production cost")
}

func stamp(ms int64) string {
	if ms == 0 {
		return "none"
	}
	return time.UnixMilli(ms).UTC().Format(time.RFC3339Nano)
}
