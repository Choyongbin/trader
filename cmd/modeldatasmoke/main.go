package main

import (
	tradespec "binance_trader/internal/label/tradespec"
	modeldata "binance_trader/internal/modeldata/tradespec"
	mainsplit "binance_trader/internal/split/main"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

func main() {
	root := `./data/labels/tradespec/v1/cost_synthetic_validation_v1/BTCUSDT`
	features := make([]string, 0, 18)
	for year := 2024; year <= 2025; year++ {
		limit := 12
		if year == 2025 {
			limit = 6
		}
		for month := 1; month <= limit; month++ {
			features = append(features, filepath.Join(`./data/features/main/v1/BTCUSDT`, fmt.Sprintf("%04d", year), fmt.Sprintf("BTCUSDT-main-features-v1-%04d-%02d.parquet", year, month)))
		}
	}
	for _, side := range []string{"long", "short"} {
		entries, err := os.ReadDir(filepath.Join(root, side))
		if err != nil {
			log.Fatal(err)
		}
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			candidateRoot := filepath.Join(root, side, entry.Name())
			manifest, err := tradespec.ReadManifest(filepath.Join(candidateRoot, "manifest.json"))
			if err != nil {
				log.Fatal(err)
			}
			loader := modeldata.NewParquetLoader(manifest, mainsplit.DefaultV1(manifest.MaxLabelDependencyMs, 0), features, map[mainsplit.Partition][]string{mainsplit.Train: {filepath.Join(candidateRoot, "train.parquet")}, mainsplit.Validation: {filepath.Join(candidateRoot, "validation.parquet")}, mainsplit.Test: {filepath.Join(candidateRoot, "test.parquet")}})
			for _, stream := range []struct {
				name string
				run  func(func(modeldata.Sample) error) (int64, error)
			}{{"TRAIN", loader.StreamTrain}, {"VALIDATION", loader.StreamValidation}, {"TEST", loader.StreamTest}} {
				count, err := stream.run(func(sample modeldata.Sample) error {
					if len(sample.Features) != 80 {
						return fmt.Errorf("feature count %d", len(sample.Features))
					}
					return nil
				})
				if err != nil || count == 0 {
					log.Fatalf("%s %s rows=%d err=%v", entry.Name(), stream.name, count, err)
				}
			}
			fmt.Printf("SMOKE PASS %s/%s\n", side, entry.Name())
		}
	}
	fmt.Println("MODEL DATA LOADER REAL SMOKE PASS; final_holdout_accessed=false")
}
