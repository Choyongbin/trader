package tradespecmodeldata

import (
	"fmt"
	"math"
	"reflect"
	"strings"

	mainfeature "binance_trader/internal/feature/main"
	maintraining "binance_trader/internal/training/main"
)

const FeatureCount = 80

type FeatureRecord struct {
	DecisionTimestampMs int64
	Values              [FeatureCount]float64
}

type Sample struct {
	DecisionTimestampMs int64
	Features            [FeatureCount]float64
	BinaryTarget        bool
	NetReturnExFunding  float64
}

var featureFieldIndexes = buildFeatureFieldIndexes()

func FeatureRecordFromV1(row mainfeature.MainFeaturesV1) (FeatureRecord, error) {
	var record FeatureRecord
	record.DecisionTimestampMs = row.DecisionTimestampMs
	value := reflect.ValueOf(row)
	for output, index := range featureFieldIndexes {
		feature := value.Field(index).Float()
		if math.IsNaN(feature) || math.IsInf(feature, 0) {
			return FeatureRecord{}, fmt.Errorf("non-finite feature %s at %d", maintraining.ModelFeatureColumns[output], row.DecisionTimestampMs)
		}
		record.Values[output] = feature
	}
	return record, nil
}

func buildFeatureFieldIndexes() [FeatureCount]int {
	if err := ValidateModelFeatureColumns(maintraining.ModelFeatureColumns); err != nil {
		panic(err)
	}
	rowType := reflect.TypeOf(mainfeature.MainFeaturesV1{})
	byColumn := map[string]int{}
	for index := 0; index < rowType.NumField(); index++ {
		byColumn[strings.Split(rowType.Field(index).Tag.Get("parquet"), ",")[0]] = index
	}
	var indexes [FeatureCount]int
	for output, column := range maintraining.ModelFeatureColumns {
		index, exists := byColumn[column]
		if !exists || rowType.Field(index).Type.Kind() != reflect.Float64 {
			panic(fmt.Sprintf("invalid model feature column %q", column))
		}
		indexes[output] = index
	}
	return indexes
}

func ValidateModelFeatureColumns(columns []string) error {
	if len(columns) != FeatureCount {
		return fmt.Errorf("model feature count=%d want=%d", len(columns), FeatureCount)
	}
	seen := map[string]bool{}
	for _, column := range columns {
		if seen[column] {
			return fmt.Errorf("duplicate model feature column %q", column)
		}
		seen[column] = true
		lower := strings.ToLower(column)
		if column == "decision_timestamp_ms" || column == "reference_close" || strings.Contains(lower, "label_valid") || strings.Contains(lower, "profitable") || strings.Contains(lower, "net_return") || strings.Contains(lower, "trade_result") || strings.Contains(lower, "market_return") || strings.Contains(lower, "fee_cost_return") || strings.Contains(lower, "modeled_slippage") || strings.Contains(lower, "entry_") || strings.Contains(lower, "future_") {
			return fmt.Errorf("leakage model feature column %q", column)
		}
	}
	return nil
}
