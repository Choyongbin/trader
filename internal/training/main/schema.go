package maintraining

import (
	"reflect"
	"strings"

	mainfeature "binance_trader/internal/feature/main"
	"binance_trader/internal/tradelabel"
)

const TrainingVersion = 1

type TrainingRowV1 struct {
	mainfeature.MainFeaturesV1
	EntryReferenceAvailable bool  `parquet:"entry_reference_available"`
	EntryWaitMs             int64 `parquet:"entry_wait_ms"`

	LongLabelValid                bool              `parquet:"long_label_valid"`
	LongTradeResult               tradelabel.Status `parquet:"long_trade_result"`
	LongGrossMarketReturn         float64           `parquet:"long_gross_market_return"`
	LongFeeCostReturn             float64           `parquet:"long_fee_cost_return"`
	LongModeledSlippageCostReturn float64           `parquet:"long_modeled_slippage_cost_return"`
	LongNetReturnExFunding        float64           `parquet:"long_net_return_ex_funding"`
	LongGrossProfitable           bool              `parquet:"long_gross_profitable"`
	LongNetProfitableExFunding    bool              `parquet:"long_net_profitable_ex_funding"`

	ShortLabelValid                bool              `parquet:"short_label_valid"`
	ShortTradeResult               tradelabel.Status `parquet:"short_trade_result"`
	ShortGrossMarketReturn         float64           `parquet:"short_gross_market_return"`
	ShortFeeCostReturn             float64           `parquet:"short_fee_cost_return"`
	ShortModeledSlippageCostReturn float64           `parquet:"short_modeled_slippage_cost_return"`
	ShortNetReturnExFunding        float64           `parquet:"short_net_return_ex_funding"`
	ShortGrossProfitable           bool              `parquet:"short_gross_profitable"`
	ShortNetProfitableExFunding    bool              `parquet:"short_net_profitable_ex_funding"`
}

var MetadataColumns = []string{"decision_timestamp_ms", "reference_close", "entry_reference_available", "entry_wait_ms"}
var ModelFeatureColumns = featureModelColumns()
var TargetColumns = []string{"long_label_valid", "long_net_profitable_ex_funding", "short_label_valid", "short_net_profitable_ex_funding"}
var AnalysisColumns = []string{
	"long_trade_result", "long_gross_market_return", "long_fee_cost_return", "long_modeled_slippage_cost_return", "long_net_return_ex_funding", "long_gross_profitable",
	"short_trade_result", "short_gross_market_return", "short_fee_cost_return", "short_modeled_slippage_cost_return", "short_net_return_ex_funding", "short_gross_profitable",
}

func featureModelColumns() []string {
	t := reflect.TypeOf(mainfeature.MainFeaturesV1{})
	result := make([]string, 0, t.NumField()-2)
	for i := 0; i < t.NumField(); i++ {
		name := strings.Split(t.Field(i).Tag.Get("parquet"), ",")[0]
		if name != "decision_timestamp_ms" && name != "reference_close" {
			result = append(result, name)
		}
	}
	return result
}

func ValidLongRows(rows []TrainingRowV1) []TrainingRowV1 {
	result := make([]TrainingRowV1, 0, len(rows))
	for _, r := range rows {
		if r.LongLabelValid {
			result = append(result, r)
		}
	}
	return result
}
