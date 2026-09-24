package mainoutcome

const OutcomeVersion = 2

var Horizons = []int{60, 180, 300, 900, 1800, 3600, 14400}

type MainOutcomeV2 struct {
	DecisionTimestampMs int64   `parquet:"decision_timestamp_ms"`
	ReferenceEntryPrice float64 `parquet:"reference_entry_price"`
	MarketReturn60s     float64 `parquet:"market_return_60s"`
	LongReturn60s       float64 `parquet:"long_return_60s"`
	ShortReturn60s      float64 `parquet:"short_return_60s"`
	FutureHigh60s       float64 `parquet:"future_high_60s"`
	FutureLow60s        float64 `parquet:"future_low_60s"`
	LongMFE60s          float64 `parquet:"long_mfe_60s"`
	LongMAE60s          float64 `parquet:"long_mae_60s"`
	ShortMFE60s         float64 `parquet:"short_mfe_60s"`
	ShortMAE60s         float64 `parquet:"short_mae_60s"`
	MarketReturn180s    float64 `parquet:"market_return_180s"`
	LongReturn180s      float64 `parquet:"long_return_180s"`
	ShortReturn180s     float64 `parquet:"short_return_180s"`
	FutureHigh180s      float64 `parquet:"future_high_180s"`
	FutureLow180s       float64 `parquet:"future_low_180s"`
	LongMFE180s         float64 `parquet:"long_mfe_180s"`
	LongMAE180s         float64 `parquet:"long_mae_180s"`
	ShortMFE180s        float64 `parquet:"short_mfe_180s"`
	ShortMAE180s        float64 `parquet:"short_mae_180s"`
	MarketReturn300s    float64 `parquet:"market_return_300s"`
	LongReturn300s      float64 `parquet:"long_return_300s"`
	ShortReturn300s     float64 `parquet:"short_return_300s"`
	FutureHigh300s      float64 `parquet:"future_high_300s"`
	FutureLow300s       float64 `parquet:"future_low_300s"`
	LongMFE300s         float64 `parquet:"long_mfe_300s"`
	LongMAE300s         float64 `parquet:"long_mae_300s"`
	ShortMFE300s        float64 `parquet:"short_mfe_300s"`
	ShortMAE300s        float64 `parquet:"short_mae_300s"`
	MarketReturn900s    float64 `parquet:"market_return_900s"`
	LongReturn900s      float64 `parquet:"long_return_900s"`
	ShortReturn900s     float64 `parquet:"short_return_900s"`
	FutureHigh900s      float64 `parquet:"future_high_900s"`
	FutureLow900s       float64 `parquet:"future_low_900s"`
	LongMFE900s         float64 `parquet:"long_mfe_900s"`
	LongMAE900s         float64 `parquet:"long_mae_900s"`
	ShortMFE900s        float64 `parquet:"short_mfe_900s"`
	ShortMAE900s        float64 `parquet:"short_mae_900s"`
	MarketReturn1800s   float64 `parquet:"market_return_1800s"`
	LongReturn1800s     float64 `parquet:"long_return_1800s"`
	ShortReturn1800s    float64 `parquet:"short_return_1800s"`
	FutureHigh1800s     float64 `parquet:"future_high_1800s"`
	FutureLow1800s      float64 `parquet:"future_low_1800s"`
	LongMFE1800s        float64 `parquet:"long_mfe_1800s"`
	LongMAE1800s        float64 `parquet:"long_mae_1800s"`
	ShortMFE1800s       float64 `parquet:"short_mfe_1800s"`
	ShortMAE1800s       float64 `parquet:"short_mae_1800s"`
	MarketReturn3600s   float64 `parquet:"market_return_3600s"`
	LongReturn3600s     float64 `parquet:"long_return_3600s"`
	ShortReturn3600s    float64 `parquet:"short_return_3600s"`
	FutureHigh3600s     float64 `parquet:"future_high_3600s"`
	FutureLow3600s      float64 `parquet:"future_low_3600s"`
	LongMFE3600s        float64 `parquet:"long_mfe_3600s"`
	LongMAE3600s        float64 `parquet:"long_mae_3600s"`
	ShortMFE3600s       float64 `parquet:"short_mfe_3600s"`
	ShortMAE3600s       float64 `parquet:"short_mae_3600s"`
	MarketReturn14400s  float64 `parquet:"market_return_14400s"`
	LongReturn14400s    float64 `parquet:"long_return_14400s"`
	ShortReturn14400s   float64 `parquet:"short_return_14400s"`
	FutureHigh14400s    float64 `parquet:"future_high_14400s"`
	FutureLow14400s     float64 `parquet:"future_low_14400s"`
	LongMFE14400s       float64 `parquet:"long_mfe_14400s"`
	LongMAE14400s       float64 `parquet:"long_mae_14400s"`
	ShortMFE14400s      float64 `parquet:"short_mfe_14400s"`
	ShortMAE14400s      float64 `parquet:"short_mae_14400s"`
}

// MainOutcomeV1 is retained as a source-compatibility alias. Artifacts created
// with the old inverse SHORT semantics remain under data/outcomes/main/v1 and
// must not be used for new training.
type MainOutcomeV1 = MainOutcomeV2
