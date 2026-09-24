package mainbaseline

import (
	"fmt"

	maintraining "binance_trader/internal/training/main"
)

const (
	AlwaysNegative   = "always_negative"
	AlwaysPositive   = "always_positive"
	TrainPrevalence  = "train_prevalence_probability"
	Momentum60s      = "momentum_60s"
	MeanReversion60s = "mean_reversion_60s"
	TakerFlow60s     = "taker_flow_60s"
)

func FeatureIndex(name string) (int, error) {
	for i, candidate := range maintraining.ModelFeatureColumns {
		if candidate == name {
			return i, nil
		}
	}
	return 0, fmt.Errorf("model feature %q not registered", name)
}

func RuleScores(side string, ret60, taker60 float64) (momentum, meanReversion, taker float64, err error) {
	sign := 1.0
	if side == "SHORT" {
		sign = -1
	} else if side != "LONG" {
		return 0, 0, 0, fmt.Errorf("invalid side %q", side)
	}
	momentum = sign * ret60
	meanReversion = -momentum
	taker = sign * taker60
	return momentum, meanReversion, taker, nil
}
