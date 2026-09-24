package mainsplit

import (
	"errors"
	"fmt"
	"io"
	"reflect"

	maintraining "binance_trader/internal/training/main"
)

var errPartitionComplete = errors.New("partition complete")

type Side string

const (
	Long  Side = "LONG"
	Short Side = "SHORT"
)

type Sample struct {
	DecisionTimestampMs int64
	Features            []float64
	Target              bool
	NetReturnExFunding  float64
	GrossMarketReturn   float64
}

type Loader struct {
	Definition SplitDefinition
	Files      []string
	Warning    io.Writer
}

func (l Loader) LoadTraining(side Side, visit func(Sample) error) (int64, error) {
	return l.load(Train, side, visit)
}
func (l Loader) LoadValidation(side Side, visit func(Sample) error) (int64, error) {
	return l.load(Validation, side, visit)
}
func (l Loader) LoadTest(side Side, visit func(Sample) error) (int64, error) {
	return l.load(Test, side, visit)
}
func (l Loader) LoadFinalHoldout(side Side, visit func(Sample) error) (int64, error) {
	if l.Warning != nil {
		fmt.Fprintln(l.Warning, "WARNING: FINAL HOLDOUT ACCESSED; evaluation-only data must not influence model or strategy selection")
	}
	return l.load(FinalHoldout, side, visit)
}

// VisitDevelopmentPartition streams included rows for one development partition.
// It intentionally rejects FINAL_HOLDOUT and stops before opening later data.
func (l Loader) VisitDevelopmentPartition(partition Partition, visit func(maintraining.TrainingRowV1) error) (int64, error) {
	if partition != Train && partition != Validation && partition != Test {
		return 0, fmt.Errorf("development loader rejects partition %q", partition)
	}
	var included int64
	_, err := l.VisitCanonical(func(row maintraining.TrainingRowV1, assignment Assignment) error {
		_, wantedRange := l.Definition.rawPartition(rangeStart(l.Definition, partition))
		if row.DecisionTimestampMs >= wantedRange.EndMs {
			return errPartitionComplete
		}
		if assignment.Partition == partition && assignment.Included {
			included++
			return visit(row)
		}
		return nil
	})
	if errors.Is(err, errPartitionComplete) {
		err = nil
	}
	return included, err
}

func rangeStart(d SplitDefinition, partition Partition) int64 {
	switch partition {
	case Train:
		return d.Train.StartMs
	case Validation:
		return d.Validation.StartMs
	case Test:
		return d.Test.StartMs
	default:
		return d.FinalHoldout.StartMs
	}
}

func (l Loader) VisitCanonical(visit func(maintraining.TrainingRowV1, Assignment) error) (int64, error) {
	var total int64
	for _, path := range l.Files {
		_, err := maintraining.Read(path, func(row maintraining.TrainingRowV1) error {
			total++
			return visit(row, l.Definition.Classify(row.DecisionTimestampMs))
		})
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func (l Loader) load(partition Partition, side Side, visit func(Sample) error) (int64, error) {
	if partition == FinalHoldout {
		// Holdout access must go through LoadFinalHoldout so the warning cannot be bypassed.
	} else if partition != Train && partition != Validation && partition != Test {
		return 0, fmt.Errorf("unsupported ML partition %q", partition)
	}
	if side != Long && side != Short {
		return 0, fmt.Errorf("unsupported side %q", side)
	}
	var count int64
	_, err := l.VisitCanonical(func(row maintraining.TrainingRowV1, a Assignment) error {
		if !a.Included || a.Partition != partition {
			return nil
		}
		valid, target := row.LongLabelValid, row.LongNetProfitableExFunding
		netReturn, grossReturn := row.LongNetReturnExFunding, row.LongGrossMarketReturn
		if side == Short {
			valid, target = row.ShortLabelValid, row.ShortNetProfitableExFunding
			netReturn, grossReturn = row.ShortNetReturnExFunding, row.ShortGrossMarketReturn
		}
		if !valid {
			return nil
		}
		count++
		return visit(Sample{DecisionTimestampMs: row.DecisionTimestampMs, Features: ModelFeatures(row), Target: target, NetReturnExFunding: netReturn, GrossMarketReturn: grossReturn})
	})
	return count, err
}

func ModelFeatures(row maintraining.TrainingRowV1) []float64 {
	result := make([]float64, 0, len(maintraining.ModelFeatureColumns))
	return ModelFeaturesInto(result, row)
}

func ModelFeaturesInto(result []float64, row maintraining.TrainingRowV1) []float64 {
	result = result[:0]
	v := reflect.ValueOf(row.MainFeaturesV1)
	for i := 0; i < v.NumField(); i++ {
		field := v.Type().Field(i)
		name := field.Tag.Get("parquet")
		if name == "decision_timestamp_ms" || name == "reference_close" {
			continue
		}
		result = append(result, v.Field(i).Float())
	}
	return result
}
