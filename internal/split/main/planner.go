package mainsplit

import (
	"fmt"

	mainbarrier "binance_trader/internal/barrier/main"
	maintraining "binance_trader/internal/training/main"
)

func MaxLabelDependencyMs(spec maintraining.TradeSpecManifest, barrier mainbarrier.ManifestV2) (int64, error) {
	if spec.HorizonSeconds <= 0 || barrier.EntryDelayMs < 0 || barrier.MaxEntryWaitMs < 0 || barrier.MaxExitReferenceWaitMs < 0 {
		return 0, fmt.Errorf("invalid label dependency configuration")
	}
	found := false
	for _, horizon := range barrier.TimeoutHorizonsSeconds {
		if horizon == spec.HorizonSeconds {
			found = true
			break
		}
	}
	if !found {
		return 0, fmt.Errorf("training horizon %d is absent from barrier timeout horizons", spec.HorizonSeconds)
	}
	return barrier.EntryDelayMs + barrier.MaxEntryWaitMs + int64(spec.HorizonSeconds)*1000 + barrier.MaxExitReferenceWaitMs, nil
}

func (d SplitDefinition) Classify(timestampMs int64) Assignment {
	p, r := d.rawPartition(timestampMs)
	if p == Outside {
		return Assignment{Partition: Outside, Reason: OutsideRange}
	}
	if p != FinalHoldout && timestampMs+d.MaxLabelDependencyMs >= r.EndMs {
		return Assignment{Partition: p, Reason: PurgedLabelOverlap}
	}
	if p != Train && d.EmbargoMs > 0 && timestampMs < r.StartMs+d.EmbargoMs {
		return Assignment{Partition: p, Reason: Embargoed}
	}
	return Assignment{Partition: p, Included: true, Reason: Included}
}

func (d SplitDefinition) rawPartition(timestampMs int64) (Partition, Range) {
	for _, item := range []struct {
		partition Partition
		range_    Range
	}{{Train, d.Train}, {Validation, d.Validation}, {Test, d.Test}, {FinalHoldout, d.FinalHoldout}} {
		if timestampMs >= item.range_.StartMs && timestampMs < item.range_.EndMs {
			return item.partition, item.range_
		}
	}
	return Outside, Range{}
}
