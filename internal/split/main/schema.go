package mainsplit

import "time"

const SplitVersion = 1

type Partition string

const (
	Train        Partition = "TRAIN"
	Validation   Partition = "VALIDATION"
	Test         Partition = "TEST"
	FinalHoldout Partition = "FINAL_HOLDOUT"
	Outside      Partition = "OUTSIDE"
)

type ExclusionReason string

const (
	Included           ExclusionReason = "INCLUDED"
	PurgedLabelOverlap ExclusionReason = "PURGED_LABEL_OVERLAP"
	Embargoed          ExclusionReason = "EMBARGOED"
	OutsideRange       ExclusionReason = "OUTSIDE_RANGE"
)

type Range struct {
	StartMs int64 `json:"start_ms"`
	EndMs   int64 `json:"end_ms"`
}

type SplitDefinition struct {
	Version              int   `json:"split_version"`
	Train                Range `json:"train"`
	Validation           Range `json:"validation"`
	Test                 Range `json:"test"`
	FinalHoldout         Range `json:"final_holdout"`
	MaxLabelDependencyMs int64 `json:"max_label_dependency_ms"`
	EmbargoMs            int64 `json:"embargo_ms"`
}

type Assignment struct {
	Partition Partition
	Included  bool
	Reason    ExclusionReason
}

func utcMs(value string) int64 {
	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		panic(err)
	}
	return t.UnixMilli()
}

func DefaultV1(maxLabelDependencyMs, embargoMs int64) SplitDefinition {
	return SplitDefinition{
		Version:              SplitVersion,
		Train:                Range{utcMs("2024-01-01T00:00:00Z"), utcMs("2024-10-01T00:00:00Z")},
		Validation:           Range{utcMs("2024-10-01T00:00:00Z"), utcMs("2025-01-01T00:00:00Z")},
		Test:                 Range{utcMs("2025-01-01T00:00:00Z"), utcMs("2025-07-01T00:00:00Z")},
		FinalHoldout:         Range{utcMs("2025-07-01T00:00:00Z"), utcMs("2026-01-01T00:00:00Z")},
		MaxLabelDependencyMs: maxLabelDependencyMs,
		EmbargoMs:            embargoMs,
	}
}
