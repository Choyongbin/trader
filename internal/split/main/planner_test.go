package mainsplit

import "testing"

func syntheticDefinition(dependency, embargo int64) SplitDefinition {
	return SplitDefinition{Version: 1, Train: Range{0, 10000}, Validation: Range{10000, 20000}, Test: Range{20000, 30000}, FinalHoldout: Range{30000, 40000}, MaxLabelDependencyMs: dependency, EmbargoMs: embargo}
}

func TestChronologicalAssignmentAndBoundaryEquality(t *testing.T) {
	d := syntheticDefinition(0, 0)
	for ts, want := range map[int64]Partition{0: Train, 9999: Train, 10000: Validation, 20000: Test, 30000: FinalHoldout, 40000: Outside} {
		if got := d.Classify(ts).Partition; got != want {
			t.Fatalf("timestamp %d: got %s want %s", ts, got, want)
		}
	}
}

func TestPurgeExactBoundary(t *testing.T) {
	d := syntheticDefinition(1000, 0)
	if got := d.Classify(8999); !got.Included {
		t.Fatalf("8999 should be included: %+v", got)
	}
	if got := d.Classify(9000); got.Reason != PurgedLabelOverlap {
		t.Fatalf("9000 should be purged: %+v", got)
	}
}

func TestEmbargoAndPastFeatureLookback(t *testing.T) {
	d := syntheticDefinition(1000, 300)
	if got := d.Classify(10000); got.Reason != Embargoed {
		t.Fatalf("right boundary should be embargoed: %+v", got)
	}
	if got := d.Classify(10300); !got.Included {
		t.Fatalf("embargo end should be included: %+v", got)
	}
	d.EmbargoMs = 0
	if got := d.Classify(10000); !got.Included {
		t.Fatalf("past feature lookback must not exclude right-boundary row: %+v", got)
	}
}

func TestDeterministicClassification(t *testing.T) {
	d := syntheticDefinition(1000, 300)
	for ts := int64(-1); ts <= 40001; ts++ {
		if d.Classify(ts) != d.Classify(ts) {
			t.Fatalf("non-deterministic result at %d", ts)
		}
	}
}
