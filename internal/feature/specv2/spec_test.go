package specv2

import (
	"testing"

	maintraining "binance_trader/internal/training/main"
)

func TestProposedValidAndCountConsistent(t *testing.T) {
	f := Proposed()
	if err := Validate(f, maintraining.ModelFeatureColumns); err != nil {
		t.Fatal(err)
	}
	if got := Count(f, Keep); got != 50 {
		t.Fatalf("KEEP count=%d want 50", got)
	}
	if got := len(maintraining.ModelFeatureColumns) + Count(f, Keep); got != 130 {
		t.Fatalf("expected total=%d want 130", got)
	}
}

func TestAmendmentOneDropsOnlyConstantFreshMasks(t *testing.T) {
	base, amended := Proposed(), Amended()
	if err := Validate(amended, maintraining.ModelFeatureColumns); err != nil {
		t.Fatal(err)
	}
	changed := map[string]bool{}
	for i := range base {
		if base[i].Decision != amended[i].Decision {
			changed[base[i].FeatureName] = true
		}
	}
	if len(changed) != 2 || !changed["metrics_fresh"] || !changed["kline_fresh"] {
		t.Fatalf("unexpected amendment changes: %v", changed)
	}
	if got := Count(amended, Keep); got != 48 {
		t.Fatalf("KEEP count=%d want 48", got)
	}
	if got := len(maintraining.ModelFeatureColumns) + Count(amended, Keep); got != 128 {
		t.Fatalf("expected total=%d want 128", got)
	}
}

func TestValidatorFailures(t *testing.T) {
	base := Proposed()
	tests := []struct {
		name   string
		mutate func([]Feature)
	}{
		{"duplicate", func(f []Feature) { f[1].FeatureName = f[0].FeatureName }},
		{"invalid lookback", func(f []Feature) { f[0].LookbackMs = -1 }},
		{"future window", func(f []Feature) { f[0].Window = "[t,t+5s)" }},
		{"unknown source", func(f []Feature) { f[0].Source = "unknown" }},
		{"missing policy", func(f []Feature) { f[0].MissingStaleBehavior = "" }},
		{"V1 duplicate", func(f []Feature) { f[0].FeatureName = maintraining.ModelFeatureColumns[0] }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := append([]Feature(nil), base...)
			tc.mutate(f)
			if err := Validate(f, maintraining.ModelFeatureColumns); err == nil {
				t.Fatal("expected failure")
			}
		})
	}
}
