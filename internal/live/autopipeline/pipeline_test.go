package autopipeline

import (
	"path/filepath"
	"testing"

	featurev2 "binance_trader/internal/feature/main/v2"
)

func TestFrozenModelBindingAndDryRunRiskIntent(t *testing.T) {
	p, err := LoadFrozen(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	if len(p.models) != 5 {
		t.Fatal("candidate binding incomplete")
	}
	var features featurev2.Snapshot
	features.DecisionTimestampMs = 1_800_000_000_000
	copy(features.Values[:], p.models[0].Scaler.Mean)
	if _, err = p.Evaluate(features, 10000, 100000); err != nil {
		t.Fatal(err)
	}
	// Force only the first candidate through the fixture thresholds. Frozen
	// production artifacts remain untouched; this exercises risk and intent.
	for i := range p.models {
		p.models[i].ClassIntercept = -1000
		p.models[i].RegressionIntercept = -1000
	}
	p.models[0].ClassIntercept = 1000
	p.models[0].RegressionIntercept = p.models[0].ReturnScale
	sink := &RecordingBroker{}
	result, err := p.EvaluateAndRecord(features, 10000, 100000, sink)
	if err != nil {
		t.Fatal(err)
	}
	if result.FinalSignal != "LONG" || result.Intent == nil || len(sink.Intents) != 1 || sink.Intents[0].NotionalUSDT <= 0 || sink.Intents[0].RiskBudgetUSDT != 25 || sink.Intents[0].ModelProfileID != ProfileID {
		t.Fatalf("bad dry-run result: %+v", result)
	}
	if result.Intent.TPPrice <= result.Intent.EntryReferencePrice || result.Intent.SLPrice >= result.Intent.EntryReferencePrice {
		t.Fatal("wrong TP/SL side")
	}
	if result.Intent.HorizonSeconds != p.entry.Candidates[0].HorizonSeconds || result.Intent.HorizonSeconds <= 0 {
		t.Fatalf("wrong holding horizon: %d", result.Intent.HorizonSeconds)
	}
}
