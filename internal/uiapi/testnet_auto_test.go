package uiapi

import (
	"testing"
	"time"

	"binance_trader/internal/live/autopipeline"
)

func TestValidAutoIntent(t *testing.T) {
	base := autopipeline.ExecutionIntent{
		Environment:         "TESTNET",
		Symbol:              "BTCUSDT",
		Side:                "LONG",
		CandidateID:         "fixture",
		QuantityBTC:         .001,
		NotionalUSDT:        100,
		RiskBudgetUSDT:      1,
		Leverage:            2,
		MarginUSDT:          50,
		EntryReferencePrice: 100000,
		TPPrice:             101000,
		SLPrice:             99000,
		HorizonSeconds:      900,
		ModelProfileID:      autopipeline.ProfileID,
		DecisionTimestampMs: time.Now().UnixMilli(),
	}
	if err := validAutoIntent(base); err != nil {
		t.Fatal(err)
	}
	bad := base
	bad.ModelProfileID = "other"
	if err := validAutoIntent(bad); err == nil {
		t.Fatal("wrong model profile accepted")
	}
	bad = base
	bad.TPPrice = 99000
	if err := validAutoIntent(bad); err == nil {
		t.Fatal("invalid LONG protective prices accepted")
	}
	bad = base
	bad.Side = "SHORT"
	bad.TPPrice = 99000
	bad.SLPrice = 101000
	if err := validAutoIntent(bad); err != nil {
		t.Fatal(err)
	}
}
