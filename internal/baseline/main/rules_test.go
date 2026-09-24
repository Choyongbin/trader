package mainbaseline

import "testing"

func TestRuleDirectionsAndZero(t *testing.T) {
	for _, side := range []string{"LONG", "SHORT"} {
		m, r, f, err := RuleScores(side, .01, .2)
		if err != nil {
			t.Fatal(err)
		}
		if side == "LONG" && !(m > 0 && r < 0 && f > 0) {
			t.Fatal("long direction")
		}
		if side == "SHORT" && !(m < 0 && r > 0 && f < 0) {
			t.Fatal("short direction")
		}
		m, r, f, _ = RuleScores(side, 0, 0)
		if m > 0 || r > 0 || f > 0 {
			t.Fatal("zero score must predict negative")
		}
	}
}

func TestFeatureResolution(t *testing.T) {
	if _, err := FeatureIndex("ret_log_60s"); err != nil {
		t.Fatal(err)
	}
	if _, err := FeatureIndex("taker_imbalance_60s"); err != nil {
		t.Fatal(err)
	}
	if _, err := FeatureIndex("long_net_return_ex_funding"); err == nil {
		t.Fatal("future column resolved as model feature")
	}
}
