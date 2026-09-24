package maintraining

import (
	"strings"
	"testing"
)

func TestModelFeatureRegistryHasNoLeakage(t *testing.T) {
	if len(ModelFeatureColumns) != 80 {
		t.Fatalf("feature count=%d want 80", len(ModelFeatureColumns))
	}
	for _, c := range ModelFeatureColumns {
		if strings.HasPrefix(c, "future_") || strings.HasPrefix(c, "market_return_") || strings.Contains(c, "mfe_") || strings.Contains(c, "mae_") || strings.Contains(c, "barrier") || strings.Contains(c, "profitable") || strings.Contains(c, "trade_result") || strings.Contains(c, "net_return") {
			t.Fatalf("leaking model feature %q", c)
		}
	}
}
func TestInvalidRowsAreFilteredNotNegative(t *testing.T) {
	rows := []TrainingRowV1{{LongLabelValid: false, LongNetProfitableExFunding: false}, {LongLabelValid: true, LongNetProfitableExFunding: false}}
	got := ValidLongRows(rows)
	if len(got) != 1 || !got[0].LongLabelValid {
		t.Fatalf("%+v", got)
	}
}
