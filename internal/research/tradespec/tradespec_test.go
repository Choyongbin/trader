package tradespec

import (
	"math"
	"testing"

	mainbarrier "binance_trader/internal/barrier/main"
)

func TestFixedGridAndControl(t *testing.T) {
	g := Grid()
	if len(g) != 28 {
		t.Fatalf("grid count %d", len(g))
	}
	seen, control := map[string]bool{}, 0
	for _, s := range g {
		if seen[s.ID()] {
			t.Fatal("duplicate")
		}
		seen[s.ID()] = true
		if s.ID() == Control().ID() {
			control++
		}
	}
	if control != 1 {
		t.Fatalf("control count %d", control)
	}
}
func TestDependencyAndQuantile(t *testing.T) {
	m := mainbarrier.ManifestV2{EntryDelayMs: 0, MaxEntryWaitMs: 1000, MaxExitReferenceWaitMs: 1000}
	if Dependency(m, 900) != 902000 || Dependency(m, 14400) != 14402000 {
		t.Fatal("dependency")
	}
	x := []float64{0, 10, 20, 30}
	if math.Abs(quantile(x, .25)-7.5) > 1e-12 || quantile(x, .5) != 15 {
		t.Fatal("linear percentile")
	}
}
