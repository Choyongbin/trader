package uiapi

import "testing"

func TestLatencyRingBoundedAndUnavailable(t *testing.T) {
	var r latencyRing
	if r.summary() != "UNAVAILABLE" { t.Fatal("unobserved latency was fabricated") }
	for i:=0;i<1000;i++ { r.add(float64(i)) }
	s,ok:=r.summary().(map[string]any)
	if !ok || s["samples"]!=uint16(256) || s["p99"].(float64)<s["p50"].(float64) { t.Fatalf("invalid bounded latency summary: %v",s) }
}
