package binance

import "testing"

func TestPublicObservationEndpointGroups(t *testing.T) {
	fastWant := map[string]bool{"mark": true, "index": true, "premium": true}
	slowWant := map[string]bool{
		"funding":              true,
		"metrics_oi":           true,
		"metrics_global":       true,
		"metrics_top_account":  true,
		"metrics_top_position": true,
		"metrics_taker":        true,
	}
	seen := map[string]bool{}
	check := func(group string, endpoints []publicObservationEndpoint, want map[string]bool) {
		t.Helper()
		if len(endpoints) != len(want) {
			t.Fatalf("%s endpoints=%d want=%d", group, len(endpoints), len(want))
		}
		for _, endpoint := range endpoints {
			if !want[endpoint.name] {
				t.Fatalf("%s unexpected endpoint %q", group, endpoint.name)
			}
			if endpoint.url == "" {
				t.Fatalf("%s endpoint %q has empty URL", group, endpoint.name)
			}
			if seen[endpoint.name] {
				t.Fatalf("duplicate endpoint %q", endpoint.name)
			}
			seen[endpoint.name] = true
		}
	}
	check("kline", publicKlineObservationEndpoints, fastWant)
	check("slow", publicSlowObservationEndpoints, slowWant)
	if len(seen) != 9 {
		t.Fatalf("combined endpoint count=%d want=9", len(seen))
	}
}
