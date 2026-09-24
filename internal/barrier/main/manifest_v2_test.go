package mainbarrier

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestManifestV2TriggerAndProxySemantics(t *testing.T) {
	p := filepath.Join(t.TempDir(), "m.json")
	m := ManifestV2{BarrierVersion: 2, TriggerPriceSource: TriggerPriceSourceContractPrice, EntryPriceSemantics: EntryPriceSemantics, HorizonOrigin: "entry_reference_timestamp_ms"}
	if e := WriteManifestV2(p, m); e != nil {
		t.Fatal(e)
	}
	b, e := os.ReadFile(p)
	if e != nil {
		t.Fatal(e)
	}
	var got ManifestV2
	if e = json.Unmarshal(b, &got); e != nil {
		t.Fatal(e)
	}
	if got.TriggerPriceSource != "CONTRACT_PRICE" || got.EntryPriceSemantics == "" || got.HorizonOrigin != "entry_reference_timestamp_ms" {
		t.Fatalf("%+v", got)
	}
}
