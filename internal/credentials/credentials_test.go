package credentials

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestParseAndPresence(t *testing.T) {
	store, err := Parse(strings.NewReader("# comment\nBINANCE_TESTNET_API_KEY=key\nBINANCE_TESTNET_API_SECRET=secret\nBINANCE_MAINNET_API_KEY=\nBINANCE_MAINNET_API_SECRET=\n"))
	if err != nil || !store.Testnet.Available() || store.Mainnet.Available() {
		t.Fatalf("store=%v err=%v", store, err)
	}
	b, err := json.Marshal(store)
	if err != nil || strings.Contains(string(b), "key") || strings.Contains(string(b), "secret") {
		t.Fatalf("secret JSON exposure: %s", b)
	}
}

func TestBOMAndEqualsInValueArePreserved(t *testing.T) {
	store, err := Parse(strings.NewReader("\uFEFFBINANCE_TESTNET_API_KEY=key\nBINANCE_TESTNET_API_SECRET=abc==\n"))
	if err != nil || store.Testnet.APISecret != "abc==" {
		t.Fatal("BOM or equals handling failed")
	}
}

func TestParseFailures(t *testing.T) {
	for _, text := range []string{
		"BINANCE_TESTNET_API_KEY=a\nBINANCE_TESTNET_API_KEY=b\n",
		"MALFORMED\n",
		"UNKNOWN=value\n",
		"BINANCE_TESTNET_API_KEY= value\n",
		"BINANCE_TESTNET_API_SECRET=\"secret\"\n",
	} {
		if _, err := Parse(strings.NewReader(text)); err == nil {
			t.Fatalf("accepted %q", text)
		}
	}
}

func TestEmptyAndRedaction(t *testing.T) {
	store, err := Parse(strings.NewReader("\n# empty\n"))
	if err != nil || store.Testnet.Available() || store.Mainnet.Available() {
		t.Fatalf("store=%v err=%v", store, err)
	}
	if got := Redact("key / secret", "key", "secret"); got != "REDACTED / REDACTED" {
		t.Fatal(got)
	}
}
