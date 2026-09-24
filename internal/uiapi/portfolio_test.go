package uiapi

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"testing/fstest"

	"binance_trader/internal/credentials"
)

type fixtureSpotAssets struct{}

func (fixtureSpotAssets) Assets(context.Context, TradingEnvironment) ([]SpotAssetBalance, error) {
	btc, eth, usdt := 100.0, 200.0, 300.0
	return []SpotAssetBalance{{"BTC", 1, 0, 1, &btc}, {"ETH", 2, 0, 2, &eth}, {"USDT", 300, 0, 300, &usdt}}, nil
}

func TestPortfolioSeparationAndUnknownSpot(t *testing.T) {
	provider := fakeProvider{credentials.CredentialStore{Testnet: credentials.EnvironmentCredentials{APIKey: "key", APISecret: "secret"}}}
	static := fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("console")}}
	backend := &fakeTestnetBackend{}
	s, err := NewServer(provider, static, Options{TestnetBackend: backend})
	if err != nil {
		t.Fatal(err)
	}
	get := func(path string) Portfolio {
		t.Helper()
		response := perform(s.Handler(), http.MethodGet, path, "", nil)
		if response.Code != http.StatusOK {
			t.Fatalf("status=%d", response.Code)
		}
		var value Portfolio
		if err := json.Unmarshal(response.Body.Bytes(), &value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	demo := get("/api/portfolio?environment=TESTNET")
	if demo.Futures.Status != "READY" || demo.Spot.Status != "UNAVAILABLE" || demo.PortfolioSummary.TotalUSDT != nil || len(demo.Futures.WalletAssets) != 1 {
		t.Fatalf("demo=%+v", demo)
	}
	mainnet := get("/api/portfolio?environment=MAINNET")
	if mainnet.Futures.Status != "NOT_CONNECTED" || len(mainnet.Futures.WalletAssets) != 0 || mainnet.Spot.Status != "NOT_CONNECTED" {
		t.Fatalf("mainnet=%+v", mainnet)
	}
	s.spotAssets = fixtureSpotAssets{}
	demo = get("/api/portfolio?environment=TESTNET")
	if demo.Spot.Status != "READY" || len(demo.Spot.Assets) != 3 || demo.PortfolioSummary.TotalUSDT == nil {
		t.Fatalf("fixture=%+v", demo)
	}
}
