package binance

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func fixtureSymbol() Symbol {
	return Symbol{Symbol: "BTCUSDT", Status: "TRADING", ContractType: "PERPETUAL", Filters: []Filter{
		{FilterType: "PRICE_FILTER", MinPrice: "0.10", MaxPrice: "1000000", TickSize: "0.10"},
		{FilterType: "LOT_SIZE", MinQty: "0.001", MaxQty: "1000", StepSize: "0.001"},
		{FilterType: "MARKET_LOT_SIZE", MinQty: "0.001", MaxQty: "100", StepSize: "0.001"},
		{FilterType: "MIN_NOTIONAL", Notional: "5"},
	}}
}

func TestNormalization(t *testing.T) {
	s := fixtureSymbol()
	p, err := NormalizePrice(s, 101.29)
	if err != nil || p != 101.2 {
		t.Fatalf("price=%g err=%v", p, err)
	}
	q, err := NormalizeQuantity(s, .0129, true)
	if err != nil || q != .012 {
		t.Fatalf("quantity=%g err=%v", q, err)
	}
	if ValidateMinNotional(s, 100, .1) != nil {
		t.Fatal("valid notional rejected")
	}
	if ValidateMinNotional(s, 100, .001) == nil {
		t.Fatal("small notional accepted")
	}
	up, err := NormalizeQuantityUp(s, .0121, true)
	if err != nil || math.Abs(up-.013) > 1e-12 {
		t.Fatalf("ceil quantity=%g err=%v", up, err)
	}
	formatted, err := FormatQuantity(s, up, true)
	if err != nil || formatted != "0.013" {
		t.Fatalf("formatted quantity=%q err=%v", formatted, err)
	}
	formattedPrice, err := FormatPrice(s, 101.2)
	if err != nil || formattedPrice != "101.2" {
		t.Fatalf("formatted price=%q err=%v", formattedPrice, err)
	}
}

func TestEndpointAndOrderGuards(t *testing.T) {
	if ValidateTestnetBaseURL("https://fapi.binance.com") == nil {
		t.Fatal("Mainnet accepted")
	}
	if ValidateTestnetBaseURL(TestnetRESTBaseURL) != nil {
		t.Fatal("Testnet rejected")
	}
	oldEnv, oldEnabled := os.Getenv("BINANCE_ENV"), os.Getenv("BINANCE_TESTNET_ENABLE_ORDERS")
	t.Cleanup(func() {
		_ = os.Setenv("BINANCE_ENV", oldEnv)
		_ = os.Setenv("BINANCE_TESTNET_ENABLE_ORDERS", oldEnabled)
	})
	_ = os.Setenv("BINANCE_ENV", "PUBLIC_ONLY")
	_ = os.Setenv("BINANCE_TESTNET_ENABLE_ORDERS", "true")
	if OrdersEnabled(PublicOnly) {
		t.Fatal("PUBLIC_ONLY enabled orders")
	}
	_ = os.Setenv("BINANCE_ENV", "TESTNET")
	if !OrdersEnabled(Testnet) {
		t.Fatal("explicit Testnet guard rejected")
	}
}

func TestClientOrderID(t *testing.T) {
	a := DeterministicClientOrderID("btc-strategy", 123456789, "tp100_sl100_h14400", "ENTRY", 1)
	b := DeterministicClientOrderID("btc-strategy", 123456789, "tp100_sl100_h14400", "ENTRY", 1)
	if a != b || len(a) > 36 {
		t.Fatalf("invalid id %q", a)
	}
}

func TestUserDataAllowedWhileOrdersRemainDisabled(t *testing.T) {
	oldEnv, oldEnabled := os.Getenv("BINANCE_ENV"), os.Getenv("BINANCE_TESTNET_ENABLE_ORDERS")
	t.Cleanup(func() {
		_ = os.Setenv("BINANCE_ENV", oldEnv)
		_ = os.Setenv("BINANCE_TESTNET_ENABLE_ORDERS", oldEnabled)
	})
	_ = os.Setenv("BINANCE_ENV", "TESTNET")
	_ = os.Setenv("BINANCE_TESTNET_ENABLE_ORDERS", "false")
	c, err := NewTestnetClient("key", "secret")
	if err != nil {
		t.Fatal(err)
	}
	c.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Host != "demo-fapi.binance.com" || r.Method != http.MethodGet || r.Header.Get("X-MBX-APIKEY") != "key" {
			t.Fatalf("unsafe request %s %s", r.Method, r.URL)
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"totalWalletBalance":"1"}`)), Header: make(http.Header)}, nil
	})}
	if _, err = c.Account(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = c.Signed(context.Background(), http.MethodPost, "/fapi/v1/order", nil, nil); err == nil {
		t.Fatal("trade request passed disabled order gate")
	}
}

func TestPositionsUsesLeverageBearingV2Endpoint(t *testing.T) {
	c, err := NewTestnetClient("key", "secret")
	if err != nil {
		t.Fatal(err)
	}
	c.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/fapi/v2/positionRisk" {
			t.Fatalf("position endpoint=%s", r.URL.Path)
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`[{"symbol":"BTCUSDT","positionAmt":"0.001","leverage":"2"}]`)), Header: make(http.Header)}, nil
	})}
	positions, err := c.Positions(context.Background(), "BTCUSDT")
	if err != nil {
		t.Fatal(err)
	}
	if len(positions) != 1 || positions[0].Leverage != "2" {
		t.Fatalf("positions=%+v", positions)
	}
}

func TestOfficialHMACVector(t *testing.T) {
	payload := "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559"
	secret := "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j"
	want := "c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71"
	if signHMAC(payload, secret) != want {
		t.Fatal("official vector mismatch")
	}
}

func TestSignedWireUsesExactPayloadAndSignatureLast(t *testing.T) {
	c, _ := NewTestnetClient("synthetic-key", "synthetic-secret")
	c.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		parts := strings.Split(r.URL.RawQuery, "&signature=")
		if len(parts) != 2 || strings.Contains(parts[0], "signature=") {
			t.Fatal("signature was not appended last")
		}
		if parts[1] != signHMAC(parts[0], "synthetic-secret") {
			t.Fatal("signed payload differs from wire payload")
		}
		if !strings.Contains(parts[0], "note=a%2Bb+%26+c") || !strings.Contains(parts[0], "recvWindow=5000") {
			t.Fatal("canonical encoding missing")
		}
		if strings.Contains(parts[0], "optional=") {
			t.Fatal("empty optional value was not excluded")
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{}`)), Header: make(http.Header)}, nil
	})}
	values := url.Values{"note": {"a+b & c"}, "timestamp": {"1499827319559"}, "optional": {""}}
	if err := c.UserData(context.Background(), "/fapi/v3/account", values, &struct{}{}); err != nil {
		t.Fatal(err)
	}
}

func TestCanonicalParameterOrderIsDeterministic(t *testing.T) {
	a := url.Values{}
	a.Set("z", "last")
	a.Set("a", "first")
	b := url.Values{}
	b.Set("a", "first")
	b.Set("z", "last")
	if a.Encode() != b.Encode() || a.Encode() != "a=first&z=last" {
		t.Fatal("parameter order is not deterministic")
	}
}

func TestSignedTransportErrorNeverLeaksQuery(t *testing.T) {
	c, _ := NewTestnetClient("synthetic-key", "synthetic-secret")
	c.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("wire error for %s", r.URL.String())
	})}
	err := c.UserData(context.Background(), "/fapi/v3/account", nil, &struct{}{})
	if err == nil || strings.Contains(err.Error(), "?") || strings.Contains(err.Error(), "signature") {
		t.Fatalf("signed query leaked: %v", err)
	}
}

func TestSignedPOSTBodyUsesExactPayloadAndSignatureLast(t *testing.T) {
	oldEnv, oldEnabled := os.Getenv("BINANCE_ENV"), os.Getenv("BINANCE_TESTNET_ENABLE_ORDERS")
	t.Cleanup(func() {
		_ = os.Setenv("BINANCE_ENV", oldEnv)
		_ = os.Setenv("BINANCE_TESTNET_ENABLE_ORDERS", oldEnabled)
	})
	_ = os.Setenv("BINANCE_ENV", "TESTNET")
	_ = os.Setenv("BINANCE_TESTNET_ENABLE_ORDERS", "true")
	c, _ := NewTestnetClient("synthetic-key", "synthetic-secret")
	c.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(r.Body)
		parts := strings.Split(string(body), "&signature=")
		if len(parts) != 2 || parts[1] != signHMAC(parts[0], "synthetic-secret") {
			t.Fatal("POST signing bytes differ from wire body")
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{}`)), Header: make(http.Header)}, nil
	})}
	values := url.Values{"symbol": {"BTCUSDT"}, "timestamp": {"1499827319559"}}
	if err := c.Signed(context.Background(), http.MethodPost, "/fapi/v1/order", values, &struct{}{}); err != nil {
		t.Fatal(err)
	}
}
