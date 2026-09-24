package binance

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const TestnetRESTBaseURL = "https://demo-fapi.binance.com"

type Environment string

const (
	Testnet    Environment = "TESTNET"
	PublicOnly Environment = "PUBLIC_ONLY"
)

func EnvironmentFromProcess() (Environment, error) {
	v := Environment(strings.TrimSpace(os.Getenv("BINANCE_ENV")))
	if v != Testnet && v != PublicOnly {
		return "", fmt.Errorf("BINANCE_ENV must be explicitly TESTNET or PUBLIC_ONLY")
	}
	return v, nil
}

func OrdersEnabled(env Environment) bool {
	return env == Testnet && strings.EqualFold(strings.TrimSpace(os.Getenv("BINANCE_TESTNET_ENABLE_ORDERS")), "true")
}

func ValidateTestnetBaseURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || !strings.EqualFold(u.Hostname(), "demo-fapi.binance.com") {
		return fmt.Errorf("non-Testnet REST endpoint rejected")
	}
	return nil
}

type Filter struct {
	FilterType, MinPrice, MaxPrice, TickSize string
	MinQty, MaxQty, StepSize, Notional       string
}

type Symbol struct {
	Symbol, Status, BaseAsset, QuoteAsset, ContractType string
	PricePrecision, QuantityPrecision                   int
	Filters                                             []Filter
}

type exchangeInfo struct {
	ServerTime int64
	Symbols    []Symbol
}

type Client struct {
	BaseURL, APIKey string
	secret          string
	HTTP            *http.Client
	clockOffsetMs   atomic.Int64
}

type serverTimeResponse struct {
	ServerTime int64 `json:"serverTime"`
}

func NewPublicTestnetClient() (*Client, error) {
	if err := ValidateTestnetBaseURL(TestnetRESTBaseURL); err != nil {
		return nil, err
	}
	return &Client{BaseURL: TestnetRESTBaseURL, HTTP: &http.Client{Timeout: 15 * time.Second}}, nil
}

func NewTestnetClientFromEnvironment() (*Client, error) {
	env, err := EnvironmentFromProcess()
	if err != nil {
		return nil, err
	}
	if !OrdersEnabled(env) {
		return nil, fmt.Errorf("Testnet order guard not enabled")
	}
	key, secret := os.Getenv("BINANCE_TESTNET_API_KEY"), os.Getenv("BINANCE_TESTNET_API_SECRET")
	if strings.TrimSpace(key) == "" || strings.TrimSpace(secret) == "" {
		return nil, fmt.Errorf("Testnet credentials unavailable")
	}
	c, err := NewPublicTestnetClient()
	if err != nil {
		return nil, err
	}
	c.APIKey, c.secret = key, secret
	return c, nil
}

// NewTestnetClient creates an authenticated Demo/Testnet client without
// requiring the order-enable flag. Trade methods still enforce that flag.
func NewTestnetClient(apiKey, secret string) (*Client, error) {
	if strings.TrimSpace(apiKey) == "" || strings.TrimSpace(secret) == "" {
		return nil, fmt.Errorf("Testnet credentials unavailable")
	}
	c, err := NewPublicTestnetClient()
	if err != nil {
		return nil, err
	}
	c.APIKey, c.secret = apiKey, secret
	return c, nil
}

func (c *Client) ExchangeSymbol(ctx context.Context, symbol string) (Symbol, int64, error) {
	var x exchangeInfo
	if err := c.getJSON(ctx, "/fapi/v1/exchangeInfo", nil, &x); err != nil {
		return Symbol{}, 0, err
	}
	for _, s := range x.Symbols {
		if s.Symbol == symbol {
			return s, x.ServerTime, nil
		}
	}
	return Symbol{}, x.ServerTime, fmt.Errorf("symbol %s absent", symbol)
}

func (c *Client) SyncTime(ctx context.Context) (int64, error) {
	before := time.Now().UnixMilli()
	var x serverTimeResponse
	if err := c.getJSON(ctx, "/fapi/v1/time", nil, &x); err != nil {
		return 0, err
	}
	after := time.Now().UnixMilli()
	if x.ServerTime <= 0 {
		return 0, fmt.Errorf("invalid Demo server time")
	}
	offset := x.ServerTime - (before+after)/2
	c.clockOffsetMs.Store(offset)
	return offset, nil
}

func (c *Client) getJSON(ctx context.Context, path string, q url.Values, out any) error {
	u := c.BaseURL + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return fmt.Errorf("Binance request transport failure")
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		return sanitizedBinanceError(resp.StatusCode, b)
	}
	return json.Unmarshal(b, out)
}

func (c *Client) Signed(ctx context.Context, method, path string, values url.Values, out any) error {
	return c.signed(ctx, method, path, values, out, true)
}

// UserData signs a read-only authenticated request. It can never submit,
// modify, or cancel an order.
func (c *Client) UserData(ctx context.Context, path string, values url.Values, out any) error {
	return c.signed(ctx, http.MethodGet, path, values, out, false)
}

func (c *Client) signed(ctx context.Context, method, path string, values url.Values, out any, trade bool) error {
	if err := ValidateTestnetBaseURL(c.BaseURL); err != nil {
		return err
	}
	if trade {
		env, err := EnvironmentFromProcess()
		if err != nil || !OrdersEnabled(env) {
			return fmt.Errorf("order endpoint safety guard rejected request")
		}
	} else if method != http.MethodGet {
		return fmt.Errorf("read-only user data method rejected")
	}
	if c.APIKey == "" || c.secret == "" {
		return fmt.Errorf("credentials unavailable")
	}
	if values == nil {
		values = url.Values{}
	} else {
		values = cloneValues(values)
	}
	values.Del("signature")
	for key, entries := range values {
		if len(entries) == 0 || (len(entries) == 1 && entries[0] == "") {
			values.Del(key)
		}
	}
	if values.Get("timestamp") == "" {
		values.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli()+c.clockOffsetMs.Load(), 10))
	}
	if values.Get("recvWindow") == "" {
		values.Set("recvWindow", "5000")
	}
	payload := values.Encode()
	signature := signHMAC(payload, c.secret)
	wirePayload := payload + "&signature=" + url.QueryEscape(signature)
	var body io.Reader
	u := c.BaseURL + path
	if method == http.MethodGet || method == http.MethodDelete {
		u += "?" + wirePayload
	} else {
		body = bytes.NewBufferString(wirePayload)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return err
	}
	req.Header.Set("X-MBX-APIKEY", c.APIKey)
	if body != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		// net/http errors include the full signed URL. Never propagate its query.
		return fmt.Errorf("Binance signed request transport failure")
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		return sanitizedBinanceError(resp.StatusCode, b)
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(b, out)
}

func signHMAC(payload, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(payload))
	return hex.EncodeToString(mac.Sum(nil))
}

func cloneValues(values url.Values) url.Values {
	result := make(url.Values, len(values))
	for key, entries := range values {
		result[key] = append([]string(nil), entries...)
	}
	return result
}

func sanitizedBinanceError(status int, body []byte) error {
	var value struct {
		Code    int    `json:"code"`
		Message string `json:"msg"`
	}
	if json.Unmarshal(body, &value) == nil && value.Message != "" {
		return fmt.Errorf("Binance HTTP %d code=%d message=%s", status, value.Code, value.Message)
	}
	return fmt.Errorf("Binance HTTP %d request rejected", status)
}

func filterFor(s Symbol, kind string) (Filter, bool) {
	for _, f := range s.Filters {
		if f.FilterType == kind {
			return f, true
		}
	}
	return Filter{}, false
}

func floorStep(value float64, stepText string) (float64, error) {
	step, err := strconv.ParseFloat(stepText, 64)
	if err != nil || step <= 0 {
		return 0, fmt.Errorf("invalid step %q", stepText)
	}
	n := math.Floor((value + step*1e-12) / step)
	return n * step, nil
}

func ceilStep(value float64, stepText string) (float64, error) {
	step, err := strconv.ParseFloat(stepText, 64)
	if err != nil || step <= 0 {
		return 0, fmt.Errorf("invalid step %q", stepText)
	}
	n := math.Ceil((value - step*1e-12) / step)
	return n * step, nil
}

func NormalizePrice(s Symbol, price float64) (float64, error) {
	f, ok := filterFor(s, "PRICE_FILTER")
	if !ok {
		return 0, fmt.Errorf("PRICE_FILTER absent")
	}
	v, err := floorStep(price, f.TickSize)
	if err != nil {
		return 0, err
	}
	min, _ := strconv.ParseFloat(f.MinPrice, 64)
	max, _ := strconv.ParseFloat(f.MaxPrice, 64)
	if v < min || (max > 0 && v > max) {
		return 0, fmt.Errorf("price outside filter")
	}
	return v, nil
}

func NormalizeQuantity(s Symbol, quantity float64, market bool) (float64, error) {
	return normalizeQuantity(s, quantity, market, false)
}

func NormalizeQuantityUp(s Symbol, quantity float64, market bool) (float64, error) {
	return normalizeQuantity(s, quantity, market, true)
}

func QuantityFilter(s Symbol, market bool) (Filter, error) {
	kind := "LOT_SIZE"
	if market {
		if _, ok := filterFor(s, "MARKET_LOT_SIZE"); ok {
			kind = "MARKET_LOT_SIZE"
		}
	}
	f, ok := filterFor(s, kind)
	if !ok {
		return Filter{}, fmt.Errorf("%s absent", kind)
	}
	return f, nil
}

func FormatQuantity(s Symbol, quantity float64, market bool) (string, error) {
	f, err := QuantityFilter(s, market)
	if err != nil {
		return "", err
	}
	return formatByIncrement(quantity, f.StepSize)
}

func FormatPrice(s Symbol, price float64) (string, error) {
	f, ok := filterFor(s, "PRICE_FILTER")
	if !ok {
		return "", fmt.Errorf("PRICE_FILTER absent")
	}
	return formatByIncrement(price, f.TickSize)
}

func formatByIncrement(value float64, increment string) (string, error) {
	if _, err := strconv.ParseFloat(increment, 64); err != nil {
		return "", err
	}
	dot := strings.IndexByte(increment, '.')
	precision := 0
	if dot >= 0 {
		precision = len(strings.TrimRight(increment[dot+1:], "0"))
	}
	return strconv.FormatFloat(value, 'f', precision, 64), nil
}

func normalizeQuantity(s Symbol, quantity float64, market, roundUp bool) (float64, error) {
	kind := "LOT_SIZE"
	if market {
		if _, ok := filterFor(s, "MARKET_LOT_SIZE"); ok {
			kind = "MARKET_LOT_SIZE"
		}
	}
	f, ok := filterFor(s, kind)
	if !ok {
		return 0, fmt.Errorf("%s absent", kind)
	}
	v, err := floorStep(quantity, f.StepSize)
	if roundUp {
		v, err = ceilStep(quantity, f.StepSize)
	}
	if err != nil {
		return 0, err
	}
	min, _ := strconv.ParseFloat(f.MinQty, 64)
	max, _ := strconv.ParseFloat(f.MaxQty, 64)
	if v < min || (max > 0 && v > max) {
		return 0, fmt.Errorf("quantity outside filter")
	}
	return v, nil
}

func ValidateMinNotional(s Symbol, price, quantity float64) error {
	f, ok := filterFor(s, "MIN_NOTIONAL")
	if !ok {
		f, ok = filterFor(s, "NOTIONAL")
	}
	if !ok {
		return fmt.Errorf("notional filter absent")
	}
	min, err := strconv.ParseFloat(f.Notional, 64)
	if err != nil {
		return err
	}
	if price*quantity < min {
		return fmt.Errorf("notional below minimum")
	}
	return nil
}

func ValidateSymbolFilters(s Symbol, price, quantity float64, market bool) error {
	if s.Status != "TRADING" || s.ContractType != "PERPETUAL" {
		return fmt.Errorf("symbol unavailable")
	}
	if _, err := NormalizePrice(s, price); err != nil {
		return err
	}
	if _, err := NormalizeQuantity(s, quantity, market); err != nil {
		return err
	}
	return ValidateMinNotional(s, price, quantity)
}

func DeterministicClientOrderID(strategy string, decision int64, candidate, action string, sequence int) string {
	raw := fmt.Sprintf("%s_%d_%s_%s_%d", strategy, decision, candidate, action, sequence)
	clean := strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' {
			return r
		}
		return -1
	}, raw)
	if len(clean) <= 36 {
		return clean
	}
	s := sha256.Sum256([]byte(clean))
	prefix := clean
	if len(prefix) > 19 {
		prefix = prefix[:19]
	}
	return prefix + "_" + hex.EncodeToString(s[:8])
}
