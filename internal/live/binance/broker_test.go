package binance

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"testing"
)

type mockTransport struct {
	submitErr     error
	algoSubmitErr error
	algoQueryErr  error
	order         Order
	algo          AlgoOrder
	calls         []string
	values        []url.Values
}

func (m *mockTransport) Signed(_ context.Context, method, path string, values url.Values, out any) error {
	return m.request(method, path, values, out)
}

func (m *mockTransport) UserData(_ context.Context, path string, values url.Values, out any) error {
	return m.request("GET", path, values, out)
}

func (m *mockTransport) request(method, path string, values url.Values, out any) error {
	m.calls = append(m.calls, method+" "+path)
	m.values = append(m.values, values)
	if path == "/fapi/v1/order" && method == "POST" && m.submitErr != nil {
		return m.submitErr
	}
	if path == "/fapi/v1/algoOrder" && method == "POST" && m.algoSubmitErr != nil {
		return m.algoSubmitErr
	}
	if path == "/fapi/v1/algoOrder" && method == "GET" && m.algoQueryErr != nil {
		return m.algoQueryErr
	}
	if out != nil {
		value := any(m.order)
		if path == "/fapi/v1/algoOrder" {
			value = m.algo
		}
		b, _ := json.Marshal(value)
		_ = json.Unmarshal(b, out)
	}
	return nil
}

func TestAlgoTimeoutWithFailedLookupReturnsUnknownWithoutRetry(t *testing.T) {
	m := &mockTransport{algoSubmitErr: errors.New("timeout"), algoQueryErr: errors.New("lookup timeout")}
	b := &BinanceTestnetBroker{transport: m}
	_, err := b.ProtectiveStop(context.Background(), "BTCUSDT", "SELL", "0.001", "100", "safe-sl")
	if !errors.Is(err, ErrSubmitOutcomeUnknown) || len(m.calls) != 2 {
		t.Fatalf("calls=%v err=%v", m.calls, err)
	}
}

func TestAlgoTimeoutAfterAcceptedAdoptsByClientID(t *testing.T) {
	m := &mockTransport{algoSubmitErr: errors.New("timeout"), algo: AlgoOrder{ClientAlgoID: "safe-sl", Symbol: "BTCUSDT", Side: "SELL", OrderType: "STOP_MARKET", Quantity: "0.001", TriggerPrice: "100", AlgoStatus: "NEW", ReduceOnly: true}}
	b := &BinanceTestnetBroker{transport: m}
	o, err := b.ProtectiveStop(context.Background(), "BTCUSDT", "SELL", "0.001", "100", "safe-sl")
	if err != nil || o.ClientOrderID != "safe-sl" || len(m.calls) != 2 || m.calls[1] != "GET /fapi/v1/algoOrder" {
		t.Fatalf("order=%+v calls=%v err=%v", o, m.calls, err)
	}
}

func TestProtectiveOrdersUseCurrentAlgoEndpoint(t *testing.T) {
	m := &mockTransport{}
	b := &BinanceTestnetBroker{transport: m}
	if _, err := b.ProtectiveStop(context.Background(), "BTCUSDT", "SELL", "0.001", "100", "safe-sl"); err != nil {
		t.Fatal(err)
	}
	if len(m.calls) != 1 || m.calls[0] != "POST /fapi/v1/algoOrder" {
		t.Fatalf("calls=%v", m.calls)
	}
	if m.values[0].Get("algoType") != "CONDITIONAL" || m.values[0].Get("triggerPrice") != "100" || m.values[0].Get("workingType") != "MARK_PRICE" || m.values[0].Get("reduceOnly") != "true" {
		t.Fatalf("values=%v", m.values[0])
	}
}

func TestTimeoutAfterAcceptedAdoptsByClientID(t *testing.T) {
	m := &mockTransport{submitErr: errors.New("timeout"), order: Order{ClientOrderID: "fixed", Symbol: "BTCUSDT", Side: "BUY", Type: "MARKET", OriginalQuantity: ".001", Status: "FILLED", ExecutedQuantity: ".001", AveragePrice: "100"}}
	b := &BinanceTestnetBroker{transport: m}
	o, e := b.Submit(context.Background(), Order{Symbol: "BTCUSDT", Side: "BUY", Type: "MARKET", OriginalQuantity: ".001", ClientOrderID: "fixed"})
	if e != nil || o.LocalState != Filled || len(m.calls) != 2 {
		t.Fatalf("order=%+v calls=%v err=%v", o, m.calls, e)
	}
}

func TestDuplicateSubmitUsesLocalIdempotency(t *testing.T) {
	m := &mockTransport{order: Order{ClientOrderID: "fixed", Status: "FILLED", ExecutedQuantity: ".001", OriginalQuantity: ".001"}}
	b := &BinanceTestnetBroker{transport: m}
	request := Order{Symbol: "BTCUSDT", Side: "BUY", Type: "MARKET", OriginalQuantity: ".001", ClientOrderID: "fixed"}
	if _, err := b.Submit(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Submit(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if len(m.calls) != 1 {
		t.Fatalf("duplicate reached transport: %v", m.calls)
	}
	remaining, err := m.order.RemainingQuantity()
	if err != nil || remaining != 0 {
		t.Fatalf("remaining=%g err=%v", remaining, err)
	}
}

func TestPartialFillAndCancelRaceStates(t *testing.T) {
	for _, x := range []struct {
		status string
		want   OrderState
	}{{"PARTIALLY_FILLED", PartiallyFilled}, {"FILLED", Filled}, {"CANCELED", Canceled}} {
		if got := stateFromExchange(x.status); got != x.want {
			t.Fatalf("%s => %s", x.status, got)
		}
	}
}

func TestReconcileCases(t *testing.T) {
	cases := []struct {
		local   ReconcileState
		qty     float64
		orders  int
		want    ReconcileState
		enabled bool
	}{{ReconcileFlat, 0, 0, ReconcileFlat, true}, {ReconcileOpen, 1, 0, ReconcileOpen, true}, {ReconcileOpen, 0, 0, ReconcileFlat, true}, {ReconcileFlat, 1, 0, ReconcileUnknown, false}, {ReconcileFlat, 0, 1, ReconcileUnknown, false}, {ReconcilePending, 1, 0, ReconcileOpen, true}}
	for _, c := range cases {
		orders := make([]Order, c.orders)
		got := Reconcile(LocalPosition{State: c.local}, c.qty, orders)
		if got.State != c.want || got.NewEntriesEnabled != c.enabled {
			t.Fatalf("case=%+v got=%+v", c, got)
		}
	}
}
