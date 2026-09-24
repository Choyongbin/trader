package binance

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"testing"
)

type mockTransport struct {
	submitErr error
	order     Order
	calls     []string
	values    []url.Values
}

func (m *mockTransport) Signed(_ context.Context, method, path string, values url.Values, out any) error {
	m.calls = append(m.calls, method+" "+path)
	m.values = append(m.values, values)
	if path == "/fapi/v1/order" && method == "POST" && m.submitErr != nil {
		return m.submitErr
	}
	if out != nil {
		b, _ := json.Marshal(m.order)
		_ = json.Unmarshal(b, out)
	}
	return nil
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
	m := &mockTransport{submitErr: errors.New("timeout"), order: Order{ClientOrderID: "fixed", Status: "FILLED", ExecutedQuantity: ".001", AveragePrice: "100"}}
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
