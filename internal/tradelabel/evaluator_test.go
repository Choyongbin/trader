package tradelabel

import (
	mainbarrier "binance_trader/internal/barrier/main"
	mainoutcome "binance_trader/internal/outcome/main"
	"fmt"
	"math"
	"reflect"
	"testing"
)

func close(a, b float64) bool { return math.Abs(a-b) < 1e-12 }
func base() (mainbarrier.BarrierOutcomeV1, mainoutcome.MainOutcomeV2) {
	b := mainbarrier.BarrierOutcomeV1{DecisionTimestampMs: 1000, EntryAvailable: true, EntryReferencePrice: 100}
	o := mainoutcome.MainOutcomeV2{DecisionTimestampMs: 1000, ReferenceEntryPrice: 100, MarketReturn300s: .1}
	return b, o
}
func TestLongTPAndSameMsID(t *testing.T) {
	b, o := base()
	set := func(up bool, bps int, h mainbarrier.BarrierHit) {
		v := reflect.ValueOf(&b).Elem()
		prefix := "Down"
		if up {
			prefix = "Up"
		}
		v.FieldByName(fmt.Sprintf("%s%dBpsTimestampMs", prefix, bps)).SetInt(h.TimestampMs)
		v.FieldByName(fmt.Sprintf("%s%dBpsAggTradeID", prefix, bps)).SetInt(h.AggTradeID)
		v.FieldByName(fmt.Sprintf("%s%dBpsPrice", prefix, bps)).SetFloat(h.Price)
	}
	set(true, 50, mainbarrier.BarrierHit{TimestampMs: 2000, AggTradeID: 10, Price: 100.5})
	set(false, 25, mainbarrier.BarrierHit{TimestampMs: 2000, AggTradeID: 11, Price: 99.75})
	r, e := Evaluate(b, o, TradeSpec{Long, 50, 25, 300}, CostProfile{})
	if e != nil || r.Status != TPFirst {
		t.Fatalf("%+v %v", r, e)
	}
}
func TestTimeoutFeeAndSlippage(t *testing.T) {
	b, o := base()
	c := CostProfile{EntryFeeRate: .001, TimeoutExitFeeRate: .002, EntrySlippageBps: 10, TimeoutSlippageBps: 10}
	r, e := Evaluate(b, o, TradeSpec{Long, 50, 25, 300}, c)
	if e != nil {
		t.Fatal(e)
	}
	if r.Status != Timeout || !close(r.GrossReturn, .1) {
		t.Fatalf("%+v", r)
	}
	wantFee := .001 + (r.AdjustedExitPrice/r.AdjustedEntryPrice)*.002
	if !close(r.FeeCostReturn, wantFee) || r.SlippageEffect >= 0 {
		t.Fatalf("%+v", r)
	}
}
func TestShortLinearReturn(t *testing.T) {
	b, o := base()
	o.MarketReturn300s = -.1
	r, e := Evaluate(b, o, TradeSpec{Short, 50, 25, 300}, CostProfile{})
	if e != nil || !close(r.GrossReturn, .1) {
		t.Fatalf("%+v %v", r, e)
	}
}
func TestFeeTurnsWinIntoLoss(t *testing.T) {
	b, o := base()
	o.MarketReturn300s = .0005
	r, e := Evaluate(b, o, TradeSpec{Long, 50, 25, 300}, CostProfile{EntryFeeRate: .0004, TimeoutExitFeeRate: .0004})
	if e != nil || !r.GrossProfitable || r.NetProfitableExFunding {
		t.Fatalf("%+v %v", r, e)
	}
}

func TestV2EntryBasedHorizonAndTimeout(t *testing.T) {
	b := mainbarrier.BarrierOutcomeV2{DecisionTimestampMs: 1000, EntryReferenceAvailable: true, EntryReferenceTimestampMs: 1500, EntryReferencePrice: 100}
	v := reflect.ValueOf(&b).Elem()
	v.FieldByName("Up50BpsTimestampMs").SetInt(2200)
	v.FieldByName("Up50BpsAggTradeID").SetInt(2)
	v.FieldByName("Up50BpsPrice").SetFloat(100.5)
	v.FieldByName("Timeout60sTimestampMs").SetInt(61500)
	v.FieldByName("Timeout60sAggTradeID").SetInt(3)
	v.FieldByName("Timeout60sReferencePrice").SetFloat(101)
	r, e := EvaluateV2(b, TradeSpec{Long, 50, 25, 60}, CostProfile{})
	if e != nil || r.Status != TPFirst || !r.LabelValid {
		t.Fatalf("%+v %v", r, e)
	}
}
func TestV2UnavailableIsUnlabeled(t *testing.T) {
	r, e := EvaluateV2(mainbarrier.BarrierOutcomeV2{DecisionTimestampMs: 1}, TradeSpec{Long, 50, 25, 60}, CostProfile{})
	if e != nil || r.Status != EntryReferenceUnavailable || r.LabelValid || r.NetProfitableExFunding {
		t.Fatalf("%+v %v", r, e)
	}
}
