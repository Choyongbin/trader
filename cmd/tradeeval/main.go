package main

import (
	mainbarrier "binance_trader/internal/barrier/main"
	mainoutcome "binance_trader/internal/outcome/main"
	"binance_trader/internal/tradelabel"
	"flag"
	"fmt"
	"log"
	"path/filepath"
)

func main() {
	if e := run(); e != nil {
		log.SetFlags(0)
		log.Fatal(e)
	}
}
func run() error {
	barriers := flag.String("barriers", ".\\data\\barriers\\main\\v1", "barrier root")
	outcomes := flag.String("outcomes", ".\\data\\outcomes\\main\\v2", "outcome root")
	symbol := flag.String("symbol", "BTCUSDT", "symbol")
	month := flag.String("month", "2024-01", "month")
	delay := flag.Int64("entry-delay-ms", 0, "delay profile")
	sideText := flag.String("side", "long", "long/short")
	tp := flag.Int("tp-bps", 50, "TP bps")
	sl := flag.Int("sl-bps", 25, "SL bps")
	h := flag.Int("horizon", 3600, "seconds")
	ef := flag.Float64("entry-fee-rate", 0, "configured entry fee rate")
	xf := flag.Float64("exit-fee-rate", 0, "configured TP/SL/timeout exit fee rate")
	es := flag.Float64("entry-slippage-bps", 0, "adverse entry slippage")
	xs := flag.Float64("exit-slippage-bps", 0, "adverse TP/SL/timeout exit slippage")
	flag.Parse()
	side, e := tradelabel.ParseSide(*sideText)
	if e != nil {
		return e
	}
	spec := tradelabel.TradeSpec{Side: side, TPBps: *tp, SLBps: *sl, HorizonSeconds: *h}
	if e = tradelabel.ValidateSpec(spec); e != nil {
		return e
	}
	cost := tradelabel.CostProfile{EntryFeeRate: *ef, TPExitFeeRate: *xf, SLExitFeeRate: *xf, TimeoutExitFeeRate: *xf, EntrySlippageBps: *es, TPExitSlippageBps: *xs, SLExitSlippageBps: *xs, TimeoutSlippageBps: *xs}
	op := filepath.Join(*outcomes, *symbol, (*month)[:4], fmt.Sprintf("%s-main-outcomes-v2-%s.parquet", *symbol, *month))
	om := map[int64]mainoutcome.MainOutcomeV2{}
	_, e = mainoutcome.Read(op, func(o mainoutcome.MainOutcomeV2) error { om[o.DecisionTimestampMs] = o; return nil })
	if e != nil {
		return e
	}
	profile := fmt.Sprintf("delay_%dms", *delay)
	bp := filepath.Join(*barriers, profile, *symbol, (*month)[:4], fmt.Sprintf("%s-main-barriers-v1-%s-%s.parquet", *symbol, *month, profile))
	counts := map[tradelabel.Status]int64{}
	var n, gwin, nwin int64
	var gross, fee, slip, net float64
	_, e = mainbarrier.Read(bp, func(b mainbarrier.BarrierOutcomeV1) error {
		o, ok := om[b.DecisionTimestampMs]
		if !ok {
			return fmt.Errorf("missing Outcome V2 at %d", b.DecisionTimestampMs)
		}
		r, er := tradelabel.Evaluate(b, o, spec, cost)
		if er != nil {
			return er
		}
		n++
		counts[r.Status]++
		if r.GrossProfitable {
			gwin++
		}
		if r.NetProfitableExFunding {
			nwin++
		}
		gross += r.GrossReturn
		fee += r.FeeCostReturn
		slip += r.SlippageEffect
		net += r.NetReturnExFunding
		return nil
	})
	if e != nil {
		return e
	}
	den := float64(n)
	if den == 0 {
		den = 1
	}
	fmt.Printf("Cost profile is user-supplied; funding is excluded.\nTrades evaluated: %d\nTP_FIRST: %d\nSL_FIRST: %d\nTIMEOUT: %d\nENTRY_UNAVAILABLE: %d\nGross profitable: %d (%.6f)\nNet profitable ex funding: %d (%.6f)\nMean gross return: %.9f\nMean fee cost: %.9f\nMean slippage effect: %.9f\nMean net return ex funding: %.9f\n", n, counts[tradelabel.TPFirst], counts[tradelabel.SLFirst], counts[tradelabel.Timeout], counts[tradelabel.EntryUnavailable], gwin, float64(gwin)/den, nwin, float64(nwin)/den, gross/den, fee/den, slip/den, net/den)
	return nil
}
