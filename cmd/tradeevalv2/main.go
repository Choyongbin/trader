package main

import (
	mainbarrier "binance_trader/internal/barrier/main"
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
	root := flag.String("barriers", ".\\data\\barriers\\main\\v2", "V2 root")
	symbol := flag.String("symbol", "BTCUSDT", "symbol")
	month := flag.String("month", "2024-01", "month")
	delay := flag.Int64("entry-delay-ms", 0, "delay")
	sideText := flag.String("side", "long", "side")
	tp := flag.Int("tp-bps", 50, "TP")
	sl := flag.Int("sl-bps", 25, "SL")
	h := flag.Int("horizon", 3600, "horizon")
	ef := flag.Float64("entry-fee-rate", 0, "configured rate")
	xf := flag.Float64("exit-fee-rate", 0, "configured rate")
	es := flag.Float64("entry-slippage-bps", 0, "modeled adverse slippage")
	xs := flag.Float64("exit-slippage-bps", 0, "modeled adverse slippage")
	flag.Parse()
	side, e := tradelabel.ParseSide(*sideText)
	if e != nil {
		return e
	}
	spec := tradelabel.TradeSpec{Side: side, TPBps: *tp, SLBps: *sl, HorizonSeconds: *h}
	cost := tradelabel.CostProfile{EntryFeeRate: *ef, TPExitFeeRate: *xf, SLExitFeeRate: *xf, TimeoutExitFeeRate: *xf, EntrySlippageBps: *es, TPExitSlippageBps: *xs, SLExitSlippageBps: *xs, TimeoutSlippageBps: *xs}
	profile := fmt.Sprintf("delay_%dms", *delay)
	p := filepath.Join(*root, profile, *symbol, (*month)[:4], fmt.Sprintf("%s-main-barriers-v2-%s-%s.parquet", *symbol, *month, profile))
	counts := map[tradelabel.Status]int64{}
	var total, valid, gw, nw int64
	var gross, fee, slip, net float64
	_, e = mainbarrier.ReadV2(p, func(b mainbarrier.BarrierOutcomeV2) error {
		r, er := tradelabel.EvaluateV2(b, spec, cost)
		if er != nil {
			return er
		}
		total++
		counts[r.Status]++
		if !r.LabelValid {
			return nil
		}
		valid++
		if r.GrossProfitable {
			gw++
		}
		if r.NetProfitableExFunding {
			nw++
		}
		gross += r.GrossMarketReturn
		fee += r.FeeCostReturn
		slip += r.ModeledSlippageCostReturn
		net += r.NetReturnExFunding
		return nil
	})
	if e != nil {
		return e
	}
	d := float64(valid)
	if d == 0 {
		d = 1
	}
	fmt.Printf("Reference-price labels; modeled costs; funding excluded.\nRows/label-valid: %d / %d\nTP_FIRST/SL_FIRST/TIMEOUT: %d / %d / %d\nENTRY_REFERENCE_UNAVAILABLE/EXIT_REFERENCE_UNAVAILABLE: %d / %d\nGross/net profitable: %d (%.6f) / %d (%.6f)\nMean gross_market_return: %.9f\nMean fee_cost_return: %.9f\nMean modeled_slippage_cost_return: %.9f\nMean net_return_ex_funding: %.9f\n", total, valid, counts[tradelabel.TPFirst], counts[tradelabel.SLFirst], counts[tradelabel.Timeout], counts[tradelabel.EntryReferenceUnavailable], counts[tradelabel.ExitReferenceUnavailable], gw, float64(gw)/d, nw, float64(nw)/d, gross/d, fee/d, slip/d, net/d)
	return nil
}
