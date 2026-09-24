package main

import (
	"binance_trader/internal/data/aggtrade"
	"binance_trader/internal/market"
	"flag"
	"fmt"
	"log"
	"path/filepath"
	"time"
)

type entry struct {
	available      bool
	id, time, wait int64
	price          float64
}
type profile struct {
	delay, next int64
	values      []entry
}

func main() {
	if e := run(); e != nil {
		log.SetFlags(0)
		log.Fatal(e)
	}
}
func run() error {
	history := flag.String("history", ".\\history", "raw root")
	symbol := flag.String("symbol", "BTCUSDT", "symbol")
	month := flag.String("month", "2024-01", "month")
	n := flag.Int("decisions", 10000, "limited decision sample")
	maxWait := flag.Int64("max-entry-wait-ms", 1000, "maximum wait")
	flag.Parse()
	t, e := time.Parse("2006-01", *month)
	if e != nil {
		return e
	}
	ps := []profile{{delay: 0, next: t.UnixMilli() + 5000}, {delay: 100, next: t.UnixMilli() + 5000}, {delay: 250, next: t.UnixMilli() + 5000}}
	done := func() bool {
		for i := range ps {
			if len(ps[i].values) < *n {
				return false
			}
		}
		return true
	}
	on := func(a market.AggTrade) error {
		for i := range ps {
			p := &ps[i]
			for len(p.values) < *n && p.next+p.delay <= a.TradeTimeMs {
				ready := p.next + p.delay
				x := entry{}
				if a.TradeTimeMs-ready <= *maxWait {
					x = entry{true, a.AggTradeID, a.TradeTimeMs, a.TradeTimeMs - ready, a.Price}
				}
				p.values = append(p.values, x)
				p.next += 5000
			}
		}
		if done() {
			return aggtrade.ErrStop
		}
		return nil
	}
	path := filepath.Join(*history, fmt.Sprintf("%s-aggTrades-%s.zip", *symbol, *month))
	if _, _, e = aggtrade.ReadArchive(path, aggtrade.ReadOptions{OnTrade: on}); e != nil {
		return e
	}
	fmt.Printf("Limited entry comparison: first %d decisions of %s (no barrier artifacts written)\n", *n, *month)
	base := ps[0]
	for _, p := range ps {
		var avail, changed int64
		var waitSum, absPriceBps float64
		var comparable int64
		for i, x := range p.values {
			if x.available {
				avail++
				waitSum += float64(x.wait)
			}
			if i < len(base.values) && x.available && base.values[i].available {
				comparable++
				if x.id != base.values[i].id {
					changed++
				}
				absPriceBps += abs(x.price/base.values[i].price-1) * 10000
			}
		}
		fmt.Printf("delay=%dms available=%d mean_wait_ms=%.3f changed_vs_0ms=%d/%d mean_abs_price_change_bps=%.6f\n", p.delay, avail, waitSum/max(1, float64(avail)), changed, comparable, absPriceBps/max(1, float64(comparable)))
	}
	return nil
}
func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
