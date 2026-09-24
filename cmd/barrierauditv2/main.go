package main

import (
	mainbarrier "binance_trader/internal/barrier/main"
	"flag"
	"fmt"
	"log"
	"math"
	"path/filepath"
	"sort"
)

func main() {
	if e := run(); e != nil {
		log.SetFlags(0)
		log.Fatal(e)
	}
}
func run() error {
	root := flag.String("root", ".\\data\\barriers\\main\\v2", "V2 root")
	symbol := flag.String("symbol", "BTCUSDT", "symbol")
	month := flag.String("month", "2024-01", "month")
	delay := flag.Int64("entry-delay-ms", 0, "delay")
	exitWait := flag.Int64("max-exit-reference-wait-ms", 1000, "timeout-reference wait")
	flag.Parse()
	profile := fmt.Sprintf("delay_%dms", *delay)
	p := filepath.Join(*root, profile, *symbol, (*month)[:4], fmt.Sprintf("%s-main-barriers-v2-%s-%s.parquet", *symbol, *month, profile))
	var rows, av, un, nan, inf, invalid, dupes, order, um, dm int64
	var prev int64
	seen := map[int64]bool{}
	waits := []int64{}
	var waitSum float64
	up := make([]int64, len(mainbarrier.BarrierGridBps))
	down := make([]int64, len(mainbarrier.BarrierGridBps))
	ta := make([]int64, len(mainbarrier.TimeoutHorizonsSeconds))
	tu := make([]int64, len(mainbarrier.TimeoutHorizonsSeconds))
	_, e := mainbarrier.ReadV2(p, func(x mainbarrier.BarrierOutcomeV2) error {
		rows++
		if seen[x.DecisionTimestampMs] {
			dupes++
		}
		seen[x.DecisionTimestampMs] = true
		if prev != 0 && x.DecisionTimestampMs <= prev {
			order++
		}
		prev = x.DecisionTimestampMs
		if x.EntryReadyTimestampMs != x.DecisionTimestampMs+x.EntryDelayMs {
			invalid++
		}
		if x.EntryReferenceAvailable {
			av++
			waits = append(waits, x.EntryWaitMs)
			waitSum += float64(x.EntryWaitMs)
			if x.EntryReferenceTimestampMs < x.EntryReadyTimestampMs || x.EntryReferenceAggTradeID <= 0 || x.EntryReferencePrice <= 0 {
				invalid++
			}
		} else {
			un++
			if x.EntryReferenceTimestampMs != 0 || x.EntryReferenceAggTradeID != 0 || x.EntryReferencePrice != 0 {
				invalid++
			}
		}
		var pu, pd mainbarrier.BarrierHit
		for i, b := range mainbarrier.BarrierGridBps {
			u, _ := x.Hit(true, b)
			d, _ := x.Hit(false, b)
			if u.TimestampMs > 0 {
				up[i]++
				if !x.EntryReferenceAvailable || u.AggTradeID <= x.EntryReferenceAggTradeID || u.TimestampMs >= x.EntryReferenceTimestampMs+int64(mainbarrier.MaxHorizonSeconds)*1000 || u.Price <= 0 {
					invalid++
				}
			} else if u.AggTradeID != 0 || u.Price != 0 {
				invalid++
			}
			if d.TimestampMs > 0 {
				down[i]++
				if !x.EntryReferenceAvailable || d.AggTradeID <= x.EntryReferenceAggTradeID || d.TimestampMs >= x.EntryReferenceTimestampMs+int64(mainbarrier.MaxHorizonSeconds)*1000 || d.Price <= 0 {
					invalid++
				}
			} else if d.AggTradeID != 0 || d.Price != 0 {
				invalid++
			}
			if i > 0 && u.TimestampMs > 0 && (pu.TimestampMs == 0 || before(u, pu)) {
				um++
			}
			if i > 0 && d.TimestampMs > 0 && (pd.TimestampMs == 0 || before(d, pd)) {
				dm++
			}
			pu, pd = u, d
			for _, z := range []float64{u.Price, d.Price} {
				if math.IsNaN(z) {
					nan++
				}
				if math.IsInf(z, 0) {
					inf++
				}
			}
		}
		for i, h := range mainbarrier.TimeoutHorizonsSeconds {
			z, _ := x.TimeoutReference(h)
			if x.EntryReferenceAvailable {
				if z.TimestampMs > 0 {
					ta[i]++
					target := x.EntryReferenceTimestampMs + int64(h)*1000
					if z.TimestampMs < target || z.TimestampMs-target > *exitWait || z.AggTradeID <= x.EntryReferenceAggTradeID || z.Price <= 0 {
						invalid++
					}
				} else {
					tu[i]++
					if z.AggTradeID != 0 || z.Price != 0 {
						invalid++
					}
				}
			}
		}
		return nil
	})
	if e != nil {
		return e
	}
	sort.Slice(waits, func(i, j int) bool { return waits[i] < waits[j] })
	q := func(p float64) int64 {
		if len(waits) == 0 {
			return 0
		}
		return waits[int(math.Round(p*float64(len(waits)-1)))]
	}
	fmt.Printf("Rows: %d\nEntry reference available/unavailable: %d / %d\nWait ms min/median/mean/p95/p99/max: %d / %d / %.3f / %d / %d / %d\n", rows, av, un, q(0), q(.5), waitSum/math.Max(1, float64(len(waits))), q(.95), q(.99), q(1))
	fmt.Println("bps up_hits up_rate down_hits down_rate")
	for i, b := range mainbarrier.BarrierGridBps {
		fmt.Printf("%4d %d %.6f %d %.6f\n", b, up[i], float64(up[i])/math.Max(1, float64(av)), down[i], float64(down[i])/math.Max(1, float64(av)))
	}
	fmt.Println("horizon timeout_available timeout_unavailable")
	for i, h := range mainbarrier.TimeoutHorizonsSeconds {
		fmt.Printf("%6d %d %d\n", h, ta[i], tu[i])
	}
	fmt.Printf("Monotonic up/down: %d/%d Duplicate/order: %d/%d NaN/Inf/Invalid: %d/%d/%d\n", um, dm, dupes, order, nan, inf, invalid)
	if um+dm+dupes+order+nan+inf+invalid > 0 {
		return fmt.Errorf("V2 audit failed")
	}
	return nil
}
func before(a, b mainbarrier.BarrierHit) bool {
	return a.TimestampMs < b.TimestampMs || a.TimestampMs == b.TimestampMs && a.AggTradeID < b.AggTradeID
}
