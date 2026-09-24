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

type hs struct {
	count, same      int64
	overSum, overMax float64
	times            []int64
}

func main() {
	if e := run(); e != nil {
		log.SetFlags(0)
		log.Fatal(e)
	}
}
func run() error {
	root := flag.String("root", ".\\data\\barriers\\main\\v1", "barrier root")
	symbol := flag.String("symbol", "BTCUSDT", "symbol")
	month := flag.String("month", "2024-01", "month")
	delay := flag.Int64("entry-delay-ms", 0, "delay profile")
	flag.Parse()
	profile := fmt.Sprintf("delay_%dms", *delay)
	p := filepath.Join(*root, profile, *symbol, (*month)[:4], fmt.Sprintf("%s-main-barriers-v1-%s-%s.parquet", *symbol, *month, profile))
	var rows, available, unavailable, nan, inf, invalid, dupes, order, upMono, downMono int64
	var prev int64
	keys := map[int64]bool{}
	waits := []int64{}
	var waitSum float64
	up := make([]hs, len(mainbarrier.BarrierGridBps))
	down := make([]hs, len(mainbarrier.BarrierGridBps))
	_, e := mainbarrier.Read(p, func(x mainbarrier.BarrierOutcomeV1) error {
		rows++
		if keys[x.DecisionTimestampMs] {
			dupes++
		}
		keys[x.DecisionTimestampMs] = true
		if prev != 0 && x.DecisionTimestampMs <= prev {
			order++
		}
		prev = x.DecisionTimestampMs
		if x.EntryAvailable {
			available++
			waits = append(waits, x.EntryWaitMs)
			waitSum += float64(x.EntryWaitMs)
			if x.EntryReferencePrice <= 0 {
				invalid++
			}
		} else {
			unavailable++
			if x.EntryTradeTimestampMs != 0 || x.EntryAggTradeID != 0 || x.EntryReferencePrice != 0 {
				invalid++
			}
		}
		for _, v := range []float64{x.EntryReferencePrice} {
			if math.IsNaN(v) {
				nan++
			}
			if math.IsInf(v, 0) {
				inf++
			}
		}
		var pu, pd mainbarrier.BarrierHit
		for i, b := range mainbarrier.BarrierGridBps {
			u, _ := x.Hit(true, b)
			d, _ := x.Hit(false, b)
			check := func(h mainbarrier.BarrierHit, s *hs, upward bool) {
				if (h.TimestampMs == 0) != (h.AggTradeID == 0) || (h.TimestampMs == 0) != (h.Price == 0) {
					invalid++
				}
				if h.TimestampMs > 0 {
					s.count++
					if h.TimestampMs == x.EntryTradeTimestampMs {
						s.same++
					}
					dt := h.TimestampMs - x.DecisionTimestampMs
					s.times = append(s.times, dt)
					target := x.EntryReferencePrice * (1 + float64(b)/10000)
					if !upward {
						target = x.EntryReferencePrice * (1 - float64(b)/10000)
					}
					ov := (h.Price - target) / x.EntryReferencePrice * 10000
					if !upward {
						ov = (target - h.Price) / x.EntryReferencePrice * 10000
					}
					s.overSum += ov
					if ov > s.overMax {
						s.overMax = ov
					}
					if ov < 0 {
						invalid++
					}
					if math.IsNaN(h.Price) {
						nan++
					}
					if math.IsInf(h.Price, 0) {
						inf++
					}
				}
			}
			check(u, &up[i], true)
			check(d, &down[i], false)
			if i > 0 && u.TimestampMs > 0 && (pu.TimestampMs == 0 || before(u, pu)) {
				upMono++
			}
			if i > 0 && d.TimestampMs > 0 && (pd.TimestampMs == 0 || before(d, pd)) {
				downMono++
			}
			pu, pd = u, d
		}
		return nil
	})
	if e != nil {
		return e
	}
	sort.Slice(waits, func(i, j int) bool { return waits[i] < waits[j] })
	q := func(a []int64, p float64) int64 {
		if len(a) == 0 {
			return 0
		}
		return a[int(math.Round(p*float64(len(a)-1)))]
	}
	fmt.Printf("Rows: %d\nEntry available/unavailable: %d / %d\nEntry wait ms min/median/mean/p95/p99/max: %d / %d / %.3f / %d / %d / %d\n", rows, available, unavailable, q(waits, 0), q(waits, .5), waitSum/math.Max(1, float64(len(waits))), q(waits, .95), q(waits, .99), q(waits, 1))
	fmt.Println("bps  up_hits up_rate down_hits down_rate up_t50 up_t90 up_t95 down_t50 down_t90 down_t95 up_avg_over down_avg_over up_max_over down_max_over")
	for i, b := range mainbarrier.BarrierGridBps {
		sort.Slice(up[i].times, func(a, c int) bool { return up[i].times[a] < up[i].times[c] })
		sort.Slice(down[i].times, func(a, c int) bool { return down[i].times[a] < down[i].times[c] })
		fmt.Printf("%4d %8d %.6f %10d %.6f %d %d %d %d %d %d %.5f %.5f %.5f %.5f\n", b, up[i].count, float64(up[i].count)/math.Max(1, float64(available)), down[i].count, float64(down[i].count)/math.Max(1, float64(available)), q(up[i].times, .5), q(up[i].times, .9), q(up[i].times, .95), q(down[i].times, .5), q(down[i].times, .9), q(down[i].times, .95), up[i].overSum/math.Max(1, float64(up[i].count)), down[i].overSum/math.Max(1, float64(down[i].count)), up[i].overMax, down[i].overMax)
	}
	fmt.Printf("Same-ms crossings up/down: %d / %d\nMonotonicity violations up/down: %d / %d\nDuplicate/order violations: %d / %d\nNaN/Inf/Invalid: %d / %d / %d\n", sumSame(up), sumSame(down), upMono, downMono, dupes, order, nan, inf, invalid)
	if dupes+order+upMono+downMono+nan+inf+invalid > 0 {
		return fmt.Errorf("audit failed")
	}
	return nil
}
func before(a, b mainbarrier.BarrierHit) bool {
	return a.TimestampMs < b.TimestampMs || a.TimestampMs == b.TimestampMs && a.AggTradeID < b.AggTradeID
}
func sumSame(a []hs) int64 {
	var n int64
	for _, x := range a {
		n += x.same
	}
	return n
}
