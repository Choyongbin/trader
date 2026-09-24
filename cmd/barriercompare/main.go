package main

import (
	"flag"
	"fmt"
	"log"
	"path/filepath"

	mainbarrier "binance_trader/internal/barrier/main"
)

var horizons = [...]int{60, 3600, 14400}
var statusNames = [...]string{"TP_FIRST", "SL_FIRST", "TIMEOUT", "ENTRY_REFERENCE_UNAVAILABLE", "EXIT_REFERENCE_UNAVAILABLE"}

type oldResults [2][3]byte

func main() {
	if e := run(); e != nil {
		log.SetFlags(0)
		log.Fatal(e)
	}
}

func run() error {
	v1root := flag.String("v1", ".\\data\\barriers\\main\\v1", "V1 root")
	v2root := flag.String("v2", ".\\data\\barriers\\main\\v2", "V2 root")
	symbol := flag.String("symbol", "BTCUSDT", "symbol")
	month := flag.String("month", "2024-01", "month")
	tp := flag.Int("tp-bps", 50, "TP bps")
	sl := flag.Int("sl-bps", 25, "SL bps")
	flag.Parse()
	profile := "delay_0ms"
	v1p := filepath.Join(*v1root, profile, *symbol, (*month)[:4], fmt.Sprintf("%s-main-barriers-v1-%s-%s.parquet", *symbol, *month, profile))
	v2p := filepath.Join(*v2root, profile, *symbol, (*month)[:4], fmt.Sprintf("%s-main-barriers-v2-%s-%s.parquet", *symbol, *month, profile))
	old := map[int64]oldResults{}
	_, e := mainbarrier.Read(v1p, func(b mainbarrier.BarrierOutcomeV1) error {
		var result oldResults
		for side := 0; side < 2; side++ {
			for i, h := range horizons {
				if !b.EntryAvailable {
					result[side][i] = 3
					continue
				}
				tpHit, slHit := hitsV1(b, side == 1, *tp, *sl)
				result[side][i] = classify(tpHit, slHit, b.DecisionTimestampMs+int64(h)*1000, true)
			}
		}
		old[b.DecisionTimestampMs] = result
		return nil
	})
	if e != nil {
		return e
	}
	var rows [2][3]int64
	var matrix [2][3][5][5]int64
	_, e = mainbarrier.ReadV2(v2p, func(b mainbarrier.BarrierOutcomeV2) error {
		prior, ok := old[b.DecisionTimestampMs]
		if !ok {
			return fmt.Errorf("missing V1 key %d", b.DecisionTimestampMs)
		}
		for side := 0; side < 2; side++ {
			for i, h := range horizons {
				var now byte
				if !b.EntryReferenceAvailable {
					now = 3
				} else {
					tpHit, slHit := hitsV2(b, side == 1, *tp, *sl)
					timeout, _ := b.TimeoutReference(h)
					now = classify(tpHit, slHit, b.EntryReferenceTimestampMs+int64(h)*1000, timeout.TimestampMs > 0)
				}
				rows[side][i]++
				matrix[side][i][prior[side][i]][now]++
			}
		}
		return nil
	})
	if e != nil {
		return e
	}
	for side := 0; side < 2; side++ {
		name := "LONG"
		if side == 1 {
			name = "SHORT"
		}
		fmt.Printf("%s TP=%dbps SL=%dbps, delay=0ms\n", name, *tp, *sl)
		for i, h := range horizons {
			var same, changed int64
			for from := 0; from < 5; from++ {
				for to := 0; to < 5; to++ {
					n := matrix[side][i][from][to]
					if from == to {
						same += n
					} else {
						changed += n
					}
				}
			}
			fmt.Printf("horizon=%ds same=%d changed=%d total=%d changed_rate=%.6f\n", h, same, changed, rows[side][i], float64(changed)/float64(rows[side][i]))
			for from := 0; from < 5; from++ {
				for to := 0; to < 5; to++ {
					if from != to && matrix[side][i][from][to] > 0 {
						fmt.Printf("  %s -> %s: %d\n", statusNames[from], statusNames[to], matrix[side][i][from][to])
					}
				}
			}
		}
	}
	return nil
}

func hitsV1(b mainbarrier.BarrierOutcomeV1, short bool, tp, sl int) (mainbarrier.BarrierHit, mainbarrier.BarrierHit) {
	if short {
		a, _ := b.Hit(false, tp)
		z, _ := b.Hit(true, sl)
		return a, z
	}
	a, _ := b.Hit(true, tp)
	z, _ := b.Hit(false, sl)
	return a, z
}
func hitsV2(b mainbarrier.BarrierOutcomeV2, short bool, tp, sl int) (mainbarrier.BarrierHit, mainbarrier.BarrierHit) {
	if short {
		a, _ := b.Hit(false, tp)
		z, _ := b.Hit(true, sl)
		return a, z
	}
	a, _ := b.Hit(true, tp)
	z, _ := b.Hit(false, sl)
	return a, z
}
func classify(tp, sl mainbarrier.BarrierHit, cutoff int64, timeoutAvailable bool) byte {
	tv := tp.TimestampMs > 0 && tp.TimestampMs < cutoff
	sv := sl.TimestampMs > 0 && sl.TimestampMs < cutoff
	if tv && (!sv || before(tp, sl)) {
		return 0
	}
	if sv {
		return 1
	}
	if timeoutAvailable {
		return 2
	}
	return 4
}
func before(a, b mainbarrier.BarrierHit) bool {
	return a.TimestampMs < b.TimestampMs || a.TimestampMs == b.TimestampMs && a.AggTradeID < b.AggTradeID
}
