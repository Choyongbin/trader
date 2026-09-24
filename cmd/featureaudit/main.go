package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"path/filepath"
	"reflect"
	"strings"

	mainfeature "binance_trader/internal/feature/main"
)

func main() {
	if err := run(); err != nil {
		log.SetFlags(0)
		log.Fatal(err)
	}
}
func run() error {
	root := flag.String("input", ".\\data\\features\\main\\v1", "feature root")
	symbol := flag.String("symbol", "BTCUSDT", "symbol")
	month := flag.String("month", "2024-01", "month YYYY-MM")
	flag.Parse()
	*symbol = strings.ToUpper(*symbol)
	path := filepath.Join(*root, *symbol, (*month)[:4], fmt.Sprintf("%s-main-features-v1-%s.parquet", *symbol, *month))
	var previous int64
	invalid, nanCount, infCount := int64(0), int64(0), int64(0)
	mins, maxs := map[string]float64{}, map[string]float64{}
	rows, err := mainfeature.Read(path, func(f mainfeature.MainFeaturesV1) error {
		if previous != 0 && f.DecisionTimestampMs != previous+5000 {
			return fmt.Errorf("decision gap: previous=%d current=%d", previous, f.DecisionTimestampMs)
		}
		previous = f.DecisionTimestampMs
		if err := mainfeature.Validate(f); err != nil {
			invalid++
			return err
		}
		v, t := reflect.ValueOf(f), reflect.TypeOf(f)
		for i := 0; i < v.NumField(); i++ {
			if v.Field(i).Kind() != reflect.Float64 {
				continue
			}
			x, name := v.Field(i).Float(), t.Field(i).Name
			if math.IsNaN(x) {
				nanCount++
			}
			if math.IsInf(x, 0) {
				infCount++
			}
			if _, ok := mins[name]; !ok || x < mins[name] {
				mins[name] = x
			}
			if _, ok := maxs[name]; !ok || x > maxs[name] {
				maxs[name] = x
			}
			if strings.HasPrefix(name, "TakerImbalance") && (x < -1-1e-9 || x > 1+1e-9) {
				return fmt.Errorf("%s out of range: %g", name, x)
			}
			if strings.HasPrefix(name, "RangePosition") && (x < -1e-9 || x > 1+1e-9) {
				return fmt.Errorf("%s out of range: %g", name, x)
			}
			if strings.HasPrefix(name, "NoTradeRatio") && (x < 0 || x > 1) {
				return fmt.Errorf("%s out of range: %g", name, x)
			}
			if (strings.HasPrefix(name, "RV") || strings.HasPrefix(name, "BaseVolumeSum") || strings.HasPrefix(name, "QuoteVolumeSum") || strings.HasPrefix(name, "AggTradeCountSum")) && x < 0 {
				return fmt.Errorf("%s is negative: %g", name, x)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	fmt.Printf("Feature file: %s\nRows: %d\nNaN: %d\nInf: %d\nInvalid: %d\nTracked min/max columns: %d\nStatus: OK\n", path, rows, nanCount, infCount, invalid, len(mins))
	_ = maxs
	return nil
}
