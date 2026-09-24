package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"binance_trader/internal/data/dataset"
	"binance_trader/internal/data/secondbar"
	mainfeature "binance_trader/internal/feature/main"
	"binance_trader/internal/market"
)

type partition struct {
	month                          string
	writer                         *mainfeature.Writer
	rows, warmup, input, synthetic int64
	start, end                     int64
	started                        time.Time
}

func main() {
	if err := run(); err != nil {
		log.SetFlags(0)
		log.Fatal(err)
	}
}
func run() error {
	input := flag.String("input", ".\\data\\derived\\1s", "1-second dataset root")
	sourceManifests := flag.String("source-manifests", ".\\data\\manifests\\1s", "1-second manifest root")
	output := flag.String("output", ".\\data\\features\\main\\v1", "feature output root")
	featureManifests := flag.String("feature-manifests", ".\\data\\manifests\\features\\main\\v1", "feature manifest root")
	symbol := flag.String("symbol", "BTCUSDT", "symbol")
	month := flag.String("month", "", "single YYYY-MM month")
	start := flag.String("start", "2024-01", "start YYYY-MM")
	end := flag.String("end", "2025-12", "end YYYY-MM")
	force := flag.Bool("force", false, "replace existing feature files")
	flag.Parse()
	*symbol = strings.ToUpper(*symbol)
	if *month != "" {
		*start, *end = *month, *month
	}
	started := time.Now()
	startTime, err := time.Parse("2006-01", *start)
	if err != nil {
		return fmt.Errorf("invalid start: %w", err)
	}
	endTime, err := time.Parse("2006-01", *end)
	if err != nil || endTime.Before(startTime) {
		return fmt.Errorf("invalid end %q", *end)
	}
	endExclusive := endTime.AddDate(0, 1, 0)
	engine := mainfeature.NewEngine()
	parts := map[string]*partition{}
	order := []string{}
	syntheticByMonth := map[string]int64{}
	getPartition := func(m string, needWriter bool) (*partition, error) {
		p := parts[m]
		if p == nil {
			p = &partition{month: m, started: time.Now()}
			parts[m] = p
			order = append(order, m)
		}
		if needWriter && p.writer == nil {
			path := filepath.Join(*output, *symbol, m[:4], fmt.Sprintf("%s-main-features-v1-%s.parquet", *symbol, m))
			w, e := mainfeature.NewWriter(path, *force)
			if e != nil {
				return nil, e
			}
			p.writer = w
		}
		return p, nil
	}
	stats, err := dataset.Stream(dataset.Options{Root: *input, ManifestRoot: *sourceManifests, Symbol: *symbol, StartMonth: *start, EndMonth: *end, OnSynthetic: func(b market.SecondBar) { syntheticByMonth[time.UnixMilli(b.TimestampMs).UTC().Format("2006-01")]++ }}, func(bar market.SecondBar) error {
		barMonth := time.UnixMilli(bar.TimestampMs).UTC().Format("2006-01")
		p, e := getPartition(barMonth, false)
		if e != nil {
			return e
		}
		p.input++
		readyBefore := engine.Ready()
		f, e := engine.Add(bar)
		if e != nil {
			return e
		}
		if f == nil {
			if !readyBefore && (bar.TimestampMs+1000)%5000 == 0 {
				p.warmup++
			}
			return nil
		}
		decisionMonth := time.UnixMilli(f.DecisionTimestampMs).UTC().Format("2006-01")
		decisionTime := time.UnixMilli(f.DecisionTimestampMs).UTC()
		if decisionTime.Before(startTime) || !decisionTime.Before(endExclusive) {
			return nil
		}
		p, e = getPartition(decisionMonth, true)
		if e != nil {
			return e
		}
		if err := p.writer.Write(*f); err != nil {
			return err
		}
		p.rows++
		if p.start == 0 {
			p.start = f.DecisionTimestampMs
		}
		p.end = f.DecisionTimestampMs
		return nil
	})
	if err != nil {
		for _, p := range parts {
			if p.writer != nil {
				p.writer.Abort()
			}
		}
		return err
	}
	for _, m := range order {
		p := parts[m]
		if p.writer == nil {
			continue
		}
		if err := p.writer.Close(); err != nil {
			return err
		}
		path := filepath.Join(*output, *symbol, m[:4], fmt.Sprintf("%s-main-features-v1-%s.parquet", *symbol, m))
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		manifest := mainfeature.Manifest{Symbol: *symbol, Month: m, PartitionKey: "decision_timestamp_ms", PartitionTimezone: "UTC", FeatureVersion: mainfeature.FeatureVersion, SourceSchemaVersion: secondbar.SchemaVersion, EmitIntervalSeconds: 5, RowCount: p.rows, StartDecisionTimestampMs: p.start, EndDecisionTimestampMs: p.end, InputBars: p.input, SyntheticBars: syntheticByMonth[m], WarmupSkipped: p.warmup, OutputSizeBytes: info.Size(), ElapsedMs: time.Since(p.started).Milliseconds(), HistoryContextStart: startTime.UTC().Format(time.RFC3339), ContinuousState: true, GlobalWarmup: m == *start, BuildScope: *start + ".." + *end}
		mp := filepath.Join(*featureManifests, *symbol, fmt.Sprintf("%s-main-features-v1-%s.json", *symbol, m))
		if err := mainfeature.WriteManifest(mp, manifest); err != nil {
			return err
		}
		fmt.Printf("%s input=%d features=%d warmup_skipped=%d size=%d\n", m, p.input, p.rows, p.warmup, info.Size())
	}
	fmt.Printf("Continuous input: stored=%d synthetic=%d output=%d\nElapsed: %s\n", stats.StoredBars, stats.SyntheticBars, stats.OutputBars, time.Since(started).Round(time.Millisecond))
	return nil
}
