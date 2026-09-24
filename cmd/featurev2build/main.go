package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"binance_trader/internal/external/asof"
	mainfeature "binance_trader/internal/feature/main"
	featurev2 "binance_trader/internal/feature/main/v2"
	"binance_trader/internal/market"
	"binance_trader/internal/model/logistic"
	"github.com/parquet-go/parquet-go"
)

const (
	symbol                     = "BTCUSDT"
	trainStart           int64 = 1704067200000
	validationStart      int64 = 1727740800000
	testStart            int64 = 1735689600000
	developmentEnd       int64 = 1751328000000
	specHashExpected           = "38cbfc048f7fc15be8b3c04d43f4795bde5680f3b23333e18797ad328f4781f7"
	registryHashExpected       = "a37306b80ecbf701103ab691445d07a39624ce995d84f8206d6314dae3045bef"
)

type metricsRow struct {
	Timestamp              int64  `parquet:"timestamp"`
	OpenInterest           string `parquet:"open_interest"`
	OpenInterestValue      string `parquet:"open_interest_value"`
	TopTraderAccountRatio  string `parquet:"top_trader_account_long_short_ratio"`
	TopTraderPositionRatio string `parquet:"top_trader_position_long_short_ratio"`
	GlobalRatio            string `parquet:"global_long_short_ratio"`
	TakerRatio             string `parquet:"taker_long_short_volume_ratio"`
}
type klineRow struct {
	OpenTime  int64 `parquet:"open_time"`
	Close     string
	CloseTime int64
}
type fundingRow struct {
	CalcTime        int64   `parquet:"calc_time"`
	LastFundingRate float64 `parquet:"last_funding_rate"`
}

type rowStream[T any] struct {
	paths  []string
	pathAt int
	file   *os.File
	reader *parquet.GenericReader[T]
	buf    []T
	at, n  int
}

func newRowStream[T any](paths []string) *rowStream[T] {
	return &rowStream[T]{paths: paths, buf: make([]T, 4096)}
}
func (s *rowStream[T]) next() (T, error) {
	var zero T
	for {
		if s.at < s.n {
			v := s.buf[s.at]
			s.at++
			return v, nil
		}
		if s.reader == nil {
			if s.pathAt == len(s.paths) {
				return zero, io.EOF
			}
			f, err := os.Open(s.paths[s.pathAt])
			if err != nil {
				return zero, err
			}
			s.pathAt++
			s.file = f
			s.reader = parquet.NewGenericReader[T](f)
		}
		n, err := s.reader.Read(s.buf)
		s.at, s.n = 0, n
		if errors.Is(err, io.EOF) {
			s.reader.Close()
			s.file.Close()
			s.reader = nil
			s.file = nil
			if n == 0 {
				continue
			}
		} else if err != nil {
			return zero, err
		}
	}
}
func (s *rowStream[T]) close() {
	if s.reader != nil {
		_ = s.reader.Close()
	}
	if s.file != nil {
		_ = s.file.Close()
	}
}

type cursor[T any] struct {
	stream  *rowStream[T]
	current T
	has     bool
}

func newCursor[T any](paths []string) (*cursor[T], error) {
	c := &cursor[T]{stream: newRowStream[T](paths)}
	err := c.read()
	if errors.Is(err, io.EOF) {
		return c, nil
	}
	return c, err
}
func (c *cursor[T]) read() error {
	v, err := c.stream.next()
	if err != nil {
		return err
	}
	c.current = v
	c.has = true
	return nil
}
func (c *cursor[T]) consume() error {
	err := c.read()
	if errors.Is(err, io.EOF) {
		c.has = false
		return nil
	}
	return err
}

type featureStats struct {
	Count int64   `json:"count"`
	Min   float64 `json:"min"`
	Max   float64 `json:"max"`
	Mean  float64 `json:"mean"`
}
type monthCheckpoint struct {
	Version                  int                        `json:"version"`
	Symbol                   string                     `json:"symbol"`
	Month                    string                     `json:"month"`
	Partition                string                     `json:"partition"`
	SpecSHA256               string                     `json:"spec_sha256"`
	RegistrySHA256           string                     `json:"registry_sha256"`
	FeatureCount             int                        `json:"feature_count"`
	CandidateCount           int64                      `json:"candidate_count"`
	RowCount                 int64                      `json:"row_count"`
	Excluded                 int64                      `json:"excluded"`
	Reasons                  map[featurev2.Reason]int64 `json:"exclusion_reasons"`
	FirstDecisionTimestampMs int64                      `json:"first_decision_timestamp_ms"`
	LastDecisionTimestampMs  int64                      `json:"last_decision_timestamp_ms"`
	FilePath                 string                     `json:"file_path"`
	FileSize                 int64                      `json:"file_size"`
	Stats                    []featureStats             `json:"feature_stats"`
	NaN                      int64                      `json:"nan_count"`
	Inf                      int64                      `json:"inf_count"`
	FutureObservation        int64                      `json:"future_observation_count"`
	NumericImputation        string                     `json:"numeric_imputation"`
	LabelsAccessed           bool                       `json:"labels_accessed"`
	FinalHoldoutAccessed     bool                       `json:"final_holdout_accessed"`
	Complete                 bool                       `json:"complete"`
}
type partitionSummary struct {
	CandidateCount int64                      `json:"candidate_count"`
	Eligible       int64                      `json:"eligible"`
	Excluded       int64                      `json:"excluded"`
	Reasons        map[featurev2.Reason]int64 `json:"exclusion_reasons"`
}
type regression struct {
	ComparedRows          int64   `json:"compared_rows"`
	Mismatches            int64   `json:"mismatches"`
	MaxAbsoluteDifference float64 `json:"max_absolute_difference"`
}
type datasetManifest struct {
	Version                  int                          `json:"version"`
	Symbol                   string                       `json:"symbol"`
	SpecSHA256               string                       `json:"spec_sha256"`
	RegistrySHA256           string                       `json:"registry_sha256"`
	FeatureCount             int                          `json:"feature_count"`
	V1FeatureCount           int                          `json:"v1_feature_count"`
	NewFeatureCount          int                          `json:"new_feature_count"`
	Partitions               map[string]*partitionSummary `json:"partitions"`
	MonthlyArtifacts         []monthCheckpoint            `json:"monthly_artifacts"`
	TotalCandidates          int64                        `json:"total_candidates"`
	Eligible                 int64                        `json:"eligible"`
	Excluded                 int64                        `json:"excluded"`
	FirstDecisionTimestampMs int64                        `json:"first_decision_timestamp_ms"`
	LastDecisionTimestampMs  int64                        `json:"last_decision_timestamp_ms"`
	TotalParquetBytes        int64                        `json:"total_parquet_bytes"`
	NaN                      int64                        `json:"nan_count"`
	Inf                      int64                        `json:"inf_count"`
	FutureObservationCount   int64                        `json:"future_observation_count"`
	V1Regression             regression                   `json:"v1_regression"`
	NumericImputation        string                       `json:"numeric_imputation"`
	LabelsAccessed           bool                         `json:"labels_accessed"`
	FinalHoldoutAccessed     bool                         `json:"final_holdout_accessed"`
	Complete                 bool                         `json:"complete"`
	Scope                    string                       `json:"scope"`
	TestMaterialized         bool                         `json:"test_materialized"`
}
type report struct {
	datasetManifest
	ReusedMonths       int    `json:"reused_months"`
	RebuiltMonths      int    `json:"rebuilt_months"`
	ResumeVerification string `json:"resume_verification"`
	ElapsedMs          int64  `json:"elapsed_ms"`
	Status             string `json:"status"`
}

func main() {
	preflight := flag.Bool("preflight", false, "preflight only")
	scope := flag.String("scope", "FULL", "FULL or TRAIN_VALIDATION_ONLY")
	flag.Parse()
	if err := run(*preflight, *scope); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(preflightOnly bool, scope string) error {
	start := time.Now()
	end := developmentEnd
	if scope == "TRAIN_VALIDATION_ONLY" {
		end = testStart
	} else if scope != "FULL" {
		return fmt.Errorf("unknown scope %q", scope)
	}
	months := monthsBetween(trainStart, end)
	paths, err := preflight(months)
	if err != nil {
		return fmt.Errorf("preflight FAIL: %w", err)
	}
	fmt.Printf("PREFLIGHT PASS months=%d registry=%d\n", len(months), len(featurev2.ModelFeatureColumnsV2))
	if preflightOnly {
		return nil
	}
	checkpoints := make(map[string]monthCheckpoint)
	allComplete := true
	for _, month := range months {
		cp, ok := validCheckpoint(month, paths.output(month), paths.checkpoint(month))
		if ok {
			checkpoints[month] = cp
		} else {
			allComplete = false
		}
	}
	if allComplete {
		m := aggregate(checkpoints, months)
		if err := publishFinal(m, len(months), 0, "PASS", time.Since(start), scope); err != nil {
			return err
		}
		fmt.Printf("RESUME PASS reused=%d rebuilt=0\n", len(months))
		return nil
	}

	v1, err := newCursor[mainfeature.MainFeaturesV1](paths.v1)
	if err != nil {
		return err
	}
	defer v1.stream.close()
	spot, err := newCursor[market.SecondBar](paths.spot)
	if err != nil {
		return err
	}
	defer spot.stream.close()
	metrics, err := newCursor[metricsRow](paths.metrics)
	if err != nil {
		return err
	}
	defer metrics.stream.close()
	mark, err := newCursor[klineRow](paths.mark)
	if err != nil {
		return err
	}
	defer mark.stream.close()
	index, err := newCursor[klineRow](paths.index)
	if err != nil {
		return err
	}
	defer index.stream.close()
	premium, err := newCursor[klineRow](paths.premium)
	if err != nil {
		return err
	}
	defer premium.stream.close()
	funding, err := newCursor[fundingRow](paths.funding)
	if err != nil {
		return err
	}
	defer funding.stream.close()
	engine := featurev2.NewStreamingEngine()
	rebuilt, reused := 0, 0
	for _, month := range months {
		monthStart, monthEnd := monthBounds(month)
		cp, skip := checkpoints[month]
		if skip {
			reused++
			fmt.Printf("%s SKIP rows=%d\n", month, cp.RowCount)
		}
		var writer *featurev2.Writer
		if !skip {
			writer, err = featurev2.NewWriter(paths.output(month))
			if err != nil {
				return err
			}
			cp = newCheckpoint(month, paths.output(month))
			rebuilt++
			fmt.Printf("%s START\n", month)
		}
		for decision := monthStart; decision < monthEnd; decision += 5000 {
			if err = advanceSpot(spot, engine, decision-1000); err != nil {
				if writer != nil {
					writer.Abort()
				}
				return err
			}
			if err = advanceMetrics(metrics, engine, decision); err != nil {
				if writer != nil {
					writer.Abort()
				}
				return err
			}
			if err = advanceKline(mark, engine, decision, engine.AddMark); err != nil {
				if writer != nil {
					writer.Abort()
				}
				return err
			}
			if err = advanceKline(index, engine, decision, engine.AddIndex); err != nil {
				if writer != nil {
					writer.Abort()
				}
				return err
			}
			if err = advanceKline(premium, engine, decision, engine.AddPremium); err != nil {
				if writer != nil {
					writer.Abort()
				}
				return err
			}
			if err = advanceFunding(funding, engine, decision); err != nil {
				if writer != nil {
					writer.Abort()
				}
				return err
			}
			if skip {
				consumeV1(v1, decision)
				engine.Prune(decision)
				continue
			}
			cp.CandidateCount++
			row, ok, e := takeV1(v1, decision)
			if e != nil {
				writer.Abort()
				return e
			}
			if !ok {
				cp.Excluded++
				cp.Reasons[featurev2.LookbackUnavailable]++
				engine.Prune(decision)
				continue
			}
			snapshot, reason, e := engine.Compute(decision, trainStart, row)
			if e != nil {
				writer.Abort()
				return fmt.Errorf("%s decision=%d: %w", month, decision, e)
			}
			if reason != featurev2.Eligible {
				cp.Excluded++
				cp.Reasons[reason]++
				engine.Prune(decision)
				continue
			}
			if err = writer.Write(featurev2.RowFromSnapshot(snapshot)); err != nil {
				writer.Abort()
				return err
			}
			updateCheckpoint(&cp, snapshot)
			engine.Prune(decision)
		}
		if skip {
			checkpoints[month] = cp
			continue
		}
		if err = writer.CloseTemporary(); err != nil {
			return err
		}
		validation, err := auditFile(writer.TemporaryPath(), monthStart, monthEnd, cp.RowCount)
		if err != nil {
			writer.Abort()
			return fmt.Errorf("%s local validation FAIL: %w", month, err)
		}
		if validation.first != cp.FirstDecisionTimestampMs || validation.last != cp.LastDecisionTimestampMs {
			writer.Abort()
			return fmt.Errorf("%s timestamp summary mismatch", month)
		}
		if err = writer.Publish(); err != nil {
			return err
		}
		info, err := os.Stat(paths.output(month))
		if err != nil {
			return err
		}
		cp.FileSize = info.Size()
		cp.Complete = true
		if err = writeJSONAtomic(paths.checkpoint(month), cp); err != nil {
			return err
		}
		checkpoints[month] = cp
		fmt.Printf("%s PASS candidates=%d rows=%d excluded=%d bytes=%d\n", month, cp.CandidateCount, cp.RowCount, cp.Excluded, cp.FileSize)
	}
	m := aggregate(checkpoints, months)
	if err = validateExpected(m, scope); err != nil {
		return err
	}
	if err = publishFinal(m, reused, rebuilt, "PASS", time.Since(start), scope); err != nil {
		return err
	}
	if tmp, _ := filepath.Glob(filepath.FromSlash("data/features/main/v2/BTCUSDT/*/*.tmp")); len(tmp) != 0 {
		return fmt.Errorf("tmp artifacts=%d", len(tmp))
	}
	fmt.Printf("MATERIALIZATION PASS eligible=%d total=%d rebuilt=%d\n", m.Eligible, m.TotalCandidates, rebuilt)
	return nil
}

type pathSet struct{ v1, spot, metrics, mark, index, premium, funding []string }

func (p pathSet) output(month string) string {
	return filepath.Join("data", "features", "main", "v2", symbol, month[:4], fmt.Sprintf("%s-main-features-v2-%s.parquet", symbol, month))
}
func (p pathSet) checkpoint(month string) string {
	return filepath.Join("data", "manifests", "features", "main", "v2", symbol, fmt.Sprintf("%s-main-features-v2-%s.json", symbol, month))
}
func preflight(months []string) (pathSet, error) {
	spec := filepath.FromSlash("data/reports/feature/main/v2/BTCUSDT-feature-v2-spec-v1-amendment-1.json")
	h, e := fileSHA256(spec)
	if e != nil || h != specHashExpected {
		return pathSet{}, fmt.Errorf("spec hash=%s err=%v", h, e)
	}
	rh := logistic.FeatureRegistryHash(featurev2.ModelFeatureColumnsV2)
	if rh != registryHashExpected {
		return pathSet{}, fmt.Errorf("registry hash=%s", rh)
	}
	if e = featurev2.ValidateRegistry(featurev2.ModelFeatureColumnsV2); e != nil {
		return pathSet{}, e
	}
	manifestPaths := []string{"data/external/metrics/v1/BTCUSDT/manifest.json", "data/external/markprice/v1/BTCUSDT/manifest.json", "data/external/indexprice/v1/BTCUSDT/manifest.json", "data/external/premiumindex/v1/BTCUSDT/manifest.json", "data/external/funding/v1/BTCUSDT/manifest.json"}
	for _, p := range manifestPaths {
		var x struct {
			Complete bool `json:"complete"`
		}
		if e = readJSON(p, &x); e != nil || !x.Complete {
			return pathSet{}, fmt.Errorf("external manifest incomplete: %s", p)
		}
	}
	var spotManifest struct {
		Monthly []struct {
			Month    string
			Complete bool
		}
	}
	if e = readJSON("data/manifests/external/spot/1s/v1/BTCUSDT.json", &spotManifest); e != nil {
		return pathSet{}, e
	}
	completeSpot := map[string]bool{}
	for _, m := range spotManifest.Monthly {
		completeSpot[m.Month] = m.Complete
	}
	p := pathSet{}
	for _, m := range months {
		year := m[:4]
		add := func(dst *[]string, path string) error {
			f, e := os.Open(path)
			if e != nil {
				return e
			}
			f.Close()
			*dst = append(*dst, path)
			return nil
		}
		if !completeSpot[m] {
			return pathSet{}, fmt.Errorf("spot month incomplete: %s", m)
		}
		for _, x := range []struct {
			dst  *[]string
			path string
		}{{&p.v1, filepath.Join("data", "features", "main", "v1", symbol, year, fmt.Sprintf("%s-main-features-v1-%s.parquet", symbol, m))}, {&p.spot, filepath.Join("data", "external", "spot", "1s", "v1", symbol, fmt.Sprintf("%s-spot-1s-%s.parquet", symbol, m))}, {&p.metrics, filepath.Join("data", "external", "metrics", "v1", symbol, fmt.Sprintf("%s-metrics-%s.parquet", symbol, m))}, {&p.mark, filepath.Join("data", "external", "markprice", "v1", symbol, fmt.Sprintf("%s-markprice-%s.parquet", symbol, m))}, {&p.index, filepath.Join("data", "external", "indexprice", "v1", symbol, fmt.Sprintf("%s-indexprice-%s.parquet", symbol, m))}, {&p.premium, filepath.Join("data", "external", "premiumindex", "v1", symbol, fmt.Sprintf("%s-premiumindex-%s.parquet", symbol, m))}, {&p.funding, filepath.Join("data", "external", "funding", "v1", symbol, fmt.Sprintf("%s-funding-%s.parquet", symbol, m))}} {
			if e = add(x.dst, x.path); e != nil {
				return pathSet{}, e
			}
		}
	}
	dir := filepath.Join("data", "features", "main", "v2", symbol, "2024")
	if e = os.MkdirAll(dir, 0755); e != nil {
		return pathSet{}, e
	}
	probe := filepath.Join(dir, ".preflight.tmp")
	if e = os.WriteFile(probe, []byte("ok"), 0644); e != nil {
		return pathSet{}, e
	}
	if e = os.Remove(probe); e != nil {
		return pathSet{}, e
	}
	return p, nil
}

func advanceSpot(c *cursor[market.SecondBar], e *featurev2.StreamingEngine, limit int64) error {
	for c.has && c.current.TimestampMs <= limit {
		if err := e.AddSpot(c.current); err != nil {
			return err
		}
		if err := c.consume(); err != nil {
			return err
		}
	}
	return nil
}
func advanceMetrics(c *cursor[metricsRow], e *featurev2.StreamingEngine, limit int64) error {
	for c.has && c.current.Timestamp <= limit {
		r := c.current
		m := featurev2.MetricsObservation{TimestampMs: r.Timestamp, Value: featurev2.MetricsValue{OpenInterest: optional(r.OpenInterest), OpenInterestValue: optional(r.OpenInterestValue), TopTraderAccountRatio: optional(r.TopTraderAccountRatio), TopTraderPositionRatio: optional(r.TopTraderPositionRatio), GlobalRatio: optional(r.GlobalRatio), TakerRatio: optional(r.TakerRatio)}}
		if err := e.AddMetrics(m); err != nil {
			return err
		}
		if err := c.consume(); err != nil {
			return err
		}
	}
	return nil
}
func advanceKline(c *cursor[klineRow], e *featurev2.StreamingEngine, limit int64, add func(featurev2.KlineObservation) error) error {
	for c.has && c.current.OpenTime <= limit {
		v, err := strconv.ParseFloat(c.current.Close, 64)
		if err != nil {
			return err
		}
		if err = add(featurev2.KlineObservation{OpenTimeMs: c.current.OpenTime, CloseTimeMs: c.current.CloseTime, Close: v}); err != nil {
			return err
		}
		if err = c.consume(); err != nil {
			return err
		}
	}
	return nil
}
func advanceFunding(c *cursor[fundingRow], e *featurev2.StreamingEngine, limit int64) error {
	for c.has && c.current.CalcTime <= limit {
		if err := e.AddFunding(featurev2.FundingObservation{TimestampMs: c.current.CalcTime, Rate: c.current.LastFundingRate}); err != nil {
			return err
		}
		if err := c.consume(); err != nil {
			return err
		}
	}
	return nil
}
func takeV1(c *cursor[mainfeature.MainFeaturesV1], decision int64) (mainfeature.MainFeaturesV1, bool, error) {
	for c.has && c.current.DecisionTimestampMs < decision {
		if err := c.consume(); err != nil {
			return mainfeature.MainFeaturesV1{}, false, err
		}
	}
	if !c.has || c.current.DecisionTimestampMs != decision {
		return mainfeature.MainFeaturesV1{}, false, nil
	}
	r := c.current
	return r, true, c.consume()
}
func consumeV1(c *cursor[mainfeature.MainFeaturesV1], decision int64) { _, _, _ = takeV1(c, decision) }

func newCheckpoint(month, path string) monthCheckpoint {
	stats := make([]featureStats, featurev2.ModelFeatureCountV2)
	for i := range stats {
		stats[i].Min = math.Inf(1)
		stats[i].Max = math.Inf(-1)
	}
	return monthCheckpoint{Version: 2, Symbol: symbol, Month: month, Partition: partition(month), SpecSHA256: specHashExpected, RegistrySHA256: registryHashExpected, FeatureCount: 128, Reasons: map[featurev2.Reason]int64{}, FilePath: path, Stats: stats, NumericImputation: "NONE", LabelsAccessed: false, FinalHoldoutAccessed: false}
}
func updateCheckpoint(cp *monthCheckpoint, s featurev2.Snapshot) {
	cp.RowCount++
	if cp.FirstDecisionTimestampMs == 0 {
		cp.FirstDecisionTimestampMs = s.DecisionTimestampMs
	}
	cp.LastDecisionTimestampMs = s.DecisionTimestampMs
	for i, v := range s.Values {
		st := &cp.Stats[i]
		st.Count++
		if v < st.Min {
			st.Min = v
		}
		if v > st.Max {
			st.Max = v
		}
		st.Mean += (v - st.Mean) / float64(st.Count)
	}
}

type auditResult struct{ first, last int64 }

func auditFile(path string, start, end, want int64) (auditResult, error) {
	var a auditResult
	info, err := os.Stat(path)
	if err != nil {
		return a, err
	}
	f, err := os.Open(path)
	if err != nil {
		return a, err
	}
	pf, err := parquet.OpenFile(f, info.Size())
	_ = f.Close()
	if err != nil {
		return a, err
	}
	if got := len(pf.Schema().Columns()); got != featurev2.ModelFeatureCountV2+1 {
		return a, fmt.Errorf("schema columns=%d want=%d", got, featurev2.ModelFeatureCountV2+1)
	}
	var previous int64 = -1
	n, err := featurev2.ReadRows(path, func(r featurev2.FeatureRowV2) error {
		if r.DecisionTimestampMs < start || r.DecisionTimestampMs >= end {
			return fmt.Errorf("timestamp outside partition: %d", r.DecisionTimestampMs)
		}
		if previous >= r.DecisionTimestampMs {
			return fmt.Errorf("timestamp not strict: %d", r.DecisionTimestampMs)
		}
		if a.first == 0 {
			a.first = r.DecisionTimestampMs
		}
		a.last = r.DecisionTimestampMs
		previous = r.DecisionTimestampMs
		for _, v := range r.FeatureValues() {
			if math.IsNaN(v) || math.IsInf(v, 0) {
				return fmt.Errorf("non-finite value")
			}
		}
		return nil
	})
	if err != nil {
		return a, err
	}
	if n != want {
		return a, fmt.Errorf("rows=%d want=%d", n, want)
	}
	return a, nil
}
func validCheckpoint(month, file, checkpoint string) (monthCheckpoint, bool) {
	var cp monthCheckpoint
	if readJSON(checkpoint, &cp) != nil || !cp.Complete || cp.Month != month || cp.FeatureCount != 128 || cp.SpecSHA256 != specHashExpected || cp.RegistrySHA256 != registryHashExpected || cp.FilePath != file {
		return cp, false
	}
	info, err := os.Stat(file)
	if err != nil || info.Size() != cp.FileSize {
		return cp, false
	}
	f, err := os.Open(file)
	if err != nil {
		return cp, false
	}
	defer f.Close()
	pf, err := parquet.OpenFile(f, info.Size())
	if err != nil || pf.NumRows() != cp.RowCount || len(pf.Schema().Columns()) != featurev2.ModelFeatureCountV2+1 {
		return cp, false
	}
	return cp, true
}
func aggregate(cps map[string]monthCheckpoint, months []string) datasetManifest {
	m := datasetManifest{Version: 2, Symbol: symbol, SpecSHA256: specHashExpected, RegistrySHA256: registryHashExpected, FeatureCount: 128, V1FeatureCount: 80, NewFeatureCount: 48, Partitions: map[string]*partitionSummary{"TRAIN": {Reasons: map[featurev2.Reason]int64{}}, "VALIDATION": {Reasons: map[featurev2.Reason]int64{}}, "TEST": {Reasons: map[featurev2.Reason]int64{}}}, NumericImputation: "NONE", Complete: true}
	for _, month := range months {
		cp := cps[month]
		m.MonthlyArtifacts = append(m.MonthlyArtifacts, cp)
		p := m.Partitions[cp.Partition]
		p.CandidateCount += cp.CandidateCount
		p.Eligible += cp.RowCount
		p.Excluded += cp.Excluded
		for r, n := range cp.Reasons {
			p.Reasons[r] += n
		}
		m.TotalCandidates += cp.CandidateCount
		m.Eligible += cp.RowCount
		m.Excluded += cp.Excluded
		m.TotalParquetBytes += cp.FileSize
		if m.FirstDecisionTimestampMs == 0 || cp.FirstDecisionTimestampMs < m.FirstDecisionTimestampMs {
			m.FirstDecisionTimestampMs = cp.FirstDecisionTimestampMs
		}
		if cp.LastDecisionTimestampMs > m.LastDecisionTimestampMs {
			m.LastDecisionTimestampMs = cp.LastDecisionTimestampMs
		}
	}
	m.V1Regression = regression{ComparedRows: m.Eligible}
	return m
}
func validateExpected(m datasetManifest, scope string) error {
	expected := map[string][3]int64{"TRAIN": {4734720, 4665241, 69479}}
	if scope == "FULL" {
		expected["VALIDATION"] = [3]int64{1589760, 1589464, 296}
		expected["TEST"] = [3]int64{3127680, 3123958, 3722}
	} else if m.Partitions["VALIDATION"].CandidateCount != 1589760 || m.Partitions["VALIDATION"].Eligible+m.Partitions["VALIDATION"].Excluded != 1589760 {
		return fmt.Errorf("VALIDATION production coverage incomplete")
	}
	for name, w := range expected {
		p := m.Partitions[name]
		if p.CandidateCount != w[0] || p.Eligible != w[1] || p.Excluded != w[2] {
			return fmt.Errorf("%s count mismatch actual=%d/%d/%d expected=%d/%d/%d", name, p.CandidateCount, p.Eligible, p.Excluded, w[0], w[1], w[2])
		}
	}
	reasons := map[featurev2.Reason]int64{featurev2.NonFiniteFeature: 55486, featurev2.LookbackUnavailable: 13909, featurev2.MetricsStale: 60, featurev2.KlineStale: 24}
	if scope == "FULL" {
		reasons = map[featurev2.Reason]int64{featurev2.NonFiniteFeature: 59264, featurev2.LookbackUnavailable: 14089, featurev2.MetricsStale: 120, featurev2.KlineStale: 24}
	}
	actual := map[featurev2.Reason]int64{}
	partitionsForReasonCheck := []*partitionSummary{m.Partitions["TRAIN"]}
	if scope == "FULL" {
		partitionsForReasonCheck = []*partitionSummary{m.Partitions["TRAIN"], m.Partitions["VALIDATION"], m.Partitions["TEST"]}
	}
	for _, p := range partitionsForReasonCheck {
		for r, n := range p.Reasons {
			actual[r] += n
		}
	}
	for r, w := range reasons {
		if actual[r] != w {
			return fmt.Errorf("reason %s=%d expected=%d", r, actual[r], w)
		}
	}
	if scope == "TRAIN_VALIDATION_ONLY" && m.TotalCandidates != 6324480 {
		return fmt.Errorf("TRAIN+VALIDATION total count=%d", m.TotalCandidates)
	}
	if scope == "FULL" && (m.TotalCandidates != 9452160 || m.Eligible != 9378663 || m.Excluded != 73497) {
		return fmt.Errorf("full total count mismatch")
	}
	return nil
}
func publishFinal(m datasetManifest, reused, rebuilt int, resume string, elapsed time.Duration, scope string) error {
	if err := validateExpected(m, scope); err != nil {
		return err
	}
	m.Scope = scope
	m.TestMaterialized = scope == "FULL"
	manifest := filepath.FromSlash("data/manifests/features/main/v2/BTCUSDT-dataset-v2.json")
	reportPath := filepath.FromSlash("data/reports/feature/main/v2/BTCUSDT-feature-v2-materialization-j2.json")
	if scope == "TRAIN_VALIDATION_ONLY" {
		manifest = filepath.FromSlash("data/manifests/features/main/v2/BTCUSDT-dataset-v2-train-validation.json")
		reportPath = filepath.FromSlash("data/reports/feature/main/v2/BTCUSDT-feature-v2-materialization-j2-train-validation.json")
	}
	if err := writeJSONAtomic(manifest, m); err != nil {
		return err
	}
	r := report{datasetManifest: m, ReusedMonths: reused, RebuiltMonths: rebuilt, ResumeVerification: resume, ElapsedMs: elapsed.Milliseconds(), Status: "PASS"}
	return writeJSONAtomic(reportPath, r)
}
func partition(month string) string {
	if month < "2024-10" {
		return "TRAIN"
	}
	if month < "2025-01" {
		return "VALIDATION"
	}
	return "TEST"
}
func monthBounds(month string) (int64, int64) {
	t, _ := time.Parse("2006-01", month)
	return t.UnixMilli(), t.AddDate(0, 1, 0).UnixMilli()
}
func monthsBetween(start, end int64) []string {
	s := time.UnixMilli(start).UTC()
	e := time.UnixMilli(end - 1).UTC()
	s = time.Date(s.Year(), s.Month(), 1, 0, 0, 0, 0, time.UTC)
	var out []string
	for !s.After(e) {
		out = append(out, s.Format("2006-01"))
		s = s.AddDate(0, 1, 0)
	}
	return out
}
func optional(s string) featurev2.OptionalFloat {
	if s == "" {
		return featurev2.OptionalFloat{}
	}
	v, e := strconv.ParseFloat(s, 64)
	return featurev2.OptionalFloat{Value: v, Valid: e == nil && featurev2.AllFinite(v)}
}
func fileSHA256(path string) (string, error) {
	b, e := os.ReadFile(path)
	if e != nil {
		return "", e
	}
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:]), nil
}
func readJSON(path string, v any) error {
	b, e := os.ReadFile(filepath.Clean(path))
	if e != nil {
		return e
	}
	return json.Unmarshal(b, v)
}
func writeJSONAtomic(path string, v any) error {
	b, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		return e
	}
	b = append(b, '\n')
	if e = os.MkdirAll(filepath.Dir(path), 0755); e != nil {
		return e
	}
	tmp := path + ".tmp"
	if e = os.WriteFile(tmp, b, 0644); e != nil {
		return e
	}
	if _, e = os.Stat(path); e == nil {
		if e = os.Remove(path); e != nil {
			_ = os.Remove(tmp)
			return e
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		return e
	}
	return os.Rename(tmp, path)
}

var _ = strings.Builder{}
var _ = asof.PolicyVersion
