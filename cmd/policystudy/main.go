package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"path/filepath"
	"time"

	calibration "binance_trader/internal/model/calibration"
	logistic "binance_trader/internal/model/logistic"
	policy "binance_trader/internal/model/policy"
	mainsplit "binance_trader/internal/split/main"
	maintraining "binance_trader/internal/training/main"
)

type sideAnalysis struct {
	Bins       []policy.Candidate `json:"equal_width_probability_bins"`
	Candidates []policy.Candidate `json:"candidate_table"`
	Selected   policy.SidePolicy  `json:"selected_policy"`
}
type sideConfirmation struct {
	Baseline   policy.OutcomeStats    `json:"all_valid_baseline"`
	Frozen     policy.OutcomeStats    `json:"frozen_threshold"`
	NonOverlap policy.NonOverlapStats `json:"non_overlap"`
	Daily      policy.DailyStats      `json:"daily"`
}
type combinedReport struct {
	LongDecisions    int64   `json:"long_decisions"`
	ShortDecisions   int64   `json:"short_decisions"`
	NoTradeDecisions int64   `json:"no_trade_decisions"`
	DecisionCoverage float64 `json:"decision_coverage"`
	Valid            int64   `json:"valid_chosen_outcomes"`
	Invalid          int64   `json:"invalid_chosen_outcomes"`
	Profitable       int64   `json:"profitable_chosen_outcomes"`
	InvalidRate      float64 `json:"invalid_chosen_outcome_rate"`
	WinRate          float64 `json:"win_rate"`
	MeanNet          float64 `json:"mean_net_return_ex_funding"`
	MedianNet        float64 `json:"median_net_return_ex_funding"`
}
type report struct {
	PolicyVersion                 int              `json:"policy_version"`
	Symbol                        string           `json:"symbol"`
	Artifact                      policy.Artifact  `json:"frozen_policy"`
	SelectionRawRows              int64            `json:"selection_raw_rows"`
	SelectionIncludedRows         int64            `json:"selection_included_rows"`
	SelectionPurgedRows           int64            `json:"selection_purged_rows"`
	ConfirmationRows              int64            `json:"confirmation_rows"`
	FinalHoldoutAccessed          bool             `json:"final_holdout_accessed"`
	ConfirmationUsedForSelection  bool             `json:"policy_confirmation_used_for_selection"`
	LongSelection                 sideAnalysis     `json:"long_selection"`
	ShortSelection                sideAnalysis     `json:"short_selection"`
	LongConfirmation              sideConfirmation `json:"long_confirmation"`
	ShortConfirmation             sideConfirmation `json:"short_confirmation"`
	CombinedConfirmation          combinedReport   `json:"combined_confirmation"`
	PredictionDistributionStorage string           `json:"performance_note"`
	ElapsedMs                     int64            `json:"elapsed_ms"`
}
type predictor struct {
	model     logistic.Artifact
	cal       calibration.Artifact
	modelHash string
	scaled    []float64
}

func (p *predictor) predict(features []float64) (float64, error) {
	if err := p.model.Scaler.TransformInto(p.scaled, features); err != nil {
		return 0, err
	}
	z := p.model.Intercept
	for i := range p.scaled {
		z += p.model.Weights[i] * p.scaled[i]
	}
	return p.cal.Calibrate(logistic.Sigmoid(z), p.modelHash)
}

func main() {
	log.SetFlags(0)
	if e := run(); e != nil {
		log.Fatal(e)
	}
}
func run() error {
	trainingRoot := flag.String("training-root", `.\data\training\main\v1\tp50_sl25_h3600\cost_synthetic_validation_v1\BTCUSDT`, "training root")
	splitPath := flag.String("split-manifest", `.\data\manifests\splits\main\v1\BTCUSDT-split-v1.json`, "split manifest")
	logRoot := flag.String("logistic-root", `.\models\main\logistic\v1\tp50_sl25_h3600\cost_synthetic_validation_v1\BTCUSDT`, "logistic artifacts")
	calRoot := flag.String("calibration-root", `.\models\main\calibration\v1\tp50_sl25_h3600\cost_synthetic_validation_v1\BTCUSDT`, "calibration artifacts")
	artifactPath := flag.String("artifact", `.\models\main\policy\v1\tp50_sl25_h3600\cost_synthetic_validation_v1\BTCUSDT\policy.json`, "policy artifact")
	reportPath := flag.String("report", `.\data\reports\models\policy\main\v1\BTCUSDT-policy-v1.json`, "report")
	flag.Parse()
	started := time.Now()
	sm, e := mainsplit.ReadManifest(*splitPath)
	if e != nil {
		return e
	}
	if !sm.FinalHoldoutSealed {
		return fmt.Errorf("final holdout is not sealed")
	}
	if sm.Definition.EmbargoMs != 0 {
		return fmt.Errorf("policy v1 requires embargo_ms=0")
	}
	lp, e := loadPredictor(filepath.Join(*logRoot, "long.json"), filepath.Join(*calRoot, "long.json"))
	if e != nil {
		return e
	}
	sp, e := loadPredictor(filepath.Join(*logRoot, "short.json"), filepath.Join(*calRoot, "short.json"))
	if e != nil {
		return e
	}
	lch, _ := policy.FileSHA256(filepath.Join(*calRoot, "long.json"))
	sch, _ := policy.FileSHA256(filepath.Join(*calRoot, "short.json"))
	selection := policy.Range{StartMs: policy.UTC("2025-01-01T00:00:00Z"), EndMs: policy.UTC("2025-04-01T00:00:00Z")}
	confirmation := policy.Range{StartMs: selection.EndMs, EndMs: policy.UTC("2025-07-01T00:00:00Z")}
	coverages := []float64{.50, .25, .10, .05, .02, .01, .005}
	longObs := make([]policy.Observation, 0, 1555000)
	shortObs := make([]policy.Observation, 0, 1555000)
	var raw, purged int64
	features := make([]float64, 0, len(maintraining.ModelFeatureColumns))
	for _, path := range files(*trainingRoot, 2025, 1, 3) {
		_, e = maintraining.Read(path, func(row maintraining.TrainingRowV1) error {
			raw++
			if !policy.IncludedInSelection(row.DecisionTimestampMs, selection, sm.Definition.MaxLabelDependencyMs) {
				purged++
				return nil
			}
			features = mainsplit.ModelFeaturesInto(features, row)
			pl, e := lp.predict(features)
			if e != nil {
				return e
			}
			ps, e := sp.predict(features)
			if e != nil {
				return e
			}
			longObs = append(longObs, observe(row, true, pl))
			shortObs = append(shortObs, observe(row, false, ps))
			return nil
		})
		if e != nil {
			return e
		}
	}
	longCandidates := policy.BuildCandidates(longObs, coverages, 5000, 30)
	shortCandidates := policy.BuildCandidates(shortObs, coverages, 5000, 30)
	longSelected := policy.Select(longCandidates)
	shortSelected := policy.Select(shortCandidates)
	a := policy.Artifact{PolicyVersion: 1, Symbol: "BTCUSDT", Source: policy.Source{LogisticVersion: 1, CalibrationVersion: 1, LongLogisticSHA256: lp.modelHash, ShortLogisticSHA256: sp.modelHash, LongCalibrationSHA256: lch, ShortCalibrationSHA256: sch}, SplitVersion: sm.SplitVersion, Selection: selection, Confirmation: confirmation, MaxLabelDependencyMs: sm.Definition.MaxLabelDependencyMs, EmbargoMs: 0, PurgeRule: "exclude selection row when decision_timestamp_ms + max_label_dependency_ms >= policy_confirmation_start_ms", CandidateCoverages: coverages, MinValidSignalCount: 5000, MinActiveDays: 30, Long: longSelected, Short: shortSelected, ConflictRule: "qualify with p >= threshold; if both qualify choose larger probability-minus-threshold margin; exact margin tie is NO_TRADE; disabled sides never qualify"}
	// The complete policy is persisted before any confirmation file is opened.
	if e = policy.WriteArtifact(*artifactPath, a); e != nil {
		return e
	}
	r := report{PolicyVersion: 1, Symbol: "BTCUSDT", Artifact: a, SelectionRawRows: raw, SelectionIncludedRows: int64(len(longObs)), SelectionPurgedRows: purged, FinalHoldoutAccessed: false, ConfirmationUsedForSelection: false, PredictionDistributionStorage: "streamed features; retained only two calibrated probability/outcome vectors for selection quantiles and exact medians", LongSelection: sideAnalysis{bins(longObs), longCandidates, longSelected}, ShortSelection: sideAnalysis{bins(shortObs), shortCandidates, shortSelected}}
	confLong := make([]policy.Observation, 0, 1572000)
	confShort := make([]policy.Observation, 0, 1572000)
	decisions := make([]policy.Decision, 0, 1572000)
	for _, path := range files(*trainingRoot, 2025, 4, 6) {
		_, e = maintraining.Read(path, func(row maintraining.TrainingRowV1) error {
			as := sm.Definition.Classify(row.DecisionTimestampMs)
			if as.Partition != mainsplit.Test || !as.Included {
				return nil
			}
			features = mainsplit.ModelFeaturesInto(features, row)
			pl, e := lp.predict(features)
			if e != nil {
				return e
			}
			ps, e := sp.predict(features)
			if e != nil {
				return e
			}
			lo, so := observe(row, true, pl), observe(row, false, ps)
			confLong = append(confLong, lo)
			confShort = append(confShort, so)
			d, e := a.Decide(pl, ps)
			if e != nil {
				return e
			}
			decisions = append(decisions, d)
			return nil
		})
		if e != nil {
			return e
		}
	}
	r.ConfirmationRows = int64(len(confLong))
	r.LongConfirmation = confirm(confLong, a.Long)
	r.ShortConfirmation = confirm(confShort, a.Short)
	r.CombinedConfirmation = combined(decisions, confLong, confShort)
	r.ElapsedMs = time.Since(started).Milliseconds()
	if e = policy.WriteJSON(*reportPath, r); e != nil {
		return e
	}
	fmt.Printf("selection included=%d purged=%d long=%v %.12g short=%v %.12g\n", len(longObs), purged, a.Long.Enabled, a.Long.Threshold, a.Short.Enabled, a.Short.Threshold)
	fmt.Printf("confirmation rows=%d LONG=%d SHORT=%d NO_TRADE=%d mean_net=%.12g holdout_accessed=false elapsed=%s\n", r.ConfirmationRows, r.CombinedConfirmation.LongDecisions, r.CombinedConfirmation.ShortDecisions, r.CombinedConfirmation.NoTradeDecisions, r.CombinedConfirmation.MeanNet, time.Since(started).Round(time.Millisecond))
	return nil
}
func loadPredictor(mp, cp string) (predictor, error) {
	m, e := logistic.ReadArtifact(mp)
	if e != nil {
		return predictor{}, e
	}
	if e = m.ValidateRegistry(maintraining.ModelFeatureColumns); e != nil {
		return predictor{}, e
	}
	mh, e := policy.FileSHA256(mp)
	if e != nil {
		return predictor{}, e
	}
	c, e := calibration.ReadArtifact(cp)
	if e != nil {
		return predictor{}, e
	}
	if c.SourceModelSHA256 != mh || c.FeatureRegistryHash != m.FeatureRegistryHash {
		return predictor{}, fmt.Errorf("calibration source integrity mismatch: %s", cp)
	}
	return predictor{m, c, mh, make([]float64, len(m.Weights))}, nil
}
func observe(r maintraining.TrainingRowV1, long bool, p float64) policy.Observation {
	if long {
		return policy.Observation{TimestampMs: r.DecisionTimestampMs, Probability: p, Valid: r.LongLabelValid, Profitable: r.LongNetProfitableExFunding, Net: r.LongNetReturnExFunding, Gross: r.LongGrossMarketReturn, Fee: r.LongFeeCostReturn, Slippage: r.LongModeledSlippageCostReturn}
	}
	return policy.Observation{TimestampMs: r.DecisionTimestampMs, Probability: p, Valid: r.ShortLabelValid, Profitable: r.ShortNetProfitableExFunding, Net: r.ShortNetReturnExFunding, Gross: r.ShortGrossMarketReturn, Fee: r.ShortFeeCostReturn, Slippage: r.ShortModeledSlippageCostReturn}
}
func bins(o []policy.Observation) []policy.Candidate {
	out := make([]policy.Candidate, 10)
	for i := 0; i < 10; i++ {
		subset := make([]policy.Observation, 0, len(o)/10)
		for _, x := range o {
			b := int(math.Floor(x.Probability * 10))
			if b == 10 {
				b = 9
			}
			if b == i {
				subset = append(subset, x)
			}
		}
		s, n, d := policy.Evaluate(subset, 0)
		s.Coverage = float64(len(subset)) / float64(len(o))
		out[i] = policy.Candidate{Threshold: float64(i) / 10, NominalCoverage: .1, Stats: s, NonOverlap: n, Daily: d}
	}
	return out
}
func confirm(o []policy.Observation, p policy.SidePolicy) sideConfirmation {
	base, _, _ := policy.Evaluate(o, 0)
	if !p.Enabled {
		return sideConfirmation{Baseline: base}
	}
	s, n, d := policy.Evaluate(o, p.Threshold)
	return sideConfirmation{base, s, n, d}
}
func combined(ds []policy.Decision, l, s []policy.Observation) combinedReport {
	var r combinedReport
	nets := []float64{}
	for i, d := range ds {
		var o policy.Observation
		switch d {
		case policy.Long:
			r.LongDecisions++
			o = l[i]
		case policy.Short:
			r.ShortDecisions++
			o = s[i]
		default:
			r.NoTradeDecisions++
			continue
		}
		if !o.Valid {
			r.Invalid++
			continue
		}
		r.Valid++
		if o.Profitable {
			r.Profitable++
		}
		r.MeanNet += o.Net
		nets = append(nets, o.Net)
	}
	chosen := r.LongDecisions + r.ShortDecisions
	if len(ds) > 0 {
		r.DecisionCoverage = float64(chosen) / float64(len(ds))
	}
	if chosen > 0 {
		r.InvalidRate = float64(r.Invalid) / float64(chosen)
	}
	if r.Valid > 0 {
		r.WinRate = float64(r.Profitable) / float64(r.Valid)
		r.MeanNet /= float64(r.Valid)
		sortFloats(nets)
		m := len(nets) / 2
		if len(nets)%2 == 1 {
			r.MedianNet = nets[m]
		} else {
			r.MedianNet = (nets[m-1] + nets[m]) / 2
		}
	}
	return r
}
func sortFloats(x []float64) {
	for i := 1; i < len(x); i++ {
		v := x[i]
		j := i - 1
		for j >= 0 && x[j] > v {
			x[j+1] = x[j]
			j--
		}
		x[j+1] = v
	}
}
func files(root string, y, a, b int) []string {
	out := []string{}
	for m := a; m <= b; m++ {
		out = append(out, filepath.Join(root, fmt.Sprintf("%04d", y), fmt.Sprintf("BTCUSDT-training-v1-%04d-%02d.parquet", y, m)))
	}
	return out
}
