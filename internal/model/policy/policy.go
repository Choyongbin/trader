package policy

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const Version = 1

type Decision string

const (
	Long    Decision = "LONG"
	Short   Decision = "SHORT"
	NoTrade Decision = "NO_TRADE"
)

type Range struct {
	StartMs int64 `json:"start_ms"`
	EndMs   int64 `json:"end_ms"`
}
type SidePolicy struct {
	Enabled   bool    `json:"enabled"`
	Threshold float64 `json:"threshold,omitempty"`
}
type Source struct {
	LogisticVersion        int    `json:"logistic_version"`
	CalibrationVersion     int    `json:"calibration_version"`
	LongLogisticSHA256     string `json:"long_logistic_sha256"`
	ShortLogisticSHA256    string `json:"short_logistic_sha256"`
	LongCalibrationSHA256  string `json:"long_calibration_sha256"`
	ShortCalibrationSHA256 string `json:"short_calibration_sha256"`
}
type Artifact struct {
	PolicyVersion        int        `json:"policy_version"`
	Symbol               string     `json:"symbol"`
	Source               Source     `json:"source"`
	SplitVersion         int        `json:"split_version"`
	Selection            Range      `json:"policy_selection_range"`
	Confirmation         Range      `json:"policy_confirmation_range"`
	MaxLabelDependencyMs int64      `json:"max_label_dependency_ms"`
	EmbargoMs            int64      `json:"embargo_ms"`
	PurgeRule            string     `json:"purge_rule"`
	CandidateCoverages   []float64  `json:"candidate_coverages"`
	MinValidSignalCount  int64      `json:"min_valid_signal_count"`
	MinActiveDays        int        `json:"min_active_days"`
	Long                 SidePolicy `json:"long"`
	Short                SidePolicy `json:"short"`
	ConflictRule         string     `json:"conflict_rule"`
}

func (a Artifact) Decide(pLong, pShort float64) (Decision, error) {
	if math.IsNaN(pLong) || math.IsNaN(pShort) || math.IsInf(pLong, 0) || math.IsInf(pShort, 0) || pLong < 0 || pLong > 1 || pShort < 0 || pShort > 1 {
		return "", fmt.Errorf("probabilities must be finite in [0,1]")
	}
	lq := a.Long.Enabled && pLong >= a.Long.Threshold
	sq := a.Short.Enabled && pShort >= a.Short.Threshold
	if !lq && !sq {
		return NoTrade, nil
	}
	if lq && !sq {
		return Long, nil
	}
	if sq && !lq {
		return Short, nil
	}
	lm, sm := pLong-a.Long.Threshold, pShort-a.Short.Threshold
	if lm > sm {
		return Long, nil
	}
	if sm > lm {
		return Short, nil
	}
	return NoTrade, nil
}

func IncludedInSelection(ts int64, r Range, dependency int64) bool {
	return ts >= r.StartMs && ts < r.EndMs && ts+dependency < r.EndMs
}

type Observation struct {
	TimestampMs               int64
	Probability               float64
	Valid, Profitable         bool
	Net, Gross, Fee, Slippage float64
}
type OutcomeStats struct {
	Signals      int64   `json:"signals"`
	Coverage     float64 `json:"coverage"`
	Valid        int64   `json:"valid"`
	Invalid      int64   `json:"invalid"`
	InvalidRate  float64 `json:"invalid_rate"`
	Profitable   int64   `json:"profitable"`
	WinRate      float64 `json:"win_rate"`
	MeanNet      float64 `json:"mean_net_return_ex_funding"`
	MedianNet    float64 `json:"median_net_return_ex_funding"`
	MeanGross    float64 `json:"mean_gross_market_return"`
	MeanFee      float64 `json:"mean_fee_cost_return"`
	MeanSlippage float64 `json:"mean_modeled_slippage_cost_return"`
}
type NonOverlapStats struct {
	Count     int64   `json:"count"`
	WinRate   float64 `json:"win_rate"`
	MeanNet   float64 `json:"mean_net_return_ex_funding"`
	MedianNet float64 `json:"median_net_return_ex_funding"`
}
type DailyStats struct {
	ActiveDays         int     `json:"active_days"`
	DailySignalCount   float64 `json:"daily_signal_count"`
	DailyMeanNet       float64 `json:"daily_mean_net_return"`
	MedianDailyMeanNet float64 `json:"median_of_daily_mean_return"`
	PositiveDays       int     `json:"positive_mean_days"`
	NegativeDays       int     `json:"negative_mean_days"`
}
type Candidate struct {
	Threshold       float64         `json:"threshold"`
	NominalCoverage float64         `json:"nominal_target_coverage"`
	Stats           OutcomeStats    `json:"stats"`
	NonOverlap      NonOverlapStats `json:"non_overlap"`
	Daily           DailyStats      `json:"daily"`
	Eligible        bool            `json:"eligible"`
}

func CandidateThresholds(obs []Observation, coverages []float64) []struct{ Threshold, Coverage float64 } {
	p := make([]float64, len(obs))
	for i := range obs {
		p[i] = obs[i].Probability
	}
	sort.Sort(sort.Reverse(sort.Float64Slice(p)))
	out := make([]struct{ Threshold, Coverage float64 }, 0, len(coverages))
	seen := map[uint64]bool{}
	for _, c := range coverages {
		if len(p) == 0 || c <= 0 || c > 1 {
			continue
		}
		k := int(math.Ceil(c * float64(len(p))))
		if k < 1 {
			k = 1
		}
		t := p[k-1]
		b := math.Float64bits(t)
		if seen[b] {
			continue
		}
		seen[b] = true
		out = append(out, struct{ Threshold, Coverage float64 }{t, c})
	}
	return out
}

func Evaluate(obs []Observation, threshold float64) (OutcomeStats, NonOverlapStats, DailyStats) {
	var s OutcomeStats
	nets := []float64{}
	noNets := []float64{}
	var noWins int64
	last := int64(math.MinInt64 / 2)
	type dayAcc struct {
		signals int64
		sum     float64
		valid   int64
	}
	days := map[int64]*dayAcc{}
	for _, o := range obs {
		if o.Probability < threshold {
			continue
		}
		s.Signals++
		d := o.TimestampMs / 86400000
		a := days[d]
		if a == nil {
			a = &dayAcc{}
			days[d] = a
		}
		a.signals++
		spaced := o.TimestampMs-last >= 3600000
		if spaced {
			last = o.TimestampMs
		}
		if !o.Valid {
			s.Invalid++
			continue
		}
		s.Valid++
		if o.Profitable {
			s.Profitable++
		}
		s.MeanNet += o.Net
		s.MeanGross += o.Gross
		s.MeanFee += o.Fee
		s.MeanSlippage += o.Slippage
		nets = append(nets, o.Net)
		a.sum += o.Net
		a.valid++
		if spaced {
			noNets = append(noNets, o.Net)
			if o.Profitable {
				noWins++
			}
		}
	}
	if len(obs) > 0 {
		s.Coverage = float64(s.Signals) / float64(len(obs))
	}
	if s.Signals > 0 {
		s.InvalidRate = float64(s.Invalid) / float64(s.Signals)
	}
	if s.Valid > 0 {
		n := float64(s.Valid)
		s.WinRate = float64(s.Profitable) / n
		s.MeanNet /= n
		s.MeanGross /= n
		s.MeanFee /= n
		s.MeanSlippage /= n
		s.MedianNet = median(nets)
	}
	no := NonOverlapStats{Count: int64(len(noNets)), MedianNet: median(noNets)}
	if len(noNets) > 0 {
		for _, v := range noNets {
			no.MeanNet += v
		}
		no.MeanNet /= float64(len(noNets))
		no.WinRate = float64(noWins) / float64(len(noNets))
	}
	dailyMeans := []float64{}
	var totalSignals int64
	for _, a := range days {
		if a.valid == 0 {
			continue
		}
		m := a.sum / float64(a.valid)
		dailyMeans = append(dailyMeans, m)
		totalSignals += a.signals
	}
	ds := DailyStats{ActiveDays: len(dailyMeans), MedianDailyMeanNet: median(dailyMeans)}
	if len(dailyMeans) > 0 {
		for _, v := range dailyMeans {
			ds.DailyMeanNet += v
			if v > 0 {
				ds.PositiveDays++
			} else if v < 0 {
				ds.NegativeDays++
			}
		}
		ds.DailyMeanNet /= float64(len(dailyMeans))
		ds.DailySignalCount = float64(totalSignals) / float64(len(dailyMeans))
	}
	return s, no, ds
}
func BuildCandidates(obs []Observation, coverages []float64, minValid int64, minDays int) []Candidate {
	ts := CandidateThresholds(obs, coverages)
	out := make([]Candidate, 0, len(ts))
	for _, x := range ts {
		s, n, d := Evaluate(obs, x.Threshold)
		out = append(out, Candidate{x.Threshold, x.Coverage, s, n, d, s.Valid >= minValid && d.ActiveDays >= minDays})
	}
	return out
}
func Select(cs []Candidate) SidePolicy {
	best := -1
	for i, c := range cs {
		if !c.Eligible || c.Stats.MeanNet <= 0 {
			continue
		}
		if best < 0 || c.Stats.MeanNet > cs[best].Stats.MeanNet+1e-15 || (math.Abs(c.Stats.MeanNet-cs[best].Stats.MeanNet) <= 1e-15 && c.Stats.Signals > cs[best].Stats.Signals) {
			best = i
		}
	}
	if best < 0 {
		return SidePolicy{}
	}
	return SidePolicy{true, cs[best].Threshold}
}
func median(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	x := append([]float64(nil), v...)
	sort.Float64s(x)
	m := len(x) / 2
	if len(x)%2 == 1 {
		return x[m]
	}
	return (x[m-1] + x[m]) / 2
}
func FileSHA256(path string) (string, error) {
	b, e := os.ReadFile(path)
	if e != nil {
		return "", e
	}
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:]), nil
}
func WriteArtifact(path string, a Artifact) error { return writeJSON(path, a) }
func ReadArtifact(path string) (Artifact, error) {
	var a Artifact
	b, e := os.ReadFile(path)
	if e != nil {
		return a, e
	}
	e = json.Unmarshal(b, &a)
	return a, e
}
func WriteJSON(path string, v any) error { return writeJSON(path, v) }
func writeJSON(path string, v any) error {
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
	if e = os.Remove(path); e != nil && !errors.Is(e, os.ErrNotExist) {
		return e
	}
	return os.Rename(tmp, path)
}
func UTC(s string) int64 {
	t, e := time.Parse(time.RFC3339, s)
	if e != nil {
		panic(e)
	}
	return t.UnixMilli()
}
