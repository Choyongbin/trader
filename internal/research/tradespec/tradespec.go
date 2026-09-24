// Package tradespec contains the bounded-memory Phase 9A economics study.
package tradespec

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"

	mainbarrier "binance_trader/internal/barrier/main"
	"binance_trader/internal/tradelabel"
)

type Spec struct {
	TPBps          int `json:"tp_bps"`
	SLBps          int `json:"sl_bps"`
	HorizonSeconds int `json:"horizon_seconds"`
}

func (s Spec) ID() string { return fmt.Sprintf("tp%d_sl%d_h%d", s.TPBps, s.SLBps, s.HorizonSeconds) }
func Grid() []Spec {
	pairs := [][2]int{{25, 25}, {50, 25}, {50, 50}, {75, 25}, {75, 50}, {100, 50}, {100, 100}}
	hs := []int{900, 1800, 3600, 14400}
	o := make([]Spec, 0, 28)
	for _, p := range pairs {
		for _, h := range hs {
			o = append(o, Spec{p[0], p[1], h})
		}
	}
	return o
}
func GridHash(g []Spec) string {
	x := ""
	for _, s := range g {
		x += s.ID() + "\n"
	}
	h := sha256.Sum256([]byte(x))
	return hex.EncodeToString(h[:])
}
func Control() Spec { return Spec{50, 25, 3600} }
func Dependency(m mainbarrier.ManifestV2, h int) int64 {
	return m.EntryDelayMs + m.MaxEntryWaitMs + int64(h)*1000 + m.MaxExitReferenceWaitMs
}
func ValidateGrid(m mainbarrier.ManifestV2, g []Spec) error {
	levels := map[int]bool{}
	for _, x := range m.BarrierGridBps {
		levels[x] = true
	}
	horizons := map[int]bool{}
	for _, x := range m.TimeoutHorizonsSeconds {
		horizons[x] = true
	}
	if len(g) != 28 {
		return fmt.Errorf("expected 28 specs, got %d", len(g))
	}
	seen := map[string]bool{}
	for _, s := range g {
		if seen[s.ID()] || !levels[s.TPBps] || !levels[s.SLBps] || !horizons[s.HorizonSeconds] {
			return fmt.Errorf("invalid/missing barrier grid spec %s", s.ID())
		}
		seen[s.ID()] = true
	}
	return nil
}

type Daily struct {
	Sum   float64
	Count int64
}
type Metrics struct {
	Raw, Valid, Invalid, EntryUnavailable, ExitUnavailable, TPFirst, SLFirst, Timeout, GrossWin, NetWin, GrossWinNetLoss int64
	SumGross, SumFee, SumSlip, SumNet, SumWin, SumLoss                                                                   float64
	Wins, Losses                                                                                                         int64
	Daily                                                                                                                map[int64]Daily
	LastNonOverlap                                                                                                       int64
	NonNets                                                                                                              []float64
	NonWins                                                                                                              int64
	Path                                                                                                                 string
	writer                                                                                                               *bufio.Writer
	file                                                                                                                 *os.File
}
type Summary struct {
	RawDecisions              int64              `json:"raw_decisions"`
	LabelValid                int64              `json:"label_valid"`
	LabelInvalid              int64              `json:"label_invalid"`
	ValidRate                 float64            `json:"valid_rate"`
	EntryReferenceUnavailable int64              `json:"entry_reference_unavailable"`
	ExitReferenceUnavailable  int64              `json:"exit_reference_unavailable"`
	TPFirst                   int64              `json:"tp_first"`
	SLFirst                   int64              `json:"sl_first"`
	Timeout                   int64              `json:"timeout"`
	GrossProfitable           int64              `json:"gross_profitable"`
	NetProfitable             int64              `json:"net_profitable_ex_funding"`
	WinRate                   float64            `json:"win_rate"`
	GrossWinNetLoss           int64              `json:"gross_profitable_net_unprofitable"`
	GrossWinNetLossRate       float64            `json:"gross_profitable_net_unprofitable_rate"`
	MeanGross                 float64            `json:"mean_gross_market_return"`
	MedianGross               float64            `json:"median_gross_market_return"`
	MeanFee                   float64            `json:"mean_fee_cost_return"`
	MeanSlippage              float64            `json:"mean_modeled_slippage_cost_return"`
	MeanNet                   float64            `json:"mean_net_return_ex_funding"`
	MedianNet                 float64            `json:"median_net_return_ex_funding"`
	MeanWinningNet            float64            `json:"mean_winning_net_return"`
	MeanLosingNet             float64            `json:"mean_losing_net_return"`
	AverageWinLossRatio       float64            `json:"average_win_loss_ratio"`
	BreakEvenWinRate          float64            `json:"empirical_break_even_win_rate"`
	Percentiles               map[string]float64 `json:"net_return_ex_funding_percentiles"`
	Daily                     DailySummary       `json:"daily"`
	NonOverlap                NonOverlap         `json:"non_overlap"`
}
type DailySummary struct {
	ActiveDays       int     `json:"active_days"`
	MeanDailyMean    float64 `json:"mean_daily_mean"`
	MedianDailyMean  float64 `json:"median_daily_mean"`
	PositiveDays     int     `json:"positive_mean_days"`
	NegativeDays     int     `json:"negative_mean_days"`
	PositiveDayRatio float64 `json:"positive_day_ratio"`
}
type NonOverlap struct {
	Count     int64   `json:"count"`
	WinRate   float64 `json:"win_rate"`
	MeanNet   float64 `json:"mean_net_return_ex_funding"`
	MedianNet float64 `json:"median_net_return_ex_funding"`
}

func NewMetrics(path string) (*Metrics, error) {
	if e := os.MkdirAll(filepath.Dir(path), 0755); e != nil {
		return nil, e
	}
	f, e := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if e != nil {
		return nil, e
	}
	return &Metrics{Daily: map[int64]Daily{}, Path: path, writer: bufio.NewWriterSize(f, 1<<20), file: f}, nil
}
func (m *Metrics) Add(b mainbarrier.BarrierOutcomeV2, s Spec, side tradelabel.Side, c tradelabel.CostProfile) error {
	m.Raw++
	r, e := tradelabel.EvaluateV2(b, tradelabel.TradeSpec{Side: side, TPBps: s.TPBps, SLBps: s.SLBps, HorizonSeconds: s.HorizonSeconds}, c)
	if e != nil {
		return e
	}
	switch r.Status {
	case tradelabel.EntryReferenceUnavailable:
		m.EntryUnavailable++
	case tradelabel.ExitReferenceUnavailable:
		m.ExitUnavailable++
	case tradelabel.TPFirst:
		m.TPFirst++
	case tradelabel.SLFirst:
		m.SLFirst++
	case tradelabel.Timeout:
		m.Timeout++
	}
	if !r.LabelValid {
		m.Invalid++
		return nil
	}
	m.Valid++
	if r.GrossProfitable {
		m.GrossWin++
		if !r.NetProfitableExFunding {
			m.GrossWinNetLoss++
		}
	}
	if r.NetProfitableExFunding {
		m.NetWin++
		m.Wins++
		m.SumWin += r.NetReturnExFunding
	} else {
		m.Losses++
		m.SumLoss += r.NetReturnExFunding
	}
	m.SumGross += r.GrossMarketReturn
	m.SumFee += r.FeeCostReturn
	m.SumSlip += r.ModeledSlippageCostReturn
	m.SumNet += r.NetReturnExFunding
	d := b.DecisionTimestampMs / 86400000
	a := m.Daily[d]
	a.Sum += r.NetReturnExFunding
	a.Count++
	m.Daily[d] = a
	if b.DecisionTimestampMs-m.LastNonOverlap >= int64(s.HorizonSeconds)*1000 {
		m.LastNonOverlap = b.DecisionTimestampMs
		m.NonNets = append(m.NonNets, r.NetReturnExFunding)
		if r.NetProfitableExFunding {
			m.NonWins++
		}
	}
	var x [16]byte
	binary.LittleEndian.PutUint64(x[:8], math.Float64bits(r.NetReturnExFunding))
	binary.LittleEndian.PutUint64(x[8:], math.Float64bits(r.GrossMarketReturn))
	_, e = m.writer.Write(x[:])
	return e
}
func (m *Metrics) Finish() (Summary, error) {
	if e := m.writer.Flush(); e != nil {
		return Summary{}, e
	}
	if e := m.file.Close(); e != nil {
		return Summary{}, e
	}
	nets := make([]float64, 0, m.Valid)
	gross := make([]float64, 0, m.Valid)
	f, e := os.Open(m.Path)
	if e != nil {
		return Summary{}, e
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	var x [16]byte
	for {
		_, e = io.ReadFull(r, x[:])
		if e == io.EOF {
			break
		}
		if e != nil {
			return Summary{}, e
		}
		nets = append(nets, math.Float64frombits(binary.LittleEndian.Uint64(x[:8])))
		gross = append(gross, math.Float64frombits(binary.LittleEndian.Uint64(x[8:])))
	}
	if len(nets) != int(m.Valid) {
		return Summary{}, fmt.Errorf("temporary returns truncated")
	}
	sort.Float64s(nets)
	sort.Float64s(gross)
	z := Summary{RawDecisions: m.Raw, LabelValid: m.Valid, LabelInvalid: m.Invalid, EntryReferenceUnavailable: m.EntryUnavailable, ExitReferenceUnavailable: m.ExitUnavailable, TPFirst: m.TPFirst, SLFirst: m.SLFirst, Timeout: m.Timeout, GrossProfitable: m.GrossWin, NetProfitable: m.NetWin, GrossWinNetLoss: m.GrossWinNetLoss, Percentiles: map[string]float64{}}
	if m.Raw > 0 {
		z.ValidRate = float64(m.Valid) / float64(m.Raw)
	}
	if m.Valid > 0 {
		n := float64(m.Valid)
		z.WinRate = float64(m.NetWin) / n
		z.GrossWinNetLossRate = float64(m.GrossWinNetLoss) / n
		z.MeanGross = m.SumGross / n
		z.MedianGross = quantile(gross, .5)
		z.MeanFee = m.SumFee / n
		z.MeanSlippage = m.SumSlip / n
		z.MeanNet = m.SumNet / n
		z.MedianNet = quantile(nets, .5)
	}
	if m.Wins > 0 {
		z.MeanWinningNet = m.SumWin / float64(m.Wins)
	}
	if m.Losses > 0 {
		z.MeanLosingNet = m.SumLoss / float64(m.Losses)
	}
	if z.MeanLosingNet < 0 {
		z.AverageWinLossRatio = z.MeanWinningNet / -z.MeanLosingNet
		z.BreakEvenWinRate = (-z.MeanLosingNet) / (z.MeanWinningNet - z.MeanLosingNet)
	}
	for _, q := range []struct {
		k string
		q float64
	}{{"p01", .01}, {"p05", .05}, {"p25", .25}, {"p50", .5}, {"p75", .75}, {"p95", .95}, {"p99", .99}} {
		z.Percentiles[q.k] = quantile(nets, q.q)
	}
	z.Daily = finishDaily(m.Daily)
	z.NonOverlap = finishNon(m.NonNets, m.NonWins)
	return z, nil
}
func quantile(x []float64, q float64) float64 {
	if len(x) == 0 {
		return 0
	}
	p := q * float64(len(x)-1)
	lo := int(math.Floor(p))
	hi := int(math.Ceil(p))
	return x[lo] + (p-float64(lo))*(x[hi]-x[lo])
}
func finishDaily(d map[int64]Daily) DailySummary {
	a := make([]float64, 0, len(d))
	for _, x := range d {
		if x.Count > 0 {
			a = append(a, x.Sum/float64(x.Count))
		}
	}
	z := DailySummary{ActiveDays: len(a)}
	if len(a) == 0 {
		return z
	}
	for _, x := range a {
		z.MeanDailyMean += x
		if x > 0 {
			z.PositiveDays++
		} else if x < 0 {
			z.NegativeDays++
		}
	}
	z.MeanDailyMean /= float64(len(a))
	sort.Float64s(a)
	z.MedianDailyMean = quantile(a, .5)
	z.PositiveDayRatio = float64(z.PositiveDays) / float64(len(a))
	return z
}
func finishNon(x []float64, w int64) NonOverlap {
	z := NonOverlap{Count: int64(len(x))}
	if len(x) == 0 {
		return z
	}
	for _, v := range x {
		z.MeanNet += v
	}
	z.MeanNet /= float64(len(x))
	z.WinRate = float64(w) / float64(len(x))
	sort.Float64s(x)
	z.MedianNet = quantile(x, .5)
	return z
}
