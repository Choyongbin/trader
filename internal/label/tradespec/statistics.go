package tradespeclabel

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"

	"binance_trader/internal/tradelabel"
)

// Statistics is the complete per-candidate, per-partition label summary.
// Its return aggregation and p50 semantics intentionally match Phase 9A's
// Metrics.Finish implementation.
type Statistics struct {
	RawDecisionCount                      int64   `json:"raw_decision_count"`
	PurgedCount                           int64   `json:"purged_count"`
	IncludedCount                         int64   `json:"included_count"`
	LabelValidCount                       int64   `json:"label_valid_count"`
	LabelInvalidCount                     int64   `json:"label_invalid_count"`
	PositiveCount                         int64   `json:"positive_count"`
	NegativeCount                         int64   `json:"negative_count"`
	ValidRate                             float64 `json:"valid_rate"`
	PositiveRate                          float64 `json:"positive_rate"`
	WinRate                               float64 `json:"win_rate"`
	TPFirstCount                          int64   `json:"tp_first_count"`
	SLFirstCount                          int64   `json:"sl_first_count"`
	TimeoutCount                          int64   `json:"timeout_count"`
	EntryReferenceUnavailableCount        int64   `json:"entry_reference_unavailable_count"`
	ExitReferenceUnavailableCount         int64   `json:"exit_reference_unavailable_count"`
	GrossProfitableCount                  int64   `json:"gross_profitable_count"`
	NetProfitableCount                    int64   `json:"net_profitable_count"`
	GrossProfitableToNetUnprofitableCount int64   `json:"gross_profitable_to_net_unprofitable_count"`
	MeanGrossMarketReturn                 float64 `json:"mean_gross_market_return"`
	MeanFeeCostReturn                     float64 `json:"mean_fee_cost_return"`
	MeanModeledSlippageCostReturn         float64 `json:"mean_modeled_slippage_cost_return"`
	MeanNetReturnExFunding                float64 `json:"mean_net_return_ex_funding"`
	MedianNetReturnExFunding              float64 `json:"median_net_return_ex_funding"`
}

// Accumulator receives each decision exactly once: Purged for a boundary-
// purged decision, or Add for a row written to the candidate-label parquet.
type Accumulator struct {
	statistics Statistics
	nets       []float64
	netPath    string
	netFile    *os.File
	netWriter  *bufio.Writer
	gross      float64
	fee        float64
	slippage   float64
	net        float64
}

// NewAccumulator stores valid net returns in a temporary binary stream until
// Finalize. This follows Phase 9A's exact-return-record approach while
// avoiding retention of all candidates and partitions during materialization.
func NewAccumulator(netPath string) (*Accumulator, error) {
	if err := os.MkdirAll(filepath.Dir(netPath), 0755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(netPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &Accumulator{netPath: netPath, netFile: file, netWriter: bufio.NewWriterSize(file, 1<<20)}, nil
}

func (a *Accumulator) Purged() {
	a.statistics.RawDecisionCount++
	a.statistics.PurgedCount++
}

func (a *Accumulator) Add(r Row) error {
	a.statistics.RawDecisionCount++
	a.statistics.IncludedCount++

	switch r.TradeResult {
	case tradelabel.TPFirst:
		a.statistics.TPFirstCount++
	case tradelabel.SLFirst:
		a.statistics.SLFirstCount++
	case tradelabel.Timeout:
		a.statistics.TimeoutCount++
	case tradelabel.EntryReferenceUnavailable:
		a.statistics.EntryReferenceUnavailableCount++
	case tradelabel.ExitReferenceUnavailable:
		a.statistics.ExitReferenceUnavailableCount++
	}

	if !r.LabelValid {
		a.statistics.LabelInvalidCount++
		return nil
	}
	for _, value := range []float64{
		r.GrossMarketReturn,
		r.FeeCostReturn,
		r.ModeledSlippageCostReturn,
		r.NetReturnExFunding,
	} {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return fmt.Errorf("valid label contains non-finite economics")
		}
	}

	a.statistics.LabelValidCount++
	if r.NetProfitableExFunding {
		a.statistics.PositiveCount++
		a.statistics.NetProfitableCount++
	} else {
		a.statistics.NegativeCount++
	}
	if r.GrossMarketReturn > 0 {
		a.statistics.GrossProfitableCount++
		if !r.NetProfitableExFunding {
			a.statistics.GrossProfitableToNetUnprofitableCount++
		}
	}
	a.gross += r.GrossMarketReturn
	a.fee += r.FeeCostReturn
	a.slippage += r.ModeledSlippageCostReturn
	a.net += r.NetReturnExFunding
	if a.netWriter == nil {
		a.nets = append(a.nets, r.NetReturnExFunding)
	} else if err := writeFloat64(a.netWriter, r.NetReturnExFunding); err != nil {
		return err
	}
	return nil
}

func (a *Accumulator) Finalize() (Statistics, error) {
	s := a.statistics
	if s.RawDecisionCount != s.PurgedCount+s.IncludedCount {
		return s, fmt.Errorf("raw decision invariant violated: raw=%d purged=%d included=%d", s.RawDecisionCount, s.PurgedCount, s.IncludedCount)
	}
	if s.LabelValidCount != s.PositiveCount+s.NegativeCount {
		return s, fmt.Errorf("valid label invariant violated: valid=%d positive=%d negative=%d", s.LabelValidCount, s.PositiveCount, s.NegativeCount)
	}
	if s.RawDecisionCount > 0 {
		s.ValidRate = float64(s.LabelValidCount) / float64(s.RawDecisionCount)
	}
	if s.LabelValidCount == 0 {
		if _, err := a.netReturns(); err != nil {
			return s, err
		}
		return s, nil
	}
	n := float64(s.LabelValidCount)
	s.PositiveRate = float64(s.PositiveCount) / n
	s.WinRate = s.PositiveRate
	s.MeanGrossMarketReturn = a.gross / n
	s.MeanFeeCostReturn = a.fee / n
	s.MeanModeledSlippageCostReturn = a.slippage / n
	s.MeanNetReturnExFunding = a.net / n
	nets, err := a.netReturns()
	if err != nil {
		return s, err
	}
	s.MedianNetReturnExFunding = Median(nets)
	return s, nil
}

func writeFloat64(w io.Writer, value float64) error {
	var record [8]byte
	binary.LittleEndian.PutUint64(record[:], math.Float64bits(value))
	_, err := w.Write(record[:])
	return err
}

func (a *Accumulator) netReturns() ([]float64, error) {
	if a.netWriter == nil {
		return a.nets, nil
	}
	if err := a.netWriter.Flush(); err != nil {
		return nil, err
	}
	if err := a.netFile.Close(); err != nil {
		return nil, err
	}
	a.netWriter = nil
	a.netFile = nil

	file, err := os.Open(a.netPath)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = file.Close()
		_ = os.Remove(a.netPath)
	}()
	reader := bufio.NewReaderSize(file, 1<<20)
	nets := make([]float64, 0, a.statistics.LabelValidCount)
	var record [8]byte
	for {
		_, err := io.ReadFull(reader, record[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read net-return records: %w", err)
		}
		nets = append(nets, math.Float64frombits(binary.LittleEndian.Uint64(record[:])))
	}
	if len(nets) != int(a.statistics.LabelValidCount) {
		return nil, fmt.Errorf("net-return record count mismatch: got=%d valid=%d", len(nets), a.statistics.LabelValidCount)
	}
	return nets, nil
}

// Median uses the exact Phase 9A p50 rule: p=q*(n-1), then linear
// interpolation between floor(p) and ceil(p), after ascending sorting.
func Median(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	p := 0.5 * float64(len(sorted)-1)
	lo := int(math.Floor(p))
	hi := int(math.Ceil(p))
	return sorted[lo] + (p-float64(lo))*(sorted[hi]-sorted[lo])
}
