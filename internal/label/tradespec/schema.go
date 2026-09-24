package tradespeclabel

import (
	"binance_trader/internal/tradelabel"
	"errors"
	"github.com/parquet-go/parquet-go"
	"io"
	"os"
	"path/filepath"
)

const Version = 1

type Row struct {
	DecisionTimestampMs       int64             `parquet:"decision_timestamp_ms"`
	EntryReferenceAvailable   bool              `parquet:"entry_reference_available"`
	EntryWaitMs               int64             `parquet:"entry_wait_ms"`
	LabelValid                bool              `parquet:"label_valid"`
	NetProfitableExFunding    bool              `parquet:"net_profitable_ex_funding"`
	TradeResult               tradelabel.Status `parquet:"trade_result"`
	GrossMarketReturn         float64           `parquet:"gross_market_return"`
	FeeCostReturn             float64           `parquet:"fee_cost_return"`
	ModeledSlippageCostReturn float64           `parquet:"modeled_slippage_cost_return"`
	NetReturnExFunding        float64           `parquet:"net_return_ex_funding"`
}
type Writer struct {
	path, tmp string
	f         *os.File
	w         *parquet.GenericWriter[Row]
}

func NewWriter(path string) (*Writer, error) {
	if e := os.MkdirAll(filepath.Dir(path), 0755); e != nil {
		return nil, e
	}
	tmp := path + ".tmp"
	_ = os.Remove(tmp)
	f, e := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if e != nil {
		return nil, e
	}
	return &Writer{path, tmp, f, parquet.NewGenericWriter[Row](f, parquet.Compression(&parquet.Zstd))}, nil
}
func (w *Writer) Write(r Row) error { _, e := w.w.Write([]Row{r}); return e }
func (w *Writer) Close() error {
	if e := w.w.Close(); e != nil {
		return e
	}
	if e := w.f.Close(); e != nil {
		return e
	}
	_ = os.Remove(w.path)
	return os.Rename(w.tmp, w.path)
}

func Read(path string, fn func(Row) error) (int64, error) {
	f, e := os.Open(path)
	if e != nil {
		return 0, e
	}
	defer f.Close()
	r := parquet.NewGenericReader[Row](f)
	defer r.Close()
	b := make([]Row, 2048)
	var nall int64
	for {
		n, er := r.Read(b)
		for i := 0; i < n; i++ {
			if e = fn(b[i]); e != nil {
				return nall, e
			}
			nall++
		}
		if errors.Is(er, io.EOF) {
			return nall, nil
		}
		if er != nil {
			return nall, er
		}
	}
}
func Write(path string, rows []Row) error {
	if e := os.MkdirAll(filepath.Dir(path), 0755); e != nil {
		return e
	}
	tmp := path + ".tmp"
	_ = os.Remove(tmp)
	f, e := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if e != nil {
		return e
	}
	w := parquet.NewGenericWriter[Row](f, parquet.Compression(&parquet.Zstd))
	_, e = w.Write(rows)
	if e == nil {
		e = w.Close()
	}
	if e == nil {
		e = f.Close()
	}
	if e != nil {
		_ = os.Remove(tmp)
		return e
	}
	_ = os.Remove(path)
	return os.Rename(tmp, path)
}
