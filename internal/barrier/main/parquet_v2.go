package mainbarrier

import (
	"errors"
	"github.com/parquet-go/parquet-go"
	"io"
	"os"
	"path/filepath"
)

type WriterV2 struct {
	path, tmp     string
	force, closed bool
	f             *os.File
	w             *parquet.GenericWriter[BarrierOutcomeV2]
	batch         []BarrierOutcomeV2
}

func NewWriterV2(path string, force bool) (*WriterV2, error) {
	if !force {
		if _, e := os.Stat(path); e == nil {
			return nil, errors.New("barrier V2 output exists; use -force")
		} else if !errors.Is(e, os.ErrNotExist) {
			return nil, e
		}
	}
	if e := os.MkdirAll(filepath.Dir(path), 0755); e != nil {
		return nil, e
	}
	tmp := path + ".tmp"
	_ = os.Remove(tmp)
	f, e := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if e != nil {
		return nil, e
	}
	return &WriterV2{path: path, tmp: tmp, force: force, f: f, w: parquet.NewGenericWriter[BarrierOutcomeV2](f, parquet.Compression(&parquet.Zstd), parquet.MaxRowsPerRowGroup(64*1024)), batch: make([]BarrierOutcomeV2, 0, 2048)}, nil
}
func (w *WriterV2) Write(x BarrierOutcomeV2) error {
	w.batch = append(w.batch, x)
	if len(w.batch) == cap(w.batch) {
		return w.flush()
	}
	return nil
}
func (w *WriterV2) flush() error {
	if len(w.batch) == 0 {
		return nil
	}
	n, e := w.w.Write(w.batch)
	if e != nil {
		return e
	}
	if n != len(w.batch) {
		return io.ErrShortWrite
	}
	w.batch = w.batch[:0]
	return nil
}
func (w *WriterV2) Abort() {
	if w.closed {
		return
	}
	w.closed = true
	_ = w.w.Close()
	_ = w.f.Close()
	_ = os.Remove(w.tmp)
}
func (w *WriterV2) Close() error {
	if e := w.flush(); e != nil {
		w.Abort()
		return e
	}
	if e := w.w.Close(); e != nil {
		w.Abort()
		return e
	}
	if e := w.f.Sync(); e != nil {
		w.Abort()
		return e
	}
	if e := w.f.Close(); e != nil {
		return e
	}
	w.closed = true
	if w.force {
		_ = os.Remove(w.path)
	}
	if e := os.Rename(w.tmp, w.path); e != nil {
		_ = os.Remove(w.tmp)
		return e
	}
	return nil
}
func ReadV2(path string, fn func(BarrierOutcomeV2) error) (int64, error) {
	f, e := os.Open(path)
	if e != nil {
		return 0, e
	}
	defer f.Close()
	r := parquet.NewGenericReader[BarrierOutcomeV2](f)
	defer r.Close()
	b := make([]BarrierOutcomeV2, 2048)
	var nall int64
	for {
		n, er := r.Read(b)
		for i := 0; i < n; i++ {
			if e := fn(b[i]); e != nil {
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
