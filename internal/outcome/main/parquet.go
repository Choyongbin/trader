package mainoutcome

import (
	"errors"
	"github.com/parquet-go/parquet-go"
	"io"
	"os"
	"path/filepath"
)

type Writer struct {
	path, tmp     string
	force, closed bool
	f             *os.File
	w             *parquet.GenericWriter[MainOutcomeV1]
	batch         []MainOutcomeV1
}

func NewWriter(path string, force bool) (*Writer, error) {
	if !force {
		if _, e := os.Stat(path); e == nil {
			return nil, errors.New("outcome output exists; use -force")
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
	return &Writer{path: path, tmp: tmp, force: force, f: f, w: parquet.NewGenericWriter[MainOutcomeV1](f, parquet.Compression(&parquet.Zstd), parquet.MaxRowsPerRowGroup(128*1024)), batch: make([]MainOutcomeV1, 0, 4096)}, nil
}
func (w *Writer) Write(o MainOutcomeV1) error {
	w.batch = append(w.batch, o)
	if len(w.batch) == cap(w.batch) {
		return w.flush()
	}
	return nil
}
func (w *Writer) flush() error {
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
func (w *Writer) Abort() {
	if w.closed {
		return
	}
	w.closed = true
	_ = w.w.Close()
	_ = w.f.Close()
	_ = os.Remove(w.tmp)
}
func (w *Writer) Close() error {
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
func Read(path string, fn func(MainOutcomeV1) error) (int64, error) {
	f, e := os.Open(path)
	if e != nil {
		return 0, e
	}
	defer f.Close()
	r := parquet.NewGenericReader[MainOutcomeV1](f)
	defer r.Close()
	b := make([]MainOutcomeV1, 4096)
	var total int64
	for {
		n, err := r.Read(b)
		for i := 0; i < n; i++ {
			if e := fn(b[i]); e != nil {
				return total, e
			}
			total++
		}
		if errors.Is(err, io.EOF) {
			return total, nil
		}
		if err != nil {
			return total, err
		}
	}
}
