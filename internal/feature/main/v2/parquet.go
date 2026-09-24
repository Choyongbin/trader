package featurev2

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/parquet-go/parquet-go"
)

type Writer struct {
	path, tmp string
	file      *os.File
	writer    *parquet.GenericWriter[FeatureRowV2]
	batch     []FeatureRowV2
	closed    bool
}

func NewWriter(path string) (*Writer, error) {
	if _, err := os.Stat(path); err == nil {
		return nil, fmt.Errorf("output exists: %s", path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}
	tmp := path + ".tmp"
	if _, err := os.Stat(tmp); err == nil {
		return nil, fmt.Errorf("temporary output exists: %s", tmp)
	}
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	w := parquet.NewGenericWriter[FeatureRowV2](f, parquet.Compression(&parquet.Zstd), parquet.MaxRowsPerRowGroup(128*1024))
	return &Writer{path: path, tmp: tmp, file: f, writer: w, batch: make([]FeatureRowV2, 0, 4096)}, nil
}

func (w *Writer) Write(row FeatureRowV2) error {
	if w.closed {
		return errors.New("writer closed")
	}
	w.batch = append(w.batch, row)
	if len(w.batch) == cap(w.batch) {
		return w.flush()
	}
	return nil
}
func (w *Writer) flush() error {
	if len(w.batch) == 0 {
		return nil
	}
	n, err := w.writer.Write(w.batch)
	if err != nil {
		return err
	}
	if n != len(w.batch) {
		return io.ErrShortWrite
	}
	w.batch = w.batch[:0]
	return nil
}
func (w *Writer) CloseTemporary() error {
	if w.closed {
		return nil
	}
	if err := w.flush(); err != nil {
		w.Abort()
		return err
	}
	if err := w.writer.Close(); err != nil {
		w.Abort()
		return err
	}
	if err := w.file.Sync(); err != nil {
		w.Abort()
		return err
	}
	if err := w.file.Close(); err != nil {
		w.Abort()
		return err
	}
	w.closed = true
	return nil
}
func (w *Writer) Publish() error {
	if !w.closed {
		return errors.New("temporary writer not finalized")
	}
	return os.Rename(w.tmp, w.path)
}
func (w *Writer) TemporaryPath() string { return w.tmp }
func (w *Writer) Abort() {
	if !w.closed {
		_ = w.writer.Close()
		_ = w.file.Close()
		w.closed = true
	}
	_ = os.Remove(w.tmp)
}

func ReadRows(path string, fn func(FeatureRowV2) error) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	r := parquet.NewGenericReader[FeatureRowV2](f)
	defer r.Close()
	buf := make([]FeatureRowV2, 2048)
	var total int64
	for {
		n, readErr := r.Read(buf)
		for i := 0; i < n; i++ {
			if err := fn(buf[i]); err != nil {
				return total, err
			}
			total++
		}
		if errors.Is(readErr, io.EOF) {
			return total, nil
		}
		if readErr != nil {
			return total, readErr
		}
	}
}
