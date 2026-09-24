package mainfeature

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/parquet-go/parquet-go"
)

type Writer struct {
	path, tmp     string
	force, closed bool
	file          *os.File
	writer        *parquet.GenericWriter[MainFeaturesV1]
	batch         []MainFeaturesV1
}

func NewWriter(path string, force bool) (*Writer, error) {
	if !force {
		if _, err := os.Stat(path); err == nil {
			return nil, fmt.Errorf("feature output exists (use -force): %s", path)
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}
	tmp := path + ".tmp"
	if err := os.Remove(tmp); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	pw := parquet.NewGenericWriter[MainFeaturesV1](f, parquet.Compression(&parquet.Zstd), parquet.MaxRowsPerRowGroup(128*1024))
	return &Writer{path: path, tmp: tmp, force: force, file: f, writer: pw, batch: make([]MainFeaturesV1, 0, 4096)}, nil
}
func (w *Writer) Write(f MainFeaturesV1) error {
	if w.closed {
		return errors.New("feature writer closed")
	}
	w.batch = append(w.batch, f)
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
func (w *Writer) Close() error {
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
		_ = os.Remove(w.tmp)
		return err
	}
	w.closed = true
	if w.force {
		if err := os.Remove(w.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			_ = os.Remove(w.tmp)
			return err
		}
	}
	if err := os.Rename(w.tmp, w.path); err != nil {
		_ = os.Remove(w.tmp)
		return err
	}
	return nil
}
func (w *Writer) Abort() {
	if w.closed {
		return
	}
	w.closed = true
	_ = w.writer.Close()
	_ = w.file.Close()
	_ = os.Remove(w.tmp)
}

func Read(path string, fn func(MainFeaturesV1) error) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	r := parquet.NewGenericReader[MainFeaturesV1](f)
	defer r.Close()
	batch := make([]MainFeaturesV1, 4096)
	var total int64
	for {
		n, e := r.Read(batch)
		for i := 0; i < n; i++ {
			if err := fn(batch[i]); err != nil {
				return total, err
			}
			total++
		}
		if errors.Is(e, io.EOF) {
			return total, nil
		}
		if e != nil {
			return total, e
		}
	}
}
