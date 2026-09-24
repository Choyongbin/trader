package tradespecmodeldata

import (
	"errors"
	"fmt"
	"io"
	"os"

	mainfeature "binance_trader/internal/feature/main"
	tradespeclabel "binance_trader/internal/label/tradespec"
	mainsplit "binance_trader/internal/split/main"
	"github.com/parquet-go/parquet-go"
)

type FeatureSource interface {
	NextFeature() (FeatureRecord, bool, error)
	Close() error
}

type CandidateLabelSource interface {
	NextLabel() (tradespeclabel.Row, bool, error)
	Close() error
}

type FeatureSourceFactory func() (FeatureSource, error)
type CandidateLabelSourceFactory func() (CandidateLabelSource, error)

type Loader struct {
	CandidateManifest tradespeclabel.Manifest
	Split             mainsplit.SplitDefinition
	Features          FeatureSourceFactory
	Labels            map[mainsplit.Partition]CandidateLabelSourceFactory
}

func NewParquetLoader(manifest tradespeclabel.Manifest, split mainsplit.SplitDefinition, featurePaths []string, labelPaths map[mainsplit.Partition][]string) Loader {
	labels := make(map[mainsplit.Partition]CandidateLabelSourceFactory, len(labelPaths))
	for partition, paths := range labelPaths {
		paths := append([]string(nil), paths...)
		labels[partition] = func() (CandidateLabelSource, error) { return NewCandidateLabelParquetSource(paths), nil }
	}
	featurePaths = append([]string(nil), featurePaths...)
	return Loader{CandidateManifest: manifest, Split: split, Features: func() (FeatureSource, error) { return NewFeatureV1ParquetSource(featurePaths), nil }, Labels: labels}
}

func (l Loader) StreamTrain(visit func(Sample) error) (int64, error) {
	return l.stream(mainsplit.Train, visit)
}

func (l Loader) StreamValidation(visit func(Sample) error) (int64, error) {
	return l.stream(mainsplit.Validation, visit)
}

func (l Loader) StreamTest(visit func(Sample) error) (int64, error) {
	return l.stream(mainsplit.Test, visit)
}

func (l Loader) stream(partition mainsplit.Partition, visit func(Sample) error) (int64, error) {
	if partition != mainsplit.Train && partition != mainsplit.Validation && partition != mainsplit.Test {
		return 0, fmt.Errorf("unsupported model-data partition %q", partition)
	}
	if l.CandidateManifest.MaxLabelDependencyMs < 0 {
		return 0, fmt.Errorf("invalid max_label_dependency_ms")
	}
	if l.Features == nil || l.Labels[partition] == nil {
		return 0, fmt.Errorf("model-data source is missing for partition %s", partition)
	}
	features, err := l.Features()
	if err != nil {
		return 0, err
	}
	defer features.Close()
	labels, err := l.Labels[partition]()
	if err != nil {
		return 0, err
	}
	defer labels.Close()

	start, end, err := splitRange(l.Split, partition)
	if err != nil {
		return 0, err
	}
	label, hasLabel, lastLabel, err := nextLabel(labels, 0, false)
	if err != nil {
		return 0, err
	}
	var lastFeature int64
	var haveFeature bool
	var emitted int64
	for {
		feature, ok, err := features.NextFeature()
		if err != nil {
			return emitted, err
		}
		if !ok {
			break
		}
		if haveFeature && feature.DecisionTimestampMs <= lastFeature {
			return emitted, fmt.Errorf("feature timestamps are not strictly increasing: previous=%d current=%d", lastFeature, feature.DecisionTimestampMs)
		}
		haveFeature, lastFeature = true, feature.DecisionTimestampMs
		if feature.DecisionTimestampMs < start {
			continue
		}
		if feature.DecisionTimestampMs >= end {
			break
		}
		if feature.DecisionTimestampMs+l.CandidateManifest.MaxLabelDependencyMs >= end {
			continue
		}
		if !hasLabel {
			return emitted, fmt.Errorf("missing included label at timestamp=%d partition=%s", feature.DecisionTimestampMs, partition)
		}
		if label.DecisionTimestampMs < feature.DecisionTimestampMs {
			return emitted, fmt.Errorf("unexpected label at timestamp=%d before feature=%d partition=%s", label.DecisionTimestampMs, feature.DecisionTimestampMs, partition)
		}
		if label.DecisionTimestampMs > feature.DecisionTimestampMs {
			return emitted, fmt.Errorf("missing included label at timestamp=%d partition=%s", feature.DecisionTimestampMs, partition)
		}
		if label.LabelValid {
			emitted++
			if err := visit(Sample{DecisionTimestampMs: feature.DecisionTimestampMs, Features: feature.Values, BinaryTarget: label.NetProfitableExFunding, NetReturnExFunding: label.NetReturnExFunding}); err != nil {
				return emitted, err
			}
		}
		label, hasLabel, lastLabel, err = nextLabel(labels, lastLabel, true)
		if err != nil {
			return emitted, err
		}
	}
	if hasLabel {
		return emitted, fmt.Errorf("unexpected label at timestamp=%d partition=%s", label.DecisionTimestampMs, partition)
	}
	return emitted, nil
}

func splitRange(split mainsplit.SplitDefinition, partition mainsplit.Partition) (int64, int64, error) {
	switch partition {
	case mainsplit.Train:
		return split.Train.StartMs, split.Train.EndMs, nil
	case mainsplit.Validation:
		return split.Validation.StartMs, split.Validation.EndMs, nil
	case mainsplit.Test:
		return split.Test.StartMs, split.Test.EndMs, nil
	default:
		return 0, 0, fmt.Errorf("unsupported model-data partition %q", partition)
	}
}

func nextLabel(source CandidateLabelSource, previous int64, havePrevious bool) (tradespeclabel.Row, bool, int64, error) {
	label, ok, err := source.NextLabel()
	if err != nil || !ok {
		return label, ok, previous, err
	}
	if havePrevious && label.DecisionTimestampMs <= previous {
		return label, false, previous, fmt.Errorf("label timestamps are not strictly increasing: previous=%d current=%d", previous, label.DecisionTimestampMs)
	}
	return label, true, label.DecisionTimestampMs, nil
}

type featureV1ParquetSource struct {
	paths  []string
	index  int
	file   *os.File
	reader *parquet.GenericReader[mainfeature.MainFeaturesV1]
	batch  []mainfeature.MainFeaturesV1
	at, n  int
	eof    bool
}

func NewFeatureV1ParquetSource(paths []string) FeatureSource {
	return &featureV1ParquetSource{paths: append([]string(nil), paths...), batch: make([]mainfeature.MainFeaturesV1, 2048)}
}

func (s *featureV1ParquetSource) NextFeature() (FeatureRecord, bool, error) {
	for {
		if s.at < s.n {
			row := s.batch[s.at]
			s.at++
			record, err := FeatureRecordFromV1(row)
			return record, err == nil, err
		}
		if s.reader != nil && s.eof {
			if err := s.closeCurrent(); err != nil {
				return FeatureRecord{}, false, err
			}
		}
		if s.reader == nil {
			if s.index == len(s.paths) {
				return FeatureRecord{}, false, nil
			}
			file, err := os.Open(s.paths[s.index])
			if err != nil {
				return FeatureRecord{}, false, err
			}
			s.index++
			s.file, s.reader, s.eof = file, parquet.NewGenericReader[mainfeature.MainFeaturesV1](file), false
		}
		n, err := s.reader.Read(s.batch)
		s.at, s.n, s.eof = 0, n, errors.Is(err, io.EOF)
		if err != nil && !s.eof {
			return FeatureRecord{}, false, err
		}
	}
}

func (s *featureV1ParquetSource) Close() error { return s.closeCurrent() }
func (s *featureV1ParquetSource) closeCurrent() error {
	if s.reader == nil {
		return nil
	}
	err := s.reader.Close()
	if closeErr := s.file.Close(); err == nil {
		err = closeErr
	}
	s.reader, s.file, s.n, s.at = nil, nil, 0, 0
	return err
}

type candidateLabelParquetSource struct {
	paths  []string
	index  int
	file   *os.File
	reader *parquet.GenericReader[tradespeclabel.Row]
	batch  []tradespeclabel.Row
	at, n  int
	eof    bool
}

func NewCandidateLabelParquetSource(paths []string) CandidateLabelSource {
	return &candidateLabelParquetSource{paths: append([]string(nil), paths...), batch: make([]tradespeclabel.Row, 2048)}
}

func (s *candidateLabelParquetSource) NextLabel() (tradespeclabel.Row, bool, error) {
	for {
		if s.at < s.n {
			row := s.batch[s.at]
			s.at++
			return row, true, nil
		}
		if s.reader != nil && s.eof {
			if err := s.closeCurrent(); err != nil {
				return tradespeclabel.Row{}, false, err
			}
		}
		if s.reader == nil {
			if s.index == len(s.paths) {
				return tradespeclabel.Row{}, false, nil
			}
			file, err := os.Open(s.paths[s.index])
			if err != nil {
				return tradespeclabel.Row{}, false, err
			}
			s.index++
			s.file, s.reader, s.eof = file, parquet.NewGenericReader[tradespeclabel.Row](file), false
		}
		n, err := s.reader.Read(s.batch)
		s.at, s.n, s.eof = 0, n, errors.Is(err, io.EOF)
		if err != nil && !s.eof {
			return tradespeclabel.Row{}, false, err
		}
	}
}

func (s *candidateLabelParquetSource) Close() error { return s.closeCurrent() }
func (s *candidateLabelParquetSource) closeCurrent() error {
	if s.reader == nil {
		return nil
	}
	err := s.reader.Close()
	if closeErr := s.file.Close(); err == nil {
		err = closeErr
	}
	s.reader, s.file, s.n, s.at = nil, nil, 0, 0
	return err
}
