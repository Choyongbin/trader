package tradespeclabel

import (
	"fmt"
	"math"
	"os"
	"path/filepath"

	mainsplit "binance_trader/internal/split/main"
	"binance_trader/internal/tradelabel"
)

type AuditExpectation struct {
	Phase9AReportSHA256 string
	FeatureRegistryHash string
	BarrierVersion      int
	MaxDependencyMs     int64
}

type AuditResult struct {
	CandidateID string
	Passed      bool
	Partitions  map[string]Statistics
}

func AuditCandidate(root string, expected AuditExpectation) (AuditResult, error) {
	manifest, err := ReadManifest(filepath.Join(root, "manifest.json"))
	if err != nil {
		return AuditResult{}, err
	}
	if err = validateManifestExpectation(manifest, expected); err != nil {
		return AuditResult{}, err
	}
	if _, err = os.Stat(filepath.Join(root, "final_holdout.parquet")); err == nil {
		return AuditResult{}, fmt.Errorf("Final Holdout artifact is forbidden")
	}
	result := AuditResult{CandidateID: manifest.CandidateID, Partitions: map[string]Statistics{}}
	split := mainsplit.DefaultV1(manifest.MaxLabelDependencyMs, 0)
	for _, item := range []struct {
		name      string
		partition mainsplit.Partition
	}{{"train", mainsplit.Train}, {"validation", mainsplit.Validation}, {"test", mainsplit.Test}} {
		expectedStats, ok := manifest.Partitions[item.name]
		if !ok {
			return result, fmt.Errorf("missing manifest partition %s", item.name)
		}
		acc := &Accumulator{}
		var last int64
		var have bool
		count, err := Read(filepath.Join(root, item.name+".parquet"), func(row Row) error {
			if have && row.DecisionTimestampMs <= last {
				return fmt.Errorf("%s timestamps are not strictly increasing", item.name)
			}
			have, last = true, row.DecisionTimestampMs
			start, end := auditRange(split, item.partition)
			if row.DecisionTimestampMs < start || row.DecisionTimestampMs >= end || row.DecisionTimestampMs+manifest.MaxLabelDependencyMs >= end {
				return fmt.Errorf("%s row violates partition/purge boundary", item.name)
			}
			return acc.Add(row)
		})
		if err != nil {
			return result, err
		}
		stats, err := acc.Finalize()
		if err != nil {
			return result, err
		}
		if count != expectedStats.Included || stats.LabelValidCount != expectedStats.Valid || stats.LabelInvalidCount != expectedStats.Invalid || stats.PositiveCount != expectedStats.Positive || stats.NegativeCount != expectedStats.Negative {
			return result, fmt.Errorf("%s manifest count mismatch", item.name)
		}
		if err = compareAuditStatistics(stats, expectedStats.Statistics); err != nil {
			return result, fmt.Errorf("%s: %w", item.name, err)
		}
		result.Partitions[item.name] = stats
	}
	if manifest.Phase9AReportPath != "" {
		report, err := LoadPhase9AReport(manifest.Phase9AReportPath)
		if err != nil {
			return result, err
		}
		if err = ValidateSHA256(manifest.Phase9AReportSHA256, report.SourceSHA256); err != nil {
			return result, err
		}
		candidate := Phase9ACandidate{SourceCandidateID: manifest.CandidateID, Side: sideFromManifest(manifest.Side), TPBps: manifest.TPBps, SLBps: manifest.SLBps, HorizonSeconds: manifest.HorizonSeconds}
		reference, err := FindPhase9AReference(report, candidate)
		if err != nil {
			return result, err
		}
		actual := CandidateStatistics{Candidate: candidate, SourcePhase9AReportSHA256: manifest.Phase9AReportSHA256, Partitions: map[mainsplit.Partition]Statistics{mainsplit.Train: result.Partitions["train"], mainsplit.Validation: result.Partitions["validation"], mainsplit.Test: result.Partitions["test"]}}
		if _, err = CompareCandidateReproduction(actual, reference, report.SourceSHA256); err != nil {
			return result, err
		}
	}
	result.Passed = true
	return result, nil
}

func sideFromManifest(side string) tradelabel.Side {
	if side == "LONG" {
		return tradelabel.Long
	}
	return tradelabel.Short
}

func VerifyResume(root string, expected AuditExpectation) error {
	if _, err := os.Stat(filepath.Join(root, "manifest.json.tmp")); err == nil {
		return fmt.Errorf("temporary manifest prevents resume")
	}
	manifest, err := ReadManifest(filepath.Join(root, "manifest.json"))
	if err != nil {
		return err
	}
	if !manifest.Complete || manifest.ReproductionStatus != ReproductionPassed {
		return fmt.Errorf("candidate is not complete and reproduced")
	}
	result, err := AuditCandidate(root, expected)
	if err != nil || !result.Passed {
		if err != nil {
			return err
		}
		return fmt.Errorf("independent audit failed")
	}
	return nil
}

func PublishComplete(root string, manifest Manifest, expected AuditExpectation) error {
	if manifest.ReproductionStatus != ReproductionPassed {
		return fmt.Errorf("reproduction must pass before publication")
	}
	manifest.Complete = false
	if err := WriteManifest(filepath.Join(root, "manifest.json"), manifest); err != nil {
		return err
	}
	if _, err := AuditCandidate(root, expected); err != nil {
		return err
	}
	manifest.Complete = true
	return WriteManifest(filepath.Join(root, "manifest.json"), manifest)
}

func validateManifestExpectation(m Manifest, expected AuditExpectation) error {
	if m.CandidateID == "" || (m.Side != "LONG" && m.Side != "SHORT") || m.TPBps <= 0 || m.SLBps <= 0 || m.HorizonSeconds <= 0 || m.MaxLabelDependencyMs < 0 {
		return fmt.Errorf("invalid candidate manifest identity/provenance")
	}
	if expected.Phase9AReportSHA256 != "" && m.Phase9AReportSHA256 != expected.Phase9AReportSHA256 {
		return fmt.Errorf("Phase 9A SHA-256 mismatch")
	}
	if expected.FeatureRegistryHash != "" && m.FeatureRegistryHash != expected.FeatureRegistryHash {
		return fmt.Errorf("feature registry hash mismatch")
	}
	if expected.BarrierVersion != 0 && m.SourceBarrierVersion != expected.BarrierVersion {
		return fmt.Errorf("barrier version mismatch")
	}
	if expected.MaxDependencyMs != 0 && m.MaxLabelDependencyMs != expected.MaxDependencyMs {
		return fmt.Errorf("max label dependency mismatch")
	}
	if m.StatisticsInvariantBroken() {
		return fmt.Errorf("manifest partition statistics invariant failed")
	}
	return nil
}

func (m Manifest) StatisticsInvariantBroken() bool {
	for _, s := range m.Partitions {
		if s.RawDecisions != s.Purged+s.Included || s.Valid != s.Positive+s.Negative {
			return true
		}
	}
	return false
}
func auditRange(d mainsplit.SplitDefinition, p mainsplit.Partition) (int64, int64) {
	if p == mainsplit.Train {
		return d.Train.StartMs, d.Train.EndMs
	}
	if p == mainsplit.Validation {
		return d.Validation.StartMs, d.Validation.EndMs
	}
	return d.Test.StartMs, d.Test.EndMs
}
func compareAuditStatistics(a, b Statistics) error {
	if a.LabelValidCount != b.LabelValidCount || a.LabelInvalidCount != b.LabelInvalidCount || a.PositiveCount != b.PositiveCount || a.NegativeCount != b.NegativeCount || a.TPFirstCount != b.TPFirstCount || a.SLFirstCount != b.SLFirstCount || a.TimeoutCount != b.TimeoutCount || a.EntryReferenceUnavailableCount != b.EntryReferenceUnavailableCount || a.ExitReferenceUnavailableCount != b.ExitReferenceUnavailableCount {
		return fmt.Errorf("statistics count mismatch")
	}
	for _, x := range [][2]float64{{a.MeanNetReturnExFunding, b.MeanNetReturnExFunding}, {a.MedianNetReturnExFunding, b.MedianNetReturnExFunding}} {
		if math.IsNaN(x[0]) || math.IsInf(x[0], 0) || !AlmostEqual(x[0], x[1], ReproductionAbsTolerance, ReproductionRelTolerance) {
			return fmt.Errorf("statistics float mismatch")
		}
	}
	return nil
}
