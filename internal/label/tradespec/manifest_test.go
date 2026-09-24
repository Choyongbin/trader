package tradespeclabel

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestManifestPersistsCompleteStatistics(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.json")
	want := Manifest{CandidateLabelVersion: Version, CandidateID: "tp50_sl25_h900", Phase9AReportSHA256: strings.Repeat("a", 64), ReproductionStatus: ReproductionNotChecked, Partitions: map[string]PartitionStats{
		"train": {Statistics: Statistics{RawDecisionCount: 11, PurgedCount: 1, IncludedCount: 10, LabelValidCount: 8, LabelInvalidCount: 2, MedianNetReturnExFunding: .0125}},
	}}
	if err := WriteManifest(path, want); err != nil {
		t.Fatal(err)
	}
	got, err := ReadManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	stats := got.Partitions["train"].Statistics
	if stats.RawDecisionCount != 11 || stats.PurgedCount != 1 || stats.IncludedCount != 10 || stats.LabelValidCount != 8 || stats.LabelInvalidCount != 2 || stats.MedianNetReturnExFunding != .0125 {
		t.Fatalf("complete statistics did not round-trip: %+v", stats)
	}
	if got.ReproductionStatus != ReproductionNotChecked {
		t.Fatalf("unexpected reproduction status %q", got.ReproductionStatus)
	}
	bytes, err := os.ReadFile(path)
	if err != nil || !strings.Contains(string(bytes), `"source_phase9a_report_sha256"`) || !strings.Contains(string(bytes), `"reproduction_status"`) || strings.Contains(string(bytes), `"Phase9AReportSHA256"`) {
		t.Fatalf("unexpected provenance JSON field: %s %v", bytes, err)
	}
}

func TestManifestReproductionStatusesRoundTrip(t *testing.T) {
	for _, status := range []ReproductionStatus{ReproductionNotChecked, ReproductionPassed, ReproductionFailed} {
		t.Run(string(status), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "manifest.json")
			if err := WriteManifest(path, Manifest{ReproductionStatus: status, Complete: false}); err != nil {
				t.Fatal(err)
			}
			got, err := ReadManifest(path)
			if err != nil || got.ReproductionStatus != status || got.Complete {
				t.Fatalf("manifest=%+v err=%v", got, err)
			}
		})
	}
}

func TestManifestDefaultsReproductionToNotChecked(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.json")
	if err := WriteManifest(path, Manifest{}); err != nil {
		t.Fatal(err)
	}
	got, err := ReadManifest(path)
	if err != nil || got.ReproductionStatus != ReproductionNotChecked {
		t.Fatalf("manifest=%+v err=%v", got, err)
	}
}

func TestManifestRejectsCompleteWithoutPassedReproduction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.json")
	if err := WriteManifest(path, Manifest{Complete: true, ReproductionStatus: ReproductionFailed}); err == nil {
		t.Fatal("expected complete manifest rejection after reproduction failure")
	}
	if err := WriteManifest(path, Manifest{Complete: true, ReproductionStatus: ReproductionNotChecked}); err == nil {
		t.Fatal("expected complete manifest rejection before reproduction check")
	}
}
