package tradespeclabel

import (
	"path/filepath"
	"testing"

	"binance_trader/internal/tradelabel"
)

func TestAuditPublishAndResume(t *testing.T) {
	root, manifest := syntheticCandidate(t)
	if _, err := AuditCandidate(root, AuditExpectation{}); err != nil {
		t.Fatal(err)
	}
	if err := PublishComplete(root, manifest, AuditExpectation{}); err != nil {
		t.Fatal(err)
	}
	if err := VerifyResume(root, AuditExpectation{}); err != nil {
		t.Fatal(err)
	}
}

func TestResumeRejectsPartialAndWrongProvenance(t *testing.T) {
	root, manifest := syntheticCandidate(t)
	if err := VerifyResume(root, AuditExpectation{}); err == nil {
		t.Fatal("complete=false accepted")
	}
	manifest.Complete = true
	manifest.Phase9AReportSHA256 = "wrong"
	if err := WriteManifest(filepath.Join(root, "manifest.json"), manifest); err != nil {
		t.Fatal(err)
	}
	if err := VerifyResume(root, AuditExpectation{Phase9AReportSHA256: "expected"}); err == nil {
		t.Fatal("wrong hash accepted")
	}
	if err := WriteManifest(filepath.Join(root, "manifest.json.tmp"), manifest); err != nil {
		t.Fatal(err)
	}
	if err := VerifyResume(root, AuditExpectation{}); err == nil {
		t.Fatal("tmp manifest accepted")
	}
}

func TestAuditRejectsWrongCountsAndPublicationFailure(t *testing.T) {
	root, manifest := syntheticCandidate(t)
	bad := manifest.Partitions["train"]
	bad.Included++
	manifest.Partitions["train"] = bad
	if err := WriteManifest(filepath.Join(root, "manifest.json"), manifest); err != nil {
		t.Fatal(err)
	}
	if _, err := AuditCandidate(root, AuditExpectation{}); err == nil {
		t.Fatal("wrong row count accepted")
	}
	manifest.ReproductionStatus = ReproductionFailed
	if err := PublishComplete(root, manifest, AuditExpectation{}); err == nil {
		t.Fatal("reproduction failure published")
	}
}

func syntheticCandidate(t *testing.T) (string, Manifest) {
	t.Helper()
	root := t.TempDir()
	rows := map[string][]Row{"train": {{DecisionTimestampMs: 1704067200010, LabelValid: true, NetProfitableExFunding: true, TradeResult: tradelabel.TPFirst, GrossMarketReturn: .01, NetReturnExFunding: .01}, {DecisionTimestampMs: 1704067200020, LabelValid: false, TradeResult: tradelabel.EntryReferenceUnavailable}}, "validation": {{DecisionTimestampMs: 1727740800010, LabelValid: true, TradeResult: tradelabel.SLFirst, NetReturnExFunding: -.01}}, "test": {{DecisionTimestampMs: 1735689600010, LabelValid: true, TradeResult: tradelabel.Timeout, NetReturnExFunding: .002}}}
	parts := map[string]PartitionStats{}
	for name, values := range rows {
		if err := Write(filepath.Join(root, name+".parquet"), values); err != nil {
			t.Fatal(err)
		}
		a := &Accumulator{}
		for _, row := range values {
			if err := a.Add(row); err != nil {
				t.Fatal(err)
			}
		}
		s, err := a.Finalize()
		if err != nil {
			t.Fatal(err)
		}
		parts[name] = PartitionStats{RawDecisions: s.RawDecisionCount, Included: s.IncludedCount, Valid: s.LabelValidCount, Invalid: s.LabelInvalidCount, Positive: s.PositiveCount, Negative: s.NegativeCount, Statistics: s}
	}
	m := Manifest{CandidateLabelVersion: 1, CandidateID: "synthetic", Side: "LONG", TPBps: 1, SLBps: 1, HorizonSeconds: 1, SourceBarrierVersion: 2, MaxLabelDependencyMs: 10, ReproductionStatus: ReproductionPassed, Partitions: parts, Complete: false}
	if err := WriteManifest(filepath.Join(root, "manifest.json"), m); err != nil {
		t.Fatal(err)
	}
	return root, m
}
