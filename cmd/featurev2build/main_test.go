package main

import (
	"os"
	"path/filepath"
	"testing"

	mainfeature "binance_trader/internal/feature/main"
)

func TestConsumeV1PropagatesReaderError(t *testing.T) {
	const decision = int64(12345)
	c := &cursor[mainfeature.MainFeaturesV1]{
		stream:  newRowStream[mainfeature.MainFeaturesV1]([]string{filepath.Join(t.TempDir(), "missing.parquet")}),
		current: mainfeature.MainFeaturesV1{DecisionTimestampMs: decision},
		has:     true,
	}
	if err := consumeV1(c, decision); err == nil {
		t.Fatal("reused-month V1 read error was discarded")
	}
}

func TestTempArtifactsBlockFinalPublicationPrecondition(t *testing.T) {
	root := t.TempDir()
	month := filepath.Join(root, "TRAIN")
	if err := os.MkdirAll(month, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(month, "partial.tmp"), []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ensureNoTempArtifacts(root); err == nil {
		t.Fatal("tmp artifact did not block final publication")
	}
	if err := os.Remove(filepath.Join(month, "partial.tmp")); err != nil {
		t.Fatal(err)
	}
	if err := ensureNoTempArtifacts(root); err != nil {
		t.Fatal(err)
	}
}
