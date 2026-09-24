package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestOpsAtomicReplacementAndCompletedCheckpointReuse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stage.json")
	if err := writeOpsAtomic(path, map[string]any{"version": 1, "status": "INCOMPLETE", "complete": false}, true); err != nil {
		t.Fatal(err)
	}
	if err := writeOpsAtomic(path, map[string]any{"version": 1, "status": "PASS", "complete": true}, true); err != nil {
		t.Fatal(err)
	}
	if err := writeOpsAtomic(path, map[string]any{"version": 1, "status": "FAIL", "complete": false}, true); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Status   string `json:"status"`
		Complete bool   `json:"complete"`
	}
	if json.Unmarshal(body, &got) != nil || got.Status != "PASS" || !got.Complete {
		t.Fatalf("complete checkpoint mutated: %s", body)
	}
	if entries, err := filepath.Glob(filepath.Join(filepath.Dir(path), "*.tmp")); err != nil || len(entries) != 0 {
		t.Fatalf("tmp remains: %v %v", entries, err)
	}
}
