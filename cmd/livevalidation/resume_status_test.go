package main

import "testing"

func TestResumeCheckpointEvidenceMustBeCompletePass(t *testing.T) {
	valid := map[string]any{"Status": "PASS", "Complete": true}
	if checkpointStatus(valid) != "PASS" || !checkpointComplete(valid) {
		t.Fatal("valid checkpoint evidence rejected")
	}
	for _, invalid := range []map[string]any{
		{"Status": "FAIL", "Complete": true},
		{"Status": "PASS", "Complete": false},
		{"status": "NOT_VERIFIED", "complete": true},
	} {
		if checkpointStatus(invalid) == "PASS" && checkpointComplete(invalid) {
			t.Fatalf("invalid checkpoint evidence accepted: %+v", invalid)
		}
	}
}
