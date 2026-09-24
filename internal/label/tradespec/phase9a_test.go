package tradespeclabel

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	mainsplit "binance_trader/internal/split/main"
	"binance_trader/internal/tradelabel"
)

func TestLoadPhase9ARealReport(t *testing.T) {
	path := filepath.Join("..", "..", "..", "data", "reports", "research", "tradespec", "main", "v1", "BTCUSDT-tradespec-v1.json")
	report, err := LoadPhase9AReport(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Frozen) != 5 {
		t.Fatalf("frozen total=%d", len(report.Frozen))
	}
	long, short := 0, 0
	for _, reference := range report.Frozen {
		if reference.Candidate.Side == tradelabel.Long {
			long++
		} else if reference.Candidate.Side == tradelabel.Short {
			short++
		}
		for _, partition := range []mainsplit.Partition{mainsplit.Train, mainsplit.Validation, mainsplit.Test} {
			metric, ok := reference.Partitions[partition]
			if !ok || metric.ValidCount == nil || metric.PositiveCount == nil || metric.WinRate == nil || metric.MeanNetReturnExFunding == nil || metric.MedianNetReturnExFunding == nil {
				t.Fatalf("%s %s reference is incomplete: %+v", reference.Candidate.SourceCandidateID, partition, metric)
			}
		}
	}
	if long != 3 || short != 2 {
		t.Fatalf("frozen sides: LONG=%d SHORT=%d", long, short)
	}
	for _, reference := range report.Frozen {
		if !reference.RequiredMetrics.ValidCount || !reference.RequiredMetrics.PositiveCount || !reference.RequiredMetrics.WinRate || !reference.RequiredMetrics.MeanNetReturnExFunding || !reference.RequiredMetrics.MedianNetReturnExFunding {
			t.Fatalf("real report required metric policy is incomplete: %+v", reference.RequiredMetrics)
		}
	}
	if len(report.SourceSHA256) != 64 || report.SourceSHA256 != strings.ToLower(report.SourceSHA256) {
		t.Fatalf("invalid SHA-256 format %q", report.SourceSHA256)
	}
	if _, err := hex.DecodeString(report.SourceSHA256); err != nil {
		t.Fatal(err)
	}
	again, err := SHA256File(path)
	if err != nil || again != report.SourceSHA256 {
		t.Fatalf("non-deterministic report SHA-256: %q %v", again, err)
	}
}

func TestParsePhase9AReportValidation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{name: "missing frozen shortlist", mutate: func(doc map[string]any) { doc["long_shortlist_frozen"] = nil }},
		{name: "wrong candidate count", mutate: func(doc map[string]any) { doc["short_shortlist_frozen"] = []any{"tp75_sl25_h900"} }},
		{name: "duplicate candidate", mutate: func(doc map[string]any) {
			doc["long_shortlist_frozen"] = []any{"tp100_sl100_h14400", "tp100_sl100_h14400", "tp50_sl50_h14400"}
		}},
		{name: "unknown side", mutate: func(doc map[string]any) { rowByID(doc, "train", "tp100_sl100_h14400", "LONG")["side"] = "UNKNOWN" }},
		{name: "invalid TP", mutate: func(doc map[string]any) {
			rowByID(doc, "train", "tp100_sl100_h14400", "LONG")["spec"].(map[string]any)["tp_bps"] = 0
		}},
		{name: "invalid SL", mutate: func(doc map[string]any) {
			rowByID(doc, "train", "tp100_sl100_h14400", "LONG")["spec"].(map[string]any)["sl_bps"] = 0
		}},
		{name: "invalid horizon", mutate: func(doc map[string]any) {
			rowByID(doc, "train", "tp100_sl100_h14400", "LONG")["spec"].(map[string]any)["horizon_seconds"] = 0
		}},
		{name: "missing candidate spec", mutate: func(doc map[string]any) { delete(rowByID(doc, "train", "tp100_sl100_h14400", "LONG"), "spec") }},
		{name: "expected frozen set mismatch", mutate: mutateExpectedSetMismatch},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := parsePhase9AReport(syntheticReport(t, test.mutate), "synthetic.json"); err == nil {
				t.Fatal("expected parse failure")
			}
		})
	}
}

func TestParsePhase9AReportReferencesAndOptionalMetrics(t *testing.T) {
	report, err := parsePhase9AReport(syntheticReport(t, nil), "synthetic.json")
	if err != nil {
		t.Fatal(err)
	}
	long := report.Frozen[0]
	short := report.Frozen[3]
	if long.Candidate.Side != tradelabel.Long || short.Candidate.Side != tradelabel.Short || *long.Partitions[mainsplit.Train].ValidCount == *short.Partitions[mainsplit.Train].ValidCount {
		t.Fatalf("candidate and partition association is incorrect: long=%+v short=%+v", long, short)
	}

	optional, err := parsePhase9AReport(syntheticReport(t, func(doc map[string]any) {
		delete(rowByID(doc, "test", "tp50_sl25_h900", "SHORT")["metrics"].(map[string]any), "median_net_return_ex_funding")
	}), "synthetic.json")
	if err != nil {
		t.Fatal(err)
	}
	if optional.Frozen[4].Partitions[mainsplit.Test].MedianNetReturnExFunding != nil {
		t.Fatal("missing optional metric must remain unavailable")
	}
}

func TestSHA256Helpers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "abc.txt")
	if err := os.WriteFile(path, []byte("abc"), 0644); err != nil {
		t.Fatal(err)
	}
	got, err := SHA256File(path)
	if err != nil {
		t.Fatal(err)
	}
	const want = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
	if got != want {
		t.Fatalf("SHA-256=%s", got)
	}
	if err := ValidateSHA256(want, got); err != nil {
		t.Fatal(err)
	}
	if err := ValidateSHA256(want, strings.Repeat("0", 64)); err == nil {
		t.Fatal("expected SHA-256 mismatch")
	}
	if err := ValidateSHA256(strings.ToUpper(want), want); err == nil {
		t.Fatal("expected uppercase SHA-256 rejection")
	}
}

func syntheticReport(t *testing.T, mutate func(map[string]any)) []byte {
	t.Helper()
	type candidate struct {
		id      string
		side    string
		tp, sl  int
		horizon int
	}
	candidates := []candidate{
		{"tp100_sl100_h14400", "LONG", 100, 100, 14400},
		{"tp75_sl50_h14400", "LONG", 75, 50, 14400},
		{"tp50_sl50_h14400", "LONG", 50, 50, 14400},
		{"tp75_sl25_h900", "SHORT", 75, 25, 900},
		{"tp50_sl25_h900", "SHORT", 50, 25, 900},
	}
	document := map[string]any{
		"long_shortlist_frozen":  []any{candidates[0].id, candidates[1].id, candidates[2].id},
		"short_shortlist_frozen": []any{candidates[3].id, candidates[4].id},
	}
	for partitionIndex, partition := range []string{"train", "validation", "test"} {
		rows := make([]any, 0, len(candidates))
		for index, candidate := range candidates {
			base := 1000*partitionIndex + 10*index
			rows = append(rows, map[string]any{
				"id":   candidate.id,
				"side": candidate.side,
				"spec": map[string]any{"tp_bps": candidate.tp, "sl_bps": candidate.sl, "horizon_seconds": candidate.horizon},
				"metrics": map[string]any{
					"label_valid":                  base + 1,
					"net_profitable_ex_funding":    base + 2,
					"win_rate":                     float64(base+3) / 10000,
					"mean_net_return_ex_funding":   float64(base+4) / 100000,
					"median_net_return_ex_funding": float64(base+5) / 100000,
				},
			})
		}
		document[partition] = rows
	}
	if mutate != nil {
		mutate(document)
	}
	bytes, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return bytes
}

func rowByID(document map[string]any, partition, id, side string) map[string]any {
	for _, raw := range document[partition].([]any) {
		row := raw.(map[string]any)
		if row["id"] == id && row["side"] == side {
			return row
		}
	}
	panic("synthetic row not found")
}

func mutateExpectedSetMismatch(document map[string]any) {
	for _, partition := range []string{"train", "validation", "test"} {
		row := rowByID(document, partition, "tp100_sl100_h14400", "LONG")
		row["id"] = "tp125_sl100_h14400"
		row["spec"].(map[string]any)["tp_bps"] = 125
	}
	document["long_shortlist_frozen"] = []any{"tp125_sl100_h14400", "tp75_sl50_h14400", "tp50_sl50_h14400"}
}
