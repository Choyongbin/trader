package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	labels "binance_trader/internal/label/tradespec"
)

func main() {
	root := flag.String("root", `./data/labels/tradespec/v1`, "candidate label root")
	flag.Parse()
	entries, err := os.ReadDir(*root)
	if os.IsNotExist(err) || (err == nil && len(entries) == 0) {
		fmt.Println("NO FINAL CANDIDATE ARTIFACTS\nPhase 9B INCOMPLETE")
		return
	}
	if err != nil {
		panic(err)
	}
	found := 0
	err = filepath.WalkDir(*root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() || entry.Name() != "manifest.json" {
			return walkErr
		}
		found++
		_, err := labels.AuditCandidate(filepath.Dir(path), labels.AuditExpectation{})
		if err != nil {
			return err
		}
		fmt.Println("AUDIT PASS", filepath.Dir(path))
		return nil
	})
	if err != nil {
		panic(err)
	}
	if found == 0 {
		fmt.Println("NO FINAL CANDIDATE ARTIFACTS\nPhase 9B INCOMPLETE")
	}
}
