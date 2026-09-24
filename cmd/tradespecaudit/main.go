package main

import (
	research "binance_trader/internal/research/tradespec"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
)

func main() {
	p := flag.String("report", `.\data\reports\research\tradespec\main\v1\BTCUSDT-tradespec-v1.json`, "report")
	flag.Parse()
	b, e := os.ReadFile(*p)
	if e != nil {
		log.Fatal(e)
	}
	var x struct {
		Grid     []research.Spec `json:"grid"`
		GridHash string          `json:"grid_hash"`
		Final    bool            `json:"final_holdout_accessed"`
		Test     bool            `json:"test_used_for_candidate_selection"`
	}
	if e = json.Unmarshal(b, &x); e != nil {
		log.Fatal(e)
	}
	if len(x.Grid) != 28 || research.GridHash(x.Grid) != x.GridHash || x.Final || x.Test {
		log.Fatal("invalid research integrity")
	}
	fmt.Println("PASS tradespec grid/hash, final-holdout guard, and TEST selection isolation")
}
