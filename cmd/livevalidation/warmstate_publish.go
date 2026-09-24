package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"binance_trader/internal/live/warmstate"
)

func publishWarmEngineSnapshot() error {
	data, err := loadWarmupDataset()
	if err != nil {
		return err
	}
	_, v1, v2, err := replayFrozenFeaturesWithEngines(data)
	if err != nil {
		return err
	}
	snapshot, err := warmstate.NewSnapshotWithEngines(data.Futures, data.Spot, data.External, v1, v2, time.Now())
	if err != nil {
		return err
	}
	path := filepath.Join(filepath.FromSlash("data/live_state/BTCUSDT/v1"), fmt.Sprintf("snapshot-%d.json", snapshot.LastEventTimeMs))
	if _, err := os.Stat(path); err == nil {
		old, err := warmstate.Load(path)
		if err != nil || old.V1State == nil || old.V2State == nil || old.LastEventTimeMs != snapshot.LastEventTimeMs {
			return fmt.Errorf("existing warm state snapshot is invalid: %s", path)
		}
		fmt.Printf("WARM STATE SNAPSHOT REUSED last=%d\n", old.LastEventTimeMs)
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	hash, err := warmstate.Save(path, snapshot)
	if err != nil {
		return err
	}
	fmt.Printf("WARM STATE SNAPSHOT PASS bars=%d/%d last=%d sha256=%s\n", len(snapshot.Futures), len(snapshot.Spot), snapshot.LastEventTimeMs, hash)
	return nil
}
