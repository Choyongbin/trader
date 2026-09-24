package warmstate

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	mainfeature "binance_trader/internal/feature/main"
	featurev2 "binance_trader/internal/feature/main/v2"
	live "binance_trader/internal/live/binance"
	"binance_trader/internal/market"
)

const (
	SnapshotVersion           = 1
	RuntimeSnapshotPath       = "data/live_state/BTCUSDT/v1/current.json"
	RetentionMs         int64 = 4*60*60*1000 + 20*60*1000
	FeatureRegistryHash       = "a37306b80ecbf701103ab691445d07a39624ce995d84f8206d6314dae3045bef"
	EntryPolicyHash           = "4fae120d9d54a5732dbf2009950beba69773bb0a28a7c3915b616e295cd3dd31"
	RiskPolicyHash            = "21e3332b5cf286dd39827973a259fbd91dde325ca067f6a067237bb6f24596f7"
)

type Snapshot struct {
	Version             int                        `json:"snapshot_version"`
	Symbol              string                     `json:"symbol"`
	CreatedAtMs         int64                      `json:"created_at_ms"`
	LastEventTimeMs     int64                      `json:"last_event_time_ms"`
	FeatureRegistryHash string                     `json:"feature_registry_hash"`
	EntryPolicyHash     string                     `json:"entry_policy_hash"`
	RiskPolicyHash      string                     `json:"risk_policy_hash"`
	Futures             []market.SecondBar         `json:"futures"`
	Spot                []market.SecondBar         `json:"spot"`
	External            []live.ExternalObservation `json:"external"`
	V1State             *mainfeature.EngineState   `json:"v1_engine_state,omitempty"`
	V2State             *featurev2.StreamState     `json:"v2_engine_state,omitempty"`
}

func NewSnapshot(futures, spot []market.SecondBar, external []live.ExternalObservation, now time.Time) (Snapshot, error) {
	if len(futures) == 0 || len(spot) == 0 {
		return Snapshot{}, fmt.Errorf("WARM_STATE_EMPTY")
	}
	last := futures[len(futures)-1].TimestampMs
	if spot[len(spot)-1].TimestampMs < last {
		last = spot[len(spot)-1].TimestampMs
	}
	cutoff := last - RetentionMs
	trim := func(rows []market.SecondBar) []market.SecondBar {
		at := 0
		for at < len(rows) && rows[at].TimestampMs < cutoff {
			at++
		}
		return append([]market.SecondBar(nil), rows[at:]...)
	}
	filteredExternal := make([]live.ExternalObservation, 0, len(external))
	for _, row := range external {
		// Funding can remain usable for 16h; keep its last prior observation.
		if row.SourceTimestampMs >= cutoff-57_600_000 {
			filteredExternal = append(filteredExternal, row)
		}
	}
	s := Snapshot{Version: SnapshotVersion, Symbol: "BTCUSDT", CreatedAtMs: now.UnixMilli(), LastEventTimeMs: last, FeatureRegistryHash: FeatureRegistryHash, EntryPolicyHash: EntryPolicyHash, RiskPolicyHash: RiskPolicyHash, Futures: trim(futures), Spot: trim(spot), External: filteredExternal}
	return s, s.Validate()
}

func NewSnapshotWithEngines(futures, spot []market.SecondBar, external []live.ExternalObservation, v1 *mainfeature.Engine, v2 *featurev2.StreamingEngine, now time.Time) (Snapshot, error) {
	if v1 == nil || v2 == nil {
		return Snapshot{}, fmt.Errorf("WARM_STATE_ENGINE_MISSING")
	}
	s, err := NewSnapshot(futures, spot, external, now)
	if err != nil {
		return Snapshot{}, err
	}
	a, b := v1.ExportState(), v2.ExportState()
	s.V1State, s.V2State = &a, &b
	return s, s.Validate()
}

func (s Snapshot) Validate() error {
	if s.Version != SnapshotVersion || s.Symbol != "BTCUSDT" || s.FeatureRegistryHash != FeatureRegistryHash || s.EntryPolicyHash != EntryPolicyHash || s.RiskPolicyHash != RiskPolicyHash {
		return fmt.Errorf("WARM_STATE_IDENTITY_MISMATCH")
	}
	if s.CreatedAtMs <= 0 || s.LastEventTimeMs <= 0 || len(s.Futures) == 0 || len(s.Spot) == 0 {
		return fmt.Errorf("WARM_STATE_INCOMPLETE")
	}
	for _, rows := range [][]market.SecondBar{s.Futures, s.Spot} {
		for i, row := range rows {
			values := [...]float64{row.Open, row.High, row.Low, row.Close, row.VWAP, row.BaseVolume, row.QuoteVolume, row.TakerBuyBaseVolume, row.TakerSellBaseVolume, row.TakerBuyQuoteVolume, row.TakerSellQuoteVolume}
			valid := row.TimestampMs > 0 && row.TimestampMs%1000 == 0 && row.Close > 0
			for _, value := range values {
				valid = valid && !math.IsNaN(value) && !math.IsInf(value, 0)
			}
			if !valid {
				return fmt.Errorf("WARM_STATE_INVALID_BAR")
			}
			if i > 0 && row.TimestampMs != rows[i-1].TimestampMs+1000 {
				return fmt.Errorf("WARM_STATE_GAP")
			}
		}
		if rows[len(rows)-1].TimestampMs < s.LastEventTimeMs {
			return fmt.Errorf("WARM_STATE_LAST_EVENT_MISMATCH")
		}
	}
	for _, row := range s.External {
		if row.SourceTimestampMs <= 0 || row.ReceiveTimestampMs <= 0 || row.ReceiveTimestampMs > s.CreatedAtMs {
			return fmt.Errorf("WARM_STATE_INVALID_EXTERNAL")
		}
	}
	if (s.V1State == nil) != (s.V2State == nil) {
		return fmt.Errorf("WARM_STATE_ENGINE_PAIR_INCOMPLETE")
	}
	if s.V1State != nil && (s.V1State.LastTimestampMs != s.Futures[len(s.Futures)-1].TimestampMs || s.V2State.LastSpot != s.Spot[len(s.Spot)-1].TimestampMs) {
		return fmt.Errorf("WARM_STATE_ENGINE_CURSOR_MISMATCH")
	}
	return nil
}

func (s Snapshot) RestoreEngines(nextFuturesMs, nextSpotMs int64, now time.Time) (*mainfeature.Engine, *featurev2.StreamingEngine, error) {
	if err := s.RestoreAtTime(nextFuturesMs, nextSpotMs, now); err != nil {
		return nil, nil, err
	}
	if s.V1State == nil || s.V2State == nil {
		return nil, nil, fmt.Errorf("WARM_STATE_ENGINE_STATE_MISSING")
	}
	v1, err := mainfeature.RestoreEngine(*s.V1State)
	if err != nil {
		return nil, nil, err
	}
	v2, err := featurev2.RestoreStreamingEngine(*s.V2State)
	if err != nil {
		return nil, nil, err
	}
	return v1, v2, nil
}

// Continuation requires exact one-second adjacency in both canonical streams.
func (s Snapshot) RestoreAt(nextFuturesMs, nextSpotMs int64) error {
	return s.RestoreAtTime(nextFuturesMs, nextSpotMs, time.Now())
}

// RestoreAtTime verifies logical adjacency and current-time freshness. The
// explicit clock permits deterministic process-restart fixtures.
func (s Snapshot) RestoreAtTime(nextFuturesMs, nextSpotMs int64, now time.Time) error {
	if err := s.Validate(); err != nil {
		return err
	}
	if nextFuturesMs != s.Futures[len(s.Futures)-1].TimestampMs+1000 || nextSpotMs != s.Spot[len(s.Spot)-1].TimestampMs+1000 {
		return fmt.Errorf("WARM_STATE_GAP")
	}
	// Exact historical adjacency cannot turn an old snapshot into current live readiness.
	if now.UnixMilli()-nextFuturesMs > 10_000 || now.UnixMilli()-nextSpotMs > 10_000 || nextFuturesMs > now.UnixMilli()+1000 || nextSpotMs > now.UnixMilli()+1000 {
		return fmt.Errorf("WARM_STATE_STALE")
	}
	return nil
}

func Save(path string, snapshot Snapshot) (string, error) {
	if err := snapshot.Validate(); err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", err
	}
	body, err := json.Marshal(snapshot)
	if err != nil {
		return "", err
	}
	file, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*.tmp")
	if err != nil {
		return "", err
	}
	tmp := file.Name()
	defer os.Remove(tmp)
	if _, err = file.Write(body); err != nil {
		file.Close()
		return "", err
	}
	if err = file.Sync(); err != nil {
		file.Close()
		return "", err
	}
	if err = file.Close(); err != nil {
		return "", err
	}
	check, err := Load(tmp)
	if err != nil || check.LastEventTimeMs != snapshot.LastEventTimeMs {
		return "", fmt.Errorf("WARM_STATE_TMP_AUDIT_FAILED")
	}
	if err = os.Rename(tmp, path); err != nil {
		return "", err
	}
	hash := sha256.Sum256(body)
	return hex.EncodeToString(hash[:]), nil
}

func Load(path string) (Snapshot, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return Snapshot{}, err
	}
	var snapshot Snapshot
	if err = json.Unmarshal(body, &snapshot); err != nil {
		return Snapshot{}, err
	}
	return snapshot, snapshot.Validate()
}
