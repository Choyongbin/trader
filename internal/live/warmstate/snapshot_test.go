package warmstate

import (
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	mainfeature "binance_trader/internal/feature/main"
	featurev2 "binance_trader/internal/feature/main/v2"
	"binance_trader/internal/market"
)

func TestFiveHourTruncatedHistoryEngineStateExact(t *testing.T) {
	const total, split = 18_050, 18_020
	base := time.Now().UTC().Truncate(time.Second).Add(-total * time.Second).UnixMilli()
	v1a, v1b := mainfeature.NewEngine(), mainfeature.NewEngine()
	v2a, v2b := featurev2.NewStreamingEngine(), featurev2.NewStreamingEngine()
	futures, spot := make([]market.SecondBar, 0, total), make([]market.SecondBar, 0, total)
	metrics := featurev2.MetricsValue{OpenInterest: featurev2.OptionalFloat{Value: 1000, Valid: true}, OpenInterestValue: featurev2.OptionalFloat{Value: 100000, Valid: true}, TopTraderAccountRatio: featurev2.OptionalFloat{Value: 1.2, Valid: true}, TopTraderPositionRatio: featurev2.OptionalFloat{Value: 1.3, Valid: true}, GlobalRatio: featurev2.OptionalFloat{Value: 1.1, Valid: true}, TakerRatio: featurev2.OptionalFloat{Value: 1.05, Valid: true}}
	step := func(i int, v1 *mainfeature.Engine, v2 *featurev2.StreamingEngine, bar, spotBar market.SecondBar) (featurev2.Snapshot, featurev2.Reason, error) {
		ts := bar.TimestampMs
		if i == 0 {
			if err := v2.AddFunding(featurev2.FundingObservation{TimestampMs: base - 8*3_600_000, Rate: .0001}); err != nil {
				return featurev2.Snapshot{}, "", err
			}
			if err := v2.AddFunding(featurev2.FundingObservation{TimestampMs: base - 3_600_000, Rate: .0002}); err != nil {
				return featurev2.Snapshot{}, "", err
			}
		}
		if i%300 == 0 {
			if err := v2.AddMetrics(featurev2.MetricsObservation{TimestampMs: ts, Value: metrics}); err != nil {
				return featurev2.Snapshot{}, "", err
			}
		}
		if i%60 == 0 {
			k := featurev2.KlineObservation{OpenTimeMs: ts - 60_000, CloseTimeMs: ts - 1, Close: 100000}
			for _, add := range []func(featurev2.KlineObservation) error{v2.AddMark, v2.AddIndex, v2.AddPremium} {
				if err := add(k); err != nil {
					return featurev2.Snapshot{}, "", err
				}
			}
		}
		if err := v2.AddSpot(spotBar); err != nil {
			return featurev2.Snapshot{}, "", err
		}
		v1row, err := v1.Add(bar)
		if err != nil {
			return featurev2.Snapshot{}, "", err
		}
		if v1row == nil {
			return featurev2.Snapshot{}, "", nil
		}
		snap, reason, err := v2.Compute(v1row.DecisionTimestampMs, base, *v1row)
		v2.Prune(v1row.DecisionTimestampMs)
		return snap, reason, err
	}
	compared, eligible := 0, 0
	for i := 0; i < total; i++ {
		if i == split {
			s, err := NewSnapshotWithEngines(futures, spot, nil, v1b, v2b, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			if s.Futures[0].TimestampMs <= base {
				t.Fatal("fixture did not truncate source history")
			}
			path := filepath.Join(t.TempDir(), "state.json")
			if _, err = Save(path, s); err != nil {
				t.Fatal(err)
			}
			loaded, err := Load(path)
			if err != nil {
				t.Fatal(err)
			}
			next := base + int64(i)*1000
			v1b, v2b, err = loaded.RestoreEngines(next, next, time.UnixMilli(next))
			if err != nil {
				t.Fatal(err)
			}
		}
		ts := base + int64(i)*1000
		price := 100000 + float64(i%29)*.1
		bar := market.SecondBar{TimestampMs: ts, Open: price - .02, High: price + .05, Low: price - .05, Close: price, VWAP: price - .01, BaseVolume: 1, QuoteVolume: 1000 + float64(i%7), TakerBuyBaseVolume: .6, TakerSellBaseVolume: .4, TakerBuyQuoteVolume: 600, TakerSellQuoteVolume: 400, AggTradeCount: 3, HasTrade: true}
		spotBar := bar
		spotBar.Close += .2
		futures, spot = append(futures, bar), append(spot, spotBar)
		a, ar, ae := step(i, v1a, v2a, bar, spotBar)
		b, br, be := step(i, v1b, v2b, bar, spotBar)
		if ae != nil || be != nil {
			t.Fatalf("compute error at %d: %v/%v", i, ae, be)
		}
		if i < split || a.DecisionTimestampMs == 0 {
			continue
		}
		compared++
		if a.DecisionTimestampMs != b.DecisionTimestampMs || ar != br {
			t.Fatalf("decision/eligibility mismatch at %d: %s/%s", i, ar, br)
		}
		if ar == featurev2.FutureObservation {
			t.Fatal("future observation")
		}
		if ar == featurev2.Eligible {
			eligible++
			for j := range a.Values {
				if math.Float64bits(a.Values[j]) != math.Float64bits(b.Values[j]) {
					t.Fatalf("feature bit mismatch at decision=%d index=%d", a.DecisionTimestampMs, j)
				}
			}
		}
	}
	if compared == 0 || eligible == 0 {
		t.Fatalf("insufficient resumed coverage compared=%d eligible=%d", compared, eligible)
	}
	t.Logf("five-hour truncated-history exact parity decisions=%d eligible=%d feature_count=128", compared, eligible)
}

func TestSnapshotAtomicRestoreAndGapSafety(t *testing.T) {
	start := time.Now().UnixMilli()/1000*1000 - RequiredWarmupMs
	futures, spot := make([]market.SecondBar, 0, 14_400), make([]market.SecondBar, 0, 14_400)
	for i := int64(0); i < 14_400; i++ {
		row := market.SecondBar{TimestampMs: start + i*1000, Open: 100, High: 100, Low: 100, Close: 100, VWAP: 100, HasTrade: true, FirstAggTradeID: i + 1, LastAggTradeID: i + 1}
		futures, spot = append(futures, row), append(spot, row)
	}
	snapshot, err := NewSnapshot(futures, spot, nil, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "current.json")
	if _, err = Save(path, snapshot); err != nil {
		t.Fatal(err)
	}
	restored, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(snapshot, restored) {
		t.Fatal("source-history replay mismatch")
	}
	if err = restored.RestoreAt(start+14_400_000, start+14_400_000); err != nil {
		t.Fatal(err)
	}
	if err = restored.RestoreAt(start+14_401_000, start+14_400_000); err == nil {
		t.Fatal("unverified gap accepted")
	}
	stale := restored
	stale.Futures = append([]market.SecondBar(nil), restored.Futures...)
	stale.Spot = append([]market.SecondBar(nil), restored.Spot...)
	for i := range stale.Futures {
		stale.Futures[i].TimestampMs -= 60_000
	}
	for i := range stale.Spot {
		stale.Spot[i].TimestampMs -= 60_000
	}
	stale.LastEventTimeMs -= 60_000
	if err = stale.RestoreAt(start+14_340_000, start+14_340_000); err == nil {
		t.Fatal("old snapshot accepted as live")
	}
	if snapshot.LastEventTimeMs-snapshot.Futures[0].TimestampMs+1000 != RequiredWarmupMs {
		t.Fatal("four-hour fixture incomplete")
	}
}

func TestSnapshotCorruptionAndHashMismatchRejected(t *testing.T) {
	dir := t.TempDir()
	corrupt := filepath.Join(dir, "corrupt.json")
	if err := os.WriteFile(corrupt, []byte(`{"snapshot_version":`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(corrupt); err == nil {
		t.Fatal("corrupt snapshot accepted")
	}
	mismatch := filepath.Join(dir, "mismatch.json")
	body := `{"snapshot_version":1,"symbol":"BTCUSDT","created_at_ms":1,"last_event_time_ms":1,"feature_registry_hash":"wrong","entry_policy_hash":"4fae120d9d54a5732dbf2009950beba69773bb0a28a7c3915b616e295cd3dd31","risk_policy_hash":"21e3332b5cf286dd39827973a259fbd91dde325ca067f6a067237bb6f24596f7","futures":[{"timestamp_ms":1000,"open":1,"high":1,"low":1,"close":1,"vwap":1}],"spot":[{"timestamp_ms":1000,"open":1,"high":1,"low":1,"close":1,"vwap":1}]}`
	if err := os.WriteFile(mismatch, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(mismatch); err == nil {
		t.Fatal("registry hash mismatch accepted")
	}
}
