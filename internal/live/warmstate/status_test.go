package warmstate

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestStaleCheckpointNeverReady(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	path := filepath.Join(t.TempDir(), "capture-state.json")
	for _, x := range []struct {
		name           string
		age, available int64
		ready, want    bool
	}{
		{"old-nearly-complete", 86_400_000, 14_398_000, false, false},
		{"old-complete", 86_400_000, 14_400_000, true, false},
		{"fresh-complete", 1_000, 14_400_000, true, true},
	} {
		t.Run(x.name, func(t *testing.T) {
			body := []byte(`{"RequiredWarmupMs":14400000,"AvailableContiguousHistoryMs":` + number(x.available) + `,"LastUpdatedMs":` + number(now.UnixMilli()-x.age) + `,"WarmupReady":` + boolean(x.ready) + `}`)
			if err := os.WriteFile(path, body, 0o600); err != nil {
				t.Fatal(err)
			}
			got := Read(path, now)
			if got.Ready != x.want {
				t.Fatalf("status=%+v", got)
			}
		})
	}
}

func number(v int64) string { return strconv.FormatInt(v, 10) }
func boolean(v bool) string { return strconv.FormatBool(v) }
