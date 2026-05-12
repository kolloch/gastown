package daemon

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	beadsdk "github.com/steveyegge/beads"
	"github.com/steveyegge/gastown/internal/deacon"
	"github.com/steveyegge/gastown/internal/tmux"
)

// TestCheckDeaconHeartbeat_HeartbeatSource is a regression test for hq-iju6g.
// It verifies that the daemon's stuck-deacon detector reads
// deacon/heartbeat.json (authoritative) and NOT deacon/state.json
// (which carries a `last_patrol` field that can stall independently of
// deacon liveness, e.g. when the mol-deacon-patrol convoy is stranded).
//
// Scenarios:
//   - fresh heartbeat → no escalation, no nudge, no kill
//   - stale heartbeat (in 5-20m band) → nudge fires (when work is in flight)
//   - missing heartbeat → no kill while Deacon is "freshly started" (grace
//     period unset means we hit the no-tracking branch and quietly return)
//   - stale state.json + fresh heartbeat → no escalation (state.json must
//     not influence the decision)
func TestCheckDeaconHeartbeat_HeartbeatSource(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skipping on Windows — fake tmux requires bash")
	}

	tests := []struct {
		name             string
		heartbeatAge     time.Duration // <0 means no heartbeat file
		writeStaleState  bool          // also write deacon/state.json with stale last_patrol
		activeWork       bool          // simulate in_progress beads in store
		wantStaleLog     bool          // "Deacon heartbeat is stale" log line
		wantNudgeLog     bool          // "nudging session" log line
		wantKillLog      bool          // "STUCK DEACON" log line
	}{
		{
			name:         "fresh heartbeat → quiet",
			heartbeatAge: 1 * time.Minute,
			activeWork:   true,
			wantStaleLog: false,
			wantNudgeLog: false,
			wantKillLog:  false,
		},
		{
			name:         "stale heartbeat (10m) with active work → nudge",
			heartbeatAge: 10 * time.Minute,
			activeWork:   true,
			wantStaleLog: true,
			wantNudgeLog: true,
			wantKillLog:  false,
		},
		{
			name:         "missing heartbeat (no grace-period tracking) → quiet",
			heartbeatAge: -1,
			activeWork:   true,
			wantStaleLog: false,
			wantNudgeLog: false,
			wantKillLog:  false,
		},
		{
			name:            "stale state.json but fresh heartbeat → quiet (regression: hq-iju6g)",
			heartbeatAge:    1 * time.Minute,
			writeStaleState: true,
			activeWork:      true,
			wantStaleLog:    false,
			wantNudgeLog:    false,
			wantKillLog:     false,
		},
		{
			name:         "very stale heartbeat (>20m) → kill path",
			heartbeatAge: 25 * time.Minute,
			activeWork:   true,
			wantStaleLog: true,
			wantNudgeLog: false,
			wantKillLog:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			townRoot := t.TempDir()
			fakeBinDir := t.TempDir()
			tmuxLog := filepath.Join(t.TempDir(), "tmux.log")
			if err := os.WriteFile(tmuxLog, []byte{}, 0o644); err != nil {
				t.Fatalf("create tmux log: %v", err)
			}

			writeFakeTmuxWithSession(t, fakeBinDir)
			t.Setenv("PATH", fakeBinDir+string(os.PathListSeparator)+os.Getenv("PATH"))
			t.Setenv("TMUX_LOG", tmuxLog)

			if tc.heartbeatAge >= 0 {
				writeDeaconHeartbeat(t, townRoot, tc.heartbeatAge)
			}

			if tc.writeStaleState {
				// Write a deacon/state.json with a stale last_patrol — the
				// daemon MUST NOT read this; it is here to prove the read
				// path is heartbeat.json only.
				stateDir := filepath.Join(townRoot, "deacon")
				if err := os.MkdirAll(stateDir, 0o755); err != nil {
					t.Fatalf("mkdir: %v", err)
				}
				stateJSON := map[string]any{
					"patrol_count":          1,
					"last_patrol":           time.Now().Add(-24 * time.Hour).UTC().Format(time.RFC3339),
					"extraordinary_action":  false,
				}
				data, _ := json.MarshalIndent(stateJSON, "", "  ")
				if err := os.WriteFile(filepath.Join(stateDir, "state.json"), data, 0o644); err != nil {
					t.Fatalf("write state.json: %v", err)
				}
			}

			var stores map[string]beadsdk.Storage
			if tc.activeWork {
				stores = map[string]beadsdk.Storage{
					"hq": &searchStorage{results: map[string][]*beadsdk.Issue{
						"in_progress": {{ID: "sc-active"}},
					}},
				}
			} else {
				stores = map[string]beadsdk.Storage{
					"hq": &searchStorage{results: map[string][]*beadsdk.Issue{}},
				}
			}

			d := &Daemon{
				config:      &Config{TownRoot: townRoot},
				logger:      log.New(io.Discard, "", 0),
				tmux:        tmux.NewTmux(),
				beadsStores: stores,
				ctx:         context.Background(),
			}
			logBuf := &strings.Builder{}
			d.logger = log.New(logBuf, "", 0)

			d.checkDeaconHeartbeat()

			out := logBuf.String()
			gotStale := strings.Contains(out, "Deacon heartbeat is stale")
			gotNudge := strings.Contains(out, "nudging session")
			gotKill := strings.Contains(out, "STUCK DEACON")

			if gotStale != tc.wantStaleLog {
				t.Errorf("stale-log present=%v want=%v\nlog:\n%s", gotStale, tc.wantStaleLog, out)
			}
			if gotNudge != tc.wantNudgeLog {
				t.Errorf("nudge-log present=%v want=%v\nlog:\n%s", gotNudge, tc.wantNudgeLog, out)
			}
			if gotKill != tc.wantKillLog {
				t.Errorf("kill-log present=%v want=%v\nlog:\n%s", gotKill, tc.wantKillLog, out)
			}
		})
	}
}

// TestCheckDeaconHeartbeat_IgnoresStateJSON is a focused regression test:
// only a stale state.json exists (no heartbeat.json). The daemon must
// treat this as "no heartbeat file" — NOT as a 24h-stale heartbeat — and
// must not emit a STUCK DEACON kill log just because state.json is old.
// (Without a grace period tracking, the function returns quietly when
// heartbeat is missing — proving state.json is never consulted.)
func TestCheckDeaconHeartbeat_IgnoresStateJSON(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skipping on Windows — fake tmux requires bash")
	}

	townRoot := t.TempDir()
	fakeBinDir := t.TempDir()
	tmuxLog := filepath.Join(t.TempDir(), "tmux.log")
	if err := os.WriteFile(tmuxLog, []byte{}, 0o644); err != nil {
		t.Fatalf("create tmux log: %v", err)
	}
	writeFakeTmuxWithSession(t, fakeBinDir)
	t.Setenv("PATH", fakeBinDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("TMUX_LOG", tmuxLog)

	// Write ONLY state.json (no heartbeat.json), with a stale last_patrol.
	stateDir := filepath.Join(townRoot, "deacon")
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	stateJSON := map[string]any{
		"patrol_count":         1,
		"last_patrol":          time.Now().Add(-24 * time.Hour).UTC().Format(time.RFC3339),
		"extraordinary_action": false,
	}
	data, _ := json.MarshalIndent(stateJSON, "", "  ")
	if err := os.WriteFile(filepath.Join(stateDir, "state.json"), data, 0o644); err != nil {
		t.Fatalf("write state.json: %v", err)
	}

	// Sanity check: deacon.ReadHeartbeat returns nil when only state.json exists.
	if hb := deacon.ReadHeartbeat(townRoot); hb != nil {
		t.Fatalf("expected ReadHeartbeat to return nil when only state.json present, got %+v", hb)
	}

	stores := map[string]beadsdk.Storage{
		"hq": &searchStorage{results: map[string][]*beadsdk.Issue{
			"in_progress": {{ID: "sc-active"}},
		}},
	}

	d := &Daemon{
		config:      &Config{TownRoot: townRoot},
		logger:      log.New(io.Discard, "", 0),
		tmux:        tmux.NewTmux(),
		beadsStores: stores,
		ctx:         context.Background(),
	}
	logBuf := &strings.Builder{}
	d.logger = log.New(logBuf, "", 0)

	d.checkDeaconHeartbeat()

	out := logBuf.String()
	if strings.Contains(out, "STUCK DEACON") {
		t.Errorf("daemon must NOT log STUCK DEACON based on state.json alone; got:\n%s", out)
	}
	if strings.Contains(out, "Deacon heartbeat is stale") {
		t.Errorf("daemon must NOT log stale heartbeat based on state.json alone; got:\n%s", out)
	}
}
