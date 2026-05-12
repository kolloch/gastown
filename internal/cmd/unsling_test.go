package cmd

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// setupByBeadTown builds a minimal town root and bd stub on PATH that the
// runUnslingByBead path can drive end-to-end without tmux.
//
// The stub records every invocation to <townRoot>/bd.log and serves:
//   - `bd show <id> --json` → a single-element JSON array with the assignee
//     and status that the test wants the bead to start in.
//   - `bd update ...` → no-op (exit 0). Tests assert against bd.log.
//
// Returns townRoot.
func setupByBeadTown(t *testing.T, prevAssignee, prevStatus string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("skipping on windows: bd stub uses /bin/sh")
	}

	townRoot := t.TempDir()
	// Make this look like a Gas Town root so workspace.FindFromCwd finds it.
	if err := os.MkdirAll(filepath.Join(townRoot, "mayor"), 0o755); err != nil {
		t.Fatalf("mkdir mayor: %v", err)
	}
	if err := os.WriteFile(filepath.Join(townRoot, "mayor", "town.json"), []byte("{}"), 0o644); err != nil {
		t.Fatalf("write town.json: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(townRoot, ".beads"), 0o755); err != nil {
		t.Fatalf("mkdir .beads: %v", err)
	}

	binDir := filepath.Join(townRoot, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("mkdir bin: %v", err)
	}

	logPath := filepath.Join(townRoot, "bd.log")
	// The stub logs every call (so tests can assert on update args) and
	// returns a deterministic JSON document for `bd show`. We strip leading
	// global flags (--allow-stale, --flat) that the beads wrapper may inject
	// so the dispatch case statement still sees the subcommand in $1.
	showJSON := `[{"id":"hq-zombie","title":"zombie task","status":"` + prevStatus + `","assignee":"` + prevAssignee + `","description":""}]`
	bdScript := `#!/bin/sh
echo "$@" >> ` + logPath + `
while [ "$1" = "--allow-stale" ] || [ "$1" = "--flat" ]; do
  shift
done
case "$1" in
  show)
    echo '` + showJSON + `'
    ;;
esac
exit 0
`
	writeBDStub(t, binDir, bdScript, "")
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	// CD into the town root so workspace.FindFromCwd locates it.
	t.Chdir(townRoot)
	return townRoot
}

// TestUnslingByBead_ClearsAssigneeAndStatus is the happy path: a zombie bead
// (status=hooked, assignee=za-rictus) gets cleared without any tmux access.
// We assert (a) the function returns no error, (b) the output mentions the
// "by-bead bypass" log line, (c) the bd stub received an update command.
func TestUnslingByBead_ClearsAssigneeAndStatus(t *testing.T) {
	townRoot := setupByBeadTown(t, "zack/polecats/rictus", "hooked")

	cmd := &cobra.Command{}
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)

	if err := runUnslingByBead(cmd, "hq-zombie", false); err != nil {
		t.Fatalf("runUnslingByBead returned error: %v\nstdout=%s\nstderr=%s", err, stdout.String(), stderr.String())
	}

	// runUnslingByBead writes to stdout via fmt.Printf, not cmd.OutOrStdout(),
	// so we can't capture it from the cobra command. We assert on bd.log
	// instead — that's the load-bearing observable side effect.
	logBytes, err := os.ReadFile(filepath.Join(townRoot, "bd.log"))
	if err != nil {
		t.Fatalf("reading bd.log: %v", err)
	}
	log := string(logBytes)

	// Show must have been called to look up the previous assignee.
	if !strings.Contains(log, "show hq-zombie") {
		t.Errorf("expected bd show in log, got:\n%s", log)
	}
	// Update must clear both status and assignee.
	if !strings.Contains(log, "update hq-zombie") {
		t.Errorf("expected bd update in log, got:\n%s", log)
	}
	if !strings.Contains(log, "--status=open") && !strings.Contains(log, "--status open") {
		t.Errorf("expected --status open in update call, got:\n%s", log)
	}
	// Assignee must be cleared (empty value).
	if !strings.Contains(log, "--assignee=") && !strings.Contains(log, "--assignee ") {
		t.Errorf("expected --assignee in update call, got:\n%s", log)
	}
}

// TestUnslingByBead_EmptyBeadID guards against operators passing --by-bead=""
// (which Cobra accepts as an empty string).
func TestUnslingByBead_EmptyBeadID(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skipping on windows")
	}
	cmd := &cobra.Command{}
	err := runUnslingByBead(cmd, "", false)
	if err == nil {
		t.Fatal("expected error for empty bead ID, got nil")
	}
	if !strings.Contains(err.Error(), "--by-bead") {
		t.Errorf("error should mention --by-bead flag, got: %v", err)
	}
}

// TestUnslingByBead_DryRun verifies --by-bead with --dry-run shows what would
// happen without calling bd update.
func TestUnslingByBead_DryRun(t *testing.T) {
	townRoot := setupByBeadTown(t, "zack/polecats/furiosa", "hooked")

	cmd := &cobra.Command{}
	if err := runUnslingByBead(cmd, "hq-zombie", true); err != nil {
		t.Fatalf("dry-run returned error: %v", err)
	}

	logBytes, _ := os.ReadFile(filepath.Join(townRoot, "bd.log"))
	log := string(logBytes)
	if strings.Contains(log, "update") {
		t.Errorf("dry-run should not invoke bd update, got log:\n%s", log)
	}
}

// TestUnslingFlag_NoTargetWithoutByBead pins down the documented contract:
// without --by-bead, calling `gt unsling <agent>` for a missing tmux session
// must still fail (we don't accidentally silently bypass for normal callers).
// We don't drive runUnslingWith here — that path needs a full session/tmux
// stack — but we verify the flag wiring on the command itself: unslingByBead
// defaults to empty.
func TestUnslingFlag_DefaultsEmpty(t *testing.T) {
	// Reset the package var (it's wired to the flag and may be left over
	// from another test in the same binary).
	unslingByBead = ""

	// Ensure the flag exists on the command.
	f := unslingCmd.Flag("by-bead")
	if f == nil {
		t.Fatal("--by-bead flag missing from unslingCmd")
	}
	if f.DefValue != "" {
		t.Errorf("expected --by-bead default to be empty, got %q", f.DefValue)
	}
}
