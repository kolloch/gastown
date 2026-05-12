package refinery

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/util"
)

// PR auto-close reconciliation (hq-k6oai)
//
// Why this exists:
//
// The primary auto-close-on-PR path is wired through the refinery's own merge
// pipeline: Engineer.HandleMRInfoSuccess (engineer.go) and Manager.PostMerge
// (manager.go) read an MR bead's SourceIssue field and close it after the
// merge lands. This works when refinery merges the PR itself via
// merge_strategy=pr.
//
// It does NOT cover the case where a PR referencing a bead is merged
// *outside* the refinery pipeline:
//
//   - A human clicks "Merge" on GitHub.
//   - A "refinery hotpatch" branch is pushed and merged manually
//     (see PR #44 on kolloch/zack-gt for bead za-w0rw — merged by the
//     overseer, never seen by refinery, source bead stayed OPEN ~21h).
//   - Any external contributor PR that lands without going through `gt mq`.
//
// ReconcileMergedPRs closes the gap. It scans recently-merged PRs on the
// rig's default branch, extracts bead IDs from the title / body /
// head_ref_name, and closes any of those beads that are still open. Each
// close + each failure is logged so the next gap surfaces fast.
//
// This is intentionally complementary to the in-pipeline close, not a
// replacement: refinery-driven merges still close beads immediately via
// HandleMRInfoSuccess; the reconciler is the safety net for everything
// else, run on every refinery patrol cycle.

// beadIDPattern matches bead IDs of the form <prefix>-<id> where the prefix
// is 1-6 lowercase letters and the id is at least 3 lowercase letters/digits.
// Word boundaries on both sides keep us from matching substrings of larger
// tokens (e.g. branch names like "feature/some-thing" do not match because
// they contain hyphens and would not satisfy the surrounding boundary).
//
// Real-world examples we want to match:
//   - "gt-abcde"
//   - "za-w0rw"
//   - "hq-k6oai"
//   - "ra-1234"
//
// Counter-examples we deliberately do NOT match:
//   - "PR-44" (uppercase prefix)
//   - "v1-x" (id too short)
//   - "some-thing-here" (hyphen inside a slug, not a bead id)
var beadIDPattern = regexp.MustCompile(`\b([a-z]{1,6}-[a-z0-9]{3,})\b`)

// PRListEntry is one row of `gh pr list ... --json ...` output.
type PRListEntry struct {
	Number      int       `json:"number"`
	Title       string    `json:"title"`
	Body        string    `json:"body"`
	HeadRefName string    `json:"headRefName"`
	MergedAt    time.Time `json:"mergedAt"`
	URL         string    `json:"url"`
}

// PRLister returns recently-merged PRs whose merge commit landed on baseBranch
// since `since`. The default implementation shells out to `gh pr list`.
type PRLister interface {
	ListMergedPRs(ctx context.Context, baseBranch string, since time.Time, limit int) ([]PRListEntry, error)
}

// ghPRLister is the production implementation backed by the gh CLI.
type ghPRLister struct {
	workDir string
}

// ListMergedPRs returns the most recent merged PRs on baseBranch, sorted
// newest-first, filtered to those merged after `since`.
func (l *ghPRLister) ListMergedPRs(ctx context.Context, baseBranch string, since time.Time, limit int) ([]PRListEntry, error) {
	if limit <= 0 {
		limit = 50
	}
	args := []string{
		"pr", "list",
		"--state", "merged",
		"--base", baseBranch,
		"--limit", fmt.Sprintf("%d", limit),
		"--json", "number,title,body,headRefName,mergedAt,url",
	}
	cmd := exec.CommandContext(ctx, "gh", args...)
	cmd.Dir = l.workDir
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("gh pr list: %w", err)
	}
	var prs []PRListEntry
	if err := json.Unmarshal(out, &prs); err != nil {
		return nil, fmt.Errorf("parse gh pr list output: %w", err)
	}
	// gh pr list does not honor a server-side mergedAt filter, so we trim
	// client-side. Empty `since` (zero time) means "no lower bound".
	if since.IsZero() {
		return prs, nil
	}
	filtered := prs[:0]
	for _, p := range prs {
		if !p.MergedAt.Before(since) {
			filtered = append(filtered, p)
		}
	}
	return filtered, nil
}

// ExtractBeadIDs returns the unique bead IDs referenced anywhere in the
// given text fields. IDs are returned sorted for deterministic processing.
//
// Bead IDs are matched by beadIDPattern (see comment there). Duplicates
// across fields are collapsed. The result is always non-nil (possibly empty).
func ExtractBeadIDs(fields ...string) []string {
	seen := map[string]struct{}{}
	for _, f := range fields {
		for _, match := range beadIDPattern.FindAllStringSubmatch(f, -1) {
			id := match[1]
			seen[id] = struct{}{}
		}
	}
	if len(seen) == 0 {
		return nil
	}
	out := make([]string, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// ReconcileBeadsCloser is the subset of *beads.Beads we need for
// reconciliation, kept narrow so tests can substitute a fake.
type ReconcileBeadsCloser interface {
	Show(id string) (*beads.Issue, error)
	ForceCloseWithReason(reason string, ids ...string) error
}

// ReconcileResult summarizes what happened during one reconcile pass.
type ReconcileResult struct {
	// PRsScanned is the number of merged PRs the reconciler examined.
	PRsScanned int

	// Closed is the set of beads that were closed by this pass, with the
	// PR number that triggered the close (for audit logging).
	Closed []ReconcileClosed

	// AlreadyClosed counts beads that were already in a terminal state
	// when the reconciler looked at them — the normal steady-state case.
	AlreadyClosed int

	// Failures are beads that the reconciler attempted to close but could
	// not, paired with the failing PR and the underlying error. Each
	// failure is also surfaced via a mayor nudge (see ReconcileMergedPRs).
	Failures []ReconcileFailure
}

// ReconcileClosed records one successful close action.
type ReconcileClosed struct {
	BeadID   string
	PRNumber int
	PRTitle  string
	Reason   string
}

// ReconcileFailure records one bead the reconciler tried to close but
// couldn't.
type ReconcileFailure struct {
	BeadID   string
	PRNumber int
	Err      error
}

// ReconcileMergedPRs scans recently-merged PRs on baseBranch and closes any
// referenced beads that are still open. Each close emits an info log; each
// failure emits a warning log AND a mayor nudge so the gap is observable.
//
// Window semantics: `since` is a soft lower bound — if the gh CLI returns
// more than `limit` PRs, the reconciler still only inspects the most recent
// `limit`, but already-closed beads are cheap (they short-circuit). Pass a
// generous limit (e.g. 50) and a recent `since` (e.g. last 24h) for a
// patrol-cycle sweep.
func (m *Manager) ReconcileMergedPRs(ctx context.Context, since time.Time, limit int) (*ReconcileResult, error) {
	lister := &ghPRLister{workDir: m.workDir}
	b := beads.New(m.rig.BeadsPath())
	return reconcileMergedPRsWith(ctx, lister, b, m.rig.DefaultBranch(), since, limit, m.output, m.workDir, m.rig.Name)
}

// reconcileMergedPRsWith is the testable core. It is parameterized over the
// PR lister and beads client so unit tests can run without the gh CLI or a
// live beads server. The `mayorNotify` closure is invoked once per failure
// with a human-readable message; the production code wires it to `gt nudge
// mayor/`.
func reconcileMergedPRsWith(
	ctx context.Context,
	lister PRLister,
	b ReconcileBeadsCloser,
	baseBranch string,
	since time.Time,
	limit int,
	out io.Writer,
	workDir string,
	rigName string,
) (*ReconcileResult, error) {
	if out == nil {
		out = io.Discard
	}
	prs, err := lister.ListMergedPRs(ctx, baseBranch, since, limit)
	if err != nil {
		return nil, fmt.Errorf("list merged PRs on %s: %w", baseBranch, err)
	}

	result := &ReconcileResult{PRsScanned: len(prs)}

	for _, pr := range prs {
		ids := ExtractBeadIDs(pr.Title, pr.Body, pr.HeadRefName)
		if len(ids) == 0 {
			continue
		}

		for _, id := range ids {
			issue, showErr := b.Show(id)
			if showErr != nil {
				// Not a real bead ID (or beads server is down for this one).
				// We don't escalate per-ID lookup misses — the regex is
				// deliberately loose and most misses are benign. Only log
				// at debug-ish level so the patrol output stays terse.
				fmt.Fprintf(out, "[ReconcilePRs] PR #%d: skip %s (show failed: %v)\n", pr.Number, id, showErr)
				continue
			}
			if beads.IssueStatus(issue.Status).IsTerminal() {
				result.AlreadyClosed++
				continue
			}

			reason := fmt.Sprintf("Merged via PR #%d (reconciled from %s)", pr.Number, pr.URL)
			if pr.URL == "" {
				reason = fmt.Sprintf("Merged via PR #%d (reconciled)", pr.Number)
			}
			if closeErr := b.ForceCloseWithReason(reason, id); closeErr != nil {
				fmt.Fprintf(out, "[ReconcilePRs] WARNING: PR #%d failed to close bead %s: %v\n", pr.Number, id, closeErr)
				result.Failures = append(result.Failures, ReconcileFailure{
					BeadID:   id,
					PRNumber: pr.Number,
					Err:      closeErr,
				})
				notifyMayor(workDir, fmt.Sprintf(
					"RECONCILE_PR_CLOSE_FAILED: rig=%s bead=%s pr=%d err=%v — manual close required",
					rigName, id, pr.Number, closeErr,
				))
				continue
			}

			fmt.Fprintf(out, "[ReconcilePRs] PR #%d → closed bead %s (was open, primary auto-close path was skipped)\n", pr.Number, id)
			result.Closed = append(result.Closed, ReconcileClosed{
				BeadID:   id,
				PRNumber: pr.Number,
				PRTitle:  pr.Title,
				Reason:   reason,
			})

			// Every successful reconciler close is itself a signal that the
			// primary close path missed. Tell mayor so the human can spot
			// recurring categories (e.g. "all my hotpatch PRs are landing
			// without closing") rather than waiting for the next audit.
			notifyMayor(workDir, fmt.Sprintf(
				"RECONCILE_PR_CLOSED: rig=%s bead=%s pr=%d title=%q — primary auto-close path was skipped",
				rigName, id, pr.Number, truncate(pr.Title, 80),
			))
		}
	}

	return result, nil
}

// notifyMayor sends a non-blocking nudge to the mayor agent. Nudges are
// preferred over mail for routine signals (no permanent Dolt commit) per
// CLAUDE.md's communication-hygiene guidance.
//
// Failures are intentionally swallowed: the caller is reporting an
// observability event and we must never let nudge plumbing break the
// reconcile pass itself.
func notifyMayor(workDir, msg string) {
	cmd := exec.Command("gt", "nudge", "mayor/", msg)
	cmd.Dir = workDir
	util.SetDetachedProcessGroup(cmd)
	_ = cmd.Run()
}

// truncate returns s shortened to max runes with an ellipsis if needed.
func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return strings.TrimSpace(s[:max]) + "…"
}
