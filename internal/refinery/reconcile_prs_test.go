package refinery

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/steveyegge/gastown/internal/beads"
)

func TestExtractBeadIDs(t *testing.T) {
	cases := []struct {
		name   string
		inputs []string
		want   []string
	}{
		{
			name:   "PR 44 real-world title + headRef",
			inputs: []string{"test(directories): quarantine flaky test_detect_workspace (za-w0rw)", "refinery-hotpatch-za-w0rw"},
			want:   []string{"za-w0rw"},
		},
		{
			name:   "title plus body with multiple references",
			inputs: []string{"fix(refinery): something (hq-k6oai)", "Refs: gt-abcde and also za-w0rw"},
			want:   []string{"gt-abcde", "hq-k6oai", "za-w0rw"},
		},
		{
			name:   "ignores uppercase prefixes like PR-44",
			inputs: []string{"PR-44 closes ABC-123"},
			want:   nil,
		},
		{
			name:   "rejects id segments that are too short",
			inputs: []string{"v1-x", "ra-12"},
			want:   nil,
		},
		{
			name:   "deduplicates across multiple inputs",
			inputs: []string{"gt-abcde", "gt-abcde mentioned again"},
			want:   []string{"gt-abcde"},
		},
		{
			name:   "empty inputs yield nil",
			inputs: []string{"", ""},
			want:   nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ExtractBeadIDs(tc.inputs...)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("ExtractBeadIDs(%v) = %v, want %v", tc.inputs, got, tc.want)
			}
		})
	}
}

// fakeLister is a deterministic in-memory PRLister for tests.
type fakeLister struct {
	prs []PRListEntry
	err error
}

func (l *fakeLister) ListMergedPRs(_ context.Context, _ string, _ time.Time, _ int) ([]PRListEntry, error) {
	if l.err != nil {
		return nil, l.err
	}
	return l.prs, nil
}

// fakeBeads records Show/ForceCloseWithReason calls and lets the test
// program the responses.
type fakeBeads struct {
	issues       map[string]*beads.Issue
	showErrs     map[string]error
	closeErrs    map[string]error
	closedWith   map[string]string // beadID -> reason
	forceClosed  []string
	showRequests []string
}

func (f *fakeBeads) Show(id string) (*beads.Issue, error) {
	f.showRequests = append(f.showRequests, id)
	if err, ok := f.showErrs[id]; ok {
		return nil, err
	}
	if i, ok := f.issues[id]; ok {
		// Return a copy so the reconciler can't mutate our fixture.
		c := *i
		return &c, nil
	}
	return nil, errors.New("not found")
}

func (f *fakeBeads) ForceCloseWithReason(reason string, ids ...string) error {
	for _, id := range ids {
		if err, ok := f.closeErrs[id]; ok {
			return err
		}
		if f.closedWith == nil {
			f.closedWith = map[string]string{}
		}
		f.closedWith[id] = reason
		f.forceClosed = append(f.forceClosed, id)
		// Update in-memory state so a follow-up Show would see it closed.
		if i, ok := f.issues[id]; ok {
			i.Status = string(beads.StatusClosed)
		}
	}
	return nil
}

func TestReconcileMergedPRs_ClosesOpenBead(t *testing.T) {
	// The scenario from PR #44: PR title and headRefName both contain the
	// bead ID; the bead is still open; the reconciler must close it.
	lister := &fakeLister{prs: []PRListEntry{{
		Number:      44,
		Title:       "test(directories): quarantine flaky test_detect_workspace (za-w0rw)",
		Body:        "Quarantines test_detect_workspace. Refs za-w0rw.",
		HeadRefName: "refinery-hotpatch-za-w0rw",
		MergedAt:    time.Now(),
		URL:         "https://github.com/kolloch/zack-gt/pull/44",
	}}}
	fb := &fakeBeads{issues: map[string]*beads.Issue{
		"za-w0rw": {ID: "za-w0rw", Status: string(beads.StatusOpen)},
	}}

	var buf bytes.Buffer
	res, err := reconcileMergedPRsWith(context.Background(), lister, fb, "main", time.Time{}, 10, &buf, t.TempDir(), "zack", nil)
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}
	if res.PRsScanned != 1 {
		t.Errorf("PRsScanned = %d, want 1", res.PRsScanned)
	}
	if len(res.Closed) != 1 || res.Closed[0].BeadID != "za-w0rw" || res.Closed[0].PRNumber != 44 {
		t.Errorf("Closed = %+v, want exactly za-w0rw via PR 44", res.Closed)
	}
	if len(fb.forceClosed) != 1 || fb.forceClosed[0] != "za-w0rw" {
		t.Errorf("ForceClose calls = %v, want [za-w0rw]", fb.forceClosed)
	}
	if got := fb.closedWith["za-w0rw"]; got == "" {
		t.Errorf("close reason for za-w0rw not recorded")
	}
	if res.AlreadyClosed != 0 {
		t.Errorf("AlreadyClosed = %d, want 0", res.AlreadyClosed)
	}
	if len(res.Failures) != 0 {
		t.Errorf("Failures = %+v, want none", res.Failures)
	}
}

func TestReconcileMergedPRs_SkipsAlreadyClosed(t *testing.T) {
	// Steady-state case: refinery merged the PR itself and HandleMRInfoSuccess
	// already closed the bead. The reconciler must NOT double-close.
	lister := &fakeLister{prs: []PRListEntry{{
		Number: 7, Title: "feat: add thing (gt-zzzzz)", MergedAt: time.Now(),
	}}}
	fb := &fakeBeads{issues: map[string]*beads.Issue{
		"gt-zzzzz": {ID: "gt-zzzzz", Status: string(beads.StatusClosed)},
	}}

	res, err := reconcileMergedPRsWith(context.Background(), lister, fb, "main", time.Time{}, 10, nil, t.TempDir(), "gastown", nil)
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}
	if len(fb.forceClosed) != 0 {
		t.Errorf("ForceClose was called %v times, want 0", fb.forceClosed)
	}
	if res.AlreadyClosed != 1 {
		t.Errorf("AlreadyClosed = %d, want 1", res.AlreadyClosed)
	}
	if len(res.Closed) != 0 {
		t.Errorf("Closed = %+v, want none", res.Closed)
	}
}

func TestReconcileMergedPRs_CloseFailureSurfaces(t *testing.T) {
	// If the close itself fails, we must record a failure AND keep going.
	// The mayor-notify is a fire-and-forget shell-out so we just assert it
	// doesn't blow up the pass.
	lister := &fakeLister{prs: []PRListEntry{{
		Number: 99, Title: "fix: weird thing (gt-broke)", MergedAt: time.Now(),
	}}}
	fb := &fakeBeads{
		issues:    map[string]*beads.Issue{"gt-broke": {ID: "gt-broke", Status: string(beads.StatusOpen)}},
		closeErrs: map[string]error{"gt-broke": errors.New("dolt unreachable")},
	}

	res, err := reconcileMergedPRsWith(context.Background(), lister, fb, "main", time.Time{}, 10, nil, t.TempDir(), "gastown", nil)
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}
	if len(res.Failures) != 1 || res.Failures[0].BeadID != "gt-broke" {
		t.Errorf("Failures = %+v, want one for gt-broke", res.Failures)
	}
	if len(res.Closed) != 0 {
		t.Errorf("Closed = %+v, want none", res.Closed)
	}
}

func TestReconcileMergedPRs_NoBeadIDsInPR(t *testing.T) {
	// A PR with no bead IDs anywhere is a no-op. Many GitHub PRs (e.g. from
	// dependabot) have no bead reference at all.
	lister := &fakeLister{prs: []PRListEntry{{
		Number: 1, Title: "chore: bump deps", Body: "Bumps foo from 1 to 2", HeadRefName: "dependabot/npm/foo",
		MergedAt: time.Now(),
	}}}
	fb := &fakeBeads{}

	res, err := reconcileMergedPRsWith(context.Background(), lister, fb, "main", time.Time{}, 10, nil, t.TempDir(), "gastown", nil)
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}
	if len(fb.showRequests) != 0 {
		t.Errorf("Show was called %v times, want 0", fb.showRequests)
	}
	if len(res.Closed)+len(res.Failures) != 0 || res.AlreadyClosed != 0 {
		t.Errorf("expected no-op, got %+v", res)
	}
}

func TestReconcileMergedPRs_ShowErrorIsSwallowed(t *testing.T) {
	// The bead-id regex matches a few false positives by design. When Show
	// fails (e.g. "not found"), we skip the ID without escalating.
	lister := &fakeLister{prs: []PRListEntry{{
		Number: 2, Title: "fix: cross references gt-abcde and not-a-bead", MergedAt: time.Now(),
	}}}
	fb := &fakeBeads{
		issues:   map[string]*beads.Issue{"gt-abcde": {ID: "gt-abcde", Status: string(beads.StatusOpen)}},
		showErrs: map[string]error{"not-a-bead": errors.New("not found")},
	}

	var buf bytes.Buffer
	res, err := reconcileMergedPRsWith(context.Background(), lister, fb, "main", time.Time{}, 10, &buf, t.TempDir(), "gastown", nil)
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}
	if len(res.Closed) != 1 || res.Closed[0].BeadID != "gt-abcde" {
		t.Errorf("Closed = %+v, want exactly gt-abcde", res.Closed)
	}
	if len(res.Failures) != 0 {
		t.Errorf("Failures = %+v, want none (show errors are not failures)", res.Failures)
	}
}

func TestReconcileMergedPRs_ListErrorPropagates(t *testing.T) {
	lister := &fakeLister{err: errors.New("gh down")}
	fb := &fakeBeads{}
	_, err := reconcileMergedPRsWith(context.Background(), lister, fb, "main", time.Time{}, 10, nil, t.TempDir(), "gastown", nil)
	if err == nil {
		t.Fatal("expected error when lister fails, got nil")
	}
}
