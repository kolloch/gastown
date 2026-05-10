// Package daemon — idle_check.go implements the witness-mediated reaper guard
// (hq-yuje / hq-32ee.A).
//
// Background: The polecat reaper kills sessions whose heartbeat has gone stale
// past `polecat_idle_session_timeout`. In practice, the heartbeat-only signal
// produces false positives:
//   - polecats between mol steps (just submitted an MR, briefly silent),
//   - polecats whose heartbeat machinery fell over but the agent is alive,
//   - polecats stuck in a recoverable state that a NUDGE could unstick.
//
// Before killing, the daemon now consults the rig's witness via mail. The
// witness inspects the polecat (tmux panes, beads, hooks) and writes a verdict
// file the daemon picks up on a subsequent reaper pass.
//
// Protocol (asynchronous, fire-and-forget mail + verdict file):
//
//  1. Daemon sees a candidate-for-reaping polecat. Before killing it calls
//     consultWitnessBeforeReap().
//  2. consultWitnessBeforeReap() checks for an existing verdict file at
//     <TownRoot>/.runtime/idle_check/<rig>__<polecat>.verdict.json.
//     - If a fresh verdict is present, the verdict file is consumed (deleted)
//       and returned.
//     - Otherwise it checks for a pending request marker
//       <TownRoot>/.runtime/idle_check/<rig>__<polecat>.pending.json.
//     - If no pending marker exists, it mails an IDLE_CHECK to <rig>/witness
//       with a structured JSON body (polecat name, idle duration, session,
//       beads context) and writes a pending marker. The reaper defers this
//       cycle (returns VerdictDefer).
//     - If the pending marker is fresh (< maxWait), the reaper defers.
//     - If the pending marker is stale (>= maxWait), the witness failed to
//       respond. We fall through to default behavior (REAP) and clean up the
//       marker so the next stale polecat gets a fresh chance.
//
// Verdicts:
//
//	ALIVE     polecat is between steps; daemon resets its idle assessment
//	          (we simply skip this cycle).
//	RESCUE    daemon sends a NUDGE with witness-supplied diagnostic text.
//	REAP      daemon proceeds with the kill (current default).
//	ESCALATE  critical state mismatch; daemon mails the mayor and does NOT kill.
//	DEFER     no verdict yet (internal-only; caller treats as "skip this cycle").
//
// Why asynchronous and not synchronous?
//
// All existing daemon ↔ witness communication is fire-and-forget mail. The
// reaper runs inside d.rigPool.runPerRig (30s per-task timeout); blocking each
// rig's reaper call for ~120s waiting on a witness response would serialize
// reaping and cause spurious task-timeout failures. The verdict-file pattern
// keeps the reaper non-blocking and lets the witness take as long as its
// patrol cycle needs.
package daemon

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/steveyegge/gastown/internal/util"
)

// IdleCheckVerdict is the witness's decision about a candidate-for-reaping
// polecat. See package doc for semantics.
type IdleCheckVerdict string

const (
	VerdictAlive    IdleCheckVerdict = "ALIVE"
	VerdictRescue   IdleCheckVerdict = "RESCUE"
	VerdictReap     IdleCheckVerdict = "REAP"
	VerdictEscalate IdleCheckVerdict = "ESCALATE"

	// VerdictDefer is internal — emitted by consultWitnessBeforeReap when no
	// witness verdict is yet available. The caller should skip this reaper
	// cycle and revisit on the next pass.
	VerdictDefer IdleCheckVerdict = "DEFER"
)

// idleCheckMaxWait bounds how long we wait for the witness to respond before
// falling through to default REAP. Two patrol cycles (~30s each, plus model
// thinking time) is generous; tune via configuration if necessary.
const idleCheckMaxWait = 5 * time.Minute

// IdleCheckRequest is the JSON payload mailed to the witness as the body of
// an IDLE_CHECK message. The witness echoes the same key set when it writes
// its verdict file.
type IdleCheckRequest struct {
	Schema           string    `json:"schema"`             // always "idle_check_request_v1"
	Rig              string    `json:"rig"`                // rig name, e.g. "myai"
	Polecat          string    `json:"polecat"`            // polecat short name
	Session          string    `json:"session"`            // tmux session name
	IdleDuration     string    `json:"idle_duration"`      // human-readable duration
	IdleSeconds      int64     `json:"idle_seconds"`       // machine-readable duration
	Threshold        string    `json:"threshold"`          // configured idle threshold
	HeartbeatState   string    `json:"heartbeat_state"`    // daemon view of heartbeat state
	HeartbeatStamp   time.Time `json:"heartbeat_stamp"`    // last heartbeat timestamp
	HookBead         string    `json:"hook_bead"`          // current hook_bead, if any
	HookBeadClosed   bool      `json:"hook_bead_closed"`   // whether the hook_bead is closed
	HasAssignedWork  bool      `json:"has_assigned_work"`  // any open work bead with this polecat as assignee
	AgentRunning     bool      `json:"agent_running"`      // d.tmux.IsAgentRunning(session)
	ReapReason       string    `json:"reap_reason"`        // why the daemon thinks this polecat is reapable
	VerdictFile      string    `json:"verdict_file"`       // absolute path the witness should write to
	RequestedAt      time.Time `json:"requested_at"`       // when this request was issued
}

// IdleCheckResponse is the JSON the witness writes into the verdict file. The
// daemon consumes the file on its next reaper pass.
type IdleCheckResponse struct {
	Schema     string           `json:"schema"`              // always "idle_check_response_v1"
	Rig        string           `json:"rig"`
	Polecat    string           `json:"polecat"`
	Verdict    IdleCheckVerdict `json:"verdict"`
	Reason     string           `json:"reason"`              // free-text rationale (witness writes)
	NudgeText  string           `json:"nudge_text,omitempty"`// for RESCUE: text to send via gt nudge
	DecidedAt  time.Time        `json:"decided_at"`
	WitnessSig string           `json:"witness_sig"`         // for audit (e.g. "myai/witness")
}

// idleCheckDir returns the directory where IDLE_CHECK request markers and
// verdict files live.
func idleCheckDir(townRoot string) string {
	return filepath.Join(townRoot, ".runtime", "idle_check")
}

// idleCheckSlot is the filename slug for a polecat's request/verdict pair.
// Using "<rig>__<polecat>" because polecat names may contain dashes but never
// double-underscores.
func idleCheckSlot(rig, polecat string) string {
	return fmt.Sprintf("%s__%s", rig, polecat)
}

// idleCheckPendingPath is the marker file written when the daemon mails the
// witness. Its existence + freshness gates further mails for the same polecat.
func idleCheckPendingPath(townRoot, rig, polecat string) string {
	return filepath.Join(idleCheckDir(townRoot), idleCheckSlot(rig, polecat)+".pending.json")
}

// idleCheckVerdictPath is the file the witness writes its verdict to.
func idleCheckVerdictPath(townRoot, rig, polecat string) string {
	return filepath.Join(idleCheckDir(townRoot), idleCheckSlot(rig, polecat)+".verdict.json")
}

// readVerdict reads and parses a witness verdict file, if present and
// well-formed. Returns (nil, false) if absent or unparseable.
func readVerdict(path string) (*IdleCheckResponse, bool) {
	data, err := os.ReadFile(path) //nolint:gosec // G304: path is daemon-constructed
	if err != nil {
		return nil, false
	}
	var resp IdleCheckResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, false
	}
	return &resp, true
}

// readPending reads and parses an existing pending-request marker, if any.
func readPending(path string) (*IdleCheckRequest, bool) {
	data, err := os.ReadFile(path) //nolint:gosec // G304: path is daemon-constructed
	if err != nil {
		return nil, false
	}
	var req IdleCheckRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, false
	}
	return &req, true
}

// removeFileQuiet deletes a file, swallowing not-found errors.
func removeFileQuiet(path string) {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		// Best-effort; nothing to do if cleanup fails.
		_ = err
	}
}

// consultWitnessBeforeReap is the entry point called by the reaper before
// killing a polecat. See package doc for the full state machine.
//
// rigName/polecatName/sessionName identify the candidate. ctx carries
// additional context the witness needs to decide. The function never blocks
// on the witness — it either returns a fresh verdict (if one is on disk) or
// emits/maintains a pending request and returns VerdictDefer.
//
// On VerdictReap from a stale-pending fall-through, reason will be set so the
// caller can log "witness failed to respond, falling through to REAP".
func (d *Daemon) consultWitnessBeforeReap(rigName, polecatName, sessionName string, ctx IdleCheckContext) (IdleCheckVerdict, string) {
	dir := idleCheckDir(d.config.TownRoot)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		// If we can't even create the state dir, fall through to REAP rather
		// than blocking the reaper indefinitely.
		d.logger.Printf("idle_check: mkdir %s failed: %v — falling through to REAP", dir, err)
		return VerdictReap, "idle_check_state_dir_unavailable"
	}

	verdictPath := idleCheckVerdictPath(d.config.TownRoot, rigName, polecatName)
	pendingPath := idleCheckPendingPath(d.config.TownRoot, rigName, polecatName)

	// (1) If a verdict is on disk, consume it.
	if resp, ok := readVerdict(verdictPath); ok {
		removeFileQuiet(verdictPath)
		removeFileQuiet(pendingPath)
		d.logger.Printf("idle_check: %s/%s verdict=%s reason=%q (witness=%s)",
			rigName, polecatName, resp.Verdict, resp.Reason, resp.WitnessSig)
		// Honor the verdict; for RESCUE, emit a nudge here too.
		if resp.Verdict == VerdictRescue && resp.NudgeText != "" {
			d.sendRescueNudge(rigName, polecatName, sessionName, resp.NudgeText)
		}
		return resp.Verdict, resp.Reason
	}

	// (2) Check for an outstanding pending request.
	if pending, ok := readPending(pendingPath); ok {
		age := time.Since(pending.RequestedAt)
		if age < idleCheckMaxWait {
			// Witness still has time to respond — defer this cycle.
			return VerdictDefer, fmt.Sprintf("awaiting_witness (age=%s)", age.Truncate(time.Second))
		}
		// Witness exceeded the patience budget. Fall through to REAP and
		// clean up so the next polecat (or this one if it survives) gets a
		// fresh shot at consultation.
		removeFileQuiet(pendingPath)
		d.logger.Printf("idle_check: %s/%s witness silent for %v — falling through to REAP",
			rigName, polecatName, age.Truncate(time.Second))
		return VerdictReap, "witness_unresponsive"
	}

	// (3) No verdict, no pending. Issue a fresh IDLE_CHECK and defer.
	req := IdleCheckRequest{
		Schema:          "idle_check_request_v1",
		Rig:             rigName,
		Polecat:         polecatName,
		Session:         sessionName,
		IdleDuration:    ctx.IdleDuration.Truncate(time.Second).String(),
		IdleSeconds:     int64(ctx.IdleDuration.Seconds()),
		Threshold:       ctx.Threshold.String(),
		HeartbeatState:  ctx.HeartbeatState,
		HeartbeatStamp:  ctx.HeartbeatStamp,
		HookBead:        ctx.HookBead,
		HookBeadClosed:  ctx.HookBeadClosed,
		HasAssignedWork: ctx.HasAssignedWork,
		AgentRunning:    ctx.AgentRunning,
		ReapReason:      ctx.ReapReason,
		VerdictFile:     verdictPath,
		RequestedAt:     time.Now().UTC(),
	}
	if err := writeJSONAtomic(pendingPath, req); err != nil {
		d.logger.Printf("idle_check: write pending %s failed: %v — falling through to REAP", pendingPath, err)
		return VerdictReap, "idle_check_pending_write_failed"
	}
	if err := d.mailIdleCheckRequest(rigName, polecatName, req); err != nil {
		// Mail failed — clean up so we retry next cycle.
		removeFileQuiet(pendingPath)
		d.logger.Printf("idle_check: mail to %s/witness failed: %v — falling through to REAP",
			rigName, err)
		return VerdictReap, "idle_check_mail_failed"
	}
	d.logger.Printf("idle_check: %s/%s IDLE_CHECK mailed to %s/witness (idle=%s, reason=%s) — deferring reap",
		rigName, polecatName, rigName, req.IdleDuration, req.ReapReason)
	return VerdictDefer, "idle_check_dispatched"
}

// IdleCheckContext bundles the per-polecat data the daemon already has at
// reap-decision time. Keeping it as a separate struct avoids a sprawling
// argument list and makes future additions cheap.
type IdleCheckContext struct {
	IdleDuration    time.Duration
	Threshold       time.Duration
	HeartbeatState  string
	HeartbeatStamp  time.Time
	HookBead        string
	HookBeadClosed  bool
	HasAssignedWork bool
	AgentRunning    bool
	ReapReason      string
}

// mailIdleCheckRequest ships the IDLE_CHECK request to the rig's witness as
// the JSON body of a gt mail send. We use -m (inline) rather than --stdin to
// match the existing notifyWitnessOf* helpers and avoid stdin plumbing.
func (d *Daemon) mailIdleCheckRequest(rigName, polecatName string, req IdleCheckRequest) error {
	body, err := json.MarshalIndent(req, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	witnessAddr := rigName + "/witness"
	subject := fmt.Sprintf("IDLE_CHECK: %s (idle %s)", polecatName, req.IdleDuration)

	cmd := exec.Command(d.gtPath, "mail", "send", witnessAddr, "-s", subject, "-m", string(body)) //nolint:gosec // G204: args constructed internally
	cmd.Dir = d.config.TownRoot
	cmd.Env = append(os.Environ(), "BD_ACTOR=daemon")
	util.SetDetachedProcessGroup(cmd)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("gt mail send: %w (output: %s)", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// sendRescueNudge issues a gt nudge to a polecat whose witness verdict was
// RESCUE. Best-effort: log on failure but do not propagate.
func (d *Daemon) sendRescueNudge(rigName, polecatName, sessionName, nudgeText string) {
	target := fmt.Sprintf("%s/%s", rigName, polecatName)
	cmd := exec.Command(d.gtPath, "nudge", target, nudgeText) //nolint:gosec // G204: args constructed internally
	cmd.Dir = d.config.TownRoot
	cmd.Env = append(os.Environ(), "BD_ACTOR=daemon")
	util.SetDetachedProcessGroup(cmd)
	if err := cmd.Run(); err != nil {
		d.logger.Printf("idle_check: rescue nudge to %s (session=%s) failed: %v",
			target, sessionName, err)
	}
}

// escalateIdleCheckToMayor mails the mayor when the witness verdict is
// ESCALATE. Best-effort.
func (d *Daemon) escalateIdleCheckToMayor(rigName, polecatName, reason string) {
	subject := fmt.Sprintf("IDLE_CHECK_ESCALATION: %s/%s", rigName, polecatName)
	body := fmt.Sprintf(`Witness for rig %s escalated an IDLE_CHECK for polecat %s.

reason: %s

Reaper deferred — daemon did NOT kill the polecat. Please investigate state
mismatch (heartbeat vs. agent process vs. beads).`, rigName, polecatName, reason)

	cmd := exec.Command(d.gtPath, "mail", "send", "mayor/", "-s", subject, "-m", body) //nolint:gosec // G204: args constructed internally
	cmd.Dir = d.config.TownRoot
	cmd.Env = append(os.Environ(), "BD_ACTOR=daemon")
	util.SetDetachedProcessGroup(cmd)
	if err := cmd.Run(); err != nil {
		d.logger.Printf("idle_check: escalation mail to mayor failed: %v", err)
	}
}

// writeJSONAtomic writes v as pretty JSON via tmpfile + rename to avoid
// partial-write races between daemon and witness.
func writeJSONAtomic(path string, v interface{}) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".idle_check-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return os.Rename(tmpName, path)
}
