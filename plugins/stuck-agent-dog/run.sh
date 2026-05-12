#!/usr/bin/env bash
# stuck-agent-dog/run.sh — Context-aware stuck/crashed agent detection.
#
# SCOPE: Only polecats and deacon. NEVER touches crew, mayor, witness, or refinery.
# The daemon detects; this plugin inspects context before acting.
#
# hq-tu4uf: candidate-list traversal is SERIAL with per-polecat re-verification.
# We never batch-nuke. Each polecat is independently re-checked after a brief
# settle delay, and only one escalation is sent per run.

set -euo pipefail

TOWN_ROOT="${GT_TOWN_ROOT:-$(gt town root 2>/dev/null)}"
RIGS_JSON_PATH="${TOWN_ROOT}/mayor/rigs.json"

# Tunables — overridable in tests.
# Re-verify delay: give a polecat time to resume between candidate scan and
# the final go/no-go decision. The daemon and witness both run on shorter
# loops than this plugin, so a 5s settle is plenty in normal operation but
# can be set to 0 for unit tests.
SAD_REVERIFY_DELAY_SEC="${SAD_REVERIFY_DELAY_SEC:-5}"

# Cap on escalations per run. Even if every polecat is genuinely stuck we
# only blast the mayor once — see feedback_no_bulk_restart_acks.md.
SAD_MAX_ESCALATIONS_PER_RUN="${SAD_MAX_ESCALATIONS_PER_RUN:-1}"

# Heartbeat staleness threshold for the polecat re-verify check.
# Matches internal/polecat/heartbeat.go SessionHeartbeatStaleThreshold (3m).
SAD_HEARTBEAT_STALE_SEC="${SAD_HEARTBEAT_STALE_SEC:-180}"

log() { echo "[stuck-agent-dog] $*"; }

# --- Per-polecat re-verification helpers -------------------------------------
#
# Each helper returns 0 if the polecat looks healthy enough to skip (i.e. we
# should NOT act on it), and 1 if the polecat still looks stuck. Keeping the
# helpers in pure-function shape lets the shell test (run_test.sh) drive them
# without needing tmux / pgrep / gt mail / the network.

# verify_heartbeat_fresh <town_root> <session_name>
# Returns 0 if the session's heartbeat file is younger than the stale
# threshold (polecat is alive — do NOT nuke). Returns 1 if the heartbeat is
# stale or absent (caller should drop to deeper checks).
verify_heartbeat_fresh() {
  local town_root="$1"
  local session_name="$2"
  local hb_file="${town_root}/.runtime/heartbeats/${session_name}.json"
  [ -f "$hb_file" ] || return 1

  local ts now age
  ts=$(jq -r '(.timestamp // empty) | sub("\\.[0-9]+Z?$"; "Z") | fromdateiso8601? // empty' "$hb_file" 2>/dev/null)
  [ -n "$ts" ] || return 1
  now=$(date +%s)
  age=$(( now - ts ))
  if [ "$age" -lt "$SAD_HEARTBEAT_STALE_SEC" ]; then
    return 0
  fi
  return 1
}

# verify_claude_alive_for_worktree <worktree_path>
# Returns 0 if there is at least one live claude process cwd'd to the given
# worktree (or its descendants). Returns 1 if no such process is found.
# Falls back to a broader pgrep when /proc/<pid>/cwd is not readable.
verify_claude_alive_for_worktree() {
  local worktree="$1"
  [ -n "$worktree" ] || return 1

  # Primary: any 'claude' process whose cwd is the worktree subtree.
  local pids pid cwd
  pids=$(pgrep -f 'claude' 2>/dev/null || true)
  for pid in $pids; do
    cwd=$(readlink "/proc/$pid/cwd" 2>/dev/null || true)
    if [ -n "$cwd" ] && [[ "$cwd" == "$worktree"* ]]; then
      return 0
    fi
  done
  return 1
}

# verify_recent_hook_activity <town_root> <rig> <pcat> <since_seconds>
# Returns 0 if the polecat has had nudge/work activity in the past
# <since_seconds> window — meaning the daemon or witness already prodded it
# and we should not racing-pile-on with a restart. Returns 1 if quiet.
#
# Activity signal: mtime on the hook directory (gt hook show updates the
# bead pointer; nudges land via gt nudge which touches the hook). This is
# best-effort — failing to find the path is treated as "no recent activity".
verify_recent_hook_activity() {
  local town_root="$1"
  local rig="$2"
  local pcat="$3"
  local since="$4"
  local hook_dir="${town_root}/${rig}/polecats/${pcat}"
  [ -d "$hook_dir" ] || return 1

  local mtime now age
  mtime=$(stat -c %Y "$hook_dir" 2>/dev/null || stat -f %m "$hook_dir" 2>/dev/null || echo 0)
  now=$(date +%s)
  age=$(( now - mtime ))
  if [ "$age" -lt "$since" ]; then
    return 0
  fi
  return 1
}

# reverify_polecat <town_root> <rig> <pcat> <session> <worktree>
# Run independent checks AFTER the settle delay. Returns 0 if the polecat
# should be SKIPPED (it's healthy / recently active), 1 if it is genuinely
# stuck and should be acted on.
reverify_polecat() {
  local town_root="$1"
  local rig="$2"
  local pcat="$3"
  local session="$4"
  local worktree="$5"

  # Check 1: fresh heartbeat → alive, skip.
  if verify_heartbeat_fresh "$town_root" "$session"; then
    log "  reverify: $session has FRESH heartbeat → skipping (not stuck)"
    return 0
  fi

  # Check 2: live claude process under the worktree → alive, skip.
  if [ -n "$worktree" ] && verify_claude_alive_for_worktree "$worktree"; then
    log "  reverify: $session has live claude process under $worktree → skipping"
    return 0
  fi

  # Check 3: recent hook activity (nudge/dispatch between scans) → skip
  # this round and let the next dog cycle re-decide.
  if verify_recent_hook_activity "$town_root" "$rig" "$pcat" "$SAD_REVERIFY_DELAY_SEC"; then
    log "  reverify: $session had hook activity within ${SAD_REVERIFY_DELAY_SEC}s → skipping"
    return 0
  fi

  return 1
}

# resolve_worktree <rig> <pcat>
# Best-effort lookup of the polecat's worktree path. Used by
# verify_claude_alive_for_worktree to scope the pgrep. If we can't resolve
# it we return empty and the caller falls back to other signals.
resolve_worktree() {
  local rig="$1"
  local pcat="$2"
  local pcat_meta="$TOWN_ROOT/$rig/polecats/$pcat/worktree"
  if [ -f "$pcat_meta" ]; then
    cat "$pcat_meta"
    return
  fi
  # Fallback: standard layout.
  local guess="$TOWN_ROOT/$rig/polecats/$pcat/worktree"
  if [ -d "$guess" ]; then
    echo "$guess"
    return
  fi
  echo ""
}

# Guard: if invoked with --self-test, exit after defining helpers so the
# shell test can source us without executing the scan.
if [ "${1:-}" = "--self-test" ]; then
  return 0 2>/dev/null || exit 0
fi

# --- Enumerate agents ---------------------------------------------------------

log "=== Checking agent health ==="

if [ ! -f "$RIGS_JSON_PATH" ]; then
  log "SKIP: rigs.json not found"
  exit 0
fi

# Build rig_name|prefix mapping
RIG_PREFIX_MAP=$(jq -r '.rigs | to_entries[] | "\(.key)|\(.value.beads.prefix // .key)"' "$RIGS_JSON_PATH" 2>/dev/null)
if [ -z "$RIG_PREFIX_MAP" ]; then
  log "SKIP: no rigs in rigs.json"
  exit 0
fi

# --- Check polecat health ----------------------------------------------------
#
# We build a CANDIDATE list (not an action list). Every candidate is later
# re-verified independently in the action loop. The candidate list is just
# "things that looked unhealthy on this sweep" — it is explicitly NOT
# trusted as ground truth for action.

CANDIDATES=()
HEALTHY=0

while IFS='|' read -r RIG PREFIX; do
  [ -z "$RIG" ] && continue
  POLECAT_DIR="$TOWN_ROOT/$RIG/polecats"
  [ -d "$POLECAT_DIR" ] || continue

  for PCAT_PATH in "$POLECAT_DIR"/*/; do
    [ -d "$PCAT_PATH" ] || continue
    PCAT_NAME=$(basename "$PCAT_PATH")
    SESSION_NAME="${PREFIX}-${PCAT_NAME}"

    if ! tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
      # Session dead — check hook
      HOOK_OUTPUT=$(gt hook show "$RIG/polecats/$PCAT_NAME" 2>/dev/null | head -1)
      HOOK_BEAD=$(echo "$HOOK_OUTPUT" | grep -v '(empty)' | awk '{print $2}' || true)

      if [ -n "$HOOK_BEAD" ]; then
        # Check agent_state
        AGENT_STATE=$(bd show "$HOOK_BEAD" --json 2>/dev/null \
          | python3 -c "import json,sys; d=json.load(sys.stdin); print(d[0].get('status',''))" 2>/dev/null || echo "")

        case "$AGENT_STATE" in
          closed) log "  SKIP $SESSION_NAME: bead closed (completed normally)"; continue ;;
        esac

        CANDIDATES+=("crashed|$SESSION_NAME|$RIG|$PCAT_NAME|$HOOK_BEAD")
        log "  CANDIDATE crashed: $SESSION_NAME (hook=$HOOK_BEAD)"
      fi
    else
      # Session alive — check process
      PANE_PID=$(tmux list-panes -t "$SESSION_NAME" -F '#{pane_pid}' 2>/dev/null | head -1)
      if [ -n "$PANE_PID" ]; then
        PROC_COMM=$(ps -o comm= -p "$PANE_PID" 2>/dev/null)
        if [ -z "$PROC_COMM" ]; then
          # Zombie: process dead, session alive
          HOOK_OUTPUT=$(gt hook show "$RIG/polecats/$PCAT_NAME" 2>/dev/null | head -1)
          HOOK_BEAD=$(echo "$HOOK_OUTPUT" | grep -v '(empty)' | awk '{print $2}' || true)
          if [ -n "$HOOK_BEAD" ]; then
            CANDIDATES+=("stuck|$SESSION_NAME|$RIG|$PCAT_NAME|$HOOK_BEAD|agent_dead")
            log "  CANDIDATE zombie: $SESSION_NAME (pid=$PANE_PID dead, hook=$HOOK_BEAD)"
          fi
        else
          # Even if the pane process looks alive, the heartbeat may be stale
          # (claude wedged inside its own loop). Mark as a soft candidate so
          # the reverify step can confirm before any action.
          if ! verify_heartbeat_fresh "$TOWN_ROOT" "$SESSION_NAME"; then
            HOOK_OUTPUT=$(gt hook show "$RIG/polecats/$PCAT_NAME" 2>/dev/null | head -1)
            HOOK_BEAD=$(echo "$HOOK_OUTPUT" | grep -v '(empty)' | awk '{print $2}' || true)
            if [ -n "$HOOK_BEAD" ]; then
              CANDIDATES+=("stuck|$SESSION_NAME|$RIG|$PCAT_NAME|$HOOK_BEAD|stale_heartbeat")
              log "  CANDIDATE stale_heartbeat: $SESSION_NAME (will reverify)"
            else
              HEALTHY=$((HEALTHY + 1))
            fi
          else
            HEALTHY=$((HEALTHY + 1))
          fi
        fi
      else
        HEALTHY=$((HEALTHY + 1))
      fi
    fi
  done
done <<< "$RIG_PREFIX_MAP"

log ""
log "Candidate count: ${#CANDIDATES[@]} (will reverify each independently), $HEALTHY healthy"

# --- Settle delay before per-polecat reverification --------------------------
#
# Give the cluster a moment to recover transient blips (Dolt reconnect,
# convoy redispatch, deacon nudge) before we make any irreversible decision.
if [ "${#CANDIDATES[@]}" -gt 0 ] && [ "$SAD_REVERIFY_DELAY_SEC" -gt 0 ]; then
  log "Settling for ${SAD_REVERIFY_DELAY_SEC}s before per-polecat reverification..."
  sleep "$SAD_REVERIFY_DELAY_SEC"
fi

# --- Check deacon health -----------------------------------------------------

log ""
log "=== Deacon Health ==="

DEACON_SESSION="hq-deacon"
DEACON_ISSUE=""

if ! tmux has-session -t "$DEACON_SESSION" 2>/dev/null; then
  log "  CRASHED: Deacon session is dead"
  DEACON_ISSUE="crashed"
else
  DEACON_PID=$(tmux list-panes -t "$DEACON_SESSION" -F '#{pane_pid}' 2>/dev/null | head -1)
  DEACON_COMM=$(ps -o comm= -p "$DEACON_PID" 2>/dev/null)
  if [ -z "$DEACON_COMM" ]; then
    log "  ZOMBIE: Deacon process dead (pid=$DEACON_PID), session alive"
    DEACON_ISSUE="zombie"
  else
    log "  Process alive: pid=$DEACON_PID comm=$DEACON_COMM"
  fi

  HEARTBEAT_FILE="$TOWN_ROOT/deacon/heartbeat.json"
  if [ -f "$HEARTBEAT_FILE" ]; then
    HEARTBEAT_TIME=$(stat -f %m "$HEARTBEAT_FILE" 2>/dev/null || stat -c %Y "$HEARTBEAT_FILE" 2>/dev/null)
    NOW=$(date +%s)
    HEARTBEAT_AGE=$(( NOW - HEARTBEAT_TIME ))

    if [ "$HEARTBEAT_AGE" -gt 1200 ]; then
      log "  STUCK: Deacon heartbeat stale (${HEARTBEAT_AGE}s old, >20m threshold)"
      DEACON_ISSUE="stuck_heartbeat_${HEARTBEAT_AGE}s"
    else
      log "  OK: Deacon heartbeat ${HEARTBEAT_AGE}s old"
    fi
  fi
fi

# --- Take action — SERIAL with per-polecat reverification --------------------
#
# hq-tu4uf: we do NOT batch. Each candidate is re-checked here, alone, and
# only one escalation is sent per run (SAD_MAX_ESCALATIONS_PER_RUN).
#
# This is the policy in feedback_no_bulk_restart_acks.md, enforced in code:
# "Don't bulk-ack RESTART_POLECAT waves; check polecats one by one."

ESCALATIONS_SENT=0
ACTED=0
SKIPPED_HEALTHY=0
RESTART_REQUESTS=()
DEFERRED=()  # candidates we declined to act on because the cap was hit

for ENTRY in "${CANDIDATES[@]:-}"; do
  [ -n "$ENTRY" ] || continue
  IFS='|' read -r KIND SESSION RIG PCAT HOOK REASON <<< "$ENTRY"
  REASON="${REASON:-${KIND}}"

  WORKTREE=$(resolve_worktree "$RIG" "$PCAT")

  log "Reverifying $SESSION ($KIND, reason=$REASON, worktree=${WORKTREE:-?})"
  if reverify_polecat "$TOWN_ROOT" "$RIG" "$PCAT" "$SESSION" "$WORKTREE"; then
    SKIPPED_HEALTHY=$((SKIPPED_HEALTHY + 1))
    continue
  fi

  if [ "$ESCALATIONS_SENT" -ge "$SAD_MAX_ESCALATIONS_PER_RUN" ]; then
    log "  DEFER $SESSION: per-run escalation cap ($SAD_MAX_ESCALATIONS_PER_RUN) reached; will revisit next cycle"
    DEFERRED+=("$SESSION")
    continue
  fi

  # Act on this ONE polecat.
  case "$KIND" in
    crashed)
      log "Requesting restart for $RIG/polecats/$PCAT (hook=$HOOK)"
      gt mail send "$RIG/witness" -s "RESTART_POLECAT: $RIG/$PCAT" --stdin <<BODY || true
Polecat $PCAT crash confirmed by stuck-agent-dog plugin (per-polecat reverify).
hook_bead: $HOOK
action: restart requested
BODY
      ;;
    stuck)
      log "Killing zombie session $SESSION and requesting restart"
      tmux kill-session -t "$SESSION" 2>/dev/null || true
      gt mail send "$RIG/witness" -s "RESTART_POLECAT: $RIG/$PCAT (zombie cleared)" --stdin <<BODY || true
Polecat $PCAT zombie session cleared by stuck-agent-dog plugin (per-polecat reverify).
hook_bead: $HOOK
reason: $REASON
action: restart requested
BODY
      ;;
    *)
      log "  WARN: unknown candidate kind '$KIND' for $SESSION; skipping"
      continue
      ;;
  esac

  RESTART_REQUESTS+=("$SESSION")
  ESCALATIONS_SENT=$((ESCALATIONS_SENT + 1))
  ACTED=$((ACTED + 1))
done

# --- Deacon escalation -------------------------------------------------------
#
# Deacon issues count toward the per-run escalation cap too. If we've already
# blasted the mayor about a polecat, defer the deacon escalation to the next
# run (or vice versa).

if [ -n "$DEACON_ISSUE" ]; then
  if [ "$ESCALATIONS_SENT" -lt "$SAD_MAX_ESCALATIONS_PER_RUN" ]; then
    log "Escalating deacon issue: $DEACON_ISSUE"
    gt escalate "Deacon $DEACON_ISSUE detected by stuck-agent-dog" -s HIGH 2>/dev/null || true
    ESCALATIONS_SENT=$((ESCALATIONS_SENT + 1))
  else
    log "DEFER deacon escalation: per-run cap reached (deacon=$DEACON_ISSUE)"
    DEFERRED+=("hq-deacon")
  fi
fi

# --- Report -------------------------------------------------------------------

SUMMARY="Agent health: ${#CANDIDATES[@]} candidates, $ACTED acted, $SKIPPED_HEALTHY recovered, ${#DEFERRED[@]} deferred, $HEALTHY healthy"
[ -n "$DEACON_ISSUE" ] && SUMMARY="$SUMMARY, deacon=$DEACON_ISSUE"
log ""
log "=== $SUMMARY ==="

bd create "stuck-agent-dog: $SUMMARY" -t chore --ephemeral \
  -l type:plugin-run,plugin:stuck-agent-dog,result:success \
  -d "$SUMMARY" --silent 2>/dev/null || true
