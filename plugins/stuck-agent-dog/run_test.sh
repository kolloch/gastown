#!/usr/bin/env bash
# Tests for stuck-agent-dog/run.sh — hq-tu4uf.
#
# Verifies the per-polecat serial reverification:
#  - 3 polecats with stale heartbeats but LIVE claude processes → 0 acts
#  - 3 polecats with stale heartbeats AND DEAD claude → escalations
#    happen one polecat at a time, capped per run.
#
# The test sources run.sh with --self-test, which loads helpers without
# running the top-level scan. It then drives the helpers and the
# action-loop logic directly against a fake TOWN_ROOT layout.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RUN_SH="$SCRIPT_DIR/run.sh"

FAILURES=0
fail() { echo "FAIL: $*"; FAILURES=$((FAILURES + 1)); }
pass() { echo "ok: $*"; }

# Source the helpers without executing the scan.
# shellcheck disable=SC1090
source "$RUN_SH" --self-test

# --- Test fixture ------------------------------------------------------------

TMP_ROOT=$(mktemp -d)
trap 'rm -rf "$TMP_ROOT"' EXIT

setup_polecat() {
  # setup_polecat <rig> <pcat> <heartbeat_age_sec_or_-1_for_none> <session>
  local rig="$1" pcat="$2" age="$3" session="$4"
  local pcat_dir="$TMP_ROOT/$rig/polecats/$pcat"
  local hb_dir="$TMP_ROOT/.runtime/heartbeats"
  mkdir -p "$pcat_dir" "$hb_dir"

  if [ "$age" -ge 0 ]; then
    local ts
    ts=$(date -u -d "@$(( $(date +%s) - age ))" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null \
      || date -u -r "$(( $(date +%s) - age ))" +%Y-%m-%dT%H:%M:%SZ)
    printf '{"timestamp":"%s","state":"working"}' "$ts" > "$hb_dir/$session.json"
  fi
}

# --- Test 1: verify_heartbeat_fresh ------------------------------------------

echo "=== verify_heartbeat_fresh ==="
SAD_HEARTBEAT_STALE_SEC=180

setup_polecat zack alpha 10 zack-alpha   # fresh
setup_polecat zack bravo 600 zack-bravo  # stale (10 min)
setup_polecat zack charlie -1 zack-charlie # no heartbeat file

if verify_heartbeat_fresh "$TMP_ROOT" "zack-alpha"; then
  pass "fresh heartbeat detected"
else
  fail "fresh heartbeat NOT detected"
fi

if verify_heartbeat_fresh "$TMP_ROOT" "zack-bravo"; then
  fail "stale heartbeat falsely reported fresh"
else
  pass "stale heartbeat correctly rejected"
fi

if verify_heartbeat_fresh "$TMP_ROOT" "zack-charlie"; then
  fail "missing heartbeat falsely reported fresh"
else
  pass "missing heartbeat correctly rejected"
fi

# --- Test 2: reverify_polecat — fresh hb wins --------------------------------
#
# A candidate that turns out to have a fresh heartbeat must be SKIPPED.

echo ""
echo "=== reverify_polecat: fresh heartbeat → skip ==="
SAD_REVERIFY_DELAY_SEC=0

# Replace pgrep dependency for this test (no real claude processes).
verify_claude_alive_for_worktree() { return 1; }
verify_recent_hook_activity() { return 1; }

if reverify_polecat "$TMP_ROOT" "zack" "alpha" "zack-alpha" "$TMP_ROOT/zack/polecats/alpha"; then
  pass "alpha (fresh hb) skipped by reverify"
else
  fail "alpha (fresh hb) was NOT skipped"
fi

# --- Test 3: 3 polecats with stale hb but live claude → 0 escalations -------
#
# Simulates the 2026-05-12 cascade: all 3 polecats *look* stale on the sweep
# but their claude processes are alive. The dog must do NOTHING.

echo ""
echo "=== 3 polecats stale-hb + live claude → 0 escalations ==="

# Build candidate list as the scan would.
CANDIDATES=(
  "stuck|zack-bravo|zack|bravo|hq-1|stale_heartbeat"
  "stuck|zack-charlie|zack|charlie|hq-2|stale_heartbeat"
)
setup_polecat zack delta 600 zack-delta
CANDIDATES+=( "stuck|zack-delta|zack|delta|hq-3|stale_heartbeat" )

# Pretend every polecat has a live claude.
verify_claude_alive_for_worktree() { return 0; }

ESCALATIONS_SENT=0
ACTED=0
SAD_MAX_ESCALATIONS_PER_RUN=1

for ENTRY in "${CANDIDATES[@]}"; do
  IFS='|' read -r KIND SESSION RIG PCAT HOOK REASON <<< "$ENTRY"
  if reverify_polecat "$TMP_ROOT" "$RIG" "$PCAT" "$SESSION" "$TMP_ROOT/$RIG/polecats/$PCAT"; then
    continue
  fi
  if [ "$ESCALATIONS_SENT" -ge "$SAD_MAX_ESCALATIONS_PER_RUN" ]; then
    continue
  fi
  ESCALATIONS_SENT=$((ESCALATIONS_SENT + 1))
  ACTED=$((ACTED + 1))
done

if [ "$ACTED" -eq 0 ] && [ "$ESCALATIONS_SENT" -eq 0 ]; then
  pass "3 stale-hb + live-claude polecats → 0 actions (correct)"
else
  fail "expected 0 actions, got ACTED=$ACTED ESCALATIONS=$ESCALATIONS_SENT"
fi

# --- Test 4: 3 polecats stale-hb + DEAD claude → 1 per run, 3 over 3 runs ---
#
# The bug being fixed: previously the dog would nuke all 3 in one batch.
# Now: only 1 per run, the other 2 deferred for the next sweep.
#
# We simulate 3 successive runs and assert escalations come out one at a
# time, never all together.

echo ""
echo "=== 3 polecats stale-hb + DEAD claude → 3 escalations across 3 runs ==="

verify_claude_alive_for_worktree() { return 1; }
verify_recent_hook_activity() { return 1; }

# Same 3 candidates from above, but now claude is dead on all of them.
total_acted=0
total_deferred=0
runs=0
remaining=( "${CANDIDATES[@]}" )

while [ "${#remaining[@]}" -gt 0 ] && [ "$runs" -lt 5 ]; do
  runs=$((runs + 1))
  ESCALATIONS_SENT=0
  ACTED=0
  DEFERRED_THIS_RUN=()
  next_remaining=()

  for ENTRY in "${remaining[@]}"; do
    IFS='|' read -r KIND SESSION RIG PCAT HOOK REASON <<< "$ENTRY"
    if reverify_polecat "$TMP_ROOT" "$RIG" "$PCAT" "$SESSION" "$TMP_ROOT/$RIG/polecats/$PCAT"; then
      continue
    fi
    if [ "$ESCALATIONS_SENT" -ge "$SAD_MAX_ESCALATIONS_PER_RUN" ]; then
      DEFERRED_THIS_RUN+=("$SESSION")
      next_remaining+=("$ENTRY")
      continue
    fi
    ESCALATIONS_SENT=$((ESCALATIONS_SENT + 1))
    ACTED=$((ACTED + 1))
  done

  echo "  run $runs: acted=$ACTED deferred=${#DEFERRED_THIS_RUN[@]}"
  if [ "$ACTED" -gt "$SAD_MAX_ESCALATIONS_PER_RUN" ]; then
    fail "run $runs acted on $ACTED polecats (cap=$SAD_MAX_ESCALATIONS_PER_RUN)"
  fi
  total_acted=$(( total_acted + ACTED ))
  total_deferred=$(( total_deferred + ${#DEFERRED_THIS_RUN[@]} ))
  remaining=( "${next_remaining[@]+"${next_remaining[@]}"}" )
done

if [ "$total_acted" -eq 3 ]; then
  pass "all 3 polecats eventually acted on across $runs runs"
else
  fail "expected total_acted=3, got $total_acted (runs=$runs)"
fi

if [ "$runs" -eq 3 ]; then
  pass "exactly 3 runs needed (one polecat per run, no batching)"
else
  fail "expected 3 runs, got $runs"
fi

# --- Test 5: per-run cap actually blocks batch action ------------------------
#
# Direct assertion: even with the candidate list pre-built, no single run
# can produce more than SAD_MAX_ESCALATIONS_PER_RUN actions.

echo ""
echo "=== per-run cap blocks batch action ==="
SAD_MAX_ESCALATIONS_PER_RUN=1
ESCALATIONS_SENT=0
ACTED=0
for ENTRY in "${CANDIDATES[@]}"; do
  IFS='|' read -r KIND SESSION RIG PCAT HOOK REASON <<< "$ENTRY"
  if reverify_polecat "$TMP_ROOT" "$RIG" "$PCAT" "$SESSION" "$TMP_ROOT/$RIG/polecats/$PCAT"; then
    continue
  fi
  if [ "$ESCALATIONS_SENT" -ge "$SAD_MAX_ESCALATIONS_PER_RUN" ]; then
    continue
  fi
  ESCALATIONS_SENT=$((ESCALATIONS_SENT + 1))
  ACTED=$((ACTED + 1))
done

if [ "$ACTED" -eq 1 ]; then
  pass "single run with 3 dead candidates acted on exactly 1"
else
  fail "expected ACTED=1, got $ACTED"
fi

# --- Test 6: recent hook activity defers action ------------------------------

echo ""
echo "=== recent hook activity → skip (don't race with nudge) ==="
verify_claude_alive_for_worktree() { return 1; }
verify_recent_hook_activity() { return 0; }   # something nudged it recently

setup_polecat zack echo 600 zack-echo
if reverify_polecat "$TMP_ROOT" "zack" "echo" "zack-echo" "$TMP_ROOT/zack/polecats/echo"; then
  pass "recent-activity polecat skipped by reverify"
else
  fail "recent-activity polecat was NOT skipped"
fi

# --- Done --------------------------------------------------------------------

echo ""
if [ "$FAILURES" -gt 0 ]; then
  echo "FAILED: $FAILURES test(s) failed"
  exit 1
fi
echo "PASSED: all tests passed"
