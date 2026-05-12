#!/usr/bin/env bash
# disk-monitor/run.sh — Periodic disk-space watchdog with optional auto-mitigation.
#
# Tiered thresholds at GT_TOWN_ROOT:
#   ok       <  80% used        — no action
#   warn     80-89% used        — mail deacon, optional cargo clean on idle polecats
#   critical >= 90% used        — gt escalate HIGH to mayor, optional cargo clean
#
# Cleanup is opt-out via ZACK_DISK_AUTO_CLEAN=0. Default ON.
#
# Filed: za-tvim (Overseer 2026-05-12 disk-full incident).

set -euo pipefail

# --- Configuration ----------------------------------------------------------

if [[ -n "${GT_TOWN_ROOT:-}" && -d "$GT_TOWN_ROOT" ]]; then
  TOWN_ROOT="$GT_TOWN_ROOT"
else
  TOWN_ROOT="${HOME:-/tmp}"
fi

WARN_PCT="${DISK_MONITOR_WARN_PCT:-80}"
CRITICAL_PCT="${DISK_MONITOR_CRITICAL_PCT:-90}"
TARGET_AGE_HOURS="${DISK_MONITOR_TARGET_AGE_HOURS:-2}"
AUTO_CLEAN="${ZACK_DISK_AUTO_CLEAN:-1}"

REPORT_ONLY=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --report-only) REPORT_ONLY=true; shift ;;
    --help|-h)
      echo "Usage: $0 [--report-only]"
      exit 0
      ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

log() { echo "[disk-monitor] $*"; }

# --- Step 1: Measure disk -----------------------------------------------------

# Use stat -f for POSIX-portable; fall back to df parsing.
read -r TOTAL_BYTES USED_BYTES AVAIL_BYTES <<<"$(
  df --output=size,used,avail -B1 "$TOWN_ROOT" | tail -1
)" || {
  log "ERROR: could not read disk for $TOWN_ROOT"
  exit 1
}

if [[ "$TOTAL_BYTES" -le 0 ]]; then
  log "ERROR: df reported total=0 for $TOWN_ROOT"
  exit 1
fi

USED_PCT=$(( USED_BYTES * 100 / TOTAL_BYTES ))

if   [[ "$USED_PCT" -ge "$CRITICAL_PCT" ]]; then LEVEL=critical
elif [[ "$USED_PCT" -ge "$WARN_PCT"     ]]; then LEVEL=warn
else                                             LEVEL=ok
fi

free_gb=$(awk -v b="$AVAIL_BYTES" 'BEGIN{printf "%.1f", b/1024/1024/1024}')
total_gb=$(awk -v b="$TOTAL_BYTES" 'BEGIN{printf "%.1f", b/1024/1024/1024}')

log "level=$LEVEL used=${USED_PCT}% free=${free_gb}GB total=${total_gb}GB root=$TOWN_ROOT"

# --- Step 2: Auto-mitigation (idle polecat cargo clean) -----------------------

ACTION_TAKEN="none"
FREED_BYTES=0

if [[ "$LEVEL" != "ok" && "$AUTO_CLEAN" == "1" && "$REPORT_ONLY" != "true" ]]; then
  ACTION_TAKEN="cargo_clean_idle_polecats"

  # Find polecat target/ dirs under the town root.
  # Pattern: $TOWN_ROOT/<rig>/polecats/<name>/<rig>/target
  while IFS= read -r target_dir; do
    [[ -d "$target_dir" ]] || continue

    # The polecat name is the second-to-last "polecats/<NAME>/..." segment.
    polecat_path="${target_dir%/target}"
    polecat_name="$(basename "$(dirname "$polecat_path")")"

    # Skip if its tmux session is alive (active polecat — don't disturb).
    # Tmux socket name uses the town hash from the daemon; query via gt tmux ls
    # if available, fall back to scanning the standard socket directories.
    is_active=false
    if command -v gt >/dev/null 2>&1; then
      if gt status 2>/dev/null | grep -qE "^[[:space:]]+${polecat_name}[[:space:]]+●"; then
        is_active=true
      fi
    fi

    if [[ "$is_active" == "true" ]]; then
      log "  skip ${polecat_name} (active per gt status)"
      continue
    fi

    # Check target/ mtime — if recently touched, also skip.
    if [[ -n "$(find "$target_dir" -maxdepth 1 -mmin -$((TARGET_AGE_HOURS * 60)) -print -quit 2>/dev/null)" ]]; then
      log "  skip ${polecat_name} (target/ active within ${TARGET_AGE_HOURS}h)"
      continue
    fi

    # Compute current size, clean, compute freed.
    before=$(du -sb "$target_dir" 2>/dev/null | awk '{print $1}' || echo 0)

    manifest_path="$(dirname "$target_dir")/Cargo.toml"
    if [[ -f "$manifest_path" ]]; then
      log "  cleaning ${polecat_name} (${target_dir}) — was ${before}B"
      if CARGO_WRAPPER_BYPASS=1 cargo clean --manifest-path "$manifest_path" 2>/dev/null; then
        after=$(du -sb "$target_dir" 2>/dev/null | awk '{print $1}' || echo 0)
        freed=$(( before - after ))
        FREED_BYTES=$(( FREED_BYTES + freed ))
        log "    freed ${freed}B (now ${after}B)"
      else
        # cargo clean failed (worktree may be in a weird state) — fall back to rm -rf
        log "    cargo clean failed, falling back to rm -rf"
        if rm -rf "$target_dir"; then
          FREED_BYTES=$(( FREED_BYTES + before ))
          log "    freed ${before}B via rm -rf"
        fi
      fi
    else
      log "  ${polecat_name}: no Cargo.toml at $(dirname "$target_dir"), trying rm -rf"
      if rm -rf "$target_dir"; then
        FREED_BYTES=$(( FREED_BYTES + before ))
        log "    freed ${before}B via rm -rf"
      fi
    fi
  done < <(find "$TOWN_ROOT" -maxdepth 6 -path "*/polecats/*/target" -type d 2>/dev/null)

  log "auto-clean complete: freed ${FREED_BYTES}B total"
fi

# --- Step 3: Escalation / notification ---------------------------------------

case "$LEVEL" in
  warn)
    if [[ "$REPORT_ONLY" != "true" ]] && command -v gt >/dev/null 2>&1; then
      gt mail send deacon/ -s "Disk WARN: ${USED_PCT}% used, ${free_gb}GB free" \
        -m "Town root: ${TOWN_ROOT}
Auto-clean freed ${FREED_BYTES}B from idle polecats.
Suggest scanning user-side checkouts (outside town root) for additional reclamation." \
        2>/dev/null || log "WARN: mail send to deacon failed"
    fi
    ;;
  critical)
    if [[ "$REPORT_ONLY" != "true" ]] && command -v gt >/dev/null 2>&1; then
      gt escalate -s HIGH "Disk critical: ${USED_PCT}% used, only ${free_gb}GB free on ${TOWN_ROOT}" \
        2>/dev/null || log "WARN: gt escalate failed"
    fi
    ;;
esac

# --- Step 4: JSON output -----------------------------------------------------

cat <<JSON
{
  "level": "${LEVEL}",
  "used_pct": ${USED_PCT},
  "free_bytes": ${AVAIL_BYTES},
  "total_bytes": ${TOTAL_BYTES},
  "town_root": "${TOWN_ROOT}",
  "action_taken": "${ACTION_TAKEN}",
  "freed_bytes": ${FREED_BYTES}
}
JSON

# Always exit clean. The plugin's escalation routing handles severity.
exit 0
