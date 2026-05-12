+++
name = "disk-monitor"
description = "Disk-space watchdog with optional auto-mitigation (cargo clean idle polecats)"
version = 1

[gate]
type = "cooldown"
duration = "5m"

[tracking]
labels = ["plugin:disk-monitor", "category:ops"]
digest = false

[execution]
timeout = "2m"
notify_on_failure = true
severity = "high"
+++

# Disk Monitor

Watches free disk space at the town root. Escalates on tiered thresholds and (optionally) auto-mitigates by running `cargo clean` on idle polecat target/ directories.

## Why

The dipgt host hit a disk-full event on 2026-05-12 — furiosa was building when ENOSPC fired, mayor manually cleaned target/ dirs to recover. gastown has on-demand `gt doctor` disk checks but nothing periodic. This plugin closes that gap.

## What it does

1. Reads free bytes + used percent at `GT_TOWN_ROOT` (or `$HOME` fallback).
2. Classifies: `ok` (<80% used), `warn` (80-90%), `critical` (≥90%).
3. Output JSON: `{free_bytes, total_bytes, used_pct, level, action_taken}`.
4. On `warn`: mails `deacon/` with "Disk approaching threshold" (severity medium).
5. On `critical`: runs `gt escalate -s HIGH "Disk critical: NN%"` (mayor sees it).
6. If `ZACK_DISK_AUTO_CLEAN=1` (default ON), at `warn` or `critical`:
   - Identifies idle polecat target/ dirs (no tmux session active, OR target/ mtime >2h old).
   - Runs `cargo clean` on each via `CARGO_WRAPPER_BYPASS=1 cargo clean --manifest-path <path>`.
   - Reports freed bytes per dir.
7. Never touches:
   - Active polecat workspaces (tmux session live AND recent target/ activity).
   - User's main checkouts outside `GT_TOWN_ROOT`.
   - `~/.cargo/registry` or other shared caches.

## Thresholds

- `WARN`: ≥80% used. Action: mail deacon, optional auto-clean.
- `CRITICAL`: ≥90% used. Action: escalate to mayor, optional auto-clean.

Defaults are conservative — running cargo clean is reversible (cargo rebuilds on next invocation).

## Config

Environment overrides (set via daemon plugin env):
- `DISK_MONITOR_WARN_PCT` (default 80)
- `DISK_MONITOR_CRITICAL_PCT` (default 90)
- `DISK_MONITOR_TARGET_AGE_HOURS` (default 2 — target/ older than this is "idle")
- `ZACK_DISK_AUTO_CLEAN` (default 1 — set to 0 to disable auto-mitigation)

## Bypass

To skip the plugin one-shot: `ZACK_DISK_AUTO_CLEAN=0 ./run.sh --report-only`.
