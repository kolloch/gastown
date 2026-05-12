//go:build !windows

package tmux

import (
	"errors"
	"os"
	"strings"
	"syscall"
)

// pidIsAlivePlatform uses kill(pid, 0) to test PID existence on POSIX.
// Returns true if the process exists (even as a zombie or owned by another
// user — EPERM still means "process exists"). (za-8bj6)
//
// Error handling:
//   - syscall.ESRCH ("no such process") → dead
//   - syscall.EINVAL → dead (PID out of range)
//   - syscall.EPERM → alive (process exists, we lack permission)
//   - os package's own sentinel strings ("process already finished",
//     "process already released") → dead. Go's os.Process.Signal wraps a
//     raw ESRCH into these typed errors; they do NOT satisfy
//     errors.Is(err, syscall.ESRCH), so we string-match as a fallback.
//   - any other error → conservative: alive (avoids spurious restarts).
func pidIsAlivePlatform(pid int) bool {
	if pid <= 0 {
		return false
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	if err := proc.Signal(syscall.Signal(0)); err != nil {
		if errors.Is(err, syscall.ESRCH) || errors.Is(err, syscall.EINVAL) {
			return false
		}
		msg := err.Error()
		if strings.Contains(msg, "already finished") || strings.Contains(msg, "already released") {
			return false
		}
		// EPERM (process exists but we lack permission) → alive.
		return true
	}
	return true
}
