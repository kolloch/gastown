//go:build windows

package tmux

import (
	"fmt"
	"os/exec"
	"strings"
)

// pidIsAlivePlatform uses tasklist to test PID existence on Windows.
// Returns true if the PID is present in the active process list. (za-8bj6)
func pidIsAlivePlatform(pid int) bool {
	out, err := exec.Command("tasklist", "/FI", fmt.Sprintf("PID eq %d", pid), "/NH").Output()
	if err != nil {
		return false
	}
	// "INFO: No tasks are running ..." appears when the PID is gone.
	return !strings.Contains(strings.ToLower(string(out)), "no tasks")
}
