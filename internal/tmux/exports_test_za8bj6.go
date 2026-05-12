package tmux

// PidIsAliveForTest exposes the internal pidIsAlive(int) liveness probe for
// cross-package tests (e.g., witness/handlers_test.go) under za-8bj6.
// Not exported in production code paths.
func PidIsAliveForTest(pid int) bool {
	return pidIsAlivePlatform(pid)
}
