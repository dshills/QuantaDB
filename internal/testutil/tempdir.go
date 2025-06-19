package testutil

import (
	"os"
	"testing"
)

// TempDir creates a temporary directory for testing and returns a cleanup function.
func TempDir(t *testing.T) (string, func()) {
	t.Helper()

	dir, err := os.MkdirTemp("", "quantadb-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	cleanup := func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("failed to remove temp dir: %v", err)
		}
	}

	return dir, cleanup
}
