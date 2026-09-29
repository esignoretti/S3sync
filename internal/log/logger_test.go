package log

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTextHandler(t *testing.T) {
	var buf bytes.Buffer
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	slog.New(h).Info("hello", "key", "val")
	if !bytes.Contains(buf.Bytes(), []byte("hello")) {
		t.Fatal("missing message")
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

// Each Init call must return a closer that closes the handle THAT call opened.
//
// With a package-level closeOnce plus a package-level fileWriter, the first
// closer closed whichever file was opened LAST. That is observable without
// reaching into the package: once the first closer has run, the logger
// installed by the second Init must still be able to write. Under the bug the
// first closer had already closed the second file, so the message never
// reached disk, while the first file was leaked and never closed at all.
func TestSecondInitClosesFirstWriter(t *testing.T) {
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })

	dir := t.TempDir()
	pathA := filepath.Join(dir, "first.log")
	pathB := filepath.Join(dir, "second.log")

	closeA := Init(Config{File: pathA})
	slog.Info("message-in-a")

	closeB := Init(Config{File: pathB})
	slog.Info("message-in-b")

	closeA()
	// The second Init's logger is still live, so this must land in second.log.
	// If closeA() closed the wrong file, this write silently fails: slog
	// discards handler errors, so nothing surfaces at runtime.
	slog.Info("after-close-a")

	closeA() // closing twice must be harmless
	closeB()

	logA, logB := readFile(t, pathA), readFile(t, pathB)

	if !strings.Contains(logA, "message-in-a") {
		t.Errorf("first.log is missing its own message: %q", logA)
	}
	if !strings.Contains(logB, "message-in-b") {
		t.Errorf("second.log is missing its own message: %q", logB)
	}
	if !strings.Contains(logB, "after-close-a") {
		t.Errorf("second.log is missing %q — the first Init's closer closed the second file instead of its own: %q",
			"after-close-a", logB)
	}
	if strings.Contains(logA, "message-in-b") || strings.Contains(logA, "after-close-a") {
		t.Errorf("first.log received messages written after the second Init: %q", logA)
	}
}

// A closer returned when no log file was configured must be safe to call.
func TestInitCloserIsSafeWithoutFile(t *testing.T) {
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })

	closer := Init(Config{})
	closer()
	closer()
}
