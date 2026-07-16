package log

import (
	"bytes"
	"log/slog"
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
