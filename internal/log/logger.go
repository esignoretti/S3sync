package log

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type Config struct {
	Level  string
	Format string
	File   string
}

var closeOnce sync.Once
var fileWriter io.WriteCloser

func Init(cfg Config) func() {
	lvl := parseLevel(cfg.Level)
	var w io.Writer = os.Stdout

	if cfg.File != "" {
		dir := filepath.Dir(cfg.File)
		os.MkdirAll(dir, 0755)
		f, err := os.OpenFile(cfg.File, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err == nil {
			w = io.MultiWriter(os.Stdout, f)
			fileWriter = f
		}
	}

	var h slog.Handler
	switch cfg.Format {
	case "json":
		h = slog.NewJSONHandler(w, &slog.HandlerOptions{Level: lvl})
	default:
		h = newTextHandler(w, &slog.HandlerOptions{Level: lvl})
	}

	slog.SetDefault(slog.New(h))
	return func() {
		closeOnce.Do(func() {
			if fileWriter != nil {
				fileWriter.Close()
			}
		})
	}
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

type textHandler struct {
	opts  slog.HandlerOptions
	attrs []slog.Attr
	mu    sync.Mutex
	w     io.Writer
}

func newTextHandler(w io.Writer, opts *slog.HandlerOptions) *textHandler {
	var o slog.HandlerOptions
	if opts != nil {
		o = *opts
	}
	return &textHandler{w: w, opts: o}
}

func (h *textHandler) Enabled(_ context.Context, lvl slog.Level) bool {
	minLevel := slog.LevelInfo
	if h.opts.Level != nil {
		minLevel = h.opts.Level.Level()
	}
	return lvl >= minLevel
}

func (h *textHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	line := r.Time.Format("15:04:05") + " " + r.Level.String() + "\t" + r.Message
	for _, a := range h.attrs {
		line += "  " + a.Key + "=" + a.Value.String()
	}
	r.Attrs(func(a slog.Attr) bool {
		line += "  " + a.Key + "=" + a.Value.String()
		return true
	})
	_, err := h.w.Write([]byte(line + "\n"))
	return err
}

func (h *textHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	cp := *h
	cp.attrs = append(append([]slog.Attr(nil), h.attrs...), attrs...)
	return &cp
}

func (h *textHandler) WithGroup(string) slog.Handler {
	return h
}
