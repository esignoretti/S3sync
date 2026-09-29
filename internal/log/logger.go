package log

import (
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

// Init installs a default slog logger and returns a closer for the file this
// call opened. The handle is captured per call rather than in package state: a
// shared variable meant the first closer closed whichever file was opened last,
// leaking the first handle and killing the second logger's writer.
func Init(cfg Config) func() {
	lvl := parseLevel(cfg.Level)
	var w io.Writer = os.Stdout

	var file io.Closer
	if cfg.File != "" {
		dir := filepath.Dir(cfg.File)
		os.MkdirAll(dir, 0755)
		f, err := os.OpenFile(cfg.File, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err == nil {
			w = io.MultiWriter(os.Stdout, f)
			file = f
		}
	}

	var h slog.Handler
	switch cfg.Format {
	case "json":
		h = slog.NewJSONHandler(w, &slog.HandlerOptions{Level: lvl})
	default:
		h = slog.NewTextHandler(w, &slog.HandlerOptions{
			Level: lvl,
			ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
				if a.Key == slog.TimeKey && len(groups) == 0 {
					return slog.String(slog.TimeKey, a.Value.Time().Format("15:04:05"))
				}
				return a
			},
		})
	}

	slog.SetDefault(slog.New(h))
	var once sync.Once
	return func() {
		once.Do(func() {
			if file != nil {
				file.Close()
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
