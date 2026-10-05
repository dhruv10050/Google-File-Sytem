package common

import (
	"log/slog"
	"os"
	"strings"
)

// NewLogger returns a text logger on stderr at the named level
// (debug, info, warn, error), tagged with the component name.
func NewLogger(level, component string) *slog.Logger {
	var l slog.Level
	switch strings.ToLower(level) {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: l})).With("c", component)
}
