// Package logging provides Lemon's single debug mechanism.
//
// Debug output is off unless LEMON_LOG is set (or --debug is passed, which
// sets the same variable). Normal operation is completely silent; LEMON_LOG
// exists so a user can hand us a log when something misbehaves.
package logging

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"
)

// EnvVar is the environment variable that enables debug logging.
const EnvVar = "LEMON_LOG"

// Levels, in increasing severity.
const (
	LevelDebug = "debug"
	LevelInfo  = "info"
	LevelWarn  = "warn"
	LevelError = "error"
	LevelOff   = "off"
)

// Logger writes optional diagnostics. The zero value discards everything,
// which is the correct behaviour for a quiet, production run.
type Logger struct {
	mu    sync.Mutex
	out   io.Writer
	level string
}

// New returns a Logger writing to out at the given level. An unknown or empty
// level yields a silent logger unless it is "debug", "info" or "warn".
func New(out io.Writer, level string) *Logger {
	lvl := normalise(level)
	l := &Logger{out: out, level: lvl}
	return l
}

// Discard returns a logger that writes nothing.
func Discard() *Logger { return &Logger{level: LevelOff} }

// FromEnv returns a Logger configured from LEMON_LOG, writing to stderr.
func FromEnv() *Logger { return New(os.Stderr, os.Getenv(EnvVar)) }

// normalise maps an arbitrary string to a known level.
func normalise(level string) string {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case LevelDebug, "trace", "verbose", "1":
		return LevelDebug
	case LevelInfo:
		return LevelInfo
	case LevelWarn, "warning":
		return LevelWarn
	case LevelError:
		return LevelError
	case LevelOff, "none", "silent", "0":
		return LevelOff
	default:
		return LevelOff
	}
}

// Enabled reports whether level would be emitted.
func (l *Logger) Enabled(level string) bool {
	if l == nil || l.out == nil || l.level == LevelOff {
		return false
	}
	return rank(level) >= rank(l.level)
}

// rank orders levels for comparison.
func rank(level string) int {
	switch level {
	case LevelDebug:
		return 0
	case LevelInfo:
		return 1
	case LevelWarn:
		return 2
	case LevelError:
		return 3
	default:
		return 4
	}
}

// Debugf logs at debug level.
func (l *Logger) Debugf(format string, args ...any) { l.logf(LevelDebug, format, args...) }

// Infof logs at info level.
func (l *Logger) Infof(format string, args ...any) { l.logf(LevelInfo, format, args...) }

// Warnf logs at warn level.
func (l *Logger) Warnf(format string, args ...any) { l.logf(LevelWarn, format, args...) }

// Errorf logs at error level.
func (l *Logger) Errorf(format string, args ...any) { l.logf(LevelError, format, args...) }

// logf writes one line when the level is enabled.
func (l *Logger) logf(level, format string, args ...any) {
	if l == nil || !l.Enabled(level) {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintf(l.out, "%s %s lemon[%d] %s\n",
		strings.ToUpper(level), time.Now().Format("15:04:05.000"), os.Getpid(),
		fmt.Sprintf(format, args...))
}

// SetLevel changes the threshold at runtime.
func (l *Logger) SetLevel(level string) {
	lvl := normalise(level)
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = lvl
}

// Level returns the current threshold.
func (l *Logger) Level() string {
	if l == nil {
		return LevelOff
	}
	return l.level
}
