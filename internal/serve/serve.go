// Package serve implements the Lemon listener daemon.
//
// One `lemon serve` process owns everything network-facing: the TCP listener
// on the configured port, the Unix control socket the CLI talks to, the
// durable outbox and its retry worker, and the history database. Every other
// command is a thin client that makes sure this process is running and then
// asks it to do the work.
package serve

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/siin/lemon/internal/config"
	"github.com/siin/lemon/internal/logging"
)

// Lock guards against a second serve process.
//
// An advisory flock held for the process lifetime is the reliable mechanism
// here. Checking whether port 6767 is open is not sufficient: another program
// could be occupying it, and a port check races against a starting daemon.
type Lock struct {
	file *os.File
}

// AcquireLock takes the exclusive serve lock in dir.
func AcquireLock(path string) (*Lock, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("cannot open serve lock: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			pid := readPID(path)
			if pid > 0 {
				return nil, &AlreadyRunningError{PID: pid}
			}
			return nil, &AlreadyRunningError{}
		}
		return nil, fmt.Errorf("cannot lock serve state: %w", err)
	}
	// Record the owning pid so `lemon status` can report it.
	if err := f.Truncate(0); err == nil {
		_, _ = f.WriteAt([]byte(strconv.Itoa(os.Getpid())), 0)
		_ = f.Sync()
	}
	return &Lock{file: f}, nil
}

// readPID extracts a pid from a lock file, best effort.
func readPID(path string) int {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	pid, err := strconv.Atoi(string(trimSpace(data)))
	if err != nil {
		return 0
	}
	return pid
}

// trimSpace strips ASCII whitespace.
func trimSpace(b []byte) []byte {
	start, end := 0, len(b)
	for start < end && isSpace(b[start]) {
		start++
	}
	for end > start && isSpace(b[end-1]) {
		end--
	}
	return b[start:end]
}

// isSpace reports whether c is ASCII whitespace.
func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f'
}

// AlreadyRunningError reports that a serve process already holds the lock.
type AlreadyRunningError struct {
	// PID is the running daemon's process id when it could be determined.
	PID int
}

func (e *AlreadyRunningError) Error() string {
	if e.PID > 0 {
		return fmt.Sprintf("lemon is already running (pid %d)", e.PID)
	}
	return "lemon is already running"
}

// Release drops the lock.
func (l *Lock) Release() {
	if l == nil || l.file == nil {
		return
	}
	_ = syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	_ = l.file.Close()
	l.file = nil
}

// IsRunning reports whether a serve lock is currently held.
func IsRunning(path string) (bool, int) {
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return false, 0
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return true, readPID(path)
		}
	}
	// We got the lock, so nobody held it.
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return false, 0
}

// Health is the response to a control-socket health probe.
type Health struct {
	OK        bool   `json:"ok"`
	PID       int    `json:"pid"`
	Version   int    `json:"version"`
	User      string `json:"user"`
	Addr      string `json:"addr"`
	StartedAt int64  `json:"started_at"`
}

// Probe checks whether a healthy daemon answers on the control socket.
//
// A socket that exists but does not answer means a half-started or wedged
// daemon, so liveness is defined by a successful round trip rather than by the
// mere presence of the file.
func Probe(ctx context.Context, socketPath string) (Health, error) {
	conn, err := dialUnix(ctx, socketPath, 2*time.Second)
	if err != nil {
		return Health{}, err
	}
	defer conn.Close()
	return exchangeHealth(ctx, conn)
}

// Start binds the TCP listener, distinguishing a foreign occupant of the port
// from a genuine failure.
func Start(port int, host string) (net.Listener, error) {
	if host == "" {
		host = "0.0.0.0"
	}
	addr := config.WithPort(host, port)
	ln, err := net.Listen("tcp", addr)
	if err == nil {
		return ln, nil
	}
	// A busy port is the common case and deserves a specific message.
	if isAddrInUse(err) {
		return nil, &PortInUseError{Addr: addr, Err: err}
	}
	return nil, fmt.Errorf("cannot listen on %s: %w", addr, err)
}

// PortInUseError reports that the Lemon port is taken by something else.
type PortInUseError struct {
	Addr string
	Err  error
}

func (e *PortInUseError) Error() string {
	return fmt.Sprintf("port %s is already in use by another program", e.Addr)
}

func (e *PortInUseError) Unwrap() error { return e.Err }

// isAddrInUse recognises EADDRINUSE across wrapped error strings.
func isAddrInUse(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.EADDRINUSE) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "address already in use") ||
		strings.Contains(msg, "only one usage of each socket address")
}

// WritePIDFile records the daemon's pid.
func WritePIDFile(path string) error {
	if err := os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())+"\n"), 0o600); err != nil {
		return fmt.Errorf("cannot write pid file: %w", err)
	}
	return nil
}

// RemovePIDFile clears the pid file.
func RemovePIDFile(path string) { _ = os.Remove(path) }

// ReadPIDFile returns the recorded pid, or 0.
func ReadPIDFile(path string) int {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	return readPIDBytes(data)
}

// readPIDBytes parses a pid from raw bytes.
func readPIDBytes(b []byte) int {
	pid, err := strconv.Atoi(string(trimSpace(b)))
	if err != nil {
		return 0
	}
	return pid
}

// processAlive reports whether a pid names a live process we can signal.
func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	// Signal 0 performs the permission and existence check without delivering.
	return proc.Signal(syscall.Signal(0)) == nil
}

// watchdog cleans up stale pid files and orphaned transfer parts.
type watchdog struct {
	log      *logging.Logger
	pidFile  string
	partDir  string
	interval time.Duration
	once     sync.Once
}

// run performs periodic housekeeping until ctx is done.
func (w *watchdog) run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	w.tick()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.tick()
		}
	}
}

// tick does one housekeeping pass.
func (w *watchdog) tick() {
	w.once.Do(func() {
		// A pid file left by a crashed daemon is noise; clear it.
		if pid := ReadPIDFile(w.pidFile); pid > 0 && !processAlive(pid) {
			RemovePIDFile(w.pidFile)
		}
	})
	if w.partDir != "" {
		if n := discardOrphanParts(w.partDir); n > 0 {
			w.log.Debugf("removed %d orphaned partial transfer file(s)", n)
		}
	}
}
