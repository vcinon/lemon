// Package cli implements Lemon's command-line interface.
//
// Design constraints that shape this package: no TUI, no interactive prompt
// except during `lemon setup`, no curses, no alternate screen. Commands print
// plain lines and exit with a meaningful status.
package cli

import (
	"fmt"
	"io"
	"os"
	"strings"
)

// Exit codes. These are part of Lemon's contract with scripts.
const (
	// ExitOK means success.
	ExitOK = 0
	// ExitError is a general failure.
	ExitError = 1
	// ExitUsage is a bad invocation.
	ExitUsage = 2
	// ExitOffline means the peer was unreachable but the work was queued.
	ExitOffline = 3
	// ExitNotConfigured means `lemon setup` has not been run.
	ExitNotConfigured = 4
	// ExitConnect is a connection or handshake failure.
	ExitConnect = 5
)

// UsageError marks an invocation problem, which exits 2.
type UsageError struct {
	msg string
}

func (e *UsageError) Error() string { return e.msg }

// Usage builds a UsageError.
func Usage(format string, args ...any) error {
	return &UsageError{msg: fmt.Sprintf(format, args...)}
}

// Env carries the streams and settings a command runs against, so tests can
// drive the CLI without touching the real terminal.
type Env struct {
	// Stdout receives normal output.
	Stdout io.Writer
	// Stderr receives errors, warnings and the autostart tip.
	Stderr io.Writer
	// Stdin is used only by `lemon setup`.
	Stdin io.Reader
	// IsTTY reports whether Stdout is a terminal, which selects between
	// in-place progress redraws and plain per-file lines.
	IsTTY bool
	// Args are the arguments after the command name.
	Args []string
	// Layout is the resolved config layout.
	Layout any
	// Global flags.
	Global Global
}

// Global holds flags accepted before or after the command name.
type Global struct {
	// Debug turns on verbose logging.
	Debug bool
	// Version prints the version and exits.
	Version bool
	// Help prints usage and exits.
	Help bool
}

// ExitCode maps an error onto a process exit status.
func ExitCode(err error) int {
	if err == nil {
		return ExitOK
	}
	if _, ok := err.(*UsageError); ok {
		return ExitUsage
	}
	return ExitError
}

// Printf writes formatted output to the env's stdout.
func (e *Env) Printf(format string, args ...any) {
	fmt.Fprintf(e.Stdout, format, args...)
}

// Line writes one line to stdout.
func (e *Env) Line(format string, args ...any) {
	fmt.Fprintf(e.Stdout, format+"\n", args...)
}

// Errf writes formatted output to stderr.
func (e *Env) Errf(format string, args ...any) {
	fmt.Fprintf(e.Stderr, format, args...)
}

// ErrLine writes one line to stderr.
func (e *Env) ErrLine(format string, args ...any) {
	fmt.Fprintf(e.Stderr, format+"\n", args...)
}

// DefaultEnv returns an Env bound to the process's own streams.
func DefaultEnv(args []string) *Env {
	layout, _ := resolveLayout()
	return &Env{
		Stdout: os.Stdout,
		Stderr: os.Stderr,
		Stdin:  os.Stdin,
		IsTTY:  isTerminal(os.Stdout),
		Args:   args,
		Layout: layout,
	}
}

// usageText is the top-level help.
const usageText = `lemon - a tiny peer-to-peer command-line messenger over Tailscale

Usage:
  lemon <command> [arguments]

Setup:
  setup                     choose a username, default peer and listen port

Messaging:
  send [peer] <text...>     send a message
  history [peer]            show stored messages
  transfer <file...>        send files to the default peer

Peers:
  peers                     list known peers and their status
  discover                  find Lemons running on this tailnet
  peer add <name> [addr]    add a peer, resolving the address from Tailscale
  peer rm <name>            forget a peer
  connect <peer|addr>       check that a peer is reachable
  disconnect <peer>         drop a peer and stop sending to it

Listener:
  serve                     run the listener in the foreground
  status                    show listener, peers and queue state
  queue                     list work waiting to be delivered
  flush                     retry queued work now

Other:
  version                   print the version
  help                      show this help

Global flags:
  --debug, -d               verbose logging (same as LEMON_LOG=debug)

Lemon needs a running listener. Most commands start it automatically; to keep
it running across logins, add 'lemon serve' to your compositor's exec-once
configuration.

Exit codes:
  0 success   1 error   2 usage   3 peer offline (queued)   4 not configured   5 connect failed
`

// UsageString returns the top-level help text.
func UsageString() string { return usageText }

// indent prefixes every line of s with pad.
func indent(s, pad string) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	for i, l := range lines {
		lines[i] = pad + l
	}
	return strings.Join(lines, "\n")
}
