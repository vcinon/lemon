// Package notify delivers desktop notifications by shelling out to an
// existing notifier. Lemon never implements notification UI itself.
//
// On Arch the binary is normally `notify-send`, so that name is tried after
// the spec's `notify`. A missing notifier is never fatal: it is logged and the
// message is still delivered.
package notify

import (
	"context"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// notifierCommand is the script form used to send a notification.
const notifierCommand = "notify send"

// ErrNoNotifier means no supported notifier is installed.
var ErrNoNotifier = errNoNotifier{}

type errNoNotifier struct{}

func (errNoNotifier) Error() string {
	return "no notification command found (tried: notify, notify-send)"
}

// Notifier sends desktop notifications through an external command.
type Notifier struct {
	// Override, when set, replaces the whole command, and the message is
	// appended as arguments. Configured via the notify_command setting.
	Override string

	mu   sync.Mutex
	name string
	// form is "notify send", "notify-send", or "override".
	form string
}

// New returns a Notifier. override may be empty.
func New(override string) *Notifier {
	n := &Notifier{Override: strings.TrimSpace(override)}
	n.detect()
	return n
}

// detect resolves the notifier command once, then caches it.
func (n *Notifier) detect() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.form != "" {
		return
	}
	if n.Override != "" {
		n.form = "override"
		return
	}
	if path, err := exec.LookPath("notify"); err == nil {
		n.form = "notify send"
		n.name = path
		return
	}
	if path, err := exec.LookPath("notify-send"); err == nil {
		n.form = "notify-send"
		n.name = path
		return
	}
	n.form = ""
}

// Command returns the resolved command form, for status output.
func (n *Notifier) Command() string {
	n.detect()
	n.mu.Lock()
	defer n.mu.Unlock()
	switch n.form {
	case "notify send":
		return notifierCommand
	case "notify-send":
		return "notify-send"
	case "override":
		return n.Override
	default:
		return ""
	}
}

// Available reports whether a notifier was found.
func (n *Notifier) Available() bool { return n.Command() != "" }

// Send delivers a notification body. A missing notifier yields ErrNoNotifier
// but never panics or blocks the caller for long.
func (n *Notifier) Send(ctx context.Context, body string) error {
	n.detect()

	n.mu.Lock()
	form, name, override := n.form, n.name, n.Override
	n.mu.Unlock()

	var args []string
	switch form {
	case "notify send":
		args = []string{"send", body}
		if name == "" {
			name = "notify"
		}
	case "notify-send":
		name = "notify-send"
		args = []string{body}
	case "override":
		name, args = override, append(strings.Fields(override), body)
	default:
		return ErrNoNotifier
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, name, args...)
	// Notifications must never pop a terminal window.
	cmd.Stdin = nil
	if out, err := cmd.CombinedOutput(); err != nil {
		return &Error{Command: form, Err: err, Output: strings.TrimSpace(string(out))}
	}
	return nil
}

// Error wraps a notifier invocation failure.
type Error struct {
	Command string
	Err     error
	Output  string
}

func (e *Error) Error() string {
	msg := e.Command + ": " + e.Err.Error()
	if e.Output != "" {
		msg += " (" + e.Output + ")"
	}
	return msg
}

func (e *Error) Unwrap() error { return e.Err }
