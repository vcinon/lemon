package cli

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/siin/lemon/internal/config"
	"github.com/siin/lemon/internal/logging"
	"github.com/siin/lemon/internal/serve"
)

// resolveLayout computes the config layout.
func resolveLayout() (config.Layout, error) { return config.Paths() }

// isTerminal reports whether f is a character device, which is our TTY test.
// We deliberately avoid cgo ioctl: a character device is good enough to decide
// between in-place redraws and plain lines.
func isTerminal(f *os.File) bool {
	info, err := f.Stat()
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeCharDevice != 0
}

// startupTimeout bounds how long we wait for an auto-started daemon.
const startupTimeout = 5 * time.Second

// probeInterval is how often we re-check a starting daemon's health.
const probeInterval = 50 * time.Millisecond

// autostartTip is the one-time hint about starting the listener at login.
//
// Lemon does not edit any compositor configuration: we don't know which
// compositor you run, where its config lives, or how it prefers to be
// configured. The tip is printed once, then acknowledged in the config file so
// it never becomes noise.
const autostartTip = `Lemon listener is not configured to start automatically.

Tip: add 'lemon serve' to your compositor's exec-once configuration so Lemon
starts automatically at login. For example, in Hyprland's autostart.conf:

    exec-once = lemon serve

Run 'lemon setup' to configure your username, default peer and port.`

// client bundles the control connection and the loaded configuration.
type client struct {
	env    *Env
	layout config.Layout
	cfg    *config.Config
	log    *logging.Logger
	conn   *serve.ControlClient
	// stdinReader is shared across prompts so buffered input is not lost.
	stdinReader *bufio.Reader
}

// startHint is shown when a command had to launch the listener.
type startHint struct{ shown bool }

// newClient loads config and prepares logging, without starting the daemon.
func newClient(env *Env) (*client, error) {
	layout, err := resolveLayout()
	if err != nil {
		return nil, err
	}
	cfg, err := config.Load(layout)
	if err != nil {
		return nil, err
	}
	level := os.Getenv(logging.EnvVar)
	if env.Global.Debug {
		level = logging.LevelDebug
	}
	log := logging.New(env.Stderr, level)
	return &client{env: env, layout: layout, cfg: cfg, log: log}, nil
}

// requireConfigured returns a clear error when setup has not been run.
func (c *client) requireConfigured() error {
	return c.cfg.RequireConfigured()
}

// isKnownPeer reports whether name is a configured peer.
func (c *client) isKnownPeer(name string) bool {
	_, ok := c.cfg.Peer(name)
	return ok
}

// connect ensures the daemon is running and returns a control connection.
//
// The health check is a real round trip over the control socket, not a port
// probe: an open 6767 could belong to an unrelated program, and a socket file
// left by a crashed daemon would look alive without answering.
//
// On success the connection is left open in c.conn so the caller can issue its
// real request without reconnecting.
func (c *client) connect(ctx context.Context) (*serve.ControlClient, *startHint, error) {
	hint := &startHint{}
	if conn, err := serve.Dial(ctx, c.layout.Socket, 2*time.Second); err == nil {
		if _, err := c.health(ctx, conn); err == nil {
			c.conn = conn
			return conn, hint, nil
		}
		conn.Close()
	} else if !errors.Is(err, os.ErrNotExist) {
		c.log.Debugf("control socket probe failed: %v", err)
	}

	if err := c.startDaemon(ctx); err != nil {
		return nil, hint, err
	}
	hint.shown = true

	conn, err := c.awaitDaemon(ctx)
	if err != nil {
		return nil, hint, err
	}
	c.conn = conn
	return conn, hint, nil
}

// health performs a ping round trip on an existing connection.
func (c *client) health(ctx context.Context, conn *serve.ControlClient) (*serve.Health, error) {
	resp, err := conn.Do(ctx, serve.Request{Op: serve.OpPing}, nil)
	if err != nil {
		return nil, err
	}
	if !resp.OK {
		return nil, errors.New(resp.Error)
	}
	if resp.Health == nil {
		return nil, errors.New("daemon returned no health payload")
	}
	return resp.Health, nil
}

// startDaemon spawns a detached `lemon serve`.
//
// setsid detaches the child from our process group so it survives the CLI
// exiting, and the daemon's own flock guarantees only one listener exists even
// if several CLIs race to start it.
func (c *client) startDaemon(ctx context.Context) error {
	if running, pid := serve.IsRunning(c.layout.LockFile); running {
		// A daemon holds the lock but its socket is not answering. It may
		// still be starting, so wait before giving up.
		c.log.Debugf("serve lock held by pid %d, waiting for socket", pid)
		return nil
	}
	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("cannot locate the lemon binary: %w", err)
	}
	if err := c.layout.EnsureDirs(); err != nil {
		return err
	}

	cmd := detach(exe, "serve")
	// When auto-started, diagnostics go to a log file rather than a terminal
	// that no longer exists.
	logFile, err := os.OpenFile(c.layout.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("cannot open the listener log: %w", err)
	}
	defer logFile.Close()
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if c.log.Level() == logging.LevelDebug {
		cmd.Env = append(os.Environ(), logging.EnvVar+"="+logging.LevelDebug)
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("cannot start the lemon listener: %w", err)
	}
	// The child is reparented to init; we deliberately do not wait on it.
	go func() { _ = cmd.Wait() }()
	c.log.Debugf("started lemon listener (pid %d)", cmd.Process.Pid)
	return nil
}

// awaitDaemon polls for a healthy control socket.
func (c *client) awaitDaemon(ctx context.Context) (*serve.ControlClient, error) {
	deadline := time.Now().Add(startupTimeout)
	var lastErr error
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		conn, err := serve.Dial(ctx, c.layout.Socket, time.Second)
		if err == nil {
			if _, err := c.health(ctx, conn); err == nil {
				return conn, nil
			}
			lastErr = err
			conn.Close()
		} else {
			lastErr = err
		}
		time.Sleep(probeInterval)
	}
	if lastErr == nil {
		lastErr = errors.New("timed out")
	}
	return nil, fmt.Errorf("the lemon listener did not start: %w (see %s)", lastErr, c.layout.LogFile)
}

// do sends a control request and returns the daemon's reply.
//
// Requests that do not stream progress carry their events in the reply, which
// are printed here in order. A streaming request passes onEvent instead and
// gets nothing in the reply.
func (c *client) do(ctx context.Context, req serve.Request, onEvent func(serve.Event)) (*serve.Response, error) {
	if c.conn == nil {
		return nil, errors.New("not connected to the lemon listener")
	}
	resp, err := c.conn.Do(ctx, req, onEvent)
	if err != nil {
		return nil, err
	}
	if len(resp.Events) > 0 {
		c.env.printEvents(resp.Events)
	}
	return resp, nil
}

// close releases the control connection.
func (c *client) close() {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}

// requireDaemon is the common preamble: verify setup ran, then make sure the
// listener is alive and leave c.conn ready to use.
func (c *client) requireDaemon(ctx context.Context) (*startHint, error) {
	if err := c.requireConfigured(); err != nil {
		return nil, err
	}
	_, hint, err := c.connect(ctx)
	if err != nil {
		return hint, err
	}
	return hint, nil
}

// promptLine reads one line from the env's stdin.
//
// The shared bufio.Reader matters: reading stdin with a fresh reader per
// prompt would discard whatever the previous ReadString buffered, losing
// answers typed on the same line.
func (c *client) promptLine(label, def string) (string, error) {
	if def != "" {
		c.env.Errf("%s [%s]: ", label, def)
	} else {
		c.env.Errf("%s: ", label)
	}
	if c.stdinReader == nil {
		c.stdinReader = bufio.NewReader(c.env.Stdin)
	}
	line, err := c.stdinReader.ReadString('\n')
	if err != nil && line == "" {
		if errors.Is(err, io.EOF) {
			// EOF with nothing buffered means the user pressed Ctrl-D; take
			// the default rather than looping forever.
			return def, nil
		}
		return "", err
	}
	line = strings.TrimRight(line, "\r\n")
	line = strings.TrimSpace(line)
	if line == "" {
		return def, nil
	}
	return line, nil
}

// promptInt reads an integer with a default.
func (c *client) promptInt(label string, def int) (int, error) {
	raw, err := c.promptLine(label, strconv.Itoa(def))
	if err != nil {
		return 0, err
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%q is not a number", raw)
	}
	return n, nil
}

// promptChoice asks which of the options to use and returns its index.
//
// Choosing by number is deliberate: a discovered peer's name comes from the
// network, and a name typed back at a prompt would mean retyping a string that
// may be long or easy to mistype. Empty input keeps the first option, so the
// suggested default is one keystroke.
func (c *client) promptChoice(options []string) (int, error) {
	if len(options) == 0 {
		return -1, errors.New("no options to choose from")
	}
	if len(options) == 1 {
		return 0, nil
	}
	for i, opt := range options {
		c.env.Errf("  %d) %s\n", i+1, opt)
	}
	raw, err := c.promptLine("Which one", "1")
	if err != nil {
		return -1, err
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return -1, fmt.Errorf("%q is not one of the choices", raw)
	}
	if n < 1 || n > len(options) {
		return -1, fmt.Errorf("%d is out of range: choose 1 to %d", n, len(options))
	}
	return n - 1, nil
}

// maybeAutostartTip prints the exec-once hint once, then records that it has
// been shown so it never repeats.
func (c *client) maybeAutostartTip(hint *startHint) {
	if hint == nil || !hint.shown || c.cfg.AutostartAck {
		return
	}
	c.env.ErrLine("")
	c.env.ErrLine(autostartTip)

	c.cfg.AutostartAck = true
	if err := c.cfg.Save(c.layout); err != nil {
		c.log.Debugf("cannot record the autostart tip: %v", err)
	}
}

// ctx returns a context bounded by the command context.
func ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Minute)
}

// ctxWithTimeout returns a context bounded by d.
func ctxWithTimeout(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}

// longCtx returns a context for operations whose runtime is bounded by the
// network rather than by us, such as transferring a large file. Ctrl-C still
// cancels it because the process exits with the command.
func longCtx() (context.Context, context.CancelFunc) {
	return context.WithCancel(context.Background())
}
