package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/siin/lemon/internal/config"
	"github.com/siin/lemon/internal/logging"
	"github.com/siin/lemon/internal/serve"
)

// Version is the build version, set with -ldflags at release time.
var Version = "dev"

// ProtocolVersion is the wire protocol version this build speaks.
const ProtocolVersion = 1

// debugEnabled reports whether verbose logging is on.
func debugEnabled() bool {
	level := strings.ToLower(strings.TrimSpace(os.Getenv(logging.EnvVar)))
	switch level {
	case "debug", "trace", "verbose":
		return true
	}
	return false
}

// runServe runs the listener in the foreground.
//
// This is the command a user puts in their compositor's exec-once. It is not an
// interactive chat session: it prints a short banner, then only reports
// events as they happen.
func runServe(env *Env, args []string) error {
	for _, a := range args {
		switch a {
		case "--help", "-h":
			env.Printf("Usage: lemon serve\n\nRuns the Lemon listener in the foreground.\nAdd 'lemon serve' to your compositor's exec-once configuration\nto have Lemon start at login.\n")
			return nil
		default:
			return Usage("lemon serve takes no arguments (got %q)", a)
		}
	}

	c, err := newClient(env)
	if err != nil {
		return err
	}
	if err := c.layout.EnsureDirs(); err != nil {
		return err
	}
	if err := c.requireConfigured(); err != nil {
		return err
	}

	d, err := serve.New(serve.Options{
		Layout:               c.layout,
		Config:               c.cfg,
		Logger:               c.log,
		NotificationsEnabled: c.notifyEnabled(),
		NotifierOverride:     c.cfg.NotifyCommand,
		// A foreground listener prints sparse event lines; an auto-started one
		// has no console and relies on the log file instead.
		Console: env.Stdout,
	})
	if err != nil {
		return err
	}
	defer d.Close()

	// A second serve must exit quietly, not fight over the port.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	go func() {
		s := <-sigs
		c.log.Infof("received %v, shutting down", s)
		cancel()
	}()

	env.Line("")
	env.Line("Lemon")
	env.Line("user: %s", c.cfg.Username)
	env.Line("listening: %s", config.WithPort(c.cfg.ListenHost(), c.cfg.Port))
	if notif := c.notifierCommand(); notif != "" {
		env.Line("notify: %s", notif)
	}
	if !c.cfg.AutostartAck {
		env.Line("")
		env.Line("%s", strings.ReplaceAll(autostartTip, "\n\n", "\n"))
		// Remember that the tip was shown in a foreground run too.
		cfg := *c.cfg
		cfg.AutostartAck = true
		if err := cfg.Save(c.layout); err != nil {
			c.log.Debugf("cannot record the autostart tip: %v", err)
		}
	}
	env.Line("")

	if err := d.Start(ctx); err != nil {
		var already *serve.AlreadyRunningError
		if errors.As(err, &already) {
			return already
		}
		var inUse *serve.PortInUseError
		if errors.As(err, &inUse) {
			return fmt.Errorf("%s (stop it, or choose another port with: lemon setup)", inUse.Error())
		}
		return err
	}
	return nil
}

// notifierCommand returns the notification command that will be used.
func (c *client) notifierCommand() string {
	override := c.cfg.NotifyCommand
	if override == "" {
		override = os.Getenv("LEMON_NOTIFY")
	}
	return notifierName(override)
}

// notifyEnabled reports whether desktop notifications are on.
func (c *client) notifyEnabled() bool {
	switch strings.ToLower(os.Getenv("LEMON_NOTIFY")) {
	case "0", "off", "false", "no":
		return false
	}
	return true
}

// notifierName mirrors the notifier package's resolution for display.
func notifierName(override string) string {
	if override != "" {
		return override
	}
	for _, candidate := range []string{"notify", "notify-send"} {
		if path, err := lookPath(candidate); err == nil {
			if candidate == "notify" {
				return "notify send"
			}
			return path
		}
	}
	return ""
}

// lookPath is exec.LookPath, wrapped in a variable so tests can stub it.
var lookPath = exec.LookPath

// absPath is filepath.Abs, wrapped in a variable so tests can stub it.
var absPath = filepath.Abs

// runSend sends a message.
func runSend(env *Env, args []string) error {
	to := ""
	var body []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--to" || a == "-t":
			if i+1 >= len(args) {
				return Usage("%s needs a peer name", a)
			}
			i++
			to = args[i]
		case strings.HasPrefix(a, "--to="):
			to = strings.TrimPrefix(a, "--to=")
		case a == "--help" || a == "-h":
			env.Printf("Usage: lemon send [peer] <text...>\n\nSends text to a peer over Tailscale. With no peer given, the message\ngoes to the default peer from 'lemon setup'. The first argument is\ntreated as a peer name only when it matches a configured peer.\n\nExamples:\n  lemon send hello\n  lemon send \"hello alice\"\n  lemon send alice hello\n  lemon send --to alice hello there\n")
			return nil
		case a == "--addr":
			return Usage("--addr is not supported; add the peer first with: lemon peer add <name> <ip>")
		case strings.HasPrefix(a, "-") && a != "-":
			return Usage("unknown option %q", a)
		default:
			body = append(body, a)
		}
	}
	if len(body) == 0 && to == "" {
		return Usage("nothing to send. usage: lemon send [peer] <text...>")
	}

	c, err := newClient(env)
	if err != nil {
		return err
	}
	if err := c.requireConfigured(); err != nil {
		return err
	}

	peer, text, err := splitPeerMessage(body, c.isKnownPeer, to)
	if err != nil {
		return err
	}
	if strings.TrimSpace(text) == "" {
		return Usage("nothing to send: the message is empty")
	}

	ctx, cancel := ctx()
	defer cancel()

	_, hint, err := c.connect(ctx)
	if err != nil {
		return err
	}
	defer c.close()

	resp, err := c.do(ctx, serve.Request{
		Op: serve.OpSend, User: c.cfg.Username, Peer: peer, Message: text,
	}, nil)
	if err != nil {
		return err
	}
	c.maybeAutostartTip(hint)

	if resp.OK {
		return nil
	}
	if resp.Offline && resp.Queued {
		// The daemon's event stream already reported "…: offline" and
		// "message queued"; add only the reassurance, and never repeat the
		// raw daemon error on top of it.
		env.Errf("It will be delivered automatically when %s is reachable.\n", resp.Peer)
		return &offlineError{msg: resp.Error, reported: true}
	}
	env.Errf("%s\n", friendlyError(resp))
	return &sendError{msg: resp.Error, reported: true}
}

// offlineError signals queued-but-undelivered, which exits 3.
type offlineError struct {
	msg string
	// reported marks a message the command already explained to the user, so
	// the top-level handler only sets the exit status.
	reported bool
}

func (e *offlineError) Error() string { return e.msg }

// sendError signals a delivery failure.
type sendError struct {
	msg      string
	reported bool
}

func (e *sendError) Error() string { return e.msg }

// reportedError is satisfied by errors whose message is already on screen.
type reportedError interface {
	error
	alreadyReported() bool
}

func (e *offlineError) alreadyReported() bool { return e.reported }
func (e *sendError) alreadyReported() bool    { return e.reported }

// friendlyError renders a daemon error for humans.
func friendlyError(resp *serve.Response) string {
	switch resp.Code {
	case serve.CodeOffline:
		return "peer is offline."
	case serve.CodeUnknownPeer:
		return resp.Error
	case serve.CodeNoDefault:
		return resp.Error
	default:
		if resp.Error == "" {
			return "the message could not be delivered."
		}
		return resp.Error
	}
}

// runTransfer sends one or more files.
func runTransfer(env *Env, args []string) error {
	var (
		to     string
		files  []string
		resume = true
	)
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--to" || a == "-t":
			if i+1 >= len(args) {
				return Usage("%s needs a peer name", a)
			}
			i++
			to = args[i]
		case strings.HasPrefix(a, "--to="):
			to = strings.TrimPrefix(a, "--to=")
		case a == "--no-resume":
			resume = false
		case a == "--help" || a == "-h":
			env.Printf("Usage: lemon transfer [options] <file>...\n\nStreams files to a peer. Files are saved on their ~/Public.\nInterrupted transfers resume from the bytes already received.\n\nOptions:\n  --to <peer>   send to a specific peer instead of the default\n  --no-resume   restart files from the beginning\n")
			return nil
		case strings.HasPrefix(a, "-") && a != "-":
			return Usage("unknown option %q", a)
		default:
			files = append(files, a)
		}
	}
	if len(files) == 0 {
		return Usage("no files given. usage: lemon transfer <file...>")
	}

	c, err := newClient(env)
	if err != nil {
		return err
	}
	if err := c.requireConfigured(); err != nil {
		return err
	}
	// A transfer takes as long as the network takes, so it must not inherit the
	// short command budget used by status and send.
	ctx, cancel := longCtx()
	defer cancel()

	_, hint, err := c.connect(ctx)
	if err != nil {
		return err
	}
	defer c.close()

	// Verify every file before touching the network, so a typo in the last
	// argument does not leave a half-finished transfer behind.
	abs := make([]string, 0, len(files))
	for _, f := range files {
		expanded, err := expandPath(f)
		if err != nil {
			return err
		}
		if _, err := os.Stat(expanded); err != nil {
			if os.IsNotExist(err) {
				return fmt.Errorf("no such file: %s", f)
			}
			return fmt.Errorf("cannot read %s: %w", f, err)
		}
		abs = append(abs, expanded)
	}

	// Live progress: the daemon streams byte updates as they happen, so the bar
	// reflects a real transfer rather than a replay once it has already ended.
	// A non-terminal run sends a quiet request instead and renders from the
	// response, which keeps piped output to one greppable line per file.
	prog := newProgress(env)
	onEvent := func(ev serve.Event) {
		switch ev.Kind {
		case "progress":
			prog.Update(ev.Name, ev.N, ev.Done)
		case "done":
			prog.Done(ev.Name, ev.Done, false)
		}
	}

	resp, err := c.do(ctx, serve.Request{
		Op: serve.OpTransfer, User: c.cfg.Username, Peer: to, Files: abs,
		Resume: resume, WantProgress: env.IsTTY, Quiet: !env.IsTTY,
	}, onEvent)
	if err != nil {
		prog.Abort()
		return err
	}
	c.maybeAutostartTip(hint)

	target := resp.Peer
	if target == "" {
		target = to
	}
	if env.IsTTY {
		prog.Start(target)
	}
	// Replay the final counts for files whose stream ended before the header
	// was drawn, so a very fast transfer still shows a completed bar.
	prog.Replay(resp.Transfers)
	prog.Finish()

	if resp.Offline && resp.Queued {
		// The event stream already said the peer is offline and the transfer
		// is queued; keep the output to one clear explanation.
		env.Errf("It will resume automatically when %s is reachable.\n", target)
		return &offlineError{msg: resp.Error, reported: true}
	}
	if !resp.OK {
		// On a terminal the live rows were already drawn, so only the
		// failures are worth repeating.
		printTransferReports(env, resp.Transfers, env.IsTTY)
		env.Errf("%s\n", friendlyError(resp))
		return &sendError{msg: resp.Error, reported: true}
	}
	printTransferReports(env, resp.Transfers, env.IsTTY)
	env.Line("")
	if len(resp.Transfers) == 1 {
		env.Line("1 file transferred ✓")
	} else {
		env.Line("%d files transferred ✓", len(resp.Transfers))
	}
	if target != "" {
		env.Line("saved by %s in ~/Public", target)
	}
	return nil
}

// printTransferReports lists per-file outcomes, skipping the progress bar.
func printTransferReports(env *Env, reports []serve.TransferReport, live bool) {
	for _, r := range reports {
		switch {
		case r.Err != "":
			env.Errf("%-24s %s\n", padRight(r.Name, 24), r.Err)
		case live:
			// Already drawn in place while the transfer ran.
		case r.Resumed:
			env.Line("%-24s %-9s %s 100%% (resumed)", padRight(r.Name, 24), formatSize(r.Size), blockBar(r.Size, r.Size))
		default:
			env.Line("%-24s %-9s %s 100%%", padRight(r.Name, 24), formatSize(r.Size), blockBar(r.Size, r.Size))
		}
	}
}

// blockBar is bar at 100%, used for completed-file summaries.
func blockBar(n, done int64) string { return bar(n, done, barWidth) }

// expandPath resolves ~ and makes a path absolute.
func expandPath(p string) (string, error) {
	if strings.HasPrefix(p, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("cannot resolve ~: %w", err)
		}
		p = home + p[1:]
	}
	abs, err := absPath(p)
	if err != nil {
		return "", err
	}
	return abs, nil
}

// shortAge renders a timestamp as a compact relative age.
func shortAge(unix int64) string {
	if unix <= 0 {
		return "?"
	}
	d := time.Since(time.Unix(unix, 0))
	switch {
	case d < time.Minute:
		return "now"
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	}
}
