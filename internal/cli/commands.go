package cli

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/siin/lemon/internal/config"
	"github.com/siin/lemon/internal/serve"
	"github.com/siin/lemon/internal/tailscale"
)

// Run dispatches a command and returns a process exit code.
func Run(env *Env, command string, args []string) int {
	var err error
	switch command {
	case "setup":
		err = runSetup(env, args)
	case "serve":
		err = runServe(env, args)
	case "send":
		err = runSend(env, args)
	case "transfer":
		err = runTransfer(env, args)
	case "history":
		err = runHistory(env, args)
	case "peers":
		err = runPeers(env, args)
	case "discover":
		err = runDiscover(env, args)
	case "peer":
		err = runPeer(env, args)
	case "connect":
		err = runConnect(env, args)
	case "disconnect":
		err = runDisconnect(env, args)
	case "status":
		err = runStatus(env, args)
	case "queue":
		err = runQueue(env, args)
	case "flush":
		err = runFlush(env, args)
	case "version":
		env.Line("lemon %s (protocol %d)", Version, ProtocolVersion)
	case "help", "":
		env.Printf("%s", UsageString())
	default:
		env.Errf("lemon: unknown command %q\n", command)
		env.Errf("Run 'lemon help' for the command list.\n")
		return ExitUsage
	}

	if err != nil {
		reportError(env, err)
		return exitFor(env, err)
	}
	return ExitOK
}

// reportError prints a concise message, and a stack trace only in debug mode.
func reportError(env *Env, err error) {
	var ue *UsageError
	if errors.As(err, &ue) {
		env.Errf("lemon: %s\n", ue.msg)
		return
	}
	var are *serve.AlreadyRunningError
	if errors.As(err, &are) {
		env.Errf("%s\n", are.Error())
		return
	}
	var re reportedError
	if errors.As(err, &re) && re.alreadyReported() {
		// The command printed its own explanation; only the status is left.
		return
	}
	env.Errf("%s\n", err.Error())
	if debugEnabled() {
		env.Errf("%+v\n", err)
	}
}

// exitFor maps an error onto an exit status.
func exitFor(env *Env, err error) int {
	var ue *UsageError
	if errors.As(err, &ue) {
		return ExitUsage
	}
	if errors.Is(err, config.ErrNotConfigured) {
		return ExitNotConfigured
	}
	// A queued-but-undelivered message or transfer is a distinct outcome, so
	// scripts can tell "sent later" apart from "failed".
	var off *offlineError
	if errors.As(err, &off) {
		return ExitOffline
	}
	var ce *connectError
	if errors.As(err, &ce) {
		return ExitConnect
	}
	return ExitError
}

// runSetup configures this node and makes lemon usable from the shell.
//
// The order matters. The identity and port are saved first, because the
// listener has to know who it is before it can answer anyone; only then is the
// binary installed, PATH made to include it, and the tailnet searched for a
// peer to talk to. Each step can fail without losing the earlier ones, since
// every one of them is written as it is completed.
func runSetup(env *Env, args []string) error {
	if len(args) > 0 {
		switch args[0] {
		case "--help", "-h":
			env.Printf("Usage: lemon setup\n\nCollects your username and listen port, installs lemon into\nyour user bin directory, adds it to PATH, starts the listener and\nsearches the tailnet for a peer to use as the default.\n\nValues are stored in %s.\n", configPathString())
			return nil
		default:
			return Usage("lemon setup takes no arguments (got %q)", args[0])
		}
	}

	c, err := newClient(env)
	if err != nil {
		return err
	}
	if err := c.layout.EnsureDirs(); err != nil {
		return err
	}

	env.Line("Lemon setup")
	env.Line("Configuration lives in %s", c.layout.Dir)
	env.Line("")

	username, err := c.promptLine("Username", c.cfg.Username)
	if err != nil {
		return err
	}
	if err := config.ValidateName(username, "username"); err != nil {
		return err
	}

	port, err := c.promptInt("Listen port", c.cfg.Port)
	if err != nil {
		return err
	}
	if port <= 0 || port > 65535 {
		return fmt.Errorf("invalid port: %d", port)
	}

	// Save before starting anything: the listener reads this file, and a
	// username that exists only in memory is not a usable node.
	c.cfg.Username = username
	c.cfg.Port = port
	if err := c.cfg.Save(c.layout); err != nil {
		return err
	}
	env.Line("Saved as %s, listening on port %d.", username, port)

	installAndPath(env, c.log)

	// The peer is chosen last: discovering it needs a running listener, and
	// the listener needs the identity saved above.
	peer, found, err := chooseDefaultPeer(env, c)
	if err != nil {
		return err
	}
	if peer == "" {
		c.cfg.DefaultPeer = ""
		if err := c.cfg.Save(c.layout); err != nil {
			return err
		}
		env.Line("")
		env.Line("No default peer set. Add one later with:  lemon peer add <name>")
		env.Line("")
		env.Line("Done. Run:  lemon status")
		return nil
	}

	// Store the address discovery reported, rather than leaving it empty for
	// Tailscale to resolve later. A lemon's username is chosen by its owner and
	// need not match the node's tailnet name, so an address learned from a
	// handshake is the only thing that is guaranteed to work.
	addr := ""
	for _, p := range found {
		if p.User == peer {
			addr = p.Addr
			break
		}
	}
	if _, ok := c.cfg.Peer(peer); !ok {
		if err := c.cfg.SetPeer(peer, addr); err != nil {
			env.Errf("Cannot add peer %q: %v\n", peer, err)
		}
	} else if addr != "" {
		if existing, _ := c.cfg.Peer(peer); existing != nil && existing.Addr == "" {
			existing.Addr = addr
		}
	}
	c.cfg.DefaultPeer = peer
	if err := c.cfg.Save(c.layout); err != nil {
		return err
	}
	env.Line("Added peer %q at %s as the default.", peer, addr)

	env.Line("")
	env.Line("Done. Next:")
	env.Line("  lemon send %s hello", peer)
	env.Line("  lemon status")
	return nil
}

// installAndPath puts lemon on PATH, reporting each step as it happens.
func installAndPath(env *Env, log Logger) {
	res, err := installToUserBin(log)
	if err != nil {
		env.Errf("Could not install into %s: %v\n", res.Dir, err)
		env.Errf("  lemon still works when called by its full path.\n")
		return
	}
	switch {
	case res.Skipped != "":
		env.Line("Already installed: %s", res.Skipped)
	case res.Linked:
		env.Line("Linked %s -> this lemon.", res.Path)
	default:
		env.Line("Installed %s.", res.Path)
	}

	path, err := ensurePathPersisted(res.Dir)
	switch {
	case err != nil:
		env.Errf("Could not add %s to PATH: %v\n", res.Dir, err)
		env.Errf("  Add it yourself with:  export PATH=\"%s:$PATH\"\n", res.Dir)
	case path.AlreadyPresent:
		env.Line("%s is on your PATH.", res.Dir)
	case path.Changed:
		env.Line("Added %s to PATH in %s.", res.Dir, strings.Join(append([]string{path.File}, path.Extra...), " and "))
		env.Line("  new shells will find lemon; this one already can, as %s", res.Path)
		env.Line("  to undo: delete the line marked %s from %s", pathMarker, path.File)
	default:
		env.Errf("%s is not on your PATH. Add it with:  export PATH=\"%s:$PATH\"\n", res.Dir, res.Dir)
	}
}

// chooseDefaultPeer starts the listener, looks for Lemons on the tailnet, and
// asks which one should be the default.
//
// It also returns everything discovery found, because the address it learned is
// worth keeping: a lemon's username is chosen by its owner and need not match
// the node's tailnet name.
//
// Finding peers is a probe of the tailnet rather than a lookup, so a node that
// is not running lemon is simply absent, and a node that is offline costs a
// short timeout instead of a wrong suggestion. When nothing is found the user
// is asked for a name, which is the only way to configure a peer that is
// temporarily down.
func chooseDefaultPeer(env *Env, c *client) (string, []serve.DiscoveredPeer, error) {
	ctx, cancel := ctxWithTimeout(60 * time.Second)
	defer cancel()

	_, hint, err := c.connect(ctx)
	if err != nil {
		// Without a listener there is nothing to be discovered by, but the
		// identity is already saved, so setup can still finish.
		env.Errf("Could not start the listener: %v\n", err)
		env.Errf("Start it later with:  lemon serve\n")
		peer, perr := c.promptPeerName()
		return peer, nil, perr
	}
	defer c.close()
	env.Line("Listener running on port %d.", c.cfg.Port)

	resp, err := c.do(ctx, serve.Request{Op: serve.OpDiscover, User: c.cfg.Username}, nil)
	if err != nil {
		env.Errf("Could not search the tailnet: %v\n", err)
		peer, perr := c.promptPeerName()
		return peer, nil, perr
	}
	c.maybeAutostartTip(hint)

	if !resp.OK {
		env.Errf("Could not search the tailnet: %s\n", friendlyError(resp))
		peer, perr := c.promptPeerName()
		return peer, nil, perr
	}

	env.Line("")
	switch len(resp.Discovered) {
	case 0:
		env.Line("No other lemon found on the tailnet (probed %d node(s)).", resp.Scanned)
		env.Line("A peer appears once it runs 'lemon serve'.")
		env.Line("")
		peer, perr := c.promptPeerName()
		return peer, nil, perr
	case 1:
		p := resp.Discovered[0]
		env.Line("Found a lemon on the tailnet: %s at %s.", p.User, p.Addr)
		return p.User, resp.Discovered, nil
	}

	env.Line("Found %d lemons on the tailnet:", len(resp.Discovered))
	options := make([]string, 0, len(resp.Discovered))
	for _, p := range resp.Discovered {
		latency := ""
		if p.LatencyMS > 0 {
			latency = fmt.Sprintf("  %dms", p.LatencyMS)
		}
		note := ""
		switch {
		case p.User == c.cfg.DefaultPeer:
			note = "  (current default)"
		case p.Configured:
			note = "  (already a peer)"
		}
		options = append(options, fmt.Sprintf("%s  %s%s%s", p.User, p.Addr, latency, note))
	}
	fmt.Fprintln(env.Stderr, "")
	idx, err := c.promptChoice(options)
	if err != nil {
		return "", nil, err
	}
	return resp.Discovered[idx].User, resp.Discovered, nil
}

// promptPeerName asks for a peer name when none could be discovered.
func (c *client) promptPeerName() (string, error) {
	peer, err := c.promptLine("Default peer", c.cfg.DefaultPeer)
	if err != nil {
		return "", err
	}
	if peer == "" {
		return "", nil
	}
	if err := config.ValidateName(peer, "peer name"); err != nil {
		return "", err
	}
	return peer, nil
}

// orNone renders an empty optional value.
func orNone(s string) string {
	if s == "" {
		return "(none)"
	}
	return s
}

// configPathString renders the config file path for help text.
func configPathString() string {
	layout, err := config.Paths()
	if err != nil {
		return "~/.config/lemon"
	}
	return layout.Dir
}

// runPeers lists configured peers with live status.
func runPeers(env *Env, args []string) error {
	if len(args) > 0 {
		return Usage("lemon peers takes no arguments (got %q)", args[0])
	}
	c, err := newClient(env)
	if err != nil {
		return err
	}
	if err := c.requireConfigured(); err != nil {
		return err
	}
	ctx, cancel := ctx()
	defer cancel()

	conn, hint, err := c.connect(ctx)
	if err != nil {
		return err
	}
	defer c.close()

	resp, err := c.do(ctx, serve.Request{Op: serve.OpPeers, Probe: true}, nil)
	if err != nil {
		return err
	}
	c.maybeAutostartTip(hint)
	_ = conn

	if len(resp.Peers) == 0 {
		env.Line("No peers yet.")
		env.Line("")
		env.Line("Add one with:  lemon peer add alice")
		return nil
	}

	env.Line("%-10s %-22s %s", "PEER", "ADDRESS", "STATUS")
	for _, p := range resp.Peers {
		addr := p.Addr
		if addr == "" {
			addr = "-"
		}
		status := "offline"
		switch {
		case p.Disabled:
			status = "disabled"
		case p.Online:
			status = "connected"
			if p.LatencyMS > 0 {
				status = fmt.Sprintf("connected  %dms", p.LatencyMS)
			}
		case p.Reason != "":
			status = "offline  " + p.Reason
		}
		marker := ""
		if p.IsDefault {
			marker = "  (default)"
		}
		env.Line("%-10s %-22s %s%s", p.Name, addr, status, marker)
	}
	return nil
}

// runDiscover finds the Lemons running on this tailnet.
//
// It is the same probe `lemon setup` uses, exposed on its own so a node can be
// found later without re-running setup.
func runDiscover(env *Env, args []string) error {
	for _, a := range args {
		switch a {
		case "--help", "-h":
			env.Printf("Usage: lemon discover\n\nFinds Lemons running on this tailnet by connecting to each node's\nLemon port and completing the protocol handshake. The peer's own username\ncomes back from that handshake, so nothing needs to be configured first.\n\nThe results are not saved. Use: lemon peer add <name> [address]\n")
			return nil
		default:
			return Usage("lemon discover takes no arguments (got %q)", a)
		}
	}

	c, err := newClient(env)
	if err != nil {
		return err
	}
	if err := c.requireConfigured(); err != nil {
		return err
	}
	// Discovery fans out over the tailnet, so it gets a longer budget than a
	// local status check.
	ctx, cancel := ctxWithTimeout(30 * time.Second)
	defer cancel()

	_, hint, err := c.connect(ctx)
	if err != nil {
		return err
	}
	defer c.close()

	resp, err := c.do(ctx, serve.Request{Op: serve.OpDiscover, User: c.cfg.Username}, nil)
	if err != nil {
		return err
	}
	c.maybeAutostartTip(hint)

	if !resp.OK {
		env.Errf("cannot search the tailnet: %s\n", friendlyError(resp))
		return &sendError{msg: resp.Error, reported: true}
	}
	printDiscovered(env, resp, c.cfg.DefaultPeer)
	return nil
}

// printDiscovered lists the peers found on the tailnet.
func printDiscovered(env *Env, resp *serve.Response, currentDefault string) {
	if len(resp.Discovered) == 0 {
		env.Line("No other Lemon found on this tailnet.")
		env.Line("")
		env.Line("Probed %d node(s). A peer appears once it runs 'lemon serve'.", resp.Scanned)
		env.Line("")
		env.Line("Add one by name with:  lemon peer add <name> [address]")
		return
	}

	env.Line("Found %d Lemon(s) on this tailnet (probed %d node(s)):", len(resp.Discovered), resp.Scanned)
	env.Line("")
	env.Line("%-14s %-22s %-10s %s", "USER", "ADDRESS", "LATENCY", "STATE")
	for _, p := range resp.Discovered {
		latency := "-"
		if p.LatencyMS > 0 {
			latency = fmt.Sprintf("%dms", p.LatencyMS)
		}
		state := "new"
		switch {
		case p.User == currentDefault:
			state = "current default"
		case p.Configured:
			state = "already a peer"
		}
		env.Line("%-14s %-22s %-10s %s", p.User, p.Addr, latency, state)
	}
}

// runPeer implements the `peer` subcommands.
func runPeer(env *Env, args []string) error {
	if len(args) == 0 {
		return Usage("usage: lemon peer add <name> [address] | lemon peer rm <name>")
	}
	sub := args[0]
	rest := args[1:]

	c, err := newClient(env)
	if err != nil {
		return err
	}
	if err := c.requireConfigured(); err != nil {
		return err
	}

	switch sub {
	case "add", "rm", "remove", "del":
	default:
		return Usage("unknown peer subcommand %q (use: add, rm)", sub)
	}
	if len(rest) == 0 {
		return Usage("lemon peer %s needs a peer name", sub)
	}

	name := rest[0]
	peer, exists := c.cfg.Peer(name)
	if err := config.ValidateName(name, "peer name"); err != nil {
		return err
	}

	switch sub {
	case "add":
		var addr string
		if len(rest) > 1 {
			// A bare host is completed with our own port, which is what an
			// operator almost always means: lemon peer add alice 100.1.2.3
			raw := strings.Join(rest[1:], " ")
			normalised, err := config.NormalizeAddr(raw)
			if err != nil {
				return err
			}
			addr = normalised
		}
		if exists && peer.Addr == "" && addr == "" {
			// Already present and still relying on discovery; nothing to do.
			env.Line("%s is already a peer (its address is resolved automatically).", name)
			return nil
		}
		if err := c.cfg.SetPeer(name, addr); err != nil {
			return err
		}
		if err := c.cfg.Save(c.layout); err != nil {
			return err
		}
		if addr != "" {
			env.Line("added %s -> %s", name, addr)
		} else {
			env.Line("added %s (its address is resolved automatically)", name)
		}
		return nil

	default: // rm
		if !exists {
			return fmt.Errorf("unknown peer: %s (known peers: %s)",
				name, orNone(strings.Join(c.cfg.PeerNames(), ", ")))
		}
		if !c.cfg.RemovePeer(name) {
			return fmt.Errorf("unknown peer: %s", name)
		}
		if err := c.cfg.Save(c.layout); err != nil {
			return err
		}
		env.Line("removed %s", name)
		return nil
	}
}

// runConnect checks that a peer is reachable.
func runConnect(env *Env, args []string) error {
	if len(args) != 1 {
		return Usage("usage: lemon connect <peer|address>")
	}
	target := args[0]
	c, err := newClient(env)
	if err != nil {
		return err
	}
	if err := c.requireConfigured(); err != nil {
		return err
	}
	ctx, cancel := ctx()
	defer cancel()

	_, hint, err := c.connect(ctx)
	if err != nil {
		return err
	}
	defer c.close()

	req := serve.Request{Op: serve.OpConnect, User: c.cfg.Username}
	if looksLikeAddress(target) {
		req.Addr = target
	} else {
		req.Peer = target
	}
	resp, err := c.do(ctx, req, nil)
	if err != nil {
		return err
	}
	if !resp.OK {
		env.Errf("cannot connect to %s: %s\n", target, resp.Error)
		return &connectError{msg: resp.Error, reported: true}
	}
	c.maybeAutostartTip(hint)

	name := resp.Peer
	if name == "" {
		name = target
	}
	env.Line("connected to %s", name)
	if resp.LatencyMS > 0 {
		env.Line("%s", formatLatency(resp.LatencyMS))
	}
	if resp.Addr != "" {
		env.Line("address: %s", resp.Addr)
	}
	return nil
}

// connectError signals a connection failure, which exits 5.
type connectError struct {
	msg      string
	reported bool
}

func (e *connectError) Error() string { return e.msg }

func (e *connectError) alreadyReported() bool { return e.reported }

// runDisconnect drops a peer.
func runDisconnect(env *Env, args []string) error {
	if len(args) < 1 || len(args) > 2 {
		return Usage("usage: lemon disconnect <peer> [--keep]")
	}
	name := args[0]
	keep := false
	for _, a := range args[1:] {
		switch a {
		case "--keep":
			keep = true
		default:
			return Usage("unknown option %q", a)
		}
	}
	c, err := newClient(env)
	if err != nil {
		return err
	}
	if err := c.requireConfigured(); err != nil {
		return err
	}
	ctx, cancel := ctx()
	defer cancel()

	_, _, err = c.connect(ctx)
	if err != nil {
		return err
	}
	defer c.close()

	resp, err := c.do(ctx, serve.Request{
		Op: serve.OpDisconnect, Peer: name, KeepEnabled: keep,
	}, nil)
	if err != nil {
		return err
	}
	if !resp.OK {
		env.Errf("%s\n", resp.Error)
		return &UsageError{msg: resp.Error}
	}
	if keep {
		env.Line("disconnected %s (still configured)", name)
	} else {
		env.Line("disconnected %s (disabled; re-enable with: lemon peer add %s)", name, name)
	}
	return nil
}

// runQueue lists pending work.
func runQueue(env *Env, args []string) error {
	if len(args) > 0 {
		return Usage("lemon queue takes no arguments (got %q)", args[0])
	}
	c, err := newClient(env)
	if err != nil {
		return err
	}
	if err := c.requireConfigured(); err != nil {
		return err
	}
	ctx, cancel := ctx()
	defer cancel()

	_, _, err = c.connect(ctx)
	if err != nil {
		return err
	}
	defer c.close()

	resp, err := c.do(ctx, serve.Request{Op: serve.OpQueue}, nil)
	if err != nil {
		return err
	}
	if !resp.OK {
		return errors.New(resp.Error)
	}
	if len(resp.History) == 0 {
		env.Line("Queue is empty.")
		return nil
	}
	env.Line("%-8s %-10s %s", "PEER", "AGE", "CONTENT")
	for _, e := range resp.History {
		env.Line("%-8s %-10s %s", e.Peer, shortAge(e.Time), oneLine(e.Content, 60))
	}
	return nil
}

// runFlush asks the daemon to retry queued work now.
func runFlush(env *Env, args []string) error {
	if len(args) > 0 {
		return Usage("lemon flush takes no arguments (got %q)", args[0])
	}
	c, err := newClient(env)
	if err != nil {
		return err
	}
	if err := c.requireConfigured(); err != nil {
		return err
	}
	ctx, cancel := ctx()
	defer cancel()

	_, _, err = c.connect(ctx)
	if err != nil {
		return err
	}
	defer c.close()

	resp, err := c.do(ctx, serve.Request{Op: serve.OpFlush}, nil)
	if err != nil {
		return err
	}
	if !resp.OK {
		return errors.New(resp.Error)
	}
	env.Line("retrying queued messages and transfers")
	return nil
}

// runStatus prints the full status report.
func runStatus(env *Env, args []string) error {
	if len(args) > 0 {
		return Usage("lemon status takes no arguments (got %q)", args[0])
	}
	c, err := newClient(env)
	if err != nil {
		return err
	}
	if err := c.requireConfigured(); err != nil {
		return err
	}
	ctx, cancel := ctx()
	defer cancel()

	_, hint, err := c.connect(ctx)
	if err != nil {
		return err
	}
	defer c.close()

	resp, err := c.do(ctx, serve.Request{Op: serve.OpStatus, Probe: true}, nil)
	if err != nil {
		return err
	}
	if !resp.OK || resp.Status == nil {
		return errors.New(orDefault(resp.Error, "cannot read status"))
	}
	c.maybeAutostartTip(hint)
	printStatus(env, c, resp.Status)
	return nil
}

// printStatus renders the status report as plain text.
func printStatus(env *Env, c *client, st *serve.StatusReport) {
	env.Line("Lemon")
	env.Line("user:       %s", orDefault(st.User, "(unset)"))
	env.Line("listener:   %s (pid %d)", st.ListenerAddr, st.ListenerPID)
	env.Line("port:       %d", st.Port)

	ts := "not running"
	if st.Tailscale != nil {
		if st.Tailscale.OK {
			ts = "connected"
			if len(st.Tailscale.IPs) > 0 {
				ts += "  " + st.Tailscale.IPs[0]
			}
			if st.Tailscale.Name != "" {
				ts += "  (" + st.Tailscale.Name + ")"
			}
		} else if st.Tailscale.Error != "" {
			ts = st.Tailscale.Error
		}
	}
	env.Line("tailscale:  %s", ts)
	env.Line("notify:     %s", notifyState(st.Notification))
	env.Line("protocol:   v%d", st.Version)

	env.Line("")
	if len(st.Peers) == 0 {
		env.Line("Peers")
		env.Line("(none configured - add one with: lemon peer add alice)")
	} else {
		env.Line("Peers")
		for _, p := range st.Peers {
			state := "offline"
			switch {
			case p.Disabled:
				state = "disabled"
			case p.Online:
				state = "connected"
				if p.LatencyMS > 0 {
					state += "  " + formatLatency(p.LatencyMS)
				}
			}
			// The state column is a minimum, not a limit: "connected  378ms" is
			// wider than the padding, and without a trailing space the address
			// would run straight into the latency.
			line := fmt.Sprintf("%-10s %-12s ", p.Name, state)
			switch {
			case !p.Online && p.LastSeenS != "":
				line += "last seen " + p.LastSeenS
			case !p.Online && p.Reason != "":
				line += p.Reason
			case p.Online && p.Addr != "":
				line += p.Addr
			}
			if p.IsDefault {
				line += "  (default)"
			}
			env.Line("%s", line)
		}
	}

	env.Line("")
	env.Line("Queue")
	env.Line("messages:   %d", st.QueuedMsgs)
	env.Line("transfers:  %d", st.QueuedXfers)
	if st.ActiveXfers > 0 {
		env.Line("active:     %d", st.ActiveXfers)
	}

	if st.AutostartTip && !c.cfg.AutostartAck {
		env.Line("")
		env.Line("Tip: add 'lemon serve' to your compositor's exec-once configuration")
		env.Line("so Lemon starts automatically at login.")
	}
}

// notifyState renders the notification dispatcher.
func notifyState(n *serve.NotificationReport) string {
	if n == nil || n.Command == "" {
		return "not available"
	}
	return n.Command
}

// runHistory prints stored messages.
func runHistory(env *Env, args []string) error {
	limit := 0
	var peer string
	for i := 0; i < len(args); i++ {
		switch a := args[i]; {
		case a == "-n" || a == "--limit":
			if i+1 >= len(args) {
				return Usage("%s needs a number", a)
			}
			i++
			n, err := strconv.Atoi(args[i])
			if err != nil || n <= 0 {
				return Usage("%q is not a positive number", args[i])
			}
			limit = n
		case strings.HasPrefix(a, "-"):
			return Usage("unknown option %q", a)
		default:
			if peer != "" {
				return Usage("lemon history accepts a single peer name (got %q and %q)", peer, a)
			}
			peer = a
		}
	}

	c, err := newClient(env)
	if err != nil {
		return err
	}
	if err := c.requireConfigured(); err != nil {
		return err
	}
	ctx, cancel := ctx()
	defer cancel()

	// History is a local read; the daemon owns the database, so ask it.
	_, _, err = c.connect(ctx)
	if err != nil {
		return err
	}
	defer c.close()

	resp, err := c.do(ctx, serve.Request{
		Op: serve.OpHistory, HistoryPeer: peer, HistoryLimit: limit,
	}, nil)
	if err != nil {
		return err
	}
	if !resp.OK {
		return errors.New(resp.Error)
	}
	if len(resp.History) == 0 {
		if peer != "" {
			env.Line("No messages with %s yet.", peer)
		} else {
			env.Line("No messages yet.")
			env.Line("")
			env.Line("Send one with:  lemon send hello")
		}
		return nil
	}
	// Readable plain output: "peer: content" with a time column when the
	// conversation is long enough to need it.
	for _, m := range resp.History {
		sender := m.Sender
		if m.Direction == "out" {
			sender = "you"
		}
		line := fmt.Sprintf("%s: %s", sender, oneLine(m.Content, 200))
		if m.Status == "queued" {
			line += "  (queued)"
		}
		env.Line("%s", line)
	}
	return nil
}

// orDefault returns s or def when s is empty.
func orDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

// oneLine collapses whitespace and truncates to n runes.
func oneLine(s string, n int) string {
	s = strings.ReplaceAll(s, "\r", " ")
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.Join(strings.Fields(s), " ")
	r := []rune(s)
	if len(r) > n {
		return string(r[:n-1]) + "…"
	}
	return s
}

// formatLatency renders a millisecond latency.
func formatLatency(ms int) string { return fmt.Sprintf("%dms", ms) }

// looksLikeAddress reports whether s looks like host:port or an IP.
func looksLikeAddress(s string) bool {
	if strings.Contains(s, ":") {
		_, _, err := config.SplitHostPort(s)
		return err == nil
	}
	return false
}

// tailscaleErrText is used when the daemon reports a tailnet problem.
func tailscaleErrText(err error) string {
	if errors.Is(err, tailscale.ErrNotRunning) || errors.Is(err, tailscale.ErrNotInstalled) {
		return err.Error()
	}
	return err.Error()
}
