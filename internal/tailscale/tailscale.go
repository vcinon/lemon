// Package tailscale wraps the `tailscale` CLI.
//
// Lemon deliberately does not reimplement anything Tailscale already does. It
// shells out to `tailscale status --json` for discovery and `tailscale ping`
// for latency. When Tailscale is absent or not running, Lemon says so and
// stops; there is no alternative Internet transport.
package tailscale

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Errors returned by this package.
var (
	// ErrNotInstalled means the tailscale binary is not on PATH.
	ErrNotInstalled = errors.New("Tailscale is not installed.")
	// ErrNotRunning means tailscaled is not running or not logged in.
	ErrNotRunning = errors.New("Tailscale is not running.")
	// ErrNotFound means a peer name could not be resolved.
	ErrNotFound = errors.New("peer not found on the tailnet")
)

// Backend states reported by tailscale.
const (
	StateRunning    = "Running"
	StateNoState    = "NoState"
	StateStopped    = "Stopped"
	StateNeedsLogin = "NeedsLogin"
)

// Timeouts for CLI invocations.
const (
	DefaultTimeout = 5 * time.Second
	PingTimeout    = 4 * time.Second
)

// Peer is one tailnet node.
type Peer struct {
	// Name is the node's short hostname.
	Name string
	// DNSName is the fully qualified MagicDNS name, with trailing dot.
	DNSName string
	// HostName is the OS hostname.
	HostName string
	// IP is the node's first IPv4 address, preferred for direct dialing.
	IP string
	// Addr is IP:port on the default Lemon port.
	Addr string
	// Online reports current reachability.
	Online bool
	// LastSeen is when the node was last active; zero when never seen.
	LastSeen time.Time
	// Self marks the local node.
	Self bool
}

// status is the subset of `tailscale status --json` we consume.
type status struct {
	BackendState string           `json:"BackendState"`
	Self         *node            `json:"Self"`
	Peer         map[string]*node `json:"Peer"`
	TailscaleIPs []string         `json:"TailscaleIPs"`
}

type node struct {
	HostName     string   `json:"HostName"`
	DNSName      string   `json:"DNSName"`
	TailscaleIPs []string `json:"TailscaleIPs"`
	Online       bool     `json:"Online"`
	LastSeen     string   `json:"LastSeen"`
}

// Local describes the local tailnet node.
type Local struct {
	// State is the backend state, e.g. "Running".
	State string
	// IPs are this node's Tailscale addresses.
	IPs []string
	// Name is this node's short hostname.
	Name string
	// Online is true when the backend is Running.
	Online bool
}

// Runner executes tailscale commands. Tests substitute a fake.
type Runner interface {
	Run(ctx context.Context, name string, args ...string) ([]byte, error)
	LookPath(file string) (string, error)
}

// execRunner runs real commands.
type execRunner struct{}

// Run executes a command and returns its stdout.
func (execRunner) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		return nil, fmt.Errorf("%s %s: %s", name, strings.Join(args, " "), msg)
	}
	return out, nil
}

// LookPath resolves a binary on PATH.
func (execRunner) LookPath(file string) (string, error) { return exec.LookPath(file) }

// Source is what a caller needs from the local tailnet.
//
// It exists so the code that reasons about peers — the registry, the resolver,
// discovery — can be tested without a tailnet, and so those callers are not
// tied to the CLI implementation. *Client implements it.
type Source interface {
	// Peers lists the other nodes on the tailnet.
	Peers(ctx context.Context) ([]Peer, error)
	// Local describes this node.
	Local(ctx context.Context) (Local, error)
	// Resolve finds the node behind a hostname or a name, for callers that
	// already know who they want to reach.
	Resolve(ctx context.Context, nameOrAddr string) (Peer, error)
	// SetPort tells the client which Lemon port peer addresses should carry.
	// The port is a property of the local lemon, so it changes when the config
	// is rewritten rather than when the tailnet changes.
	SetPort(port int)
	// Invalidate drops the cached status, forcing the next read to re-run the
	// CLI.
	Invalidate()
}

// Client queries the local tailscale instance.
type Client struct {
	runner Runner
	port   int

	mu    sync.Mutex
	cache *status
	at    time.Time
}

// cacheTTL bounds how often we re-run the CLI.
const cacheTTL = 3 * time.Second

// New returns a Client using the real tailscale CLI, defaulting to port.
func New(port int) *Client { return &Client{runner: execRunner{}, port: port} }

// NewWithRunner returns a Client with an injected runner, for tests.
func NewWithRunner(r Runner, port int) *Client { return &Client{runner: r, port: port} }

// SetPort updates the port used when building peer addresses.
func (c *Client) SetPort(port int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.port = port
}

// Port returns the Lemon port used for peer addresses.
func (c *Client) Port() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.port <= 0 {
		return 6767
	}
	return c.port
}

// Invalidate clears the discovery cache.
func (c *Client) Invalidate() {
	c.mu.Lock()
	c.cache, c.at = nil, time.Time{}
	c.mu.Unlock()
}

// fetch runs `tailscale status --json`, caching briefly.
func (c *Client) fetch(ctx context.Context) (*status, error) {
	c.mu.Lock()
	if c.cache != nil && time.Since(c.at) < cacheTTL {
		st := c.cache
		c.mu.Unlock()
		return st, nil
	}
	c.mu.Unlock()

	if _, err := c.runner.LookPath("tailscale"); err != nil {
		return nil, ErrNotInstalled
	}

	ctx, cancel := context.WithTimeout(ctx, DefaultTimeout)
	defer cancel()

	out, err := c.runner.Run(ctx, "tailscale", "status", "--json")
	if err != nil {
		if isNotRunning(err) {
			return nil, ErrNotRunning
		}
		return nil, fmt.Errorf("cannot read tailscale status: %w", err)
	}

	var st status
	if err := json.Unmarshal(out, &st); err != nil {
		return nil, fmt.Errorf("cannot parse tailscale status: %w", err)
	}
	if st.BackendState != StateRunning {
		return nil, fmt.Errorf("%w (state: %s)", ErrNotRunning, stateLabel(st.BackendState))
	}
	if st.Self == nil {
		return nil, fmt.Errorf("%w: local node missing from status", ErrNotRunning)
	}

	c.mu.Lock()
	c.cache, c.at = &st, time.Now()
	c.mu.Unlock()
	return &st, nil
}

// isNotRunning recognises the CLI's error text for a down backend.
func isNotRunning(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, needle := range []string{
		"is tailscale running",
		"failed to connect to local tailscaled",
		"the tailscaled daemon is not running",
		"stopped",
		"needslogin",
		"no state",
		"no such file or directory",
		"executable file not found",
	} {
		if strings.Contains(msg, needle) {
			return true
		}
	}
	return false
}

// stateLabel renders a backend state for humans.
func stateLabel(s string) string {
	if s == "" {
		return "unknown"
	}
	return s
}

// Local returns this node's tailnet identity.
func (c *Client) Local(ctx context.Context) (Local, error) {
	st, err := c.fetch(ctx)
	if err != nil {
		return Local{}, err
	}
	ips := st.Self.TailscaleIPs
	if len(ips) == 0 {
		ips = st.TailscaleIPs
	}
	return Local{
		State:  st.BackendState,
		IPs:    ips,
		Name:   st.Self.HostName,
		Online: st.BackendState == StateRunning,
	}, nil
}

// Peers lists every other tailnet node, sorted by name.
func (c *Client) Peers(ctx context.Context) ([]Peer, error) {
	st, err := c.fetch(ctx)
	if err != nil {
		return nil, err
	}
	var out []Peer
	for _, p := range st.Peer {
		peer := c.toPeer(p)
		out = append(out, peer)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// toPeer converts a status node into a Peer.
func (c *Client) toPeer(n *node) Peer {
	ip := ""
	if len(n.TailscaleIPs) > 0 {
		// Prefer IPv4: it avoids bracket handling in log output and is what
		// operators read off `tailscale status`.
		ip = n.TailscaleIPs[0]
		for _, candidate := range n.TailscaleIPs {
			if !strings.Contains(candidate, ":") {
				ip = candidate
				break
			}
		}
	}
	return Peer{
		Name:     n.HostName,
		DNSName:  n.DNSName,
		HostName: n.HostName,
		IP:       ip,
		Addr:     joinAddr(ip, c.Port()),
		Online:   n.Online,
		LastSeen: parseTime(n.LastSeen),
		Self:     false,
	}
}

// joinAddr builds host:port, bracketing IPv6.
func joinAddr(host string, port int) string {
	if host == "" {
		return ""
	}
	p := strconv.Itoa(port)
	if strings.Contains(host, ":") {
		return "[" + host + "]:" + p
	}
	return host + ":" + p
}

// parseTime decodes tailscale's RFC3339 timestamps, tolerating the zero time.
func parseTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}
	}
	if t.IsZero() || t.Year() <= 1 {
		return time.Time{}
	}
	return t
}

// Resolve finds a peer by name, hostname, DNS name, IP or address.
//
// Matching is case-insensitive and accepts: short hostname, MagicDNS name
// with or without the tailnet suffix, a bare Tailscale IP, or host:port.
func (c *Client) Resolve(ctx context.Context, nameOrAddr string) (Peer, error) {
	query := strings.TrimSpace(nameOrAddr)
	if query == "" {
		return Peer{}, fmt.Errorf("%w: empty name", ErrNotFound)
	}
	// Accept "alice@100.1.2.3" and "alice:6767" by taking the name part.
	if at := strings.Index(query, "@"); at > 0 {
		query = query[:at]
	}
	if host, _, err := splitHostPort(query); err == nil && host != "" {
		query = host
	}
	query = strings.TrimSuffix(query, ".")

	peers, err := c.Peers(ctx)
	if err != nil {
		return Peer{}, err
	}
	for _, p := range peers {
		if strings.EqualFold(p.Name, query) ||
			strings.EqualFold(p.HostName, query) ||
			strings.EqualFold(p.DNSName, query) ||
			strings.EqualFold(strings.SplitN(p.DNSName, ".", 2)[0], query) ||
			p.IP == query {
			return p, nil
		}
	}
	return Peer{}, fmt.Errorf("%w: %s", ErrNotFound, nameOrAddr)
}

// IsSelf reports whether nameOrAddr refers to the local node.
func (c *Client) IsSelf(ctx context.Context, nameOrAddr string) (bool, error) {
	local, err := c.Local(ctx)
	if err != nil {
		return false, err
	}
	query := strings.TrimSuffix(strings.TrimSpace(nameOrAddr), ".")
	if strings.EqualFold(query, local.Name) {
		return true, nil
	}
	for _, ip := range local.IPs {
		if query == ip {
			return true, nil
		}
	}
	return false, nil
}

// splitHostPort is a forgiving host:port split used during resolution.
func splitHostPort(s string) (host, port string, err error) {
	if s == "" {
		return "", "", errors.New("empty")
	}
	idx := strings.LastIndex(s, ":")
	if idx < 0 {
		return s, "", nil
	}
	host = strings.Trim(s[:idx], "[]")
	port = s[idx+1:]
	if _, err := strconv.Atoi(port); err != nil {
		return s, "", nil
	}
	return host, port, nil
}

// Ping measures round-trip latency to a tailnet address.
func (c *Client) Ping(ctx context.Context, addr string) (time.Duration, error) {
	if _, err := c.runner.LookPath("tailscale"); err != nil {
		return 0, ErrNotInstalled
	}
	ctx, cancel := context.WithTimeout(ctx, PingTimeout)
	defer cancel()

	out, err := c.runner.Run(ctx, "tailscale", "ping", "-c", "1", "--timeout", "2s", addr)
	if err != nil {
		return 0, fmt.Errorf("cannot ping %s: %w", addr, err)
	}
	return parsePing(string(out)), nil
}

// parsePing extracts the RTT from `tailscale ping` output.
//
// Example line: "pong from desktop (100.64.0.5) via 10.0.0.1 in 12ms"
func parsePing(out string) time.Duration {
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(line)
		for i, f := range fields {
			if !strings.HasSuffix(f, "ms") && !strings.HasSuffix(f, "s") {
				continue
			}
			// Prefer a field directly preceded by "in".
			if i > 0 && fields[i-1] == "in" {
				if d, ok := parseDuration(f); ok {
					return d
				}
			}
		}
		// Fall back to the last duration-looking token.
		for i := len(fields) - 1; i >= 0; i-- {
			if d, ok := parseDuration(fields[i]); ok {
				return d
			}
		}
	}
	return 0
}

// parseDuration converts "12ms" or "1.2s" to a Duration.
func parseDuration(s string) (time.Duration, bool) {
	if !strings.HasSuffix(s, "ms") && !strings.HasSuffix(s, "s") {
		return 0, false
	}
	f, err := strconv.ParseFloat(strings.TrimRight(s, "ms"), 64)
	if err != nil {
		return 0, false
	}
	if strings.HasSuffix(s, "ms") {
		return time.Duration(f * float64(time.Millisecond)), true
	}
	return time.Duration(f * float64(time.Second)), true
}

// Online reports whether a resolved peer is currently reachable.
func (p Peer) Reachable() bool { return p.Online && p.Addr != "" }

// LastSeenString renders a last-seen time relative to now, or "" when unknown.
func LastSeenString(t time.Time, now time.Time) string {
	if t.IsZero() {
		return ""
	}
	d := now.Sub(t)
	switch {
	case d < time.Minute:
		return "just now"
	case d < 2*time.Minute:
		return "1m ago"
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd ago", int(d.Hours()/24))
	}
}
