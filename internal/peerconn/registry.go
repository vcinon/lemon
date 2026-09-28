// Package peerconn resolves peers to Tailscale addresses and tracks their
// reachability.
//
// A connection is deliberately short-lived: one TCP session per logical
// exchange. What this package adds is a cache of the last observed state
// (address, online flag, latency, last-seen) so `lemon status` and the retry
// worker do not each shell out to `tailscale status` independently.
package peerconn

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/siin/lemon/internal/config"
	"github.com/siin/lemon/internal/logging"
	"github.com/siin/lemon/internal/tailscale"
)

// Errors surfaced to the user.
var (
	// ErrUnknownPeer means the name is not in the configured peer list.
	ErrUnknownPeer = errors.New("unknown peer")
	// ErrNoDefault means no default peer is configured.
	ErrNoDefault = errors.New("no default peer")
	// ErrDisabled means the peer is configured but switched off.
	ErrDisabled = errors.New("peer is disabled")
)

// stateTTL is how long a cached observation stays fresh.
const stateTTL = 5 * time.Second

// State is the last known condition of a peer.
type State struct {
	// Name is the configured peer name.
	Name string
	// Addr is the address Lemon dials.
	Addr string
	// Online reports tailnet reachability.
	Online bool
	// Latency is the last measured round-trip time.
	Latency time.Duration
	// LastSeen is when the peer was last observed online.
	LastSeen time.Time
	// Resolved marks Addr as coming from tailscale rather than config.
	Resolved bool
	// Reason explains why a peer is unreachable, when known.
	Reason string
}

// Registry tracks peer states for one Lemon installation.
type Registry struct {
	mu     sync.RWMutex
	states map[string]*State
	last   time.Time
	// tailscaleOffline latches a "not running" result so every peer lookup in
	// the same window reports the same clear error.
	tsOffline bool
	tsErr     error
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{states: map[string]*State{}} }

// Refresh re-reads tailnet state and updates every configured peer.
func (r *Registry) Refresh(ctx context.Context, ts tailscale.Source, peers map[string]*config.Peer, port int) error {
	ts.SetPort(port)
	ts.Invalidate()

	local, err := ts.Local(ctx)
	if err != nil {
		r.mu.Lock()
		r.tsOffline = true
		r.tsErr = err
		for name, st := range r.states {
			st.Online = false
			st.Reason = err.Error()
			r.states[name] = st
		}
		r.mu.Unlock()
		return err
	}
	list, err := ts.Peers(ctx)
	if err != nil {
		r.mu.Lock()
		r.tsOffline = true
		r.tsErr = err
		r.mu.Unlock()
		return err
	}
	byName := make(map[string]tailscale.Peer, len(list))
	for _, p := range list {
		byName[strings.ToLower(p.Name)] = p
	}

	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tsOffline = false
	r.tsErr = nil
	r.last = now

	for name, peer := range peers {
		st, ok := r.states[name]
		if !ok {
			st = &State{Name: name}
			r.states[name] = st
		}
		if peer != nil && peer.Disabled {
			st.Online = false
			st.Reason = ErrDisabled.Error()
			st.Addr = ""
			continue
		}
		// An explicitly configured address wins over discovery. We cannot
		// learn its reachability from tailscale status, so Online keeps its
		// previous value and the caller probes it instead.
		if peer != nil && peer.Addr != "" {
			st.Addr = peer.Addr
			st.Resolved = false
			if st.Reason == "" || st.Reason == "offline" {
				st.Reason = "unknown (explicit address)"
			}
		} else if tp, found := byName[strings.ToLower(name)]; found {
			st.Addr = tp.Addr
			st.Resolved = true
			if tp.Online {
				st.Online = true
				st.LastSeen = now
				st.Reason = ""
			} else {
				st.Online = false
				st.LastSeen = tp.LastSeen
				st.Reason = "offline"
			}
		} else {
			// Not on the tailnet under that name; keep a config address if we
			// have one, otherwise report why resolution failed.
			st.Online = false
			if st.Addr == "" {
				st.Reason = "not on your tailnet (check the name, or set an address: lemon peer add " + name + " <ip>)"
			} else {
				st.Reason = "not on your tailnet"
			}
		}
		r.states[name] = st
	}
	_ = local
	return nil
}

// State returns the cached state for a peer.
func (r *Registry) State(name string) (State, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	st, ok := r.states[name]
	if !ok {
		return State{}, false
	}
	return *st, true
}

// Set records a successful exchange, refreshing last-seen.
func (r *Registry) Set(name, addr string, latency time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	st, ok := r.states[name]
	if !ok {
		st = &State{Name: name}
	}
	st.Online = true
	st.Reason = ""
	if addr != "" {
		st.Addr = addr
	}
	if latency > 0 {
		st.Latency = latency
	}
	st.LastSeen = time.Now()
	r.states[name] = st
}

// SetOffline records a failed exchange.
func (r *Registry) SetOffline(name, reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	st, ok := r.states[name]
	if !ok {
		st = &State{Name: name}
	}
	st.Online = false
	if reason != "" {
		st.Reason = reason
	}
	r.states[name] = st
}

// Forget drops a peer's state, used by `lemon disconnect` and `peer rm`.
func (r *Registry) Forget(name string) {
	r.mu.Lock()
	delete(r.states, name)
	r.mu.Unlock()
}

// Names lists tracked peers in insertion-agnostic order.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.states))
	for n := range r.states {
		out = append(out, n)
	}
	return out
}

// TailscaleError returns the latched tailscale failure, if any.
func (r *Registry) TailscaleError() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.tsOffline {
		return r.tsErr
	}
	return nil
}

// Fresh reports whether cached states are recent enough to trust.
func (r *Registry) Fresh() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return !r.last.IsZero() && time.Since(r.last) < stateTTL
}

// Invalidate marks cached states as stale.
func (r *Registry) Invalidate() {
	r.mu.Lock()
	r.last = time.Time{}
	r.mu.Unlock()
}

// Resolver turns a peer name into a dialable address.
type Resolver struct {
	reg *Registry
	ts  tailscale.Source
	log *logging.Logger
}

// NewResolver returns a Resolver backed by reg and ts.
func NewResolver(reg *Registry, ts tailscale.Source, log *logging.Logger) *Resolver {
	if log == nil {
		log = logging.Discard()
	}
	return &Resolver{reg: reg, ts: ts, log: log}
}

// Resolve returns the address to dial for a peer name.
//
// Explicit configuration wins; otherwise the tailnet is consulted. An address
// literal is accepted too, so `lemon connect 100.101.102.103` works even for a
// peer that is not in the config file.
func (r *Resolver) Resolve(ctx context.Context, name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", ErrUnknownPeer
	}

	// A literal host:port needs no discovery.
	if host, _, err := config.SplitHostPort(name); err == nil && looksLikeHost(host) {
		return config.NormalizeAddr(name)
	}

	if st, ok := r.reg.State(name); ok && st.Addr != "" {
		if st.Resolved && !st.Online && r.reg.Fresh() {
			// The peer is known but offline; the address is still correct and
			// the dial attempt will tell us the truth.
			r.log.Debugf("peer %s resolved to %s (cached, offline)", name, st.Addr)
		}
		return st.Addr, nil
	}

	peer, err := r.ts.Resolve(ctx, name)
	if err != nil {
		return "", err
	}
	if peer.Addr == "" {
		return "", fmt.Errorf("peer %s has no Tailscale address", name)
	}
	return peer.Addr, nil
}

// looksLikeHost reports whether s is a bare hostname or IP rather than a name.
func looksLikeHost(s string) bool {
	if s == "" || strings.ContainsAny(s, " \t") {
		return false
	}
	if strings.Count(s, ".") >= 1 || strings.Contains(s, ":") {
		return true
	}
	return false
}
