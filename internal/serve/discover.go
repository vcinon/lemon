package serve

import (
	"context"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/siin/lemon/internal/protocol"
)

// Discovery tuning.
//
// Probing is a bounded fan-out over the tailnet, so the cost is dominated by
// how long a silent node takes to fail. These values keep a discovery pass
// short enough to sit inside an interactive `lemon setup` while still tolerating
// a node that is slow to answer.
const (
	// DiscoverDialTimeout is the per-node connection budget.
	DiscoverDialTimeout = 1500 * time.Millisecond
	// discoverParallel is how many nodes are probed at once. The tailnet is
	// small, but a cap keeps a large one from opening hundreds of sockets.
	discoverParallel = 8
	// discoverOverallTimeout bounds a whole discovery pass.
	discoverOverallTimeout = 10 * time.Second
)

// DefaultPort is the port Lemon uses when nothing else is configured. It is
// also probed alongside the local port during discovery, because a peer's port
// is not knowable in advance: two nodes both moved off the default would
// otherwise never find each other, and neither is misconfigured.
const DefaultPort = 6767

// discoverPorts returns the ports to probe, local first.
//
// The local port leads because a node on a tailnet is overwhelmingly likely to
// run the port its owner chose, and because the answer we report should match
// the port we can actually reach.
func discoverPorts(local, fallback int) []int {
	if local <= 0 || local == fallback {
		return []int{fallback}
	}
	return []int{local, fallback}
}

// defaultPortValue is the port probed in addition to the local one.
//
// It is a field so a test can exercise the two-port path without binding the
// real default port, which anything on the machine may already hold.
func (d *Daemon) defaultPortValue() int {
	if d.defaultPort > 0 {
		return d.defaultPort
	}
	return DefaultPort
}

// opDiscover finds the Lemons running on this tailnet.
//
// Discovery is a probe, not a broadcast: for each node `tailscale status` knows
// about, Lemon opens a short TCP connection to the Lemon port and performs the
// normal protocol handshake. A successful handshake is itself the proof that a
// Lemon is listening, and the hello frame carries the peer's own username. No
// new wire format, no registry, and nothing to keep in sync with a directory
// service.
func (d *Daemon) opDiscover(ctx context.Context, req Request) *Response {
	if d.cfg.Username == "" {
		return fail(CodeNotConfigured, "lemon is not configured on this node")
	}

	peers, err := d.ts.Peers(ctx)
	if err != nil {
		// Tailscale being down is reported, not fatal: setup can still fall
		// back to asking for a peer by name.
		return &Response{
			OK:    false,
			Code:  CodeOffline,
			Error: err.Error(),
			State: "offline",
		}
	}

	// Probing our own tailnet address would only find ourselves.
	selfIPs := map[string]bool{}
	if local, lerr := d.ts.Local(ctx); lerr == nil {
		for _, ip := range local.IPs {
			selfIPs[ip] = true
		}
	}
	// The port is not carried by the tailnet, so each node is probed on the
	// ports Lemon might plausibly be listening on.
	ports := discoverPorts(d.cfg.Port, d.defaultPortValue())

	type candidate struct {
		ip   string
		host string
	}
	var targets []candidate
	for _, p := range peers {
		if p.IP == "" || selfIPs[p.IP] {
			continue
		}
		targets = append(targets, candidate{ip: p.IP, host: p.Name})
	}
	// Stable order keeps the prompt in setup predictable between runs.
	sort.Slice(targets, func(i, j int) bool { return targets[i].ip < targets[j].ip })

	results := make([]DiscoveredPeer, len(targets))
	var wg sync.WaitGroup
	sem := make(chan struct{}, discoverParallel)

	for i, target := range targets {
		// Ports are raced, so a node that is silent on the first one does not
		// pay for it twice.
		var once sync.Once
		record := func(id protocol.Identity, addr string) {
			once.Do(func() {
				if id.User == "" || strings.EqualFold(id.User, d.cfg.Username) {
					// An empty or matching name is our own listener reached
					// through a path we did not recognise as local.
					return
				}
				_, configured := d.cfg.Peer(id.User)
				results[i] = DiscoveredPeer{
					User:       id.User,
					Addr:       addr,
					Host:       target.host,
					LatencyMS:  int(id.Latency.Milliseconds()),
					Version:    id.Version,
					Configured: configured,
				}
			})
		}

		for _, port := range ports {
			wg.Add(1)
			go func(ip string, port int) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()

				probeCtx, cancel := context.WithTimeout(ctx, DiscoverDialTimeout)
				defer cancel()

				addr := net.JoinHostPort(ip, strconv.Itoa(port))
				id, err := protocol.Identify(probeCtx, addr, d.cfg.Username)
				if err != nil {
					// A node that is offline, or is not running Lemon, is
					// simply not a result. It is not an error worth
					// reporting.
					d.log.Debugf("discover: %s: %v", addr, err)
					return
				}
				record(id, addr)
			}(target.ip, port)
		}
	}

	// A single overall bound, so one wedged node cannot hang the command.
	waitDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitDone)
	}()
	overall, cancel := context.WithTimeout(ctx, discoverOverallTimeout)
	defer cancel()
	select {
	case <-waitDone:
	case <-overall.Done():
		d.log.Debugf("discover: gave up after %s with results so far", discoverOverallTimeout)
	}

	found := make([]DiscoveredPeer, 0, len(results))
	for _, r := range results {
		if r.User != "" {
			found = append(found, r)
		}
	}
	// Present them the way a person would choose one: fastest first, so the
	// suggested default is the most responsive peer.
	sort.SliceStable(found, func(i, j int) bool {
		if found[i].LatencyMS != found[j].LatencyMS {
			return found[i].LatencyMS < found[j].LatencyMS
		}
		return found[i].User < found[j].User
	})

	d.log.Infof("discover: %d tailnet node(s) probed, %d lemon(s) found", len(targets), len(found))
	return &Response{OK: true, Discovered: found, Scanned: len(targets)}
}
