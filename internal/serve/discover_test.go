package serve

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/siin/lemon/internal/config"
	"github.com/siin/lemon/internal/logging"
	"github.com/siin/lemon/internal/protocol"
	"github.com/siin/lemon/internal/tailscale"
)

// fakeTailnet stands in for a tailnet so discovery can be tested where a real
// one is not available.
//
// The node IPs are loopback aliases, which behave like distinct hosts on
// Linux, so a real listener on 127.0.0.2 is a genuine peer to 127.0.0.1 — the
// probe does real TCP and a real handshake, not a stubbed one.
type fakeTailnet struct {
	self  string
	peers []tailscale.Peer
	err   error
}

func (f *fakeTailnet) Peers(context.Context) ([]tailscale.Peer, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.peers, nil
}

func (f *fakeTailnet) Local(context.Context) (tailscale.Local, error) {
	return tailscale.Local{IPs: []string{f.self}, State: "Running", Online: true}, nil
}

func (f *fakeTailnet) Resolve(context.Context, string) (tailscale.Peer, error) {
	return tailscale.Peer{}, errors.New("not implemented")
}
func (f *fakeTailnet) SetPort(int) {}
func (f *fakeTailnet) Invalidate() {}

// silentAddr is an address nothing listens on, so a probe to it fails fast.
func silentAddr() string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// startPeerLemon runs a real Lemon listener on ip:port and returns its
// shutdown. Discovery talks to it over real TCP and a real handshake, so the
// test exercises the same path as a node on the tailnet.
func startPeerLemon(t *testing.T, ip string, port int, user string) {
	t.Helper()
	ln, err := net.Listen("tcp", net.JoinHostPort(ip, fmt.Sprint(port)))
	if err != nil {
		t.Skipf("cannot listen on %s:%d: %v", ip, port, err)
	}
	srv := protocol.NewServer(ln, protocol.ServerOptions{
		LocalUser: user,
		Handler:   protocol.SessionHandlerFunc(answerPing),
	})
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = srv.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		_ = ln.Close()
	})
}

// answerPing is a minimal but real Lemon session: it replies to pings, which is
// what a real node does and what proves the connection is live.
func answerPing(ctx context.Context, s *protocol.Session) {
	for {
		raw, err := s.ReadFrameTimeout(30 * time.Second)
		if err != nil {
			return
		}
		var p protocol.Ping
		if err := protocol.Decode(raw, &p, protocol.TypePing); err != nil {
			return
		}
		if err := s.WriteFrame(&protocol.Pong{Type: protocol.TypePong, TS: p.TS}); err != nil {
			return
		}
	}
}

// testDaemon builds a daemon with just enough state for discovery.
func testDaemon(t *testing.T, user string, port int, ts Tailscale, peers map[string]*config.Peer) *Daemon {
	t.Helper()
	cfg := &config.Config{Username: user, Port: port, Peers: peers}
	return &Daemon{cfg: cfg, log: logging.Discard(), ts: ts}
}

// unusedPort stands in for the fallback port. Tests point the daemon's
// defaultPort here so the real 6767 is never probed: a lemon running on the
// test machine listens on 0.0.0.0:6767 and would answer for every loopback
// address, which is a real lemon rather than the peer a test means to find.
const unusedPort = 16799

// quietNode is an address in the RFC 5737 documentation range. Nothing can be
// listening there, so it is a reliably silent neighbour no matter what the
// machine running the test happens to have bound — a loopback alias would not
// do, because a listener on 0.0.0.0 answers on every one of them.
const quietNode = "192.0.2.1"

// TestDiscoverFindsRunningLemon checks the whole discovery path against a real
// listener: the probe completes a handshake and reports the peer's own name.
func TestDiscoverFindsRunningLemon(t *testing.T) {
	const (
		peerIP   = "127.0.0.2"
		selfIP   = "127.0.0.1"
		peerPort = 16777
	)

	// A real Lemon listener on a distinct loopback address.
	startPeerLemon(t, peerIP, peerPort, "bob")

	ts := &fakeTailnet{
		self: selfIP,
		peers: []tailscale.Peer{
			{IP: peerIP, Name: "bob-laptop", Online: true},
			{IP: quietNode, Name: "quiet-node", Online: true},
		},
	}
	// We listen on the same port, which is the normal case: discovery probes
	// the local port and the fallback one.
	d := testDaemon(t, "alice", peerPort, ts, map[string]*config.Peer{})
	d.defaultPort = unusedPort

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	resp := d.opDiscover(ctx, Request{Op: OpDiscover})

	if !resp.OK {
		t.Fatalf("discover failed: %s", resp.Error)
	}
	if resp.Scanned != 2 {
		t.Errorf("scanned = %d, want 2 (one node answered, one is silent)", resp.Scanned)
	}
	if len(resp.Discovered) != 1 {
		t.Fatalf("discovered %d peers, want 1: %+v", len(resp.Discovered), resp.Discovered)
	}

	got := resp.Discovered[0]
	if got.User != "bob" {
		t.Errorf("user = %q, want %q — the name must come from the peer's handshake", got.User, "bob")
	}
	wantAddr := net.JoinHostPort(peerIP, fmt.Sprint(peerPort))
	if got.Addr != wantAddr {
		t.Errorf("addr = %q, want %q", got.Addr, wantAddr)
	}
	if got.Host != "bob-laptop" {
		t.Errorf("host = %q, want %q", got.Host, "bob-laptop")
	}
	if got.Configured {
		t.Error("configured = true, want false for a peer not in the config")
	}
	if got.Version != protocol.Version {
		t.Errorf("version = %d, want %d", got.Version, protocol.Version)
	}
}

// TestDiscoverSkipsSelf proves the node does not report its own listener as a
// peer to message.
func TestDiscoverSkipsSelf(t *testing.T) {
	const selfIP = "127.0.0.1"
	startPeerLemon(t, selfIP, 16778, "alice")

	// The tailnet reports our own IP as a peer, as `tailscale status` can when
	// the backend has just come up.
	ts := &fakeTailnet{self: selfIP, peers: []tailscale.Peer{{IP: selfIP, Name: "this-node"}}}
	d := testDaemon(t, "alice", 16778, ts, map[string]*config.Peer{})
	d.defaultPort = unusedPort

	resp := d.opDiscover(context.Background(), Request{Op: OpDiscover})
	if !resp.OK {
		t.Fatalf("discover failed: %s", resp.Error)
	}
	if len(resp.Discovered) != 0 {
		t.Errorf("discovered %+v, want nothing — our own node is not a peer", resp.Discovered)
	}
	if resp.Scanned != 0 {
		t.Errorf("scanned = %d, want 0 — our own IP is skipped before probing", resp.Scanned)
	}
}

// TestDiscoverProbesDefaultPortWhenLocalPortDiffers covers the case that broke
// first: a node on a custom port must still be found by a node whose own port
// differs, because the tailnet carries no port information.
func TestDiscoverProbesDefaultPortWhenLocalPortDiffers(t *testing.T) {
	const (
		peerIP   = "127.0.0.3"
		peerPort = 16990
		selfPort = 16999
	)

	// The peer listens on the default port; we are on a custom one.
	startPeerLemon(t, peerIP, peerPort, "carol")

	ts := &fakeTailnet{self: "127.0.0.1", peers: []tailscale.Peer{{IP: peerIP, Name: "carol-pc", Online: true}}}
	d := testDaemon(t, "alice", selfPort, ts, map[string]*config.Peer{})
	// Our own port differs from the port the peer answers on, which is the
	// situation the two-port probe exists for.
	d.defaultPort = peerPort

	resp := d.opDiscover(context.Background(), Request{Op: OpDiscover})
	if !resp.OK {
		t.Fatalf("discover failed: %s", resp.Error)
	}
	if len(resp.Discovered) != 1 {
		t.Fatalf("discovered %d, want 1: a peer on the default port must be found from a custom port", len(resp.Discovered))
	}
	if got := resp.Discovered[0]; got.User != "carol" || got.Addr != net.JoinHostPort(peerIP, fmt.Sprint(peerPort)) {
		t.Errorf("got %+v, want carol at %s", got, net.JoinHostPort(peerIP, fmt.Sprint(peerPort)))
	}
}

// TestDiscoverReportsConfiguredPeers checks the flag setup uses to tell an old
// peer from a new one.
func TestDiscoverReportsConfiguredPeers(t *testing.T) {
	const (
		peerIP   = "127.0.0.4"
		peerPort = 16780
	)
	startPeerLemon(t, peerIP, peerPort, "dave")

	ts := &fakeTailnet{self: "127.0.0.1", peers: []tailscale.Peer{{IP: peerIP, Name: "dave", Online: true}}}
	d := testDaemon(t, "alice", peerPort, ts, map[string]*config.Peer{
		"dave": {},
	})
	d.defaultPort = unusedPort

	resp := d.opDiscover(context.Background(), Request{Op: OpDiscover})
	if len(resp.Discovered) != 1 {
		t.Fatalf("discovered %d, want 1", len(resp.Discovered))
	}
	if !resp.Discovered[0].Configured {
		t.Error("configured = false, want true for a peer already in the config")
	}
}

// TestDiscoverFailsWithoutUsername keeps an unconfigured node from reporting
// strangers as peers.
func TestDiscoverFailsWithoutUsername(t *testing.T) {
	d := testDaemon(t, "", DefaultPort, &fakeTailnet{self: "127.0.0.1"}, nil)
	resp := d.opDiscover(context.Background(), Request{Op: OpDiscover})
	if resp.OK {
		t.Error("discover succeeded without a username, want a not-configured failure")
	}
	if resp.Code != CodeNotConfigured {
		t.Errorf("code = %q, want %q", resp.Code, CodeNotConfigured)
	}
}

// TestDiscoverPorts documents which ports are probed and in what order.
func TestDiscoverPorts(t *testing.T) {
	tests := []struct {
		local    int
		fallback int
		want     []int
	}{
		{local: 6767, fallback: 6767, want: []int{6767}},
		{local: 7000, fallback: 6767, want: []int{7000, 6767}},
		{local: 0, fallback: 6767, want: []int{6767}},
	}
	for _, tt := range tests {
		got := discoverPorts(tt.local, tt.fallback)
		if len(got) != len(tt.want) {
			t.Errorf("discoverPorts(%d, %d) = %v, want %v", tt.local, tt.fallback, got, tt.want)
			continue
		}
		for i := range got {
			if got[i] != tt.want[i] {
				t.Errorf("discoverPorts(%d, %d) = %v, want %v", tt.local, tt.fallback, got, tt.want)
				break
			}
		}
	}
}
