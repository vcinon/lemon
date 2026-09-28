package serve

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/siin/lemon/internal/config"
	"github.com/siin/lemon/internal/logging"
	"github.com/siin/lemon/internal/queue"
	"github.com/siin/lemon/internal/transfer"
)

// MaxControlFrame caps a control request or response.
const MaxControlFrame = 1 << 20

// ControlTimeout bounds a single control exchange.
//
// A transfer is a single exchange too, but a multi-gigabyte file over a slow
// link legitimately outlives any small budget, so it gets its own generous
// deadline instead of being cut off mid-stream.
const (
	ControlTimeout     = 2 * time.Minute
	TransferTimeout    = 24 * time.Hour
	controlReadTimeout = 30 * time.Second
)

// opTimeout returns the exchange budget for one operation.
func opTimeout(op string) time.Duration {
	if op == OpTransfer {
		return TransferTimeout
	}
	return ControlTimeout
}

// discardOrphanParts removes stale .part files.
func discardOrphanParts(dir string) int {
	return transfer.DiscardOrphanParts(dir, transfer.DefaultPartTTL)
}

// Request is one command from the CLI to the daemon.
//
// Fields are pointers or explicitly-tagged so "not supplied" is
// distinguishable from "set to the zero value".
type Request struct {
	// Op names the operation, e.g. "send" or "status".
	Op string `json:"op"`
	// User is the local username, used to detect a config change under us.
	User string `json:"user,omitempty"`
	// Peer is the target peer name.
	Peer string `json:"peer,omitempty"`
	// Addr is an explicit target address, overriding peer resolution.
	Addr string `json:"addr,omitempty"`
	// Message is the text for a send.
	Message string `json:"message,omitempty"`
	// MessageID lets a caller track a specific queued message.
	MessageID string `json:"message_id,omitempty"`
	// Files are absolute source paths for a transfer.
	Files []string `json:"files,omitempty"`
	// Resume allows the sender to seek to the receiver's offset.
	Resume bool `json:"resume,omitempty"`
	// History filters for a history listing.
	HistoryPeer  string `json:"history_peer,omitempty"`
	HistoryLimit int    `json:"history_limit,omitempty"`
	// Notifications toggles desktop notifications, for tests.
	Notifications *bool `json:"notifications,omitempty"`
	// Quiet suppresses progress events.
	Quiet bool `json:"quiet,omitempty"`
	// Probe asks for an active latency measurement in peer listings.
	Probe bool `json:"probe,omitempty"`
	// KeepEnabled makes `disconnect` forget a peer without disabling it.
	KeepEnabled bool `json:"keep_enabled,omitempty"`
	// WantProgress enables streamed transfer progress events.
	WantProgress bool `json:"want_progress,omitempty"`
}

// ProgressSink reports whether the caller asked for progress events.
func (r Request) ProgressSink() bool { return r.WantProgress && !r.Quiet }

// Response is the daemon's reply.
type Response struct {
	// OK reports success.
	OK bool `json:"ok"`
	// Error is a human-readable failure reason.
	Error string `json:"error,omitempty"`
	// Code is a stable slug the CLI maps onto an exit code.
	Code string `json:"code,omitempty"`
	// Offline marks a peer-unreachable outcome that led to queueing.
	Offline bool `json:"offline,omitempty"`
	// Queued reports that the work is durable and awaiting delivery.
	Queued bool `json:"queued,omitempty"`
	// State is a short state-machine label: connecting, connected, sending,
	// sent, delivered, failed, offline.
	State string `json:"state,omitempty"`
	// MessageID identifies a message or transfer.
	MessageID string `json:"message_id,omitempty"`
	// Peer echoes the resolved peer name.
	Peer string `json:"peer,omitempty"`
	// Addr is the address actually used.
	Addr string `json:"addr,omitempty"`
	// LatencyMS is a measured round-trip time.
	LatencyMS int `json:"latency_ms,omitempty"`

	// Events are incremental progress lines for the CLI to print.
	Events []Event `json:"events,omitempty"`

	// Status is populated for the status op.
	Status *StatusReport `json:"status,omitempty"`
	// History is populated for the history op.
	History []HistoryEntry `json:"history,omitempty"`
	// Peers is populated for the peers op.
	Peers []PeerReport `json:"peers,omitempty"`
	// Discovered is populated for the discover op.
	Discovered []DiscoveredPeer `json:"discovered,omitempty"`
	// Scanned counts tailnet nodes probed by the discover op.
	Scanned int `json:"scanned,omitempty"`
	// Transfers is populated for transfer ops.
	Transfers []TransferReport `json:"transfers,omitempty"`
	// Tailscale describes the local tailnet state.
	Tailscale *TailscaleReport `json:"tailscale,omitempty"`
	// Notification describes how notifications are dispatched.
	Notification *NotificationReport `json:"notification,omitempty"`
	// Health is populated for the ping op.
	Health *Health `json:"health,omitempty"`
}

// Event is a progress line streamed back to the CLI.
type Event struct {
	// Kind is "state", "progress" or "info".
	Kind string `json:"kind"`
	// Text is the rendered line.
	Text string `json:"text"`
	// Name and Done/N carry transfer progress.
	Name string `json:"name,omitempty"`
	N    int64  `json:"n,omitempty"`
	Done int64  `json:"done,omitempty"`
}

// HistoryEntry is one message in a history listing.
type HistoryEntry struct {
	ID        string `json:"id"`
	Peer      string `json:"peer"`
	Sender    string `json:"sender"`
	Content   string `json:"content"`
	Direction string `json:"direction"`
	Time      int64  `json:"time"`
	Status    string `json:"status"`
}

// PeerReport describes one configured peer.
type PeerReport struct {
	Name      string `json:"name"`
	Addr      string `json:"addr"`
	Online    bool   `json:"online"`
	LatencyMS int    `json:"latency_ms,omitempty"`
	LastSeen  int64  `json:"last_seen,omitempty"`
	LastSeenS string `json:"last_seen_str,omitempty"`
	Resolved  bool   `json:"resolved"`
	Reason    string `json:"reason,omitempty"`
	IsDefault bool   `json:"is_default"`
	Disabled  bool   `json:"disabled"`
}

// TransferReport is one file's transfer outcome.
type TransferReport struct {
	Name    string `json:"name"`
	Path    string `json:"path,omitempty"`
	Size    int64  `json:"size"`
	Resumed bool   `json:"resumed"`
	Err     string `json:"err,omitempty"`
}

// DiscoveredPeer is a Lemon found on the tailnet by probing.
type DiscoveredPeer struct {
	// User is the username the node announced in its handshake.
	User string `json:"user"`
	// Addr is the address that answered.
	Addr string `json:"addr"`
	// Host is the tailnet hostname, which may differ from User.
	Host string `json:"host,omitempty"`
	// LatencyMS is the handshake round trip.
	LatencyMS int `json:"latency_ms,omitempty"`
	// Version is the protocol version the node speaks.
	Version int `json:"version,omitempty"`
	// Configured is true when this peer is already in the local config.
	Configured bool `json:"configured"`
	// Self marks our own node, which answered on the loopback address.
	Self bool `json:"self,omitempty"`
}

// TailscaleReport describes the local tailnet connection.
type TailscaleReport struct {
	OK      bool     `json:"ok"`
	State   string   `json:"state,omitempty"`
	IPs     []string `json:"ips,omitempty"`
	Name    string   `json:"name,omitempty"`
	Error   string   `json:"error,omitempty"`
	Version string   `json:"version,omitempty"`
}

// NotificationReport describes notification dispatch.
type NotificationReport struct {
	Command string `json:"command"`
	OK      bool   `json:"ok"`
	Error   string `json:"error,omitempty"`
}

// StatusReport is the payload of `lemon status`.
type StatusReport struct {
	User         string              `json:"user"`
	ListenerAddr string              `json:"listener_addr"`
	ListenerUp   bool                `json:"listener_up"`
	ListenerPID  int                 `json:"listener_pid,omitempty"`
	Port         int                 `json:"port"`
	StartedAt    int64               `json:"started_at,omitempty"`
	Version      int                 `json:"protocol_version"`
	Peers        []PeerReport        `json:"peers"`
	QueuedMsgs   int                 `json:"queued_messages"`
	QueuedXfers  int                 `json:"queued_transfers"`
	ActiveXfers  int                 `json:"active_transfers"`
	Tailscale    *TailscaleReport    `json:"tailscale"`
	Notification *NotificationReport `json:"notification"`
	AutostartTip bool                `json:"autostart_tip"`
}

// Control request op names.
const (
	OpPing       = "ping"
	OpSend       = "send"
	OpTransfer   = "transfer"
	OpStatus     = "status"
	OpHistory    = "history"
	OpPeers      = "peers"
	OpDiscover   = "discover"
	OpConnect    = "connect"
	OpDisconnect = "disconnect"
	OpQueue      = "queue"
	OpFlush      = "flush"
)

// controlServer serves the CLI-facing Unix socket.
type controlServer struct {
	path string
	d    *Daemon
	log  *logging.Logger
	ln   net.Listener
}

// newControlServer binds the control socket, replacing a stale file left by a
// crashed daemon.
func newControlServer(path string, d *Daemon, log *logging.Logger) (*controlServer, error) {
	// A socket file left behind by a dead daemon must not block startup. A
	// live daemon would hold the flock we already own, so reaching here means
	// the previous owner is gone.
	if err := os.RemoveAll(path); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("cannot clear stale control socket: %w", err)
	}
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("cannot create control socket: %w", err)
	}
	// Owner-only: the control socket can send messages on the user's behalf.
	if err := os.Chmod(path, 0o600); err != nil {
		ln.Close()
		return nil, fmt.Errorf("cannot secure control socket: %w", err)
	}
	return &controlServer{path: path, d: d, log: log, ln: ln}, nil
}

// serve accepts control connections until ctx is cancelled.
func (c *controlServer) serve(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		_ = c.ln.Close()
	}()
	for {
		conn, err := c.ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("control accept: %w", err)
		}
		go c.handle(ctx, conn)
	}
}

// handle serves a control connection.
//
// A connection may carry several requests: the CLI reuses one socket for its
// health probe and its real command, and looping here avoids a needless
// reconnect in between.
func (c *controlServer) handle(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	br := bufio.NewReaderSize(conn, 32<<10)

	for {
		// Bound only the wait for the request itself; the operation budget is
		// set once we know which op this is.
		_ = conn.SetDeadline(time.Now().Add(controlReadTimeout))
		line, err := readLine(br, MaxControlFrame)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				c.log.Debugf("control: read ended: %v", err)
			}
			return
		}
		var req Request
		if err := json.Unmarshal(line, &req); err != nil {
			if werr := writeResponse(conn, &Response{
				OK: false, Code: "bad_request", Error: "malformed request",
			}); werr != nil {
				return
			}
			return
		}
		c.log.Debugf("control: op=%s peer=%q msg=%q files=%d",
			req.Op, req.Peer, truncate(req.Message, 40), len(req.Files))

		_ = conn.SetDeadline(time.Now().Add(opTimeout(req.Op)))
		resp := c.d.dispatch(ctx, req, conn)
		if req.ProgressSink() {
			// Events already went out one by one; the reply is just the verdict.
			resp.Events = nil
		} else {
			resp.Events = c.d.drainEvents()
		}
		if err := writeResponse(conn, resp); err != nil {
			c.log.Debugf("control: cannot write response: %v", err)
			return
		}
	}
}

// readLine reads one newline-terminated line under a size cap.
func readLine(br *bufio.Reader, max int) ([]byte, error) {
	var buf []byte
	for {
		chunk, isPrefix, err := br.ReadLine()
		if err != nil {
			return nil, err
		}
		buf = append(buf, chunk...)
		if len(buf) > max {
			return nil, fmt.Errorf("control frame exceeds %d bytes", max)
		}
		if !isPrefix {
			return buf, nil
		}
	}
}

// writeResponse sends one JSON response.
func writeResponse(w net.Conn, resp *Response) error {
	data, err := json.Marshal(resp)
	if err != nil {
		return fmt.Errorf("cannot encode response: %w", err)
	}
	if len(data) > MaxControlFrame {
		return fmt.Errorf("response exceeds %d bytes", MaxControlFrame)
	}
	if _, err := w.Write(append(data, '\n')); err != nil {
		return err
	}
	return nil
}

// truncate shortens s for log output.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// dialUnix connects to a Unix socket with a timeout.
func dialUnix(ctx context.Context, path string, timeout time.Duration) (net.Conn, error) {
	d := net.Dialer{Timeout: timeout}
	return d.DialContext(ctx, "unix", path)
}

// exchangeHealth performs a health round trip over an open control connection.
func exchangeHealth(ctx context.Context, conn net.Conn) (Health, error) {
	_ = conn.SetDeadline(time.Now().Add(ControlTimeout))
	req, err := json.Marshal(Request{Op: OpPing})
	if err != nil {
		return Health{}, err
	}
	if _, err := conn.Write(append(req, '\n')); err != nil {
		return Health{}, fmt.Errorf("control socket is not answering: %w", err)
	}
	br := bufio.NewReaderSize(conn, 8<<10)
	line, err := readLine(br, MaxControlFrame)
	if err != nil {
		return Health{}, fmt.Errorf("control socket is not answering: %w", err)
	}
	var resp Response
	if err := json.Unmarshal(line, &resp); err != nil {
		return Health{}, fmt.Errorf("control socket returned garbage: %w", err)
	}
	if !resp.OK {
		return Health{}, fmt.Errorf("control socket unhealthy: %s", resp.Error)
	}
	if resp.Health == nil {
		return Health{}, fmt.Errorf("control socket returned no health payload")
	}
	return *resp.Health, nil
}

// ControlClient talks to the running daemon.
type ControlClient struct {
	conn net.Conn
	br   *bufio.Reader
}

// Dial connects to the control socket.
func Dial(ctx context.Context, socketPath string, timeout time.Duration) (*ControlClient, error) {
	conn, err := dialUnix(ctx, socketPath, timeout)
	if err != nil {
		return nil, err
	}
	return &ControlClient{conn: conn, br: bufio.NewReaderSize(conn, 32<<10)}, nil
}

// Close ends the control connection.
func (c *ControlClient) Close() error { return c.conn.Close() }

// Do sends a request and returns the response.
//
// When req asks for progress, the daemon streams event frames first; onEvent
// receives each one as it arrives and Do returns only once the final response
// lands. A request that does not ask for progress gets a single reply.
func (c *ControlClient) Do(ctx context.Context, req Request, onEvent func(Event)) (*Response, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("cannot encode request: %w", err)
	}
	if len(data) > MaxControlFrame {
		return nil, fmt.Errorf("request exceeds %d bytes", MaxControlFrame)
	}
	if _, err := c.conn.Write(append(data, '\n')); err != nil {
		return nil, fmt.Errorf("cannot talk to the lemon listener: %w", err)
	}
	for {
		line, err := readLine(c.br, MaxControlFrame)
		if err != nil {
			return nil, fmt.Errorf("cannot read reply from the lemon listener: %w", err)
		}
		// A frame carrying an event is progress; anything else is the reply.
		var frame eventFrame
		if err := json.Unmarshal(line, &frame); err == nil && frame.Event != nil {
			if onEvent != nil {
				onEvent(*frame.Event)
			}
			continue
		}
		var resp Response
		if err := json.Unmarshal(line, &resp); err != nil {
			return nil, fmt.Errorf("lemon listener returned garbage: %w", err)
		}
		return &resp, nil
	}
}

// SocketPath returns the control socket path for a layout.
func SocketPath(l config.Layout) string { return filepath.Clean(l.Socket) }

// queueLen is a small helper for log lines.
func queueLen(q *queue.Queue) int { return q.Len() }
