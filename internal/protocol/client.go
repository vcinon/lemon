package protocol

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"time"
)

// Client is a single request/response conversation with a remote Lemon peer.
//
// A session is deliberately short-lived: one Client performs one logical
// exchange, then closes. This keeps reconnect and retry logic trivial and
// removes any chance of half-open state leaking between messages.
type Client struct {
	conn   net.Conn
	br     *bufio.Reader
	remote string
	user   string
	// remoteUser is the identity the peer announced, learned during the
	// handshake. Discovery reads it to learn who answered without a second
	// round trip.
	remoteUser string
	// version is the protocol version the peer announced.
	version int
}

// Dial establishes a TCP session to addr and performs the version handshake.
func Dial(ctx context.Context, addr, user string) (*Client, error) {
	d := net.Dialer{Timeout: DefaultDialTimeout}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	c := &Client{conn: conn, br: bufio.NewReaderSize(conn, 64<<10), remote: addr, user: user}
	if err := c.handshake(user); err != nil {
		conn.Close()
		return nil, err
	}
	return c, nil
}

// User returns the local username announced during the handshake.
func (c *Client) User() string { return c.user }

// RemoteUser returns the peer's username, learned during the handshake.
//
// This is how Lemon recognises a peer without any account, key or shared
// secret: the node answers on the common tailnet, states its own name, and
// both sides trust that statement the same way SSH trusts a host key exchange
// in reverse. Authentication is deliberately out of scope; confidentiality
// comes from the tailnet, not from Lemon.
func (c *Client) RemoteUser() string { return c.remoteUser }

// Version returns the protocol version the peer announced.
func (c *Client) Version() int { return c.version }

// handshake exchanges hello frames and rejects version mismatches.
func (c *Client) handshake(user string) error {
	if err := c.WriteHello(user); err != nil {
		return err
	}
	_ = c.conn.SetReadDeadline(time.Now().Add(DefaultFrameTimeout))
	raw, err := ReadFrame(c.br)
	if err != nil {
		return c.wrapFrameErr(err, "waiting for hello")
	}
	switch t, _ := PeekType(raw); t {
	case TypeHello:
		hello, err := DecodeHello(raw)
		if err != nil {
			_ = c.WriteError("bad_protocol", err.Error())
			return err
		}
		c.remoteUser, c.version = hello.User, hello.Version
		return nil
	case TypeError:
		var e Error
		if err := Decode(raw, &e, TypeError); err == nil {
			return peerError(e)
		}
		return fmt.Errorf("peer refused the connection")
	default:
		return fmt.Errorf("%w: expected hello, got %q", ErrBadFrame, t)
	}
}

// peerError converts a remote error frame into a local error.
func peerError(e Error) error {
	code := e.Code
	if code == "" {
		code = "remote_error"
	}
	return &RemoteError{Code: code, Reason: e.Reason}
}

// RemoteError is a failure reported by the peer via an error frame.
type RemoteError struct {
	Code   string
	Reason string
}

func (e *RemoteError) Error() string {
	if e.Reason == "" {
		return e.Code
	}
	return e.Reason
}

// Remote error codes.
const (
	CodeBusy     = "busy"
	CodeNotFound = "not_found"
	CodeInvalid  = "invalid"
	CodeIO       = "io_error"
	CodeInternal = "internal"
)

// Remote returns the RemoteError carried by err, if any.
func Remote(err error) (*RemoteError, bool) {
	var re *RemoteError
	if errors.As(err, &re) {
		return re, true
	}
	return nil, false
}

// Close releases the connection.
func (c *Client) Close() error { return c.conn.Close() }

// RemoteAddr returns the peer's address.
func (c *Client) RemoteAddr() string { return c.remote }

// WriteHello sends this side's hello frame.
func (c *Client) WriteHello(user string) error {
	_ = c.conn.SetWriteDeadline(time.Now().Add(DefaultFrameTimeout))
	defer c.clearWriteDeadline()
	return WriteFrame(c.conn, &Hello{Type: TypeHello, Version: Version, User: user, Kind: "client"})
}

// WriteError sends an error frame.
func (c *Client) WriteError(code, reason string) error {
	_ = c.conn.SetWriteDeadline(time.Now().Add(DefaultFrameTimeout))
	defer c.clearWriteDeadline()
	return WriteFrame(c.conn, &Error{Type: TypeError, Code: code, Reason: reason})
}

// clearWriteDeadline resets the write deadline so raw streaming can proceed.
func (c *Client) clearWriteDeadline() { _ = c.conn.SetWriteDeadline(time.Time{}) }

// Send writes a frame and waits for the reply frame.
func (c *Client) Send(req any) ([]byte, error) {
	_ = c.conn.SetWriteDeadline(time.Now().Add(DefaultFrameTimeout))
	if err := WriteFrame(c.conn, req); err != nil {
		c.clearWriteDeadline()
		return nil, err
	}
	c.clearWriteDeadline()

	_ = c.conn.SetReadDeadline(time.Now().Add(DefaultFrameTimeout))
	defer c.conn.SetReadDeadline(time.Time{})

	raw, err := ReadFrame(c.br)
	if err != nil {
		return nil, c.wrapFrameErr(err, "waiting for reply")
	}
	if t, _ := PeekType(raw); t == TypeError {
		var e Error
		if err := Decode(raw, &e, TypeError); err == nil {
			return nil, peerError(e)
		}
	}
	return raw, nil
}

// SendMessage delivers a message and returns the peer's ack.
func (c *Client) SendMessage(m *Message) (*Ack, error) {
	raw, err := c.Send(m)
	if err != nil {
		return nil, err
	}
	var ack Ack
	if err := Decode(raw, &ack, TypeAck); err != nil {
		return nil, fmt.Errorf("peer sent %q where an ack was expected", peekOr(raw))
	}
	if ack.ID != m.ID {
		return nil, fmt.Errorf("%w: ack id %q does not match message id %q", ErrBadFrame, ack.ID, m.ID)
	}
	return &ack, nil
}

// peekOr returns a frame's type for error messages.
func peekOr(raw []byte) string {
	t, _ := PeekType(raw)
	return t
}

// StartTransfer performs transfer_begin and returns the receiver's offsets.
func (c *Client) StartTransfer(t *TransferBegin) (*TransferReady, error) {
	raw, err := c.Send(t)
	if err != nil {
		return nil, err
	}
	var ready TransferReady
	if err := Decode(raw, &ready, TypeTransferReady); err != nil {
		return nil, fmt.Errorf("peer sent %q where transfer_ready was expected", peekOr(raw))
	}
	if ready.ID != t.ID {
		return nil, fmt.Errorf("%w: transfer_ready id %q does not match %q", ErrBadFrame, ready.ID, t.ID)
	}
	if err := ValidateTransferReady(&ready); err != nil {
		return nil, err
	}
	return &ready, nil
}

// StartRaw switches the connection into raw streaming mode for one file.
// StartRaw arms the idle deadline for a raw byte stream. The deadline is an
// inactivity timer, not a total-transfer budget, so a multi-gigabyte transfer
// over a slow link is not cut off.
func (c *Client) StartRaw(size int64) error {
	return c.conn.SetWriteDeadline(time.Now().Add(TransferIdleTimeout))
}

// WriteFile streams exactly n bytes of r to the peer and reads the file_end
// reply. progress, if non-nil, is called with the running byte count.
func (c *Client) WriteFile(id string, r io.Reader, n int64, progress func(int64)) (*FileEnd, error) {
	if err := c.StartRaw(n); err != nil {
		return nil, err
	}
	counter := &countingWriter{
		w:        c.conn,
		progress: progress,
		touch: func() {
			_ = c.conn.SetWriteDeadline(time.Now().Add(TransferIdleTimeout))
		},
	}
	written, err := io.Copy(counter, io.LimitReader(r, n))
	c.clearWriteDeadline()
	if err != nil {
		return nil, fmt.Errorf("cannot stream %d bytes: %w", n, err)
	}
	if written != n {
		return nil, fmt.Errorf("short read: expected %d bytes, got %d", n, written)
	}
	if progress != nil {
		progress(written)
	}
	_ = c.conn.SetReadDeadline(time.Now().Add(TransferIdleTimeout))
	raw, err := ReadFrame(c.br)
	if err != nil {
		return nil, c.wrapFrameErr(err, "waiting for file_end")
	}
	var fe FileEnd
	if err := Decode(raw, &fe, TypeFileEnd); err != nil {
		return nil, fmt.Errorf("peer sent %q where file_end was expected", peekOr(raw))
	}
	if fe.ID != id {
		return nil, fmt.Errorf("%w: file_end id %q does not match %q", ErrBadFrame, fe.ID, id)
	}
	if err := ValidateFileEnd(&fe); err != nil {
		return nil, err
	}
	return &fe, nil
}

// FinishTransfer signals completion and reads the receiver's verdict.
func (c *Client) FinishTransfer(id string) (*TransferOK, error) {
	raw, err := c.Send(&TransferDone{Type: TypeTransferDone, ID: id})
	if err != nil {
		return nil, err
	}
	var ok TransferOK
	if err := Decode(raw, &ok, TypeTransferOK); err != nil {
		return nil, fmt.Errorf("peer sent %q where transfer_ok was expected", peekOr(raw))
	}
	if ok.ID != id {
		return nil, fmt.Errorf("%w: transfer_ok id %q does not match %q", ErrBadFrame, ok.ID, id)
	}
	return &ok, nil
}

// wrapFrameErr converts EOF into a clearer message.
func (c *Client) wrapFrameErr(err error, what string) error {
	if errors.Is(err, ErrClosed) {
		return fmt.Errorf("%s: %w", what, ErrClosed)
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return fmt.Errorf("%s: timed out", what)
	}
	return fmt.Errorf("%s: %w", what, err)
}

// Identity is what a probe learned about a node on the tailnet.
type Identity struct {
	// User is the username the node announced in its hello.
	User string
	// Version is the protocol version it speaks.
	Version int
	// Latency is the time the handshake took, as a reachability signal.
	Latency time.Duration
}

// Identify dials addr and learns who is there.
//
// A successful return means a Lemon answered: the handshake itself is the
// proof, and the answer is the peer's own username. Discovery uses it to find
// other Lemons on the tailnet without any prior configuration.
func Identify(ctx context.Context, addr, user string) (Identity, error) {
	start := time.Now()
	c, err := Dial(ctx, addr, user)
	if err != nil {
		return Identity{}, err
	}
	defer c.Close()
	return Identity{
		User:    c.RemoteUser(),
		Version: c.Version(),
		Latency: time.Since(start),
	}, nil
}

// MeasureLatency dials addr and times one ping/pong round trip.
func MeasureLatency(ctx context.Context, addr, user string) (time.Duration, error) {
	c, err := Dial(ctx, addr, user)
	if err != nil {
		return 0, err
	}
	defer c.Close()

	start := time.Now()
	raw, err := c.Send(&Ping{Type: TypePing, TS: start.UnixNano()})
	if err != nil {
		return 0, err
	}
	var p Pong
	if err := Decode(raw, &p, TypePong); err != nil {
		return 0, fmt.Errorf("peer sent %q where pong was expected", peekOr(raw))
	}
	return time.Since(start), nil
}

// IsOffline reports whether err looks like an unreachable peer rather than a
// protocol or logic failure. Use it to decide whether a message should be
// queued instead of retried immediately.
func IsOffline(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrClosed) {
		return true
	}
	// Timeouts are net.Error; refused/reset/unreachable are wrapped syscall
	// errors, so check both.
	var ne net.Error
	if errors.As(err, &ne) {
		return true
	}
	for _, errno := range []syscall.Errno{
		syscall.ECONNREFUSED, syscall.ECONNRESET, syscall.ECONNABORTED,
		syscall.EHOSTUNREACH, syscall.ENETUNREACH, syscall.ENETDOWN,
		syscall.ETIMEDOUT, syscall.EPIPE,
	} {
		if errors.Is(err, errno) {
			return true
		}
	}
	return false
}
