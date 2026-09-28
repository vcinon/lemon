package protocol

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

// Handler processes one inbound session. Returning an error causes the server
// to reply with an error frame and close the connection.
type Handler interface {
	Handle(ctx context.Context, s *Session)
}

// Session is the server side of one inbound TCP conversation.
type Session struct {
	conn net.Conn
	br   *bufio.Reader
	// RemoteUser is the peer's announced username, valid after the handshake.
	RemoteUser string
	// RemoteAddr is the peer's TCP address.
	RemoteAddr string
	// ID is a per-connection identifier used in log lines.
	ID string
	// mu serialises writes so handlers and keepalives never interleave frames.
	mu sync.Mutex
}

// Remote returns the peer's address string.
func (s *Session) Remote() string { return s.RemoteAddr }

// Conn exposes the underlying connection for streaming.
func (s *Session) Conn() net.Conn { return s.conn }

// NewSession builds a Session around an accepted connection. It performs the
// server-side handshake and validates the protocol version.
func NewSession(conn net.Conn, localUser string, timeout time.Duration) (*Session, error) {
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	s := &Session{
		conn:       conn,
		br:         bufio.NewReaderSize(conn, 64<<10),
		RemoteAddr: conn.RemoteAddr().String(),
		ID:         newConnID(),
	}
	if err := s.handshake(localUser, timeout); err != nil {
		return nil, err
	}
	return s, nil
}

// newConnID returns a short connection identifier for logs.
func newConnID() string {
	id, err := NewID(3)
	if err != nil {
		return "??????"
	}
	return id
}

// handshake reads the client's hello and answers with ours.
func (s *Session) handshake(localUser string, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = DefaultFrameTimeout
	}
	_ = s.conn.SetReadDeadline(time.Now().Add(timeout))
	raw, err := ReadFrame(s.br)
	if err != nil {
		return fmt.Errorf("handshake: %w", err)
	}
	hello, err := DecodeHello(raw)
	if err != nil {
		// Try to tell the peer why before hanging up.
		_ = s.WriteError(CodeInvalid, err.Error())
		return err
	}
	s.RemoteUser = hello.User
	if err := s.WriteHello(localUser); err != nil {
		return fmt.Errorf("handshake reply: %w", err)
	}
	_ = s.conn.SetReadDeadline(time.Time{})
	return nil
}

// WriteFrame sends a control frame.
func (s *Session) WriteFrame(v any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = s.conn.SetWriteDeadline(time.Now().Add(DefaultFrameTimeout))
	defer s.conn.SetWriteDeadline(time.Time{})
	return WriteFrame(s.conn, v)
}

// WriteHello announces this side.
func (s *Session) WriteHello(user string) error {
	return s.WriteFrame(&Hello{Type: TypeHello, Version: Version, User: user, Kind: "server"})
}

// WriteError sends an error frame.
func (s *Session) WriteError(code, reason string) error {
	return s.WriteFrame(&Error{Type: TypeError, Code: code, Reason: reason})
}

// WriteAck acknowledges a message.
func (s *Session) WriteAck(id, status string) error {
	return s.WriteFrame(&Ack{Type: TypeAck, ID: id, Status: status})
}

// ReadFrame reads the next control frame from the peer.
func (s *Session) ReadFrame() ([]byte, error) {
	return ReadFrame(s.br)
}

// ReadFrameTimeout reads the next frame under a deadline.
func (s *Session) ReadFrameTimeout(d time.Duration) ([]byte, error) {
	if err := s.conn.SetReadDeadline(time.Now().Add(d)); err != nil {
		return nil, err
	}
	defer s.conn.SetReadDeadline(time.Time{})
	return ReadFrame(s.br)
}

// ReadInto streams exactly size bytes into w, tolerating short writes.
//
// The read deadline is an inactivity timer refreshed as bytes arrive, so a
// slow but healthy transfer of any size still completes.
func (s *Session) ReadInto(w io.Writer, size int64) (int64, error) {
	if size < 0 {
		return 0, fmt.Errorf("negative read size %d", size)
	}
	refresh := &deadlineReader{
		r:        s.conn,
		interval: 4 << 20, // 4 MiB
		reset: func() {
			_ = s.conn.SetReadDeadline(time.Now().Add(TransferIdleTimeout))
		},
	}
	refresh.reset()
	defer s.conn.SetReadDeadline(time.Time{})
	n, err := io.Copy(w, io.LimitReader(refresh, size))
	if err != nil {
		return n, err
	}
	if n != size {
		return n, fmt.Errorf("incomplete stream: expected %d bytes, got %d", size, n)
	}
	return n, nil
}

// Close terminates the session politely.
func (s *Session) Close() error { return s.conn.Close() }

// SetReadDeadline exposes deadline control to handlers.
func (s *Session) SetReadDeadline(t time.Time) error { return s.conn.SetReadDeadline(t) }

// SetWriteDeadline exposes deadline control to handlers.
func (s *Session) SetWriteDeadline(t time.Time) error { return s.conn.SetWriteDeadline(t) }

// Server accepts Lemon sessions on a TCP listener.
type Server struct {
	ln        net.Listener
	addr      string
	localUser string
	handler   Handler
	logf      func(string, ...any)
	onEvent   func(string, ...any)
	stopping  chan struct{}
	once      sync.Once
}

// ServerOptions configures a Server.
type ServerOptions struct {
	// LocalUser is announced in the handshake reply.
	LocalUser string
	// Handler processes each accepted session.
	Handler Handler
	// Logf receives debug diagnostics; nil discards them.
	Logf func(string, ...any)
	// Eventf receives one sparse line per notable event; nil discards them.
	Eventf func(string, ...any)
}

// NewServer binds ln and serves sessions as described by opt.
func NewServer(ln net.Listener, opt ServerOptions) *Server {
	logf := opt.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}
	eventf := opt.Eventf
	if eventf == nil {
		eventf = func(string, ...any) {}
	}
	return &Server{
		ln:        ln,
		addr:      ln.Addr().String(),
		localUser: opt.LocalUser,
		handler:   opt.Handler,
		logf:      logf,
		onEvent:   eventf,
		stopping:  make(chan struct{}),
	}
}

// eventf emits one sparse operator-facing line.
func (s *Server) eventf(format string, args ...any) { s.onEvent(format, args...) }

// Addr returns the bound address.
func (s *Server) Addr() string { return s.addr }

// Serve accepts connections until the listener is closed.
func (s *Server) Serve(ctx context.Context) error {
	// Unblock Accept when ctx is cancelled.
	go func() {
		select {
		case <-ctx.Done():
			_ = s.ln.Close()
		case <-s.stopping:
		}
	}()

	var wg sync.WaitGroup
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			s.once.Do(func() { close(s.stopping) })
			wg.Wait()
			if ctx.Err() != nil {
				return nil
			}
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("accept: %w", err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.serveConn(ctx, conn)
		}()
	}
}

func (s *Server) serveConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	sess, err := NewSession(conn, s.localUser, DefaultFrameTimeout)
	if err != nil {
		s.logf("handshake failed from %s: %v", conn.RemoteAddr(), err)
		return
	}
	s.logf("connection %s from %s as %q", sess.ID, sess.RemoteAddr, sess.RemoteUser)
	defer s.logf("connection %s from %s closed", sess.ID, sess.RemoteUser)

	// One line per accepted connection, so the operator can see peers arrive
	// without the listener turning into a chat log.
	s.eventf("[%s] %s connected", time.Now().Format("15:04:05"), sess.RemoteUser)

	defer func() {
		if r := recover(); r != nil {
			s.logf("connection %s panicked: %v", sess.ID, r)
		}
	}()
	s.handler.Handle(ctx, sess)
}

// Close stops the server.
func (s *Server) Close() error {
	s.once.Do(func() { close(s.stopping) })
	return s.ln.Close()
}

// SessionHandlerFunc adapts a function to Handler.
type SessionHandlerFunc func(ctx context.Context, s *Session)

// Handle implements Handler.
func (f SessionHandlerFunc) Handle(ctx context.Context, s *Session) { f(ctx, s) }

// ExceededFrameLimit reports whether err is a frame-size rejection, so the
// caller can log it distinctly from a malformed-peer error.
func ExceededFrameLimit(err error) bool {
	return errors.Is(err, ErrFrameTooLarge)
}

// DescribePeer renders "user@addr" for logs and notifications.
func DescribePeer(user, addr string) string {
	user = strings.TrimSpace(user)
	if user == "" {
		return addr
	}
	if addr == "" {
		return user
	}
	return user + "@" + addr
}
