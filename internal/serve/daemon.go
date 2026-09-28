package serve

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/siin/lemon/internal/config"
	"github.com/siin/lemon/internal/history"
	"github.com/siin/lemon/internal/logging"
	"github.com/siin/lemon/internal/notify"
	"github.com/siin/lemon/internal/peerconn"
	"github.com/siin/lemon/internal/protocol"
	"github.com/siin/lemon/internal/queue"
	"github.com/siin/lemon/internal/tailscale"
	"github.com/siin/lemon/internal/transfer"
)

// Tailscale is the part of the tailnet the daemon needs.
//
// It is an interface so discovery can be tested without a real tailnet. The
// probe logic is the part worth testing, and it is entirely about which
// addresses get tried.
type Tailscale = tailscale.Source

// Daemon owns the TCP listener, control socket, outbox retry loop and history.
type Daemon struct {
	layout config.Layout
	cfg    *config.Config
	log    *logging.Logger

	store  *history.Store
	queue  *queue.Queue
	reg    *peerconn.Registry
	ts     Tailscale
	notify *notify.Notifier

	lock    *Lock
	tcpLn   net.Listener
	control *controlServer
	started time.Time
	receive string
	// defaultPort overrides the fallback port discovery probes. Zero means
	// DefaultPort; only tests set it.
	defaultPort int
	// stopOnce guards listener teardown; closeOnce guards store closure.
	stopOnce  sync.Once
	closeOnce sync.Once
	wg        sync.WaitGroup

	// events buffers progress lines for the control request in flight. The
	// evMu guards the streaming sink for the request in flight. When the client
	// asked for progress, events are written to the control connection as they
	// happen; otherwise they are buffered into the single final response.
	evMu     sync.Mutex
	events   []Event
	evTarget *eventSink

	// wakeSignal nudges the retry loop to drain the outbox now.
	wakeMu     sync.Mutex
	wakeSignal chan struct{}

	// work guards the queue entries currently being delivered, so the inline
	// path and the retry loop cannot both send the same one.
	work workSet

	// console receives the sparse event lines a foreground `lemon serve`
	// prints. An auto-started daemon has no console, so it stays nil.
	consoleMu sync.Mutex
	console   io.Writer
}

// eventSink marks which control request is currently in flight and, when the
// client wants live progress, streams events onto its connection.
type eventSink struct {
	conn net.Conn
	// stream is true when events should reach the client as they happen.
	stream bool
	// writeMu serialises writes to conn, since progress is emitted from the
	// transfer goroutine while the handler goroutine may also reply.
	writeMu sync.Mutex
}

// eventFrame is one newline-delimited progress frame on the control socket.
//
// A frame with Event set is a progress update; a frame without it is the final
// response. That keeps one socket in both directions and needs no second path.
type eventFrame struct {
	Event *Event `json:"event,omitempty"`
}

// writeEvent streams one progress event, ignoring a client that has gone away.
func (s *eventSink) writeEvent(e Event) {
	data, err := json.Marshal(eventFrame{Event: &e})
	if err != nil {
		return
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_ = s.conn.SetWriteDeadline(time.Now().Add(protocol.TransferIdleTimeout))
	defer s.conn.SetWriteDeadline(time.Time{})
	_, _ = s.conn.Write(append(data, '\n'))
}

// pid returns this process's id, used in health and status payloads.
func pid() int { return os.Getpid() }

// Options configures a Daemon.
type Options struct {
	// Layout is the resolved directory layout.
	Layout config.Layout
	// Config is the loaded configuration.
	Config *config.Config
	// Logger receives diagnostics; nil means silent.
	Logger *logging.Logger
	// NotifierOverride replaces the notification command.
	NotifierOverride string
	// TailscaleRunner injects a fake tailscale CLI, for tests.
	TailscaleRunner tailscale.Runner
	// NotificationsEnabled turns desktop notifications off in tests.
	NotificationsEnabled bool
	// Console receives event lines. A foreground `lemon serve` sets this to
	// stdout; an auto-started daemon leaves it nil and logs instead.
	Console io.Writer
}

// New builds a Daemon and opens its state. Sockets are bound by Start.
func New(opt Options) (*Daemon, error) {
	if opt.Logger == nil {
		opt.Logger = logging.Discard()
	}
	if err := opt.Layout.EnsureDirs(); err != nil {
		return nil, err
	}

	store, err := history.Open(opt.Layout.HistoryDB)
	if err != nil {
		return nil, err
	}
	q, err := queue.New(opt.Layout.QueueDir)
	if err != nil {
		store.Close()
		return nil, err
	}

	override := opt.NotifierOverride
	if override == "" {
		override = opt.Config.NotifyCommand
	}
	var n *notify.Notifier
	if opt.NotificationsEnabled {
		n = notify.New(override)
	} else {
		n = &notify.Notifier{Override: "__lemon_disabled__"}
	}

	port := opt.Config.Port
	var ts *tailscale.Client
	if opt.TailscaleRunner != nil {
		ts = tailscale.NewWithRunner(opt.TailscaleRunner, port)
	} else {
		ts = tailscale.New(port)
	}

	receive := opt.Layout.PublicDir
	if opt.Config.ReceiveDir != "" {
		receive = opt.Config.ReceiveDir
	}
	if err := transfer.EnsureDir(receive); err != nil {
		store.Close()
		return nil, err
	}

	d := &Daemon{
		layout:  opt.Layout,
		cfg:     opt.Config,
		log:     opt.Logger,
		store:   store,
		queue:   q,
		reg:     peerconn.NewRegistry(),
		ts:      ts,
		notify:  n,
		started: time.Now(),
		receive: receive,
	}
	d.setConsole(opt.Console)
	return d, nil
}

// Close releases every resource the daemon holds.
func (d *Daemon) Close() error {
	var err error
	d.closeOnce.Do(func() {
		d.release()
		d.wg.Wait()
		if cerr := d.store.Close(); cerr != nil {
			err = cerr
		}
	})
	return err
}

// ReceiveDir returns where inbound files are saved.
func (d *Daemon) ReceiveDir() string { return d.receive }

// Store exposes the history store, for tests.
func (d *Daemon) Store() *history.Store { return d.store }

// Queue exposes the outbox, for tests.
func (d *Daemon) Queue() *queue.Queue { return d.queue }

// Start binds the TCP and control sockets, then serves until ctx is done.
func (d *Daemon) Start(ctx context.Context) error {
	lock, err := AcquireLock(d.layout.LockFile)
	if err != nil {
		return err
	}
	d.lock = lock

	ln, err := Start(d.cfg.Port, d.cfg.ListenHost())
	if err != nil {
		lock.Release()
		d.lock = nil
		return err
	}
	d.tcpLn = ln

	ctrl, err := newControlServer(d.layout.Socket, d, d.log)
	if err != nil {
		ln.Close()
		lock.Release()
		d.lock = nil
		return err
	}
	d.control = ctrl

	if err := WritePIDFile(d.layout.PIDFile); err != nil {
		d.log.Warnf("cannot record pid: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// A prior run may have left the outbox mid-flight; make everything due now
	// so a restart flushes promptly instead of waiting out a backoff.
	d.queue.Pending()
	for _, e := range d.queue.Pending() {
		_ = d.queue.Reset(e.ID)
	}

	w := &watchdog{log: d.log, pidFile: d.layout.PIDFile, partDir: d.receive, interval: time.Hour}

	// Every goroutine started here must be counted, including the watchdog.
	var wg sync.WaitGroup
	wg.Add(4)
	go func() { defer wg.Done(); d.serveTCP(runCtx) }()
	go func() { defer wg.Done(); ctrl.serve(runCtx) }()
	go func() { defer wg.Done(); d.retryLoop(runCtx) }()
	go func() { defer wg.Done(); w.run(runCtx) }()

	<-runCtx.Done()
	d.release()
	wg.Wait()
	return nil
}

// release tears down the listening resources exactly once.
//
// Close and a completed Start both route through here, so a daemon shut down
// by Ctrl-C and one closed by its parent cannot double-close.
func (d *Daemon) release() {
	d.stopOnce.Do(func() {
		if d.control != nil {
			_ = d.control.ln.Close()
			_ = os.Remove(d.layout.Socket)
		}
		if d.tcpLn != nil {
			_ = d.tcpLn.Close()
		}
		RemovePIDFile(d.layout.PIDFile)
		if d.lock != nil {
			d.lock.Release()
			d.lock = nil
		}
	})
}

// serveTCP accepts peer sessions and answers control frames.
func (d *Daemon) serveTCP(ctx context.Context) {
	srv := protocol.NewServer(d.tcpLn, protocol.ServerOptions{
		LocalUser: d.cfg.Username,
		Handler:   protocol.SessionHandlerFunc(d.handleSession),
		Logf:      func(format string, args ...any) { d.log.Debugf(format, args...) },
		Eventf:    d.consolef,
	})
	if err := srv.Serve(ctx); err != nil {
		d.log.Warnf("listener stopped: %v", err)
	}
}

// handleSession runs one inbound TCP conversation.
func (d *Daemon) handleSession(ctx context.Context, s *protocol.Session) {
	if d.cfg.Username == "" {
		_ = s.WriteError(protocol.CodeInvalid, "lemon is not configured on this node")
		return
	}
	for {
		raw, err := s.ReadFrameTimeout(5 * time.Minute)
		if err != nil {
			if !errors.Is(err, protocol.ErrClosed) {
				d.log.Debugf("peer %s: %v", s.Remote(), err)
			}
			d.reg.SetOffline(s.RemoteUser, "")
			return
		}
		typ, _ := protocol.PeekType(raw)
		d.log.Debugf("peer %s -> %s", s.Remote(), typ)

		switch typ {
		case protocol.TypeMessage:
			if !d.handleMessage(s, raw) {
				return
			}
		case protocol.TypePing:
			var p protocol.Ping
			if err := protocol.Decode(raw, &p, protocol.TypePing); err != nil {
				d.reject(s, err)
				return
			}
			_ = s.WriteFrame(&protocol.Pong{Type: protocol.TypePong, TS: p.TS})
			d.reg.Set(s.RemoteUser, "", 0)
		case protocol.TypeTransferBegin:
			if !d.handleTransfer(ctx, s, raw) {
				return
			}
		case protocol.TypeBye:
			d.log.Debugf("peer %s said goodbye", s.Remote())
			return
		default:
			d.reject(s, fmt.Errorf("unexpected frame %q", typ))
			return
		}
	}
}

// reject reports a protocol error and ends the session.
func (d *Daemon) reject(s *protocol.Session, err error) {
	d.log.Debugf("rejecting %s: %v", s.Remote(), err)
	_ = s.WriteError(protocol.CodeInvalid, err.Error())
	_ = s.WriteFrame(&protocol.Bye{Type: protocol.TypeBye})
}

// handleMessage persists an inbound message, acknowledges it, and notifies.
// It returns false when the session must end.
func (d *Daemon) handleMessage(s *protocol.Session, raw []byte) bool {
	var m protocol.Message
	if err := protocol.Decode(raw, &m, protocol.TypeMessage); err != nil {
		d.reject(s, err)
		return false
	}
	if err := protocol.ValidateMessage(&m); err != nil {
		d.reject(s, err)
		return false
	}

	// The message is only "delivered" once it is durably stored, so the ack
	// is sent after the insert, not before.
	ts := time.Now()
	if m.Timestamp != "" {
		if parsed, err := time.Parse(time.RFC3339, m.Timestamp); err == nil {
			ts = parsed
		}
	}
	fresh, err := d.store.InsertMessage(history.Message{
		ID:        m.ID,
		Peer:      m.Sender,
		PeerAddr:  s.Remote(),
		Direction: history.In,
		Sender:    m.Sender,
		Content:   m.Content,
		Time:      ts,
		Status:    history.StatusReceived,
	})
	if err != nil {
		d.log.Errorf("cannot store message from %s: %v", m.Sender, err)
		_ = s.WriteError(protocol.CodeInternal, "cannot store message")
		return false
	}

	status := protocol.AckDelivered
	if !fresh {
		// A duplicate: acknowledge again, but do not notify twice.
		status = protocol.AckDuplicate
		d.log.Debugf("duplicate message %s from %s ignored", m.ID, m.Sender)
	} else {
		d.log.Infof("message received from %s", m.Sender)
		// One concise event line, matching the format in the design brief.
		d.consolef("[%s] message received from %s", time.Now().Format("15:04:05"), m.Sender)
		d.reg.Set(m.Sender, "", 0)
		_ = d.store.RecordPeerSeen(m.Sender, 0)
		d.notifyMessage(m.Sender, m.Content)
	}
	_ = s.WriteFrame(&protocol.Ack{Type: protocol.TypeAck, ID: m.ID, Status: status})
	return true
}

// notifyMessage emits a desktop notification, tolerating a missing notifier.
// consolef writes one event line to the listener's own output.
//
// `lemon serve` should report events as they happen without becoming a chat
// session, so the only thing printed after the banner is a timestamped line.
func (d *Daemon) consolef(format string, args ...any) {
	if d.console == nil {
		return
	}
	fmt.Fprintf(d.console, format+"\n", args...)
}

// consoleWriter receives listener event lines; nil discards them.
func (d *Daemon) setConsole(w io.Writer) {
	d.consoleMu.Lock()
	d.console = w
	d.consoleMu.Unlock()
}

// notifyMessage delivers a desktop notification for an inbound message.
func (d *Daemon) notifyMessage(peer, content string) {
	if d.notify == nil || !d.notify.Available() {
		return
	}
	body := peer + ": " + oneLine(content, notifyMaxLen)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := d.notify.Send(ctx, body); err != nil {
		// A failed notification must never affect delivery.
		d.log.Warnf("cannot send notification: %v", err)
	}
}

// notifyMaxLen keeps a notification to one readable line.
const notifyMaxLen = 120

// oneLine collapses a message into a single line for notifications.
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

// handleTransfer receives a set of files, then replies with the verdict.
func (d *Daemon) handleTransfer(ctx context.Context, s *protocol.Session, raw []byte) bool {
	var begin protocol.TransferBegin
	if err := protocol.Decode(raw, &begin, protocol.TypeTransferBegin); err != nil {
		d.reject(s, err)
		return false
	}
	if begin.Sender == "" {
		begin.Sender = s.RemoteUser
	}

	// One receive per transfer ID. Two receivers for the same ID would append
	// into the same staging file, producing a file that is longer than the
	// sender's and fails its checksum; one of them would also commit the
	// staging file out from under the other. Refusing the duplicate is honest
	// and keeps the staging area consistent, whoever caused the repeat.
	if !d.work.begin(begin.ID) {
		d.log.Warnf("refusing duplicate transfer %s from %s: it is already being received", begin.ID, begin.Sender)
		_ = s.WriteError(protocol.CodeBusy, "transfer "+begin.ID+" is already being received")
		return false
	}
	defer d.work.end(begin.ID)

	d.log.Infof("incoming transfer from %s: %d file(s)", begin.Sender, len(begin.Files))
	d.consolef("[%s] transfer from %s: %d file(s), %d bytes",
		time.Now().Format("15:04:05"), begin.Sender, len(begin.Files), sumSizes(begin.Files))
	if _, err := d.store.InsertTransfer(history.Transfer{
		ID: begin.ID, Peer: begin.Sender, PeerAddr: s.Remote(),
		Kind: "receive", Time: time.Now(), Status: history.StatusSent,
		Files: joinNames(begin.Files), Bytes: sumSizes(begin.Files),
	}); err != nil {
		d.log.Warnf("cannot record transfer: %v", err)
	}

	outcomes, err := transfer.Receive(s, &begin, d.receive)
	if err != nil {
		d.log.Warnf("transfer from %s failed: %v", begin.Sender, err)
		_ = s.WriteError(protocol.CodeIO, err.Error())
		_ = d.store.UpdateTransfer(begin.ID, history.StatusFailed, 0, err.Error())
		return false
	}

	ok := &protocol.TransferOK{Type: protocol.TypeTransferOK, ID: begin.ID}
	var saved []string
	var total int64
	for _, o := range outcomes {
		if o.Err != nil {
			ok.Failed = map[string]string{}
			ok.Failed[o.Slot.Name] = o.Err.Error()
			d.log.Warnf("cannot save %s from %s: %v", o.Spec.Name, begin.Sender, o.Err)
			continue
		}
		saved = append(saved, o.Result.Path)
		total += o.Result.Size
		if o.Result.Resumed {
			d.log.Infof("resumed %s at %d bytes", o.Result.Name, o.Result.Size)
		}
	}
	ok.Saved = saved
	_ = s.WriteFrame(ok)

	status := history.StatusDelivered
	if len(ok.Failed) > 0 && len(saved) == 0 {
		status = history.StatusFailed
	}
	_ = d.store.UpdateTransfer(begin.ID, status, total, joinFailures(ok.Failed))
	if status == history.StatusDelivered {
		d.notifyFiles(begin.Sender, saved)
	}
	d.reg.Set(begin.Sender, "", 0)
	return true
}

// notifyFiles emits one notification per received file.
func (d *Daemon) notifyFiles(peer string, paths []string) {
	if d.notify == nil || !d.notify.Available() {
		return
	}
	for _, p := range paths {
		body := peer + ": " + filepath.Base(p)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := d.notify.Send(ctx, body)
		cancel()
		if err != nil {
			d.log.Warnf("cannot send notification: %v", err)
		}
	}
}

// joinNames lists file names for the history record.
func joinNames(files []protocol.FileSpec) string {
	out := ""
	for i, f := range files {
		if i > 0 {
			out += ", "
		}
		out += f.Name
	}
	return out
}

// sumSizes totals announced file sizes.
func sumSizes(files []protocol.FileSpec) int64 {
	var n int64
	for _, f := range files {
		n += f.Size
	}
	return n
}

// joinFailures renders a failure map for the history record.
func joinFailures(m map[string]string) string {
	out := ""
	for name, reason := range m {
		if out != "" {
			out += "; "
		}
		out += name + ": " + reason
	}
	return out
}

// ListenAddr returns the bound TCP address, for status output.
func (d *Daemon) ListenAddr() string {
	if d.tcpLn == nil {
		return config.WithPort("0.0.0.0", d.cfg.Port)
	}
	return d.tcpLn.Addr().String()
}

// drainEvents returns and clears buffered progress events.
func (d *Daemon) drainEvents() []Event {
	d.evMu.Lock()
	defer d.evMu.Unlock()
	out := d.events
	d.events = nil
	return out
}

// emit reports a progress event for the active request.
//
// When the client asked for live progress the event goes straight to the
// socket; otherwise it is buffered and returned with the final response, so
// `lemon transfer` in a script still produces one tidy result.
func (d *Daemon) emit(e Event) {
	d.evMu.Lock()
	sink := d.evTarget
	if sink == nil || sink.stream {
		d.evMu.Unlock()
		if sink != nil {
			sink.writeEvent(e)
		}
		return
	}
	d.events = append(d.events, e)
	d.evMu.Unlock()
}

// portString renders the configured port.
func (d *Daemon) portString() string { return strconv.Itoa(d.cfg.Port) }
