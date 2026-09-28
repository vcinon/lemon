package serve

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/siin/lemon/internal/config"
	"github.com/siin/lemon/internal/history"
	"github.com/siin/lemon/internal/peerconn"
	"github.com/siin/lemon/internal/protocol"
	"github.com/siin/lemon/internal/queue"
	"github.com/siin/lemon/internal/transfer"
)

// Stable error codes the CLI maps onto exit statuses.
const (
	CodeNotConfigured = "not_configured"
	CodeUnknownPeer   = "unknown_peer"
	CodeNoDefault     = "no_default_peer"
	CodeOffline       = "offline"
	CodeInvalid       = "invalid"
	CodeNotFound      = "not_found"
	CodeIO            = "io_error"
	CodeInternal      = "internal"
	CodeBusy          = "busy"
)

// dispatch routes a control request.
//
// The config is re-read first, on every request. The listener is a long-lived
// process while `lemon peer add`, `lemon setup` and friends are short-lived
// ones that rewrite config.json; without this, a peer added after the listener
// started would be unknown to the only process that can act on it, and the
// change would appear to do nothing until something restarted. The file is
// written by an atomic rename, so a read here sees either the old file or the
// new one, never a half-written one.
func (d *Daemon) dispatch(ctx context.Context, req Request, conn net.Conn) *Response {
	d.evMu.Lock()
	d.evTarget = &eventSink{conn: conn, stream: req.ProgressSink()}
	d.events = nil
	d.evMu.Unlock()
	defer func() {
		d.evMu.Lock()
		d.evTarget = nil
		d.evMu.Unlock()
	}()

	d.reloadConfig(ctx)

	switch req.Op {
	case OpPing:
		return d.opPing()
	case OpSend:
		return d.opSend(ctx, req)
	case OpTransfer:
		return d.opTransfer(ctx, req)
	case OpStatus:
		return d.opStatus(ctx, req)
	case OpHistory:
		return d.opHistory(req)
	case OpPeers:
		return d.opPeers(ctx, req)
	case OpDiscover:
		return d.opDiscover(ctx, req)
	case OpConnect:
		return d.opConnect(ctx, req)
	case OpDisconnect:
		return d.opDisconnect(ctx, req)
	case OpQueue:
		return d.opQueue(ctx)
	case OpFlush:
		return d.opFlush(ctx)
	default:
		return fail(CodeInvalid, "unknown operation %q", req.Op)
	}
}

// fail builds an error response.
func fail(code, format string, args ...any) *Response {
	// %w is not supported by Sprintf, so normalise it to %v.
	return &Response{OK: false, Code: code, Error: fmt.Sprintf(normaliseVerb(format), args...)}
}

// normaliseVerb rewrites %w to %v for use with fmt.Sprintf.
func normaliseVerb(format string) string { return strings.ReplaceAll(format, "%w", "%v") }

// classify maps a network or protocol error onto a stable code.
func classify(err error) (code string, offline bool) {
	if err == nil {
		return "", false
	}
	if re, ok := protocol.Remote(err); ok {
		return re.Code, re.Code == protocol.CodeBusy
	}
	if errors.Is(err, config.ErrNotConfigured) {
		return CodeNotConfigured, false
	}
	if errors.Is(err, peerconn.ErrUnknownPeer) {
		return CodeUnknownPeer, false
	}
	if errors.Is(err, peerconn.ErrNoDefault) {
		return CodeNoDefault, false
	}
	if errors.Is(err, transfer.ErrUnsafeName) {
		return CodeInvalid, false
	}
	if errors.Is(err, history.ErrNotFound) {
		return CodeNotFound, false
	}
	if protocol.IsOffline(err) {
		return CodeOffline, true
	}
	if errors.Is(err, protocol.ErrVersionMismatch) {
		return CodeInvalid, false
	}
	return CodeIO, false
}

// errorResponse converts an error into a response.
func errorResponse(err error) *Response {
	code, offline := classify(err)
	return &Response{OK: false, Code: code, Error: err.Error(), Offline: offline}
}

// opPing answers a health probe.
func (d *Daemon) opPing() *Response {
	return &Response{
		OK: true,
		Health: &Health{
			OK: true, PID: pid(), Version: protocol.Version,
			User: d.cfg.Username, Addr: d.ListenAddr(), StartedAt: d.started.Unix(),
		},
	}
}

// resolveTarget determines the peer name and address for a request.
func (d *Daemon) resolveTarget(ctx context.Context, req Request) (name, addr string, err error) {
	cfg := d.cfg
	if req.Addr != "" {
		// An explicit address bypasses naming, but a name is still useful for
		// history and notifications.
		name = req.Peer
		normalised, aerr := config.NormalizeAddr(req.Addr)
		if aerr != nil {
			return "", "", aerr
		}
		return name, normalised, nil
	}

	name = req.Peer
	if name == "" {
		if cfg.DefaultPeer == "" {
			return "", "", fmt.Errorf("%w: set one with: lemon peer add <name> [<ip>]", peerconn.ErrNoDefault)
		}
		name = cfg.DefaultPeer
	}
	peer, ok := cfg.Peer(name)
	if !ok {
		return "", "", fmt.Errorf("%w: %s (known peers: %s)",
			peerconn.ErrUnknownPeer, name, strings.Join(cfg.PeerNames(), ", "))
	}
	if peer.Disabled {
		return "", "", fmt.Errorf("%w: %s", peerconn.ErrDisabled, name)
	}
	if peer.Addr != "" {
		normalised, aerr := config.NormalizeAddr(peer.Addr)
		if aerr != nil {
			return "", "", aerr
		}
		return name, normalised, nil
	}
	resolver := peerconn.NewResolver(d.reg, d.ts, d.log)
	resolved, rerr := resolver.Resolve(ctx, name)
	if rerr == nil {
		return name, resolved, nil
	}

	// A lemon's username is chosen by its owner and need not be the node's
	// tailnet name, so the tailnet lookup can fail for a peer that is plainly
	// there. Discovery knows better: it reads the name from the peer's own
	// handshake. Falling back to it means `lemon peer add <name>` works without
	// the user having to know or type an address.
	if addr, ok := d.findByDiscovery(ctx, name); ok {
		return name, addr, nil
	}
	return name, "", rerr
}

// findByDiscovery searches the tailnet for a lemon that answers to name and
// returns its address.
func (d *Daemon) findByDiscovery(ctx context.Context, name string) (string, bool) {
	resp := d.opDiscover(ctx, Request{Op: OpDiscover})
	if !resp.OK {
		d.log.Debugf("discovery fallback for %s failed: %s", name, resp.Error)
		return "", false
	}
	for _, p := range resp.Discovered {
		if p.User == name && p.Addr != "" {
			d.log.Debugf("resolved %s to %s via discovery", name, p.Addr)
			// Cache it, so the next send does not repeat the search.
			if d.reg != nil {
				d.reg.Set(name, p.Addr, 0)
			}
			return p.Addr, true
		}
	}
	return "", false
}

// opSend queues a message durably, then attempts immediate delivery.
func (d *Daemon) opSend(ctx context.Context, req Request) *Response {
	if d.cfg.Username == "" {
		return fail(CodeNotConfigured, "%v", config.ErrNotConfigured)
	}
	name, addr, err := d.resolveTarget(ctx, req)
	if err != nil {
		return errorResponse(err)
	}
	if err := protocol.ValidateContent(req.Message); err != nil {
		return fail(CodeInvalid, "%v", err)
	}

	id, err := protocol.MessageID()
	if err != nil {
		return fail(CodeInternal, "%v", err)
	}
	msg := &queue.Message{
		ID: id, Sender: d.cfg.Username, Content: req.Message, Created: time.Now(),
	}

	// Durable before we touch the network: a crash here must not lose the
	// message.
	if err := d.queue.EnqueueMessage(name, addr, msg); err != nil {
		return fail(CodeIO, "cannot queue message: %v", err)
	}
	if _, err := d.store.InsertMessage(history.Message{
		ID: id, Peer: name, PeerAddr: addr, Direction: history.Out,
		Sender: d.cfg.Username, Content: msg.Content, Time: msg.Created,
		Status: history.StatusQueued,
	}); err != nil {
		d.log.Warnf("cannot record outgoing message: %v", err)
	}

	d.emit(Event{Kind: "state", Text: "connecting to " + name + "..."})
	// Claim the queued entry, so the retry loop does not also deliver this
	// message while it is still in flight and send it twice.
	if !d.work.begin(id) {
		return fail(CodeBusy, "message is already being sent to %s", name)
	}
	defer d.work.end(id)

	start := time.Now()
	ack, err := deliverMessage(ctx, d, name, addr, msg)
	if err != nil {
		code, offline := classify(err)
		if offline {
			d.log.Debugf("peer %s offline, message %s stays queued: %v", name, id, err)
			d.reg.SetOffline(name, "offline")
			_ = d.store.RecordPeerSeen(name, 0)
			d.emit(Event{Kind: "state", Text: name + ": offline"})
			d.emit(Event{Kind: "state", Text: "message queued"})
			return &Response{
				OK: false, Code: code, Error: err.Error(),
				Offline: true, Queued: true, State: "offline",
				MessageID: id, Peer: name, Addr: addr,
			}
		}
		return &Response{
			OK: false, Code: code, Error: err.Error(),
			State: "failed", MessageID: id, Peer: name, Addr: addr,
		}
	}

	latency := time.Since(start)
	d.reg.Set(name, addr, latency)
	_ = d.store.RecordPeerSeen(name, int(latency.Milliseconds()))
	d.emit(Event{Kind: "state", Text: "connected"})
	d.emit(Event{Kind: "state", Text: "sent"})
	if ack.Status == protocol.AckDuplicate {
		d.emit(Event{Kind: "state", Text: "delivered (already received earlier)"})
	} else {
		d.emit(Event{Kind: "state", Text: "delivered"})
	}
	return &Response{
		OK: true, State: "delivered", MessageID: id, Peer: name, Addr: addr,
		LatencyMS: int(latency.Milliseconds()),
	}
}

// deliverMessage attempts one delivery and cleans up on acknowledgement.
func deliverMessage(ctx context.Context, d *Daemon, name, addr string, msg *queue.Message) (*protocol.Ack, error) {
	c, err := protocol.Dial(ctx, addr, d.cfg.Username)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	frame, err := protocol.NewMessage(msg.ID, msg.Sender, msg.Content, msg.Created)
	if err != nil {
		return nil, err
	}
	if err := d.store.SetStatus(msg.ID, history.StatusSent); err != nil && !errors.Is(err, history.ErrNotFound) {
		d.log.Debugf("cannot mark message sent: %v", err)
	}
	ack, err := c.SendMessage(frame)
	if err != nil {
		return nil, err
	}
	// Only an acknowledgement removes the queued copy.
	if err := d.queue.MarkDelivered(msg.ID); err != nil && !errors.Is(err, queue.ErrNotFound) {
		d.log.Warnf("cannot clear queue entry: %v", err)
	}
	if err := d.store.SetStatus(msg.ID, history.StatusDelivered); err != nil && !errors.Is(err, history.ErrNotFound) {
		d.log.Debugf("cannot mark message delivered: %v", err)
	}
	return ack, nil
}

// opTransfer streams files to a peer, queueing the job if the peer is away.
func (d *Daemon) opTransfer(ctx context.Context, req Request) *Response {
	if d.cfg.Username == "" {
		return fail(CodeNotConfigured, "%v", config.ErrNotConfigured)
	}
	name, addr, err := d.resolveTarget(ctx, req)
	if err != nil {
		return errorResponse(err)
	}
	sources, err := transfer.StatAll(req.Files)
	if err != nil {
		return fail(CodeNotFound, "%v", err)
	}

	transferID, err := protocol.TransferID()
	if err != nil {
		return fail(CodeInternal, "%v", err)
	}
	// Digests are computed up front so the receiver can verify integrity and
	// so a resumed transfer is verified across the whole file.
	qfiles := make([]queue.File, 0, len(sources))
	for i := range sources {
		digest, derr := transfer.DigestFile(sources[i].Path)
		if derr != nil {
			return fail(CodeIO, "%v", derr)
		}
		sources[i].SHA256 = digest
		qfiles = append(qfiles, queue.File{
			Path: sources[i].Path, Name: sources[i].Name,
			Size: sources[i].Size, SHA256: digest,
		})
	}

	job := &queue.Transfer{ID: transferID, Peer: name, Files: qfiles, Created: time.Now()}
	if err := d.queue.EnqueueTransfer(job); err != nil {
		return fail(CodeIO, "cannot queue transfer: %v", err)
	}
	if _, err := d.store.InsertTransfer(history.Transfer{
		ID: transferID, Peer: name, PeerAddr: addr, Kind: "send",
		Time: job.Created, Status: history.StatusQueued,
		Bytes: transfer.TotalSize(sources), Files: joinSources(sources),
	}); err != nil {
		d.log.Warnf("cannot record transfer: %v", err)
	}

	d.emit(Event{Kind: "state", Text: "checking " + name + "... connected"})
	// Claim the queued entry for the duration of the inline attempt. The retry
	// loop runs on a timer and can otherwise pick this same entry up while this
	// transfer is still streaming, sending the files twice into one staging file.
	if !d.work.begin(transferID) {
		return fail(CodeBusy, "transfer is already being sent to %s", name)
	}
	defer d.work.end(transferID)

	results, err := d.runTransfer(ctx, name, addr, job, sources, req.ProgressSink())
	if err != nil {
		code, offline := classify(err)
		if offline {
			d.reg.SetOffline(name, "offline")
			_ = d.store.RecordPeerSeen(name, 0)
			d.emit(Event{Kind: "state", Text: name + ": offline"})
			d.emit(Event{Kind: "state", Text: "transfer queued"})
			return &Response{
				OK: false, Code: code, Error: err.Error(), Offline: true, Queued: true,
				State: "offline", MessageID: transferID, Peer: name, Addr: addr,
			}
		}
		_ = d.store.UpdateTransfer(transferID, history.StatusFailed, 0, err.Error())
		return &Response{
			OK: false, Code: code, Error: err.Error(),
			State: "failed", MessageID: transferID, Peer: name, Addr: addr,
		}
	}

	reports := make([]TransferReport, 0, len(results))
	var sentBytes int64
	failed := 0
	for _, r := range results {
		rep := TransferReport{Name: r.Source.Name, Size: r.Source.Size, Resumed: r.Result != nil && r.Result.Resumed}
		if r.Err != nil {
			rep.Err = r.Err.Error()
			failed++
		} else {
			sentBytes += r.Result.Size
			rep.Path = r.Result.Path
			if rep.Path == "" {
				rep.Path = r.Result.Name
			}
			d.emit(Event{Kind: "done", Text: r.Result.Name, Name: r.Result.Name, Done: r.Result.Size, N: r.Result.Size})
		}
		reports = append(reports, rep)
	}

	if failed > 0 {
		err := fmt.Errorf("%d of %d file(s) could not be sent", failed, len(results))
		_ = d.store.UpdateTransfer(transferID, history.StatusFailed, sentBytes, err.Error())
		return &Response{
			OK: false, Code: CodeIO, Error: err.Error(),
			State: "failed", MessageID: transferID, Peer: name, Addr: addr,
			Transfers: reports,
		}
	}

	if err := d.queue.MarkDelivered(transferID); err != nil && !errors.Is(err, queue.ErrNotFound) {
		d.log.Warnf("cannot clear transfer queue entry: %v", err)
	}
	_ = d.store.UpdateTransfer(transferID, history.StatusDelivered, sentBytes, "")
	d.reg.Set(name, addr, 0)
	_ = d.store.RecordPeerSeen(name, 0)
	return &Response{
		OK: true, State: "delivered", MessageID: transferID, Peer: name, Addr: addr,
		Transfers: reports,
	}
}

// runTransfer performs one transfer attempt, emitting progress events.
func (d *Daemon) runTransfer(ctx context.Context, name, addr string, job *queue.Transfer, sources []transfer.Source, progress bool) ([]transfer.SendFileResult, error) {
	c, err := protocol.Dial(ctx, addr, d.cfg.Username)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	sj := transfer.SendJob{ID: job.ID, Files: sources, Resume: true}
	if progress {
		sj.Progress = func(file string, n, done int64) {
			d.emit(Event{Kind: "progress", Name: file, N: n, Done: done})
		}
	}
	return transfer.Send(c, sj)
}

// joinSources lists source basenames for the history record.
func joinSources(sources []transfer.Source) string {
	parts := make([]string, 0, len(sources))
	for _, s := range sources {
		parts = append(parts, s.Name)
	}
	return strings.Join(parts, ", ")
}

// opHistory lists stored messages.
func (d *Daemon) opHistory(req Request) *Response {
	peer := req.HistoryPeer
	if peer != "" {
		if _, ok := d.cfg.Peer(peer); !ok {
			return fail(CodeUnknownPeer, "%w: %s", peerconn.ErrUnknownPeer, peer)
		}
	}
	msgs, err := d.store.List(history.Options{Peer: peer, Limit: req.HistoryLimit})
	if err != nil {
		return fail(CodeIO, "%v", err)
	}
	out := make([]HistoryEntry, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, HistoryEntry{
			ID: m.ID, Peer: m.Peer, Sender: m.Sender, Content: m.Content,
			Direction: string(m.Direction), Time: m.Time.Unix(), Status: m.Status,
		})
	}
	return &Response{OK: true, History: out}
}

// opPeers lists configured peers with live status.
func (d *Daemon) opPeers(ctx context.Context, req Request) *Response {
	reports := d.peerReports(ctx, req.Probe)
	return &Response{OK: true, Peers: reports}
}

// peerReports builds the peer list, optionally measuring latency.
func (d *Daemon) peerReports(ctx context.Context, probe bool) []PeerReport {
	// A refresh makes offline/online accurate; failures are reported per peer
	// rather than failing the whole command.
	_ = d.reg.Refresh(ctx, d.ts, d.cfg.Peers, d.cfg.Port)

	names := d.cfg.PeerNames()
	out := make([]PeerReport, 0, len(names))
	for _, name := range names {
		peer := d.cfg.Peers[name]
		rep := PeerReport{Name: name, IsDefault: d.cfg.DefaultPeer == name}
		if peer != nil {
			rep.Addr = peer.Addr
			rep.Disabled = peer.Disabled
		}
		if st, ok := d.reg.State(name); ok {
			if rep.Addr == "" {
				rep.Addr = st.Addr
			}
			rep.Online = st.Online && !rep.Disabled
			rep.Resolved = st.Resolved
			rep.Reason = st.Reason
			rep.LatencyMS = int(st.Latency.Milliseconds())
			if !st.LastSeen.IsZero() {
				rep.LastSeen = st.LastSeen.Unix()
				rep.LastSeenS = tailscaleLastSeen(st.LastSeen)
			}
		}
		// An explicit address tells us nothing about reachability, so probe
		// it even when tailscale reported the peer as offline or unknown.
		shouldProbe := probe && !rep.Disabled && rep.Addr != "" &&
			(rep.Online || !rep.Resolved)
		if shouldProbe {
			latency, perr := protocol.MeasureLatency(ctx, rep.Addr, d.cfg.Username)
			if perr == nil {
				rep.Online = true
				rep.Reason = ""
				rep.LatencyMS = int(latency.Milliseconds())
				if rep.LastSeen == 0 {
					rep.LastSeen = time.Now().Unix()
					rep.LastSeenS = "just now"
				}
				d.reg.Set(name, rep.Addr, latency)
				_ = d.store.RecordPeerSeen(name, rep.LatencyMS)
			} else {
				rep.Online = false
				if protocol.IsOffline(perr) {
					rep.Reason = "offline"
				} else {
					rep.Reason = perr.Error()
				}
				d.reg.SetOffline(name, rep.Reason)
			}
		}
		if !rep.Online {
			if rep.Reason == "" {
				rep.Reason = "offline"
			}
			// A tailscale outage explains every peer at once; say so once.
			if tsErr := d.reg.TailscaleError(); tsErr != nil {
				rep.Reason = tsErr.Error()
			}
		}
		out = append(out, rep)
	}
	return out
}

// opStatus assembles the full status report.
func (d *Daemon) opStatus(ctx context.Context, req Request) *Response {
	cfg := d.cfg
	queuedMsgs, queuedXfers := d.queue.Counts()
	activeXfers, err := d.store.ActiveTransferCount()
	if err != nil {
		activeXfers = 0
	}

	tsReport := &TailscaleReport{}
	if local, err := d.ts.Local(ctx); err != nil {
		tsReport.Error = err.Error()
	} else {
		tsReport.OK = local.Online
		tsReport.State = local.State
		tsReport.IPs = local.IPs
		tsReport.Name = local.Name
	}

	notif := &NotificationReport{Command: d.notify.Command(), OK: d.notify.Available()}
	if !notif.OK {
		notif.Error = "no notification command found (tried: notify, notify-send)"
	}

	report := &StatusReport{
		User:         cfg.Username,
		ListenerAddr: d.ListenAddr(),
		ListenerUp:   true,
		ListenerPID:  pid(),
		Port:         cfg.Port,
		StartedAt:    d.started.Unix(),
		Version:      protocol.Version,
		Peers:        d.peerReports(ctx, req.Probe),
		QueuedMsgs:   queuedMsgs,
		QueuedXfers:  queuedXfers,
		ActiveXfers:  activeXfers,
		Tailscale:    tsReport,
		Notification: notif,
		AutostartTip: !cfg.AutostartAck,
	}
	return &Response{OK: true, Status: report}
}

// opConnect verifies reachability and warms the peer.
func (d *Daemon) opConnect(ctx context.Context, req Request) *Response {
	name, addr, err := d.resolveTarget(ctx, req)
	if err != nil {
		return errorResponse(err)
	}
	d.emit(Event{Kind: "state", Text: "connecting to " + name + "..."})

	latency, err := protocol.MeasureLatency(ctx, addr, d.cfg.Username)
	if err != nil {
		d.reg.SetOffline(name, "unreachable")
		code, offline := classify(err)
		return &Response{
			OK: false, Code: code, Error: err.Error(), Offline: offline,
			State: "offline", Peer: name, Addr: addr,
		}
	}
	d.reg.Set(name, addr, latency)
	_ = d.store.RecordPeerSeen(name, int(latency.Milliseconds()))
	// A peer that just answered may have queued work for us.
	d.queue.ResetPeer(name)
	return &Response{
		OK: true, State: "connected", Peer: name, Addr: addr,
		LatencyMS: int(latency.Milliseconds()),
	}
}

// opDisconnect drops a peer's state, optionally disabling it.
func (d *Daemon) opDisconnect(ctx context.Context, req Request) *Response {
	name := req.Peer
	if name == "" {
		return fail(CodeInvalid, "a peer name is required")
	}
	peer, ok := d.cfg.Peer(name)
	if !ok {
		return fail(CodeUnknownPeer, "%w: %s", peerconn.ErrUnknownPeer, name)
	}
	// Sessions are short-lived, so "disconnecting" means: forget cached
	// reachability and stop queueing new work for it.
	d.reg.Forget(name)
	if req.KeepEnabled {
		return &Response{OK: true, State: "disconnected", Peer: name}
	}
	peer.Disabled = true
	return &Response{OK: true, State: "disabled", Peer: name}
}

// opQueue reports outbox contents.
func (d *Daemon) opQueue(ctx context.Context) *Response {
	messages, transfers := d.queue.Counts()
	entries := make([]HistoryEntry, 0)
	for _, e := range d.queue.Pending() {
		if m, ok := e.Payload(); ok {
			entries = append(entries, HistoryEntry{
				ID: m.ID, Peer: e.Peer, Sender: m.Sender, Content: m.Content,
				Direction: "out", Time: m.Created.Unix(), Status: "queued",
			})
			continue
		}
		if t, ok := e.TransferPayload(); ok {
			entries = append(entries, HistoryEntry{
				ID: t.ID, Peer: e.Peer, Sender: d.cfg.Username,
				Content: joinQueueFiles(t.Files), Direction: "out",
				Time: t.Created.Unix(), Status: "queued",
			})
		}
	}
	return &Response{
		OK: true, History: entries, Status: &StatusReport{
			QueuedMsgs: messages, QueuedXfers: transfers,
		},
	}
}

// joinQueueFiles renders a queued transfer's file list.
func joinQueueFiles(files []queue.File) string {
	names := make([]string, 0, len(files))
	for _, f := range files {
		names = append(names, f.Name)
	}
	return "transfer: " + strings.Join(names, ", ")
}

// opFlush forces an immediate outbox drain.
func (d *Daemon) opFlush(ctx context.Context) *Response {
	d.wake()
	return &Response{OK: true, State: "flushing"}
}

// retryLoop drains the outbox on a timer and on demand.
func (d *Daemon) retryLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.drain(ctx)
		case <-d.wakeCh():
			d.drain(ctx)
		}
	}
}

// wakeCh returns the channel signalled by opFlush.
func (d *Daemon) wakeCh() <-chan struct{} {
	d.wakeOnce()
	return d.wakeSignal
}

// drain attempts every due outbox entry once.
func (d *Daemon) drain(ctx context.Context) {
	if d.queue.Len() == 0 {
		return
	}
	d.log.Debugf("draining %d queued entr(ies)", d.queue.Len())

	// Loop because one drain pass may unblock or create further work.
	for pass := 0; pass < 16; pass++ {
		entry, ok := d.queue.Peek("")
		if !ok {
			return
		}
		if !d.attempt(ctx, entry) {
			// The entry was rescheduled; stop so a failing peer does not spin.
			return
		}
	}
}

// attempt delivers one entry. It returns true when the entry is gone.
func (d *Daemon) attempt(ctx context.Context, e *queue.Entry) bool {
	if d.queue.Exhausted(e.ID) {
		reason := fmt.Errorf("giving up after %d attempts: %s", e.Attempts, e.LastError)
		d.log.Warnf("queued entry %s failed permanently: %v", e.ID, reason)
		_ = d.queue.MarkFailed(e.ID, reason)
		_ = d.store.SetStatus(e.ID, history.StatusFailed)
		_ = d.store.UpdateTransfer(e.ID, history.StatusFailed, 0, reason.Error())
		return true
	}

	// A command that created this entry is already delivering it inline. Leave
	// it to that attempt: sending it here as well would deliver it twice.
	if !d.work.begin(e.ID) {
		d.log.Debugf("queued entry %s is already in flight; skipping this pass", e.ID)
		return false
	}
	defer d.work.end(e.ID)

	addr := e.Addr
	if addr == "" {
		_, resolved, err := d.resolveTarget(ctx, Request{Peer: e.Peer})
		if err != nil {
			return d.deferEntry(e, err)
		}
		addr = resolved
	}

	switch {
	case e.Message != nil:
		if _, err := deliverMessage(ctx, d, e.Peer, addr, e.Message); err != nil {
			return d.deferEntry(e, err)
		}
		d.log.Debugf("flushed queued message %s to %s", e.ID, e.Peer)
		d.reg.Set(e.Peer, addr, 0)
		_ = d.store.RecordPeerSeen(e.Peer, 0)
		return true

	case e.Transfer != nil:
		sources, err := queueToSources(e.Transfer)
		if err != nil {
			// A vanished source file cannot succeed on retry.
			d.log.Warnf("queued transfer %s cannot proceed: %v", e.ID, err)
			_ = d.queue.MarkFailed(e.ID, err)
			_ = d.store.UpdateTransfer(e.ID, history.StatusFailed, 0, err.Error())
			return true
		}
		results, err := d.runTransfer(ctx, e.Peer, addr, e.Transfer, sources, false)
		if err != nil {
			return d.deferEntry(e, err)
		}
		var sent int64
		for _, r := range results {
			if r.Err != nil {
				d.log.Warnf("queued transfer %s: %s failed: %v", e.ID, r.Source.Name, r.Err)
				_ = d.store.UpdateTransfer(e.ID, history.StatusFailed, sent, r.Err.Error())
				return d.deferEntry(e, fmt.Errorf("%s: %w", r.Source.Name, r.Err))
			}
			sent += r.Result.Size
		}
		_ = d.queue.MarkDelivered(e.ID)
		_ = d.store.UpdateTransfer(e.ID, history.StatusDelivered, sent, "")
		d.log.Debugf("flushed queued transfer %s to %s", e.ID, e.Peer)
		d.reg.Set(e.Peer, addr, 0)
		_ = d.store.RecordPeerSeen(e.Peer, 0)
		return true
	}
	// Unknown kind: drop it rather than retry forever.
	_ = d.queue.MarkFailed(e.ID, errors.New("unknown queue entry kind"))
	return true
}

// deferEntry records a failed attempt and applies backoff.
func (d *Daemon) deferEntry(e *queue.Entry, err error) bool {
	if e.Peer != "" {
		d.reg.SetOffline(e.Peer, shortReason(err))
	}
	_ = d.queue.MarkAttempt(e.ID, err)
	d.log.Debugf("deferring %s (attempt %d): %v", e.ID, e.Attempts+1, err)
	return false
}

// queueToSources rebuilds transfer sources from a queued job, re-verifying
// each file still exists with the recorded size.
func queueToSources(t *queue.Transfer) ([]transfer.Source, error) {
	out := make([]transfer.Source, 0, len(t.Files))
	for _, f := range t.Files {
		src, err := transfer.Stat(f.Path)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", f.Name, err)
		}
		if src.Size != f.Size {
			return nil, fmt.Errorf("%s changed size since it was queued (%d -> %d bytes)", f.Name, f.Size, src.Size)
		}
		src.SHA256 = f.SHA256
		out = append(out, src)
	}
	return out, nil
}

// shortReason trims an error to a status-friendly phrase.
func shortReason(err error) string {
	if err == nil {
		return ""
	}
	if protocol.IsOffline(err) {
		return "offline"
	}
	msg := err.Error()
	if len(msg) > 80 {
		msg = msg[:80]
	}
	return msg
}

// wake signals the retry loop to drain immediately.
func (d *Daemon) wake() {
	d.wakeOnce()
	select {
	case d.wakeSignal <- struct{}{}:
	default:
	}
}

// wakeOnce lazily initialises the wake channel.
func (d *Daemon) wakeOnce() {
	d.wakeMu.Lock()
	defer d.wakeMu.Unlock()
	if d.wakeSignal == nil {
		d.wakeSignal = make(chan struct{}, 1)
	}
}

// tailscaleLastSeen renders a relative last-seen string.
func tailscaleLastSeen(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	d := time.Since(t)
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
