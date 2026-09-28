// Package queue is the durable outbox for messages and transfers.
//
// Nothing is ever "in flight" in memory only. A message or transfer job is
// written to disk and fsynced before any network attempt, so a crash, a
// reboot, or a Tailscale outage cannot lose work. The retry worker drains the
// outbox with exponential backoff, and an acknowledgement is what removes an
// entry.
package queue

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Kind distinguishes queued entries.
type Kind string

// Entry kinds.
const (
	KindMessage  Kind = "message"
	KindTransfer Kind = "transfer"
)

// Status values for a queued entry.
type Status string

// Queued entry statuses.
const (
	StatusPending Status = "pending"
	StatusActive  Status = "active"
	StatusFailed  Status = "failed"
)

// Backoff bounds for retry scheduling.
const (
	// MinBackoff is the first retry delay after a failed attempt.
	MinBackoff = 5 * time.Second
	// MaxBackoff caps the retry delay.
	MaxBackoff = 5 * time.Minute
	// MaxAttempts before an entry is marked failed and left for the user.
	MaxAttempts = 12
)

// Errors returned by the queue.
var (
	// ErrNotFound means no queued entry with that ID.
	ErrNotFound = errors.New("queued entry not found")
	// ErrCorrupt means a queue file could not be parsed.
	ErrCorrupt = errors.New("corrupt queue entry")
	// ErrFileMissing means a transfer's source file vanished.
	ErrFileMissing = errors.New("transfer source file is missing")
)

// Message is the payload of a queued message.
type Message struct {
	ID      string    `json:"id"`
	Sender  string    `json:"sender"`
	Content string    `json:"content"`
	Created time.Time `json:"created"`
}

// File names one file inside a queued transfer.
type File struct {
	// Path is the absolute local source path.
	Path string `json:"path"`
	// Name is the basename announced to the receiver.
	Name string `json:"name"`
	Size int64  `json:"size"`
	// SHA256 is the hex digest computed when the job was queued.
	SHA256 string `json:"sha256"`
}

// Transfer is the payload of a queued transfer.
type Transfer struct {
	ID      string    `json:"id"`
	Peer    string    `json:"peer"`
	Files   []File    `json:"files"`
	Created time.Time `json:"created"`
}

// TotalBytes sums the transfer's file sizes.
func (t *Transfer) TotalBytes() int64 {
	var n int64
	for _, f := range t.Files {
		n += f.Size
	}
	return n
}

// Entry is one item in the outbox.
type Entry struct {
	Kind Kind `json:"kind"`
	// Peer is the destination name, empty when the message targets the
	// default peer and the target is resolved at delivery time.
	Peer string `json:"peer,omitempty"`
	// Addr is the resolved peer address recorded at queue time, if known.
	Addr string `json:"addr,omitempty"`
	// Status tracks the entry's lifecycle.
	Status Status `json:"status"`
	// Attempts counts delivery attempts so far.
	Attempts int `json:"attempts"`
	// NextAt is the earliest time the retry worker may attempt this entry.
	NextAt time.Time `json:"next_at"`
	// LastError records the most recent failure for diagnostics.
	LastError string `json:"last_error,omitempty"`

	Message  *Message  `json:"message,omitempty"`
	Transfer *Transfer `json:"transfer,omitempty"`

	// ID is the message or transfer ID, copied out of the payload.
	ID string `json:"id"`
	// Created is when the entry was queued.
	Created time.Time `json:"created"`
}

// Payload returns the message payload, if this is a message entry.
func (e *Entry) Payload() (*Message, bool) {
	if e.Message == nil {
		return nil, false
	}
	return e.Message, true
}

// TransferPayload returns the transfer payload, if this is a transfer entry.
func (e *Entry) TransferPayload() (*Transfer, bool) {
	if e.Transfer == nil {
		return nil, false
	}
	return e.Transfer, true
}

// Queue is a filesystem-backed outbox.
type Queue struct {
	dir string

	mu      sync.Mutex
	entries map[string]*Entry
}

// New opens (creating if needed) a queue directory.
func New(dir string) (*Queue, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("cannot create queue directory: %w", err)
	}
	q := &Queue{dir: dir, entries: map[string]*Entry{}}
	if err := q.load(); err != nil {
		return nil, err
	}
	return q, nil
}

// Dir returns the queue's directory.
func (q *Queue) Dir() string { return q.dir }

// load reads every entry file from disk. Entries that fail to parse are moved
// aside rather than dropped, so nothing is silently lost.
func (q *Queue) load() error {
	items, err := os.ReadDir(q.dir)
	if err != nil {
		return fmt.Errorf("cannot read queue directory: %w", err)
	}
	for _, item := range items {
		if item.IsDir() || !strings.HasSuffix(item.Name(), ".json") {
			continue
		}
		path := filepath.Join(q.dir, item.Name())
		e, err := readEntry(path)
		if err != nil {
			quarantine(path)
			continue
		}
		if e.ID == "" {
			quarantine(path)
			continue
		}
		q.entries[e.ID] = e
	}
	return nil
}

// quarantine renames an unreadable entry so it stops being retried.
func quarantine(path string) {
	_ = os.Rename(path, path+".corrupt")
}

// readEntry parses one entry file.
func readEntry(path string) (*Entry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var e Entry
	if err := json.Unmarshal(data, &e); err != nil {
		return nil, fmt.Errorf("%w: %s: %v", ErrCorrupt, filepath.Base(path), err)
	}
	return &e, nil
}

// pathFor returns the file backing an entry ID.
func (q *Queue) pathFor(id string) string {
	return filepath.Join(q.dir, id+".json")
}

// EnqueueMessage durably records a message for delivery.
func (q *Queue) EnqueueMessage(peer, addr string, m *Message) error {
	e := &Entry{
		Kind:    KindMessage,
		Peer:    peer,
		Addr:    addr,
		Message: m,
		ID:      m.ID,
		Created: m.Created,
		Status:  StatusPending,
		NextAt:  time.Now(),
	}
	return q.put(e)
}

// EnqueueTransfer durably records a transfer job.
//
// The job keeps a stable ID across retries, which is what makes resume
// possible: the receiver keys its partial file on the transfer ID and reports
// an offset, and the sender seeks to it.
func (q *Queue) EnqueueTransfer(t *Transfer) error {
	e := &Entry{
		Kind:     KindTransfer,
		Peer:     t.Peer,
		Transfer: t,
		ID:       t.ID,
		Created:  t.Created,
		Status:   StatusPending,
		NextAt:   time.Now(),
	}
	return q.put(e)
}

// put writes an entry atomically and fsyncs it before returning, so a
// successfully queued message survives a crash.
func (q *Queue) put(e *Entry) error {
	if e.ID == "" {
		return errors.New("queued entry needs an id")
	}
	if e.Created.IsZero() {
		e.Created = time.Now()
	}
	if err := writeFileSync(q.pathFor(e.ID), e); err != nil {
		return err
	}
	q.mu.Lock()
	q.entries[e.ID] = e
	q.mu.Unlock()
	return nil
}

// writeFileSync writes JSON atomically, fsyncing both file and directory.
func writeFileSync(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("cannot encode queue entry: %w", err)
	}
	data = append(data, '\n')

	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".queue-*.json")
	if err != nil {
		return fmt.Errorf("cannot write queue entry: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return fmt.Errorf("cannot set queue entry permissions: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("cannot write queue entry: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("cannot flush queue entry: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("cannot close queue entry: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("cannot install queue entry: %w", err)
	}
	// fsync the directory so the rename is durable.
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		d.Close()
	}
	return nil
}

// Remove deletes an acknowledged entry.
func (q *Queue) Remove(id string) error {
	q.mu.Lock()
	_, ok := q.entries[id]
	delete(q.entries, id)
	q.mu.Unlock()
	if !ok {
		return ErrNotFound
	}
	if err := os.Remove(q.pathFor(id)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("cannot remove queue entry: %w", err)
	}
	return nil
}

// Get returns a copy of one entry.
func (q *Queue) Get(id string) (*Entry, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	e, ok := q.entries[id]
	if !ok {
		return nil, false
	}
	return e.clone(), true
}

// clone returns a deep-enough copy that callers cannot mutate queue state.
func (e *Entry) clone() *Entry {
	cp := *e
	if e.Message != nil {
		m := *e.Message
		cp.Message = &m
	}
	if e.Transfer != nil {
		t := *e.Transfer
		t.Files = append([]File(nil), e.Transfer.Files...)
		cp.Transfer = &t
	}
	return &cp
}

// Len returns the number of queued entries.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.entries)
}

// Counts returns queued message and transfer totals.
func (q *Queue) Counts() (messages, transfers int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, e := range q.entries {
		switch e.Kind {
		case KindMessage:
			messages++
		case KindTransfer:
			transfers++
		}
	}
	return
}

// Peek returns the next entry that is due, oldest first. peer may be empty to
// consider every peer.
func (q *Queue) Peek(peer string) (*Entry, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := time.Now()
	var best *Entry
	for _, e := range q.entries {
		if peer != "" && e.Peer != peer {
			continue
		}
		if e.NextAt.After(now) {
			continue
		}
		if best == nil || e.Created.Before(best.Created) {
			best = e
		}
	}
	if best == nil {
		return nil, false
	}
	return best.clone(), true
}

// Pending reports the entries awaiting delivery, oldest first.
func (q *Queue) Pending() []*Entry {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]*Entry, 0, len(q.entries))
	for _, e := range q.entries {
		out = append(out, e.clone())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Created.Before(out[j].Created) })
	return out
}

// MarkAttempt records a failed attempt and schedules the next retry using
// exponential backoff capped at MaxBackoff.
func (q *Queue) MarkAttempt(id string, cause error) error {
	q.mu.Lock()
	e, ok := q.entries[id]
	if !ok {
		q.mu.Unlock()
		return ErrNotFound
	}
	cp := e.clone()
	q.mu.Unlock()

	cp.Attempts++
	cp.Status = StatusPending
	if cause != nil {
		cp.LastError = cause.Error()
	}
	cp.NextAt = time.Now().Add(Backoff(cp.Attempts))

	if err := writeFileSync(q.pathFor(id), cp); err != nil {
		return err
	}
	q.mu.Lock()
	q.entries[id] = cp
	q.mu.Unlock()
	return nil
}

// MarkDelivered removes an entry after acknowledgement.
func (q *Queue) MarkDelivered(id string) error { return q.Remove(id) }

// MarkFailed records a terminal failure and removes the entry, so a hopeless
// job cannot block the outbox forever. The failure stays visible in history.
func (q *Queue) MarkFailed(id string, cause error) error {
	if err := q.UpdateStatus(id, string(StatusFailed), cause); err != nil {
		return err
	}
	return q.Remove(id)
}

// UpdateStatus rewrites an entry's status and last error.
func (q *Queue) UpdateStatus(id, status string, cause error) error {
	q.mu.Lock()
	e, ok := q.entries[id]
	if !ok {
		q.mu.Unlock()
		return ErrNotFound
	}
	cp := e.clone()
	q.mu.Unlock()

	cp.Status = Status(status)
	if cause != nil {
		cp.LastError = cause.Error()
	}
	if err := writeFileSync(q.pathFor(id), cp); err != nil {
		return err
	}
	q.mu.Lock()
	q.entries[id] = cp
	q.mu.Unlock()
	return nil
}

// Reset makes an entry due immediately, used when a peer comes back online.
func (q *Queue) Reset(id string) error {
	q.mu.Lock()
	e, ok := q.entries[id]
	if !ok {
		q.mu.Unlock()
		return ErrNotFound
	}
	cp := e.clone()
	q.mu.Unlock()

	cp.Attempts = 0
	cp.NextAt = time.Now()
	if err := writeFileSync(q.pathFor(id), cp); err != nil {
		return err
	}
	q.mu.Lock()
	q.entries[id] = cp
	q.mu.Unlock()
	return nil
}

// ResetPeer makes every entry for a peer due now.
func (q *Queue) ResetPeer(peer string) int {
	q.mu.Lock()
	var ids []string
	for id, e := range q.entries {
		if e.Peer == peer {
			ids = append(ids, id)
		}
	}
	q.mu.Unlock()
	n := 0
	for _, id := range ids {
		if err := q.Reset(id); err == nil {
			n++
		}
	}
	return n
}

// Exhausted reports whether an entry has burnt through its retries.
func (q *Queue) Exhausted(id string) bool {
	e, ok := q.Get(id)
	return ok && e.Attempts >= MaxAttempts
}

// Backoff returns the delay before attempt n (1-based), doubling up to
// MaxBackoff.
func Backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := MinBackoff
	for i := 1; i < attempt; i++ {
		d *= 2
		if d >= MaxBackoff {
			return MaxBackoff
		}
	}
	if d > MaxBackoff {
		return MaxBackoff
	}
	return d
}

// NextDue returns when the earliest entry becomes due, for ticker sizing.
func (q *Queue) NextDue() (time.Time, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var next time.Time
	for _, e := range q.entries {
		if next.IsZero() || e.NextAt.Before(next) {
			next = e.NextAt
		}
	}
	if next.IsZero() {
		return time.Time{}, false
	}
	return next, true
}
