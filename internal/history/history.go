// Package history stores sent and received messages in SQLite.
//
// Every message carries a globally unique ID, and that ID is the table's
// primary key, so deduplication falls out of the schema: INSERT OR IGNORE
// reports zero rows affected for a message we have already stored, which is
// exactly the signal the receiver needs to avoid a second notification.
package history

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// Direction distinguishes outbound from inbound messages.
type Direction string

// Message directions.
const (
	In  Direction = "in"
	Out Direction = "out"
)

// Status values for a message or transfer record.
const (
	StatusQueued    = "queued"
	StatusSent      = "sent"
	StatusDelivered = "delivered"
	StatusFailed    = "failed"
	StatusReceived  = "received"
)

// ErrNotFound is returned when a record does not exist.
var ErrNotFound = errors.New("not found")

const schema = `
CREATE TABLE IF NOT EXISTS messages (
	id         TEXT PRIMARY KEY,
	peer       TEXT NOT NULL,
	peer_addr  TEXT NOT NULL DEFAULT '',
	direction  TEXT NOT NULL,
	sender     TEXT NOT NULL,
	content    TEXT NOT NULL,
	ts         INTEGER NOT NULL,
	status     TEXT NOT NULL,
	acked_at   INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS messages_ts  ON messages(ts);
CREATE INDEX IF NOT EXISTS messages_peer ON messages(peer);

CREATE TABLE IF NOT EXISTS transfers (
	id         TEXT PRIMARY KEY,
	peer       TEXT NOT NULL,
	peer_addr  TEXT NOT NULL DEFAULT '',
	kind       TEXT NOT NULL DEFAULT 'send',
	ts         INTEGER NOT NULL,
	status     TEXT NOT NULL,
	bytes      INTEGER NOT NULL DEFAULT 0,
	files      TEXT NOT NULL DEFAULT '',
	err        TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS transfers_ts   ON transfers(ts);
CREATE INDEX IF NOT EXISTS transfers_peer ON transfers(peer);

-- Remembers the last time we observed each peer, for the status command.
CREATE TABLE IF NOT EXISTS peer_state (
	peer        TEXT PRIMARY KEY,
	last_seen   INTEGER NOT NULL DEFAULT 0,
	last_latency_ms INTEGER NOT NULL DEFAULT 0
);
`

// Message is one row of the messages table.
type Message struct {
	ID        string
	Peer      string
	PeerAddr  string
	Direction Direction
	Sender    string
	Content   string
	Time      time.Time
	Status    string
	AckedAt   time.Time
}

// Transfer is one row of the transfers table.
type Transfer struct {
	ID       string
	Peer     string
	PeerAddr string
	Kind     string
	Time     time.Time
	Status   string
	Bytes    int64
	Files    string
	Err      string
}

// Store is a SQLite-backed message log.
type Store struct {
	db *sql.DB
}

// Open creates or opens the history database at path.
func Open(path string) (*Store, error) {
	dsn := fmt.Sprintf("file:%s?_busy_timeout=5000&_journal_mode=WAL&_foreign_keys=on&_synchronous=FULL", path)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("cannot open history database: %w", err)
	}
	// A single writer avoids SQLITE_BUSY under concurrency; reads still work.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("cannot open history database: %w", err)
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("cannot initialise history database: %w", err)
	}
	return &Store{db: db}, nil
}

// Close releases the database.
func (s *Store) Close() error { return s.db.Close() }

// DB exposes the handle for tests.
func (s *Store) DB() *sql.DB { return s.db }

// InsertMessage stores a message. It reports whether the row was new, which
// is false for a duplicate ID.
func (s *Store) InsertMessage(m Message) (bool, error) {
	if m.Time.IsZero() {
		m.Time = time.Now()
	}
	if m.Status == "" {
		m.Status = string(StatusReceived)
	}
	var acked int64
	if !m.AckedAt.IsZero() {
		acked = m.AckedAt.Unix()
	}
	res, err := s.db.Exec(
		`INSERT OR IGNORE INTO messages
		 (id, peer, peer_addr, direction, sender, content, ts, status, acked_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		m.ID, m.Peer, m.PeerAddr, string(m.Direction), m.Sender, m.Content,
		m.Time.Unix(), m.Status, acked,
	)
	if err != nil {
		return false, fmt.Errorf("cannot record message: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("cannot record message: %w", err)
	}
	return n > 0, nil
}

// SetStatus updates a message's delivery status.
func (s *Store) SetStatus(id, status string) error {
	acked := int64(0)
	if status == StatusDelivered {
		acked = time.Now().Unix()
	}
	res, err := s.db.Exec(`UPDATE messages SET status = ?, acked_at = ? WHERE id = ?`, status, acked, id)
	if err != nil {
		return fmt.Errorf("cannot update message status: %w", err)
	}
	return requireRow(res)
}

// Has reports whether a message ID is already stored.
func (s *Store) Has(id string) (bool, error) {
	var one int
	err := s.db.QueryRow(`SELECT 1 FROM messages WHERE id = ?`, id).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("cannot query history: %w", err)
	}
	return true, nil
}

// Message returns one message by ID.
func (s *Store) Message(id string) (Message, error) {
	row := s.db.QueryRow(
		`SELECT id, peer, peer_addr, direction, sender, content, ts, status, acked_at
		 FROM messages WHERE id = ?`, id)
	return scanMessage(row)
}

// Options filters a history listing.
type Options struct {
	// Peer restricts results to one conversation partner.
	Peer string
	// Limit caps the number of rows; 0 means no limit.
	Limit int
	// Since restricts results to messages at or after this time.
	Since time.Time
}

// List returns messages oldest-first, optionally filtered.
func (s *Store) List(opt Options) ([]Message, error) {
	query := `SELECT id, peer, peer_addr, direction, sender, content, ts, status, acked_at FROM messages`
	var where []string
	var args []any
	if opt.Peer != "" {
		where = append(where, "peer = ?")
		args = append(args, opt.Peer)
	}
	if !opt.Since.IsZero() {
		where = append(where, "ts >= ?")
		args = append(args, opt.Since.Unix())
	}
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	// Newest-first for the LIMIT, then reversed by the caller-facing order.
	query += " ORDER BY ts DESC, id DESC"
	if opt.Limit > 0 {
		query += " LIMIT ?"
		args = append(args, opt.Limit)
	}

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("cannot read history: %w", err)
	}
	defer rows.Close()

	var out []Message
	for rows.Next() {
		m, err := scanMessage(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("cannot read history: %w", err)
	}
	// Present oldest-first, the way a conversation reads.
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out, nil
}

// scanMessage reads one row into a Message.
func scanMessage(row interface{ Scan(...any) error }) (Message, error) {
	var (
		m         Message
		dir       string
		ts, acked int64
	)
	err := row.Scan(&m.ID, &m.Peer, &m.PeerAddr, &dir, &m.Sender, &m.Content, &ts, &m.Status, &acked)
	if errors.Is(err, sql.ErrNoRows) {
		return Message{}, ErrNotFound
	}
	if err != nil {
		return Message{}, fmt.Errorf("cannot read history row: %w", err)
	}
	m.Direction = Direction(dir)
	m.Time = time.Unix(ts, 0)
	if acked > 0 {
		m.AckedAt = time.Unix(acked, 0)
	}
	return m, nil
}

// CountByStatus returns how many messages hold each status.
func (s *Store) CountByStatus() (map[string]int, error) {
	rows, err := s.db.Query(`SELECT status, count(*) FROM messages GROUP BY status`)
	if err != nil {
		return nil, fmt.Errorf("cannot count messages: %w", err)
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var status string
		var n int
		if err := rows.Scan(&status, &n); err != nil {
			return nil, fmt.Errorf("cannot count messages: %w", err)
		}
		out[status] = n
	}
	return out, rows.Err()
}

// QueuedCount returns the number of messages still awaiting delivery.
func (s *Store) QueuedCount() (int, error) {
	var n int
	err := s.db.QueryRow(`SELECT count(*) FROM messages WHERE status = ?`, StatusQueued).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("cannot count queued messages: %w", err)
	}
	return n, nil
}

// InsertTransfer records a transfer attempt.
func (s *Store) InsertTransfer(t Transfer) (bool, error) {
	if t.Time.IsZero() {
		t.Time = time.Now()
	}
	if t.Kind == "" {
		t.Kind = "send"
	}
	if t.Status == "" {
		t.Status = StatusQueued
	}
	res, err := s.db.Exec(
		`INSERT OR IGNORE INTO transfers (id, peer, peer_addr, kind, ts, status, bytes, files, err)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		t.ID, t.Peer, t.PeerAddr, t.Kind, t.Time.Unix(), t.Status, t.Bytes, t.Files, t.Err,
	)
	if err != nil {
		return false, fmt.Errorf("cannot record transfer: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("cannot record transfer: %w", err)
	}
	return n > 0, nil
}

// UpdateTransfer advances a transfer's status, byte count and error text.
func (s *Store) UpdateTransfer(id, status string, bytes int64, errText string) error {
	_, err := s.db.Exec(`UPDATE transfers SET status = ?, bytes = ?, err = ? WHERE id = ?`,
		status, bytes, errText, id)
	if err != nil {
		return fmt.Errorf("cannot update transfer: %w", err)
	}
	return nil
}

// ActiveTransferCount returns transfers that are neither delivered nor failed.
func (s *Store) ActiveTransferCount() (int, error) {
	var n int
	err := s.db.QueryRow(
		`SELECT count(*) FROM transfers WHERE status IN (?, ?)`, StatusQueued, StatusSent).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("cannot count transfers: %w", err)
	}
	return n, nil
}

// Transfers lists recent transfers, newest first.
func (s *Store) Transfers(limit int) ([]Transfer, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.Query(
		`SELECT id, peer, peer_addr, kind, ts, status, bytes, files, err
		 FROM transfers ORDER BY ts DESC, id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("cannot read transfers: %w", err)
	}
	defer rows.Close()
	var out []Transfer
	for rows.Next() {
		var t Transfer
		var ts int64
		if err := rows.Scan(&t.ID, &t.Peer, &t.PeerAddr, &t.Kind, &ts, &t.Status, &t.Bytes, &t.Files, &t.Err); err != nil {
			return nil, fmt.Errorf("cannot read transfers: %w", err)
		}
		t.Time = time.Unix(ts, 0)
		out = append(out, t)
	}
	return out, rows.Err()
}

// RecordPeerSeen stores the last observation of a peer.
func (s *Store) RecordPeerSeen(peer string, latencyMS int) error {
	_, err := s.db.Exec(
		`INSERT INTO peer_state (peer, last_seen, last_latency_ms) VALUES (?, ?, ?)
		 ON CONFLICT(peer) DO UPDATE SET
		   last_seen = max(last_seen, excluded.last_seen),
		   last_latency_ms = CASE WHEN excluded.last_latency_ms > 0
		                         THEN excluded.last_latency_ms ELSE last_latency_ms END`,
		peer, time.Now().Unix(), latencyMS)
	if err != nil {
		return fmt.Errorf("cannot record peer state: %w", err)
	}
	return nil
}

// PeerSeen returns when a peer was last observed and its latency.
func (s *Store) PeerSeen(peer string) (time.Time, int, error) {
	var ts, lat int64
	err := s.db.QueryRow(`SELECT last_seen, last_latency_ms FROM peer_state WHERE peer = ?`, peer).Scan(&ts, &lat)
	if errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, 0, nil
	}
	if err != nil {
		return time.Time{}, 0, fmt.Errorf("cannot read peer state: %w", err)
	}
	if ts == 0 {
		return time.Time{}, 0, nil
	}
	return time.Unix(ts, 0), int(lat), nil
}

// ForgetPeer removes a peer's last-seen record.
func (s *Store) ForgetPeer(peer string) error {
	_, err := s.db.Exec(`DELETE FROM peer_state WHERE peer = ?`, peer)
	return err
}

// requireRow converts a zero-row update into ErrNotFound.
func requireRow(res sql.Result) error {
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("cannot update row: %w", err)
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}
