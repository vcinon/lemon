// Package protocol defines Lemon's version 1 wire format: newline-delimited
// JSON control frames followed by raw bytes for file payloads.
//
// The design is intentionally small and inspectable. One TCP session carries
// one request and one response, so reconnect logic stays trivial.
package protocol

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"
)

// Wire limits. Every bound here is enforced before memory is committed.
const (
	// Version is the protocol version implemented by this package.
	Version = 1

	// MaxFrameBytes caps a single control frame (newline-delimited JSON).
	MaxFrameBytes = 1 << 20 // 1 MiB

	// MaxContentBytes caps message content.
	MaxContentBytes = 8 << 10 // 8 KiB

	// MaxIDBytes caps an identifier such as a message or transfer ID.
	MaxIDBytes = 64

	// MaxNameBytes caps a username or peer name.
	MaxNameBytes = 32

	// MaxFilenameBytes caps a single transferred filename.
	MaxFilenameBytes = 255

	// MaxFilesPerTransfer caps how many files one transfer may carry.
	MaxFilesPerTransfer = 64

	// MaxRemoteSizeBytes caps a single announced file size (16 GiB).
	MaxRemoteSizeBytes = 16 << 30
)

// Default timeouts for control frames and full sessions.
const (
	DefaultFrameTimeout = 10 * time.Second
	DefaultDialTimeout  = 5 * time.Second
	// TransferIdleTimeout bounds a stalled transfer stream.
	TransferIdleTimeout = 60 * time.Second
)

// Frame types.
const (
	TypeHello         = "hello"
	TypeError         = "error"
	TypeMessage       = "message"
	TypeAck           = "ack"
	TypeTransferBegin = "transfer_begin"
	TypeTransferReady = "transfer_ready"
	TypeFileEnd       = "file_end"
	TypeTransferDone  = "transfer_done"
	TypeTransferOK    = "transfer_ok"
	TypePing          = "ping"
	TypePong          = "pong"
	TypeBye           = "bye"
)

// KnownTypes lists every frame type this implementation accepts.
var KnownTypes = map[string]bool{
	TypeHello: true, TypeError: true, TypeMessage: true, TypeAck: true,
	TypeTransferBegin: true, TypeTransferReady: true, TypeFileEnd: true,
	TypeTransferDone: true, TypeTransferOK: true, TypePing: true,
	TypePong: true, TypeBye: true,
}

// Protocol errors. Callers map these onto user-facing messages.
var (
	// ErrFrameTooLarge means a control frame exceeded MaxFrameBytes.
	ErrFrameTooLarge = errors.New("control frame too large")
	// ErrUnknownType means an unrecognised frame type was received.
	ErrUnknownType = errors.New("unknown frame type")
	// ErrBadFrame means a frame failed structural or field validation.
	ErrBadFrame = errors.New("malformed frame")
	// ErrVersionMismatch means the peer speaks a different protocol version.
	ErrVersionMismatch = errors.New("protocol version mismatch")
	// ErrClosed means the peer hung up.
	ErrClosed = errors.New("connection closed by peer")
)

// Hello introduces a connection. Kind distinguishes the two roles.
type Hello struct {
	Type    string `json:"type"`
	Version int    `json:"version"`
	User    string `json:"user"`
	// Kind is "client" or "server".
	Kind string `json:"kind,omitempty"`
}

// Error is a protocol-level failure with a human-readable reason.
type Error struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`
	// Code is a stable short slug, e.g. "offline" or "busy".
	Code string `json:"code,omitempty"`
}

func (e *Error) Error() string {
	if e.Code != "" {
		return e.Reason
	}
	return e.Reason
}

// Message is a chat message.
type Message struct {
	Type      string `json:"type"`
	ID        string `json:"id"`
	Sender    string `json:"sender"`
	Timestamp string `json:"timestamp"`
	Content   string `json:"content"`
}

// Ack confirms delivery of the message named by ID. Delivered is only ever
// true once the receiver has persisted the message.
type Ack struct {
	Type string `json:"type"`
	ID   string `json:"id"`
	// Status is "delivered" or "duplicate".
	Status string `json:"status"`
}

// Ack statuses.
const (
	AckDelivered = "delivered"
	AckDuplicate = "duplicate"
)

// FileSpec describes one file inside a transfer.
type FileSpec struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
	// SHA256 is the lowercase hex digest, known in advance when the sender
	// computed it; empty means the receiver must not expect a digest.
	SHA256 string `json:"sha256,omitempty"`
}

// TransferBegin opens a transfer session.
type TransferBegin struct {
	Type   string     `json:"type"`
	ID     string     `json:"id"`
	Sender string     `json:"sender"`
	Files  []FileSpec `json:"files"`
	// Resume asks the receiver to report offsets for any partial data it holds.
	Resume bool `json:"resume,omitempty"`
}

// FileSlot is the receiver's per-file decision.
type FileSlot struct {
	// Name is the sanitised final filename on disk.
	Name string `json:"name"`
	// Offset is how many bytes the receiver already has; the sender seeks here.
	Offset int64 `json:"offset"`
	// Path is informational, shown in notifications.
	Path string `json:"path,omitempty"`
}

// TransferReady answers TransferBegin with per-file offsets.
type TransferReady struct {
	Type   string     `json:"type"`
	ID     string     `json:"id"`
	Files  []FileSlot `json:"files"`
	Reject []string   `json:"reject,omitempty"`
}

// FileEnd terminates one file's byte stream.
type FileEnd struct {
	Type string `json:"type"`
	ID   string `json:"id"`
	Name string `json:"name"`
	Size int64  `json:"size"`
	// SHA256 is the hex digest of the bytes actually received.
	SHA256 string `json:"sha256"`
	// Path is the receiver's final location, when successful.
	Path string `json:"path,omitempty"`
}

// TransferDone signals the sender finished streaming all files.
type TransferDone struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

// TransferOK is the receiver's verdict on a completed transfer.
type TransferOK struct {
	Type string `json:"type"`
	ID   string `json:"id"`
	// Saved lists receiver-side filenames.
	Saved []string `json:"saved,omitempty"`
	// Failed maps filename to a short reason.
	Failed map[string]string `json:"failed,omitempty"`
}

// Ping measures round-trip latency.
type Ping struct {
	Type string `json:"type"`
	TS   int64  `json:"ts,omitempty"`
}

// Pong answers a Ping.
type Pong struct {
	Type string `json:"type"`
	TS   int64  `json:"ts"`
}

// Bye is an orderly disconnect signal.
type Bye struct {
	Type string `json:"type"`
}

// Frame is the decoded header of a control message. Payload carries the
// type-specific struct.
type Frame struct {
	Type string
	Raw  []byte
}

// NewID returns a random lowercase hex identifier of n bytes.
func NewID(n int) (string, error) {
	if n <= 0 || n > 32 {
		return "", fmt.Errorf("invalid id length %d", n)
	}
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("cannot generate id: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// MessageID returns a fresh 12-hex-char message identifier.
func MessageID() (string, error) { return NewID(6) }

// TransferID returns a fresh 16-hex-char transfer identifier.
func TransferID() (string, error) { return NewID(8) }

// FormatTime renders t in the protocol's UTC RFC3339 form.
func FormatTime(t time.Time) string { return t.UTC().Format(time.RFC3339) }

// ValidID reports whether s is an acceptable identifier.
func ValidID(s string) bool {
	if s == "" || len(s) > MaxIDBytes {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || c == '-' {
			continue
		}
		return false
	}
	return true
}

// ValidUser reports whether s is an acceptable protocol username.
func ValidUser(s string) bool {
	if s == "" || len(s) > MaxNameBytes {
		return false
	}
	if strings.ContainsAny(s, "/\\\x00\n\r\t ") {
		return false
	}
	return utf8.ValidString(s)
}

// NewMessage builds a validated message frame.
func NewMessage(id, sender, content string, ts time.Time) (*Message, error) {
	if err := ValidateIDField(id, "message id"); err != nil {
		return nil, err
	}
	if err := ValidateUserField(sender); err != nil {
		return nil, err
	}
	if err := ValidateContent(content); err != nil {
		return nil, err
	}
	return &Message{
		Type:      TypeMessage,
		ID:        id,
		Sender:    sender,
		Timestamp: FormatTime(ts),
		Content:   content,
	}, nil
}

// ValidateIDField checks an identifier for protocol use.
func ValidateIDField(id, field string) error {
	if id == "" {
		return fmt.Errorf("%w: %s is empty", ErrBadFrame, field)
	}
	if len(id) > MaxIDBytes {
		return fmt.Errorf("%w: %s exceeds %d bytes", ErrBadFrame, field, MaxIDBytes)
	}
	return nil
}

// ValidateUserField checks a sender name for protocol use.
func ValidateUserField(user string) error {
	if user == "" {
		return fmt.Errorf("%w: sender is empty", ErrBadFrame)
	}
	if len(user) > MaxNameBytes {
		return fmt.Errorf("%w: sender name exceeds %d bytes", ErrBadFrame, MaxNameBytes)
	}
	if strings.ContainsAny(user, "/\\\x00\n\r\t ") {
		return fmt.Errorf("%w: sender name contains illegal characters", ErrBadFrame)
	}
	if !utf8.ValidString(user) {
		return fmt.Errorf("%w: sender name is not valid UTF-8", ErrBadFrame)
	}
	return nil
}

// ValidateContent bounds message text and rejects NUL bytes.
func ValidateContent(content string) error {
	if len(content) > MaxContentBytes {
		return fmt.Errorf("message is too long (%d bytes, max %d)", len(content), MaxContentBytes)
	}
	if strings.ContainsRune(content, 0) {
		return fmt.Errorf("%w: message contains a NUL byte", ErrBadFrame)
	}
	if !utf8.ValidString(content) {
		return fmt.Errorf("%w: message is not valid UTF-8", ErrBadFrame)
	}
	return nil
}

// ValidateFileName checks a remote filename's length and encoding. It does not
// perform path sanitisation; see package transfer.
func ValidateFileName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: filename is empty", ErrBadFrame)
	}
	if len(name) > MaxFilenameBytes {
		return fmt.Errorf("%w: filename exceeds %d bytes", ErrBadFrame, MaxFilenameBytes)
	}
	if strings.ContainsAny(name, "/\\\x00") {
		return fmt.Errorf("%w: filename contains a path separator or NUL", ErrBadFrame)
	}
	if !utf8.ValidString(name) {
		return fmt.Errorf("%w: filename is not valid UTF-8", ErrBadFrame)
	}
	return nil
}

// ValidateSize bounds an announced size.
func ValidateSize(size int64) error {
	if size < 0 {
		return fmt.Errorf("%w: negative file size", ErrBadFrame)
	}
	if size > MaxRemoteSizeBytes {
		return fmt.Errorf("%w: file exceeds the %d byte limit", ErrBadFrame, MaxRemoteSizeBytes)
	}
	return nil
}

// WriteFrame encodes v as a single newline-terminated JSON frame.
func WriteFrame(w io.Writer, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("cannot encode frame: %w", err)
	}
	if len(data)+1 > MaxFrameBytes {
		return fmt.Errorf("%w: %d bytes", ErrFrameTooLarge, len(data))
	}
	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("cannot write frame: %w", err)
	}
	if _, err := w.Write([]byte{'\n'}); err != nil {
		return fmt.Errorf("cannot write frame terminator: %w", err)
	}
	return nil
}

// ReadFrame reads one newline-terminated JSON frame and returns its "type".
//
// The returned slice is only valid until the next call on r.
func ReadFrame(r *bufio.Reader) ([]byte, error) {
	var buf []byte
	for {
		chunk, isPrefix, err := r.ReadLine()
		if err != nil {
			if err == io.EOF && len(buf) == 0 {
				return nil, ErrClosed
			}
			return nil, fmt.Errorf("cannot read frame: %w", err)
		}
		buf = append(buf, chunk...)
		if len(buf) > MaxFrameBytes {
			return nil, fmt.Errorf("%w: exceeded %d bytes", ErrFrameTooLarge, MaxFrameBytes)
		}
		if !isPrefix {
			break
		}
	}
	if len(strings.TrimSpace(string(buf))) == 0 {
		return nil, fmt.Errorf("%w: empty frame", ErrBadFrame)
	}
	var probe struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(buf, &probe); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadFrame, err)
	}
	if probe.Type == "" {
		return nil, fmt.Errorf("%w: frame has no type", ErrBadFrame)
	}
	if !KnownTypes[probe.Type] {
		return nil, fmt.Errorf("%w: %q", ErrUnknownType, probe.Type)
	}
	return buf, nil
}

// PeekType returns the "type" field of a raw frame without full decoding.
func PeekType(raw []byte) (string, error) {
	var probe struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return "", fmt.Errorf("%w: %v", ErrBadFrame, err)
	}
	return probe.Type, nil
}

// Decode unmarshals a raw frame into v and verifies the type matches want.
func Decode(raw []byte, v any, want string) error {
	got, err := PeekType(raw)
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("%w: expected %q, got %q", ErrBadFrame, want, got)
	}
	if err := json.Unmarshal(raw, v); err != nil {
		return fmt.Errorf("%w: %v", ErrBadFrame, err)
	}
	return nil
}

// DecodeHello validates and decodes a hello frame.
func DecodeHello(raw []byte) (*Hello, error) {
	var h Hello
	if err := Decode(raw, &h, TypeHello); err != nil {
		return nil, err
	}
	if h.Version != Version {
		return nil, fmt.Errorf("%w: peer speaks v%d, this build speaks v%d", ErrVersionMismatch, h.Version, Version)
	}
	if !ValidUser(h.User) {
		return nil, fmt.Errorf("%w: invalid username %q", ErrBadFrame, h.User)
	}
	return &h, nil
}

// ValidateMessage checks an inbound message for structural validity.
func ValidateMessage(m *Message) error {
	if m == nil {
		return fmt.Errorf("%w: missing message", ErrBadFrame)
	}
	if err := ValidateIDField(m.ID, "message id"); err != nil {
		return err
	}
	if err := ValidateUserField(m.Sender); err != nil {
		return err
	}
	if err := ValidateContent(m.Content); err != nil {
		return err
	}
	if m.Timestamp != "" {
		if _, err := time.Parse(time.RFC3339, m.Timestamp); err != nil {
			return fmt.Errorf("%w: bad timestamp %q", ErrBadFrame, m.Timestamp)
		}
	}
	return nil
}

// ValidateTransferBegin checks a transfer request.
func ValidateTransferBegin(t *TransferBegin) error {
	if t == nil {
		return fmt.Errorf("%w: missing transfer", ErrBadFrame)
	}
	if err := ValidateIDField(t.ID, "transfer id"); err != nil {
		return err
	}
	if err := ValidateUserField(t.Sender); err != nil {
		return err
	}
	if len(t.Files) == 0 {
		return fmt.Errorf("%w: transfer contains no files", ErrBadFrame)
	}
	if len(t.Files) > MaxFilesPerTransfer {
		return fmt.Errorf("%w: transfer carries %d files, max %d", ErrBadFrame, len(t.Files), MaxFilesPerTransfer)
	}
	seen := make(map[string]bool, len(t.Files))
	for i, f := range t.Files {
		if err := ValidateFileName(f.Name); err != nil {
			return err
		}
		if err := ValidateSize(f.Size); err != nil {
			return err
		}
		if seen[f.Name] {
			return fmt.Errorf("%w: duplicate filename %q in transfer", ErrBadFrame, f.Name)
		}
		seen[f.Name] = true
		if f.SHA256 != "" {
			if err := ValidateDigest(f.SHA256); err != nil {
				return fmt.Errorf("file %d: %w", i, err)
			}
		}
	}
	return nil
}

// ValidateFileEnd checks a completed-file report.
func ValidateFileEnd(f *FileEnd) error {
	if f == nil {
		return fmt.Errorf("%w: missing file_end", ErrBadFrame)
	}
	if err := ValidateIDField(f.ID, "transfer id"); err != nil {
		return err
	}
	if err := ValidateFileName(f.Name); err != nil {
		return err
	}
	if err := ValidateSize(f.Size); err != nil {
		return err
	}
	return ValidateDigest(f.SHA256)
}

// ValidateDigest checks a lowercase hex SHA-256 digest.
func ValidateDigest(d string) error {
	if len(d) != 64 {
		return fmt.Errorf("%w: digest must be 64 hex characters", ErrBadFrame)
	}
	for i := 0; i < len(d); i++ {
		c := d[i]
		if (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') {
			continue
		}
		return fmt.Errorf("%w: digest is not lowercase hex", ErrBadFrame)
	}
	return nil
}

// ValidateTransferReady checks the receiver's slot list.
func ValidateTransferReady(t *TransferReady) error {
	if t == nil {
		return fmt.Errorf("%w: missing transfer_ready", ErrBadFrame)
	}
	if err := ValidateIDField(t.ID, "transfer id"); err != nil {
		return err
	}
	if len(t.Files) == 0 && len(t.Reject) == 0 {
		return fmt.Errorf("%w: transfer_ready lists no files", ErrBadFrame)
	}
	if len(t.Files) > MaxFilesPerTransfer {
		return fmt.Errorf("%w: transfer_ready lists %d files, max %d", ErrBadFrame, len(t.Files), MaxFilesPerTransfer)
	}
	for _, f := range t.Files {
		if err := ValidateFileName(f.Name); err != nil {
			return err
		}
		if f.Offset < 0 {
			return fmt.Errorf("%w: negative offset", ErrBadFrame)
		}
		if err := ValidateSize(f.Offset); err != nil {
			return err
		}
	}
	return nil
}
