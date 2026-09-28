package transfer

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"time"

	"github.com/siin/lemon/internal/protocol"
)

// ProgressFunc receives byte-count updates while a file streams. done is the
// file's full size, n is the number of bytes sent or received so far.
type ProgressFunc func(name string, n, done int64)

// ReceiveResult describes one file after a successful receive.
type ReceiveResult struct {
	// Name is the final on-disk filename.
	Name string
	// Path is the absolute destination path.
	Path string
	// Size is the file's byte length.
	Size int64
	// SHA256 is the verified digest.
	SHA256 string
	// Resumed is true when bytes from an earlier attempt were reused.
	Resumed bool
}

// FileOutcome is the per-file result of a receive.
type FileOutcome struct {
	Spec    protocol.FileSpec
	Slot    protocol.FileSlot
	Result  *ReceiveResult
	Err     error
	Skipped bool
}

// receiveFile streams one file from a session into the receive directory and
// commits it once its digest verifies.
//
// offset is what the receiver already holds. Because a resumed stream lacks
// the file's leading bytes, the existing prefix is re-read to rebuild the
// digest: verification stays honest instead of being skipped on resume.
func receiveFile(s *protocol.Session, transferID, receiveDir string, spec protocol.FileSpec, offset int64) (res *ReceiveResult, err error) {
	if err := protocol.ValidateFileName(spec.Name); err != nil {
		return nil, err
	}
	if err := protocol.ValidateSize(spec.Size); err != nil {
		return nil, err
	}
	if offset > spec.Size {
		offset = 0
	}

	stage, err := StageFile(receiveDir, spec.Name, transferID, offset)
	if err != nil {
		return nil, err
	}
	// Defence in depth: the committed path must remain inside the directory.
	inside, err := Contains(receiveDir, stage.Final)
	if err != nil || !inside {
		return nil, fmt.Errorf("%w: %q escapes the receive directory", ErrUnsafeName, spec.Name)
	}

	have, err := stage.CurrentSize()
	if err != nil {
		return nil, err
	}
	// Discard anything beyond the agreed offset, or a stale fragment.
	if have != offset {
		stage.Discard()
		offset = 0
	}

	digest := sha256.New()
	if offset > 0 {
		if err := hashPrefix(stage.Part, digest); err != nil {
			return nil, err
		}
	}

	out, err := stage.OpenPart()
	if err != nil {
		return nil, err
	}
	defer func() {
		out.Close()
		if err != nil {
			// Keep the part file: a later retry can resume from it.
			if errors.Is(err, errIntegrity) {
				stage.Discard()
			}
		}
	}()

	remaining := spec.Size - offset
	var written int64
	if remaining > 0 {
		// Tee the stream into the digest as it lands, so a partial prefix plus
		// the new bytes hash to the sender's digest exactly once.
		if _, err := s.ReadInto(io.MultiWriter(out, digest), remaining); err != nil {
			return nil, err
		}
		written = remaining
		// Flush so the bytes survive a crash mid-transfer.
		if err := out.Sync(); err != nil {
			return nil, fmt.Errorf("cannot flush %s: %w", stage.Part, err)
		}
	}
	total := offset + written

	sum := hex.EncodeToString(digest.Sum(nil))
	if spec.SHA256 != "" && !equalFold(sum, spec.SHA256) {
		return nil, fmt.Errorf("%w: %s digest mismatch (expected %s, got %s)",
			errIntegrity, spec.Name, spec.SHA256, sum)
	}

	if err := stage.Commit(); err != nil {
		return nil, err
	}
	return &ReceiveResult{
		Name:    stage.Name,
		Path:    stage.Final,
		Size:    total,
		SHA256:  sum,
		Resumed: offset > 0,
	}, nil
}

// errIntegrity marks a digest mismatch, which means the part file is useless.
var errIntegrity = errors.New("integrity check failed")

// hashPrefix feeds an existing partial file's bytes into the digest.
func hashPrefix(path string, h hash.Hash) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("cannot read %s: %w", path, err)
	}
	defer f.Close()
	if _, err := io.Copy(h, f); err != nil {
		return fmt.Errorf("cannot hash %s: %w", path, err)
	}
	return nil
}

// equalFold compares ASCII hex digests case-insensitively.
func equalFold(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]
		if ca >= 'A' && ca <= 'F' {
			ca += 'a' - 'A'
		}
		if cb >= 'A' && cb <= 'F' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}

// Receive runs a whole inbound transfer on a server session and returns the
// per-file outcomes. The caller is responsible for the handshake and for
// writing the transfer_done reply.
func Receive(s *protocol.Session, begin *protocol.TransferBegin, receiveDir string) ([]FileOutcome, error) {
	if err := protocol.ValidateTransferBegin(begin); err != nil {
		return nil, err
	}
	if err := EnsureDir(receiveDir); err != nil {
		return nil, err
	}

	// Advertise which bytes we already hold so the sender can seek.
	slots := make([]protocol.FileSlot, 0, len(begin.Files))
	offsets := make([]int64, len(begin.Files))
	for i, spec := range begin.Files {
		stage, err := StageFile(receiveDir, spec.Name, begin.ID, 0)
		if err != nil {
			return nil, err
		}
		offset := int64(0)
		if begin.Resume {
			// The size on disk is the only offset we can actually back up.
			if have, err := stage.CurrentSize(); err == nil && have <= spec.Size {
				offset = have
			}
		} else {
			// A fresh transfer must not inherit stale partial data.
			stage.Discard()
		}
		offsets[i] = offset
		slots = append(slots, protocol.FileSlot{Name: stage.Name, Offset: offset})
	}

	ready := &protocol.TransferReady{Type: protocol.TypeTransferReady, ID: begin.ID, Files: slots}
	if err := s.WriteFrame(ready); err != nil {
		return nil, err
	}

	outcomes := make([]FileOutcome, 0, len(begin.Files))
	for i, spec := range begin.Files {
		outcome := FileOutcome{Spec: spec, Slot: slots[i]}
		if offsets[i] == spec.Size {
			// Already complete from a previous attempt. The sender still owes us
			// a file_end, so answer it exactly like a fresh arrival.
			res, err := finalizeComplete(receiveDir, spec, begin.ID, offsets[i])
			if err != nil {
				outcome.Err = err
				_ = s.WriteFrame(&protocol.FileEnd{
					Type: protocol.TypeFileEnd, ID: begin.ID, Name: spec.Name,
					Size: 0, SHA256: zeroDigest,
				})
			} else {
				outcome.Result = res
				_ = s.WriteFrame(&protocol.FileEnd{
					Type: protocol.TypeFileEnd, ID: begin.ID, Name: res.Name,
					Size: res.Size, SHA256: res.SHA256, Path: res.Path,
				})
			}
		} else {
			res, err := receiveFile(s, begin.ID, receiveDir, spec, offsets[i])
			if err != nil {
				outcome.Err = err
				// Report per-file failure without aborting the whole transfer.
				_ = s.WriteFrame(&protocol.FileEnd{
					Type: protocol.TypeFileEnd, ID: begin.ID, Name: spec.Name,
					Size: 0, SHA256: zeroDigest,
				})
			} else {
				outcome.Result = res
				_ = s.WriteFrame(&protocol.FileEnd{
					Type: protocol.TypeFileEnd, ID: begin.ID, Name: res.Name,
					Size: res.Size, SHA256: res.SHA256, Path: res.Path,
				})
			}
		}
		outcomes = append(outcomes, outcome)
	}
	return outcomes, nil
}

// finalizeComplete commits a file that arrived entirely in an earlier attempt.
func finalizeComplete(receiveDir string, spec protocol.FileSpec, transferID string, size int64) (*ReceiveResult, error) {
	stage, err := StageFile(receiveDir, spec.Name, transferID, size)
	if err != nil {
		return nil, err
	}
	digest := sha256.New()
	if err := hashPrefix(stage.Part, digest); err != nil {
		return nil, err
	}
	sum := hex.EncodeToString(digest.Sum(nil))
	if spec.SHA256 != "" && !equalFold(sum, spec.SHA256) {
		stage.Discard()
		return nil, fmt.Errorf("%w: %s digest mismatch", errIntegrity, spec.Name)
	}
	if err := stage.Commit(); err != nil {
		return nil, err
	}
	return &ReceiveResult{Name: stage.Name, Path: stage.Final, Size: size, SHA256: sum, Resumed: true}, nil
}

// zeroDigest is the digest reported for a file that failed to receive. The
// sender treats a mismatch as a failure rather than a success.
const zeroDigest = "0000000000000000000000000000000000000000000000000000000000000000"

// SendResult describes one file after a successful send.
type SendResult struct {
	// Name is the receiver's final filename.
	Name string
	// Size is the number of bytes sent in this attempt.
	Size int64
	// Path is where the receiver saved it, when it reported one.
	Path string
	// Resumed is true when the receiver already held part of the file.
	Resumed bool
}

// SendJob is a transfer queued for delivery.
type SendJob struct {
	// ID is the transfer ID, stable across retries so resume works.
	ID string
	// Files are the sources to send.
	Files []Source
	// Progress, if set, receives byte updates.
	Progress ProgressFunc
	// Resume allows seeking to the receiver's reported offset.
	Resume bool
}

// SendFileResult pairs a source with its send outcome.
type SendFileResult struct {
	Source Source
	Result *SendResult
	Err    error
}

// Send performs a whole outbound transfer over an open client session.
func Send(c *protocol.Client, job SendJob) ([]SendFileResult, error) {
	specs := make([]protocol.FileSpec, 0, len(job.Files))
	for _, src := range job.Files {
		if err := protocol.ValidateFileName(src.Name); err != nil {
			return nil, err
		}
		if err := protocol.ValidateSize(src.Size); err != nil {
			return nil, err
		}
		if src.SHA256 != "" {
			if err := protocol.ValidateDigest(src.SHA256); err != nil {
				return nil, err
			}
		}
		specs = append(specs, protocol.FileSpec{
			Name: src.Name, Size: src.Size, SHA256: src.SHA256,
		})
	}
	begin := &protocol.TransferBegin{
		Type: protocol.TypeTransferBegin, ID: job.ID, Sender: c.User(),
		Files: specs, Resume: job.Resume,
	}
	ready, err := c.StartTransfer(begin)
	if err != nil {
		return nil, err
	}
	if len(ready.Reject) > 0 {
		return nil, fmt.Errorf("receiver rejected: %s", joinList(ready.Reject))
	}

	slotByName := make(map[string]protocol.FileSlot, len(ready.Files))
	for _, slot := range ready.Files {
		slotByName[slot.Name] = slot
	}

	results := make([]SendFileResult, 0, len(job.Files))
	for i, src := range job.Files {
		slot, ok := slotByName[specs[i].Name]
		if !ok {
			// The receiver renamed the file; match by position as a fallback
			// so progress output still lines up.
			if i < len(ready.Files) {
				slot = ready.Files[i]
			}
		}
		offset := slot.Offset
		if offset > src.Size {
			offset = 0
		}
		remain := src.Size - offset
		if remain < 0 {
			remain = 0
		}

		f, err := src.Open()
		if err != nil {
			results = append(results, SendFileResult{Source: src, Err: err})
			continue
		}
		if offset > 0 {
			if _, err := f.Seek(offset, io.SeekStart); err != nil {
				f.Close()
				results = append(results, SendFileResult{
					Source: src,
					Err:    fmt.Errorf("cannot resume %s: %w", src.Name, err),
				})
				continue
			}
		}

		name := src.Name
		if slot.Name != "" {
			name = slot.Name
		}
		report := func(n int64) {
			if job.Progress != nil {
				job.Progress(name, offset+n, src.Size)
			}
		}
		fe, err := c.WriteFile(job.ID, f, remain, report)
		f.Close()
		if err != nil {
			results = append(results, SendFileResult{Source: src, Err: err})
			// The connection is unusable after a mid-stream failure, so stop
			// here and let the caller retry the whole job.
			return results, fmt.Errorf("transfer interrupted: %w", err)
		}
		results = append(results, SendFileResult{
			Source: src,
			Result: &SendResult{
				Name:    fe.Name,
				Size:    fe.Size,
				Path:    fe.Path,
				Resumed: offset > 0,
			},
		})
	}

	ok, err := c.FinishTransfer(job.ID)
	if err != nil {
		return results, err
	}
	for name, reason := range ok.Failed {
		for i := range results {
			if results[i].Source.Name == name || (results[i].Result != nil && results[i].Result.Name == name) {
				results[i].Result = nil
				results[i].Err = errors.New(reason)
			}
		}
	}
	return results, nil
}

// joinList renders a list of names for an error message.
func joinList(items []string) string {
	if len(items) == 1 {
		return items[0]
	}
	out := ""
	for i, s := range items {
		if i > 0 {
			out += ", "
		}
		out += s
	}
	return out
}

// DigestFile hashes a local file, for precomputing checksums.
func DigestFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("cannot open %s: %w", path, err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("cannot hash %s: %w", path, err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// StatAll resolves a list of user-supplied paths into transfer sources,
// reporting every problem before any network work begins.
func StatAll(paths []string) ([]Source, error) {
	if len(paths) == 0 {
		return nil, errors.New("no files given")
	}
	if len(paths) > protocol.MaxFilesPerTransfer {
		return nil, fmt.Errorf("too many files: %d, max %d", len(paths), protocol.MaxFilesPerTransfer)
	}
	out := make([]Source, 0, len(paths))
	seen := map[string]bool{}
	for _, p := range paths {
		src, err := Stat(p)
		if err != nil {
			return nil, err
		}
		if seen[src.Name] {
			return nil, fmt.Errorf("duplicate filename in transfer: %s", src.Name)
		}
		seen[src.Name] = true
		out = append(out, src)
	}
	return out, nil
}

// TotalSize sums source sizes.
func TotalSize(sources []Source) int64 {
	var n int64
	for _, s := range sources {
		n += s.Size
	}
	return n
}

// FormatSize renders a byte count in human units.
func FormatSize(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit && exp < 4; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(n)/float64(div), "KMGTP"[exp])
}

// DefaultPartTTL is how long orphaned .part files are kept before cleanup.
const DefaultPartTTL = 7 * 24 * time.Hour
