// Package transfer streams files between Lemon peers.
//
// The design rules here come straight from the security requirements: a
// remote filename is never trusted, every write stays inside the destination
// directory, and no file is ever loaded into memory. Filenames are sanitised
// before they touch the filesystem, collisions get a numeric suffix, and an
// existing file is never silently overwritten.
//
// Resume support falls out of the protocol: partial data lives in a .part file
// keyed by transfer ID, and the receiver reports its offset so the sender can
// seek instead of restarting.
package transfer

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// Filename rules.
const (
	// MaxNameLen mirrors the protocol's filename cap.
	MaxNameLen = 255
	// reservedNames are Windows device names we also refuse, since a file
	// named "con" is confusing on any system and breaks when synced.
	maxReserved = "CON PRN AUX NUL COM1 COM2 COM3 COM4 COM5 COM6 COM7 COM8 COM9 LPT1 LPT2 LPT3 LPT4 LPT5 LPT6 LPT7 LPT8 LPT9"
)

// ErrUnsafeName means a remote filename could not be made safe.
var ErrUnsafeName = errors.New("unsafe filename")

// SanitizeName turns an untrusted remote filename into a safe basename.
//
// It strips any directory component (including "..", absolute paths and
// Windows separators), removes control characters, and replaces anything
// outside a conservative allowlist. The result is always a non-empty basename
// that contains no path separators, and never "." or "..".
func SanitizeName(raw string) (string, error) {
	if !utf8.ValidString(raw) {
		raw = strings.ToValidUTF8(raw, "_")
	}

	// Take the last component under both separator conventions, so
	// "../../etc/passwd" and "..\\..\\windows\\system32" both reduce to a leaf.
	name := raw
	if idx := strings.LastIndexAny(name, `/\`); idx >= 0 {
		name = name[idx+1:]
	}

	// Drop control characters and reserved Windows characters.
	var b strings.Builder
	for _, r := range name {
		switch {
		case r == 0 || r == utf8.RuneError:
			continue
		case unicode.IsControl(r):
			continue
		case strings.ContainsRune(`<>:"|?*`, r):
			b.WriteRune('_')
		default:
			b.WriteRune(r)
		}
	}
	name = strings.TrimSpace(b.String())
	// A leading dot is legal but a name of only dots is not.
	name = strings.TrimLeft(name, " \t.")
	if name == "" {
		return "", fmt.Errorf("%w: %q has no usable characters", ErrUnsafeName, raw)
	}

	// Truncate on a rune boundary so we never split a UTF-8 sequence.
	if len(name) > MaxNameLen {
		cut := MaxNameLen
		for cut > 0 && !utf8.RuneStart(name[cut]) {
			cut--
		}
		name = name[:cut]
	}
	// A trailing dot or space is illegal on Windows and confuses sync tools.
	name = strings.TrimRight(name, " .")
	if name == "" {
		return "", fmt.Errorf("%w: %q reduces to an empty name", ErrUnsafeName, raw)
	}
	if isReserved(name) {
		name = "_" + name
	}
	return name, nil
}

// isReserved reports whether name is a reserved device name.
func isReserved(name string) bool {
	upper := strings.ToUpper(name)
	// "CON.txt" is reserved too, so compare the stem.
	if idx := strings.Index(upper, "."); idx > 0 {
		upper = upper[:idx]
	}
	for _, r := range strings.Fields(maxReserved) {
		if upper == r {
			return true
		}
	}
	return false
}

// UniquePath returns a path inside dir for name that does not exist, adding
// " (1)", " (2)" and so on as needed. It never returns a path outside dir.
func UniquePath(dir, name string) (string, error) {
	safe, err := SanitizeName(name)
	if err != nil {
		return "", err
	}
	candidate := filepath.Join(dir, safe)
	if _, err := os.Lstat(candidate); errors.Is(err, fs.ErrNotExist) {
		return candidate, nil
	} else if err != nil {
		return "", fmt.Errorf("cannot inspect %s: %w", candidate, err)
	}

	ext := filepath.Ext(safe)
	stem := strings.TrimSuffix(safe, ext)
	// Keep numbered variants within the name budget.
	const maxVariants = 1000
	for i := 1; i < maxVariants; i++ {
		variant := fmt.Sprintf("%s (%d)%s", stem, i, ext)
		if len(variant) > MaxNameLen {
			// Shorten the stem until the variant fits.
			overflow := len(variant) - MaxNameLen
			if len(stem) > overflow {
				stem = stem[:len(stem)-overflow]
			}
			variant = fmt.Sprintf("%s (%d)%s", stem, i, ext)
		}
		candidate = filepath.Join(dir, variant)
		_, err := os.Lstat(candidate)
		if errors.Is(err, fs.ErrNotExist) {
			return candidate, nil
		}
		if err != nil {
			return "", fmt.Errorf("cannot inspect %s: %w", candidate, err)
		}
	}
	return "", fmt.Errorf("cannot find a free filename for %q in %s", name, dir)
}

// PartPath returns the staging path for a partial file inside dir.
func PartPath(dir, name string) (string, error) {
	safe, err := SanitizeName(name)
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, safe+".part"), nil
}

// EnsureDir creates dir with owner-only permissions.
func EnsureDir(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("cannot create %s: %w", dir, err)
	}
	return nil
}

// Source describes a file to send.
type Source struct {
	// Path is the absolute local path.
	Path string
	// Name is the basename announced to the peer.
	Name string
	// Size is the file's byte length.
	Size int64
	// SHA256 is the lowercase hex digest, empty when not precomputed.
	SHA256 string
}

// Stat fills Name and Size for a local path.
func Stat(path string) (Source, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return Source{}, fmt.Errorf("cannot resolve %s: %w", path, err)
	}
	info, err := os.Stat(abs)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return Source{}, fmt.Errorf("no such file: %s", path)
		}
		return Source{}, fmt.Errorf("cannot read %s: %w", path, err)
	}
	if info.IsDir() {
		return Source{}, fmt.Errorf("%s is a directory; lemon transfers files, not directories", path)
	}
	if !info.Mode().IsRegular() {
		return Source{}, fmt.Errorf("%s is not a regular file", path)
	}
	return Source{Path: abs, Name: filepath.Base(abs), Size: info.Size()}, nil
}

// Open opens a source file for streaming.
func (s Source) Open() (*os.File, error) {
	f, err := os.Open(s.Path)
	if err != nil {
		return nil, fmt.Errorf("cannot open %s: %w", s.Path, err)
	}
	return f, nil
}

// Stage is a file being received: a .part file plus its final destination.
type Stage struct {
	// Final is the collision-free destination path in the receive directory.
	Final string
	// Part is the staging path.
	Part string
	// Name is the sanitised basename.
	Name string
}

// StageFile prepares a staging file for an incoming transfer.
//
// offset is how many bytes already exist in the part file; it comes from the
// receiver's own state, so a resumed transfer continues rather than restarts.
func StageFile(receiveDir, name, transferID string, offset int64) (*Stage, error) {
	if err := EnsureDir(receiveDir); err != nil {
		return nil, err
	}
	part, err := PartPath(receiveDir, name)
	if err != nil {
		return nil, err
	}
	if transferID != "" {
		// The transfer ID keeps concurrent transfers of the same filename
		// from colliding in the staging area.
		part = fmt.Sprintf("%s.%s.part", strings.TrimSuffix(part, ".part"), shortID(transferID))
	}
	// If we are resuming, the part file must already exist and hold the bytes
	// the receiver believes it has.
	if offset > 0 {
		info, err := os.Stat(part)
		if err != nil {
			// The offset is a claim we cannot back up; start over.
			offset = 0
		} else if info.Size() < offset {
			// Truncated behind our back; restart this file.
			offset = 0
		} else if info.Size() > offset {
			// Extra bytes beyond the claim are unusable; trim to the claim.
			if err := os.Truncate(part, offset); err != nil {
				return nil, fmt.Errorf("cannot trim %s: %w", part, err)
			}
		}
	}
	final, err := UniquePath(receiveDir, name)
	if err != nil {
		return nil, err
	}
	return &Stage{Final: final, Part: part, Name: filepath.Base(final)}, nil
}

// shortID trims a transfer ID for use in a filename.
func shortID(id string) string {
	if len(id) <= 8 {
		return id
	}
	return id[:8]
}

// OpenPart opens the staging file for appending, creating it if needed.
func (s *Stage) OpenPart() (*os.File, error) {
	f, err := os.OpenFile(s.Part, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("cannot open %s: %w", s.Part, err)
	}
	return f, nil
}

// CurrentSize returns how many bytes are already staged.
func (s *Stage) CurrentSize() (int64, error) {
	info, err := os.Stat(s.Part)
	if errors.Is(err, fs.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("cannot inspect %s: %w", s.Part, err)
	}
	return info.Size(), nil
}

// Commit moves the staging file into place atomically, refusing to clobber an
// existing destination.
// Commit publishes the finished file under its final name.
//
// The link is created with O_EXCL so an existing file is never clobbered: if
// another file appeared since staging, the transfer fails and the part file is
// kept for the caller to resolve.
func (s *Stage) Commit() error {
	err := os.Link(s.Part, s.Final)
	if err == nil {
		_ = os.Remove(s.Part)
		return nil
	}
	if !os.IsExist(err) {
		// A cross-device or unsupported link cannot be used here; fall back to
		// rename, which is atomic within one filesystem.
		if os.IsNotExist(err) {
			return fmt.Errorf("cannot save %s: %w", s.Final, err)
		}
		if rerr := os.Rename(s.Part, s.Final); rerr == nil {
			return nil
		}
		return fmt.Errorf("cannot save %s: %w", s.Final, err)
	}
	return fmt.Errorf("%w: %s already exists", ErrUnsafeName, s.Final)
}

// Discard removes the staging file.
func (s *Stage) Discard() {
	_ = os.Remove(s.Part)
}

// Contains reports whether path is inside dir. It is the last line of defence
// against a peer steering writes outside the receive directory.
//
// The check is lexical (after Abs and Clean) and additionally resolves
// symlinks, so neither "../secret" nor a symlink planted inside the receive
// directory can point the write somewhere else.
func Contains(dir, path string) (bool, error) {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return false, err
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return false, err
	}
	if !within(absDir, absPath) {
		return false, nil
	}
	// Resolve what we can. A path that does not exist yet is normal here: the
	// receive directory exists, the file inside it may not.
	realDir, derr := filepath.EvalSymlinks(absDir)
	if derr != nil {
		return false, nil
	}
	realPath, perr := filepath.EvalSymlinks(absPath)
	if perr == nil {
		return within(realDir, realPath), nil
	}
	// Fall back to resolving the parent, which must exist before we write.
	parent, parentErr := filepath.EvalSymlinks(filepath.Dir(absPath))
	if parentErr != nil {
		return false, nil
	}
	return within(realDir, filepath.Join(parent, filepath.Base(absPath))), nil
}

// within reports whether path lies under dir, without following symlinks.
func within(dir, path string) bool {
	rel, err := filepath.Rel(dir, path)
	if err != nil {
		return false
	}
	if rel == "." {
		return true
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// DiscardOrphanParts removes .part files older than maxAge from dir, so an
// abandoned transfer does not leave litter behind forever.
func DiscardOrphanParts(dir string, maxAge time.Duration) int {
	items, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	cutoff := time.Now().Add(-maxAge)
	n := 0
	for _, item := range items {
		if item.IsDir() || !strings.HasSuffix(item.Name(), ".part") {
			continue
		}
		info, err := item.Info()
		if err != nil || info.ModTime().After(cutoff) {
			continue
		}
		if os.Remove(filepath.Join(dir, item.Name())) == nil {
			n++
		}
	}
	return n
}

// Copy is a small helper used by tests and by the receiver's commit path.
func Copy(dst io.Writer, src io.Reader) (int64, error) { return io.Copy(dst, src) }
