package cli

import (
	"debug/buildinfo"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// UserBinDir returns the per-user binary directory Lemon installs itself into.
//
// This is the XDG data directory's `bin`, which is where a user-installed
// program belongs when installing into /usr is not an option. It is a
// convention, not a guarantee: plenty of systems do not put it on PATH, which
// is why setup says so rather than assuming.
func UserBinDir() (string, error) {
	data := strings.TrimSpace(os.Getenv("XDG_DATA_HOME"))
	if data == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("cannot determine home directory: %w", err)
		}
		data = filepath.Join(home, ".local", "share")
	}
	abs, err := filepath.Abs(filepath.Join(data, "bin"))
	if err != nil {
		return "", fmt.Errorf("cannot resolve the user bin directory: %w", err)
	}
	return abs, nil
}

// InstallResult describes what installToUserBin did.
type InstallResult struct {
	// Path is the link or copy that now provides `lemon`.
	Path string
	// Dir is the directory it lives in.
	Dir string
	// Linked is true when a symlink was used instead of a copy.
	Linked bool
	// OnPath is true when Dir appears in PATH.
	OnPath bool
	// Changed is false when the target already pointed at this binary.
	Changed bool
	// Skipped is set when nothing was written, with the reason.
	Skipped string
}

// installToUserBin makes the running lemon available from the user bin
// directory, so `lemon` works from any shell and from a compositor's exec-once.
//
// It links rather than copies where it can, because a symlink keeps the
// installed lemon and the one being run from the same file: rebuilding and
// reinstalling cannot leave two versions disagreeing about which is which.
// Nothing outside the user bin directory is touched, and an existing file that
// is not a lemon build is never replaced.
func installToUserBin(log Logger) (InstallResult, error) {
	dir, err := UserBinDir()
	if err != nil {
		return InstallResult{}, err
	}
	res := InstallResult{Dir: dir, Path: filepath.Join(dir, "lemon"), OnPath: dirOnPath(dir)}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return InstallResult{}, fmt.Errorf("cannot create %s: %w", dir, err)
	}

	exe, err := os.Executable()
	if err != nil {
		return InstallResult{}, fmt.Errorf("cannot locate the lemon binary: %w", err)
	}
	if resolved, rerr := filepath.EvalSymlinks(exe); rerr == nil {
		exe = resolved
	}
	if sameFile(res.Path, exe) {
		// Already installed, or we are running from the install path.
		res.Skipped = fmt.Sprintf("%s already runs this binary", res.Path)
		return res, nil
	}

	// Refuse to replace something that is not ours. Arch's core/lemon parser
	// generator and a developer's own script both live in bin directories.
	if existing, statErr := os.Lstat(res.Path); statErr == nil {
		if existing.Mode()&fs.ModeSymlink == 0 {
			if !isLemonBinary(res.Path) {
				return res, fmt.Errorf("%s already exists and is not a lemon build; leaving it alone", res.Path)
			}
		} else if target, rerr := os.Readlink(res.Path); rerr == nil {
			if absTarget, aerr := filepath.Abs(target); aerr == nil && sameFile(absTarget, exe) {
				res.Linked = true
				res.Skipped = fmt.Sprintf("%s already points at this binary", res.Path)
				return res, nil
			}
		}
	}

	// Link first: it is atomic, needs no space, and never leaves a half-written
	// executable on PATH. A link is impossible across filesystems or where
	// symlinks are unavailable, in which case copy.
	tmp := res.Path + ".new"
	_ = os.Remove(tmp)
	if err := os.Symlink(exe, tmp); err == nil {
		if err := os.Rename(tmp, res.Path); err != nil {
			_ = os.Remove(tmp)
			log.Debugf("cannot install by link: %v", err)
		} else {
			res.Linked, res.Changed = true, true
			return res, nil
		}
	} else {
		log.Debugf("cannot link into %s: %v", dir, err)
	}

	data, err := os.ReadFile(exe)
	if err != nil {
		return res, fmt.Errorf("cannot read the lemon binary: %w", err)
	}
	if err := os.WriteFile(tmp, data, 0o755); err != nil {
		return res, fmt.Errorf("cannot install into %s: %w", dir, err)
	}
	if err := os.Rename(tmp, res.Path); err != nil {
		_ = os.Remove(tmp)
		return res, fmt.Errorf("cannot install into %s: %w", dir, err)
	}
	res.Changed = true
	return res, nil
}

// sameFile reports whether two paths refer to the same file, following links.
func sameFile(a, b string) bool {
	fa, err := os.Stat(a)
	if err != nil {
		return false
	}
	fb, err := os.Stat(b)
	if err != nil {
		return false
	}
	return os.SameFile(fa, fb)
}

// isLemonBinary reports whether path was built from this module.
//
// It reads the build info the Go toolchain embeds, so the answer stays correct
// across toolchain versions. An unreadable or stripped binary is reported as
// not ours, which is the safe direction: we decline to replace it.
func isLemonBinary(path string) bool {
	info, err := buildinfo.ReadFile(path)
	if err != nil || info == nil {
		return false
	}
	if info.Main.Path == modulePath {
		return true
	}
	// A binary built by an older layout may report the main module elsewhere.
	for _, dep := range info.Deps {
		if dep == nil {
			continue
		}
		if dep.Path == modulePath {
			return true
		}
	}
	return false
}

// modulePath is this module, used to recognise our own binaries.
const modulePath = "github.com/siin/lemon"

// dirOnPath reports whether dir appears in PATH as a usable entry.
func dirOnPath(dir string) bool {
	path := os.Getenv("PATH")
	if path == "" {
		return false
	}
	for _, entry := range filepath.SplitList(path) {
		if entry == "" {
			continue
		}
		if abs, err := filepath.Abs(entry); err == nil && abs == dir {
			return true
		}
	}
	return false
}

// Logger is the small logging surface installToUserBin needs, kept as an
// interface so a test can capture what was reported.
type Logger interface {
	Debugf(format string, args ...any)
}
