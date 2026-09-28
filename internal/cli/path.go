package cli

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// PATH persistence.
//
// Installing a binary into a directory the shell does not look in is the same as
// not installing it, so setup makes the directory usable. That means editing one
// shell startup file, which is the only place a PATH change can outlive the
// process. Three rules keep this safe:
//
//   - never duplicate: the edit is guarded by a marker and by the path itself;
//   - never clobber: the file is appended to, and only if it exists or is the
//     shell's conventional startup file;
//   - always reversible: the exact added lines are printed at the end.

// pathMarker identifies the block setup adds, so it can be found and removed.
const pathMarker = "# added by 'lemon setup'"

// PathResult describes what ensurePathPersisted did.
type PathResult struct {
	// File is the startup file that was edited, or would need editing.
	File string
	// Method is the line added, for display.
	Method string
	// Changed is true when a file was modified.
	Changed bool
	// AlreadyPresent is true when PATH already covers the directory.
	AlreadyPresent bool
	// Shell is the detected shell name, e.g. "bash".
	Shell string
	// Extra lists further startup files that also needed the edit.
	Extra []string
	// Reason explains a skip.
	Reason string
}

// ensurePathPersisted makes dir usable in future shells by adding it to the
// detected shell's startup file.
//
// It is deliberately conservative: an unknown shell reports why and leaves the
// decision to the user rather than guessing at a file to edit.
func ensurePathPersisted(dir string) (PathResult, error) {
	if dirOnPath(dir) {
		return PathResult{AlreadyPresent: true}, nil
	}

	shell, files, err := shellStartupFiles()
	if err != nil {
		return PathResult{}, err
	}
	if len(files) == 0 {
		return PathResult{Shell: shell, Reason: "no startup file to edit"}, nil
	}
	res := PathResult{File: files[0], Shell: shell, Method: pathLine(shell, dir)}
	block := pathMarker + "\n" + res.Method + "\n"

	var firstErr error
	for _, rcFile := range files {
		changed, present, err := appendToStartupFile(rcFile, dir, block)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if present && !res.Changed {
			res.AlreadyPresent = true
			res.Reason = fmt.Sprintf("%s already adds %s to PATH", rcFile, dir)
		}
		if changed {
			res.Changed = true
			if rcFile != res.File {
				res.Extra = append(res.Extra, rcFile)
			}
		}
	}
	if res.Changed {
		// A successful edit is the outcome; a failure on a secondary file is
		// worth reporting but must not mask it.
		return res, firstErr
	}
	return res, firstErr
}

// appendToStartupFile adds block to one startup file, once.
//
// It reports whether the file changed and whether PATH was already covered.
func appendToStartupFile(rcFile, dir, block string) (changed, present bool, err error) {
	data, err := os.ReadFile(rcFile)
	switch {
	case err == nil:
		if alreadyHasPath(data, dir) {
			return false, true, nil
		}
	case errors.Is(err, os.ErrNotExist):
		// A conventional startup file that does not exist yet: create it, so
		// the change takes effect rather than being silently written nowhere.
	case errors.Is(err, os.ErrPermission):
		return false, false, fmt.Errorf("cannot read %s: permission denied", rcFile)
	default:
		return false, false, fmt.Errorf("cannot read %s: %w", rcFile, err)
	}

	f, err := os.OpenFile(rcFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return false, false, fmt.Errorf("cannot update %s: %w", rcFile, err)
	}
	defer f.Close()
	if len(data) > 0 && !strings.HasSuffix(string(data), "\n") {
		// Do not glue our line onto an unterminated last line.
		if _, err := f.WriteString("\n"); err != nil {
			return false, false, err
		}
	}
	if _, err := f.WriteString(block); err != nil {
		return false, false, fmt.Errorf("cannot update %s: %w", rcFile, err)
	}
	return true, false, nil
}

// pathLine renders the line that adds dir to PATH for a shell.
func pathLine(shell, dir string) string {
	if shell == "fish" {
		return "fish_add_path " + shellQuote(dir)
	}
	return `export PATH="` + dir + `:$PATH"`
}

// alreadyHasPath reports whether data adds dir to PATH, whether by our marker
// or by the user's own hand.
func alreadyHasPath(data []byte, dir string) bool {
	text := string(data)
	if strings.Contains(text, pathMarker) && strings.Contains(text, dir) {
		return true
	}
	for _, line := range strings.Split(text, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "export") && !strings.HasPrefix(trimmed, "setenv") {
			continue
		}
		if strings.Contains(trimmed, "PATH") && strings.Contains(trimmed, dir) {
			return true
		}
	}
	return false
}

// shellStartupFiles returns the detected shell and the files it reads at
// startup, most important first.
func shellStartupFiles() (string, []string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", nil, fmt.Errorf("cannot determine home directory: %w", err)
	}
	shell := filepath.Base(strings.TrimSpace(os.Getenv("SHELL")))

	switch shell {
	case "bash":
		// Interactive non-login shells read .bashrc. A login shell — what a
		// terminal usually starts — reads .bash_profile and only reaches
		// .bashrc if that file sources it. Edit whichever is actually read.
		bashrc := filepath.Join(home, ".bashrc")
		profile := filepath.Join(home, ".bash_profile")
		if data, err := os.ReadFile(profile); err == nil && !sourcesFile(data, ".bashrc") {
			return shell, []string{profile, bashrc}, nil
		}
		return shell, []string{bashrc}, nil
	case "zsh":
		return shell, []string{filepath.Join(home, ".zshrc")}, nil
	case "fish":
		return shell, []string{filepath.Join(home, ".config", "fish", "config.fish")}, nil
	case "sh", "dash", "ash":
		// A POSIX shell login reads .profile. That covers servers and
		// containers, where $SHELL is /bin/sh rather than a desktop shell.
		return shell, []string{filepath.Join(home, ".profile")}, nil
	case "":
		return "", nil, errors.New("cannot tell which shell you use: SHELL is not set")
	default:
		return shell, nil, fmt.Errorf("lemon does not know how to update PATH for %s: add it yourself", shell)
	}
}

// sourcesFile reports whether shell data reads the file named name.
func sourcesFile(data []byte, name string) bool {
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, ".") || strings.HasPrefix(line, "source") {
			if strings.Contains(line, name) {
				return true
			}
		}
	}
	return false
}

// shellQuote quotes a path for fish, which does not expand variables in
// fish_add_path the way bash does in an export.
func shellQuote(path string) string {
	if strings.ContainsAny(path, " \t'\"$") {
		return "'" + strings.ReplaceAll(path, "'", `\'`) + "'"
	}
	return path
}
