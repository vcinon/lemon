package cli

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"

	"github.com/siin/lemon/internal/serve"
	"github.com/siin/lemon/internal/transfer"
)

// progress is a simple line-based transfer indicator.
//
// On a terminal it redraws a single line in place with \r. When the output is
// redirected it prints one line per completed file, so `lemon transfer ... >
// log` stays greppable. There is no curses, no alternate screen and no
// alternate mode: the terminal is left exactly as we found it.
type progress struct {
	env *Env
	tty bool
	// live tracks whether a redrawn region is currently on screen.
	live bool
	// liveLines is how many lines that region occupies.
	liveLines int
	// seen deduplicates file completion lines.
	seen map[string]bool
	// summary accumulates per-file totals for the final line.
	summary []fileProgress
}

// fileProgress is one row of the progress display.
type fileProgress struct {
	name string
	n    int64
	done int64
	// resumed marks a file that continued from a partial download.
	resumed bool
}

// newProgress builds a progress renderer for the env's stdout.
func newProgress(env *Env) *progress {
	return &progress{env: env, tty: env.IsTTY, seen: map[string]bool{}, summary: []fileProgress{}}
}

// Start prints the header lines for a transfer to peer.
func (p *progress) Start(peer string) {
	if peer == "" {
		peer = "peer"
	}
	p.env.Errf("transferring to %s...\n", peer)
}

// Update records progress for one file.
//
// Updates arrive from the daemon as the bytes actually move, so the bar tracks
// a real transfer instead of replaying it once it has already finished.
func (p *progress) Update(name string, n, done int64) {
	row := p.find(name)
	row.n, row.done = n, done
	p.redraw()
}

// Done records a completed file.
func (p *progress) Done(name string, size int64, resumed bool) {
	row := p.find(name)
	row.n, row.done, row.resumed = size, size, resumed
	if !p.seen[name] {
		p.seen[name] = true
		if !p.tty {
			p.env.Line("%s %s", padRight(name, 24), bar(size, size, 20))
		}
	}
	p.redraw()
}

// Fail records a file that could not be sent.
func (p *progress) Fail(name string, reason string) {
	p.clear()
	p.env.Errf("%s: %s\n", name, reason)
}

// find returns the progress row for a file, creating it if needed.
func (p *progress) find(name string) *fileProgress {
	for i := range p.summary {
		if p.summary[i].name == name {
			return &p.summary[i]
		}
	}
	p.summary = append(p.summary, fileProgress{name: name})
	return &p.summary[len(p.summary)-1]
}

// redraw repaints the live region.
func (p *progress) redraw() {
	if !p.tty {
		return
	}
	var b strings.Builder
	for _, row := range p.summary {
		if row.done == 0 {
			continue
		}
		b.WriteString(padRight(row.name, 24))
		b.WriteByte(' ')
		b.WriteString(bar(row.n, row.done, 20))
		b.WriteByte(' ')
		b.WriteString(fmt.Sprintf("%3d%%", percent(row.n, row.done)))
		b.WriteByte('\n')
	}
	out := b.String()
	// Erase previously drawn lines before repainting.
	var clear strings.Builder
	for i := 0; i < p.liveLines; i++ {
		clear.WriteString("\r\x1b[2K")
		if i < p.liveLines-1 {
			clear.WriteString("\x1b[1A")
		}
	}
	fmt.Fprint(p.env.Stdout, clear.String()+strings.TrimSuffix(out, "\n"))
	p.liveLines = countLines(out)
	p.live = p.liveLines > 0
}

// clear wipes the live region so a permanent line can be printed.
func (p *progress) clear() {
	if !p.tty || !p.live {
		return
	}
	var b strings.Builder
	for i := 0; i < p.liveLines; i++ {
		b.WriteString("\r\x1b[2K")
		if i < p.liveLines-1 {
			b.WriteString("\x1b[1A")
		}
	}
	fmt.Fprint(p.env.Stdout, b.String())
	p.liveLines = 0
	p.live = false
}

// Replay folds the final per-file counts into the display, so a transfer that
// finished before the header was drawn still shows a completed bar.
func (p *progress) Replay(reports []serve.TransferReport) {
	for _, r := range reports {
		if r.Err != "" {
			p.Fail(r.Name, r.Err)
			continue
		}
		p.Done(r.Name, r.Size, r.Resumed)
	}
}

// Finish clears the live region. The summary is printed by the command, so it
// can also report a failed or queued transfer from one place.
func (p *progress) Finish() { p.clear() }

// Abort clears the live region after a failure, leaving no half-drawn bar.
func (p *progress) Abort() { p.clear() }

// lineCount is the number of newlines in s.
func countLines(s string) int {
	if s == "" {
		return 0
	}
	return strings.Count(s, "\n") + 1
}

// percent returns the completion ratio of n out of done, clamped to 100.
func percent(n, done int64) int {
	if done <= 0 {
		return 0
	}
	p := int(float64(n) / float64(done) * 100)
	if p > 100 {
		return 100
	}
	if p < 0 {
		return 0
	}
	return p
}

// barWidth is the block count in a progress bar.
const barWidth = 20

// bar renders a block bar, e.g. "██████████░░░░░░░░░░".
func bar(n, done int64, width int) string {
	p := percent(n, done)
	filled := p * width / 100
	var b strings.Builder
	for i := 0; i < width; i++ {
		if i < filled {
			b.WriteString("█")
		} else {
			b.WriteString("░")
		}
	}
	return b.String()
}

// padRight pads s with spaces to width, truncating when it is longer.
func padRight(s string, width int) string {
	r := []rune(s)
	if len(r) > width {
		if width > 1 {
			return string(r[:width-1]) + "…"
		}
		return string(r[:width])
	}
	return s + strings.Repeat(" ", width-len(r))
}

// formatSize exposes the byte formatter for command output.
func formatSize(n int64) string { return transfer.FormatSize(n) }

// detach builds the command that launches a background listener.
//
// setsid puts the child in its own session so it is not killed when the
// invoking terminal closes, and Stdin is /dev/null so it can never read.
func detach(exe string, args ...string) *exec.Cmd {
	cmd := exec.Command(exe, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	devNull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		return cmd
	}
	cmd.Stdin = devNull
	// Release the descriptor in the parent once the child has inherited it.
	// exec.Cmd dups the file into the child, so closing here is safe.
	defer devNull.Close()
	return cmd
}

// exitCodeFor maps a daemon response onto a process exit status.
func exitCodeFor(resp *serve.Response, err error) int {
	if err != nil {
		return ExitError
	}
	if resp == nil {
		return ExitError
	}
	if resp.OK {
		return ExitOK
	}
	switch resp.Code {
	case serve.CodeNotConfigured:
		return ExitNotConfigured
	case serve.CodeUnknownPeer, serve.CodeNoDefault:
		return ExitUsage
	case serve.CodeInvalid, serve.CodeNotFound:
		return ExitUsage
	case serve.CodeOffline:
		return ExitOffline
	default:
		return ExitError
	}
}
