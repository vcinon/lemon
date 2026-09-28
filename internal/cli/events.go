package cli

import (
	"strings"

	"github.com/siin/lemon/internal/serve"
)

// printEvents renders the daemon's progress events as plain lines.
func (e *Env) printEvents(events []serve.Event) {
	for _, ev := range events {
		switch ev.Kind {
		case "progress":
			// Byte-level progress is rendered by the transfer command's own
			// renderer, which knows about the file layout.
			continue
		case "done":
			continue
		default:
			if ev.Text != "" {
				e.Line("%s", ev.Text)
			}
		}
	}
}

// transferEvents converts daemon events into per-file progress updates.
func transferEvents(events []serve.Event) (updates []serve.Event, done []serve.Event) {
	for _, ev := range events {
		switch ev.Kind {
		case "progress":
			updates = append(updates, ev)
		case "done":
			done = append(done, ev)
		}
	}
	return updates, done
}

// splitPeerMessage resolves `lemon send` arguments into a peer and a body.
//
// The rule is deliberately simple and documented: the first argument is treated
// as a peer name only when it exactly matches a configured peer. That keeps
// `lemon send hello there` working while still allowing `lemon send alice hi`.
// `--to` overrides the inference entirely.
func splitPeerMessage(args []string, known func(string) bool, to string) (peer, body string, err error) {
	if to != "" {
		if len(args) == 0 {
			return "", "", Usage("nothing to send: try: lemon send --to %s <text>", to)
		}
		return to, strings.Join(args, " "), nil
	}
	if len(args) == 0 {
		return "", "", Usage("nothing to send. usage: lemon send [peer] <text...>")
	}
	if len(args) > 1 && known(args[0]) {
		return args[0], strings.Join(args[1:], " "), nil
	}
	return "", strings.Join(args, " "), nil
}
