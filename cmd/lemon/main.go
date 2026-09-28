// Command lemon is a tiny peer-to-peer command-line messenger that runs over
// Tailscale.
//
// It is deliberately not a GUI, not a TUI, and not an interactive application:
// it behaves like a set of Unix commands (send, transfer, history, status).
package main

import (
	"os"
	"strings"

	"github.com/siin/lemon/internal/cli"
)

// buildMarker is embedded in the binary so `make install` can recognise a lemon
// build and avoid clobbering an unrelated program at the same path.
const buildMarker = "lemon-p2p/buildinfo github.com/siin/lemon"

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr, os.Stdin))
}

// run is the testable entry point.
func run(argv []string, stdout, stderr, stdin *os.File) int {
	// Referencing buildMarker keeps it in the binary's read-only data, which
	// is what scripts/ismine.sh greps for.
	_ = buildMarker

	env := &cli.Env{
		Stdout: stdout,
		Stderr: stderr,
		Stdin:  stdin,
		IsTTY:  isTerminal(stdout),
	}

	command, args, global, err := parseArgs(argv)
	if err != nil {
		env.Errf("lemon: %v\n", err)
		env.Errf("Run 'lemon help' for usage.\n")
		return cli.ExitUsage
	}
	env.Global = global

	switch command {
	case "version":
		env.Line("lemon %s (protocol %d)", cli.Version, cli.ProtocolVersion)
		return cli.ExitOK
	case "help":
		env.Printf("%s", cli.UsageString())
		return cli.ExitOK
	}

	return cli.Run(env, command, args)
}

// parseArgs splits global flags from the command and its arguments.
//
// Flags are accepted before and after the command name, so both
// `lemon --debug serve` and `lemon send --debug hi` parse.
func parseArgs(argv []string) (command string, args []string, global cli.Global, err error) {
	var positional []string

	for i := 0; i < len(argv); i++ {
		a := argv[i]

		// Everything after the command word belongs to the command, including
		// its own flags. Recognising global flags anywhere would swallow them:
		// `lemon transfer --to alice file` would be rejected as an unknown
		// flag instead of reaching the command that implements it.
		if len(positional) > 0 {
			positional = append(positional, a)
			continue
		}

		switch {
		case a == "--":
			positional = append(positional, argv[i+1:]...)
			cmd, cmdArgs, _ := splitPositional(positional)
			return cmd, cmdArgs, global, nil

		case a == "--debug" || a == "-d":
			global.Debug = true

		case a == "--version" || a == "-v":
			return "version", nil, global, nil

		case a == "--help" || a == "-h":
			// Pass through so the command can render its own help text.
			if len(positional) > 0 {
				positional = append(positional, "--help")
			} else {
				positional = append(positional, "help")
			}

		case a == "--config" || a == "-c":
			if i+1 >= len(argv) {
				return "", nil, global, errNeedsValue(a)
			}
			i++
			if err := os.Setenv("LEMON_CONFIG_DIR", argv[i]); err != nil {
				return "", nil, global, err
			}

		case strings.HasPrefix(a, "--config="):
			if err := os.Setenv("LEMON_CONFIG_DIR", strings.TrimPrefix(a, "--config=")); err != nil {
				return "", nil, global, err
			}

		case len(a) > 1 && strings.HasPrefix(a, "-"):
			return "", nil, global, &argError{msg: "unknown flag " + a}

		default:
			positional = append(positional, a)
		}
	}
	cmd, cmdArgs, _ := splitPositional(positional)
	return cmd, cmdArgs, global, nil
}

// splitPositional separates the command from its arguments.
func splitPositional(positional []string) (string, []string, cli.Global) {
	if len(positional) == 0 {
		return "help", nil, cli.Global{}
	}
	return positional[0], positional[1:], cli.Global{}
}

// argError is a bad command line.
type argError struct{ msg string }

func (e *argError) Error() string { return e.msg }

// errNeedsValue builds the error for a flag missing its value.
func errNeedsValue(flag string) error {
	return &argError{msg: flag + " needs a directory"}
}

// isTerminal reports whether f is a character device, which is our TTY test.
func isTerminal(f *os.File) bool {
	if f == nil {
		return false
	}
	info, err := f.Stat()
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeCharDevice != 0
}
