package main

import "testing"

// TestParseArgsSubcommandFlags is a regression test.
//
// Global flags used to be recognised anywhere on the line, so a command's own
// options were rejected before the command ever saw them. `lemon transfer --to
// alice notes.txt` is exactly what an operator types, and it failed with
// "unknown flag --to".
func TestParseArgsSubcommandFlags(t *testing.T) {
	tests := []struct {
		name    string
		argv    []string
		cmd     string
		args    []string
		debug   bool
		wantErr bool
	}{
		{
			name: "subcommand flag reaches the command",
			argv: []string{"transfer", "--to", "alice", "notes.txt"},
			cmd:  "transfer",
			args: []string{"--to", "alice", "notes.txt"},
		},
		{
			name:  "global flag before the command",
			argv:  []string{"--debug", "send", "hello"},
			cmd:   "send",
			args:  []string{"hello"},
			debug: true,
		},
		{
			name:  "global and subcommand flags together",
			argv:  []string{"-d", "transfer", "--no-resume", "--to", "bob", "a", "b"},
			cmd:   "transfer",
			args:  []string{"--no-resume", "--to", "bob", "a", "b"},
			debug: true,
		},
		{
			name: "help after the command is passed through",
			argv: []string{"transfer", "--help"},
			cmd:  "transfer",
			args: []string{"--help"},
		},
		{
			name: "value form of a subcommand flag",
			argv: []string{"send", "--to=alice", "hi"},
			cmd:  "send",
			args: []string{"--to=alice", "hi"},
		},
		{
			name: "double dash after the command belongs to it",
			argv: []string{"send", "--", "--not-a-flag"},
			cmd:  "send",
			args: []string{"--", "--not-a-flag"},
		},
		{
			name:    "unknown global flag is still an error",
			argv:    []string{"--nope", "send"},
			wantErr: true,
		},
		{
			name: "double dash before the command ends global parsing",
			argv: []string{"--", "send", "-x"},
			cmd:  "send",
			args: []string{"-x"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd, args, global, err := parseArgs(tt.argv)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseArgs(%v) = %q, %v; want an error", tt.argv, cmd, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseArgs(%v): %v", tt.argv, err)
			}
			if cmd != tt.cmd {
				t.Errorf("cmd = %q, want %q", cmd, tt.cmd)
			}
			if len(args) != len(tt.args) {
				t.Fatalf("args = %v, want %v", args, tt.args)
			}
			for i := range args {
				if args[i] != tt.args[i] {
					t.Fatalf("args = %v, want %v", args, tt.args)
				}
			}
			if global.Debug != tt.debug {
				t.Errorf("debug = %v, want %v", global.Debug, tt.debug)
			}
		})
	}
}

func TestParseArgsVersion(t *testing.T) {
	for _, argv := range [][]string{{"--version"}, {"-v"}} {
		cmd, _, _, err := parseArgs(argv)
		if err != nil {
			t.Fatalf("parseArgs(%v): %v", argv, err)
		}
		if cmd != "version" {
			t.Errorf("parseArgs(%v) cmd = %q, want version", argv, cmd)
		}
	}
}

func TestParseArgsNoArgs(t *testing.T) {
	cmd, _, _, err := parseArgs(nil)
	if err != nil {
		t.Fatalf("parseArgs(nil): %v", err)
	}
	if cmd != "help" {
		t.Errorf("cmd = %q, want help", cmd)
	}
}
