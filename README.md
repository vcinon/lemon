# lemon

A tiny peer-to-peer command-line messenger for people who already share a
Tailscale tailnet. Two machines on your tailnet, two commands, and you are
talking — no accounts, no server, no directory service.

```
lemon setup                                  # on each machine
lemon send alice "dinner at seven?"
lemon transfer alice ./invoice.pdf
```

## What it does

- **Talks directly.** Plain TCP from node to node over your tailnet. There is no
  relay and nothing to run in the middle.
- **Finds people for you.** `lemon discover` finds the other Lemons on your
  tailnet on its own: the peer's name comes from its handshake, so you never
  configure an address by hand.
- **Waits for you.** Messages and files are queued durably before delivery is
  attempted, and retried until they arrive. A peer that is offline is not an
  error, it is a pending job.
- **Checks what it sends.** Every file is verified with SHA-256 on both ends, and
  a partial transfer resumes instead of starting over.
- **Keeps a history.** SQLite, on each machine, so `lemon history` works
  offline.

## Install

Lemon is one static-ish binary. It uses cgo (SQLite), so build it where it will
run:

```sh
go build -o lemon ./cmd/lemon
sudo install -m 0755 lemon /usr/local/bin/lemon
```

`lemon setup` will also install the running binary into your user bin directory
(`~/.local/share/bin`) and add that directory to your shell's startup file if it
is not already on `PATH`. It never edits a file you did not ask it to, and it
refuses to replace a `lemon` that is not a Lemon build.

To keep the listener running across logins, run it from your compositor's
exec-once:

```
# Hyprland, for example
exec-once = lemon serve
```

## Getting started

On the first machine:

```sh
lemon setup
```

Setup asks for a username and a listening port, installs the binary, starts the
listener, and then searches the tailnet for other Lemons. If it finds one, it
becomes your default peer. Run it on the second machine the same way and the two
will know about each other.

### Commands

| Command | What it does |
| --- | --- |
| `lemon setup` | Configure this node, install the binary, start the listener, find a peer |
| `lemon send [peer] <text>` | Send a message; queued if the peer is offline |
| `lemon transfer <file...>` | Send files to the default peer |
| `lemon history [peer]` | Show stored messages |
| `lemon discover` | Find Lemons running on the tailnet |
| `lemon peers` | List configured peers and whether they are reachable |
| `lemon peer add <name> [addr]` | Add a peer; the address is resolved for you |
| `lemon peer rm <name>` | Forget a peer |
| `lemon connect <peer>` | Check that a peer is reachable now |
| `lemon status` | Listener, peers and queue state |
| `lemon queue` | Work waiting to be delivered |
| `lemon flush` | Retry queued work now |
| `lemon serve` | Run the listener in the foreground |
| `lemon version` | Print the version |

`lemon help` lists the flags each command accepts.

### Where things live

| Path | Contents |
| --- | --- |
| `~/.config/lemon/config.json` | Your username, port and peers |
| `~/.config/lemon/history.db` | Messages and transfers |
| `~/.config/lemon/queue/` | Work not yet delivered |
| `~/.config/lemon/serve.log` | The detached listener's log |
| `~/Public/` | Where received files land |

Set `LEMON_CONFIG_DIR` to keep several independent nodes on one machine — handy
for testing, and for running one Lemon per identity.

## Exit codes

| Code | Meaning |
| --- | --- |
| 0 | Success |
| 1 | Error |
| 2 | Bad usage |
| 3 | Peer offline; the work was queued |
| 4 | Not configured yet — run `lemon setup` |
| 5 | Could not connect |

## How discovery works

There is no directory and nothing to register with. To find peers, Lemon asks
Tailscale which nodes exist, then opens a short TCP connection to the Lemon port
on each one and performs the ordinary protocol handshake. A completed handshake
*is* the proof that a Lemon is running there, and the hello frame carries the
peer's own username — which is how a Lemon is identified without configuring
anything in advance.

Two consequences worth knowing:

- A lemon's username is yours to choose and need not match the node's tailnet
  name, so addresses are stored when they are discovered rather than looked up by
  name later.
- A tailnet carries no port information, so discovery probes the local port and
  the default (`6767`). A peer listening on a third, non-default port is not
  found by discovery; add it with `lemon peer add <name> <address>`.

## Development

```sh
make build     # build ./lemon
make test      # unit tests, plus ./tests/... once it lands
make check     # fmt, vet and test
```

Layout:

| Package | Responsibility |
| --- | --- |
| `cmd/lemon` | Argument parsing and exit codes |
| `internal/cli` | Command implementations, prompts, progress, setup |
| `internal/serve` | The daemon: control socket, request dispatch, retry loop |
| `internal/protocol` | Wire format, framing, handshake, client and server |
| `internal/transfer` | File staging, resume, digests, safe naming |
| `internal/peerconn` | Peer registry and address resolution |
| `internal/queue` | Durable outbox for messages and files |
| `internal/history` | SQLite history |
| `internal/tailscale` | Reading local tailnet state |
| `internal/config` | Configuration and paths |

The daemon is long-lived and every command is short-lived, so the daemon re-reads
`config.json` on each request. A peer you add is usable immediately, with no
restart.

## Licence

MIT. See `LICENSE`.
