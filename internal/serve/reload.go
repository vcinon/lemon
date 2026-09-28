package serve

import (
	"context"
	"strings"

	"github.com/siin/lemon/internal/config"
)

// reloadConfig re-reads config.json into the running daemon.
//
// The listener outlives the commands that configure it. `lemon setup` writes a
// username and then adds a peer, `lemon peer add` adds one later, and none of
// those processes are the one that has to act on the result. Without a reload,
// the daemon keeps answering from a snapshot taken at startup and a freshly
// added peer is reported as unknown until something restarts the listener.
//
// The fields are copied into the existing Config rather than replacing the
// pointer, because the rest of the daemon holds that pointer.
func (d *Daemon) reloadConfig(ctx context.Context) {
	fresh, err := config.Load(d.layout)
	if err != nil {
		// A broken or half-written file must not take the listener down: the
		// previous configuration is still the best known good one.
		d.log.Warnf("cannot reload %s: %v; continuing with the loaded config", d.layout.ConfigFile, err)
		return
	}

	prevPort := d.cfg.Port
	prevPeers := peerFingerprint(d.cfg)

	*d.cfg = *fresh
	// Keep the maps non-nil: the rest of the daemon writes to them.
	if d.cfg.Peers == nil {
		d.cfg.Peers = map[string]*config.Peer{}
	}

	if d.cfg.Port != prevPort {
		// The port is bound for the lifetime of the process. Saying so beats
		// silently running on the old port while the config claims the new one.
		d.log.Warnf("listen port changed from %d to %d in %s; restart the listener to use it",
			prevPort, d.cfg.Port, d.layout.ConfigFile)
	}

	if peerFingerprint(d.cfg) != prevPeers && d.reg != nil {
		// Peers changed, so the cached resolutions are stale.
		if err := d.reg.Refresh(ctx, d.ts, d.cfg.Peers, d.cfg.Port); err != nil {
			d.log.Debugf("cannot refresh peers after config change: %v", err)
		}
	}
}

// peerFingerprint summarises the peer set for change detection.
//
// Only what affects routing is included: adding a peer, or changing where one
// is dialled, is worth a refresh. A disabled flag is included because it
// changes whether a peer is used at all.
func peerFingerprint(cfg *config.Config) string {
	var b strings.Builder
	names := cfg.PeerNames()
	for _, name := range names {
		p, _ := cfg.Peer(name)
		b.WriteString(name)
		b.WriteByte('=')
		if p == nil {
			b.WriteString("?")
		} else {
			b.WriteString(p.Addr)
			if p.Disabled {
				b.WriteString("!off")
			}
		}
		b.WriteByte(';')
	}
	b.WriteString("default=")
	b.WriteString(cfg.DefaultPeer)
	return b.String()
}
