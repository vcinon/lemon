package serve

import (
	"context"
	"os"
	"testing"

	"github.com/siin/lemon/internal/config"
	"github.com/siin/lemon/internal/logging"
	"github.com/siin/lemon/internal/peerconn"
)

// TestReloadConfigPicksUpNewPeer is a regression test.
//
// The listener is a long-lived process and `lemon peer add` is a short-lived
// one. When the daemon answered from the config it read at startup, a peer
// added afterwards was unknown to the only process able to send to it, and the
// command looked like it had done nothing.
func TestReloadConfigPicksUpNewPeer(t *testing.T) {
	layout, err := config.Paths()
	if err != nil {
		t.Fatalf("Paths: %v", err)
	}
	layout.Dir = t.TempDir()
	layout.ConfigFile = layout.Dir + "/config.json"

	// The listener starts with an identity and no peers.
	initial := &config.Config{Username: "alice", Port: 6767, Peers: map[string]*config.Peer{}}
	if err := initial.Save(layout); err != nil {
		t.Fatalf("Save: %v", err)
	}

	d := &Daemon{
		layout: layout,
		cfg:    initial,
		log:    logging.Discard(),
		reg:    peerconn.NewRegistry(),
		ts:     &fakeTailnet{self: "127.0.0.1"},
	}

	// Another process adds a peer while the listener is running.
	other, err := config.Load(layout)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if err := other.SetPeer("bob", ""); err != nil {
		t.Fatalf("SetPeer: %v", err)
	}
	other.DefaultPeer = "bob"
	if err := other.Save(layout); err != nil {
		t.Fatalf("Save: %v", err)
	}

	d.reloadConfig(context.Background())

	if _, ok := d.cfg.Peer("bob"); !ok {
		t.Error("peer added after startup is still unknown to the daemon")
	}
	if d.cfg.DefaultPeer != "bob" {
		t.Errorf("default peer = %q, want %q", d.cfg.DefaultPeer, "bob")
	}
}

// TestReloadConfigKeepsPointerStable checks that the reload updates the config
// the daemon already holds rather than swapping the pointer out from under it.
func TestReloadConfigKeepsPointerStable(t *testing.T) {
	layout, err := config.Paths()
	if err != nil {
		t.Fatalf("Paths: %v", err)
	}
	layout.Dir = t.TempDir()
	layout.ConfigFile = layout.Dir + "/config.json"

	initial := &config.Config{Username: "alice", Port: 6767, Peers: map[string]*config.Peer{}}
	if err := initial.Save(layout); err != nil {
		t.Fatalf("Save: %v", err)
	}
	d := &Daemon{layout: layout, cfg: initial, log: logging.Discard(), ts: &fakeTailnet{}}

	before := d.cfg
	d.reloadConfig(context.Background())
	if d.cfg != before {
		t.Error("reload replaced the Config pointer; held references would go stale")
	}
	if d.cfg.Username != "alice" {
		t.Errorf("username = %q, want alice", d.cfg.Username)
	}
}

// TestReloadConfigSurvivesBrokenFile makes sure a bad config cannot take the
// listener down or wipe the settings it is running with.
func TestReloadConfigSurvivesBrokenFile(t *testing.T) {
	layout, err := config.Paths()
	if err != nil {
		t.Fatalf("Paths: %v", err)
	}
	layout.Dir = t.TempDir()
	layout.ConfigFile = layout.Dir + "/config.json"

	initial := &config.Config{Username: "alice", Port: 6767, Peers: map[string]*config.Peer{}}
	if err := initial.Save(layout); err != nil {
		t.Fatalf("Save: %v", err)
	}
	d := &Daemon{layout: layout, cfg: initial, log: logging.Discard(), ts: &fakeTailnet{}}

	// Invalid JSON, as an interrupted write or a stray editor would leave.
	if err := writeFile(layout.ConfigFile, "{not json"); err != nil {
		t.Fatalf("write: %v", err)
	}
	d.reloadConfig(context.Background())

	if d.cfg.Username != "alice" || d.cfg.Port != 6767 {
		t.Errorf("config was damaged by a failed reload: %+v", d.cfg)
	}
	if d.cfg.Peers == nil {
		t.Error("peers map became nil; later writes would panic")
	}
}

// TestPeerFingerprintDetectsChange covers the change detection that decides
// when cached peer resolutions have to be thrown away.
func TestPeerFingerprintDetectsChange(t *testing.T) {
	base := func() *config.Config {
		return &config.Config{Peers: map[string]*config.Peer{
			"bob": {Addr: "100.1.1.1:6767"},
		}}
	}

	before := peerFingerprint(base())

	same := base()
	if got := peerFingerprint(same); got != before {
		t.Errorf("identical configs differ:\n %s\n %s", before, got)
	}

	added := base()
	added.Peers["carol"] = &config.Peer{}
	if got := peerFingerprint(added); got == before {
		t.Error("adding a peer did not change the fingerprint")
	}

	repointed := base()
	repointed.Peers["bob"] = &config.Peer{Addr: "100.2.2.2:6767"}
	if got := peerFingerprint(repointed); got == before {
		t.Error("repointing a peer did not change the fingerprint")
	}

	disabled := base()
	disabled.Peers["bob"] = &config.Peer{Addr: "100.1.1.1:6767", Disabled: true}
	if got := peerFingerprint(disabled); got == before {
		t.Error("disabling a peer did not change the fingerprint")
	}

	defaulted := base()
	defaulted.DefaultPeer = "bob"
	if got := peerFingerprint(defaulted); got == before {
		t.Error("setting a default peer did not change the fingerprint")
	}
}

// writeFile is a small helper for the failure case above.
func writeFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o644)
}
