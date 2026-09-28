// Package config loads and persists Lemon's local configuration and resolves
// the on-disk layout under the user's config directory.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// Protocol and filesystem constants.
const (
	// Version is the Lemon protocol version advertised in handshakes.
	Version = 1

	// DefaultPort is the default Lemon listening port.
	DefaultPort = 6767

	// MaxPeers is the maximum number of configured peers (network size 4).
	MaxPeers = 3

	// MaxUsernameLen bounds a stored username.
	MaxUsernameLen = 32

	// MaxPeerNameLen bounds a peer name.
	MaxPeerNameLen = 32
)

// ErrNotConfigured is returned when no username has been set yet.
var ErrNotConfigured = errors.New("lemon is not configured: run: lemon setup")

// ErrPeerLimit is returned when adding a peer would exceed MaxPeers.
var ErrPeerLimit = errors.New("peer limit reached")

// Peer is a known remote Lemon instance.
type Peer struct {
	// Addr is host:port. Empty means "resolve from tailscale status".
	Addr string `json:"addr,omitempty"`
	// Disabled removes the peer from active use without deleting it.
	Disabled bool `json:"disabled,omitempty"`
}

// Config is the contents of ~/.config/lemon/config.json.
type Config struct {
	Version int `json:"version"`
	// Username is the local identity announced to peers.
	Username string `json:"username,omitempty"`
	// Port is the local listening port.
	Port int `json:"port"`
	// DefaultPeer is used by `lemon send` when no peer is given.
	DefaultPeer string `json:"default_peer,omitempty"`
	// Peers maps peer name to its connection details.
	Peers map[string]*Peer `json:"peers,omitempty"`
	// AutostartAck records that the exec-once tip has been shown.
	AutostartAck bool `json:"autostart_ack,omitempty"`
	// NotifyCommand overrides notification dispatch (e.g. "notify-send").
	NotifyCommand string `json:"notify_command,omitempty"`
	// ReceiveDir overrides ~/Public for received files.
	ReceiveDir string `json:"receive_dir,omitempty"`
}

// Layout holds resolved filesystem paths for one Lemon installation.
type Layout struct {
	Dir        string
	ConfigFile string
	Socket     string
	LockFile   string
	PIDFile    string
	LogFile    string
	HistoryDB  string
	QueueDir   string
	TransferIn string
	PublicDir  string
}

// Paths resolves the Lemon directory layout.
//
// Precedence: LEMON_CONFIG_DIR, then XDG_CONFIG_HOME/lemon, then
// ~/.config/lemon.
func Paths() (Layout, error) {
	dir := strings.TrimSpace(os.Getenv("LEMON_CONFIG_DIR"))
	if dir == "" {
		xdg := strings.TrimSpace(os.Getenv("XDG_CONFIG_HOME"))
		if xdg == "" {
			home, err := os.UserHomeDir()
			if err != nil {
				return Layout{}, fmt.Errorf("cannot determine home directory: %w", err)
			}
			xdg = filepath.Join(home, ".config")
		}
		dir = filepath.Join(xdg, "lemon")
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return Layout{}, fmt.Errorf("cannot resolve config directory: %w", err)
	}

	public := strings.TrimSpace(os.Getenv("LEMON_RECEIVE_DIR"))
	if public == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return Layout{}, fmt.Errorf("cannot determine home directory: %w", err)
		}
		public = filepath.Join(home, "Public")
	}
	if absPublic, err := filepath.Abs(public); err == nil {
		public = absPublic
	}

	return Layout{
		Dir:        abs,
		ConfigFile: filepath.Join(abs, "config.json"),
		Socket:     filepath.Join(abs, "lemon.sock"),
		LockFile:   filepath.Join(abs, "serve.lock"),
		PIDFile:    filepath.Join(abs, "serve.pid"),
		LogFile:    filepath.Join(abs, "serve.log"),
		HistoryDB:  filepath.Join(abs, "history.db"),
		QueueDir:   filepath.Join(abs, "queue"),
		TransferIn: filepath.Join(abs, "transfers", "incoming"),
		PublicDir:  public,
	}, nil
}

// EnsureDirs creates the Lemon directory tree.
func (l Layout) EnsureDirs() error {
	for _, d := range []string{l.Dir, l.QueueDir, l.TransferIn} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return fmt.Errorf("cannot create %s: %w", d, err)
		}
	}
	return nil
}

// Err reports the first directory that could not be created.
func (l Layout) Err() error { return l.EnsureDirs() }

// Load reads config.json from dir, returning defaults when absent.
func Load(l Layout) (*Config, error) {
	cfg := &Config{Version: Version, Port: DefaultPort, Peers: map[string]*Peer{}}

	data, err := os.ReadFile(l.ConfigFile)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return nil, fmt.Errorf("cannot read %s: %w", l.ConfigFile, err)
	}
	if len(data) == 0 {
		return cfg, nil
	}
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("cannot parse %s: %w", l.ConfigFile, err)
	}
	if cfg.Peers == nil {
		cfg.Peers = map[string]*Peer{}
	}
	if cfg.Port <= 0 || cfg.Port > 65535 {
		cfg.Port = DefaultPort
	}
	if cfg.Version == 0 {
		cfg.Version = Version
	}
	return cfg, nil
}

// Save writes config.json atomically with owner-only permissions.
func (c *Config) Save(l Layout) error {
	if c.Peers == nil {
		c.Peers = map[string]*Peer{}
	}
	if c.Port <= 0 || c.Port > 65535 {
		return fmt.Errorf("invalid port: %d", c.Port)
	}
	if c.Version == 0 {
		c.Version = Version
	}
	if err := os.MkdirAll(l.Dir, 0o700); err != nil {
		return fmt.Errorf("cannot create %s: %w", l.Dir, err)
	}

	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return fmt.Errorf("cannot encode config: %w", err)
	}
	data = append(data, '\n')

	tmp, err := os.CreateTemp(l.Dir, ".config-*.json")
	if err != nil {
		return fmt.Errorf("cannot write config: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return fmt.Errorf("cannot set config permissions: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("cannot write config: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("cannot flush config: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("cannot close config: %w", err)
	}
	if err := os.Rename(tmpName, l.ConfigFile); err != nil {
		return fmt.Errorf("cannot install config: %w", err)
	}
	return syncDir(l.Dir)
}

// syncDir flushes a directory entry so a rename survives a crash.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return nil
	}
	defer d.Close()
	// Directory fsync is unsupported on some filesystems; ignore that case.
	_ = d.Sync()
	return nil
}

// Configured reports whether the username has been set.
func (c *Config) Configured() bool { return c.Username != "" }

// RequireConfigured returns ErrNotConfigured when setup has not been run.
func (c *Config) RequireConfigured() error {
	if !c.Configured() {
		return ErrNotConfigured
	}
	return nil
}

// PeerNames returns configured peer names in sorted order.
func (c *Config) PeerNames() []string {
	names := make([]string, 0, len(c.Peers))
	for n := range c.Peers {
		names = append(names, n)
	}
	sortStrings(names)
	return names
}

// Peer returns the named peer.
func (c *Config) Peer(name string) (*Peer, bool) {
	p, ok := c.Peers[name]
	return p, ok
}

// SetPeer adds or replaces a peer, enforcing the peer limit and name rules.
func (c *Config) SetPeer(name, addr string) error {
	name = strings.TrimSpace(name)
	if err := ValidateName(name, "peer name"); err != nil {
		return err
	}
	if addr != "" {
		if err := ValidateAddr(addr); err != nil {
			return err
		}
	}
	if _, exists := c.Peers[name]; !exists && len(c.Peers) >= MaxPeers {
		return fmt.Errorf("%w: lemon supports at most %d peers, remove one with: lemon peer rm <name>", ErrPeerLimit, MaxPeers)
	}
	if c.Peers == nil {
		c.Peers = map[string]*Peer{}
	}
	c.Peers[name] = &Peer{Addr: addr}
	return nil
}

// RemovePeer deletes a peer, clearing the default if it pointed there.
func (c *Config) RemovePeer(name string) bool {
	if _, ok := c.Peers[name]; !ok {
		return false
	}
	delete(c.Peers, name)
	if c.DefaultPeer == name {
		c.DefaultPeer = ""
	}
	return true
}

// Addr returns the listen address for this config.
func (c *Config) Addr() string { return ":" + strconv.Itoa(c.Port) }

// ListenHost returns the host portion for status output.
func (c *Config) ListenHost() string { return "0.0.0.0" }

// ValidateName checks a username or peer name for safe, printable content.
func ValidateName(name, kind string) error {
	if name == "" {
		return fmt.Errorf("%s must not be empty", kind)
	}
	if len(name) > MaxPeerNameLen {
		return fmt.Errorf("%s %q is too long (max %d characters)", kind, name, MaxPeerNameLen)
	}
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '-', r == '_', r == '.':
		default:
			return fmt.Errorf("%s %q may only contain letters, digits, '-', '_' and '.'", kind, name)
		}
	}
	if strings.HasPrefix(name, ".") {
		return fmt.Errorf("%s %q must not start with '.'", kind, name)
	}
	return nil
}

// ValidateAddr checks a host:port pair.
func ValidateAddr(addr string) error {
	host, port, err := SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("invalid address %q: %w", addr, err)
	}
	if host == "" {
		return fmt.Errorf("invalid address %q: missing host", addr)
	}
	n, err := strconv.Atoi(port)
	if err != nil || n <= 0 || n > 65535 {
		return fmt.Errorf("invalid address %q: bad port", addr)
	}
	return nil
}

// SplitHostPort splits addr, tolerating a missing port by assuming DefaultPort.
func SplitHostPort(addr string) (host, port string, err error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return "", "", errors.New("empty address")
	}
	idx := strings.LastIndex(addr, ":")
	if idx < 0 {
		return strings.Trim(addr, "[]"), strconv.Itoa(DefaultPort), nil
	}
	host = strings.Trim(addr[:idx], "[]")
	port = addr[idx+1:]
	if host == "" {
		return "", "", errors.New("missing host")
	}
	return host, port, nil
}

// NormalizeAddr validates addr and returns it with an explicit port.
func NormalizeAddr(addr string) (string, error) {
	if err := ValidateAddr(addr); err != nil {
		return "", err
	}
	host, port, _ := SplitHostPort(addr)
	return host + ":" + port, nil
}

// WithPort returns host joined to port, bracketing IPv6 literals.
func WithPort(host string, port int) string {
	return net.JoinHostPort(host, strconv.Itoa(port))
}

// sortStrings is a tiny insertion sort to keep imports minimal in hot paths.
func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// Store is a Config paired with its Layout, guarded for concurrent use.
type Store struct {
	mu   sync.Mutex
	l    Layout
	cfg  *Config
	once bool
}

// NewStore loads configuration for the given layout.
func NewStore(l Layout) (*Store, error) {
	cfg, err := Load(l)
	if err != nil {
		return nil, err
	}
	return &Store{l: l, cfg: cfg}, nil
}

// Layout returns the store's directory layout.
func (s *Store) Layout() Layout { return s.l }

// Get returns the current config. Callers must not mutate it.
func (s *Store) Get() *Config {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cfg
}

// Update mutates the config under lock and persists the result.
func (s *Store) Update(fn func(*Config) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := fn(s.cfg); err != nil {
		return err
	}
	return s.cfg.Save(s.l)
}

// Reload re-reads config.json from disk.
func (s *Store) Reload() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cfg, err := Load(s.l)
	if err != nil {
		return err
	}
	s.cfg = cfg
	return nil
}
