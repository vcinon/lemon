package serve

import "sync"

// workSet remembers the queued entries currently being delivered.
//
// A queued entry is delivered twice in a way that is easy to miss: the command
// that created it also delivers it inline, while the retry loop delivers
// whatever is still pending. Both can pick the same entry, and a transfer is not
// safe to run twice — the receiver appends both attempts into the same staging
// file, so the result is a file that is too long and fails its checksum, or a
// staging file that one attempt commits out from under the other.
//
// Marking the entry before the work starts, and clearing it afterwards, makes
// the two paths mutually exclusive.
type workSet struct {
	mu sync.Mutex
	m  map[string]bool
}

// begin claims id. It returns false when the entry is already in flight, which
// tells the caller to leave it alone.
func (w *workSet) begin(id string) bool {
	if id == "" {
		return true
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.m == nil {
		w.m = make(map[string]bool)
	}
	if w.m[id] {
		return false
	}
	w.m[id] = true
	return true
}

// end releases the claim taken by begin.
func (w *workSet) end(id string) {
	if id == "" {
		return
	}
	w.mu.Lock()
	delete(w.m, id)
	w.mu.Unlock()
}

// inFlight reports whether id is currently claimed.
func (w *workSet) inFlight(id string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.m[id]
}
