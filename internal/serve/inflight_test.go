package serve

import (
	"testing"
)

// TestWorkSetExcludesDoubleDelivery covers the guard that keeps a queued entry
// from being delivered twice.
//
// The bug it prevents is not hypothetical: a transfer was delivered by the
// command that created it and again by the retry loop a couple of seconds
// later, both appending into the same staging file on the receiver. The
// resulting file was longer than the source and failed its checksum, and the
// command reported success.
func TestWorkSetExcludesDoubleDelivery(t *testing.T) {
	var w workSet

	if !w.begin("a") {
		t.Fatal("first claim on a free entry should succeed")
	}
	if w.begin("a") {
		t.Error("a second claim on an in-flight entry should be refused")
	}
	if !w.inFlight("a") {
		t.Error("inFlight should report a claimed entry")
	}

	// A different entry is unaffected.
	if !w.begin("b") {
		t.Error("a different entry should be claimable while another is in flight")
	}

	w.end("a")
	if w.inFlight("a") {
		t.Error("inFlight should be false after the claim is released")
	}
	if !w.begin("a") {
		t.Error("an entry should be claimable again once released")
	}
}

// TestWorkSetIgnoresEmptyID documents that a missing id is never treated as an
// in-flight entry, which would otherwise wedge every later delivery.
func TestWorkSetIgnoresEmptyID(t *testing.T) {
	var w workSet
	if !w.begin("") {
		t.Fatal("an empty id should be claimable")
	}
	if !w.begin("") {
		t.Error("an empty id should not be treated as in flight")
	}
	w.end("")
	if w.inFlight("") {
		t.Error("inFlight on an empty id should be false")
	}
}

// TestWorkSetIsConcurrencySafe exercises the guard the way the daemon does:
// many goroutines racing to claim the same entry, exactly one of which may win.
func TestWorkSetIsConcurrencySafe(t *testing.T) {
	var w workSet
	const racers = 32

	results := make(chan bool, racers)
	start := make(chan struct{})
	for i := 0; i < racers; i++ {
		go func() {
			<-start
			results <- w.begin("job")
		}()
	}
	close(start)

	wins := 0
	for i := 0; i < racers; i++ {
		if <-results {
			wins++
		}
	}
	if wins != 1 {
		t.Errorf("%d goroutines claimed the same entry, want exactly 1", wins)
	}
}
