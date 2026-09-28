package protocol

import "io"

// countingWriter forwards writes, reports cumulative progress in coarse
// chunks, and refreshes an idle deadline as bytes keep flowing.
type countingWriter struct {
	w        io.Writer
	progress func(int64)
	n        int64
	step     int64
	last     int64
	// touch, if set, is called often enough to keep a long transfer from
	// tripping the idle deadline.
	touch func()
	// touchBytes counts bytes between touch calls; one syscall per chunk is
	// cheap next to the network cost of the chunk.
	touchBytes int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	if n > 0 {
		c.n += int64(n)
		if c.progress != nil {
			if c.step == 0 {
				c.last = c.n
				c.step = 256 << 10 // 256 KiB
			} else if c.n-c.last >= c.step {
				c.last = c.n
				c.progress(c.n)
			}
		}
		if c.touch != nil {
			c.touchBytes += int64(n)
			if c.touchBytes >= 4<<20 { // 4 MiB
				c.touchBytes = 0
				c.touch()
			}
		}
	}
	return n, err
}

// deadlineReader refreshes an idle deadline as data keeps arriving, so a slow
// but healthy stream is never mistaken for a stalled one.
type deadlineReader struct {
	r        io.Reader
	reset    func()
	seen     int64
	interval int64
}

func (d *deadlineReader) Read(p []byte) (int, error) {
	n, err := d.r.Read(p)
	if n > 0 {
		d.seen += int64(n)
		if d.reset != nil && d.seen >= d.interval {
			d.seen = 0
			d.reset()
		}
	}
	return n, err
}
