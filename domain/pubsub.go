package domain

import (
	"errors"
	"io"
	"sync/atomic"
	"time"

	"github.com/rlibaert/gocast/domain/internal"
)

// pubsub wraps an [internal.Pubsub] to buffer writes and burst data when a new subscriber connects.
type pubsub struct {
	internal.Pubsub

	index  int64     // the currently written chunk
	chunks [4][]byte // time-constant-ish data chunks
	start  time.Time // when the current chunk write started
}

// Write buffers the data in ringed chunks of roughly equal durations.
func (ps *pubsub) Write(p []byte) (int, error) {
	ps.chunks[ps.index] = append(ps.chunks[ps.index], p...)
	if time.Since(ps.start) < 2*time.Second {
		return len(p), nil
	}

	_, err := ps.Pubsub.Write(ps.chunks[ps.index])

	atomic.StoreInt64(&ps.index, (ps.index+1)%int64(len(ps.chunks)))
	ps.chunks[ps.index] = ps.chunks[ps.index][:0]
	ps.start = time.Now()

	return len(p), err
}

// Close flushes buffered data and closes the underlying [internal.Pubsub].
func (ps *pubsub) Close() error {
	_, err := ps.Pubsub.Write(ps.chunks[ps.index])
	return errors.Join(ps.Pubsub.Close(), err)
}

// WriteTo starts by writing the buffered data chunks, excepted the current
// and the next to be (for clearance, meaning that we need at least 3 chunks).
func (ps *pubsub) WriteTo(w io.Writer) (int64, error) {
	var n int64

	index := atomic.LoadInt64(&ps.index)
	for _, buf := range append(ps.chunks[index:], ps.chunks[:index]...)[2:] {
		wn, err := w.Write(buf)
		n += int64(wn)
		if err != nil {
			return n, err
		}
	}

	m, err := ps.Pubsub.WriteTo(w)
	return n + m, err
}
