package internal

import (
	"errors"
	"io"
	"slices"
	"sync"
	"sync/atomic"
)

// Pubsub is a publisher-subcribers communication interface.
type Pubsub interface {
	io.WriteCloser // publisher
	io.WriterTo    // subscribers
}

var errPubsubClosed = errors.New("pubsub: closed")

// refbuf is a shared buffer with a reference counter.
type refbuf struct {
	b []byte
	n uint64
}

// pubsub implements [Pubsub].
type pubsub struct {
	refbufs sync.Pool

	mu       sync.Mutex
	closed   bool
	previous *refbuf
	inflight *refbuf
	subs     []chan<- *refbuf
}

func NewPubsub() Pubsub {
	return new(pubsub{
		refbufs: sync.Pool{
			New: func() any { return new(refbuf) },
		},
		previous: &refbuf{b: nil, n: 1},
		inflight: &refbuf{b: nil, n: 1},
	})
}

// refbuf returns a pooled [*refbuf] with the given bytes and reference count.
func (ps *pubsub) refbuf(p []byte, n uint64) *refbuf {
	rb := ps.refbufs.Get().(*refbuf) //nolint: errcheck // always a non-nil [*refbuf]
	rb.b = append(rb.b[:0], p...)
	rb.n = n
	return rb
}

// unref atomically decrements the [*refbuf] reference counter
// and puts it back into the pool when the count reaches zero.
func (ps *pubsub) unref(rb *refbuf) {
	if atomic.AddUint64(&rb.n, ^uint64(0)) == 0 {
		ps.refbufs.Put(rb)
	}
}

// replay increments references of recent [*refbuf]s and sends them to the given channel.
func (ps *pubsub) replay(ch chan<- *refbuf) {
	atomic.AddUint64(&ps.previous.n, 1)
	ch <- ps.previous
	atomic.AddUint64(&ps.inflight.n, 1)
	ch <- ps.inflight
}

func (ps *pubsub) Write(p []byte) (int, error) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if ps.closed {
		return 0, errPubsubClosed
	}

	ps.unref(ps.previous)
	ps.previous = ps.inflight
	ps.inflight = ps.refbuf(p, 1+uint64(len(ps.subs)))
	for _, ch := range ps.subs {
		select {
		case ch <- ps.inflight:
		default:
			ps.unref(ps.inflight)
		}
	}

	return len(p), nil
}

func (ps *pubsub) Close() error {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if ps.closed {
		return errPubsubClosed
	}
	ps.closed = true

	for _, ch := range ps.subs {
		close(ch)
	}
	ps.subs = nil

	return nil
}

func (ps *pubsub) WriteTo(w io.Writer) (int64, error) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if ps.closed {
		return 0, errPubsubClosed
	}

	const refbufQueueSize = 8
	ch := make(chan *refbuf, refbufQueueSize)
	ps.replay(ch)
	ps.subs = append(ps.subs, ch)
	defer func() {
		// fast unordered delete
		if i := slices.Index(ps.subs, ch); i != -1 {
			ps.subs[i], ps.subs[len(ps.subs)-1] = ps.subs[len(ps.subs)-1], nil
			ps.subs = ps.subs[:len(ps.subs)-1]
		}
	}()

	ps.mu.Unlock()
	defer ps.mu.Lock()

	n := int64(0)
	for rb := range ch {
		wn, werr := w.Write(rb.b)
		ps.unref(rb)
		n += int64(wn)
		if werr != nil {
			return n, werr
		}
	}

	return n, nil
}
