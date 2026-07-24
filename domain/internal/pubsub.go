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
//
// It uses pooling and reference counting to share data buffers among subscribers.
// When a new buffer is written, it sends it to all current subscribers.
// So subscribers can receive data immediately upon subscription, the publisher
// keeps references of recent buffers. Sending more than just the buffer in
// flight ensures subscribers always have enough data chunks to process
// (as long as the production & consumption rates are equal).
type pubsub struct {
	refbufs sync.Pool

	mu      sync.Mutex
	closed  bool
	recents []*refbuf
	oldest  int
	subs    []chan<- *refbuf
}

// NewPubsub returns a new [Pubsub].
// The burst argument specifies the number of extra writes to do upon subscription.
func NewPubsub(burst int) Pubsub {
	ps := new(pubsub{
		refbufs: sync.Pool{
			New: func() any { return new(refbuf) },
		},
		recents: make([]*refbuf, 1+burst),
	})
	for i := range ps.recents {
		ps.recents[i] = ps.refbuf(nil, 1)
	}
	return ps
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

func (ps *pubsub) Write(p []byte) (int, error) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if ps.closed {
		return 0, errPubsubClosed
	}

	ps.unref(ps.recents[ps.oldest])
	ps.recents[ps.oldest] = ps.refbuf(p, 1+uint64(len(ps.subs)))
	for _, ch := range ps.subs {
		select {
		case ch <- ps.recents[ps.oldest]:
		default:
			ps.unref(ps.recents[ps.oldest])
		}
	}
	ps.oldest = (ps.oldest + 1) % len(ps.recents)

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

	ch := make(
		chan *refbuf,
		max(8, len(ps.recents)+4), //nolint: mnd // fit recent writes and have some room for slow writers
	)
	for _, rb := range append(ps.recents[ps.oldest:], ps.recents[:ps.oldest]...) {
		ch <- rb
		atomic.AddUint64(&rb.n, 1)
	}

	ps.subs = append(ps.subs, ch)
	defer func() {
		// fast unordered delete
		if i := slices.Index(ps.subs, ch); i != -1 {
			ps.subs[i], ps.subs[len(ps.subs)-1] = ps.subs[len(ps.subs)-1], nil
			ps.subs = ps.subs[:len(ps.subs)-1]
			close(ch)
		}
		for rb := range ch {
			ps.unref(rb)
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
