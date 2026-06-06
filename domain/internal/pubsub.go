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

var ErrPubsubClosed = errors.New("domain: pubsub closed")

// refbuf is a shared buffer with a reference counter.
type refbuf struct {
	b []byte
	n uint64
}

// pubsub implements [Pubsub].
type pubsub struct {
	mu     sync.Mutex
	closed bool
	subs   []chan<- *refbuf
	subsWg sync.WaitGroup

	refbufs sync.Pool
}

func NewPubsub() Pubsub {
	return new(pubsub{
		refbufs: sync.Pool{
			New: func() any { return new(refbuf) },
		},
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

func (ps *pubsub) Write(p []byte) (int, error) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if ps.closed {
		return 0, ErrPubsubClosed
	}

	if len(ps.subs) != 0 {
		rb := ps.refbuf(p, uint64(len(ps.subs)))
		for _, ch := range ps.subs {
			select {
			case ch <- rb:
			default:
				ps.unref(rb)
			}
		}
	}

	return len(p), nil
}

func (ps *pubsub) Close() error {
	defer ps.subsWg.Wait()

	ps.mu.Lock()
	defer ps.mu.Unlock()

	if ps.closed {
		return ErrPubsubClosed
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
		return 0, ErrPubsubClosed
	}

	const refbufQueueSize = 8
	ch := make(chan *refbuf, refbufQueueSize)
	ps.subs = append(ps.subs, ch)
	ps.subsWg.Add(1)

	ps.mu.Unlock()
	defer func() {
		ps.mu.Lock()

		if i := slices.Index(ps.subs, ch); i != -1 {
			close(ch)
			ps.subs[i], ps.subs[len(ps.subs)-1] = ps.subs[len(ps.subs)-1], nil
			ps.subs = ps.subs[:len(ps.subs)-1]
		}
		ps.subsWg.Done()
	}()

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
