package internal

import (
	"errors"
	"io"
	"sync"
	"sync/atomic"
)

// Pubsub is a publisher-subcribers communication interface.
type Pubsub interface {
	io.WriteCloser // publisher
	io.WriterTo    // subscribers
}

var ErrPubsubClosed = errors.New("domain: pubsub closed")

// pubsub implements [Pubsub].
type pubsub struct {
	mu     sync.Mutex
	closed bool
	subs   map[chan<- *refbuf]struct{}
	subsWg sync.WaitGroup

	refbufs sync.Pool
}

// refbuf is a shared buffer with a reference counter.
type refbuf struct {
	b []byte
	n int64
}

func NewPubsub() Pubsub {
	return new(pubsub{
		subs: map[chan<- *refbuf]struct{}{},
		refbufs: sync.Pool{
			New: func() any { return new(refbuf) },
		},
	})
}

func (ps *pubsub) Write(p []byte) (int, error) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if ps.closed {
		return 0, ErrPubsubClosed
	}

	if len(ps.subs) != 0 {
		rb := ps.refbufs.Get().(*refbuf) //nolint: errcheck // always a non-nil [*refbuf]
		rb.b = append(rb.b[:0], p...)
		rb.n = int64(len(ps.subs))
		for ch := range ps.subs {
			ch <- rb
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

	for ch := range ps.subs {
		close(ch)
		delete(ps.subs, ch)
	}

	return nil
}

func (ps *pubsub) WriteTo(w io.Writer) (int64, error) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if ps.closed {
		return 0, ErrPubsubClosed
	}

	const refbufQueueSize = 16
	ch := make(chan *refbuf, refbufQueueSize)
	ps.subs[ch] = struct{}{}
	ps.subsWg.Add(1)

	ps.mu.Unlock()
	defer func() {
		ps.mu.Lock()

		if _, exist := ps.subs[ch]; exist {
			close(ch)
			delete(ps.subs, ch)
		}
		ps.subsWg.Done()
	}()

	n := int64(0)
	for rb := range ch {
		wn, werr := w.Write(rb.b)
		if atomic.AddInt64(&rb.n, -1) == 0 {
			ps.refbufs.Put(rb)
		}
		n += int64(wn)
		if werr != nil {
			return n, werr
		}
	}

	return n, nil
}
