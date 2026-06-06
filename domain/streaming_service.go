package domain

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rlibaert/gocast/domain/internal"
)

var (
	ErrStreamExists       = errors.New("domain: stream exists")
	ErrStreamNotFound     = errors.New("domain: stream not found")
	ErrStreamNotAvailable = errors.New("domain: stream not available")
)

type (
	// StreamPub is a published stream.
	// Any [StreamPub] may be subscribed hence the [StreamPub.AsSub] method.
	StreamPub string
	// StreamSub is a subscribable stream served to clients.
	// A [StreamSub] may be an alias of a [StreamPub].
	StreamSub string
)

// AsSub implements the subscribable nature of published streams.
func (s StreamPub) AsSub() StreamSub { return StreamSub(s) }

type StreamingService interface {
	Publish(context.Context, StreamPub, io.Reader) (int64, error)
	Subscribe(context.Context, StreamSub, io.Writer) (int64, error)
}

func NewStreamingService(debounce time.Duration) StreamingService {
	var svc StreamingService = &streamingService{
		streamsMapping: map[StreamSub]StreamPub{},
	}
	svc = debouncedStreamingService{svc, debounce}
	return svc
}

type streamingService struct {
	mu             sync.RWMutex
	streamsMapping map[StreamSub]StreamPub
	streamsPubsub  sync.Map
}

func (svc *streamingService) Publish(ctx context.Context, s StreamPub, r io.Reader) (int64, error) {
	r = internal.ContextReader{Context: ctx, Reader: r}

	svc.mu.Lock()
	defer svc.mu.Unlock()

	if svc.streamsMapping[s.AsSub()] == s {
		return 0, ErrStreamExists
	}
	svc.streamsMapping[s.AsSub()] = s

	svc.mu.Unlock()
	defer func() {
		svc.mu.Lock()

		for sub, pub := range svc.streamsMapping {
			if pub != s {
				continue
			}

			svc.streamsMapping[sub] = "foo"
		}
	}()

	w := internal.FuncWriter(func(p []byte) (int, error) {
		svc.mu.RLock()
		defer svc.mu.RUnlock()

		for sub, pub := range svc.streamsMapping {
			if pub != s {
				continue
			}

			ps, _ := svc.streamsPubsub.Load(sub)
			if ps == nil {
				ps, _ = svc.streamsPubsub.LoadOrStore(sub, newStreamingPubsub())
			}

			ps.(internal.Pubsub).Write(p) //nolint: errcheck,gosec // always valid and never fails
		}

		return len(p), nil
	})

	return io.Copy(w, r)
}

func (svc *streamingService) Subscribe(ctx context.Context, s StreamSub, w io.Writer) (int64, error) {
	w = internal.ContextWriter{Context: ctx, Writer: w}

	switch ps, loaded := svc.streamsPubsub.Load(s); {
	case !loaded:
		return 0, ErrStreamNotFound
	case ps == nil:
		return 0, ErrStreamNotAvailable
	default:
		return ps.(internal.Pubsub).WriteTo(w) //nolint: errcheck // always valid
	}
}

// streamingPubsub wraps an [internal.Pubsub] to buffer writes and burst data when a new subscriber connects.
type streamingPubsub struct {
	internal.Pubsub

	index  int64     // the currently written chunk
	chunks [4][]byte // time-constant-ish data chunks
	start  time.Time // when the current chunk write started
}

func newStreamingPubsub() internal.Pubsub {
	return &streamingPubsub{Pubsub: internal.NewPubsub()}
}

// Write buffers the data in ringed chunks of roughly equal durations.
func (ps *streamingPubsub) Write(p []byte) (int, error) {
	ps.chunks[ps.index] = append(ps.chunks[ps.index], p...)
	if time.Since(ps.start) < time.Second {
		return len(p), nil
	}

	_, err := ps.Pubsub.Write(ps.chunks[ps.index])

	atomic.StoreInt64(&ps.index, (ps.index+1)%int64(len(ps.chunks)))
	ps.chunks[ps.index] = ps.chunks[ps.index][:0]
	ps.start = time.Now()

	return len(p), err
}

// WriteTo starts by writing the buffered data chunks, excepted the current
// and the next to be (for clearance, meaning that we need at least 3 chunks).
func (ps *streamingPubsub) WriteTo(w io.Writer) (int64, error) {
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

type debouncedStreamingService struct {
	StreamingService

	duration time.Duration
}

func (svc debouncedStreamingService) Publish(ctx context.Context, s StreamPub, r io.Reader) (int64, error) {
	type stopError struct{ error }

	t := time.Now().Add(svc.duration)
	w := internal.FuncWriter(func(p []byte) (int, error) {
		if time.Now().After(t) {
			return 0, stopError{}
		}
		return len(p), nil
	})

	n, err := io.Copy(w, r)
	if err != error(stopError{}) { //nolint: errorlint // function-scoped error type returned right above
		return n, err
	}

	m, err := svc.StreamingService.Publish(ctx, s, r)
	return n + m, err
}
