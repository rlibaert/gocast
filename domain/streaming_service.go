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

// StreamPub is a published stream.
// Any [StreamPub] may be subscribed hence the [StreamPub.AsSub] method.
type StreamPub string

// StreamSub is a subscribable stream served to clients.
// A [StreamSub] may be an alias of a [StreamPub].
type StreamSub string

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

// streamingPubsub wraps an [internal.Pubsub] to buffer data and burst some when a new subscriber connects.
type streamingPubsub struct {
	internal.Pubsub

	bufferingSince time.Time
	buffer         *[]byte
	burst          atomic.Pointer[[]byte]
}

func newStreamingPubsub() internal.Pubsub {
	ps := streamingPubsub{
		Pubsub:         internal.NewPubsub(),
		bufferingSince: time.Now(),
		buffer:         new([]byte),
	}
	ps.burst.Store(new([]byte))
	return &ps
}

func (ps *streamingPubsub) Write(p []byte) (int, error) {
	*ps.buffer = append(*ps.buffer, p...)
	if time.Since(ps.bufferingSince) < 3*time.Second {
		return len(p), nil
	}

	_, err := ps.Pubsub.Write(*ps.buffer)
	ps.bufferingSince = time.Now()
	ps.buffer = ps.burst.Swap(ps.buffer)
	*ps.buffer = (*ps.buffer)[:0]

	return len(p), err
}

func (ps *streamingPubsub) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(*ps.burst.Load())
	if err != nil {
		return int64(n), err
	}
	m, err := ps.Pubsub.WriteTo(w)
	return int64(n) + m, err
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
