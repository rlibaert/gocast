package domain

import (
	"context"
	"errors"
	"io"
	"maps"
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

type StreamsService interface {
	Publish(context.Context, StreamPub, io.Reader) (int64, error)
	Subscribe(context.Context, StreamSub, io.Writer) (int64, error)

	streamsMap() map[StreamSub]StreamPub
}

func StreamsServiceStreamsMap(svc StreamsService) map[StreamSub]StreamPub {
	return svc.streamsMap()
}

type StreamsServiceHooks struct {
	PublishStart   func(ctx context.Context, s StreamPub)
	PublishStop    func(ctx context.Context, s StreamPub, start time.Time)
	SubscribeStart func(ctx context.Context, s StreamSub)
	SubscribeStop  func(ctx context.Context, s StreamSub, start time.Time)
}

func NewStreamsService(
	hooks StreamsServiceHooks,
	debounce time.Duration,
) StreamsService {
	var svc StreamsService = &streamsService{
		hooks:          hooks,
		streamsMapping: map[StreamSub]StreamPub{},
	}
	svc = debouncedStreamsService{svc, debounce}
	return svc
}

type streamsService struct {
	hooks StreamsServiceHooks

	mu             sync.RWMutex
	streamsMapping map[StreamSub]StreamPub
	streamsPubsub  sync.Map
}

func (svc *streamsService) Publish(ctx context.Context, s StreamPub, r io.Reader) (int64, error) {
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
				ps, _ = svc.streamsPubsub.LoadOrStore(sub, newStreamsPubsub())
			}

			ps.(internal.Pubsub).Write(p) //nolint: errcheck,gosec // always valid and never fails
		}

		return len(p), nil
	})

	svc.hooks.PublishStart(ctx, s)
	defer svc.hooks.PublishStop(ctx, s, time.Now())
	return io.Copy(w, r)
}

func (svc *streamsService) Subscribe(ctx context.Context, s StreamSub, w io.Writer) (int64, error) {
	w = internal.ContextWriter{Context: ctx, Writer: w}

	switch ps, loaded := svc.streamsPubsub.Load(s); {
	case !loaded:
		return 0, ErrStreamNotFound
	case ps == nil:
		return 0, ErrStreamNotAvailable
	default:
		svc.hooks.SubscribeStart(ctx, s)
		defer svc.hooks.SubscribeStop(ctx, s, time.Now())
		return ps.(internal.Pubsub).WriteTo(w) //nolint: errcheck // always valid
	}
}

func (svc *streamsService) streamsMap() map[StreamSub]StreamPub {
	svc.mu.RLock()
	defer svc.mu.RUnlock()
	return maps.Clone(svc.streamsMapping)
}

// streamsPubsub wraps an [internal.Pubsub] to buffer writes and burst data when a new subscriber connects.
type streamsPubsub struct {
	internal.Pubsub

	index  int64     // the currently written chunk
	chunks [4][]byte // time-constant-ish data chunks
	start  time.Time // when the current chunk write started
}

func newStreamsPubsub() internal.Pubsub {
	return &streamsPubsub{Pubsub: internal.NewPubsub()}
}

// Write buffers the data in ringed chunks of roughly equal durations.
func (ps *streamsPubsub) Write(p []byte) (int, error) {
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
func (ps *streamsPubsub) WriteTo(w io.Writer) (int64, error) {
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

type debouncedStreamsService struct {
	StreamsService

	duration time.Duration
}

func (svc debouncedStreamsService) Publish(ctx context.Context, s StreamPub, r io.Reader) (int64, error) {
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

	m, err := svc.StreamsService.Publish(ctx, s, r)
	return n + m, err
}
