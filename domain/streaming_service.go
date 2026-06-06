package domain

import (
	"context"
	"errors"
	"io"
	"sync"
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

	return io.Copy(internal.FuncWriter(func(p []byte) (int, error) {
		svc.mu.RLock()
		defer svc.mu.RUnlock()

		for sub, pub := range svc.streamsMapping {
			if pub != s {
				continue
			}

			ps, _ := svc.streamsPubsub.Load(sub)
			if ps == nil {
				ps, _ = svc.streamsPubsub.LoadOrStore(sub, internal.NewPubsub())
			}

			ps.(internal.Pubsub).Write(p) //nolint: errcheck,gosec // always valid and never fails
		}

		return len(p), nil
	}), r)
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
