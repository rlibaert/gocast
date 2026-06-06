package domain

import (
	"context"
	"errors"
	"io"
	"maps"
	"sync"
	"time"

	"github.com/rlibaert/gocast/domain/internal"
)

var (
	ErrStreamExists   = errors.New("domain: stream exists")
	ErrStreamNotFound = errors.New("domain: stream not found")
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

type Service interface {
	Publish(context.Context, StreamPub, io.Reader) (int64, error)
	Subscribe(context.Context, StreamSub, io.Writer) (int64, error)
	PublishTitle(context.Context, StreamPub, string) error

	streamSubTitle(StreamSub) (string, bool)
	streamsMap() map[StreamSub]StreamPub
}

func ServiceStreamSubTitle(svc Service, s StreamSub) (string, bool) {
	return svc.streamSubTitle(s)
}

func ServiceStreamsMap(svc Service) map[StreamSub]StreamPub {
	return svc.streamsMap()
}

type ServiceHooks struct {
	PublishStart   func(ctx context.Context, s StreamPub)
	PublishStop    func(ctx context.Context, s StreamPub, start time.Time)
	SubscribeStart func(ctx context.Context, s StreamSub)
	SubscribeStop  func(ctx context.Context, s StreamSub, start time.Time)
}

func NewService(
	hooks ServiceHooks,
	debounce time.Duration,
) Service {
	var svc Service = &service{
		hooks:          hooks,
		streamsMapping: map[StreamSub]StreamPub{},
	}
	svc = serviceDebounced{svc, debounce}
	return svc
}

type service struct {
	hooks ServiceHooks

	mu             sync.RWMutex
	streamsMapping map[StreamSub]StreamPub
	streamsPubsub  sync.Map
	streamsTitle   sync.Map
}

func (svc *service) Publish(ctx context.Context, s StreamPub, r io.Reader) (int64, error) {
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
			ps.(internal.Pubsub).Write(p) //nolint: errcheck,gosec // always valid and never fails
		}

		return len(p), nil
	})

	svc.streamsPubsub.LoadOrStore(s.AsSub(), &pubsub{Pubsub: internal.NewPubsub()})

	svc.hooks.PublishStart(ctx, s)
	defer svc.hooks.PublishStop(ctx, s, time.Now())
	return io.Copy(w, r)
}

func (svc *service) Subscribe(ctx context.Context, s StreamSub, w io.Writer) (int64, error) {
	w = internal.ContextWriter{Context: ctx, Writer: w}

	ps, loaded := svc.streamsPubsub.Load(s)
	if !loaded {
		return 0, ErrStreamNotFound
	}

	svc.hooks.SubscribeStart(ctx, s)
	defer svc.hooks.SubscribeStop(ctx, s, time.Now())
	return ps.(internal.Pubsub).WriteTo(w) //nolint: errcheck // always valid
}

func (svc *service) PublishTitle(ctx context.Context, s StreamPub, title string) error {
	svc.mu.RLock()
	defer svc.mu.RUnlock()

	err := ErrStreamNotFound
	for sub, pub := range svc.streamsMapping {
		if pub != s {
			continue
		}
		err = nil

		svc.streamsTitle.Store(sub, title)
	}

	return err
}

func (svc *service) streamSubTitle(s StreamSub) (string, bool) {
	v, ok := svc.streamsTitle.Load(s)
	if !ok {
		return "", false
	}

	return v.(string), true
}

func (svc *service) streamsMap() map[StreamSub]StreamPub {
	svc.mu.RLock()
	defer svc.mu.RUnlock()
	return maps.Clone(svc.streamsMapping)
}

type serviceDebounced struct {
	Service

	duration time.Duration
}

func (svc serviceDebounced) Publish(ctx context.Context, s StreamPub, r io.Reader) (int64, error) {
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

	m, err := svc.Service.Publish(ctx, s, r)
	return n + m, err
}
