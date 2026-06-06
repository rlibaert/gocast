package domain

import (
	"bytes"
	"context"
	"errors"
	"io"
	"iter"
	"maps"
	"slices"
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

	resetFallbacks(map[StreamSub][]StreamPub)
	streamSubTitle(StreamSub) *string
	streamsMap() map[StreamSub]StreamPub
}

func ServiceResetFallbacks(svc Service, m map[StreamSub][]StreamPub) {
	svc.resetFallbacks(m)
}

func ServiceStreamSubTitle(svc Service, s StreamSub) *string {
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
		hooks:     hooks,
		wirings:   map[StreamPub][]StreamSub{},
		fallbacks: map[StreamSub][]StreamPub{},
	}
	if debounce > 0 {
		svc = serviceDebounced{svc, debounce}
	}
	return svc
}

type service struct {
	hooks ServiceHooks

	mu        sync.RWMutex
	wirings   map[StreamPub][]StreamSub
	fallbacks map[StreamSub][]StreamPub

	streamsPubsub sync.Map
	streamsTitle  sync.Map
}

// rewire reassigns subs to their best [StreamPub] available.
// It then tries to wire subs in the sequence, ignoring those already wired
// or not yet wireable and creating or closing according [*pubsub].
func (svc *service) rewire(try iter.Seq[StreamSub]) {
	var rewire []StreamSub
	for pub, subs := range svc.wirings {
		rewire = append(rewire, subs...)
		svc.wirings[pub] = nil
	}

rewiring:
	for _, sub := range rewire {
		for _, pub := range svc.fallbacks[sub] {
			if _, avail := svc.wirings[pub]; avail {
				svc.wirings[pub] = append(svc.wirings[pub], sub)
				title, _ := svc.streamsTitle.Load(pub.AsSub())
				svc.streamsTitle.Store(sub, title)
				continue rewiring
			}
		}

		svc.streamsTitle.Delete(sub)
		if v, ok := svc.streamsPubsub.LoadAndDelete(sub); ok {
			v.(*pubsub).Close()
		}
	}

wiring:
	for sub := range try {
		if slices.Contains(rewire, sub) {
			continue
		}

		for _, pub := range svc.fallbacks[sub] {
			if _, avail := svc.wirings[pub]; avail {
				svc.wirings[pub] = append(svc.wirings[pub], sub)
				title, _ := svc.streamsTitle.Load(pub.AsSub())
				svc.streamsTitle.Store(sub, title)
				svc.streamsPubsub.LoadOrStore(sub, &pubsub{Pubsub: internal.NewPubsub()})
				continue wiring
			}
		}

		svc.streamsTitle.Delete(sub)
		if v, ok := svc.streamsPubsub.LoadAndDelete(sub); ok {
			v.(*pubsub).Close()
		}
	}
}

func (svc *service) resetFallbacks(m map[StreamSub][]StreamPub) {
	svc.mu.Lock()
	defer svc.mu.Unlock()

	svc.fallbacks = make(map[StreamSub][]StreamPub, len(m))
	for sub, pubs := range m {
		svc.fallbacks[sub] = slices.Clone(pubs)
	}

	svc.rewire(maps.Keys(svc.fallbacks))
}

func (svc *service) Publish(ctx context.Context, s StreamPub, r io.Reader) (int64, error) {
	svc.mu.Lock()
	defer svc.mu.Unlock()

	if _, exists := svc.wirings[s]; exists {
		return 0, ErrStreamExists
	}

	pubsub := &pubsub{Pubsub: internal.NewPubsub()}
	defer pubsub.Close()

	svc.streamsPubsub.Store(s.AsSub(), pubsub)
	svc.wirings[s] = nil
	svc.rewire(maps.Keys(svc.fallbacks))
	defer func() {
		svc.streamsTitle.Delete(s.AsSub())
		svc.streamsPubsub.Delete(s.AsSub())
		subs := svc.wirings[s]
		delete(svc.wirings, s)
		svc.rewire(slices.Values(subs))
	}()

	svc.mu.Unlock()
	defer svc.mu.Lock()

	svc.hooks.PublishStart(ctx, s)
	defer svc.hooks.PublishStop(ctx, s, time.Now())
	return svc.publish(s, internal.ContextReader{Context: ctx, Reader: r})
}

// publish copies from r to every [StreamSub] mapped to [StreamPub].
func (svc *service) publish(s StreamPub, r io.Reader) (int64, error) {
	w := internal.FuncWriter(func(p []byte) (int, error) {
		svc.mu.RLock()
		defer svc.mu.RUnlock()

		for _, sub := range append(svc.wirings[s], s.AsSub()) {
			ps, _ := svc.streamsPubsub.Load(sub)
			ps.(internal.Pubsub).Write(p) //nolint: errcheck,gosec // always valid and never fails
		}

		return len(p), nil
	})

	return StreamCopy(w, r)
}

// StreamCopy is the function used to copy stream data.
// It can be customized to control how data is copied.
var StreamCopy = io.Copy

func (svc *service) Subscribe(ctx context.Context, s StreamSub, w io.Writer) (int64, error) {
	ps, loaded := svc.streamsPubsub.Load(s)
	if !loaded {
		return 0, ErrStreamNotFound
	}

	svc.hooks.SubscribeStart(ctx, s)
	defer svc.hooks.SubscribeStop(ctx, s, time.Now())
	return ps.(internal.Pubsub).WriteTo(internal.ContextWriter{Context: ctx, Writer: w}) //nolint: errcheck // always valid
}

func (svc *service) PublishTitle(_ context.Context, s StreamPub, title string) error {
	svc.mu.RLock()
	defer svc.mu.RUnlock()

	subs, exists := svc.wirings[s]
	if !exists {
		return ErrStreamNotFound
	}

	svc.streamsTitle.Store(s.AsSub(), &title)
	for _, sub := range subs {
		svc.streamsTitle.Store(sub, &title)
	}

	return nil
}

func (svc *service) streamSubTitle(s StreamSub) *string {
	if v, _ := svc.streamsTitle.Load(s); v != nil {
		return v.(*string)
	}
	return nil
}

func (svc *service) streamsMap() map[StreamSub]StreamPub {
	svc.mu.RLock()
	defer svc.mu.RUnlock()

	m := make(map[StreamSub]StreamPub)
	for pub, subs := range svc.wirings {
		m[pub.AsSub()] = pub
		for _, sub := range subs {
			m[sub] = pub
		}
	}

	return m
}

type serviceDebounced struct {
	Service

	duration time.Duration
}

func (svc serviceDebounced) Publish(ctx context.Context, s StreamPub, r io.Reader) (int64, error) {
	type stopError struct{ error }

	t := time.Now().Add(svc.duration)
	b := bytes.NewBuffer(nil)
	w := internal.FuncWriter(func(p []byte) (int, error) {
		if time.Now().After(t) {
			return 0, stopError{}
		}
		return b.Write(p)
	})

	n, err := io.Copy(w, r)
	if err != error(stopError{}) { //nolint: errorlint // function-scoped error type returned right above
		return n, err
	}
	b, r = nil, io.MultiReader(b, r)

	m, err := svc.Service.Publish(ctx, s, r)
	return n + m, err
}
