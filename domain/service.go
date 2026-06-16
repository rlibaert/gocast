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
	PublishStartStop   func(ctx context.Context, s StreamPub) (stop func())
	SubscribeStartStop func(ctx context.Context, s StreamSub) (stop func())
}

// ServiceStreamCopy is the default function for copying stream data.
func ServiceStreamCopy(w io.Writer, r io.Reader) (int64, error) { return io.Copy(w, r) }

func NewService(
	hooks ServiceHooks,
	streamCopy func(io.Writer, io.Reader) (int64, error),
	debounce time.Duration,
) Service {
	var svc Service = &service{
		hooks:      hooks,
		streamCopy: streamCopy,
		wirings:    map[StreamPub][]StreamSub{},
		fallbacks:  map[StreamSub][]StreamPub{},
	}
	if debounce > 0 {
		svc = serviceDebounced{svc, debounce}
	}
	return svc
}

type service struct {
	hooks      ServiceHooks
	streamCopy func(io.Writer, io.Reader) (int64, error)

	mu        sync.RWMutex
	wirings   map[StreamPub][]StreamSub
	fallbacks map[StreamSub][]StreamPub

	streamsPubsub sync.Map
	streamsTitle  sync.Map
}

func (svc *service) newPubsub() *pubsub {
	return newPubsub(3, time.Second) //nolint: mnd // nice defaults (writes every 1s, initial buffer > 3s)
}

// rewire reassigns subs to their best [StreamPub] available.
// It then tries to wire subs in the sequence, ignoring those already wired
// or not yet wireable and creating or closing according [*pubsub].
func (svc *service) rewire(try iter.Seq[StreamSub]) { //nolint: gocognit
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
			v.(internal.Pubsub).Close() //nolint: errcheck,gosec // always valid
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
				svc.streamsPubsub.LoadOrStore(sub, svc.newPubsub())
				continue wiring
			}
		}

		svc.streamsTitle.Delete(sub)
		if v, ok := svc.streamsPubsub.LoadAndDelete(sub); ok {
			v.(internal.Pubsub).Close() //nolint: errcheck,gosec // always valid
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

	pubsub := svc.newPubsub()
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

	defer svc.hooks.PublishStartStop(ctx, s)()
	return svc.streamCopy(internal.WriterContext(ctx, svc.streamPubWriter(s)), r)
}

// streamPubWriter returns an [io.Writer] that writes every [StreamSub] mapped to [StreamPub].
func (svc *service) streamPubWriter(s StreamPub) io.Writer {
	return internal.WriterFunc(func(p []byte) (int, error) {
		svc.mu.RLock()
		defer svc.mu.RUnlock()

		for _, sub := range append(svc.wirings[s], s.AsSub()) {
			ps, _ := svc.streamsPubsub.Load(sub)
			ps.(internal.Pubsub).Write(p) //nolint: errcheck,gosec // always valid and never fails
		}

		return len(p), nil
	})
}

func (svc *service) Subscribe(ctx context.Context, s StreamSub, w io.Writer) (int64, error) {
	ps, loaded := svc.streamsPubsub.Load(s)
	if !loaded {
		return 0, ErrStreamNotFound
	}

	defer svc.hooks.SubscribeStartStop(ctx, s)()
	return ps.(internal.Pubsub).WriteTo(internal.WriterContext(ctx, w)) //nolint: errcheck // always valid
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
		return v.(*string) //nolint: errcheck // always valid
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

// serviceDebounced delays [Service.Publish] to avoid short-lived streams.
type serviceDebounced struct {
	Service

	duration time.Duration
}

func (svc serviceDebounced) Publish(ctx context.Context, s StreamPub, r io.Reader) (int64, error) {
	n, err, r, ok := svc.debounce(ctx, r, svc.duration)
	if ok {
		return svc.Service.Publish(ctx, s, r)
	}
	return n, err
}

// debounce ensures a reader can be read for a minimum duration.
// It actually buffers the reader and returns the number of bytes read & error,
// a replacement for the altered reader and a success flag.
func (serviceDebounced) debounce(parent context.Context, r io.Reader, d time.Duration) (int64, error, io.Reader, bool) { //nolint:revive,staticcheck,golines // not an idiomatic error
	errStop := struct{ error }{}

	ctx, cancel := context.WithTimeoutCause(parent, d, errStop)
	defer cancel()

	b := bytes.NewBuffer(nil)
	n, err := b.ReadFrom(internal.ReaderContext(ctx, r))

	return n, err, io.MultiReader(b, r), errors.Is(context.Cause(ctx), errStop)
}
