package domaintest

import (
	"cmp"
	"context"
	"io"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/rlibaert/gocast/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type rwFunc func(p []byte) (int, error)

func (f rwFunc) Read(p []byte) (int, error)  { return f(p) }
func (f rwFunc) Write(p []byte) (int, error) { return f(p) }

func pubReader(start func(), pub domain.StreamPub) io.Reader {
	started := false
	return rwFunc(func(p []byte) (int, error) {
		time.Sleep(time.Millisecond)
		if start != nil && !started {
			start()
			started = true
		}
		return copy(p, pub), nil
	})
}

type ServiceTester struct {
	Service domain.Service
}

// baseDur is duration from which all relative deadlines are calculated.
const baseDur = 5 * time.Second

func (st ServiceTester) TestPublishSubscribe(t *testing.T) {
	wg := sync.WaitGroup{}
	defer wg.Wait()

	var (
		dlPubStops = time.Now().Add(baseDur)
	)

	wgPubsPublishing := sync.WaitGroup{}
	for _, pub := range []domain.StreamPub{"foo", "bar"} {
		wgPubsPublishing.Add(1)
		wg.Go(func() {
			ctx, cancel := context.WithDeadline(t.Context(), dlPubStops)
			defer cancel()

			n, err := st.Service.Publish(ctx, pub, pubReader(wgPubsPublishing.Done, pub))
			require.ErrorIs(t, err, context.DeadlineExceeded)
			assert.Positive(t, n)
		})
	}
	wgPubsPublishing.Wait()

	for _, sub := range []domain.StreamSub{"foo", "bar", "foo", "bar"} {
		wg.Go(func() {
			ctx := t.Context()

			re := regexp.MustCompile(`^(` + string(sub) + `)*$`)
			n, err := st.Service.Subscribe(ctx, sub, rwFunc(func(p []byte) (int, error) {
				assert.True(t, re.Match(p), string(p), "not "+re.String())
				return len(p), nil
			}))
			require.NoError(t, err)
			assert.Positive(t, n)
		})
	}
}

func (st ServiceTester) TestFallback(t *testing.T) {
	wg := sync.WaitGroup{}
	defer wg.Wait()

	domain.ServiceResetFallbacks(st.Service, map[domain.StreamSub][]domain.StreamPub{"toto": {"foo", "bar"}})

	var (
		dlFooStops = time.Now().Add(baseDur)
		dlBarStops = dlFooStops.Add(baseDur)
	)

	wgPubsPublishing := sync.WaitGroup{}
	wgPubsPublishing.Add(1)
	wg.Go(func() {
		ctx, cancel := context.WithDeadline(t.Context(), dlFooStops)
		defer cancel()

		n, err := st.Service.Publish(ctx, "foo", pubReader(wgPubsPublishing.Done, "foo"))
		require.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Positive(t, n)
	})
	wgPubsPublishing.Add(1)
	wg.Go(func() {
		ctx, cancel := context.WithDeadline(t.Context(), dlBarStops)
		defer cancel()

		n, err := st.Service.Publish(ctx, "bar", pubReader(wgPubsPublishing.Done, "bar"))
		require.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Positive(t, n)
	})
	wgPubsPublishing.Wait()

	wg.Go(func() {
		ctx := t.Context()

		re1 := regexp.MustCompile(`^(foo)+(bar)*$`) // foo & foo -> bar transition
		re2 := regexp.MustCompile(`^(bar)+(foo)*$`) // bar & bar -> foo transition (maybe, when the test concludes)
		re := re1
		n, err := st.Service.Subscribe(ctx, "toto", rwFunc(func(p []byte) (int, error) {
			if re == re1 && re2.Match(p) {
				re = re2
			}
			assert.Truef(t, re.Match(p) || len(p) == 0, "%v does not match %s", re, string(p))
			return len(p), nil
		}))
		require.NoError(t, err)
		assert.Positive(t, n)
		assert.Same(t, re, re2)
	})
}

func (st ServiceTester) TestBackup(t *testing.T) {
	wg := sync.WaitGroup{}
	defer wg.Wait()

	domain.ServiceResetFallbacks(st.Service, map[domain.StreamSub][]domain.StreamPub{"toto": {"foo", "bar"}})

	var (
		dlFooStarts = time.Now().Add(baseDur)
		dlFooStops  = dlFooStarts.Add(baseDur)
		dlBarStops  = dlFooStops
	)

	wg.Go(func() {
		time.Sleep(time.Until(dlFooStarts))
		ctx, cancel := context.WithDeadline(t.Context(), dlFooStops)
		defer cancel()

		n, err := st.Service.Publish(ctx, "foo", pubReader(nil, "foo"))
		require.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Positive(t, n)
	})

	wgPubsPublishing := sync.WaitGroup{}
	wgPubsPublishing.Add(1)
	wg.Go(func() {
		ctx, cancel := context.WithDeadline(t.Context(), dlBarStops)
		defer cancel()

		n, err := st.Service.Publish(ctx, "bar", pubReader(wgPubsPublishing.Done, "bar"))
		require.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Positive(t, n)
	})
	wgPubsPublishing.Wait()

	wg.Go(func() {
		ctx := t.Context()

		re1 := regexp.MustCompile(`^(bar)+(foo)*$`) // bar & bar -> foo transition
		re2 := regexp.MustCompile(`^(foo)+(bar)*$`) // foo & foo -> bar transition (maybe, when the test concludes)
		re := re1
		n, err := st.Service.Subscribe(ctx, "toto", rwFunc(func(p []byte) (int, error) {
			if re == re1 && re2.Match(p) {
				re = re2
			}
			assert.Truef(t, re.Match(p) || len(p) == 0, "%v does not match %s", re, string(p))
			return len(p), nil
		}))
		require.NoError(t, err)
		assert.Positive(t, n)
		assert.Same(t, re, re2)
	})
}

func (st ServiceTester) TestPublishTitle(t *testing.T) {
	wg := sync.WaitGroup{}
	defer wg.Wait()

	var (
		dlFoo = time.Now().Add(baseDur)
	)

	wgPubsPublishing := sync.WaitGroup{}
	wgPubsPublishing.Add(1)
	wg.Go(func() {
		ctx, cancel := context.WithDeadline(t.Context(), dlFoo)
		defer cancel()

		n, err := st.Service.Publish(ctx, "foo", pubReader(wgPubsPublishing.Done, "foo"))
		require.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Positive(t, n)
	})
	wgPubsPublishing.Wait()

	domain.ServiceResetFallbacks(st.Service, map[domain.StreamSub][]domain.StreamPub{
		"toto": {"foo"},
		"tata": {"foo"},
	})

	const title = "lorem ipsum"
	require.NoError(t, st.Service.PublishTitle(t.Context(), "foo", title))
	assert.Equal(t, title, *cmp.Or(domain.ServiceStreamSubTitle(st.Service, "foo"), new("<nil>")))
	assert.Equal(t, title, *cmp.Or(domain.ServiceStreamSubTitle(st.Service, "toto"), new("<nil>")))
	assert.Equal(t, title, *cmp.Or(domain.ServiceStreamSubTitle(st.Service, "tata"), new("<nil>")))
	require.ErrorIs(t, st.Service.PublishTitle(t.Context(), "bar", ""), domain.ErrStreamNotFound)
}

func (st ServiceTester) TestCloseOnFallbacksRemoved(t *testing.T) {
	wg := sync.WaitGroup{}
	defer wg.Wait()

	var (
		dlReset = time.Now().Add(baseDur)
		dlFoo   = dlReset.Add(baseDur)
	)

	wg.Go(func() {
		domain.ServiceResetFallbacks(st.Service, map[domain.StreamSub][]domain.StreamPub{"toto": {"foo"}})
		time.Sleep(time.Until(dlReset))
		domain.ServiceResetFallbacks(st.Service, map[domain.StreamSub][]domain.StreamPub{})
	})

	wgPubsPublishing := sync.WaitGroup{}
	wgPubsPublishing.Add(1)
	wg.Go(func() {
		ctx, cancel := context.WithDeadline(t.Context(), dlFoo)
		defer cancel()

		n, err := st.Service.Publish(ctx, "foo", pubReader(wgPubsPublishing.Done, "foo"))
		require.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Positive(t, n)
	})
	wgPubsPublishing.Wait()

	t.Run("toto gets closed", func(t *testing.T) {
		n, err := st.Service.Subscribe(t.Context(), "toto", io.Discard)
		require.NoError(t, err)
		assert.Positive(t, n)
	})

	t.Run("foo still opened", func(t *testing.T) {
		n, err := st.Service.Subscribe(t.Context(), "foo", io.Discard)
		require.NoError(t, err)
		assert.Positive(t, n)
	})
}
