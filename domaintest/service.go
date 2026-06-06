package domaintest

import (
	"context"
	"io"
	"regexp"
	"sync"
	"testing"
	"time"

	. "github.com/rlibaert/gocast/domain"
	"github.com/rlibaert/gocast/testing/assert"
)

type rwFunc func(p []byte) (int, error)

func (f rwFunc) Read(p []byte) (int, error)  { return f(p) }
func (f rwFunc) Write(p []byte) (int, error) { return f(p) }

func pubReader(throttle time.Duration, start func(), pub StreamPub) io.Reader {
	started := false
	return rwFunc(func(p []byte) (int, error) {
		time.Sleep(throttle)
		if start != nil && !started {
			start()
			started = true
		}
		return copy(p, pub), nil
	})
}

type ServiceTester struct {
	Service Service
}

func (st ServiceTester) TestPublishSubscribe(t *testing.T) {
	wg := sync.WaitGroup{}
	defer wg.Wait()

	wgPubsPublishing := sync.WaitGroup{}
	for _, pub := range []StreamPub{"foo", "bar"} {
		wgPubsPublishing.Add(1)
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			n, err := st.Service.Publish(ctx, pub, pubReader(time.Millisecond, wgPubsPublishing.Done, pub))
			assert.ErrIs(t, err, context.DeadlineExceeded)
			assert.GT(t, n, 0)
		})
	}
	wgPubsPublishing.Wait()

	for _, sub := range []StreamSub{"foo", "bar", "foo", "bar"} {
		wg.Go(func() {
			ctx := t.Context()

			re := regexp.MustCompile(`^(` + string(sub) + `)*$`)
			n, err := st.Service.Subscribe(ctx, sub, rwFunc(func(p []byte) (int, error) {
				assert.Expected(t, re.Match(p), string(p), "not "+re.String())
				return len(p), nil
			}))
			assert.ErrIs(t, err, nil)
			assert.GT(t, n, 0)
		})
	}
}

func (st ServiceTester) TestFallback(t *testing.T) {
	wg := sync.WaitGroup{}
	defer wg.Wait()

	ServiceResetFallbacks(st.Service, map[StreamSub][]StreamPub{"toto": {"foo", "bar"}})

	wgPubsPublishing := sync.WaitGroup{}
	wgPubsPublishing.Add(1)
	wg.Go(func() {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		n, err := st.Service.Publish(ctx, "foo", pubReader(time.Millisecond, wgPubsPublishing.Done, "foo"))
		assert.ErrIs(t, err, context.DeadlineExceeded)
		assert.GT(t, n, 0)
	})
	wgPubsPublishing.Add(1)
	wg.Go(func() {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()

		n, err := st.Service.Publish(ctx, "bar", pubReader(time.Millisecond, wgPubsPublishing.Done, "bar"))
		assert.ErrIs(t, err, context.DeadlineExceeded)
		assert.GT(t, n, 0)
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
			assert.Expected(t, re.Match(p) || len(p) == 0, re, "does not match", string(p))
			return len(p), nil
		}))
		assert.ErrIs(t, err, nil)
		assert.GT(t, n, 0)
		assert.EQ(t, re, re2)
	})
}

func (st ServiceTester) TestBackup(t *testing.T) {
	wg := sync.WaitGroup{}
	defer wg.Wait()

	ServiceResetFallbacks(st.Service, map[StreamSub][]StreamPub{"toto": {"foo", "bar"}})

	wg.Go(func() {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		time.Sleep(5 * time.Second)

		n, err := st.Service.Publish(ctx, "foo", pubReader(time.Millisecond, nil, "foo"))
		assert.ErrIs(t, err, context.DeadlineExceeded)
		assert.GT(t, n, 0)
	})

	wgPubsPublishing := sync.WaitGroup{}
	wgPubsPublishing.Add(1)
	wg.Go(func() {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()

		n, err := st.Service.Publish(ctx, "bar", pubReader(time.Millisecond, wgPubsPublishing.Done, "bar"))
		assert.ErrIs(t, err, context.DeadlineExceeded)
		assert.GT(t, n, 0)
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
			assert.Expected(t, re.Match(p) || len(p) == 0, re, "does not match", string(p))
			return len(p), nil
		}))
		assert.ErrIs(t, err, nil)
		assert.GT(t, n, 0)
		assert.EQ(t, re, re2)
	})
}
