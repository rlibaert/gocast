package domaintest

import (
	"context"
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

type ServiceTester struct {
	Service Service
}

func (st ServiceTester) Test(t *testing.T) {
	t.Run("TestPublishSubscribe", st.TestPublishSubscribe)
	t.Run("TestFallback", st.TestFallback)
	t.Run("TestBackup", st.TestBackup)
}

func (st ServiceTester) TestPublishSubscribe(t *testing.T) {
	wg := sync.WaitGroup{}
	defer wg.Wait()

	for _, pub := range []StreamPub{"foo", "bar"} {
		wg.Add(1)
		go t.Run("pub "+string(pub), func(t *testing.T) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			n, err := st.Service.Publish(ctx, pub, rwFunc(func(p []byte) (int, error) {
				time.Sleep(time.Millisecond)
				return copy(p, pub), nil
			}))
			assert.ErrIs(t, err, context.DeadlineExceeded)
			assert.GT(t, n, 0)
		})
	}

	time.Sleep(time.Second)

	for _, sub := range []StreamSub{"foo", "bar", "foo", "bar"} {
		wg.Add(1)
		go t.Run("sub "+string(sub), func(t *testing.T) {
			defer wg.Done()

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
	ServiceResetFallbacks(st.Service, map[StreamSub][]StreamPub{"toto": {"foo", "bar"}})

	wg := sync.WaitGroup{}
	defer wg.Wait()

	for pub, timeout := range map[StreamPub]time.Duration{
		"foo": 5 * time.Second,
		"bar": 10 * time.Second,
	} {
		wg.Add(1)
		go t.Run("pub "+string(pub), func(t *testing.T) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(t.Context(), timeout)
			defer cancel()

			n, err := st.Service.Publish(ctx, pub, rwFunc(func(p []byte) (int, error) {
				time.Sleep(time.Millisecond)
				return copy(p, pub), nil
			}))
			assert.ErrIs(t, err, context.DeadlineExceeded)
			assert.GT(t, n, 0)
		})
	}

	time.Sleep(time.Second)

	for _, sub := range []StreamSub{"toto", "toto"} {
		wg.Add(1)
		go t.Run("sub "+string(sub), func(t *testing.T) {
			defer wg.Done()

			ctx := t.Context()

			re1 := regexp.MustCompile(`^(foo)+(bar)*$`)
			re2 := regexp.MustCompile(`^(foo)*(bar)+$`)
			re := re1
			n, err := st.Service.Subscribe(ctx, sub, rwFunc(func(p []byte) (int, error) {
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
}

func (st ServiceTester) TestBackup(t *testing.T) {
	ServiceResetFallbacks(st.Service, map[StreamSub][]StreamPub{"toto": {"foo", "bar"}})

	wg := sync.WaitGroup{}
	defer wg.Wait()

	for pub, delay := range map[StreamPub]time.Duration{
		"foo": 5 * time.Second,
		"bar": 0 * time.Second,
	} {
		wg.Add(1)
		go t.Run("pub "+string(pub), func(t *testing.T) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			time.Sleep(delay)

			n, err := st.Service.Publish(ctx, pub, rwFunc(func(p []byte) (int, error) {
				time.Sleep(time.Millisecond)
				return copy(p, pub), nil
			}))
			assert.ErrIs(t, err, context.DeadlineExceeded)
			assert.GT(t, n, 0)
		})
	}

	time.Sleep(time.Second)

	for _, sub := range []StreamSub{"toto", "toto"} {
		wg.Add(1)
		go t.Run("sub "+string(sub), func(t *testing.T) {
			defer wg.Done()

			ctx := t.Context()

			re1 := regexp.MustCompile(`^(bar)+(foo)*$`)
			re2 := regexp.MustCompile(`^(bar)*(foo)+$`)
			re := re1
			n, err := st.Service.Subscribe(ctx, sub, rwFunc(func(p []byte) (int, error) {
				if re == re1 && re2.Match(p) {
					re = re2
				}
				assert.Expected(t, re.Match(p) || len(p) == 0, re, "does not match", string(p)) // FIXME: can fail because the two sources stop
				_ = 0                                                                           // at the same time, triggering spurious fallbacks
				return len(p), nil
			}))
			assert.ErrIs(t, err, nil)
			assert.GT(t, n, 0)
			assert.EQ(t, re, re2)
		})
	}
}
