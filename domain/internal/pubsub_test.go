package internal_test

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rlibaert/gocast/domain/internal"
	"github.com/rlibaert/gocast/testing/assert"
)

func TestPubsub_data_copy(t *testing.T) {
	ps := internal.NewPubsub()

	wg := sync.WaitGroup{}
	wg.Go(func() {
		b := strings.Builder{}
		n, err := ps.WriteTo(&b)
		assert.ErrNil(t, err)
		assert.EQ(t, n, 9)
		assert.EQ(t, b.String(), "foobarbaz")
	})
	wg.Go(func() {
		time.Sleep(time.Second)
		fmt.Fprint(ps, "foo")
		fmt.Fprint(ps, "bar")
		fmt.Fprint(ps, "baz")
		assert.ErrNil(t, ps.Close())
	})

	wg.Wait()
}

func TestPubsub_waits_subs_on_close(t *testing.T) {
	ps := internal.NewPubsub()
	mu := sync.Mutex{}
	flag := false

	go func() {
		ps.WriteTo(internal.FuncWriter(func(p []byte) (int, error) {
			time.Sleep(time.Second)
			return len(p), nil
		}))
		mu.Lock()
		defer mu.Unlock()
		flag = true
	}()

	time.Sleep(time.Second)
	fmt.Fprint(ps, "foo")
	fmt.Fprint(ps, "bar")
	fmt.Fprint(ps, "baz")

	tick := time.Now()
	assert.ErrNil(t, ps.Close())
	assert.GT(t, time.Since(tick), time.Second)

	mu.Lock()
	defer mu.Unlock()
	assert.Expected(t, flag, "flag was not set")
}

func BenchmarkPubsub_Write10k(b *testing.B) {
	ps := internal.NewPubsub()
	defer ps.Close()

	buf := make([]byte, 4096)
	for range 10_000 {
		go func() {
			_, err := ps.WriteTo(io.Discard)
			assert.ErrIs(b, err, nil)
		}()
	}
	go func() {
		ps.WriteTo(internal.FuncWriter(func(p []byte) (int, error) {
			time.Sleep(time.Second)
			return len(p), nil
		}))
	}()

	b.ResetTimer()
	for b.Loop() {
		n, err := ps.Write(buf)
		assert.ErrNil(b, err)
		b.SetBytes(int64(n))
	}
}
