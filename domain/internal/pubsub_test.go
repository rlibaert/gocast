package internal_test

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rlibaert/gocast/domain/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPubsub_data_copy(t *testing.T) {
	ps := internal.NewPubsub()

	wg := sync.WaitGroup{}
	wg.Go(func() {
		b := strings.Builder{}
		n, err := ps.WriteTo(&b)
		require.NoError(t, err)
		assert.Equal(t, int64(9), n)
		assert.Equal(t, "foobarbaz", b.String())
	})
	wg.Go(func() {
		time.Sleep(time.Second)
		fmt.Fprint(ps, "foo")
		fmt.Fprint(ps, "bar")
		fmt.Fprint(ps, "baz")
		require.NoError(t, ps.Close())
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
	require.NoError(t, ps.Close())
	assert.Greater(t, time.Since(tick), time.Second)

	mu.Lock()
	defer mu.Unlock()
	assert.True(t, flag)
}

func BenchmarkPubsub_Write10k(b *testing.B) {
	ps := internal.NewPubsub()
	defer ps.Close()

	buf := make([]byte, 4096)
	for range 10_000 {
		go func() {
			_, err := ps.WriteTo(io.Discard)
			require.NoError(b, err)
		}()
	}

	b.ResetTimer()
	for b.Loop() {
		n, err := ps.Write(buf)
		require.NoError(b, err)
		b.SetBytes(int64(n))
	}
}
