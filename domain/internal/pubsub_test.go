package internal_test

import (
	"context"
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
	ps := internal.NewPubsub(0)

	wg := sync.WaitGroup{}
	wg.Go(func() {
		b := strings.Builder{}
		n, err := ps.WriteToContext(t.Context(), &b)
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

func TestPubsubCancelledWriteToContext(t *testing.T) {
	ps := internal.NewPubsub(0)
	defer ps.Close()

	wg := sync.WaitGroup{}
	wg.Go(func() {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		time.AfterFunc(time.Second, cancel)

		n, err := ps.WriteToContext(ctx, io.Discard)
		require.ErrorIs(t, err, context.Canceled)
		assert.Zero(t, n)
	})
	wg.Wait()
}

func BenchmarkPubsub_Write10k(b *testing.B) {
	ps := internal.NewPubsub(0)
	defer ps.Close()

	buf := make([]byte, 4096)
	for range 10_000 {
		go func() {
			_, err := ps.WriteToContext(b.Context(), io.Discard)
			assert.NoError(b, err)
		}()
	}
	go func() {
		ps.WriteToContext(b.Context(), internal.WriterFunc(func(p []byte) (int, error) {
			time.Sleep(time.Second)
			return len(p), nil
		}))
	}()

	b.ResetTimer()
	for b.Loop() {
		n, err := ps.Write(buf)
		require.NoError(b, err)
		b.SetBytes(int64(n))
	}
}
