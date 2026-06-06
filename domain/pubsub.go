package domain

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/rlibaert/gocast/domain/internal"
)

// pubsub extends [internal.Pubsub] functionality to:
//   - buffer writes in chunks of roughly equal durations
//   - wait for all readers to finish before returning from close
type pubsub struct {
	internal.Pubsub

	chunk bytes.Buffer
	start time.Time
	dur   time.Duration

	readers sync.WaitGroup
}

func newPubsub(burst int, dur time.Duration) *pubsub {
	return &pubsub{
		Pubsub: internal.NewPubsub(burst),
		dur:    dur,
	}
}

func (ps *pubsub) Flush() error {
	_, err := ps.chunk.WriteTo(ps.Pubsub)
	return err
}

func (ps *pubsub) Write(p []byte) (int, error) {
	n, err := ps.chunk.Write(p)
	if time.Since(ps.start) > ps.dur {
		err = errors.Join(err, ps.Flush())
		ps.start = time.Now()
	}
	return n, err
}

func (ps *pubsub) Close() error {
	defer ps.readers.Wait()
	return errors.Join(ps.Flush(), ps.Pubsub.Close())
}

func (ps *pubsub) WriteTo(w io.Writer) (int64, error) {
	ps.readers.Add(1)
	defer ps.readers.Done()
	return ps.Pubsub.WriteTo(w)
}
