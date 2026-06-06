package domain

import (
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

	chunk []byte
	start time.Time

	readers sync.WaitGroup
}

func (ps *pubsub) flush() error {
	_, err := ps.Pubsub.Write(ps.chunk)
	ps.chunk, ps.start = ps.chunk[:0], time.Now()
	return err
}

func (ps *pubsub) Write(p []byte) (int, error) {
	ps.chunk = append(ps.chunk, p...)

	var err error
	if time.Since(ps.start) > time.Second {
		err = ps.flush()
	}

	return len(p), err
}

func (ps *pubsub) Close() error {
	defer ps.readers.Wait()
	return errors.Join(ps.flush(), ps.Pubsub.Close())
}

func (ps *pubsub) WriteTo(w io.Writer) (int64, error) {
	ps.readers.Add(1)
	defer ps.readers.Done()
	return ps.Pubsub.WriteTo(w)
}
