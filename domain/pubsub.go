package domain

import (
	"errors"
	"time"

	"github.com/rlibaert/gocast/domain/internal"
)

// pubsub wraps an [internal.Pubsub] to buffer writes in chunks of roughly equal durations.
type pubsub struct {
	internal.Pubsub

	bytes []byte
	start time.Time
}

func (ps *pubsub) Write(p []byte) (int, error) {
	ps.bytes = append(ps.bytes, p...)

	var err error
	if time.Since(ps.start) > time.Second {
		_, err = ps.Pubsub.Write(ps.bytes)
		ps.bytes, ps.start = ps.bytes[:0], time.Now()
	}

	return len(p), err
}

func (ps *pubsub) Close() error {
	_, err := ps.Pubsub.Write(ps.bytes)
	return errors.Join(ps.Pubsub.Close(), err)
}
