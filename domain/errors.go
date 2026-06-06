package domain

import "errors"

var (
	ErrStreamExists       = errors.New("domain: stream exists")
	ErrStreamNotFound     = errors.New("domain: stream not found")
	ErrStreamNotAvailable = errors.New("domain: stream not available")
)
