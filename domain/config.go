package domain

import "context"

type Config struct {
	Fallbacks map[StreamSub][]StreamPub
}

type Getter[T any] interface {
	Get(context.Context) (T, error)
}
