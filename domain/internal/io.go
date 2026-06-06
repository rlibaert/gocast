package internal

import (
	"context"
	"io"
)

type FuncWriter func(p []byte) (int, error)

func (f FuncWriter) Write(p []byte) (int, error) { return f(p) }

type ContextWriter struct {
	context.Context
	io.Writer
}

func (w ContextWriter) Write(p []byte) (int, error) {
	select {
	case <-w.Context.Done():
		return 0, w.Context.Err()
	default:
		return w.Writer.Write(p)
	}
}

type ContextReader struct {
	context.Context
	io.Reader
}

func (r ContextReader) Read(p []byte) (int, error) {
	select {
	case <-r.Context.Done():
		return 0, r.Context.Err()
	default:
		return r.Reader.Read(p)
	}
}
