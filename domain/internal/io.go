package internal

import (
	"context"
	"io"
)

type readWriterFunc func(p []byte) (int, error)

func (f readWriterFunc) Read(p []byte) (int, error)  { return f(p) }
func (f readWriterFunc) Write(p []byte) (int, error) { return f(p) }

func ReaderFunc(f func(p []byte) (int, error)) io.Reader { return readWriterFunc(f) }
func WriterFunc(f func(p []byte) (int, error)) io.Writer { return readWriterFunc(f) }

func ReaderContext(ctx context.Context, r io.Reader) io.Reader {
	return readWriterFunc(func(p []byte) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
			return r.Read(p)
		}
	})
}

func WriterContext(ctx context.Context, w io.Writer) io.Writer {
	return readWriterFunc(func(p []byte) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
			return w.Write(p)
		}
	})
}
