package internal

import "io"

// PageWriter wraps an [io.Writer] to write a footer at regular intervals.
// Footer bytes are not included in the bytes written count returned.
func PageWriter(w io.Writer, capacity int, footer func() []byte) io.Writer {
	return &pageWriter{
		Writer:   w,
		capacity: capacity,
		length:   0,
		footer:   footer,
	}
}

type pageWriter struct {
	io.Writer

	capacity int
	length   int
	footer   func() []byte
}

func (w *pageWriter) Write(p []byte) (int, error) {
	n := 0

	for len(p) >= w.capacity-w.length {
		wn, err := w.Writer.Write(p[:w.capacity-w.length])
		n += wn
		p = p[wn:]
		if err != nil {
			return n, err
		}

		_, err = w.Writer.Write(w.footer())
		if err != nil {
			return n, err
		}
		w.length = 0
	}

	wn, err := w.Writer.Write(p)
	w.length += wn
	return n + wn, err
}
