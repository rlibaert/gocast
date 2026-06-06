package proto

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/rlibaert/gocast/domain"
)

type ServiceRegisterer struct {
	StreamsService domain.StreamsService
}

func httpStatusTextError(w http.ResponseWriter, code int) {
	http.Error(w, http.StatusText(code), code)
}

func (reg ServiceRegisterer) Register(mux *http.ServeMux) {
	mux.HandleFunc("PUT /{stream}", func(w http.ResponseWriter, r *http.Request) {
		if http.CanonicalHeaderKey(r.Header.Get("Expect")) == "100-Continue" {
			w.WriteHeader(http.StatusContinue)
		}

		conn, buf, err := http.NewResponseController(w).Hijack()
		if err != nil {
			httpStatusTextError(w, http.StatusInternalServerError)
			return
		}
		defer conn.Close()

		stream := r.PathValue("stream")
		_, _ = reg.StreamsService.Publish(r.Context(), domain.StreamPub(stream), buf)
	})

	mux.HandleFunc("GET /{stream}", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		stream := r.PathValue("stream")

		var writer io.Writer = w
		if r.Header.Get("icy-metadata") == "1" {
			writer = &paginatedWriter{
				Writer:   w,
				pageSize: metaInt,
				onPageEnd: func() {
					var m metadata
					if title, ok := domain.StreamsServiceStreamSubTitle(reg.StreamsService, domain.StreamSub(stream)); ok {
						m.StreamTitle = &title
					}
					b, _ := m.MarshalBinary()
					_, _ = w.Write(b)
				},
			}
			w.Header().Set("icy-metaint", metaIntStr)
		}

		w.Header().Set("Content-Type", "audio/mpeg")

		_, err := reg.StreamsService.Subscribe(ctx, domain.StreamSub(stream), writer)
		switch {
		case errors.Is(err, nil), errors.Is(err, context.Canceled), errors.Is(err, io.EOF):
		case errors.Is(err, domain.ErrStreamNotFound):
			httpStatusTextError(w, http.StatusNotFound)
		default:
			httpStatusTextError(w, http.StatusInternalServerError)
		}
	})

	mux.HandleFunc("GET /admin/metadata", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		q := r.URL.Query()

		if q.Get("mode") != "updinfo" {
			httpStatusTextError(w, http.StatusBadRequest)
			return
		}

		stream := strings.TrimPrefix(q.Get("mount"), "/")

		var title string
		switch {
		case q.Has("song"):
			title = q.Get("song")
		case q.Has("artist") && q.Has("title"):
			title = fmt.Sprint(q.Get("artist"), " - ", q.Get("title"))
		default:
			return
		}

		err := reg.StreamsService.PublishTitle(ctx, domain.StreamPub(stream), title)
		switch {
		case errors.Is(err, nil), errors.Is(err, context.Canceled):
		case errors.Is(err, domain.ErrStreamNotFound):
			httpStatusTextError(w, http.StatusNotFound)
		default:
			httpStatusTextError(w, http.StatusInternalServerError)
		}
	})
}

type paginatedWriter struct {
	io.Writer

	pageSize   int
	pageLength int
	onPageEnd  func()
}

func (pw *paginatedWriter) Write(p []byte) (int, error) {
	n := 0
	for len(p) > 0 {
		wn, err := pw.Writer.Write(p[:min(len(p), pw.pageSize-pw.pageLength)])
		n += wn
		pw.pageLength += wn
		p = p[wn:]
		if err != nil {
			return n, err
		}
		if pw.pageLength == pw.pageSize {
			pw.onPageEnd()
			pw.pageLength = 0
		}
	}
	return n, nil
}
