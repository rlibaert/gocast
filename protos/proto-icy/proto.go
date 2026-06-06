package proto

import (
	"context"
	"errors"
	"io"
	"net/http"

	"github.com/rlibaert/gocast/domain"
)

type ServiceRegisterer struct {
	StreamsService domain.StreamsService
}

func httpStatusTextError(w http.ResponseWriter, code int) {
	http.Error(w, http.StatusText(code), code)
}

func (reg ServiceRegisterer) Register(mux *http.ServeMux) {
	mux.HandleFunc("PUT /icy/{stream}", func(w http.ResponseWriter, r *http.Request) {
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

	mux.HandleFunc("GET /icy/{stream}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "audio/mpeg")

		ctx := r.Context()
		stream := r.PathValue("stream")
		_, err := reg.StreamsService.Subscribe(ctx, domain.StreamSub(stream), w)
		switch {
		case errors.Is(err, nil), errors.Is(err, context.Canceled), errors.Is(err, io.EOF):
		case errors.Is(err, domain.ErrStreamNotFound):
			httpStatusTextError(w, http.StatusNotFound)
		default:
			httpStatusTextError(w, http.StatusInternalServerError)
		}
	})
}
