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
	mux.HandleFunc("POST /streams/{stream}", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		stream := r.PathValue("stream")
		_, err := reg.StreamsService.Publish(ctx, domain.StreamPub(stream), r.Body)
		switch {
		case errors.Is(err, nil), errors.Is(err, context.Canceled), errors.Is(err, io.EOF):
		case errors.Is(err, domain.ErrStreamExists):
			httpStatusTextError(w, http.StatusLocked)
		default:
			httpStatusTextError(w, http.StatusInternalServerError)
		}
	})

	mux.HandleFunc("GET /streams/{stream}", func(w http.ResponseWriter, r *http.Request) {
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
