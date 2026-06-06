package proto

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/rlibaert/gocast/domain"
)

type ServiceRegisterer struct {
	Service domain.Service
}

func httpStatusTextError(w http.ResponseWriter, code int) {
	http.Error(w, http.StatusText(code), code)
}

func (reg ServiceRegisterer) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /streams/{stream}", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		stream := r.PathValue("stream")

		_, err := reg.Service.Publish(ctx, domain.StreamPub(stream), r.Body)
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
		_, err := reg.Service.Subscribe(ctx, domain.StreamSub(stream), w)
		switch {
		case errors.Is(err, nil), errors.Is(err, context.Canceled), errors.Is(err, io.EOF):
		case errors.Is(err, domain.ErrStreamNotFound):
			httpStatusTextError(w, http.StatusNotFound)
		default:
			httpStatusTextError(w, http.StatusInternalServerError)
		}
	})

	mux.HandleFunc("PUT /streams/{stream}/metadata", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		stream := r.PathValue("stream")
		title := r.URL.Query().Get("title")
		err := reg.Service.PublishTitle(ctx, domain.StreamPub(stream), title)
		switch {
		case errors.Is(err, nil), errors.Is(err, context.Canceled):
		case errors.Is(err, domain.ErrStreamNotFound):
			httpStatusTextError(w, http.StatusNotFound)
		default:
			httpStatusTextError(w, http.StatusInternalServerError)
		}
	})

	mux.HandleFunc("GET /streams/{stream}/metadata", func(w http.ResponseWriter, r *http.Request) {
		stream := r.PathValue("stream")
		title, ok := domain.ServiceStreamSubTitle(reg.Service, domain.StreamSub(stream))
		if !ok {
			httpStatusTextError(w, http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintln(w, "title:", title)
	})
}
