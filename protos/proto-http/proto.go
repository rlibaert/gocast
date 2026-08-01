package proto

import (
	"errors"
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
		stream := r.PathValue("stream")

		_, err := reg.Service.Publish(domain.StreamPub(stream), r.Body)
		switch {
		case errors.Is(err, domain.ErrStreamExists):
			httpStatusTextError(w, http.StatusConflict)
		case errors.Is(err, nil):
		default:
			httpStatusTextError(w, http.StatusInternalServerError)
		}
	})

	mux.HandleFunc("GET /streams/{stream}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "audio/mpeg")

		stream := r.PathValue("stream")
		_, err := reg.Service.Subscribe(domain.StreamSub(stream), w)
		switch {
		case errors.Is(err, domain.ErrStreamNotFound):
			httpStatusTextError(w, http.StatusNotFound)
		case errors.Is(err, nil), r.Context().Err() != nil:
		default:
			httpStatusTextError(w, http.StatusInternalServerError)
		}
	})

	mux.HandleFunc("PUT /streams/{stream}/metadata", func(w http.ResponseWriter, r *http.Request) {
		stream := r.PathValue("stream")
		title := r.URL.Query().Get("title")
		err := reg.Service.PublishTitle(r.Context(), domain.StreamPub(stream), title)
		switch {
		case errors.Is(err, nil):
		case errors.Is(err, domain.ErrStreamNotFound):
			httpStatusTextError(w, http.StatusNotFound)
		default:
			httpStatusTextError(w, http.StatusInternalServerError)
		}
	})
}
