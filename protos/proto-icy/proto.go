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
	Service domain.Service
}

func httpStatusTextError(w http.ResponseWriter, code int) {
	http.Error(w, http.StatusText(code), code)
}

func (reg ServiceRegisterer) Register(mux *http.ServeMux) {
	mux.HandleFunc("SOURCE /{stream}", reg.sourceStream)
	mux.HandleFunc("PUT /{stream}", reg.putStream)
	mux.HandleFunc("GET /{stream}", reg.getStream)
	mux.HandleFunc("GET /admin/metadata", reg.getAdminMetadata)
}

func (reg ServiceRegisterer) sourceStream(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)

	conn, buf, err := http.NewResponseController(w).Hijack()
	if err != nil {
		httpStatusTextError(w, http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	stream := r.PathValue("stream")
	_, _ = reg.Service.Publish(r.Context(), domain.StreamPub(stream), buf)
}

func (reg ServiceRegisterer) putStream(w http.ResponseWriter, r *http.Request) {
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
	_, _ = reg.Service.Publish(r.Context(), domain.StreamPub(stream), buf)
}

func (reg ServiceRegisterer) getStream(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	stream := r.PathValue("stream")

	var writer io.Writer = w
	if r.Header.Get("Icy-Metadata") == "1" {
		mtitle, mbytes := (*string)(nil), metadata(nil)
		writer = &paginatedWriter{
			Writer:   w,
			pageSize: Metaint.int,
			pageFooter: func() []byte {
				t := domain.ServiceStreamSubTitle(reg.Service, domain.StreamSub(stream))
				switch t {
				case mtitle:
					// no changes
				case nil:
					mtitle, mbytes = t, metadata(mbytes)
				default:
					mtitle, mbytes = t, metadata(mbytes, "StreamTitle='", *t, "';")
				}
				return mbytes
			},
		}
		w.Header().Set("Icy-Metaint", Metaint.string)
	}

	w.Header().Set("Content-Type", "audio/mpeg")

	_, err := reg.Service.Subscribe(ctx, domain.StreamSub(stream), writer)
	switch {
	case errors.Is(err, nil), errors.Is(err, context.Canceled):
	case errors.Is(err, domain.ErrStreamNotFound):
		httpStatusTextError(w, http.StatusNotFound)
	default:
		httpStatusTextError(w, http.StatusInternalServerError)
	}
}

func (reg ServiceRegisterer) getAdminMetadata(w http.ResponseWriter, r *http.Request) {
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

	err := reg.Service.PublishTitle(ctx, domain.StreamPub(stream), title)
	switch {
	case errors.Is(err, nil), errors.Is(err, context.Canceled):
	case errors.Is(err, domain.ErrStreamNotFound):
		httpStatusTextError(w, http.StatusNotFound)
	default:
		httpStatusTextError(w, http.StatusInternalServerError)
	}
}

type paginatedWriter struct {
	io.Writer

	pageSize   int
	pageLength int
	pageFooter func() []byte
}

func (pw *paginatedWriter) Write(p []byte) (int, error) {
	n := 0

	for len(p) >= pw.pageSize-pw.pageLength {
		wn, err := pw.Writer.Write(p[:pw.pageSize-pw.pageLength])
		n += wn
		p = p[wn:]
		if err != nil {
			return n, err
		}

		_, err = pw.Writer.Write(pw.pageFooter())
		if err != nil {
			return n, err
		}
		pw.pageLength = 0
	}

	wn, err := pw.Writer.Write(p)
	pw.pageLength += wn
	return n + wn, err
}
