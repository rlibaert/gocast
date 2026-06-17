package proto_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rlibaert/gocast/domain"
	"github.com/rlibaert/gocast/domaintest"
	"github.com/rlibaert/gocast/protos/proto-http"
	"github.com/stretchr/testify/assert"
)

func TestServiceRegisterer(t *testing.T) {
	svc := &domaintest.ServiceMock{}
	reg := proto.ServiceRegisterer{Service: svc}
	mux := http.NewServeMux()
	reg.Register(mux)

	t.Run("Publish", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(
			t.Context(),
			http.MethodPost, "/streams/foo",
			io.NopCloser(strings.NewReader("testbody")),
		)

		svc.On("Publish", req.Context(), domain.StreamPub("foo"), req.Body).
			Return(0, nil).Once()
		mux.ServeHTTP(rec, req)
		svc.AssertExpectations(t)

		assert.Equal(t, http.StatusOK, rec.Result().StatusCode)
	})

	t.Run("Publish existing", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(
			t.Context(),
			http.MethodPost, "/streams/foo",
			io.NopCloser(strings.NewReader("testbody")),
		)

		svc.On("Publish", req.Context(), domain.StreamPub("foo"), req.Body).
			Return(0, domain.ErrStreamExists).Once()
		mux.ServeHTTP(rec, req)
		svc.AssertExpectations(t)

		assert.Equal(t, http.StatusConflict, rec.Result().StatusCode)
	})

	t.Run("Subscribe", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(
			t.Context(),
			http.MethodGet, "/streams/foo",
			http.NoBody,
		)

		svc.On("Subscribe", req.Context(), domain.StreamSub("foo"), rec).
			Return(0, nil).Once()
		mux.ServeHTTP(rec, req)
		svc.AssertExpectations(t)

		assert.Equal(t, http.StatusOK, rec.Result().StatusCode)
		assert.Subset(t, rec.Result().Header, http.Header{"Content-Type": {"audio/mpeg"}})
	})

	t.Run("Subscribe not found", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(
			t.Context(),
			http.MethodGet, "/streams/foo",
			http.NoBody,
		)

		svc.On("Subscribe", req.Context(), domain.StreamSub("foo"), rec).
			Return(0, domain.ErrStreamNotFound).Once()
		mux.ServeHTTP(rec, req)
		svc.AssertExpectations(t)

		assert.Equal(t, http.StatusNotFound, rec.Result().StatusCode)
	})
}
