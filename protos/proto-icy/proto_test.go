package proto_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rlibaert/gocast/domain"
	"github.com/rlibaert/gocast/domaintest"
	"github.com/rlibaert/gocast/protos/proto-icy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestServiceRegisterer(t *testing.T) {
	svc := &domaintest.ServiceMock{}
	reg := proto.ServiceRegisterer{Service: svc}
	mux := http.NewServeMux()
	reg.Register(mux)

	t.Run("Publish", func(t *testing.T) {
		t.Skip()
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(
			t.Context(),
			http.MethodPut, "/foo",
			io.NopCloser(strings.NewReader("testbody")),
		)

		svc.On("Publish", req.Context(), domain.StreamPub("foo"), req.Body).
			Return(0, nil).Once()
		mux.ServeHTTP(rec, req)
		svc.AssertExpectations(t)

		assert.Equal(t, http.StatusOK, rec.Result().StatusCode)
	})

	t.Run("Publish existing", func(t *testing.T) {
		t.Skip()
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
			http.MethodGet, "/foo",
			http.NoBody,
		)

		svc.On("Subscribe", req.Context(), domain.StreamSub("foo"), rec).
			Return(0, nil).Once()
		mux.ServeHTTP(rec, req)
		svc.AssertExpectations(t)

		assert.Equal(t, http.StatusOK, rec.Result().StatusCode)
		assert.Subset(t, rec.Result().Header, http.Header{"Content-Type": {"audio/mpeg"}})
	})

	t.Run("Subscribe with metadata", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(
			t.Context(),
			http.MethodGet, "/foo",
			http.NoBody,
		)
		req.Header.Set("Icy-Metadata", "1")

		svc.On("Subscribe", req.Context(), domain.StreamSub("foo"), mock.Anything).
			Return(0, nil).Once()
		mux.ServeHTTP(rec, req)
		svc.AssertExpectations(t)

		assert.Equal(t, http.StatusOK, rec.Result().StatusCode)
		assert.Subset(t, rec.Result().Header, http.Header{
			"Content-Type": {"audio/mpeg"},
			"Icy-Metaint":  {"16000"}, // see [proto.Metaint]
		})
	})

	t.Run("Subscribe not found", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(
			t.Context(),
			http.MethodGet, "/foo",
			http.NoBody,
		)

		svc.On("Subscribe", req.Context(), domain.StreamSub("foo"), rec).
			Return(0, domain.ErrStreamNotFound).Once()
		mux.ServeHTTP(rec, req)
		svc.AssertExpectations(t)

		assert.Equal(t, http.StatusNotFound, rec.Result().StatusCode)
	})

	t.Run("Publish Title (song)", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(
			t.Context(),
			http.MethodGet, "/admin/metadata",
			http.NoBody,
		)
		req.URL.RawQuery = "mode=updinfo&mount=foo&song=deadb33f"

		svc.On("PublishTitle", req.Context(), domain.StreamPub("foo"), "deadb33f").
			Return(nil).Once()
		mux.ServeHTTP(rec, req)
		svc.AssertExpectations(t)

		assert.Equal(t, http.StatusOK, rec.Result().StatusCode)
	})

	t.Run("Publish Title (artist+title)", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(
			t.Context(),
			http.MethodGet, "/admin/metadata",
			http.NoBody,
		)
		req.URL.RawQuery = "mode=updinfo&mount=foo&artist=deadb33f&title=helloworld"

		svc.On("PublishTitle", req.Context(), domain.StreamPub("foo"), "deadb33f - helloworld").
			Return(nil).Once()
		mux.ServeHTTP(rec, req)
		svc.AssertExpectations(t)

		assert.Equal(t, http.StatusOK, rec.Result().StatusCode)
	})

	t.Run("Publish Title (missing song or artist+title)", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(
			t.Context(),
			http.MethodGet, "/admin/metadata",
			http.NoBody,
		)
		req.URL.RawQuery = "mode=updinfo&mount=foo"

		mux.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusBadRequest, rec.Result().StatusCode)
	})

	t.Run("Publish Title not found", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(
			t.Context(),
			http.MethodGet, "/admin/metadata",
			http.NoBody,
		)
		req.URL.RawQuery = "mode=updinfo&mount=foo&song=deadb33f"

		svc.On("PublishTitle", req.Context(), domain.StreamPub("foo"), "deadb33f").
			Return(domain.ErrStreamNotFound).Once()
		mux.ServeHTTP(rec, req)
		svc.AssertExpectations(t)

		assert.Equal(t, http.StatusNotFound, rec.Result().StatusCode)
	})
}
