//nolint: golines,nonamedreturns,goimports,nolintlint
package observability

import (
	"context"
	"io"
	"log/slog"
	"time"

	"github.com/rlibaert/gocast/domain"
)

type observableStreamingService struct {
	domain.StreamingService

	logger *slog.Logger
}

func ObservableStreamingService(s domain.StreamingService, l *slog.Logger) domain.StreamingService {
	return observableStreamingService{s, l}
}

func (svc observableStreamingService) onEnter(ctx context.Context, method string, attrs ...any) {
	attrs = append(attrs, slog.String("method", method))
	svc.logger.DebugContext(ctx, "service entered", attrs...)
}

func (svc observableStreamingService) onLeave(ctx context.Context, method string, entered time.Time, pErr *error, attrs ...any) {
	attrs = append(attrs, slog.String("method", method), slog.Int64("dur_ms", time.Since(entered).Milliseconds()))
	if pErr != nil && *pErr != nil {
		attrs = append(attrs, slog.Any("err", *pErr))
	}
	svc.logger.InfoContext(ctx, "service left", attrs...)
}

func (svc observableStreamingService) Publish(ctx context.Context, s domain.StreamPub, r io.Reader) (_ int64, err error) {
	svc.onEnter(ctx, "publish", slog.String("stream", string(s)))
	defer svc.onLeave(ctx, "publish", time.Now(), &err, slog.String("stream", string(s)))
	return svc.StreamingService.Publish(ctx, s, r)
}

func (svc observableStreamingService) Subscribe(ctx context.Context, s domain.StreamSub, w io.Writer) (_ int64, err error) {
	svc.onEnter(ctx, "subscribe", slog.String("stream", string(s)))
	defer svc.onLeave(ctx, "subscribe", time.Now(), &err, slog.String("stream", string(s)))
	return svc.StreamingService.Subscribe(ctx, s, w)
}
