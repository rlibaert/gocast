package observability

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/rlibaert/gocast/domain"
)

func ObservableStreamingService(
	svc domain.StreamingService,
	logger *slog.Logger,
) (domain.StreamingService, func(io.Writer, string)) {
	observable := observableStreamingService{
		svc,
		newLogsFunc(logger),
		newMetricsMap(methodsFor[domain.StreamingService](true)...),
	}
	return observable, func(w io.Writer, prefix string) {
		for k, v := range observable.metrics {
			fmt.Fprintf(w, "%sfunc_calls_total{name=%q} %d\n", prefix, k, v.total.Load())
			fmt.Fprintf(w, "%sfunc_calls_in_flight{name=%q} %d\n", prefix, k, v.inFlight.Load())
		}
	}
}

type observableStreamingService struct {
	domain.StreamingService

	logs    func(fname) *logs
	metrics map[fname]*metrics
}

func (svc observableStreamingService) Publish(ctx context.Context, s domain.StreamPub, r io.Reader) (_ int64, err error) { //nolint: golines,nonamedreturns // simpler use with defer
	const fname = "StreamingService.Publish"
	defer svc.logs(fname).in(s).out(time.Now(), &err)
	defer svc.metrics[fname].in().out()
	return svc.StreamingService.Publish(ctx, s, r)
}

func (svc observableStreamingService) Subscribe(ctx context.Context, s domain.StreamSub, w io.Writer) (_ int64, err error) { //nolint: golines,nonamedreturns // simpler use with defer
	const fname = "StreamingService.Subscribe"
	defer svc.logs(fname).in(s).out(time.Now(), &err)
	defer svc.metrics[fname].in().out()
	return svc.StreamingService.Subscribe(ctx, s, w)
}
