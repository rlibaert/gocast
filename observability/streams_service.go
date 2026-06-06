package observability

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/rlibaert/gocast/domain"
)

func ObservableStreamsService(
	svc domain.StreamsService,
	logger *slog.Logger,
) (domain.StreamsService, func(io.Writer, string)) {
	observable := observableStreamsService{
		svc,
		newLogsFunc(logger),
		newMetricsMap(methodsFor[domain.StreamsService](true)...),
	}
	return observable, func(w io.Writer, prefix string) {
		for k, v := range observable.metrics {
			fmt.Fprintf(w, "%sfunc_calls_total{name=%q} %d\n", prefix, k, v.total.Load())
			fmt.Fprintf(w, "%sfunc_calls_in_flight{name=%q} %d\n", prefix, k, v.inFlight.Load())
		}
	}
}

type observableStreamsService struct {
	domain.StreamsService

	logs    func(fname) *logs
	metrics map[fname]*metrics
}

func (svc observableStreamsService) Publish(ctx context.Context, s domain.StreamPub, r io.Reader) (_ int64, err error) { //nolint: golines,nonamedreturns // simpler use with defer
	const fname = "StreamsService.Publish"
	defer svc.logs(fname).in(s).out(time.Now(), &err)
	defer svc.metrics[fname].in().out()
	return svc.StreamsService.Publish(ctx, s, r)
}

func (svc observableStreamsService) Subscribe(ctx context.Context, s domain.StreamSub, w io.Writer) (_ int64, err error) { //nolint: golines,nonamedreturns // simpler use with defer
	const fname = "StreamsService.Subscribe"
	defer svc.logs(fname).in(s).out(time.Now(), &err)
	defer svc.metrics[fname].in().out()
	return svc.StreamsService.Subscribe(ctx, s, w)
}

func (svc observableStreamsService) PublishTitle(ctx context.Context, s domain.StreamPub, title string) (err error) { //nolint: golines,nonamedreturns // simpler use with defer
	const fname = "StreamsService.PublishTitle"
	defer svc.logs(fname).in(s, title).out(time.Now(), &err)
	defer svc.metrics[fname].in().out()
	return svc.StreamsService.PublishTitle(ctx, s, title)
}
