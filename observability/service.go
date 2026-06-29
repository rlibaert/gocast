package observability

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/rlibaert/gocast/domain"
)

func ObservableService(
	svc domain.Service,
	logger *slog.Logger,
	metricsPrefix string,
) (domain.Service, func(io.Writer)) {
	observable := observableService{
		svc,
		newLogsFunc(logger),
		newMetricsMap(methodsFor[domain.Service](true)...),
	}
	return observable, func(w io.Writer) {
		for k, v := range observable.metrics {
			fmt.Fprintf(w, "%sfunc_calls_total{name=%q} %d\n", metricsPrefix, k, v.total.Load())
			fmt.Fprintf(w, "%sfunc_calls_in_flight{name=%q} %d\n", metricsPrefix, k, v.inFlight.Load())
		}
	}
}

type observableService struct {
	domain.Service

	logs    func(fname) *logs
	metrics map[fname]*metrics
}

func (svc observableService) Reconfigure(ctx context.Context) (err error) { //nolint: golines,nonamedreturns // simpler use with defer
	const fname = "Service.Reconfigure"
	defer svc.logs(fname).in().out(time.Now(), &err)
	defer svc.metrics[fname].in().out()
	return svc.Service.Reconfigure(ctx)
}

func (svc observableService) Publish(ctx context.Context, s domain.StreamPub, r io.Reader) (_ int64, err error) { //nolint: golines,nonamedreturns // simpler use with defer
	const fname = "Service.Publish"
	defer svc.logs(fname).in(s).out(time.Now(), &err)
	defer svc.metrics[fname].in().out()
	return svc.Service.Publish(ctx, s, r)
}

func (svc observableService) Subscribe(ctx context.Context, s domain.StreamSub, w io.Writer) (_ int64, err error) { //nolint: golines,nonamedreturns // simpler use with defer
	const fname = "Service.Subscribe"
	defer svc.logs(fname).in(s).out(time.Now(), &err)
	defer svc.metrics[fname].in().out()
	return svc.Service.Subscribe(ctx, s, w)
}

func (svc observableService) PublishTitle(ctx context.Context, s domain.StreamPub, title string) (err error) { //nolint: golines,nonamedreturns // simpler use with defer
	const fname = "Service.PublishTitle"
	defer svc.logs(fname).in(s, title).out(time.Now(), &err)
	defer svc.metrics[fname].in().out()
	return svc.Service.PublishTitle(ctx, s, title)
}
