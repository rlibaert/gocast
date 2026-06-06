package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/VictoriaMetrics/metrics"
	srt "github.com/datarhei/gosrt"
	"github.com/rlibaert/gocast/domain"
	"github.com/rlibaert/gocast/observability"
	protohttp "github.com/rlibaert/gocast/protos/proto-http"
	protoicy "github.com/rlibaert/gocast/protos/proto-icy"
	protosrt "github.com/rlibaert/gocast/protos/proto-srt"
)

func main() {
	var (
		logLevel              = slog.LevelInfo
		httpAddr              = flag.String("http.addr", ":8080", "HTTP server binding `host:port`")
		httpReadHeaderTimeout = flag.Duration("http.read-header-timeout", 15*time.Second, "")
		srtAddr               = flag.String("srt.addr", ":6000", "SRT server binding `host:port`")
		svcDebounce           = flag.Duration("service.debounce", 15*time.Second, "ingested stream healthy time")
	)
	flag.TextVar(&logLevel, "log.level", logLevel, "logging `level`")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: logLevel,
	}))
	slog.SetDefault(logger)

	logger.Info("program starting")
	defer logger.Info("program exiting")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	svc := domain.NewStreamingService(
		domain.StreamingServiceHooks{
			StreamPubStart: func(ctx context.Context, s domain.StreamPub) {
				logger.InfoContext(ctx, "StreamingServiceHooks.StreamPubStart", "stream", s)
				metrics.GetOrCreateCounter(fmt.Sprintf("%sstreams_pub_total", "gocast_")).Inc()
				metrics.GetOrCreateCounter(fmt.Sprintf("%sstreams_pub_in_flight", "gocast_")).Inc()
			},
			StreamPubStop: func(ctx context.Context, s domain.StreamPub, start time.Time) {
				dur := time.Since(start)
				metrics.GetOrCreateCounter(fmt.Sprintf("%sstreams_pub_in_flight", "gocast_")).Dec()
				metrics.GetOrCreateHistogram(fmt.Sprintf("%sstreams_pub_seconds{sub=%q}", "gocast_", s)).Update(dur.Seconds())
				logger.InfoContext(ctx, "StreamingServiceHooks.StreamPubStop", "stream", s, "dur", dur)
			},
			StreamSubStart: func(ctx context.Context, s domain.StreamSub) {
				logger.InfoContext(ctx, "StreamingServiceHooks.StreamSubStart", "stream", s)
				metrics.GetOrCreateCounter(fmt.Sprintf("%sstreams_sub_total{sub=%q}", "gocast_", s)).Inc()
				metrics.GetOrCreateCounter(fmt.Sprintf("%sstreams_sub_in_flight{sub=%q}", "gocast_", s)).Inc()
			},
			StreamSubStop: func(ctx context.Context, s domain.StreamSub, start time.Time) {
				dur := time.Since(start)
				metrics.GetOrCreateCounter(fmt.Sprintf("%sstreams_sub_in_flight{sub=%q}", "gocast_", s)).Dec()
				metrics.GetOrCreateHistogram(fmt.Sprintf("%sstreams_sub_seconds{sub=%q}", "gocast_", s)).Update(dur.Seconds())
				logger.InfoContext(ctx, "StreamingServiceHooks.StreamSubStop", "stream", s, "dur", dur)
			},
		},
		*svcDebounce,
	)
	svc, metricsWriter := observability.ObservableStreamingService(svc, logger)

	wg := sync.WaitGroup{}
	defer wg.Wait()

	wg.Go(func() {
		mux := http.NewServeMux()
		protohttp.ServiceRegisterer{
			StreamingService: svc,
		}.Register(mux)
		protoicy.ServiceRegisterer{
			StreamingService: svc,
		}.Register(mux)
		mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
			metricsWriter(w, "gocast_")
			for s, p := range domain.StreamingServiceStreamsMap(svc) {
				fmt.Fprintf(w, "%sstreams_map{sub=%q,pub=%q} 1\n", "gocast_", s, p)
			}
			metrics.WritePrometheus(w, true)
		})
		srv := &http.Server{
			BaseContext:       func(net.Listener) context.Context { return ctx },
			ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
			Addr:              *httpAddr,
			ReadHeaderTimeout: *httpReadHeaderTimeout,
			Handler:           mux,
		}

		wg.Go(func() {
			<-ctx.Done()
			ctx2, cancel2 := context.WithTimeout(context.Background(), time.Minute)
			defer cancel2()
			logger.Info("server shutdown", "proto", "http", "err", srv.Shutdown(ctx2))
		})

		logger.Info("server returned", "proto", "http", "err", srv.ListenAndServe())
	})

	wg.Go(func() {
		srv := &srt.Server{
			Addr:   *srtAddr,
			Config: new(srt.DefaultConfig()),
		}
		protosrt.ServiceRegisterer{
			BaseContext:      func() context.Context { return ctx },
			StreamingService: svc,
		}.Register(srv)

		wg.Go(func() {
			<-ctx.Done()
			srv.Shutdown()
			logger.Info("server shutdown", "proto", "srt")
		})

		logger.Info("server returned", "proto", "srt", "err", srv.ListenAndServe())
	})
}
