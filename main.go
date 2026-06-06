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
	"time"

	"github.com/VictoriaMetrics/metrics"
	srt "github.com/datarhei/gosrt"
	"github.com/rlibaert/gocast/domain"
	"github.com/rlibaert/gocast/observability"
	protohttp "github.com/rlibaert/gocast/protos/proto-http"
	protoicy "github.com/rlibaert/gocast/protos/proto-icy"
	protosrt "github.com/rlibaert/gocast/protos/proto-srt"
	"golang.org/x/sync/errgroup"
)

func main() {
	var (
		logLevel              = slog.LevelInfo
		httpAddr              = flag.String("http.addr", ":8080", "HTTP server binding `host:port`")
		httpReadHeaderTimeout = flag.Duration("http.read-header-timeout", 15*time.Second, "")
		icyAddr               = flag.String("icy.addr", ":8000", "Icecast-like server binding `host:port`")
		srtAddr               = flag.String("srt.addr", ":6000", "SRT server binding `host:port`")
		svcDebounce           = flag.Duration("service.debounce", 15*time.Second, "ingested stream healthy time")
	)
	flag.TextVar(&logLevel, "log.level", logLevel, "logging `level`")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: logLevel,
	}))
	slog.SetDefault(logger)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	svcHooks := domain.StreamsServiceHooks{
		PublishStart: func(ctx context.Context, s domain.StreamPub) {
			logger.InfoContext(ctx, "publishing", "stream", s)
			metrics.GetOrCreateCounter(fmt.Sprintf("%sstreams_pub_total", "gocast_")).Inc()
			metrics.GetOrCreateCounter(fmt.Sprintf("%sstreams_pub_in_flight", "gocast_")).Inc()
		},
		PublishStop: func(ctx context.Context, s domain.StreamPub, start time.Time) {
			dur := time.Since(start)
			metrics.GetOrCreateCounter(fmt.Sprintf("%sstreams_pub_in_flight", "gocast_")).Dec()
			metrics.GetOrCreateHistogram(fmt.Sprintf("%sstreams_pub_seconds", "gocast_")).Update(dur.Seconds())
			logger.InfoContext(ctx, "unpublished", "stream", s, "dur_ms", dur.Milliseconds())
		},
		SubscribeStart: func(ctx context.Context, s domain.StreamSub) {
			logger.InfoContext(ctx, "subscribing", "stream", s)
			metrics.GetOrCreateCounter(fmt.Sprintf("%sstreams_sub_total{sub=%q}", "gocast_", s)).Inc()
			metrics.GetOrCreateCounter(fmt.Sprintf("%sstreams_sub_in_flight{sub=%q}", "gocast_", s)).Inc()
		},
		SubscribeStop: func(ctx context.Context, s domain.StreamSub, start time.Time) {
			dur := time.Since(start)
			metrics.GetOrCreateCounter(fmt.Sprintf("%sstreams_sub_in_flight{sub=%q}", "gocast_", s)).Dec()
			metrics.GetOrCreateHistogram(fmt.Sprintf("%sstreams_sub_seconds{sub=%q}", "gocast_", s)).Update(dur.Seconds())
			logger.InfoContext(ctx, "unsubscribed", "stream", s, "dur_ms", dur.Milliseconds())
		},
	}

	svc := domain.NewStreamsService(svcHooks, *svcDebounce)
	svc, metricsWriter := observability.ObservableStreamsService(svc, logger)

	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		mux := http.NewServeMux()
		protohttp.ServiceRegisterer{
			StreamsService: svc,
		}.Register(mux)
		mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
			metricsWriter(w, "gocast_")
			for s, p := range domain.StreamsServiceStreamsMap(svc) {
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

		eg.Go(func() error {
			<-ctx.Done()
			logger.Info("http server shutting down")
			ctx2, cancel2 := context.WithTimeout(context.Background(), time.Minute)
			defer cancel2()
			return srv.Shutdown(ctx2)
		})

		logger.Info("http server starting")
		defer logger.Info("http server stopped")
		return srv.ListenAndServe()
	})

	eg.Go(func() error {
		mux := http.NewServeMux()
		protoicy.ServiceRegisterer{
			StreamsService: svc,
		}.Register(mux)
		srv := &http.Server{
			BaseContext:       func(net.Listener) context.Context { return ctx },
			ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
			Addr:              *icyAddr,
			ReadHeaderTimeout: *httpReadHeaderTimeout,
			Handler:           mux,
		}

		eg.Go(func() error {
			<-ctx.Done()
			logger.Info("icy server shutting down")
			ctx2, cancel2 := context.WithTimeout(context.Background(), time.Minute)
			defer cancel2()
			return srv.Shutdown(ctx2)
		})

		logger.Info("icy server starting")
		defer logger.Info("icy server stopped")
		return srv.ListenAndServe()
	})

	eg.Go(func() error {
		srv := &srt.Server{
			Addr:   *srtAddr,
			Config: new(srt.DefaultConfig()),
		}
		protosrt.ServiceRegisterer{
			BaseContext:    func() context.Context { return ctx },
			StreamsService: svc,
		}.Register(srv)

		eg.Go(func() error {
			<-ctx.Done()
			logger.Info("srt server shutting down")
			srv.Shutdown()
			return nil
		})

		logger.Info("srt server starting")
		defer logger.Info("srt server stopped")
		return srv.ListenAndServe()
	})

	logger.Error("exiting", "err", eg.Wait(), "cause", context.Cause(ctx))
}
