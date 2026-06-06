package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
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
		svcDebounce           = flag.Duration("service.debounce", 15*time.Second, "Ingested stream healthy time")
		httpReadHeaderTimeout = flag.Duration("http.read-header-timeout", 15*time.Second, "Time to read HTTP request headers")
		httpAddr              = flag.String("http.addr", ":8080", "HTTP server binding `host:port`")
		icyAddr               = flag.String("icy.addr", ":8000", "Icecast-like server binding `host:port`")
		srtAddr               = flag.String("srt.addr", ":6000", "SRT server binding `host:port`")
	)
	flag.TextVar(&logLevel, "log.level", logLevel, "logging `level`")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: logLevel,
	}))
	slog.SetDefault(logger)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	svcHooks := domain.ServiceHooks{
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

	svc := domain.NewService(svcHooks, *svcDebounce)
	svc, metricsWriter := observability.ObservableService(svc, logger)

	domain.ServiceResetFallbacks(svc, map[domain.StreamSub][]domain.StreamPub{
		"toto": {"bar", "baz"},
		"tata": {"quu", "foo"},
	})

	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		mux := http.NewServeMux()
		protohttp.ServiceRegisterer{
			Service: svc,
		}.Register(mux)
		mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
			metricsWriter(w, "gocast_")
			for sub, pub := range domain.ServiceStreamsMap(svc) {
				fmt.Fprintf(w, "%sstreams_map{pub=%q,sub=%q} 1\n", "gocast_", pub, sub)
			}
			metrics.WritePrometheus(w, true)
		})
		srv := &http.Server{
			BaseContext:       func(net.Listener) context.Context { return ctx },
			ErrorLog:          slog.NewLogLogger(logger.With("srv", "http").Handler(), slog.LevelWarn),
			Addr:              *httpAddr,
			ReadHeaderTimeout: *httpReadHeaderTimeout,
			Handler:           mux,
		}

		eg.Go(func() error {
			<-ctx.Done()
			srv.ErrorLog.Print("server shutting down")
			ctx2, cancel2 := context.WithTimeout(context.Background(), time.Minute)
			defer cancel2()
			return srv.Shutdown(ctx2)
		})

		srv.ErrorLog.Print("server starting")
		defer srv.ErrorLog.Print("server stopped")
		return srv.ListenAndServe()
	})

	eg.Go(func() error {
		mux := http.NewServeMux()
		protoicy.ServiceRegisterer{
			Service: svc,
		}.Register(mux)
		srv := &http.Server{
			BaseContext:       func(net.Listener) context.Context { return ctx },
			ErrorLog:          slog.NewLogLogger(logger.With("srv", "icy").Handler(), slog.LevelWarn),
			Addr:              *icyAddr,
			ReadHeaderTimeout: *httpReadHeaderTimeout,
			Handler:           mux,
		}

		eg.Go(func() error {
			<-ctx.Done()
			srv.ErrorLog.Print("server shutting down")
			ctx2, cancel2 := context.WithTimeout(context.Background(), time.Minute)
			defer cancel2()
			return srv.Shutdown(ctx2)
		})

		srv.ErrorLog.Print("server starting")
		defer srv.ErrorLog.Print("server stopped")
		return srv.ListenAndServe()
	})

	eg.Go(func() error {
		srvErrorLog := slog.NewLogLogger(logger.With("srv", "srt").Handler(), slog.LevelWarn)
		srv := &srt.Server{
			Addr:   *srtAddr,
			Config: new(srt.DefaultConfig()),
		}
		srv.Config.Logger = &srtLogger{srvErrorLog}
		protosrt.ServiceRegisterer{
			BaseContext: func() context.Context { return ctx },
			Service:     svc,
		}.Register(srv)

		eg.Go(func() error {
			<-ctx.Done()
			srvErrorLog.Print("server shutting down")
			srv.Shutdown()
			return nil
		})

		srvErrorLog.Print("server starting")
		defer srvErrorLog.Print("server stopped")
		return srv.ListenAndServe()
	})

	logger.Error("exiting", "err", eg.Wait(), "cause", context.Cause(ctx))
}

type srtLogger struct{ l *log.Logger }

func (l *srtLogger) Listen() <-chan srt.Log     { panic("implementation not needed") }
func (l *srtLogger) Close()                     { panic("implementation not needed") }
func (l *srtLogger) HasTopic(topic string) bool { return strings.HasSuffix(topic, ":error") }

func (l *srtLogger) Print(topic string, _ uint32, _ int, message func() string) {
	if l.HasTopic(topic) {
		l.l.Print(message())
	}
}
