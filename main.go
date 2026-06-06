package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/VictoriaMetrics/metrics"
	srt "github.com/datarhei/gosrt"
	"github.com/rlibaert/gocast/domain"
	"github.com/rlibaert/gocast/observability"
	protoconfig "github.com/rlibaert/gocast/protos/proto-config"
	protohttp "github.com/rlibaert/gocast/protos/proto-http"
	protoicy "github.com/rlibaert/gocast/protos/proto-icy"
	protosrt "github.com/rlibaert/gocast/protos/proto-srt"
	"golang.org/x/sync/errgroup"
)

func main() {
	//nolint:golines
	var (
		logLevel              = slog.LevelInfo
		svcDebounce           = flag.Duration("service.debounce", 15*time.Second, "Ingested stream healthy time")
		configFilename        = flag.String("config.filename", "/etc/gocast/config.json", "Configuration file `path`")
		httpReadHeaderTimeout = flag.Duration("http.read-header-timeout", 15*time.Second, "Time to read HTTP request headers")
		httpAddr              = flag.String("http.addr", ":8080", "HTTP server binding `host:port`")
		icyAddr               = flag.String("icy.addr", ":8000", "Icecast-like server binding `host:port`")
		srtAddr               = flag.String("srt.addr", ":6000", "SRT server binding `host:port`")
	)
	flag.TextVar(&logLevel, "log.level", logLevel, "logging `level`")
	flag.TextVar(&protoicy.Metaint, "icy.metaint", protoicy.Metaint, "Icecast in-band metadata `bytes` interval")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: logLevel,
	}))
	slog.SetDefault(logger)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
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
			metrics.GetOrCreateHistogram(fmt.Sprintf("%sstreams_sub_seconds{sub=%q}", "gocast_", s)).Update(dur.Seconds()) //nolint:golines
			logger.InfoContext(ctx, "unsubscribed", "stream", s, "dur_ms", dur.Milliseconds())
		},
	}

	svc := domain.NewService(svcHooks, streamCopy, *svcDebounce)
	svc, metricsWriter := observability.ObservableService(svc, logger)

	eg, ctx := errgroup.WithContext(ctx)
	goWatchConfig(ctx, eg, svc, *configFilename, syscall.SIGHUP, logger)
	goServeHTTP(ctx, eg, svc, *httpAddr, *httpReadHeaderTimeout, logger, metricsWriter)
	goServeICY(ctx, eg, svc, *icyAddr, *httpReadHeaderTimeout, logger)
	goServeSRT(ctx, eg, svc, *srtAddr, logger)
	logger.Error("exiting", "err", eg.Wait(), "cause", context.Cause(ctx))
}

func generateFromJSONFile[T any](ch chan<- *T, filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	var v T
	err = json.NewDecoder(f).Decode(&v)
	if err != nil {
		return err
	}
	ch <- &v

	return nil
}

func goWatchConfig(
	ctx context.Context,
	eg *errgroup.Group,
	svc domain.Service,
	filename string,
	reload os.Signal,
	logger *slog.Logger,
) {
	configs := make(chan *protoconfig.Config)
	protoconfig.ServiceRegisterer{Service: svc}.Register(configs)
	eg.Go(func() error {
		defer close(configs)

		if err := generateFromJSONFile(configs, filename); err != nil {
			return err
		}

		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, reload)
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()

			case sig := <-sigs:
				logger.Info("reloaded config", "signal", sig, "err", generateFromJSONFile(configs, filename))
			}
		}
	})
}

func goServe(parent context.Context, eg *errgroup.Group, srv *http.Server) {
	eg.Go(func() error {
		<-parent.Done()
		srv.ErrorLog.Print("server shutting down")
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		return errors.Join(srv.Shutdown(ctx), srv.Close())
	})
	eg.Go(func() error {
		srv.ErrorLog.Print("server starting")
		defer srv.ErrorLog.Print("server stopped")
		return srv.ListenAndServe()
	})
}

func goServeHTTP(
	ctx context.Context,
	eg *errgroup.Group,
	svc domain.Service,
	addr string,
	rht time.Duration,
	logger *slog.Logger,
	metricsWriter func(io.Writer, string),
) {
	mux := http.NewServeMux()
	protohttp.ServiceRegisterer{Service: svc}.Register(mux)
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		metricsWriter(w, "gocast_")
		for sub, pub := range domain.ServiceStreamsMap(svc) {
			fmt.Fprintf(w, "%sstreams_map{pub=%q,sub=%q} 1\n", "gocast_", pub, sub)
		}
		metrics.WritePrometheus(w, true)
	})
	goServe(ctx, eg, &http.Server{
		BaseContext:       func(net.Listener) context.Context { return ctx },
		ErrorLog:          slog.NewLogLogger(logger.With("srv", "http").Handler(), slog.LevelWarn),
		Addr:              addr,
		ReadHeaderTimeout: rht,
		Handler:           mux,
	})
}

func goServeICY(
	ctx context.Context,
	eg *errgroup.Group,
	svc domain.Service,
	addr string,
	rht time.Duration,
	logger *slog.Logger,
) {
	mux := http.NewServeMux()
	protoicy.ServiceRegisterer{Service: svc}.Register(mux)
	goServe(ctx, eg, &http.Server{
		BaseContext:       func(net.Listener) context.Context { return ctx },
		ErrorLog:          slog.NewLogLogger(logger.With("srv", "icy").Handler(), slog.LevelWarn),
		Addr:              addr,
		ReadHeaderTimeout: rht,
		Handler:           mux,
	})
}

func goServeSRT(
	ctx context.Context,
	eg *errgroup.Group,
	svc domain.Service,
	addr string,
	logger *slog.Logger,
) {
	srvErrorLog := slog.NewLogLogger(logger.With("srv", "srt").Handler(), slog.LevelWarn)
	srv := &srt.Server{
		Addr:   addr,
		Config: new(srt.DefaultConfig()),
	}
	srv.Config.Logger = &srtLogger{srvErrorLog}
	protosrt.ServiceRegisterer{
		BaseContext: func() context.Context { return ctx },
		Service:     svc,
	}.Register(srv)

	eg.Go(func() error {
		srvErrorLog.Print("server starting")
		defer srvErrorLog.Print("server stopped")

		err := srv.Listen()
		if err != nil {
			return err
		}

		// Making sure to start shutdown goroutine after listening because
		// [srt.Server.ListenAndServe] still works after [srt.Server.Shutdown].
		eg.Go(func() error {
			<-ctx.Done()
			srvErrorLog.Print("server shutting down")
			srv.Shutdown()
			return ctx.Err()
		})

		return srv.Serve()
	})
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
