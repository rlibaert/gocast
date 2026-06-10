package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
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
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	main2(ctx)
}

func main2(ctx context.Context) {
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
	flag.VisitAll(func(f *flag.Flag) {
		env := "GOCAST_" + strings.NewReplacer(".", "_", "-", "").Replace(strings.ToUpper(f.Name))
		f.Usage = fmt.Sprintf("%s (env %s)", f.Usage, env)
		if value, ok := os.LookupEnv(env); ok {
			f.Value.Set(value) //nolint: errcheck,gosec // discard invalid value
		}
	})
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: logLevel,
	}))
	slog.SetDefault(logger)

	metricsWriterProcess := metrics.WriteProcessMetrics
	metrics := metrics.NewSet()
	metrics.RegisterMetricsWriter(metricsWriterProcess)

	svc := domain.NewService(serviceHooks(logger, metrics), serviceStreamCopy, *svcDebounce)
	metrics.RegisterMetricsWriter(func(w io.Writer) {
		for sub, pub := range domain.ServiceStreamsMap(svc) {
			fmt.Fprintf(w, "%sstreams_map{pub=%q,sub=%q} 1\n", "gocast_", pub, sub)
		}
	})

	svc, svcMetricsWriter := observability.ObservableService(svc, logger, "gocast_")
	metrics.RegisterMetricsWriter(svcMetricsWriter)

	wg := sync.WaitGroup{}
	defer wg.Wait()

	var (
		svcSigh = svcSignalHandler(svc, logger, *configFilename)
		svcHTTP = svcHTTPServer(svc, logger.With("srv", "http"), *httpAddr, *httpReadHeaderTimeout, metrics)
		svcICY  = svcICYServer(svc, logger.With("srv", "icy"), *icyAddr, *httpReadHeaderTimeout)
		svcSRT  = svcSRTServer(svc, logger.With("srv", "srt"), *srtAddr)
	)
	//nolint:errcheck,gosec // such deferred statements usually discard errors
	defer func() {
		shutdownContext, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		wg.Go(func() { svcHTTP.Shutdown(shutdownContext); svcHTTP.Close() })
		wg.Go(func() { svcICY.Shutdown(shutdownContext); svcICY.Close() })
		wg.Go(func() {
			time.Sleep(time.Second) // mitigate race with [srt.Server.ListenAndServe]
			svcSRT.Shutdown()
		})
		logger.Info("routines stopping")
	}()

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	wg.Go(func() { cancel(svcSigh(ctx, syscall.SIGHUP)) })
	wg.Go(func() { cancel(svcHTTP.ListenAndServe()) })
	wg.Go(func() { cancel(svcICY.ListenAndServe()) })
	wg.Go(func() { cancel(svcSRT.ListenAndServe()) })
	logger.Info("routines starting")

	<-ctx.Done()
	logger.Error("context done", "err", ctx.Err(), "cause", context.Cause(ctx))
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

func svcSignalHandler(
	svc domain.Service,
	logger *slog.Logger,
	filename string,
) func(context.Context, ...os.Signal) error {
	configs := make(chan *protoconfig.Config)
	protoconfig.ServiceRegisterer{Service: svc}.Register(configs)

	return func(ctx context.Context, signals ...os.Signal) error {
		if err := generateFromJSONFile(configs, filename); err != nil {
			return err
		}

		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, signals...)

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()

			case sig := <-sigs:
				switch err := generateFromJSONFile(configs, filename); err {
				case nil:
					logger.Info("config reload ok", "signal", sig)
				default:
					logger.Warn("config reload not ok", "signal", sig, "err", err)
				}
			}
		}
	}
}

func svcHTTPServer(
	svc domain.Service,
	logger *slog.Logger,
	addr string,
	rht time.Duration,
	metrics *metrics.Set,
) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		metrics.WritePrometheus(w)
	})
	protohttp.ServiceRegisterer{Service: svc}.Register(mux)
	return &http.Server{
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
		Addr:              addr,
		ReadHeaderTimeout: rht,
		Handler:           mux,
	}
}

func svcICYServer(
	svc domain.Service,
	logger *slog.Logger,
	addr string,
	rht time.Duration,
) *http.Server {
	mux := http.NewServeMux()
	protoicy.ServiceRegisterer{Service: svc}.Register(mux)
	return &http.Server{
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
		Addr:              addr,
		ReadHeaderTimeout: rht,
		Handler:           mux,
	}
}

func svcSRTServer(
	svc domain.Service,
	logger *slog.Logger,
	addr string,
) *srt.Server {
	srvErrorLog := slog.NewLogLogger(logger.Handler(), slog.LevelWarn)
	srv := &srt.Server{
		Addr:   addr,
		Config: new(srt.DefaultConfig()),
	}
	srv.Config.Logger = &srtLogger{srvErrorLog}
	protosrt.ServiceRegisterer{Service: svc}.Register(srv)
	return srv
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
