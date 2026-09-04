package main

import (
	"context"
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
	protohttp "github.com/rlibaert/gocast/protos/proto-http"
	protoicy "github.com/rlibaert/gocast/protos/proto-icy"
	protosrt "github.com/rlibaert/gocast/protos/proto-srt"
)

// build-time information
//
//nolint:gochecknoglobals // set by builder through ldflags
var (
	version  string
	revision string
	date     string
)

type args struct {
	logLevel              slog.Level
	svcDebounce           time.Duration
	configFilename        string
	httpReadHeaderTimeout time.Duration
	httpAddr              string
	icyAddr               string
	srtAddr               string
	streamCopyFactory     StreamCopyFactory
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP)
	defer context.AfterFunc(ctx, func() {
		signal.Stop(signals)
		close(signals)
	})()

	a := &args{}
	//nolint: golines
	{
		flag.TextVar(&a.logLevel, "log.level", slog.LevelInfo, "logging `level`")
		flag.DurationVar(&a.svcDebounce, "service.debounce", 15*time.Second, "Ingested stream healthy time")
		flag.StringVar(&a.configFilename, "config.filename", "/etc/gocast/config.json", "Configuration file `path`")
		flag.DurationVar(&a.httpReadHeaderTimeout, "http.read-header-timeout", 15*time.Second, "Time to read HTTP request headers")
		flag.StringVar(&a.httpAddr, "http.addr", ":8080", "HTTP server binding `[host]:port`")
		flag.StringVar(&a.icyAddr, "icy.addr", ":8000", "Icecast-like server binding `[host]:port`")
		flag.StringVar(&a.srtAddr, "srt.addr", ":6000", "SRT server binding `[host]:port`")
		flag.TextVar(&protoicy.Metaint, "icy.metaint", protoicy.Metaint, "Icecast in-band metadata `bytes` interval")
		flag.Func("stream-copy.pts-muxer-offset", "Stream copy PTS muxer offset `duration`", func(s string) error {
			d, err := time.ParseDuration(s)
			if err != nil {
				return err
			}
			a.streamCopyFactory.PTSMuxerOffset = &d
			return nil
		})

		flag.VisitAll(func(f *flag.Flag) {
			env := "GOCAST_" + strings.NewReplacer(".", "_", "-", "").Replace(strings.ToUpper(f.Name))
			f.Usage = fmt.Sprintf("%s (env %s)", f.Usage, env)
			if value, ok := os.LookupEnv(env); ok {
				f.Value.Set(value) //nolint: errcheck,gosec // discard invalid value
			}
		})
		flag.BoolFunc("version", "print version and exit", func(string) error {
			fmt.Fprintln(os.Stdout, version)
			os.Exit(0)
			return nil
		})
		flag.Parse()
	}

	h := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: a.logLevel})
	l := slog.New(h)
	slog.SetDefault(l)

	main2(ctx, signals, a, l)
}

func main2(ctx context.Context, signals <-chan os.Signal, args *args, logger *slog.Logger) {
	logger.Info("starting Gocast", slog.Group("build",
		"version", version,
		"revision", revision,
		"date", date,
	))

	metricsWriterProcess := metrics.WriteProcessMetrics
	metrics := metrics.NewSet()
	metrics.RegisterMetricsWriter(metricsWriterProcess)

	metricsInfo := fmt.Sprintf("%sinfo{version=%q,revision=%q} 1\n", "gocast_", version, revision)
	metrics.RegisterMetricsWriter(func(w io.Writer) { fmt.Fprint(w, metricsInfo) })

	svc, err := domain.NewService(
		JSONConfigGetter(args.configFilename),
		serviceHooks(logger, metrics),
		args.streamCopyFactory.AVPacketSafe,
		args.svcDebounce)
	if err != nil {
		logger.Error("failed to create service", "err", err)
		os.Exit(1)
	}

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
		svcHTTP = svcHTTPServer(svc, logger.With("srv", "http"), args.httpAddr, args.httpReadHeaderTimeout, metrics)
		svcICY  = svcICYServer(svc, logger.With("srv", "icy"), args.icyAddr, args.httpReadHeaderTimeout)
		svcSRT  = svcSRTServer(svc, logger.With("srv", "srt"), args.srtAddr)
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

	wg.Go(func() {
		for range signals {
			_ = svc.Reconfigure(ctx)
		}
		cancel(nil)
	})
	wg.Go(func() { cancel(svcHTTP.ListenAndServe()) })
	wg.Go(func() { cancel(svcICY.ListenAndServe()) })
	wg.Go(func() { cancel(svcSRT.ListenAndServe()) })
	logger.Info("routines starting")

	<-ctx.Done()
	logger.Error("context done", "err", ctx.Err(), "cause", context.Cause(ctx))
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

// srtLogger is an implementation of [srt.Logger].
type srtLogger struct{ l *log.Logger }

func (l *srtLogger) Listen() <-chan srt.Log     { panic("unexpected method call") }
func (l *srtLogger) Close()                     { panic("unexpected method call") }
func (l *srtLogger) HasTopic(topic string) bool { return strings.HasSuffix(topic, ":error") }

func (l *srtLogger) Print(topic string, _ uint32, _ int, message func() string) {
	if l.HasTopic(topic) {
		l.l.Print(message())
	}
}
