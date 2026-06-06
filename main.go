package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	srt "github.com/datarhei/gosrt"
	"github.com/rlibaert/gocast/domain"
	"github.com/rlibaert/gocast/observability"
	protohttp "github.com/rlibaert/gocast/protos/proto-http"
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

	svc := domain.NewStreamingService(*svcDebounce)
	svc = observability.ObservableStreamingService(svc, logger)

	wg := sync.WaitGroup{}
	defer wg.Wait()

	wg.Go(func() {
		mux := http.NewServeMux()
		protohttp.ServiceRegisterer{
			StreamingService: svc,
		}.Register(mux)
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
