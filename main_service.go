package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/rlibaert/gocast/av"
	"github.com/rlibaert/gocast/domain"
)

func serviceHooks(logger *slog.Logger, set *metrics.Set) domain.ServiceHooks {
	type counters struct {
		total    *metrics.Counter
		inflight *metrics.Counter
		histo    *metrics.Histogram
	}
	countersInc := func(c counters) { c.total.Inc(); c.inflight.Inc() }
	countersDec := func(c counters, d time.Duration) { c.inflight.Dec(); c.histo.Update(d.Seconds()) }

	pubCounters := counters{
		total:    set.GetOrCreateCounter("gocast_" + "streams_pub_total"),
		inflight: set.GetOrCreateCounter("gocast_" + "streams_pub_in_flight"),
		histo:    set.GetOrCreateHistogram("gocast_" + "streams_pub_seconds"),
	}
	subsCounters := sync.Map{}

	return domain.ServiceHooks{
		PublishStartStop: func(ctx context.Context, s domain.StreamPub) func() {
			start := time.Now()
			logger.InfoContext(ctx, "publishing", "stream", s)
			countersInc(pubCounters)
			return func() {
				dur := time.Since(start)
				countersDec(pubCounters, dur)
				logger.InfoContext(ctx, "unpublished", "stream", s, "dur_ms", dur.Milliseconds())
			}
		},
		SubscribeStartStop: func(ctx context.Context, s domain.StreamSub) func() {
			v, ok := subsCounters.Load(s)
			if !ok {
				labels := fmt.Sprintf("{sub=%q}", s)
				v, _ = subsCounters.LoadOrStore(s, counters{
					total:    set.GetOrCreateCounter("gocast_" + "streams_sub_total" + labels),
					inflight: set.GetOrCreateCounter("gocast_" + "streams_sub_in_flight" + labels),
					histo:    set.GetOrCreateHistogram("gocast_" + "streams_sub_seconds" + labels),
				})
			}
			subCounters := v.(counters) //nolint: errcheck // always valid

			start := time.Now()
			logger.InfoContext(ctx, "subscribing", "stream", s)
			countersInc(subCounters)
			return func() {
				dur := time.Since(start)
				countersDec(subCounters, dur)
				logger.InfoContext(ctx, "unsubscribed", "stream", s, "dur_ms", dur.Milliseconds())
			}
		},
	}
}

// serviceStreamCopy is like [domain.ServiceStreamCopy] but preserves packet boudaries.
func serviceStreamCopy(w io.Writer, r io.Reader) (int64, error) {
	buf := bytes.NewBuffer(nil)

	demuxer, err := av.NewDemuxer(io.TeeReader(r, buf))
	if err != nil {
		return 0, err
	}
	defer demuxer.Close()

	n := int64(0)
	_, err = av.Remux(av.Discard, demuxerFunc(func(p *av.Packet) error {
		derr := demuxer.Demux(p)
		if derr != nil {
			return derr
		}
		wn, werr := buf.WriteTo(w)
		n += wn
		return werr
	}))
	return n, err
}

type demuxerFunc func(*av.Packet) error

func (f demuxerFunc) Demux(p *av.Packet) error { return f(p) }
