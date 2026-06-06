// Package observability provides primitives to produce logs and metrics.
package observability

import (
	"context"
	"log/slog"
	"reflect"
	"strconv"
	"sync/atomic"
	"time"
)

// fname is a function name.
type fname string

func methodsFor[T any](skipUnexported bool) []fname {
	t := reflect.TypeFor[T]()
	s := make([]fname, 0, t.NumMethod())

	for m := range t.Methods() {
		if skipUnexported && !m.IsExported() {
			continue
		}
		s = append(s, fname(t.Name()+"."+m.Name))
	}

	return s
}

type logs struct {
	l   *slog.Logger
	msg string
}

func (l *logs) in(args ...any) *logs {
	l.l.LogAttrs(context.Background(), slog.LevelDebug, l.msg, slog.GroupAttrs("in", slog.Any("args", args)))
	return l
}

func (l *logs) out(started time.Time, perr *error) {
	attrs := []slog.Attr{slog.Int64("dur_ms", time.Since(started).Milliseconds()), {}}
	if perr != nil && *perr != nil {
		attrs[1] = slog.Any("err", *perr)
	}
	l.l.LogAttrs(context.Background(), slog.LevelDebug, l.msg, slog.GroupAttrs("out", attrs...))
}

func newLogsFunc(l *slog.Logger) func(fname) *logs {
	var counter atomic.Uint64
	return func(fname fname) *logs {
		return &logs{l, string(strconv.AppendUint([]byte(fname+"#"), counter.Add(1), 10))} //nolint: mnd // decimal
	}
}

type metrics struct {
	total    atomic.Uint64
	inFlight atomic.Uint64
}

func (m *metrics) in() *metrics {
	m.total.Add(1)
	m.inFlight.Add(1)
	return m
}

func (m *metrics) out() {
	m.inFlight.Add(^uint64(0))
}

func newMetricsMap(fnames ...fname) map[fname]*metrics {
	m := make(map[fname]*metrics, len(fnames))
	s := make([]metrics, len(fnames))
	for i, fname := range fnames {
		m[fname] = &s[i]
	}
	return m
}
