// Package av provides functions to handle multimedia streams.
// It relies on CGO and [FFmpeg]'s libav* libraries.
//
// [FFmpeg]: https://ffmpeg.org
package av

/*
#cgo LDFLAGS: -Wl,--start-group -lavutil -lavformat -lavcodec -Wl,--end-group -lm -lz

#include <stdio.h>
#include <libavutil/log.h>

extern void go_log(int, char *, int);

static void log_callback(void *avcl, int level, const char *fmt, va_list vl) {
    char line[1024];
    int length = vsnprintf(line, sizeof(line), fmt, vl);
    go_log(level, line, length);
}

static void set_log_callback() {
    av_log_set_callback(log_callback);
}
*/
import "C"
import (
	"context"
	"log/slog"
	"unsafe"
)

func init() { C.set_log_callback() } //nolint: gochecknoinits // set global logger of libav

//export go_log
func go_log(level C.int, line *C.char, length C.int) {
	lvl := slog.LevelInfo
	switch level {
	case C.AV_LOG_QUIET:
		return
	case C.AV_LOG_PANIC, C.AV_LOG_FATAL, C.AV_LOG_ERROR:
		lvl = slog.LevelError
	case C.AV_LOG_WARNING:
		lvl = slog.LevelWarn
	case C.AV_LOG_INFO, C.AV_LOG_VERBOSE:
		lvl = slog.LevelInfo
	case C.AV_LOG_DEBUG, C.AV_LOG_TRACE:
		lvl = slog.LevelDebug
	}
	slog.Log( //nolint: sloglint // as per global logger of libav
		context.Background(),
		lvl,
		unsafe.String((*byte)(unsafe.Pointer(line)), length),
	)
}
