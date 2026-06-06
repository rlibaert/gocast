package av

/*
#include <libavformat/avformat.h>

extern int cgoRead(void *, uint8_t *, int);
*/
import "C"
import (
	"io"
	"runtime/cgo"
	"unsafe"
)

type Packet C.AVPacket

func (p *Packet) Unref() { C.av_packet_unref((*C.AVPacket)(p)) }

type (
	Demuxer interface{ Demux(*Packet) error }
	Muxer   interface{ Mux(*Packet) error }
)

func RemuxPacket(m Muxer, d Demuxer, p *Packet) (int64, error) {
	for count := int64(0); ; count++ {
		err := d.Demux(p)
		if err == io.EOF {
			return count, nil
		}
		if err == nil {
			err = m.Mux(p)
		}
		if err != nil {
			return count, err
		}
	}
}

func Remux(m Muxer, d Demuxer) (int64, error) {
	p := C.av_packet_alloc()
	if p == nil {
		return 0, errNomem
	}
	defer C.av_packet_free(&p)
	return RemuxPacket(m, d, (*Packet)(p))
}

type discard struct{}

func (discard) Mux(p *Packet) error { p.Unref(); return nil }

var Discard Muxer = discard{}

type demuxer struct {
	reader *cgo.Handle
	io     *C.AVIOContext
	fmt    *C.AVFormatContext
}

//export cgoRead
func cgoRead(opaque unsafe.Pointer, buf *C.uint8_t, bufsize C.int) C.int {
	n, err := (*cgo.Handle)(opaque).
		Value().(io.Reader).
		Read(unsafe.Slice((*byte)(buf), bufsize))
	if n > 0 {
		return C.int(n)
	}
	switch err {
	case nil:
		return 0
	case io.EOF:
		return C.AVERROR_EOF
	default:
		return C.AVERROR_EXTERNAL
	}
}

type DemuxCloser interface {
	Demuxer
	Close() error
}

func NewDemuxer(r io.Reader) (DemuxCloser, error) {
	reader := cgo.NewHandle(r)
	readPacketArg, readPacket := unsafe.Pointer(&reader), (*[0]byte)(C.cgoRead)
	io := C.avio_alloc_context(nil, 0, 0, readPacketArg, readPacket, nil, nil)
	fmt := C.avformat_alloc_context()
	if io == nil || fmt == nil {
		C.avformat_free_context(fmt)
		C.avio_context_free(&io)
		reader.Delete()
		return nil, errNomem
	}
	io.direct |= C.AVIO_FLAG_DIRECT
	fmt.pb = io
	fmt.flags |= C.AVFMT_FLAG_CUSTOM_IO

	if err := C.avformat_open_input(&fmt, nil, nil, nil); err < 0 {
		C.avformat_free_context(fmt)
		C.avio_context_free(&io)
		reader.Delete()
		return nil, Error(err)
	}
	d := &demuxer{
		reader: &reader,
		io:     io,
		fmt:    fmt,
	}

	if err := C.avformat_find_stream_info(d.fmt, nil); err < 0 {
		d.Close()
		return nil, Error(err)
	}

	return d, nil
}

func (d *demuxer) Demux(p *Packet) error {
	if err := C.av_read_frame(d.fmt, (*C.AVPacket)(p)); err < 0 {
		switch err {
		case C.AVERROR_EOF:
			return io.EOF
		default:
			return Error(err)
		}
	}
	return nil
}

func (d *demuxer) Close() error {
	C.avformat_close_input(&d.fmt)
	C.avio_context_free(&d.io)
	d.reader.Delete()
	return nil
}
