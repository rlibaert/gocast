package av

/*
#include <libavformat/avformat.h>

extern int cgoReaderRead(void *, uint8_t *, int);
*/
import "C"
import (
	"io"
	"runtime/cgo"
	"syscall"
	"unsafe"
)

type Packet C.AVPacket

type (
	Demuxer interface{ Demux(*Packet) error } // Demuxer reads [Packet]s and returns an error if none are available.
	Muxer   interface{ Mux(*Packet) error }   // Muxer writes [Packet]s to the underlying packet stream.
)

// RemuxPacket is identical to [Remux] except that it stages through the provided
// [Packet] rather than allocating a temporary one ([io.CopyBuffer] style).
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

// Remux muxes to [Muxer] packets demuxed from [Demuxer] ([io.Copy] style).
func Remux(m Muxer, d Demuxer) (int64, error) {
	p := C.av_packet_alloc()
	if p == nil {
		return 0, syscall.ENOMEM
	}
	defer C.av_packet_free(&p)
	return RemuxPacket(m, d, (*Packet)(p))
}

type discard struct{}

func (discard) Mux(p *Packet) error {
	C.av_packet_unref((*C.AVPacket)(p))
	return nil
}

// Discard is a [Muxer] on which all Mux calls simply discards the [Packet].
var Discard Muxer = discard{} //nolint: gochecknoglobals // as per [io.Discard]

// demuxer implements [Demuxer].
type demuxer struct {
	reader *cgo.Handle
	io     *C.AVIOContext
	fmt    *C.AVFormatContext
}

//export cgoReaderRead
func cgoReaderRead(opaque unsafe.Pointer, buf *C.uint8_t, bufsize C.int) C.int {
	r := (*cgo.Handle)(opaque).Value().(io.Reader) //nolint: errcheck // always valid
	n, err := r.Read(unsafe.Slice((*byte)(buf), bufsize))
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

func newDemuxer(r io.Reader) (*demuxer, error) {
	reader := cgo.NewHandle(r)
	readPacketArg, readPacket := unsafe.Pointer(&reader), (*[0]byte)(C.cgoReaderRead)
	io := C.avio_alloc_context(nil, 0, 0, readPacketArg, readPacket, nil, nil)
	fmt := C.avformat_alloc_context()
	if io == nil || fmt == nil {
		C.avformat_free_context(fmt)
		C.avio_context_free(&io)
		reader.Delete()
		return nil, syscall.ENOMEM
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

type DemuxCloser interface {
	Demuxer
	Close() error
}

// NewDemuxer returns a [Demuxer] that demuxes packets by reading the [io.Reader].
func NewDemuxer(r io.Reader) (DemuxCloser, error) {
	return newDemuxer(r)
}
