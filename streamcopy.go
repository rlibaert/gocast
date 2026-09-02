package main

import (
	"bytes"
	"errors"
	"io"
	"time"

	"github.com/rlibaert/gocast/av"
	"github.com/rlibaert/gocast/domain"
)

// StreamCopyFactory is a factory of [domain.ServiceStreamCopy] alternatives.
type StreamCopyFactory struct {
	RealtimeMuxerOffset *time.Duration
}

func (f *StreamCopyFactory) Default(w io.Writer, r io.Reader) (int64, error) {
	return domain.ServiceStreamCopy(w, r)
}

// AVPacketSafe ensures that the bytestream is a stream of [av.Packet]s
// and preserves their boundaries.
//
// The principle behind this highly empirical function is to buffer every
// reads required to demux a packet before actually copying to the writer.
// Also, note that [bufio] is not used: its fixed-size writes would likely
// cut through packets.
func (f *StreamCopyFactory) AVPacketSafe(w io.Writer, r io.Reader) (int64, error) {
	buf := bytes.NewBuffer(nil)

	demuxer, err := av.NewDemuxer(io.TeeReader(r, buf))
	if err != nil {
		return 0, err
	}
	defer demuxer.Close()

	var n int64
	var muxer av.Muxer = muxerFunc(func(p *av.Packet) error {
		wn, werr := buf.WriteTo(w)
		n += wn
		return errors.Join(werr, av.Discard.Mux(p))
	})

	if f.RealtimeMuxerOffset != nil {
		muxer = av.RealtimeMuxer(muxer, *f.RealtimeMuxerOffset)
	}

	_, err = av.Remux(muxer, demuxer)
	return n, err
}

type muxerFunc func(*av.Packet) error

func (f muxerFunc) Mux(p *av.Packet) error { return f(p) }
