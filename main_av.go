package main

import (
	"bytes"
	"io"

	"github.com/rlibaert/gocast/av"
	"github.com/rlibaert/gocast/domain"
)

func init() {
	// preserve packet boudaries
	domain.StreamCopy = func(dst io.Writer, src io.Reader) (int64, error) {
		buf := bytes.NewBuffer(nil)

		demuxer, err := av.NewDemuxer(io.TeeReader(src, buf))
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
			wn, werr := buf.WriteTo(dst)
			n += wn
			return werr
		}))
		return n, err
	}
}

type demuxerFunc func(*av.Packet) error

func (f demuxerFunc) Demux(p *av.Packet) error { return f(p) }
