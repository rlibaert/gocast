package main

import (
	"bytes"
	"io"

	"github.com/rlibaert/gocast/av"
	_ "github.com/rlibaert/gocast/domain" // for documentation links to [domain]
)

// streamCopy is like [domain.ServiceStreamCopy] but preserves packet boudaries.
func streamCopy(w io.Writer, r io.Reader) (int64, error) {
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
