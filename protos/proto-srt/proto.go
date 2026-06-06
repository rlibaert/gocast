package proto

import (
	"context"
	"strings"

	srt "github.com/datarhei/gosrt"
	"github.com/rlibaert/gocast/domain"
)

type ServiceRegisterer struct {
	BaseContext    func() context.Context
	StreamsService domain.StreamsService
}

func (reg ServiceRegisterer) Register(s *srt.Server) {
	if reg.BaseContext == nil {
		reg.BaseContext = context.Background
	}

	s.HandleConnect = func(req srt.ConnRequest) srt.ConnType {
		if req.Version() == 5 { //nolint:mnd // SRT version number
			switch verb, _, ok := strings.Cut(req.StreamId(), ":"); {
			case verb == "pub" && ok:
				return srt.PUBLISH
			case verb == "sub" && ok:
				return srt.SUBSCRIBE
			}
		}
		return srt.REJECT
	}
	s.HandlePublish = func(conn srt.Conn) {
		defer conn.Close()
		ctx := reg.BaseContext()
		_, stream, _ := strings.Cut(conn.StreamId(), ":")
		_, _ = reg.StreamsService.Publish(ctx, domain.StreamPub(stream), conn)
	}
	s.HandleSubscribe = func(conn srt.Conn) {
		defer conn.Close()
		ctx := reg.BaseContext()
		_, stream, _ := strings.Cut(conn.StreamId(), ":")
		_, _ = reg.StreamsService.Subscribe(ctx, domain.StreamSub(stream), conn)
	}
}
