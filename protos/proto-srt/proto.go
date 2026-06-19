package proto

import (
	"context"
	"strings"

	srt "github.com/datarhei/gosrt"
	"github.com/rlibaert/gocast/domain"
)

type ServiceRegisterer struct {
	BaseContext func() context.Context
	Service     domain.Service
}

func (reg ServiceRegisterer) Register(s *srt.Server) {
	if reg.BaseContext == nil {
		reg.BaseContext = context.Background
	}

	s.HandleConnect = func(req srt.ConnRequest) srt.ConnType {
		switch {
		case req.Version() != 5: //nolint:mnd // SRT version number
			return srt.REJECT
		case strings.HasPrefix(req.StreamId(), "publish:"):
			return srt.PUBLISH
		default:
			return srt.SUBSCRIBE
		}
	}
	s.HandlePublish = func(conn srt.Conn) {
		defer conn.Close()
		ctx := reg.BaseContext()
		stream := strings.TrimPrefix(conn.StreamId(), "publish:")
		_, _ = reg.Service.Publish(ctx, domain.StreamPub(stream), conn)
	}
	s.HandleSubscribe = func(conn srt.Conn) {
		defer conn.Close()
		ctx := reg.BaseContext()
		stream := conn.StreamId()
		_, _ = reg.Service.Subscribe(ctx, domain.StreamSub(stream), conn)
	}
}
