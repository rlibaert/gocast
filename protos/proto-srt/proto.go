package proto

import (
	"context"
	"strings"

	srt "github.com/datarhei/gosrt"
	"github.com/rlibaert/gocast/domain"
)

type ServiceRegisterer struct {
	BaseContext      func() context.Context
	StreamingService domain.StreamingService
}

func (reg ServiceRegisterer) Register(s *srt.Server) {
	if reg.BaseContext == nil {
		reg.BaseContext = context.Background
	}

	s.HandleConnect = func(req srt.ConnRequest) srt.ConnType {
		if verb, _, ok := strings.Cut(req.StreamId(), ":"); ok {
			switch verb {
			case "pub":
				return srt.PUBLISH
			case "sub":
				return srt.SUBSCRIBE
			}
		}
		return srt.REJECT
	}
	s.HandlePublish = func(conn srt.Conn) {
		defer conn.Close()
		ctx := reg.BaseContext()
		_, stream, _ := strings.Cut(conn.StreamId(), ":")
		_, _ = reg.StreamingService.Publish(ctx, domain.StreamPub(stream), conn)
	}
	s.HandleSubscribe = func(conn srt.Conn) {
		defer conn.Close()
		ctx := reg.BaseContext()
		_, stream, _ := strings.Cut(conn.StreamId(), ":")
		_, _ = reg.StreamingService.Subscribe(ctx, domain.StreamSub(stream), conn)
	}
}
