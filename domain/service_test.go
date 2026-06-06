package domain_test

import (
	"context"
	"testing"
	"time"

	. "github.com/rlibaert/gocast/domain"
	"github.com/rlibaert/gocast/domaintest"
)

func TestService(t *testing.T) {
	domaintest.ServiceTester{
		Service: NewService(ServiceHooks{
			PublishStart:   func(_ context.Context, s StreamPub) { t.Log("pub start", s) },
			SubscribeStart: func(_ context.Context, s StreamSub) { t.Log("sub start", s) },
			PublishStop:    func(_ context.Context, s StreamPub, start time.Time) { t.Log("pub stop", s, time.Since(start)) },
			SubscribeStop:  func(_ context.Context, s StreamSub, start time.Time) { t.Log("sub stop", s, time.Since(start)) },
		}, 0),
	}.Test(t)
}
