package domain_test

import (
	"context"
	"testing"
	"time"

	. "github.com/rlibaert/gocast/domain"
	"github.com/rlibaert/gocast/domaintest"
)

var defaultHooks = ServiceHooks{
	PublishStart:   func(context.Context, StreamPub) {},
	SubscribeStart: func(context.Context, StreamSub) {},
	PublishStop:    func(context.Context, StreamPub, time.Time) {},
	SubscribeStop:  func(context.Context, StreamSub, time.Time) {},
}

func TestServicePublishSubscribe(t *testing.T) {
	t.Parallel()
	domaintest.ServiceTester{Service: NewService(defaultHooks, 0)}.
		TestPublishSubscribe(t)
}

func TestServicePublishTitle(t *testing.T) {
	t.Parallel()
	domaintest.ServiceTester{Service: NewService(defaultHooks, 0)}.
		TestPublishTitle(t)
}

func TestServiceFallback(t *testing.T) {
	t.Parallel()
	domaintest.ServiceTester{Service: NewService(defaultHooks, 0)}.
		TestFallback(t)
}

func TestServiceBackup(t *testing.T) {
	t.Parallel()
	domaintest.ServiceTester{Service: NewService(defaultHooks, 0)}.
		TestBackup(t)
}
