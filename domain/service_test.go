package domain_test

import (
	"context"
	"testing"
	"time"

	"github.com/rlibaert/gocast/domain"
	"github.com/rlibaert/gocast/domaintest"
)

func tbServiceHooks(tb testing.TB) domain.ServiceHooks {
	return domain.ServiceHooks{
		PublishStart:   func(_ context.Context, s domain.StreamPub) { tb.Log("PublishStart", s) },
		SubscribeStart: func(_ context.Context, s domain.StreamSub) { tb.Log("SubscribeStart", s) },
		PublishStop:    func(_ context.Context, s domain.StreamPub, t time.Time) { tb.Log("PublishStop", s, time.Since(t)) },
		SubscribeStop:  func(_ context.Context, s domain.StreamSub, t time.Time) { tb.Log("SubscribeStop", s, time.Since(t)) },
	}
}

func TestServicePublishSubscribe(t *testing.T) {
	t.Parallel()
	domaintest.ServiceTester{Service: domain.NewService(tbServiceHooks(t), domain.ServiceStreamCopy, 0)}.
		TestPublishSubscribe(t)
}

func TestServicePublishTitle(t *testing.T) {
	t.Parallel()
	domaintest.ServiceTester{Service: domain.NewService(tbServiceHooks(t), domain.ServiceStreamCopy, 0)}.
		TestPublishTitle(t)
}

func TestServiceFallback(t *testing.T) {
	t.Parallel()
	domaintest.ServiceTester{Service: domain.NewService(tbServiceHooks(t), domain.ServiceStreamCopy, 0)}.
		TestFallback(t)
}

func TestServiceBackup(t *testing.T) {
	t.Parallel()
	domaintest.ServiceTester{Service: domain.NewService(tbServiceHooks(t), domain.ServiceStreamCopy, 0)}.
		TestBackup(t)
}

func TestServiceCloseOnFallbacksRemoved(t *testing.T) {
	t.Parallel()
	domaintest.ServiceTester{Service: domain.NewService(tbServiceHooks(t), domain.ServiceStreamCopy, 0)}.
		TestCloseOnFallbacksRemoved(t)
}
