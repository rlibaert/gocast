package domain_test

import (
	"testing"
	"time"

	"github.com/rlibaert/gocast/domain"
	"github.com/rlibaert/gocast/domaintest"
	"github.com/stretchr/testify/require"
)

func tbServiceHooks(tb testing.TB) domain.ServiceHooks {
	return domain.ServiceHooks{
		PublishStartStop: func(s domain.StreamPub) func() {
			t := time.Now()
			tb.Log("PublishStart", s)
			return func() { tb.Log("PublishStop", s, time.Since(t)) }
		},
		SubscribeStartStop: func(s domain.StreamSub) func() {
			t := time.Now()
			tb.Log("SubscribeStart", s)
			return func() { tb.Log("SubscribeStop", s, time.Since(t)) }
		},
	}
}

func tbConfig(_ testing.TB) *domaintest.ConfigMock {
	c := &domaintest.ConfigMock{}
	c.On("Get").Return(&domain.Config{}, nil)
	return c
}

func TestServicePublishSubscribe(t *testing.T) {
	t.Parallel()
	svc, err := domain.NewService(tbConfig(t), tbServiceHooks(t), domain.ServiceStreamCopy, 0)
	require.NoError(t, err)
	domaintest.ServiceTester{Service: svc}.
		TestPublishSubscribe(t)
}

func TestServicePublishTitle(t *testing.T) {
	t.Parallel()
	svc, err := domain.NewService(tbConfig(t), tbServiceHooks(t), domain.ServiceStreamCopy, 0)
	require.NoError(t, err)
	domaintest.ServiceTester{Service: svc}.
		TestPublishTitle(t)
}

func TestServiceFallback(t *testing.T) {
	t.Parallel()
	svc, err := domain.NewService(tbConfig(t), tbServiceHooks(t), domain.ServiceStreamCopy, 0)
	require.NoError(t, err)
	domaintest.ServiceTester{Service: svc}.
		TestFallback(t)
}

func TestServiceBackup(t *testing.T) {
	t.Parallel()
	svc, err := domain.NewService(tbConfig(t), tbServiceHooks(t), domain.ServiceStreamCopy, 0)
	require.NoError(t, err)
	domaintest.ServiceTester{Service: svc}.
		TestBackup(t)
}

func TestServiceCloseOnFallbacksRemoved(t *testing.T) {
	t.Parallel()
	svc, err := domain.NewService(tbConfig(t), tbServiceHooks(t), domain.ServiceStreamCopy, 0)
	require.NoError(t, err)
	domaintest.ServiceTester{Service: svc}.
		TestCloseOnFallbacksRemoved(t)
}
