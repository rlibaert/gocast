package domain_test

import (
	"testing"
	"time"

	"github.com/rlibaert/gocast/domain"
	"github.com/rlibaert/gocast/domaintest"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
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

func TestService(t *testing.T) {
	suite.Run(t, &domaintest.ServiceSuite{
		NewService: func(tb testing.TB) domain.Service {
			svc, err := domain.NewService(tbConfig(tb), tbServiceHooks(tb), domain.ServiceStreamCopy, 0)
			require.NoError(tb, err)
			return svc
		},
	})
}
