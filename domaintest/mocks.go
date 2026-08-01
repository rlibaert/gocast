package domaintest

import (
	"context"
	"io"

	"github.com/rlibaert/gocast/domain"
	"github.com/stretchr/testify/mock"
)

type ServiceMock struct {
	mock.Mock
	domain.Service
}

var _ domain.Service = (*ServiceMock)(nil)

func (m *ServiceMock) Publish(pub domain.StreamPub, r io.Reader) (int64, error) {
	args := m.Called(pub, r)
	return int64(args.Int(0)), args.Error(1)
}

func (m *ServiceMock) Subscribe(sub domain.StreamSub, w io.Writer) (int64, error) {
	args := m.Called(sub, w)
	return int64(args.Int(0)), args.Error(1)
}

func (m *ServiceMock) PublishTitle(ctx context.Context, pub domain.StreamPub, title string) error {
	args := m.Called(ctx, pub, title)
	return args.Error(0)
}

type ConfigMock struct {
	mock.Mock
}

func (m *ConfigMock) Get(context.Context) (*domain.Config, error) {
	args := m.Called()
	c, ok := args.Get(0).(*domain.Config)
	if !ok {
		panic("invalid mocked argument (unexpected type)")
	}
	return c, args.Error(1)
}
