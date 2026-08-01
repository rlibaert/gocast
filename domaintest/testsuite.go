package domaintest

import (
	"cmp"
	"context"
	"io"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/rlibaert/gocast/domain"
	"github.com/stretchr/testify/suite"
)

type rwFunc func(p []byte) (int, error)

func (f rwFunc) Read(p []byte) (int, error)  { return f(p) }
func (f rwFunc) Write(p []byte) (int, error) { return f(p) }

// pubReader returns an [io.Reader] that can be read until deadline,
// at which point it fails with [context.DeadlineExceeded].
func pubReader(start func(), pub domain.StreamPub, deadline time.Time) io.Reader {
	started := false
	return rwFunc(func(p []byte) (int, error) {
		if time.Now().After(deadline) {
			return 0, context.DeadlineExceeded
		}
		time.Sleep(time.Millisecond)
		if start != nil && !started {
			start()
			started = true
		}
		return copy(p, pub), nil
	})
}

// ServiceSuite exercises any [domain.Service] implementation.
type ServiceSuite struct {
	suite.Suite

	// NewService builds a fresh [domain.Service] before each test.
	NewService func(tb testing.TB) domain.Service

	service domain.Service
}

func (s *ServiceSuite) SetupTest() {
	s.service = s.NewService(s.T())
}

// baseDur is duration from which all relative deadlines are calculated.
const baseDur = 5 * time.Second

func (s *ServiceSuite) TestPublishSubscribe() {
	wg := sync.WaitGroup{}
	defer wg.Wait()

	// deadlines
	var (
		dlPubStops = time.Now().Add(baseDur)
	)

	wgPubsPublishing := sync.WaitGroup{}
	for _, pub := range []domain.StreamPub{"foo", "bar"} {
		wgPubsPublishing.Add(1)
		wg.Go(func() {
			n, err := s.service.Publish(pub, pubReader(wgPubsPublishing.Done, pub, dlPubStops))
			s.Require().ErrorIs(err, context.DeadlineExceeded)
			s.Positive(n)
		})
	}
	wgPubsPublishing.Wait()

	for _, sub := range []domain.StreamSub{"foo", "bar", "foo", "bar"} {
		wg.Go(func() {
			re := regexp.MustCompile(`^(` + string(sub) + `)*$`)
			n, err := s.service.Subscribe(sub, rwFunc(func(p []byte) (int, error) {
				s.True(re.Match(p), string(p), "not "+re.String())
				return len(p), nil
			}))
			s.Require().NoError(err)
			s.Positive(n)
		})
	}
}

func (s *ServiceSuite) TestFallback() {
	wg := sync.WaitGroup{}
	defer wg.Wait()

	domain.ServiceResetFallbacks(s.service, map[domain.StreamSub][]domain.StreamPub{"toto": {"foo", "bar"}})

	// deadlines
	var (
		dlFooStops = time.Now().Add(baseDur)
		dlBarStops = dlFooStops.Add(baseDur)
	)

	wgPubsPublishing := sync.WaitGroup{}
	wgPubsPublishing.Add(1)
	wg.Go(func() {
		n, err := s.service.Publish("foo", pubReader(wgPubsPublishing.Done, "foo", dlFooStops))
		s.Require().ErrorIs(err, context.DeadlineExceeded)
		s.Positive(n)
	})
	wgPubsPublishing.Add(1)
	wg.Go(func() {
		n, err := s.service.Publish("bar", pubReader(wgPubsPublishing.Done, "bar", dlBarStops))
		s.Require().ErrorIs(err, context.DeadlineExceeded)
		s.Positive(n)
	})
	wgPubsPublishing.Wait()

	wg.Go(func() {
		re1 := regexp.MustCompile(`^(foo)+(bar)*$`) // foo & foo -> bar transition
		re2 := regexp.MustCompile(`^(bar)+(foo)*$`) // bar & bar -> foo transition (maybe, when the test concludes)
		re := re1
		n, err := s.service.Subscribe("toto", rwFunc(func(p []byte) (int, error) {
			if re == re1 && re2.Match(p) {
				re = re2
			}
			s.Truef(re.Match(p) || len(p) == 0, "%v does not match %s", re, string(p))
			return len(p), nil
		}))
		s.Require().NoError(err)
		s.Positive(n)
		s.Same(re, re2)
	})
}

func (s *ServiceSuite) TestBackup() {
	wg := sync.WaitGroup{}
	defer wg.Wait()

	domain.ServiceResetFallbacks(s.service, map[domain.StreamSub][]domain.StreamPub{"toto": {"foo", "bar"}})

	// deadlines
	var (
		dlFooStarts = time.Now().Add(baseDur)
		dlFooStops  = dlFooStarts.Add(baseDur)
		dlBarStops  = dlFooStops
	)

	wg.Go(func() {
		time.Sleep(time.Until(dlFooStarts))
		n, err := s.service.Publish("foo", pubReader(nil, "foo", dlFooStops))
		s.Require().ErrorIs(err, context.DeadlineExceeded)
		s.Positive(n)
	})

	wgPubsPublishing := sync.WaitGroup{}
	wgPubsPublishing.Add(1)
	wg.Go(func() {
		n, err := s.service.Publish("bar", pubReader(wgPubsPublishing.Done, "bar", dlBarStops))
		s.Require().ErrorIs(err, context.DeadlineExceeded)
		s.Positive(n)
	})
	wgPubsPublishing.Wait()

	wg.Go(func() {
		re1 := regexp.MustCompile(`^(bar)+(foo)*$`) // bar & bar -> foo transition
		re2 := regexp.MustCompile(`^(foo)+(bar)*$`) // foo & foo -> bar transition (maybe, when the test concludes)
		re := re1
		n, err := s.service.Subscribe("toto", rwFunc(func(p []byte) (int, error) {
			if re == re1 && re2.Match(p) {
				re = re2
			}
			s.Truef(re.Match(p) || len(p) == 0, "%v does not match %s", re, string(p))
			return len(p), nil
		}))
		s.Require().NoError(err)
		s.Positive(n)
		s.Same(re, re2)
	})
}

func (s *ServiceSuite) TestPublishTitle() {
	t := s.T()

	wg := sync.WaitGroup{}
	defer wg.Wait()

	// deadlines
	var (
		dlFoo = time.Now().Add(baseDur)
	)

	wgPubsPublishing := sync.WaitGroup{}
	wgPubsPublishing.Add(1)
	wg.Go(func() {
		n, err := s.service.Publish("foo", pubReader(wgPubsPublishing.Done, "foo", dlFoo))
		s.Require().ErrorIs(err, context.DeadlineExceeded)
		s.Positive(n)
	})
	wgPubsPublishing.Wait()

	domain.ServiceResetFallbacks(s.service, map[domain.StreamSub][]domain.StreamPub{
		"toto": {"foo"},
		"tata": {"foo"},
	})

	const title = "lorem ipsum"
	s.Require().NoError(s.service.PublishTitle(t.Context(), "foo", title))
	s.Equal(title, *cmp.Or(domain.ServiceStreamSubTitle(s.service, "foo"), new("<nil>")))
	s.Equal(title, *cmp.Or(domain.ServiceStreamSubTitle(s.service, "toto"), new("<nil>")))
	s.Equal(title, *cmp.Or(domain.ServiceStreamSubTitle(s.service, "tata"), new("<nil>")))
	s.Require().ErrorIs(s.service.PublishTitle(t.Context(), "bar", ""), domain.ErrStreamNotFound)
}

func (s *ServiceSuite) TestCloseOnFallbacksRemoved() {
	wg := sync.WaitGroup{}
	defer wg.Wait()

	var (
		dlReset = time.Now().Add(baseDur)
		dlFoo   = dlReset.Add(baseDur)
	)

	wg.Go(func() {
		domain.ServiceResetFallbacks(s.service, map[domain.StreamSub][]domain.StreamPub{"toto": {"foo"}})
		time.Sleep(time.Until(dlReset))
		domain.ServiceResetFallbacks(s.service, map[domain.StreamSub][]domain.StreamPub{})
	})

	wgPubsPublishing := sync.WaitGroup{}
	wgPubsPublishing.Add(1)
	wg.Go(func() {
		n, err := s.service.Publish("foo", pubReader(wgPubsPublishing.Done, "foo", dlFoo))
		s.Require().ErrorIs(err, context.DeadlineExceeded)
		s.Positive(n)
	})
	wgPubsPublishing.Wait()

	s.Run("toto gets closed", func() {
		n, err := s.service.Subscribe("toto", io.Discard)
		s.Require().NoError(err)
		s.Positive(n)
	})

	s.Run("foo still opened", func() {
		n, err := s.service.Subscribe("foo", io.Discard)
		s.Require().NoError(err)
		s.Positive(n)
	})
}
