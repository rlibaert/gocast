package proto

import "github.com/rlibaert/gocast/domain"

type Config struct {
	Fallbacks map[domain.StreamSub][]domain.StreamPub `json:"fallbacks"`
}

type ServiceRegisterer struct {
	Service domain.Service
}

func (reg ServiceRegisterer) Register(ch <-chan *Config) {
	go func() {
		for config := range ch {
			domain.ServiceResetFallbacks(reg.Service, config.Fallbacks)
		}
	}()
}
