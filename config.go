package main

import (
	"context"
	"encoding/json"
	"io"
	"os"

	"github.com/rlibaert/gocast/domain"
)

func NewConfigFromJSON(r io.Reader) (*domain.Config, error) {
	var v struct {
		Fallbacks map[domain.StreamSub][]domain.StreamPub `json:"fallbacks"`
	}

	err := json.NewDecoder(r).Decode(&v)
	if err != nil {
		return nil, err
	}

	return &domain.Config{
		Fallbacks: v.Fallbacks,
	}, nil
}

type JSONConfigGetter string

func (g JSONConfigGetter) Get(context.Context) (*domain.Config, error) {
	f, err := os.Open(string(g))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return NewConfigFromJSON(f)
}
