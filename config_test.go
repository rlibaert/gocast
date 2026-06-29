package main_test

import (
	"bytes"
	_ "embed"
	"testing"

	main "github.com/rlibaert/gocast"
	"github.com/rlibaert/gocast/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//go:embed config.json
var configJSON []byte

func TestConfigJSON(t *testing.T) {
	expected := &domain.Config{
		Fallbacks: map[domain.StreamSub][]domain.StreamPub{},
	}

	actual, err := main.NewConfigFromJSON(bytes.NewReader(configJSON))
	require.NoError(t, err)
	assert.Equal(t, expected, actual)
}
