package main_test

import (
	_ "embed"
	"encoding/json"
	"testing"

	"github.com/rlibaert/gocast/domain"
	"github.com/rlibaert/gocast/protos/proto-config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//go:embed config.json
var configJSON []byte

func TestConfigJSON(t *testing.T) {
	expected := proto.Config{
		Fallbacks: map[domain.StreamSub][]domain.StreamPub{},
	}

	var config proto.Config
	require.NoError(t, json.Unmarshal(configJSON, &config))
	assert.Equal(t, expected, config)
}
