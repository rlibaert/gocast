package av_test

import (
	"testing"

	"github.com/rlibaert/gocast/av"
	"github.com/stretchr/testify/require"
)

func TestError(t *testing.T) {
	require.EqualError(t, av.Error(-2), "av: No such file or directory")
	require.EqualError(t, av.Error(12), "av: Error number 12 occurred")
}
