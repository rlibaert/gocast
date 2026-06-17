package proto_test

import (
	"testing"

	"github.com/rlibaert/gocast/protos/proto-icy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetaint(t *testing.T) {
	orig := proto.Metaint
	t.Cleanup(func() { proto.Metaint = orig }) //nolint: reassign // only to restore original after testing

	require.Error(t, proto.Metaint.UnmarshalText([]byte("foo")))
	require.NoError(t, proto.Metaint.UnmarshalText([]byte("12345")))

	b, err := proto.Metaint.MarshalText()
	require.NoError(t, err)
	assert.Equal(t, "12345", string(b))
}

//nolint:golines
func TestMetadata(t *testing.T) {
	assert.Equal(t, "\000", string(proto.Metadata(nil)))
	assert.Equal(t, "\001foo bar baz\000\000\000\000\000", string(proto.Metadata(nil, "foo bar baz")))
	assert.Equal(t, "\001foo bar baz quu\000", string(proto.Metadata(nil, "foo bar baz quu")))
	assert.Equal(t, "\002foo bar baz quu \000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000", string(proto.Metadata(nil, "foo bar baz quu ")))
	assert.Equal(t, "\002foo bar baz quu qux\000\000\000\000\000\000\000\000\000\000\000\000\000", string(proto.Metadata(nil, "foo bar baz quu qux")))
}
