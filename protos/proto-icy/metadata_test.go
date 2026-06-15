package proto_test

import (
	"testing"

	"github.com/rlibaert/gocast/protos/proto-icy"
	"github.com/stretchr/testify/assert"
)

//nolint:golines
func TestMetadata(t *testing.T) {
	assert.Equal(t, "\000", string(proto.Metadata(nil)))
	assert.Equal(t, "\001foo bar baz\000\000\000\000\000", string(proto.Metadata(nil, "foo bar baz")))
	assert.Equal(t, "\001foo bar baz quu\000", string(proto.Metadata(nil, "foo bar baz quu")))
	assert.Equal(t, "\002foo bar baz quu \000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000", string(proto.Metadata(nil, "foo bar baz quu ")))
	assert.Equal(t, "\002foo bar baz quu qux\000\000\000\000\000\000\000\000\000\000\000\000\000", string(proto.Metadata(nil, "foo bar baz quu qux")))
}
