package proto_test

import (
	"testing"

	"github.com/rlibaert/gocast/protos/proto-icy"
	"github.com/rlibaert/gocast/testing/assert"
)

//nolint:golines
func TestMetadata(t *testing.T) {
	assert.EQ(t, string(proto.Metadata(nil)), "\000")
	assert.EQ(t, string(proto.Metadata(nil, "foo bar baz")), "\001foo bar baz\000\000\000\000\000")
	assert.EQ(t, string(proto.Metadata(nil, "foo bar baz quu")), "\001foo bar baz quu\000")
	assert.EQ(t, string(proto.Metadata(nil, "foo bar baz quu ")), "\002foo bar baz quu \000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000")
	assert.EQ(t, string(proto.Metadata(nil, "foo bar baz quu qux")), "\002foo bar baz quu qux\000\000\000\000\000\000\000\000\000\000\000\000\000")
}
