package internal_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/rlibaert/gocast/protos/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetadataWriter(t *testing.T) {
	buf := new(bytes.Buffer)

	const sample = "012345678901234567890123456789"
	n, err := fmt.Fprint(internal.PageWriter(buf, 7, func() []byte {
		return []byte("foobar")
	}), sample)

	require.NoError(t, err)
	assert.Equal(t, len(sample), n)
	assert.Equal(t, ""+
		"0123456foobar"+
		"7890123foobar"+
		"4567890foobar"+
		"1234567foobar"+
		"89", buf.String())
}
