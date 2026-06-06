package av_test

import (
	"strconv"
	"testing"

	"github.com/rlibaert/gocast/av"
	"github.com/rlibaert/gocast/testing/assert"
)

func TestError(t *testing.T) {
	testCases := []struct {
		code   av.Error
		expect string
	}{
		{-2, "av: No such file or directory"},
		{12, "av: Error number 12 occurred"},
	}

	for _, tc := range testCases {
		t.Run(strconv.Itoa(int(tc.code)), func(t *testing.T) {
			assert.EQ(t, tc.code.Error(), tc.expect)
		})
	}
}
