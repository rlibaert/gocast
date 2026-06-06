package av_test

import (
	"embed"
	"testing"

	"github.com/rlibaert/gocast/av"
	"github.com/rlibaert/gocast/testing/assert"
)

//go:embed testdata
var testdataFS embed.FS

func TestDemux(t *testing.T) {
	testcases := []struct {
		file    string
		packets int64
	}{
		{"testdata/samples/aac", 45},
		{"testdata/samples/mp3", 40},
	}
	for _, tc := range testcases {
		t.Run(tc.file, func(t *testing.T) {
			f, err := testdataFS.Open(tc.file)
			assert.ErrNil(t, err)
			defer f.Close()

			d, err := av.NewDemuxer(f)
			assert.ErrNil(t, err)
			defer d.Close()

			n, err := av.Remux(av.Discard, d)
			assert.ErrNil(t, err)
			assert.EQ(t, n, tc.packets)
		})
	}
}
