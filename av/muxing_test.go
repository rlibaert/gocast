package av_test

import (
	"embed"
	"testing"
	"time"

	"github.com/rlibaert/gocast/av"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//go:embed testdata
var testdataFS embed.FS

func TestDemux(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		file    string
		packets int64
	}{
		{"testdata/samples/aac", 45},
		{"testdata/samples/mp3", 40},
	}

	for _, tc := range testcases {
		t.Run(tc.file, func(t *testing.T) {
			t.Parallel()

			f, err := testdataFS.Open(tc.file)
			require.NoError(t, err)
			defer f.Close()

			d, err := av.NewDemuxer(f)
			require.NoError(t, err)
			defer d.Close()

			n, err := av.Remux(av.Discard, d)
			require.NoError(t, err)
			assert.Equal(t, n, tc.packets)
		})
	}
}

func TestPTSMuxer(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		file   string
		offset time.Duration
		dur    time.Duration
	}{
		{"testdata/samples/aac", 0, time.Second},
		{"testdata/samples/mp3", 0, time.Second},
		{"testdata/samples/aac", -500 * time.Millisecond, 500 * time.Millisecond},
		{"testdata/samples/mp3", -500 * time.Millisecond, 500 * time.Millisecond},
		{"testdata/samples/aac", +500 * time.Millisecond, 1500 * time.Millisecond},
		{"testdata/samples/mp3", +500 * time.Millisecond, 1500 * time.Millisecond},
	}

	for _, tc := range testcases {
		t.Run(tc.file, func(t *testing.T) {
			t.Parallel()

			f, err := testdataFS.Open(tc.file)
			require.NoError(t, err)
			defer f.Close()

			d, err := av.NewDemuxer(f)
			require.NoError(t, err)
			defer d.Close()

			tick := time.Now()
			_, err = av.Remux(av.PTSMuxer(av.Discard, tc.offset), d)
			require.NoError(t, err)
			dur := time.Since(tick)
			assert.Greater(t, dur, tc.dur)
			assert.Less(t, dur, tc.dur+100*time.Millisecond)
		})
	}
}
