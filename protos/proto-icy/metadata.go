package proto

import "strconv"

// Metaint is the number of stream bytes written before sending in-band metadata.
var Metaint = metaint{16000, "16000"}

type metaint struct {
	int
	string
}

func (m metaint) MarshalText() ([]byte, error) {
	return []byte(m.string), nil
}

func (m *metaint) UnmarshalText(text []byte) error {
	i, err := strconv.Atoi(string(text))
	if err != nil {
		return err
	}
	*m = metaint{i, string(text)}
	return nil
}

// metadata takes ownership of the buffer to encode Icecast in-band metadata
// and returns the new buffer.
//
// The encoding consists in:
//   - a heading byte containing the number of data blocks to come
//   - the actual metadata string
//   - a NUL byte terminating the string
//   - padding bytes filling the last block
//
// The metadata string is formatted as a series of **key='value';** pairs,
// provided in the elems argument. It is the caller's responsibility to ensure
// proper formatting and a length within limits.
//
// Usage example:
//
//	_ = metadata(nil, "StreamTitle='", title, "';")
func metadata(buf []byte, elems ...string) []byte {
	buf = append(buf[:0], 0)
	if len(elems) == 0 {
		return buf
	}

	for _, elem := range elems {
		buf = append(buf, elem...)
	}
	buf = append(buf, '0') // NUL-terminate the string

	type block [16]byte
	blocks := len(buf[1:]) / len(block{})
	remain := len(buf[1:]) % len(block{})
	if remain != 0 {
		var padding block
		buf = append(buf, padding[remain:]...)
		blocks++
	}

	const blocksLimit = ^byte(0)
	if blocks > int(blocksLimit) {
		return metadata(buf) // TODO: log
	}
	buf[0] = byte(blocks)

	return buf
}
