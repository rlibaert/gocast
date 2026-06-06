package proto

import (
	"fmt"
	"strconv"
)

var (
	// metaInt is the number of stream bytes written before sending in-band metadata.
	metaInt = 16000
	// metaIntStr is the string representation of [metaInt] to use as value for icy-metaint header.
	metaIntStr = strconv.Itoa(metaInt)
)

func SetMetaInt(v int) { metaInt, metaIntStr = v, strconv.Itoa(v) }

type metadata struct {
	StreamTitle *string
	StreamURL   *string
}

func (m metadata) MarshalBinary() (buf []byte, _ error) {
	buf = []byte{0}

	if m.StreamTitle != nil {
		buf = fmt.Appendf(buf, "StreamTitle='%s';", *m.StreamTitle)
	}
	if m.StreamURL != nil {
		buf = fmt.Appendf(buf, "StreamUrl='%s';", *m.StreamURL)
	}

	if len(buf) == 1 {
		return buf, nil
	}

	// Always NUL-terminate to ensure compatibility with C strings
	buf = append(buf, '0')

	const blockSize = 16
	if lastBlockLength := len(buf[1:]) % blockSize; lastBlockLength > 0 {
		buf = append(buf, make([]byte, blockSize-lastBlockLength)...)
	}
	buf[0] = byte(len(buf[1:]) / blockSize)

	return buf, nil
}
