package av

/*
#include <libavutil/error.h>
*/
import "C"
import (
	"bytes"
	"unsafe"
)

// Error turns libav* error codes into Go errors.
type Error C.int

func (e Error) Error() string {
	const prefix = "av: "
	const maxlen = C.AV_ERROR_MAX_STRING_SIZE

	buf := append([]byte(prefix), make([]byte, maxlen)...)
	C.av_strerror(C.int(e), (*C.char)(unsafe.Pointer(&buf[len(prefix)])), maxlen)
	buf, _, _ = bytes.Cut(buf, []byte{0})

	return unsafe.String(&buf[0], len(buf))
}
