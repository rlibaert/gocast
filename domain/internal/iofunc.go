package internal

type ReaderFunc func(p []byte) (int, error)
type WriterFunc func(p []byte) (int, error)

func (f ReaderFunc) Read(p []byte) (int, error)  { return f(p) }
func (f WriterFunc) Write(p []byte) (int, error) { return f(p) }
