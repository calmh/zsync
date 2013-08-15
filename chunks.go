package main

import (
	"encoding/binary"
	"io"
)

type ChunkedWriter struct {
	io.Writer
}

func (w ChunkedWriter) Write(p []byte) (n int, err error) {
	l := uint32(len(p))
	err = binary.Write(w.Writer, binary.BigEndian, &l)
	if err != nil {
		return
	}
	n, err = w.Writer.Write(p)
	return
}

func (w ChunkedWriter) Flush() error {
	var l uint32
	return binary.Write(w.Writer, binary.BigEndian, &l)
}

type ChunkedReader struct {
	io.Reader
}

func (r ChunkedReader) Read(bs []byte) (n int, err error) {
	var l uint32
	err = binary.Read(r.Reader, binary.BigEndian, &l)
	if err != nil {
		return
	}

	if l == 0 {
		err = io.EOF
		return
	}

	if int(l) > cap(bs) {
		err = io.ErrShortBuffer
		return
	}

	bs = bs[:l]
	n, err = io.ReadFull(r.Reader, bs)
	return
}
