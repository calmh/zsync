package main

import (
	"encoding/binary"
	"io"
)

type ChunkedWriter struct {
	io.WriteCloser
}

func (w ChunkedWriter) Write(p []byte) (n int, err error) {
	l := uint32(len(p))
	err = binary.Write(w.WriteCloser, binary.BigEndian, &l)
	if err != nil {
		return
	}
	n, err = w.WriteCloser.Write(p)
	return
}

func (w ChunkedWriter) Close() error {
	var l uint32
	return binary.Write(w.WriteCloser, binary.BigEndian, &l)
}
