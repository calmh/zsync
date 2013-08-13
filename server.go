package main

import (
	"bufio"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"os"

	"github.com/calmh/zfs"
)

func server() {
	bstdin := bufio.NewReader(os.Stdin)
	e := gob.NewEncoder(os.Stdout)
	d := gob.NewDecoder(bstdin)

	negotiateVersion(e, d)

	var c Command

	for {
		err := d.Decode(&c)
		if err == io.EOF {
			break
		}
		panicOn(err)

		switch c.Command {
		case CmdListSnapshots:
			s, _ := zfs.ListSnapshots(c.Params[0])
			err = e.Encode(s)
			panicOn(err)
		case CmdReceive:
			logf(DEBUG, "zfs.Receive(%v)\n", c.Params)
			rs, err := zfs.Receive(c.Params...)
			panicOn(err)
			for {
				var l uint32
				err := binary.Read(bstdin, binary.BigEndian, &l)
				if l == 0 {
					break
				}
				panicOn(err)
				bs := make([]byte, l)
				_, err = io.ReadFull(bstdin, bs)
				panicOn(err)
				n, err := rs.Write(bs)
				if n != int(l) {
					panic(fmt.Errorf("short write: %d != %d", n, l))
				}
				panicOn(err)
			}
			rs.Close()
		}
	}
}
