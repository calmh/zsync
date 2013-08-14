package main

import (
	"bufio"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"os/exec"

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
			logf(DEBUG, "listing snapshots\n")
			s, _ := zfs.ListSnapshots(c.Params[0])
			err = e.Encode(s)
			panicOn(err)
		case CmdReceive:
			logf(DEBUG, "zfs recv %v\n", c.Params)

			args := []string{"recv"}
			args = append(args, c.Params...)
			cmd := exec.Command("zfs", args...)
			stdin, _ := cmd.StdinPipe()
			err := cmd.Start()
			panicOn(err)

			for {
				var l uint32
				err := binary.Read(bstdin, binary.BigEndian, &l)
				if l == 0 {
					stdin.Close()
					break
				}
				panicOn(err)
				bs := make([]byte, l)
				_, err = io.ReadFull(bstdin, bs)
				panicOn(err)
				n, err := stdin.Write(bs)
				if n != int(l) {
					panic(fmt.Errorf("short write: %d != %d", n, l))
				}
				panicOn(err)
			}

			err = cmd.Wait()
			panicOn(err)

			resp := Command{Command: CmdResult}
			err = e.Encode(&resp)
			panicOn(err)
		}
	}
}
