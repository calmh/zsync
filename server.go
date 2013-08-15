package main

import (
	"bufio"
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

	logf(VERBOSE, "server: starting up\n")

	var c Command

	for {
		err := d.Decode(&c)
		if err == io.EOF {
			break
		}
		panicOn(err)

		switch c.Command {
		case CmdListSnapshots:
			logf(DEBUG, "server: listing snapshots\n")
			s, _ := zfs.ListSnapshots(c.Params[0])
			err = e.Encode(s)
			panicOn(err)
		case CmdReceive:
			logf(DEBUG, "server: zfs recv %v\n", c.Params)

			args := []string{"recv"}
			args = append(args, c.Params...)
			cmd := exec.Command("zfs", args...)
			stdin, err := cmd.StdinPipe()
			panicOn(err)
			stderr, err := cmd.StderrPipe()
			panicOn(err)
			stdout, err := cmd.StdoutPipe()
			panicOn(err)
			bufstdin := bufio.NewWriterSize(stdin, opts.bufferBytes)

			go printLines("zfs recv: ", stderr)
			go printLines("zfs recv: ", stdout)

			err = cmd.Start()
			panicOn(err)

			var bs []byte
			cr := ChunkedReader{bstdin}

			for {
				bs, err = cr.Read(bs)
				if err == io.EOF {
					err = bufstdin.Flush()
					panicOn(err)
					err = stdin.Close()
					panicOn(err)
					break
				}
				panicOn(err)

				rb := len(bs)
				wb, err := bufstdin.Write(bs)
				panicOn(err)

				if wb != rb {
					panic(fmt.Errorf("server: short write: w%d != r%d", wb, rb))
				}
			}

			err = cmd.Wait()
			panicOn(err)

			resp := Command{Command: CmdResult}
			err = e.Encode(&resp)
			panicOn(err)
		}
	}
}
