package main

import (
	"bufio"
	"encoding/gob"
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
			receive(c, e, bstdin)
		}
	}
}

func receive(c Command, e *gob.Encoder, in io.Reader) {
	args := []string{"recv"}
	args = append(args, c.Params...)
	cmd := exec.Command("zfs", args...)

	recvIn, err := cmd.StdinPipe()
	panicOn(err)

	recvErr, err := cmd.StderrPipe()
	panicOn(err)
	go printLines("zfs recv: ", recvErr)

	recvOut, err := cmd.StdoutPipe()
	panicOn(err)
	go printLines("zfs recv: ", recvOut)

	err = cmd.Start()
	panicOn(err)

	bufRecvIn := bufio.NewWriterSize(recvIn, opts.bufferBytes)
	cr := ChunkedReader{in}
	_, err = io.Copy(bufRecvIn, cr)
	panicOn(err)

	err = bufRecvIn.Flush()
	panicOn(err)

	err = recvIn.Close()
	panicOn(err)

	err = cmd.Wait()
	panicOn(err)

	resp := Command{Command: CmdResult}
	err = e.Encode(&resp)
	panicOn(err)
}
