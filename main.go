package main

import (
	"bufio"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/calmh/zfs"
	"github.com/jessevdk/go-flags"
)

const protocolVersion = "zsync/1.0"

type LogLevel int

const (
	INFO LogLevel = iota
	VERBOSE
	DEBUG
)

type CommandIndex uint32

const (
	CmdVersion CommandIndex = iota
	CmdListSnapshots
	CmdReceive
	CmdZfsData
)

type Command struct {
	Command CommandIndex
	Params  []string
	Data    []byte
}

var opts struct {
	Verbose          []bool `long:"verbose" short:"v" description:"increase the output verbosity"`
	MountDestination bool   `long:"mount-destination" description:"mount the destination dataset after replication (i.e. do not do zfs recv -u)"`
	NoRollback       bool   `long:"no-rollback" description:"do not rollback the destination dataset prior to replication (i.e. do not do zfs recv -F)"`
	ZsyncPath        string `long:"zsync-path" default:"zsync" value-name:"PROGRAM" description:"specify the zsync to run on remote machine"`
	Server           bool   `long:"server" description:"[internal]"`
	verbosity        LogLevel
	//SetReadOnly      bool   `long:"set-readonly" description:"do zfs set readonly=on on the destination"`
}

func main() {
	parser := flags.NewParser(&opts, flags.PassDoubleDash|flags.PrintErrors)
	parser.Usage = "[OPTIONS] <srcds>[@snapshot] <host>[:dstds]"
	args, err := parser.Parse()
	opts.verbosity = LogLevel(len(opts.Verbose))

	if err != nil || !opts.Server && len(args) != 2 {
		fmt.Fprintln(os.Stderr)
		parser.WriteHelp(os.Stderr)
		fmt.Fprintf(os.Stderr, "\nExample:\n  %s tank/data root@172.16.32.12:tank/replicated\n\n", parser.ApplicationName)
		os.Exit(2)
	}

	if opts.Server {
		server()
	} else {
		client(args[0], args[1])
	}
}

func negotiateVersion(e *gob.Encoder, d *gob.Decoder) {
	var c Command
	c.Command = CmdVersion
	c.Params = []string{protocolVersion}
	err := e.Encode(c)
	panicOn(err)
	err = d.Decode(&c)
	if c.Params[0] != protocolVersion {
		panic(fmt.Errorf("Mismatched protocol version %s != %s", c.Params[0], protocolVersion))
	}
	panicOn(err)
}

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
			s, err := zfs.ListSnapshots(c.Params[0])
			//panicOn(err)
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

func client(ds, host string) {
	var serverDs string
	if strings.ContainsRune(host, ':') {
		fs := strings.SplitN(host, ":", 2)
		host = fs[0]
		serverDs = fs[1]
	} else {
		serverDs = ds
	}

	logf(DEBUG, "exec: ssh %s %s --server\n", host, opts.ZsyncPath)
	cmd := exec.Command("ssh", host, opts.ZsyncPath, "--server")
	stdin, err := cmd.StdinPipe()
	panicOn(err)
	stdout, err := cmd.StdoutPipe()
	panicOn(err)
	stderr, err := cmd.StderrPipe()
	panicOn(err)
	err = cmd.Start()
	panicOn(err)
	go func() {
		err := cmd.Wait()
		panicOn(err)
	}()

	go func() {
		br := bufio.NewReader(stderr)
		for {
			bs, _, err := br.ReadLine()
			if err == io.EOF {
				break
			}
			fmt.Printf("[REMOTE] %s\n", bs)
		}
	}()

	e := gob.NewEncoder(stdin)
	d := gob.NewDecoder(stdout)

	negotiateVersion(e, d)

	l := Command{Command: CmdListSnapshots, Params: []string{serverDs}}
	err = e.Encode(l)
	panicOn(err)

	var serverSnapshots []zfs.SnapshotEntry
	err = d.Decode(&serverSnapshots)
	panicOn(err)

	clientSnapshots, err := zfs.ListSnapshots(ds)
	panicOn(err)

	// fmt.Printf("%#v\n", serverSnapshots)
	// fmt.Printf("%#v\n", clientSnapshots)
	toSend := clientSnapshots[len(clientSnapshots)-1]
	logf(VERBOSE, "our latest: %s@%s\n", toSend.Dataset, toSend.Snapshot)

	latest := latestCommon(serverSnapshots, clientSnapshots)
	if latest != nil {
		logf(VERBOSE, "remote latest: %s@%s\n", latest.Dataset, latest.Snapshot)
	}

	if latest != nil && toSend.Snapshot == latest.Snapshot {
		logf(INFO, "nothing to send (destination in sync)\n")
		return
	}

	params := []string{"-R"}
	if latest != nil {
		params = append(params, "-I", "@"+latest.Snapshot)
	}
	params = append(params, ds+"@"+toSend.Snapshot)

	logf(DEBUG, "DEBUG: zfs.Send(%v)\n", params)
	stream, err := zfs.Send(params...)
	panicOn(err)

	params = nil
	if !opts.NoRollback {
		params = append(params, "-F")
	}
	if !opts.MountDestination {
		params = append(params, "-u")
	}
	params = append(params, serverDs)
	sc := Command{Command: CmdReceive, Params: params}
	err = e.Encode(sc)
	panicOn(err)

	logf(VERBOSE, "sending\n")

	t0 := time.Now()
	tot, qd := bufferedCopyOut(stdin, stream)

	td := time.Since(t0)
	logf(INFO, "sent %s@%s; %d bytes in %.2f seconds (%3.1f KBps), qd=%d\n", toSend.Dataset, toSend.Snapshot, tot, td.Seconds(), float64(tot/1024)/td.Seconds(), qd)
}

func latestCommon(o, n []zfs.SnapshotEntry) *zfs.SnapshotEntry {
	for i := len(n) - 1; i >= 0; i-- {
		latest := n[i]
		for j := len(o) - 1; j >= 0; j-- {
			if o[j].Snapshot == latest.Snapshot {
				return &o[j]
			}
		}
	}
	return nil
}

func panicOn(e error) {
	if e != nil {
		panic(e)
	}
}

func logf(level LogLevel, format string, args ...interface{}) {
	if opts.verbosity >= level {
		fmt.Fprintf(os.Stderr, format, args...)
	}
}

func bufferedCopyOut(w io.Writer, r io.Reader) (int, int32) {
	nbufs := 1000
	bufsize := 65536

	wb := make(chan []byte, nbufs)
	rb := make(chan []byte, nbufs)
	for i := 0; i < nbufs; i++ {
		wb <- make([]byte, bufsize)
	}

	var depth, maxDepth, queue int32
	var tot int
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		for {
			b := <-wb
			atomic.AddInt32(&depth, 1)
			if depth > maxDepth {
				maxDepth = depth
			}

			b = b[:cap(b)]
			n, e := io.ReadFull(r, b)

			b = b[:n]
			atomic.AddInt32(&queue, int32(n))
			rb <- b

			if e == io.ErrUnexpectedEOF {
				rb <- []byte{}
				break
			} else if e == io.EOF {
				break
			}
			panicOn(e)
		}
		wg.Done()
	}()

	go func() {
		t0 := time.Now()
		t1 := t0

		for {
			b := <-rb
			l := uint32(len(b))
			atomic.AddInt32(&queue, int32(-l))

			err := binary.Write(w, binary.BigEndian, &l)
			panicOn(err)

			if l == 0 {
				break
			}

			n, err := w.Write(b)

			atomic.AddInt32(&depth, -1)
			wb <- b

			tot += n
			panicOn(err)

			td := time.Since(t1)
			if td.Seconds() > 1 {
				fmt.Printf("\r%6sB  %6sB/s %6sB", toSi(tot), toSi(int(float64(tot)/time.Since(t0).Seconds())), toSi(int(queue)))
				t1 = time.Now()
			}
		}
		fmt.Println()
		wg.Done()
	}()

	wg.Wait()
	return tot, maxDepth
}

func toSi(n int) string {
	if n > 1e9 {
		return fmt.Sprintf("%.01fG", float64(n)/1e9)
	} else if n > 1e6 {
		return fmt.Sprintf("%.01fM", float64(n)/1e6)
	} else if n > 1e3 {
		return fmt.Sprintf("%.01fk", float64(n)/1e3)
	} else {
		return fmt.Sprintf("%d", n)
	}
}
