package main

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/calmh/zfs"
)

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
			fmt.Printf("REMOTE: %s\n", bs)
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
	tot := bufferedCopyOut(ChunkedWriter{stdin}, stream)

	td := time.Since(t0)
	logf(INFO, "sent %s@%s; %sB in %.2f seconds (%sB/s)\n", toSend.Dataset, toSend.Snapshot, toSi(tot), td.Seconds(), toSi(int(float64(tot)/td.Seconds())))
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

func bufferedCopyOut(w io.WriteCloser, r io.Reader) int {
	nbufs := 1000
	bufsize := 65536

	wb := make(chan []byte, nbufs)
	rb := make(chan []byte, nbufs)
	for i := 0; i < nbufs; i++ {
		wb <- make([]byte, bufsize)
	}

	var tot int
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		for {
			b := <-wb

			b = b[:cap(b)]
			n, e := io.ReadFull(r, b)
			b = b[:n]

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
		var printed bool
		t0 := time.Now()
		t1 := t0

		for {
			b := <-rb

			if len(b) == 0 {
				w.Close()
				break
			}

			n, err := w.Write(b)
			panicOn(err)

			wb <- b

			tot += n

			td := time.Since(t1)
			if td.Seconds() > 1 {
				printed = true
				rate := int(float64(tot) / time.Since(t0).Seconds())
				fmt.Printf("\rsending  %6sB  %6sB/s", toSi(tot), toSi(rate))
				t1 = time.Now()
			}
		}

		if printed {
			fmt.Println()
		}
		wg.Done()
	}()

	wg.Wait()
	return tot
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
