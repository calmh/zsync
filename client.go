package main

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"

	"github.com/calmh/zfs"
)

func client(ds, host string) {
	var command Command
	var sourceSs string
	var serverDs string

	if strings.ContainsRune(host, ':') {
		fs := strings.SplitN(host, ":", 2)
		host = fs[0]
		serverDs = fs[1]
	} else {
		serverDs = ds
	}

	if strings.ContainsRune(ds, '@') {
		fs := strings.SplitN(ds, "@", 2)
		ds = fs[0]
		sourceSs = fs[1]
	}

	sshCmd := exec.Command("ssh", host, opts.ZsyncPath, "--server")
	stdin, err := sshCmd.StdinPipe()
	panicOn(err)
	stdout, err := sshCmd.StdoutPipe()
	panicOn(err)
	stderr, err := sshCmd.StderrPipe()
	panicOn(err)

	go printLines("remote: ", stderr)

	err = sshCmd.Start()
	panicOn(err)

	e := gob.NewEncoder(stdin)
	d := gob.NewDecoder(stdout)

	negotiateVersion(e, d)

	command = Command{Command: CmdListSnapshots, Params: []string{serverDs}}
	err = e.Encode(&command)
	panicOn(err)

	var serverSnapshots []zfs.SnapshotEntry
	err = d.Decode(&serverSnapshots)
	panicOn(err)

	clientSnapshots, err := zfs.ListSnapshots(ds)
	panicOn(err)

	var toSend *zfs.SnapshotEntry
	if sourceSs != "" {
		for i, s := range clientSnapshots {
			if s.Snapshot == sourceSs {
				toSend = &s
				clientSnapshots = clientSnapshots[:i+1]
				break
			}
		}
	} else {
		toSend = &clientSnapshots[len(clientSnapshots)-1]
		logf(VERBOSE, "zsync: our latest: %s@%s\n", toSend.Dataset, toSend.Snapshot)
	}

	if toSend == nil {
		logf(INFO, "zsync: no snapshot to send\n")
		return
	}

	latest := latestCommon(serverSnapshots, clientSnapshots)
	if latest != nil {
		logf(VERBOSE, "zsync: snapshot in common: %s@%s\n", latest.Dataset, latest.Snapshot)
		if toSend.Snapshot == latest.Snapshot {
			logf(INFO, "zsync: nothing to send (destination in sync)\n")
			return
		}
	} else {
		logf(VERBOSE, "zsync: remote dataset missing or no snapshots in common\n")
	}

	params := []string{"send"}
	if opts.Recursive {
		params = append(params, "-R")
	}
	if latest != nil {
		params = append(params, "-I", "@"+latest.Snapshot)
	}
	params = append(params, ds+"@"+toSend.Snapshot)

	sendCmd := exec.Command("zfs", params...)
	stream, _ := sendCmd.StdoutPipe()
	sendStderr, err := sendCmd.StderrPipe()
	panicOn(err)

	go printLines("zfs send: ", sendStderr)

	err = sendCmd.Start()
	panicOn(err)

	params = nil
	if opts.Rollback {
		params = append(params, "-F")
	}
	if opts.NoMount {
		params = append(params, "-u")
	}
	params = append(params, serverDs)
	sc := Command{Command: CmdReceive, Params: params}
	err = e.Encode(sc)
	panicOn(err)

	logf(VERBOSE, "zsync: sending %s@%s\n", toSend.Dataset, toSend.Snapshot)

	t0 := time.Now()
	bufout := bufio.NewWriterSize(stdin, opts.bufferBytes)
	chunkout := ChunkedWriter{bufout}
	tot, err := io.Copy(chunkout, stream)
	panicOn(err)
	err = chunkout.Flush()
	panicOn(err)
	err = bufout.Flush()
	panicOn(err)

	err = sendCmd.Wait()
	panicOn(err)

	err = d.Decode(&command)
	panicOn(err)

	stdin.Close()

	err = sshCmd.Wait()
	panicOn(err)

	td := time.Since(t0)
	logf(INFO, "zsync: sent %s@%s; %sB in %.2f seconds (%sB/s)\n", toSend.Dataset, toSend.Snapshot, toSi(int(tot)), td.Seconds(), toSi(int(float64(tot)/td.Seconds())))
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
