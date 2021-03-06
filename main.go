package main

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"io"
	"os"

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
	CmdResult
)

type Command struct {
	Command CommandIndex
	Params  []string
	Data    []byte
}

var opts struct {
	Verbose     []bool `long:"verbose" short:"v" description:"increase the output verbosity"`
	Progress    bool   `long:"progress" short:"p" description:"show progress indicator during send"`
	NoMount     bool   `long:"no-mount" short:"u" description:"do not mount the destination dataset after replication (i.e. do zfs recv -u)"`
	Rollback    bool   `long:"rollback" short:"F" description:"rollback the destination dataset prior to replication (i.e. do zfs recv -F)"`
	Recursive   bool   `long:"recursive" short:"R" description:"recursively send snapshots and child datasets (i.e. do zfs send -R)"`
	BufferMB    int    `long:"buffer" description:"buffer size (send & receive)" value-name:"MB" default:"128"`
	ZsyncPath   string `long:"zsync-path" default:"zsync" value-name:"PROGRAM" description:"specify the zsync to run on remote machine"`
	Server      bool   `long:"server"`
	verbosity   LogLevel
	bufferBytes int
	//SetReadOnly      bool   `long:"set-readonly" description:"do zfs set readonly=on on the destination"`
}

func main() {
	parser := flags.NewParser(&opts, flags.PassDoubleDash|flags.PrintErrors)
	parser.Usage = "[OPTIONS] <srcds>[@snapshot] <host>[:dstds]"
	args, err := parser.Parse()
	opts.verbosity = LogLevel(len(opts.Verbose))
	opts.bufferBytes = opts.BufferMB * 1024 * 1024

	if err != nil || !opts.Server && len(args) != 2 {
		fmt.Fprintln(os.Stderr)
		parser.WriteHelp(os.Stderr)
		fmt.Fprintf(os.Stderr, "\nExample:\n")
		fmt.Fprintf(os.Stderr, "  %s tank/data 172.16.32.12:tank/replicated\n", parser.ApplicationName)
		fmt.Fprintf(os.Stderr, "  %s -vpFuR tank/data@snap42 root@remote:tank/data\n\n", parser.ApplicationName)
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

func panicOn(e error) {
	if e != nil {
		fmt.Fprintf(os.Stderr, "panic: %v\n", e)
		os.Exit(3)
	}
}

func logf(level LogLevel, format string, args ...interface{}) {
	if opts.verbosity >= level {
		fmt.Fprintf(os.Stderr, format, args...)
	}
}

func printLines(prefix string, r io.Reader) {
	br := bufio.NewReader(r)
	for {
		bs, _, err := br.ReadLine()
		if err == io.EOF {
			break
		}
		fmt.Fprintf(os.Stderr, "%s%s\n", prefix, bs)
	}
}
