package zfs

import (
	"io"
	"os/exec"
	"strings"
)

func zfs(args ...string) (lines []string, err error) {
	cmd := exec.Command("zfs", args...)
	bytes, err := cmd.CombinedOutput()

	tmpLines := strings.Split(string(bytes), "\n")
	lines = make([]string, 0, len(lines))
	for _, line := range tmpLines {
		if line != "" {
			lines = append(lines, line)
		}
	}
	return
}

func zfsPipe(args ...string) (io.WriteCloser, io.Reader, error) {
	cmd := exec.Command("zfs", args...)
	stdin, _ := cmd.StdinPipe()
	stdout, _ := cmd.StdoutPipe()
	err := cmd.Start()
	if err != nil {
		return nil, nil, err
	}
	return stdin, stdout, nil
}

func Receive(args ...string) (io.WriteCloser, error) {
	cmd := []string{"recv"}
	cmd = append(cmd, args...)
	stdin, _, err := zfsPipe(cmd...)
	return stdin, err
}

func Send(args ...string) (io.Reader, error) {
	cmd := []string{"send"}
	cmd = append(cmd, args...)
	_, stdout, err := zfsPipe(cmd...)
	return stdout, err
}
