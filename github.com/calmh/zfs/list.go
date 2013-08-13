package zfs

import (
	"fmt"
	"strings"
	"time"
)

type ListEntry struct {
	// Name of the filesystem, volume or clone in standard pool/fs format.
	Name string
	// NUmber of bytes used.
	Used uint64
	// NUmber of bytes available.
	Avail uint64
	// NUmber of bytes referred to.
	Refer uint64
	// File system mountpoint or "-".
	Mountpoint string
	// "filesystem", "volume" or "clone"
	Type string
}

type SnapshotEntry struct {
	Dataset  string
	Snapshot string
	Used     uint64
	Refer    uint64
	Creation time.Time
}

// ListDatasets lists regular ZFS datasets, i.e. filesystems, volumes and
// clones. Snapshots are not included, similarly to how they are not included
// in "zfs list" by default.
func ListDatasets() ([]ListEntry, error) {
	lines, err := zfs("list", "-Ho", "name,mountpoint,type")
	if err != nil {
		return nil, err
	}

	entries := make([]ListEntry, 0, len(lines))
	for _, line := range lines {
		if line == "" {
			continue
		}

		fields := strings.Fields(string(line))
		if len(fields) != 6 {
			return nil, fmt.Errorf("Unparseable line: %#v", line)
		}

		name := fields[0]
		mountpoint := fields[1]
		fstype := fields[2]

		e := ListEntry{Name: name, Mountpoint: mountpoint, Type: fstype}
		entries = append(entries, e)
	}
	return entries, nil
}

// ListSnapshots lists all ZFS snapshots on the specified dataset.
func ListSnapshots(ds string) ([]SnapshotEntry, error) {
	lines, err := zfs("list", "-Ho", "name,creation", "-t", "snapshot", "-r", "-d", "1", ds)
	if err != nil {
		return nil, err
	}

	entries := make([]SnapshotEntry, 0, len(lines))
	for _, line := range lines {
		fields := strings.SplitN(line, "\t", 2)

		name := fields[0]
		nameFields := strings.SplitN(name, "@", 2)
		creation, err := time.Parse("Mon Jan _2 15:04 2006", fields[1])
		if err != nil {
			return nil, err
		}

		e := SnapshotEntry{Dataset: nameFields[0], Snapshot: nameFields[1], Creation: creation}
		entries = append(entries, e)
	}
	return entries, nil
}
