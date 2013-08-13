package zfs_test

import (
	"github.com/calmh/zsync/github.com/calmh/zfs"
	"testing"
)

func TestSnapshotNonexistant(t *testing.T) {
	err := zfs.TakeSnapshot("nonexistant", "foo")
	if err == nil {
		t.Error("Unexpected success for snapshotting nonexistant dataset")
	}
}

func TestSnapshotOk(t *testing.T) {
	err := zfs.TakeSnapshot("zones", "foo")
	if err != nil {
		t.Error("Unexpected error", err)
	}
}
