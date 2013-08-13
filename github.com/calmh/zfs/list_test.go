package zfs_test

import (
	"github.com/calmh/zsync/github.com/calmh/zfs"
	"os"
	"testing"
)

func init() {
	pwd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	os.Setenv("PATH", pwd+"/testbin:"+os.Getenv("PATH"))
}

var testListResult = []struct {
	Idx			int
	Name			string
	Used, Avail, Refer	uint64
	Mountpoint, Type	string
}{
	{0, "zones", 2923759939584, 1980555841536, 446464, "/zones", "filesystem"},
	{73, "zones/swap", 17721196544, 1997258719232, 1018318848, "-", "volume"},
}

func TestList(t *testing.T) {
	nItems := 77

	l, err := zfs.ListDatasets()
	if err != nil {
		t.Error(err)
	}
	if len(l) != nItems {
		t.Errorf("List should countain %d items but had %d", nItems, len(l))
	}
	for _, res := range testListResult {
		i := l[res.Idx]
		if i.Name != res.Name {
			t.Errorf("Name mismatch for %d: %s != %s", res.Idx, i.Name, res.Name)
		}
		if i.Used != res.Used {
			t.Errorf("Used mismatch for %d: %d != %d", res.Idx, i.Used, res.Used)
		}
		if i.Avail != res.Avail {
			t.Errorf("Avail mismatch for %d: %d != %d", res.Idx, i.Avail, res.Avail)
		}
		if i.Refer != res.Refer {
			t.Errorf("Refer mismatch for %d: %d != %d", res.Idx, i.Refer, res.Refer)
		}
		if i.Mountpoint != res.Mountpoint {
			t.Errorf("Mountpoint mismatch for %d: %s != %s", res.Idx, i.Mountpoint, res.Mountpoint)
		}
		if i.Type != res.Type {
			t.Errorf("Type mismatch for %d: %s != %s", res.Idx, i.Type, res.Type)
		}
	}
}

var testSnapshotResult = []struct {
	Idx			int
	Dataset, Snapshot	string
	Used, Refer		uint64
	Creation		int32
}{
	{9, "zones/0d6e2251-aa11-452b-afb7-e43c8e7bfe1c", "weekly-20130624T000004Z", 31232, 110592, 1372032004},
	{2483, "zones/var", "quick-20130715T130505Z", 49664, 2713229312, 1373893505},
}

func TestSnapshot(t *testing.T) {
	nItems := 2484

	l, err := zfs.ListSnapshots()
	if err != nil {
		t.Error(err)
	}
	if len(l) != nItems {
		t.Errorf("List should countain %d items but had %d", nItems, len(l))
	}
	for _, res := range testSnapshotResult {
		i := l[res.Idx]
		if i.Dataset != res.Dataset {
			t.Errorf("Dataset mismatch for %d: %s != %s", res.Idx, i.Dataset, res.Dataset)
		}
		if i.Snapshot != res.Snapshot {
			t.Errorf("Snapshot mismatch for %d: %s != %s", res.Idx, i.Snapshot, res.Snapshot)
		}
		if i.Used != res.Used {
			t.Errorf("Used mismatch for %d: %d != %d", res.Idx, i.Used, res.Used)
		}
		if i.Refer != res.Refer {
			t.Errorf("Refer mismatch for %d: %d != %d", res.Idx, i.Refer, res.Refer)
		}
		if i.Creation != res.Creation {
			t.Errorf("Type mismatch for %d: %d != %d", res.Idx, i.Creation, res.Creation)
		}
	}
}
