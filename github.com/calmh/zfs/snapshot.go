package zfs

// TakeSnapshot takes a non-recursive snapshot of dataset called name.
func TakeSnapshot(dataset, name string) error {
	_, err := zfs("snapshot", dataset+"@"+name)
	return err
}

// TakeSnapshotRecursive take s a recursive snapshot of dataset called name.
func TakeSnapshotRecursive(dataset, name string) error {
	_, err := zfs("snapshot", "-r", dataset+"@"+name)
	return err
}
