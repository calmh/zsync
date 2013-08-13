zsync_src = main.go
zfs_src = $(shell ls github.com/calmh/zfs/*.go | grep -v _test)
zfs_obj = github.com/calmh/zfs.o
flags_src = $(shell ls github.com/jessevdk/go-flags/*.go | grep -v _test | grep -v _other | grep -v _linux | grep -v _windows) 
flags_obj = github.com/jessevdk/go-flags.o

zsync: $(zsync_src) $(zfs_obj) $(flags_obj)
        gccgo -static-libgo -static-libgcc -lnsl -lsocket -o $@ $^

$(zfs_obj): $(zfs_src)
        gccgo -c -o $@ $^

$(flags_obj): $(flags_src)
        gccgo -c -o $@ $^

clean:
        rm zsync $(zfs_obj) $(flags_obj)
