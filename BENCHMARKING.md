
For Benchmarking with ucsb you first need to compile the project with `build_release.sh`
or `build_debug.sh`. For that enable the databases to build in `CMakeLists.txt`
**before starting the compile**. If you already started compiling with other databases
enabled you *may* have to **rebuild from scratch**. To do that just delete the 
`build_release` or `build_debug` directory.

To be able to build haura you need to compile it separately and then copy the static
library `libbetree_storage_stack.a` to the `src/haura` directory.

Because the original project isn't really maintained that actively some databases
may not compile out of the box.
The following libraries compile and run right now for me in release mode without
changing any code:
- leveldb
- lmdb
- haura

The following libraries may compile and run with minor modifications such as 
missing headers:
- rocksdb (missing cstdint in 2 files)
- wiredtiger (missing a NULL check for a print)

The following libraries may compile with minor modifications such as missing headers
but do not run:
- mongodb (missing cstdint in 1 file, but it did not run because of missing mongod)
- redis (missing cstdint in 1 file, but it did not run because of missing redis-server and redis-cli)
