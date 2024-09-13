The following things should to be added or changed in the future:
- compile haura from scratch with the provided CMakeLists.txt to remove the need
    for manually copying the static `libbetree_storage_stack.a` file
- enable the `scan` benchmark on haura by providing and element wise scan in the haura bindings
- enable the `range_select` benchmark by providing that functionality in haura
- dynamically load configs based on the benchmark size by providing the option to
    load a config from a file in haura
- dynamically changing the file place to the one expected by ucsb by enabling to
    change some config parameters or by the previous point
- investigate on how to run the mongodb benchmark
- investigate on how to run the redis benchmark
- list the prerequisites to compile and run the benchmarks
- create some scripts to plot relevant statistics
