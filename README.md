# Capstone-Project

A Raft Consensus Algorithm Implementation in Golang

- Phase 1 ( Done ): Leader Election
- Phase 2 ( Done ): Log Replication
- Phase 3 ( TODO ): Data Persistence

## Run Tests & See Goroutine Stack Dump

```bash
# Turn on debug logger
./run_tests.sh -debug

# Turn off debug logger
./run_tests.sh
```

Then go to your browser at [http://localhost:{port}/debug/pprof/goroutine?debug=2](http://localhost:{port}/debug/pprof/goroutine?debug=2) to see the goroutine stack dump.
