# Capstone-Project

A Fault-Tolerant Key/Value Storage System with Raft Consensus Algorithm

## Run Tests & See Goroutine Stack Dump

```bash
# Turn on debug logger
./run_tests.sh -debug

# Turn off debug logger
./run_tests.sh
```

Then go to your browser at [http://localhost:{port}/debug/pprof/goroutine?debug=2](http://localhost:{port}/debug/pprof/goroutine?debug=2) to see the goroutine stack dump.
