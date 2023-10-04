#!/bin/bash

# Change directory to 'raft'
cd raft || exit

echo "Go to your browser at http://localhost:6060/debug/pprof/goroutine?debug=2 to see the goroutine stack dump."

# Run the tests with profiling
go test -v -run TestElectionBasic -bench=. -cpuprofile=cpu.pprof -memprofile=mem.pprof

# Open the browser with the pprof URL
if [[ $? -eq 0 ]]; then
    go tool pprof -http=:6060 cpu.pprof
else
    echo "Tests failed. Not opening the browser."
fi
