#!/bin/bash

IsDebugMode=""

# Check for flags
while [ "$1" != "" ]; do
    case $1 in
        -debug )
            IsDebugMode="true"
            ;;
    esac
    shift
done

# Example usage
if [ "$IsDebugMode" = "true" ]; then
    echo "Debug mode is enabled"
fi


# Change directory to 'raft'
cd raft || exit

echo "Go to your browser at http://localhost:6060/debug/pprof/goroutine?debug=2 to see the goroutine stack dump."


# When you run your script with the -debug flag, it will set IsDebugMode to "true," enabling debug logging. If you run your script without the -debug flag, IsDebugMode will remain empty, and debug logging will be disabled.
#for ((i=1; i<=10; i++))
#do
    IsDebugMode=$IsDebugMode go test -v -run TestElectionBasic -bench=. -cpuprofile=cpu.pprof -memprofile=mem.pprof
# done

#for ((i=1; i<=10; i++))
#do
# IsDebugMode=$IsDebugMode go test -v -run TestElectionLeaderDisconnect -bench=. -cpuprofile=cpu.pprof -memprofile=mem.pprof
#done

# IsDebugMode=$IsDebugMode go test -v -run TestElectionLeaderAndAnotherDisconnect -bench=. -cpuprofile=cpu.pprof -memprofile=mem.pprof
# IsDebugMode=$IsDebugMode go test -v -run TestDisconnectAllThenRestore -bench=. -cpuprofile=cpu.pprof -memprofile=mem.pprof
# IsDebugMode=$IsDebugMode go test -v -run TestElectionLeaderDisconnectThenReconnect -bench=. -cpuprofile=cpu.pprof -memprofile=mem.pprof

# Open the browser with the pprof URL
if [[ $? -eq 0 ]]; then
    go tool pprof -http=:6060 cpu.pprof
else
    echo "Tests failed. Not opening the browser."
fi
