#!/bin/bash

# Delete timestamp.csv if it exists
if [ -e "raft/timestamp.csv" ]; then
    rm "raft/timestamp.csv"
    echo "Deleted existing timestamp.csv"
fi

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

echo "Go to your browser at http://localhost:{port}/debug/pprof/goroutine?debug=2 to see the goroutine stack dump."


# When you run your script with the -debug flag, it will set IsDebugMode to "true," enabling debug logging. If you run your script without the -debug flag, IsDebugMode will remain empty, and debug logging will be disabled.
#for ((i=1; i<=50; i++))
#do
IsDebugMode=$IsDebugMode go test -race -v -run TestElectionBasic -bench=. -cpuprofile=cpu.pprof -memprofile=mem.pprof
#done

#for ((i=1; i<=50; i++))
#do
IsDebugMode=$IsDebugMode go test -race -v -run TestElectionLeaderDisconnect -bench=. -cpuprofile=cpu.pprof -memprofile=mem.pprof
#done

#for ((i=1; i<=20; i++))
#do
IsDebugMode=$IsDebugMode go test -race -v -run TestElectionLeaderAndAnotherDisconnect -bench=. -cpuprofile=cpu.pprof -memprofile=mem.pprof
#done

#for ((i=1; i<=50; i++))
#do
IsDebugMode=$IsDebugMode go test -race -v -run TestDisconnectAllThenRestore -bench=. -cpuprofile=cpu.pprof -memprofile=mem.pprof
#done

#for ((i=1; i<=50; i++))
#do
IsDebugMode=$IsDebugMode go test -race -v -run TestElectionLeaderDisconnectThenReconnect -bench=. -cpuprofile=cpu.pprof -memprofile=mem.pprof
#done

#for ((i=1; i<=50; i++))
#do
IsDebugMode=$IsDebugMode go test -race -v -run TestCommitOneCommandWithLeader -bench=. -cpuprofile=cpu.pprof -memprofile=mem.pprof
#done

#for ((i=1; i<=50; i++))
#do
IsDebugMode=$IsDebugMode go test -race -v -run TestSubmitNonLeaderFails -bench=. -cpuprofile=cpu.pprof -memprofile=mem.pprof
#done

#for ((i=1; i<=50; i++))
#do
IsDebugMode=$IsDebugMode go test -race -v -run TestCommitMultipleCommands -bench=. -cpuprofile=cpu.pprof -memprofile=mem.pprof
#done

#for ((i=1; i<=50; i++))
#do
IsDebugMode=$IsDebugMode go test -race -v -run TestCommitWithPeerDisconnectionAndRecover -bench=. -cpuprofile=cpu.pprof -memprofile=mem.pprof
#done

#for ((i=1; i<=50; i++))
#do
IsDebugMode=$IsDebugMode go test -race -v -run TestCommitsWithLeaderDisconnects -bench=. -cpuprofile=cpu.pprof -memprofile=mem.pprof
#done

#for ((i=1; i<=50; i++))
#do
IsDebugMode=$IsDebugMode go test -race -v -run TestNoCommitWithNoQuorum -bench=. -cpuprofile=cpu.pprof -memprofile=mem.pprof
#done

# Open the browser with the pprof URL
if [[ $? -eq 0 ]]; then
    echo "Tests passed. Opening the browser at: http://localhost:{port}/debug/pprof/goroutine?debug=2 to see the goroutine stack dump."
else
    echo "Tests failed. Not opening the browser."
fi
