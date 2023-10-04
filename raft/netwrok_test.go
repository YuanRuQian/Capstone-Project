package raft

// command line to see the goroutinue stack dump: go test -v -run TestElectionBasic -bench=. -cpuprofile=cpu.pprof -memprofile=mem.pprof
// see the content at: http://localhost:6060/debug/pprof/goroutine?debug=2

import (
	"net/http"
	_ "net/http/pprof"
	"testing"
)

func startPProfServer() {
	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()
}

func TestElectionBasic(t *testing.T) {
	startPProfServer()

	h := MakeNewCluster(t, 3)
	defer h.Shutdown()

	h.CheckSingleLeader()
}
