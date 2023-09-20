package raft

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
