package raft

// command line to see the goroutinue stack dump: go test -v -run TestElectionBasic -bench=. -cpuprofile=cpu.pprof -memprofile=mem.pprof
// see the content at: http://localhost:6060/debug/pprof/goroutine?debug=2

import (
	"net/http"
	_ "net/http/pprof"
	"testing"
	"time"
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

func TestElectionLeaderDisconnect(t *testing.T) {
	startPProfServer()

	h := MakeNewCluster(t, 3)
	defer h.Shutdown()

	origLeaderId, origTerm := h.CheckSingleLeader()

	h.DisconnectPeer(origLeaderId)
	sleepWithMilliseconds(350)

	newLeaderId, newTerm := h.CheckSingleLeader()

	if newLeaderId == origLeaderId {
		t.Errorf("want new leader to be different from orig leader")
	}
	if newTerm <= origTerm {
		t.Errorf("want newTerm <= origTerm, got %d and %d", newTerm, origTerm)
	}
}

func TestElectionLeaderAndAnotherDisconnect(t *testing.T) {
	startPProfServer()

	h := MakeNewCluster(t, 3)
	defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()

	h.DisconnectPeer(origLeaderId)
	otherId := (origLeaderId + 1) % 3
	h.DisconnectPeer(otherId)

	// No quorum.
	sleepWithMilliseconds(450)
	h.CheckNoLeader()

	// Reconnect one other server; now we'll have quorum.
	h.ReconnectPeer(otherId)
	h.CheckSingleLeader()
}

func sleepWithMilliseconds(n int) {
	time.Sleep(time.Duration(n) * time.Millisecond)
}
