package raft

// command line to see the goroutinue stack dump: go test -v -run TestElectionBasic -bench=. -cpuprofile=cpu.pprof -memprofile=mem.pprof
// see the content at: http://localhost:6060/debug/pprof/goroutine?debug=2

import (
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"testing"
)

// findAvailablePort find an available port on localhost
func findAvailablePort() string {
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		panic(err)
	}
	defer func(listener net.Listener) {
		err := listener.Close()
		if err != nil {
			panic(err)
		}
	}(listener)
	return fmt.Sprintf("localhost:%d", listener.Addr().(*net.TCPAddr).Port)
}

func startPProfServer() {
	port := findAvailablePort()
	fmt.Printf("[TEST] Starting pprof server at %s\n", port)
	go func() {
		err := http.ListenAndServe(port, nil)
		if err != nil {
			panic(err)
		}
	}()
}

// test the most basic cluster election
func TestElectionBasic(t *testing.T) {
	startPProfServer()

	cluster := MakeNewCluster(t, 2)
	defer cluster.Shutdown()

	cluster.CheckSingleLeader()
}

// test if remove the leader, the cluster will elect a new leader
func TestElectionLeaderDisconnect(t *testing.T) {
	startPProfServer()

	cluster := MakeNewCluster(t, 5)
	defer cluster.Shutdown()

	origLeaderId, origTerm := cluster.CheckSingleLeader()

	cluster.DisconnectPeer(origLeaderId)
	sleepWithMilliseconds(500)

	newLeaderId, newTerm := cluster.CheckSingleLeader()

	if newLeaderId == origLeaderId {
		t.Errorf("want new leader to be different from orig leader")
	}
	if newTerm <= origTerm {
		t.Errorf("want newTerm <= origTerm, got %d and %d", newTerm, origTerm)
	}
}

// test if adding back nodes will bring back the election
func TestElectionLeaderAndAnotherDisconnect(t *testing.T) {
	startPProfServer()

	cluster := MakeNewCluster(t, 3)
	defer cluster.Shutdown()

	origLeaderId, _ := cluster.CheckSingleLeader()

	cluster.DisconnectPeer(origLeaderId)
	otherId := (origLeaderId + 1) % cluster.n
	cluster.DisconnectPeer(otherId)

	// One node left only, no quorum.
	sleepWithMilliseconds(500)
	cluster.CheckNoLeader()

	// Reconnect one other networkInterface; two nodes are active, now we'll have quorum.
	cluster.ReconnectPeer(otherId)
	sleepWithMilliseconds(500)
	cluster.CheckSingleLeader()
}

func TestDisconnectAllThenRestore(t *testing.T) {
	startPProfServer()

	cluster := MakeNewCluster(t, 3)
	defer cluster.Shutdown()

	sleepWithMilliseconds(500)
	//	Disconnect all servers from the start. There will be no leader.
	for i := 0; i < cluster.n; i++ {
		cluster.DisconnectPeer(i)
	}
	sleepWithMilliseconds(500)
	cluster.CheckNoLeader()

	// Reconnect all servers. A leader will be found.
	for i := 0; i < cluster.n; i++ {
		cluster.ReconnectPeer(i)
	}
	sleepWithMilliseconds(500)
	cluster.CheckSingleLeader()
}

func TestElectionLeaderDisconnectThenReconnect(t *testing.T) {
	startPProfServer()

	cluster := MakeNewCluster(t, 3)
	defer cluster.Shutdown()
	origLeaderId, _ := cluster.CheckSingleLeader()

	cluster.DisconnectPeer(origLeaderId)

	sleepWithMilliseconds(500)
	newLeaderId, newTerm := cluster.CheckSingleLeader()

	cluster.ReconnectPeer(origLeaderId)
	sleepWithMilliseconds(500)

	againLeaderId, againTerm := cluster.CheckSingleLeader()

	if newLeaderId != againLeaderId {
		t.Errorf("again leader id got %d; want %d", againLeaderId, newLeaderId)
	}
	if againTerm != newTerm {
		t.Errorf("again term got %d; want %d", againTerm, newTerm)
	}
}

func TestCommitOneCommand(t *testing.T) {
	startPProfServer()

	cluster := MakeNewCluster(t, 3)
	defer cluster.Shutdown()

	origLeaderId, _ := cluster.CheckSingleLeader()

	DebuggerLog("Cluster submitting 42 to Leader Node %d", origLeaderId)
	isLeader := cluster.SubmitToServer(origLeaderId, 42)

	if !isLeader {
		t.Errorf("Cluster want id=%d leader, but it's not", origLeaderId)
	}

	sleepWithMilliseconds(500)
	cluster.CheckCommittedN(42, 3)
}
