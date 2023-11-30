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

func TestCommitOneCommandWithLeader(t *testing.T) {
	startPProfServer()

	cluster := MakeNewCluster(t, 3)
	defer cluster.Shutdown()

	leaderId, _ := cluster.CheckSingleLeader()

	value := 42
	DebuggerLog("Cluster submitting %d to Leader Node %d", value, leaderId)
	isLeader := cluster.SubmitToServer(leaderId, value)

	if !isLeader {
		t.Errorf("Cluster want id=%d leader, but it's not", leaderId)
	}

	sleepWithMilliseconds(500)
	cluster.CheckCommittedN(42, 3)

	cluster.CheckIfLogsInOrder([]int{value})
}

func TestSubmitNonLeaderFails(t *testing.T) {
	startPProfServer()

	size := 5
	cluster := MakeNewCluster(t, size)
	defer cluster.Shutdown()

	leaderId, _ := cluster.CheckSingleLeader()
	nonLeaderNodeId := (leaderId + 1) % size

	value := 42
	DebuggerLog("Cluster submitting %d to Non Leader Node %d", value, leaderId)
	isLeader := cluster.SubmitToServer(nonLeaderNodeId, value)
	if isLeader {
		t.Errorf("Cluster want Non Leader Node %d, but it is leader", nonLeaderNodeId)
	}

	cluster.CheckIfLogsInOrder([]int{})
}

func TestCommitMultipleCommands(t *testing.T) {
	startPProfServer()

	size := 5
	cluster := MakeNewCluster(t, size)
	defer cluster.Shutdown()

	leaderId, _ := cluster.CheckSingleLeader()

	values := []int{42, 55, 81}
	for _, v := range values {
		DebuggerLog("Cluster submitting %v to Leader Node %d", v, leaderId)
		isLeader := cluster.SubmitToServer(leaderId, v)
		if !isLeader {
			t.Errorf("Cluster want Leader Node %d, but it is not a leader", leaderId)
		}
		sleepWithMilliseconds(100)
	}

	sleepWithMilliseconds(150)

	areLogsInOrder := cluster.CheckIfLogsInOrder(values)

	if !areLogsInOrder {
		t.Errorf("Cluster want logs in order, but they are not")
	}

	prevI := -1

	for _, v := range values {
		nc, i := cluster.CheckCommitted(v)
		if nc != size {
			t.Errorf("Cluster want nc=%d, got %d", size, nc)
		}
		if prevI >= i {
			t.Errorf("Cluster want prevI<i, got prevI=%d i=%d", prevI, i)
		}
		prevI = i
	}
}

func TestCommitWithPeerDisconnectionAndRecover(t *testing.T) {
	startPProfServer()

	size := 3
	cluster := MakeNewCluster(t, size)
	defer cluster.Shutdown()

	leaderId, _ := cluster.CheckSingleLeader()

	values := []int{42, 55, 81}

	for _, v := range values {
		DebuggerLog("Cluster submitting %v to Leader Node %d", v, leaderId)
		isLeader := cluster.SubmitToServer(leaderId, v)
		if !isLeader {
			t.Errorf("Cluster want Leader Node %d, but it is not a leader", leaderId)
		}
		sleepWithMilliseconds(100)
	}

	sleepWithMilliseconds(500)

	for _, v := range values {
		cluster.CheckCommittedN(v, size)
	}

	peerId := (leaderId + 1) % size
	cluster.DisconnectPeer(peerId)
	sleepWithMilliseconds(250)

	newVal := 21

	cluster.SubmitToServer(leaderId, newVal)
	sleepWithMilliseconds(250)
	cluster.CheckCommittedN(newVal, size-1)

	cluster.ReconnectPeer(peerId)
	sleepWithMilliseconds(200)
	cluster.CheckSingleLeader()

	sleepWithMilliseconds(150)
	cluster.CheckCommittedN(newVal, size)

	cluster.CheckIfLogsInOrder([]int{42, 55, 81, 21})
}

func TestCommitsWithLeaderDisconnects(t *testing.T) {
	startPProfServer()

	size := 5

	cluster := MakeNewCluster(t, size)
	defer cluster.Shutdown()

	oldLeaderId, _ := cluster.CheckSingleLeader()

	values := []int{42, 55}
	for _, v := range values {
		cluster.SubmitToServer(oldLeaderId, v)
	}

	sleepWithMilliseconds(150)

	for _, v := range values {
		cluster.CheckCommittedN(v, size)
	}

	cluster.DisconnectPeer(oldLeaderId)
	sleepWithMilliseconds(150)

	newVal1 := 21
	cluster.SubmitToServer(oldLeaderId, newVal1)

	sleepWithMilliseconds(150)
	cluster.CheckNotCommitted(newVal1)

	newLeaderId, _ := cluster.CheckSingleLeader()

	newVal2 := 22

	cluster.SubmitToServer(newLeaderId, newVal2)
	sleepWithMilliseconds(150)
	cluster.CheckCommittedN(newVal2, size-1)

	cluster.ReconnectPeer(oldLeaderId)
	sleepWithMilliseconds(500)

	finalLeaderId, _ := cluster.CheckSingleLeader()
	if finalLeaderId == oldLeaderId {
		t.Errorf("Cluster got finalLeaderId==oldLeaderId==%d, yet want them different", finalLeaderId)
	}

	newVal3 := 23
	cluster.SubmitToServer(newLeaderId, newVal3)
	sleepWithMilliseconds(300)
	cluster.CheckCommittedN(newVal3, size)
	cluster.CheckCommittedN(newVal2, size)
	cluster.CheckNotCommitted(newVal1)

	cluster.CheckIfLogsInOrder([]int{42, 55, 22, 23})
}

func TestNoCommitWithNoQuorum(t *testing.T) {
	startPProfServer()

	size := 3

	cluster := MakeNewCluster(t, size)
	defer cluster.Shutdown()

	values := []int{42, 55, 81}
	ogLeaderId, ogTerm := cluster.CheckSingleLeader()

	for _, v := range values {
		cluster.SubmitToServer(ogLeaderId, v)
	}

	sleepWithMilliseconds(300)
	for _, v := range values {
		cluster.CheckCommittedN(v, size)
	}

	peer1 := (ogLeaderId + 1) % size
	peer2 := (ogLeaderId + 2) % size
	cluster.DisconnectPeer(peer1)
	cluster.DisconnectPeer(peer2)
	sleepWithMilliseconds(250)

	newVal1 := 21
	cluster.SubmitToServer(ogLeaderId, newVal1)
	sleepWithMilliseconds(250)
	cluster.CheckNotCommitted(newVal1)

	cluster.ReconnectPeer(peer1)
	cluster.ReconnectPeer(peer2)
	sleepWithMilliseconds(600)

	cluster.CheckNotCommitted(newVal1)

	newLeaderId, newTerm := cluster.CheckSingleLeader()
	if ogTerm == newTerm {
		t.Errorf("Cluster got ogTerm==newTerm==%d; but want them different", ogTerm)
	}

	newValues := []int{22, 23, 24}
	for _, v := range newValues {
		cluster.SubmitToServer(newLeaderId, v)
	}

	sleepWithMilliseconds(350)

	for _, v := range newValues {
		cluster.CheckCommittedN(v, size)
	}

	cluster.CheckIfLogsInOrder([]int{42, 55, 81, 22, 23, 24})
}
