package raft

import (
	"sync"
	"testing"
	"time"
)

type ReportReply struct {
	id       int
	term     int
	isLeader bool
}

type Cluster struct {
	// cluster is a list of all the raft servers participating in a cluster.
	cluster []*NetworkInterface

	// connected has a bool per networkInterface in cluster, specifying whether this networkInterface
	// is currently connected to peers (if false, it's partitioned and no messages
	// will pass to or from it).
	connected []bool

	allNodesAreReadyForIncomingSignal sync.WaitGroup
	readyForNewIncomingReport         *sync.WaitGroup

	commitChs []chan CommitEntry
	commits   [][]CommitEntry

	reportReplyCh chan *ReportReply

	n int
	t *testing.T
}

// MakeNewCluster creates a new test Cluster, initialized with n servers connected
// to each other.
func MakeNewCluster(t *testing.T, n int) *Cluster {
	ns := make([]*NetworkInterface, n)
	connected := make([]bool, n)
	ready := make(chan interface{})
	commitChs := make([]chan CommitEntry, n)
	commits := make([][]CommitEntry, n)
	readyForNewIncomingReport := sync.WaitGroup{}
	reportReplyCh := make(chan *ReportReply, n)

	// Create all Servers in this cluster, assign ids and peer ids.
	for i := 0; i < n; i++ {
		peerIds := make([]int, 0)
		for p := 0; p < n; p++ {
			if p != i {
				peerIds = append(peerIds, p)
			}
		}

		commitChs[i] = make(chan CommitEntry)
		ns[i] = MakeNewNetworkInterface(i, peerIds, ready, commitChs[i], &sync.WaitGroup{}, &readyForNewIncomingReport, reportReplyCh)
		ns[i].Serve()
	}

	// Connect all peers to each other.
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j {
				err := ns[i].ConnectToPeer(j, ns[j].server)
				if err != nil {
					panic(err)
				}
			}
		}
		connected[i] = true
	}
	close(ready)

	cluster := &Cluster{
		cluster:                           ns,
		connected:                         connected,
		allNodesAreReadyForIncomingSignal: sync.WaitGroup{},
		readyForNewIncomingReport:         &readyForNewIncomingReport,
		reportReplyCh:                     reportReplyCh,
		n:                                 n,
		t:                                 t,
		commitChs:                         commitChs,
		commits:                           commits,
	}

	for i := 0; i < n; i++ {
		go cluster.collectCommits(i)
	}

	return cluster
}

// Shutdown shuts down all the servers in the harness and waits for them to
// stop running.
func (cluster *Cluster) Shutdown() {
	for i := 0; i < cluster.n; i++ {
		cluster.cluster[i].DisconnectAll()
		cluster.connected[i] = false
	}
	for i := 0; i < cluster.n; i++ {
		go cluster.cluster[i].Shutdown()
	}
}

// DisconnectPeer disconnects a networkInterface from all other servers in the cluster.
func (cluster *Cluster) DisconnectPeer(id int) {
	DebuggerLog("Disconnect Node %d", id)
	cluster.cluster[id].DisconnectAll()
	for j := 0; j < cluster.n; j++ {
		if j != id {
			err := cluster.cluster[j].DisconnectPeer(id)
			if err != nil {
				panic(err)
			}
		}
	}
	cluster.connected[id] = false
}

// ReconnectPeer connects a networkInterface to all other servers in the cluster.
func (cluster *Cluster) ReconnectPeer(id int) {
	DebuggerLog("Reconnect Node %d", id)
	for j := 0; j < cluster.n; j++ {
		if j != id {
			if err := cluster.cluster[id].ConnectToPeer(j, cluster.cluster[j].server); err != nil {
				DebuggerLog("ReconnectPeer Node %d: %v", id, err)
			}
			if err := cluster.cluster[j].ConnectToPeer(id, cluster.cluster[id].server); err != nil {
				DebuggerLog("ReconnectPeer Node %d: %v", j, err)
			}
		}
	}
	cluster.connected[id] = true
}

// CheckSingleLeader checks that only a single networkInterface thinks it's the leader.
// Returns the leader's id and term. It retries several times if no leader is
// identified yet.
func (cluster *Cluster) CheckSingleLeader() (int, int) {
	for r := 0; r < cluster.n*10; r++ {
		leaderId := -1
		leaderTerm := -1
		for i := 0; i < cluster.n; i++ {
			if cluster.connected[i] {
				cluster.cluster[i].server.node.Report()
				reportReply := <-cluster.reportReplyCh
				term := reportReply.term
				isLeader := reportReply.isLeader
				DebuggerLog("CheckSingleLeader: networkInterface %d term %d isLeader %v", i, term, isLeader)
				if isLeader {
					if leaderId < 0 {
						leaderId = i
						leaderTerm = term
					} else {
						if leaderTerm == term {
							cluster.t.Fatalf("both %d and %d think they're leaders during term %d", leaderId, i, term)
						} else {
							// update the leaderId to the one with the higher term
							if leaderTerm < term {
								leaderId = i
								leaderTerm = term
							}
						}
					}
				}
			}
		}
		if leaderId >= 0 {
			DebuggerLog("CheckSingleLeader: return with leaderId %d leaderTerm %d", leaderId, leaderTerm)
			return leaderId, leaderTerm
		}
		time.Sleep(200 * time.Millisecond)
	}

	DebuggerLog("leader not found")
	return -1, -1
}

// CheckNoLeader checks that no connected networkInterface considers itself the leader.
func (cluster *Cluster) CheckNoLeader() {
	for i := 0; i < cluster.n; i++ {
		if cluster.connected[i] {
			cluster.cluster[i].server.node.Report()
			reportReply := <-cluster.reportReplyCh
			isLeader := reportReply.isLeader
			DebuggerLog("CheckNoLeader: networkInterface %d isLeader %v", i, isLeader)
			if isLeader {
				DebuggerLog("networkInterface %d leader; want none", i)
			}
		}
	}
}

func (cluster *Cluster) collectCommits(id int) {
	for c := range cluster.commitChs[id] {
		DebuggerLog("collectCommits(%d) got %+v", id, c)
		cluster.commits[id] = append(cluster.commits[id], c)
	}
}
