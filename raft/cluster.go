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
	mutex sync.Mutex // Lock to protect shared access to this peer's state
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

	reportReplyCh        chan *ReportReply
	commandSubmitReplyCh chan bool

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
	commandSubmitReplyCh := make(chan bool)

	// Create all Servers in this cluster, assign ids and peer ids.
	for i := 0; i < n; i++ {
		peerIds := make([]int, 0)
		for p := 0; p < n; p++ {
			if p != i {
				peerIds = append(peerIds, p)
			}
		}

		commitChs[i] = make(chan CommitEntry)
		ns[i] = MakeNewNetworkInterface(i, peerIds, ready, commitChs[i], &sync.WaitGroup{}, &readyForNewIncomingReport, reportReplyCh, commandSubmitReplyCh)
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
		commandSubmitReplyCh:              commandSubmitReplyCh,
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
	for i := 0; i < cluster.n; i++ {
		close(cluster.commitChs[i])
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

func (cluster *Cluster) CheckCommitted(cmd int) (nc int, index int) {
	cluster.mutex.Lock()
	defer cluster.mutex.Unlock()

	// Find the length of the commits slice for connected servers.
	commitsLen := -1
	for i := 0; i < cluster.n; i++ {
		if cluster.connected[i] {
			if commitsLen >= 0 {
				// If this was set already, expect the new length to be the same.
				if len(cluster.commits[i]) != commitsLen {
					DebuggerLog("Cluster commits[%d] = %d, commitsLen = %d", i, cluster.commits[i], commitsLen)
				}
			} else {
				commitsLen = len(cluster.commits[i])
			}
		}
	}

	for c := 0; c < commitsLen; c++ {
		cmdAtC := -1
		for i := 0; i < cluster.n; i++ {
			if cluster.connected[i] {
				cmdOfN := cluster.commits[i][c].Command.(int)
				if cmdAtC >= 0 {
					if cmdOfN != cmdAtC {
						DebuggerLog("Cluster got %d, want %d at h.commits[%d][%d]", cmdOfN, cmdAtC, i, c)
					}
				} else {
					cmdAtC = cmdOfN
				}
			}
		}
		if cmdAtC == cmd {
			// Check consistency of Index.
			index := -1
			nc := 0
			for i := 0; i < cluster.n; i++ {
				if cluster.connected[i] {
					if index >= 0 && cluster.commits[i][c].Index != index {
						DebuggerLog("Cluster got Index=%d, want %d at h.commits[%d][%d]", cluster.commits[i][c].Index, index, i, c)
					} else {
						index = cluster.commits[i][c].Index
						DebuggerLog("Cluster got Index=%d", index)
					}
					nc++
				}
			}
			return nc, index
		}
	}

	// If there's no early return, we haven't found the command we were looking
	// for.
	cluster.t.Errorf("Cluster cmd=%d not found in commits", cmd)
	return -1, -1
}

func (cluster *Cluster) CheckCommittedN(cmd int, n int) {
	nc, _ := cluster.CheckCommitted(cmd)
	if nc != n {
		cluster.t.Errorf("CheckCommittedN got nc=%d, want %d", nc, n)
	}
}

func (cluster *Cluster) CheckNotCommitted(cmd int) {
	cluster.mutex.Lock()
	defer cluster.mutex.Unlock()

	for i := 0; i < cluster.n; i++ {
		if cluster.connected[i] {
			for c := 0; c < len(cluster.commits[i]); c++ {
				gotCmd := cluster.commits[i][c].Command.(int)
				if gotCmd == cmd {
					DebuggerLog("Cluster found %d at commits[%d][%d], expected none", cmd, i, c)
				}
			}
		}
	}
}

func (cluster *Cluster) SubmitToServer(serverId int, cmd interface{}) bool {
	DebuggerLog("Cluster SubmitToServer(%d, %+v)", serverId, cmd)
	cluster.cluster[serverId].server.node.Submit(cmd)
	return <-cluster.commandSubmitReplyCh
}
