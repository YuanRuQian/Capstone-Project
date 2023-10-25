package raft

import (
	"sync"
	"testing"
	"time"
)

type Cluster struct {
	// cluster is a list of all the raft servers participating in a cluster.
	cluster []*Server

	// connected has a bool per server in cluster, specifying whether this server
	// is currently connected to peers (if false, it's partitioned and no messages
	// will pass to or from it).
	connected []bool

	allNodesAreReadyForIncomingSignal sync.WaitGroup

	n int
	t *testing.T
}

// MakeNewCluster creates a new test Cluster, initialized with n servers connected
// to each other.
func MakeNewCluster(t *testing.T, n int) *Cluster {
	ns := make([]*Server, n)
	connected := make([]bool, n)
	ready := make(chan interface{})

	// Create all Servers in this cluster, assign ids and peer ids.
	for i := 0; i < n; i++ {
		peerIds := make([]int, 0)
		for p := 0; p < n; p++ {
			if p != i {
				peerIds = append(peerIds, p)
			}
		}

		ns[i] = MakeNewServer(i, peerIds, ready, &sync.WaitGroup{})
		ns[i].Serve()
	}

	// Connect all peers to each other.
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j {
				ns[i].ConnectToPeer(j, ns[j].GetListenAddr())
			}
		}
		connected[i] = true
	}
	close(ready)

	return &Cluster{
		cluster:                           ns,
		connected:                         connected,
		allNodesAreReadyForIncomingSignal: sync.WaitGroup{},
		n:                                 n,
		t:                                 t,
	}
}

// Shutdown shuts down all the servers in the harness and waits for them to
// stop running.
func (c *Cluster) Shutdown() {
	for i := 0; i < c.n; i++ {
		c.cluster[i].DisconnectAll()
		c.connected[i] = false
	}
	for i := 0; i < c.n; i++ {
		go c.cluster[i].Shutdown()
	}
}

// DisconnectPeer disconnects a server from all other servers in the cluster.
func (c *Cluster) DisconnectPeer(id int) {
	tlog("Disconnect %d", id)
	c.cluster[id].DisconnectAll()
	for j := 0; j < c.n; j++ {
		if j != id {
			c.cluster[j].DisconnectPeer(id)
		}
	}
	c.connected[id] = false
}

// ReconnectPeer connects a server to all other servers in the cluster.
func (c *Cluster) ReconnectPeer(id int) {
	tlog("Reconnect %d", id)
	for j := 0; j < c.n; j++ {
		if j != id {
			if err := c.cluster[id].ConnectToPeer(j, c.cluster[j].GetListenAddr()); err != nil {
				c.t.Fatal(err)
			}
			if err := c.cluster[j].ConnectToPeer(id, c.cluster[id].GetListenAddr()); err != nil {
				c.t.Fatal(err)
			}
		}
	}
	c.connected[id] = true
}

// CheckSingleLeader checks that only a single server thinks it's the leader.
// Returns the leader's id and term. It retries several times if no leader is
// identified yet.
func (c *Cluster) CheckSingleLeader() (int, int) {
	for r := 0; r < 20; r++ {
		leaderId := -1
		leaderTerm := -1
		for i := 0; i < c.n; i++ {
			if c.connected[i] {
				_, term, isLeader := c.cluster[i].node.Report()
				DebuggerLog("CheckSingleLeader: server %d term %d isLeader %v", i, term, isLeader)
				if isLeader {
					if leaderId < 0 {
						leaderId = i
						leaderTerm = term
					} else {
						if leaderTerm == term {
							c.t.Fatalf("both %d and %d think they're leaders during term %d", leaderId, i, term)
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
		time.Sleep(150 * time.Millisecond)
	}

	c.t.Fatalf("leader not found")
	return -1, -1
}

// CheckNoLeader checks that no connected server considers itself the leader.
func (c *Cluster) CheckNoLeader() {
	for i := 0; i < c.n; i++ {
		if c.connected[i] {
			_, _, isLeader := c.cluster[i].node.Report()
			DebuggerLog("CheckNoLeader: server %d isLeader %v", i, isLeader)
			if isLeader {
				c.t.Fatalf("server %d leader; want none", i)
			}
		}
	}
}
