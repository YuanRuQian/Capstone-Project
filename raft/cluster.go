package raft

import (
	"testing"
	"time"
)

type Cluster struct {
	cluster     []*Server
	isConnected []bool
	size        int
}

func MakeAndStartNewCluster(t *testing.T, size int) *Cluster {
	servers := make([]*Server, size)
	isConnected := make([]bool, size)
	isReadyToStart := make(chan interface{})

	// create servers and start them
	for i := 0; i < size; i++ {
		peersIds := make([]int, 0)
		for peerId := 0; peerId < size; peerId++ {
			// for peers, exclude self
			if peerId != i {
				peersIds = append(peersIds, peerId)
			}
		}
		servers[i] = MakeNewServer(i, peersIds, isReadyToStart)
		servers[i].Start()
	}

	// connect servers to each other
	for i := 0; i < size; i++ {
		for j := 0; j < size; j++ {
			if i != j {
				err := servers[i].ConnectTo(j, servers[j].listener.Addr())
				if err != nil {
					panic(err)
				}
			}
		}
		isConnected[i] = true
	}

	// Signal readiness by closing the isReadyToStart channel
	// So each node can start its main loop after they are all connected
	// Else would cause server not found error during intialization
	close(isReadyToStart)

	return &Cluster{
		cluster:     servers,
		isConnected: isConnected,
		size:        size,
	}
}

func (c *Cluster) KillAll() {
	for i := 0; i < c.size; i++ {
		c.cluster[i].DisconnectFromAll()
		c.isConnected[i] = false
	}

	for i := 0; i < c.size; i++ {
		c.cluster[i].Kill()
	}
}

func (c *Cluster) DisconnectServerFromPeers(serverId int) {
	c.cluster[serverId].DisconnectFromAll()

	for i := 0; i < c.size; i++ {
		if i != serverId {
			err := c.cluster[i].DisconnectFrom(serverId)
			if err != nil {
				return
			}
		}
	}

	c.isConnected[serverId] = false
}

func (c *Cluster) ConnectServerToPeers(serverId int) {
	for i := 0; i < c.size; i++ {
		if i != serverId {
			err := c.cluster[serverId].ConnectTo(i, c.cluster[i].listener.Addr())
			if err != nil {
				panic(err)
			}
		}
	}

	c.isConnected[serverId] = true
}

func (c *Cluster) GetLeaderIDAndTerm() (int, int) {

	for attempts := 0; attempts < 10; attempts++ {

		leaderId := -1
		leaderTerm := -1

		for i := 0; i < c.size; i++ {
			if c.isConnected[i] {
				_, term, isLeader := c.cluster[i].GetIDTermIsLeader()
				if isLeader {
					if leaderId != -1 {
						DebuggerLog("GetLeaderIDAndTerm : %v is already leader", i)
					}
					leaderId = i
					leaderTerm = term
				}
			}
		}

		if leaderId > 0 {
			DebuggerLog("GetLeaderIDAndTerm : Leader is %v, term is %v", leaderId, leaderTerm)
			return leaderId, leaderTerm
		}

		c.Sleep(150)
	}

	panic("No leader found")
	return -1, -1
}

func (c *Cluster) Sleep(i int) {
	time.Sleep(time.Duration(i) * time.Millisecond)
}
