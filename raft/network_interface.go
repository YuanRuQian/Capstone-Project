package raft

import (
	"fmt"
	"sync"
)

type Server struct {
	node *Node
}

func (s Server) HandleStopRPC() {
	s.node.HandleStopRPC()
}

type NetworkInterface struct {
	mu sync.Mutex

	serverId    int
	peerIds     []int
	peerServers map[int]*Server

	commitCh chan<- CommitEntry

	server *Server

	isReadyToStart         <-chan interface{}
	quit                   chan interface{}
	serverClusterWaitGroup sync.WaitGroup

	allNodesAreReadyForIncomingSignal *sync.WaitGroup
	readyForNewIncomingReport         *sync.WaitGroup

	reportReplyCh        chan *ReportReply
	commandSubmitReplyCh chan bool

	hasBeenShutdown bool
}

func MakeNewNetworkInterface(serverId int, peerIds []int, ready <-chan interface{}, commitCh chan<- CommitEntry, allNodesAreReadyForIncomingSignal, readyForNewIncomingReport *sync.WaitGroup, reportReplyCh chan *ReportReply, commandSubmitReplyCh chan bool) *NetworkInterface {
	return &NetworkInterface{
		serverId:                          serverId,
		peerIds:                           peerIds,
		peerServers:                       make(map[int]*Server),
		isReadyToStart:                    ready,
		quit:                              make(chan interface{}),
		allNodesAreReadyForIncomingSignal: allNodesAreReadyForIncomingSignal,
		readyForNewIncomingReport:         readyForNewIncomingReport,
		reportReplyCh:                     reportReplyCh,
		hasBeenShutdown:                   true,
		commitCh:                          commitCh,
		commandSubmitReplyCh:              commandSubmitReplyCh,
	}
}

func (networkInterface *NetworkInterface) Serve() {
	networkInterface.mu.Lock()
	node := MakeNewNode(networkInterface.serverId, networkInterface.peerIds, networkInterface, networkInterface.isReadyToStart, networkInterface.commitCh, networkInterface.allNodesAreReadyForIncomingSignal)
	networkInterface.server = &Server{node: node}
	networkInterface.hasBeenShutdown = false
	networkInterface.mu.Unlock()

	networkInterface.serverClusterWaitGroup.Add(1)
	go func() {
		defer networkInterface.serverClusterWaitGroup.Done()

		for {
			select {
			case <-networkInterface.quit:
				return
			default:
			}
			networkInterface.serverClusterWaitGroup.Add(1)
			go func() {
				networkInterface.serverClusterWaitGroup.Done()
			}()
		}
	}()
}

// DisconnectAll closes all the client connections to peers for this networkInterface.
func (networkInterface *NetworkInterface) DisconnectAll() {
	networkInterface.mu.Lock()
	defer networkInterface.mu.Unlock()
	for id := range networkInterface.peerServers {
		if networkInterface.peerServers[id] != nil {
			networkInterface.peerServers[id] = nil
		}
	}
}

// Shutdown closes the networkInterface and waits for it to shut down properly.
func (networkInterface *NetworkInterface) Shutdown() {
	DebuggerLog(fmt.Sprintf("NetworkInterface %v shutdown", networkInterface.serverId))
	networkInterface.mu.Lock()
	networkInterface.hasBeenShutdown = true
	networkInterface.mu.Unlock()
	networkInterface.server.HandleStopRPC()
	close(networkInterface.quit)
	networkInterface.serverClusterWaitGroup.Wait()
}

func (networkInterface *NetworkInterface) ConnectToPeer(peerId int, server *Server) error {
	networkInterface.mu.Lock()
	defer networkInterface.mu.Unlock()
	if networkInterface.peerServers[peerId] == nil {
		networkInterface.peerServers[peerId] = server
	}
	return nil
}

// DisconnectPeer disconnects this networkInterface from the peer identified by peerId.
func (networkInterface *NetworkInterface) DisconnectPeer(peerId int) error {
	networkInterface.mu.Lock()
	defer networkInterface.mu.Unlock()
	if networkInterface.peerServers[peerId] != nil {
		networkInterface.peerServers[peerId] = nil
	}
	return nil
}

func (networkInterface *NetworkInterface) prePRCShutdownCheck() bool {
	networkInterface.mu.Lock()
	defer networkInterface.mu.Unlock()
	return networkInterface.hasBeenShutdown
}

func (networkInterface *NetworkInterface) RequestVote(id int, args RequestVoteArgs) {
	if hasBeenShutdown := networkInterface.prePRCShutdownCheck(); hasBeenShutdown {
		DebuggerLog(fmt.Sprintf("NetworkInterface %v has been shutdown, no more RequestVote", networkInterface.serverId))
		return
	}

	networkInterface.mu.Lock()
	defer networkInterface.mu.Unlock()

	if networkInterface.peerServers[id] == nil || networkInterface.peerServers[id].node == nil {
		return
	}

	err := networkInterface.peerServers[id].node.HandleRequestVoteRPC(args)
	if err != nil {
		panic(fmt.Sprintf("Error in RequestVote RPC: %v", err))
	}
}

func (networkInterface *NetworkInterface) AppendEntries(currentNextIndex, receiverId int, args AppendEntriesArgs) {
	if hasBeenShutdown := networkInterface.prePRCShutdownCheck(); hasBeenShutdown {
		DebuggerLog(fmt.Sprintf("NetworkInterface %v has been shutdown, no more AppendEntries", networkInterface.serverId))
		return
	}

	networkInterface.mu.Lock()
	defer networkInterface.mu.Unlock()

	if networkInterface.peerServers[receiverId] == nil || networkInterface.peerServers[receiverId].node == nil {
		return
	}

	err := networkInterface.peerServers[receiverId].node.HandleAppendEntriesRPC(currentNextIndex, receiverId, args)
	if err != nil {
		panic(fmt.Sprintf("Error in AppendEntries RPC: %v", err))
	}
}

func (networkInterface *NetworkInterface) SendAppendEntriesReply(currentNextIndex, replierId, destinationId int, args AppendEntriesArgs, reply AppendEntriesReply) {
	if hasBeenShutdown := networkInterface.prePRCShutdownCheck(); hasBeenShutdown {
		DebuggerLog(fmt.Sprintf("NetworkInterface %v has been shutdown, no more SendAppendEntriesReply", networkInterface.serverId))
		return
	}

	networkInterface.mu.Lock()
	defer networkInterface.mu.Unlock()

	if networkInterface.peerServers[destinationId] == nil || networkInterface.peerServers[destinationId].node == nil {
		return
	}

	networkInterface.peerServers[destinationId].node.HandleAppendEntriesReplyRPC(replierId, args, reply)
}

func (networkInterface *NetworkInterface) SendRequestVoteReply(replierId, destinationId int, reply RequestVoteReply) {
	if hasBeenShutdown := networkInterface.prePRCShutdownCheck(); hasBeenShutdown {
		DebuggerLog(fmt.Sprintf("NetworkInterface %v has been shutdown, no more SendRequestVoteReply", networkInterface.serverId))
		return
	}

	networkInterface.mu.Lock()
	defer networkInterface.mu.Unlock()

	if networkInterface.peerServers[destinationId] == nil || networkInterface.peerServers[destinationId].node == nil {
		return
	}

	networkInterface.peerServers[destinationId].node.HandleRequestVoteReplyRPC(replierId, reply)
}
