package raft

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"time"
)

type Server struct {
	mu sync.Mutex

	serverId int
	peerIds  []int

	node     *Node
	rpcProxy *RPCProxy

	rpcServer *rpc.Server
	listener  net.Listener

	peerClients map[int]*rpc.Client

	isReadyToStart         <-chan interface{}
	quit                   chan interface{}
	serverClusterWaitGroup sync.WaitGroup

	allNodesAreReadyForIncomingSignal *sync.WaitGroup
}

func MakeNewServer(serverId int, peerIds []int, ready <-chan interface{}, allNodesAreReadyForIncomingSignal *sync.WaitGroup) *Server {
	return &Server{
		serverId:                          serverId,
		peerIds:                           peerIds,
		peerClients:                       make(map[int]*rpc.Client),
		isReadyToStart:                    ready,
		quit:                              make(chan interface{}),
		allNodesAreReadyForIncomingSignal: allNodesAreReadyForIncomingSignal,
	}
}

func (s *Server) Serve() {
	s.mu.Lock()
	s.node = MakeNewNode(s.serverId, s.peerIds, s, s.isReadyToStart, s.allNodesAreReadyForIncomingSignal)

	// Create a new RPC server and register a RPCProxy that forwards all methods
	// to n.node
	s.rpcServer = rpc.NewServer()
	s.rpcProxy = &RPCProxy{node: s.node}
	s.rpcServer.RegisterName("Node", s.rpcProxy)

	var err error
	// randomly pick a port
	s.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}
	DebuggerLog("Server %v listening at %s", s.serverId, s.listener.Addr())
	s.mu.Unlock()

	s.serverClusterWaitGroup.Add(1)
	go func() {
		defer s.serverClusterWaitGroup.Done()

		for {
			conn, err := s.listener.Accept()
			if err != nil {
				select {
				case <-s.quit:
					return
				default:
					log.Fatal("accept error:", err)
				}
			}
			s.serverClusterWaitGroup.Add(1)
			go func() {
				s.rpcServer.ServeConn(conn)
				s.serverClusterWaitGroup.Done()
			}()
		}
	}()
}

// DisconnectAll closes all the client connections to peers for this server.
func (s *Server) DisconnectAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := range s.peerClients {
		if s.peerClients[id] != nil {
			s.peerClients[id].Close()
			s.peerClients[id] = nil
		}
	}
}

// Shutdown closes the server and waits for it to shut down properly.
func (s *Server) Shutdown() {
	DebuggerLog(fmt.Sprintf("Server %v shutdown", s.serverId))
	s.node.HandleStopRPC()
	close(s.quit)
	s.listener.Close()
	s.serverClusterWaitGroup.Wait()
}

func (s *Server) GetListenAddr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listener.Addr()
}

func (s *Server) ConnectToPeer(peerId int, addr net.Addr) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerClients[peerId] == nil {
		client, err := rpc.Dial(addr.Network(), addr.String())
		if err != nil {
			return err
		}
		s.peerClients[peerId] = client
	}
	return nil
}

// DisconnectPeer disconnects this server from the peer identified by peerId.
func (s *Server) DisconnectPeer(peerId int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerClients[peerId] != nil {
		err := s.peerClients[peerId].Close()
		s.peerClients[peerId] = nil
		return err
	}
	return nil
}

func (s *Server) Call(id int, serviceMethod string, args interface{}, reply interface{}) error {
	s.mu.Lock()
	peer := s.peerClients[id]
	s.mu.Unlock()

	// If this is called after shutdown (where client.Close is called), it will
	// return an error.
	if peer == nil {
		return fmt.Errorf("call client %d after it's closed", id)
	} else {
		return peer.Call(serviceMethod, args, reply)
	}
}

// RPCProxy is a trivial pass-thru proxy type for ConsensusModule's RPC methods.
// It's useful for:
//   - Simulating a small delay in RPC transmission.
//   - Avoiding running into https://github.com/golang/go/issues/19957
//   - Simulating possible unreliable connections by delaying some messages
//     significantly and dropping others when RAFT_UNRELIABLE_RPC is set.
type RPCProxy struct {
	node *Node
}

func (rpp *RPCProxy) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	DebuggerLog(fmt.Sprintf("RPCProxy.RequestVote from server %v: %+v", rpp.node.id, args))
	time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	return rpp.node.HandleRequestVoteRPC(args, reply)
}

func (rpp *RPCProxy) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	DebuggerLog(fmt.Sprintf("RPCProxy.AppendEntries: %+v", args))
	time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	return rpp.node.HandleAppendEntriesRPC(args, reply)
}
