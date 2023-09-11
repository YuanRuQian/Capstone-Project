package raft

// This code is designed to work in a local testing environment to simulate a Raft cluster

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
)

type RPCProxy struct {
	node *Node
}

type Server struct {
	mu sync.Mutex

	serverId int
	peerIds  []int

	node *Node
	// The RPCProxy is used to register RPC methods for the node
	rpcProxy *RPCProxy

	rpcServer *rpc.Server
	listener  net.Listener

	peerIdToClientsMapping map[int]*rpc.Client

	isReadyToStart <-chan interface{}
}

func MakeNewServer(serverId int, peersIds []int, isReadyToStart <-chan interface{}) *Server {
	return &Server{
		serverId:               serverId,
		peerIds:                peersIds,
		peerIdToClientsMapping: make(map[int]*rpc.Client),
		isReadyToStart:         isReadyToStart,
	}
}

func (s *Server) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.node = MakeNewNode(s.serverId, s.peerIds, s, s.isReadyToStart)

	s.rpcServer = rpc.NewServer()
	s.rpcProxy = &RPCProxy{s.node}
	err := s.rpcServer.RegisterName("Node", s.rpcProxy)

	if err != nil {
		DebuggerLog("Error in registering RPC: %v", err)
		panic(err)
	}

	// ":0" : A special notation in Go to indicate that the operating system should choose an available port, typically an unused port greater than 1024
	s.listener, err = net.Listen("tcp", ":0")

	if err != nil {
		DebuggerLog("Error in starting server: %v", err)
		panic(err)
	}

	DebuggerLog("Server %v started at %v", s.serverId, s.listener.Addr())

}

func (s *Server) Kill() {
	s.node.Kill()
	err := s.listener.Close()
	if err != nil {
		DebuggerLog("Error in closing listener: %v", err)
		panic(err)
	}
}

// Call Make RPC calls to other servers in the cluster
func (s *Server) Call(id int, serviceMethod string, args interface{}, reply interface{}) error {
	s.mu.Lock()
	client := s.peerIdToClientsMapping[id]
	s.mu.Unlock()

	if client == nil {
		return fmt.Errorf("client %v not found or it has been closed", id)
	} else {
		return client.Call(serviceMethod, args, reply)
	}
}

// RequestVote delegate RPC RequestVote calls to the associated node
func (proxy *RPCProxy) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	return proxy.node.RequestVote(args, reply)
}

// AppendEntries delegate RPC AppendEntries calls to the associated node
func (proxy *RPCProxy) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	return proxy.node.AppendEntries(args, reply)
}

func (s *Server) ConnectTo(peerId int, addr net.Addr) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerIdToClientsMapping[peerId] == nil {
		client, err := rpc.Dial(addr.Network(), addr.String())
		if err != nil {
			return err
		}
		s.peerIdToClientsMapping[peerId] = client
		DebuggerLog("Server %v connected to %v", s.serverId, peerId)
	}
	return nil
}

func (s *Server) DisconnectFrom(peerId int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerIdToClientsMapping[peerId] != nil {
		err := s.peerIdToClientsMapping[peerId].Close()
		if err != nil {
			return err
		}
		s.peerIdToClientsMapping[peerId] = nil
		DebuggerLog("Server %v disconnected from %v", s.serverId, peerId)
	}
	return nil
}

func (s *Server) DisconnectFromAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for peerId := range s.peerIdToClientsMapping {
		err := s.DisconnectFrom(peerId)
		if err != nil {
			panic(err)
		}
	}
}

func (s *Server) GetIDTermIsLeader() (int, int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.node.currentTerm, s.node.id, s.node.state == Leader
}
