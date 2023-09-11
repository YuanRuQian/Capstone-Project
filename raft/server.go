package raft

// This code is designed to work in a local testing environment to simulate a Raft cluster

import (
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"time"
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

	shouldQuit chan interface{}

	waitGroup sync.WaitGroup
}

func MakeNewServer(serverId int, peersIds []int, isReadyToStart <-chan interface{}) *Server {
	return &Server{
		serverId:               serverId,
		peerIds:                peersIds,
		peerIdToClientsMapping: make(map[int]*rpc.Client),
		isReadyToStart:         isReadyToStart,
		shouldQuit:             make(chan interface{}),
	}
}

func (s *Server) Start() {
	s.mu.Lock()

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
	s.mu.Unlock()

	s.waitGroup.Add(1)

	go func() {
		defer s.waitGroup.Done()

		for {
			conn, err := s.listener.Accept()
			if err != nil {
				select {
				case <-s.shouldQuit:
					return
				default:
					DebuggerLog("Error in accepting connection: %v", err)
					panic(err)
				}
			}
			s.waitGroup.Add(1)
			go func() {
				s.rpcServer.ServeConn(conn)
				s.waitGroup.Done()
			}()
		}
	}()
}

func (s *Server) Kill() {
	DebuggerLog("Begin Server %v kill", s.serverId)
	s.node.Kill()
	close(s.shouldQuit)
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
	time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	return proxy.node.RequestVote(args, reply)
}

// AppendEntries delegate RPC AppendEntries calls to the associated node
func (proxy *RPCProxy) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
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
		if s.peerIdToClientsMapping[peerId] != nil {
			err := s.peerIdToClientsMapping[peerId].Close()
			if err != nil {
				panic(err)
			}
			s.peerIdToClientsMapping[peerId] = nil
		}
	}
}
