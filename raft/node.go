package raft

import (
	"sync"
	"time"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
	Dead
)

type VolatileStateInfo struct {
	State             State
	CurrentTerm       int
	LastElectionReset time.Time
	VotedFor          int
}

// Node represents a single Raft node.
type Node struct {
	server                            *Server
	id                                int
	volatileStateInfo                 VolatileStateInfo
	log                               []LogEntry
	commitIndex                       int
	lastApplied                       int
	leaderID                          int
	peers                             []int
	appendEntriesOpCh                 chan *AppendEntriesOp
	requestVoteOpCh                   chan *RequestVoteOp
	isReadyToRun                      <-chan interface{}
	stopOpCh                          chan *StopOp
	allNodesAreReadyForIncomingSignal *sync.WaitGroup
}

// LogEntry represents a log entry in Raft.
type LogEntry struct {
	Term    int
	Command interface{}
}

// AppendEntriesArgs represents an AppendEntries RPC.
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// RequestVoteArgs represents a RequestVote RPC.
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func MakeNewNode(id int, peers []int, server *Server, isReadyToRun <-chan interface{}, allNodesAreReadyForIncomingSignal *sync.WaitGroup) *Node {
	defaultVolatileStateInfo := VolatileStateInfo{
		State:             Follower,
		CurrentTerm:       0,
		LastElectionReset: time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),
		VotedFor:          -1,
	}
	node := &Node{
		server:                            server,
		id:                                id,
		volatileStateInfo:                 defaultVolatileStateInfo,
		log:                               make([]LogEntry, 0),
		commitIndex:                       0,
		lastApplied:                       0,
		leaderID:                          -1,
		peers:                             peers,
		appendEntriesOpCh:                 make(chan *AppendEntriesOp),
		requestVoteOpCh:                   make(chan *RequestVoteOp),
		isReadyToRun:                      isReadyToRun,
		stopOpCh:                          make(chan *StopOp),
		allNodesAreReadyForIncomingSignal: allNodesAreReadyForIncomingSignal,
	}

	allNodesAreReadyForIncomingSignal.Add(1)

	go node.run()

	return node
}

func (m AppendEntriesArgs) isHeartbeat() bool {
	return 0 == len(m.Entries)
}

func (node *Node) run() {
	<-node.isReadyToRun

	go func() {
		DebuggerLog("Node %v: start single thread listener", node.id)

		node.allNodesAreReadyForIncomingSignal.Done()

		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()

		for {
			DebuggerLog("Node %v: before for loop select", node.id)
			select {

			case t := <-ticker.C:
				DebuggerLog("Node %v: ticker.C: %v", node.id, t)
				node.handleElectionTicker()

			case stopOp := <-node.stopOpCh:
				node.handleStopRunning(stopOp)
				ticker.Stop()

			case appendEntriesOp := <-node.appendEntriesOpCh:
				node.handleAppendEntries(appendEntriesOp)

			case requestVoteOp := <-node.requestVoteOpCh:
				node.handleRequestVote(requestVoteOp)
			}
			DebuggerLog("Node %v: after for loop select", node.id)
		}
	}()

	node.volatileStateInfo.LastElectionReset = time.Now()
}

func (node *Node) handleAppendEntries(op *AppendEntriesOp) {
	DebuggerLog("Node %v: run appendEntries: %+v", node.id, op.args)

	DebuggerLog("Node %v: run appendEntries before getVolatileStateInfo: %+v", node.id, op.args)

	if node.volatileStateInfo.State == Dead {
		DebuggerLog("Node %v: state is dead, return", node.id)
		return
	}
	DebuggerLog("Node %v: Receive AppendEntries from %v : %+v", node.id, op.args.LeaderID, op.args)

	if op.args.Term > node.volatileStateInfo.CurrentTerm {
		DebuggerLog("Node %v: term out of date in AppendEntries", node.id)
		node.transitionToFollower(op.args.Term)
	}

	var reply AppendEntriesReply

	reply.Success = false
	if op.args.Term == node.volatileStateInfo.CurrentTerm {
		if node.volatileStateInfo.State != Follower {
			node.transitionToFollower(op.args.Term)
		}

		node.volatileStateInfo.LastElectionReset = time.Now()
		reply.Success = true
	}

	reply.Term = node.volatileStateInfo.CurrentTerm
	DebuggerLog("Node %v: Send AppendEntries reply to %v: %+v", node.id, op.args.LeaderID, reply)
	op.reply <- reply
	DebuggerLog("Node %v: run appendEntries done", node.id)
}

func (node *Node) handleRequestVote(op *RequestVoteOp) {
	DebuggerLog("Node %v: run requestVote before getVolatileStateInfo: %+v", node.id, op.args)

	DebuggerLog("Node %v: run requestVote after getVolatileStateInfo: %+v", node.id, op.args)

	if node.volatileStateInfo.State == Dead {
		DebuggerLog("Node %v: state is dead, return", node.id)
		return
	}
	DebuggerLog("Node %v: Receive RequestVote from %v", node.id, op.args.CandidateID)

	if op.args.Term > node.volatileStateInfo.CurrentTerm {
		DebuggerLog("Node %v: term out of date in RequestVote", node.id)
		node.transitionToFollower(op.args.Term)
	}

	var reply RequestVoteReply

	if node.volatileStateInfo.CurrentTerm == op.args.Term &&
		(node.volatileStateInfo.VotedFor == -1 || node.volatileStateInfo.VotedFor == op.args.CandidateID) {
		reply.VoteGranted = true

		node.volatileStateInfo.VotedFor = op.args.CandidateID
		node.volatileStateInfo.LastElectionReset = time.Now()

	} else {
		reply.VoteGranted = false
	}
	reply.Term = node.volatileStateInfo.CurrentTerm
	DebuggerLog("Node %v: Send RequestVote reply to %v : %+v", node.id, op.args.CandidateID, reply)
	op.reply <- reply
}

func (node *Node) startElection() {

	DebuggerLog("Node %v: just in startElection", node.id)

	node.volatileStateInfo.State = Candidate
	node.volatileStateInfo.CurrentTerm += 1
	node.volatileStateInfo.LastElectionReset = time.Now()
	node.volatileStateInfo.VotedFor = node.id

	DebuggerLog("Node %v: startElection before getVolatileStateInfo", node.id)

	savedCurrentTerm := node.volatileStateInfo.CurrentTerm

	DebuggerLog("Node %v: Start election for term %v", node.id, savedCurrentTerm)

	votesReceived := 1

	for _, peerId := range node.peers {
		go func(peerId int) {
			args := RequestVoteArgs{
				Term:        savedCurrentTerm,
				CandidateID: node.id,
			}
			var reply RequestVoteReply
			DebuggerLog("Node %v: Send RequestVote to %v", node.id, peerId)

			// service method: rpc proxy delegate method, not the node's method
			if err := node.server.Call(peerId, "Node.RequestVote", args, &reply); err == nil {

				DebuggerLog("Node %v: Receive RequestVoteReply from %v : %+v", node.id, peerId, reply)

				if node.volatileStateInfo.State != Candidate {
					DebuggerLog("Node %v: state changed to %v, stop sending RequestVote", node.id, node.volatileStateInfo.State)
					return
				}

				if reply.Term > savedCurrentTerm {
					DebuggerLog("Node %v: term out of date in RequestVoteReply", node.id)
					node.transitionToFollower(reply.Term)
					return
				} else if reply.Term == savedCurrentTerm {
					if reply.VoteGranted {
						votesReceived += 1
						if votesReceived*2 > len(node.peers)+1 {
							DebuggerLog("Node %v: wins election with %d votes", node.id, votesReceived)
							node.transitionToLeader()
							return
						}
					}
				}
			}
		}(peerId)
	}
}

func (node *Node) HandleStopRPC() {
	stopOp := &StopOp{
		reply: make(chan bool),
	}
	node.stopOpCh <- stopOp
	<-stopOp.reply
}

func (node *Node) HandleRequestVoteRPC(args RequestVoteArgs, reply *RequestVoteReply) error {
	DebuggerLog("Node %v: HandleRequestVoteRPC Receive RequestVote from %v: %+v ", node.id, args.CandidateID, args)
	requestVoteOp := &RequestVoteOp{
		args:  args,
		reply: make(chan RequestVoteReply),
	}
	node.requestVoteOpCh <- requestVoteOp
	ret := <-requestVoteOp.reply
	DebuggerLog("Node %v: HandleRequestVoteRPC right after ret := <-node.requestVoteReply | current reply: %+v", node.id, reply)
	reply.Term = ret.Term
	reply.VoteGranted = ret.VoteGranted
	return nil
}

func (node *Node) HandleAppendEntriesRPC(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	DebuggerLog("Node %v: HandleAppendEntriesRPC Receive AppendEntries from %v: %+v", node.id, args.LeaderID, args)
	appendEntriesOp := &AppendEntriesOp{
		args:  args,
		reply: make(chan AppendEntriesReply),
	}
	node.appendEntriesOpCh <- appendEntriesOp
	ret := <-appendEntriesOp.reply
	DebuggerLog("Node %v: HandleAppendEntriesRPC right after ret := <-node.appendEntriesReply | current reply: %+v", node.id, reply)
	reply.Term = ret.Term
	reply.Success = ret.Success
	return nil
}

func (node *Node) handleStopRunning(op *StopOp) {
	DebuggerLog("Node %v: run stopOpCh", node.id)

	node.volatileStateInfo.State = Dead

	DebuggerLog("Node %v: run stopOpCh before reply", node.id)
	DebuggerLog("Node %v: run stopOpCh done", node.id)
	op.reply <- true
}

func (node *Node) transitionToFollower(term int) {
	DebuggerLog("Node %v: term out of date in RequestVoteReply", node.id)
	node.volatileStateInfo.State = Follower
	node.volatileStateInfo.CurrentTerm = term
	node.volatileStateInfo.VotedFor = -1
	node.volatileStateInfo.LastElectionReset = time.Now()
}

func (node *Node) transitionToLeader() {
	node.volatileStateInfo.State = Leader
	DebuggerLog("Node %v: transition to leader", node.id)

	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		// Send periodic heartbeats, as long as still leader.
		for {
			node.sendHeartbeats()
			<-ticker.C

			if node.volatileStateInfo.State != Leader {
				return
			}
		}
	}()
}

func (node *Node) sendHeartbeats() {
	DebuggerLog("Node %v: send heartbeats, try to read current volatile info", node.id)
	DebuggerLog("Node %v: send heartbeats before getVolatileStateInfo", node.id)

	if node.volatileStateInfo.State != Leader {
		return
	}

	savedCurrentTerm := node.volatileStateInfo.CurrentTerm

	for _, peerId := range node.peers {
		args := AppendEntriesArgs{
			Term:     savedCurrentTerm,
			LeaderID: node.id,
		}
		go func(peerId int) {
			DebuggerLog("Node %v: Send AppendEntries to %v", node.id, peerId)
			var reply AppendEntriesReply

			// service method: rpc proxy delegate method, not the node's method
			if err := node.server.Call(peerId, "Node.AppendEntries", args, &reply); err == nil {
				if reply.Term > savedCurrentTerm {
					DebuggerLog("Node %v: term out of date in heartbeat reply", node.id)
					node.transitionToFollower(reply.Term)
					return
				}
			}
		}(peerId)
	}
}

func (node *Node) Report() (id int, term int, isLeader bool) {
	// Wait for all nodes in the cluster to be ready for incoming signals
	node.allNodesAreReadyForIncomingSignal.Wait()

	DebuggerLog("Node %v: report, before getVolatileStateInfo", node.id)
	DebuggerLog("Node %v: report info: %+v", node.id, node.volatileStateInfo)
	return node.id, node.volatileStateInfo.CurrentTerm, node.volatileStateInfo.State == Leader
}

func (node *Node) handleElectionTicker() {
	if node.volatileStateInfo.State != Candidate && node.volatileStateInfo.State != Follower {
		DebuggerLog("Node %v: Election timer bailed out because node is not candidate or follower", node.id)
		return
	}
}
