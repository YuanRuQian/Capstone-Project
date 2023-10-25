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
	electionStatusCheckerTicker       *time.Ticker
	electionTermStarted               int
	electionTimeout                   time.Duration
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
		electionStatusCheckerTicker:       time.NewTicker(10 * time.Millisecond),
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

		node.volatileStateInfo.LastElectionReset = time.Now()

		node.startElectionTimer()

		leaderSendHeartbeatTicker := time.NewTicker(20 * time.Millisecond)
		defer leaderSendHeartbeatTicker.Stop()

		isRunning := true

		for isRunning {
			select {

			case <-node.electionStatusCheckerTicker.C:
				node.handleElectionStatusCheck()

			case <-leaderSendHeartbeatTicker.C:
				node.handleLeaderSendHeartbeatTicker()

			case stopOp := <-node.stopOpCh:
				node.handleStopRunning(stopOp)
				leaderSendHeartbeatTicker.Stop()
				node.electionStatusCheckerTicker.Stop()
				isRunning = false
				DebuggerLog("Node %v: stop running", node.id)

			case appendEntriesOp := <-node.appendEntriesOpCh:
				node.handleAppendEntries(appendEntriesOp)

			case requestVoteOp := <-node.requestVoteOpCh:
				node.handleRequestVote(requestVoteOp)
			}
		}

		DebuggerLog("Node %v: end single thread listener", node.id)
	}()

}

func (node *Node) handleAppendEntries(op *AppendEntriesOp) {
	if node.volatileStateInfo.State == Dead {
		DebuggerLog("Node %v: state is dead, return", node.id)
		return
	}

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
	op.reply <- reply
}

func (node *Node) handleRequestVote(op *RequestVoteOp) {
	if node.volatileStateInfo.State == Dead {
		DebuggerLog("Node %v: state is dead, return", node.id)
		return
	}

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
	op.reply <- reply
}

func (node *Node) startElection() {
	node.volatileStateInfo.State = Candidate
	node.volatileStateInfo.CurrentTerm += 1
	node.volatileStateInfo.LastElectionReset = time.Now()
	node.volatileStateInfo.VotedFor = node.id

	DebuggerLog("Node %v: Start election for term %v", node.id, node.volatileStateInfo.CurrentTerm)

	votesReceived := 1

	for _, peerId := range node.peers {
		go func(peerId int) {
			args := RequestVoteArgs{
				Term:        node.volatileStateInfo.CurrentTerm,
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

				if reply.Term > node.volatileStateInfo.CurrentTerm {
					DebuggerLog("Node %v: term out of date in RequestVoteReply", node.id)
					node.transitionToFollower(reply.Term)
					return
				} else if reply.Term == node.volatileStateInfo.CurrentTerm {
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

	node.startElectionTimer()
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
	op.reply <- true
}

func (node *Node) transitionToFollower(term int) {
	DebuggerLog("Node %v: term out of date in RequestVoteReply", node.id)
	node.volatileStateInfo.State = Follower
	node.volatileStateInfo.CurrentTerm = term
	node.volatileStateInfo.VotedFor = -1
	node.volatileStateInfo.LastElectionReset = time.Now()

	node.startElectionTimer()
}

func (node *Node) transitionToLeader() {
	node.volatileStateInfo.State = Leader
	DebuggerLog("Node %v: transition to leader", node.id)
}

func (node *Node) Report() (id int, term int, isLeader bool) {
	// Wait for all nodes in the cluster to be ready for incoming signals
	node.allNodesAreReadyForIncomingSignal.Wait()
	return node.id, node.volatileStateInfo.CurrentTerm, node.volatileStateInfo.State == Leader
}

func (node *Node) handleLeaderSendHeartbeatTicker() {
	if node.volatileStateInfo.State != Leader {
		return
	}

	args := AppendEntriesArgs{
		Term:     node.volatileStateInfo.CurrentTerm,
		LeaderID: node.id,
	}

	for _, peerId := range node.peers {
		var reply AppendEntriesReply
		if err := node.server.Call(peerId, "Node.AppendEntries", args, &reply); err == nil {
			if reply.Term > node.volatileStateInfo.CurrentTerm {
				node.transitionToFollower(reply.Term)
				return
			}
		}
	}
}

func (node *Node) handleElectionStatusCheck() {
	if node.volatileStateInfo.State != Candidate && node.volatileStateInfo.State != Follower {
		DebuggerLog("Node %v: Election timer stopped because it is not in Candidate or Follower state", node.id)
		return
	}

	if node.volatileStateInfo.CurrentTerm != node.electionTermStarted {
		DebuggerLog("Node %v: Election timer stopped because term has changed", node.id)
		return
	}

	if timePassedSinceLastReset := time.Since(node.volatileStateInfo.LastElectionReset); timePassedSinceLastReset >= node.electionTimeout {
		node.startElection()
		return
	}
}

func (node *Node) startElectionTimer() {
	node.electionTimeout = getElectionTimeout(150, 300)
	node.electionTermStarted = node.volatileStateInfo.CurrentTerm
}
