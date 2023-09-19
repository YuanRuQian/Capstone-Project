package raft

import "time"

type State int

const (
	Follower State = iota
	Candidate
	Leader
	Dead
)

// Node represents a single Raft node.
type Node struct {
	server                     *Server
	id                         int
	currentTerm                int
	votedFor                   int
	log                        []LogEntry
	commitIndex                int
	lastApplied                int
	state                      State
	leaderID                   int
	peers                      []int
	appendEntries              chan AppendEntriesArgs
	appendEntriesReply         chan AppendEntriesReply
	requestVote                chan RequestVoteArgs
	requestVoteReply           chan RequestVoteReply
	isReadyToRun               <-chan interface{}
	stopRunning                chan interface{}
	lastElectionTimerResetTime time.Time
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

func MakeNewNode(id int, peers []int, server *Server, isReadyToRun <-chan interface{}) *Node {
	node := &Node{
		server:             server,
		id:                 id,
		currentTerm:        0,
		votedFor:           -1,
		log:                []LogEntry{},
		commitIndex:        0,
		lastApplied:        0,
		state:              Follower,
		leaderID:           -1,
		peers:              peers,
		appendEntries:      make(chan AppendEntriesArgs),
		appendEntriesReply: make(chan AppendEntriesReply),
		requestVote:        make(chan RequestVoteArgs),
		requestVoteReply:   make(chan RequestVoteReply),
		isReadyToRun:       isReadyToRun,
	}

	go node.run()

	return node
}

func (m AppendEntriesArgs) isHeartbeat() bool {
	return 0 == len(m.Entries)
}

func (node *Node) run() {
	<-node.isReadyToRun

	// Start the election timer in the background
	go node.runElectionTimer()

	for {
		select {

		case <-node.stopRunning:
			node.handleStopRunning()

		case msg := <-node.appendEntries:
			node.handleAppendEntries(msg)

		case msg := <-node.requestVote:
			node.handleRequestVote(msg)
		}
	}
}

func (node *Node) handleAppendEntries(args AppendEntriesArgs) {
	if node.state == Dead {
		return
	}
	DebuggerLog("Node %v: Receive AppendEntries from %v : %+v", node.id, args.LeaderID, args)

	if args.Term > node.currentTerm {
		DebuggerLog("Node %v: term out of date in AppendEntries", node.id)
		node.transitionToFollower(args.Term)
	}

	var reply AppendEntriesReply

	reply.Success = false
	if args.Term == node.currentTerm {
		if node.state != Follower {
			node.transitionToFollower(args.Term)
		}
		node.lastElectionTimerResetTime = time.Now()
		reply.Success = true
	}

	reply.Term = node.currentTerm
	DebuggerLog("Node %v: Send AppendEntries reply to %v: %+v", node.id, args.LeaderID, reply)
	node.appendEntriesReply <- reply
}

func (node *Node) handleRequestVote(args RequestVoteArgs) {
	if node.state == Dead {
		return
	}
	DebuggerLog("Node %v: Receive RequestVote from %v", node.id, args.CandidateID)

	if args.Term > node.currentTerm {
		DebuggerLog("Node %v: term out of date in RequestVote", node.id)
		node.transitionToFollower(args.Term)
	}

	var reply RequestVoteReply

	if node.currentTerm == args.Term &&
		(node.votedFor == -1 || node.votedFor == args.CandidateID) {
		reply.VoteGranted = true
		node.votedFor = args.CandidateID
		node.lastElectionTimerResetTime = time.Now()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = node.currentTerm
	DebuggerLog("Node %v: Send RequestVote reply to %v", node.id, args.CandidateID)
	node.requestVoteReply <- reply
}

func (node *Node) runElectionTimer() {
	electionTimeout := getElectionTimeout(150, 300)
	termStarted := node.currentTerm
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		<-ticker.C

		if node.state != Candidate && node.state != Follower {
			DebuggerLog("Node %v: Election timer stopped because node is not candidate or follower", node.id)
			return
		}

		if node.currentTerm != termStarted {
			DebuggerLog("Node %v: Election timer stopped because term changed", node.id)
			return
		}

		if elapse := time.Since(node.lastElectionTimerResetTime); elapse >= electionTimeout {
			DebuggerLog("Node %v: Election timer timed out without winner, start a new election", node.id)
			node.startElection()
			return
		}

	}
}

func (node *Node) startElection() {
	node.state = Candidate
	node.currentTerm++
	savedCurrentTerm := node.currentTerm
	node.lastElectionTimerResetTime = time.Now()
	node.votedFor = node.id
	DebuggerLog("Node %v: Start election for term %v", node.id, node.currentTerm)

	votesReceived := 1

	for _, peerId := range node.peers {
		go func(peerId int) {
			args := RequestVoteArgs{
				Term:        savedCurrentTerm,
				CandidateID: node.id,
			}
			var reply RequestVoteReply
			DebuggerLog("Node %v: Send RequestVote to %v", node.id, peerId)
			if err := node.server.Call(peerId, "Node.HandleRequestVoteRPC", args, &reply); err == nil {

				DebuggerLog("Node %v: Receive RequestVoteReply from %v : %+v", node.id, peerId, reply)

				if node.state != Candidate {
					DebuggerLog("Node %v: state changed to %v, stop sending RequestVote", node.id, node.state)
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
	node.stopRunning <- nil
}

func (node *Node) HandleRequestVoteRPC(args RequestVoteArgs, reply *RequestVoteReply) error {
	node.requestVote <- args
	ret := <-node.requestVoteReply
	reply.Term = ret.Term
	reply.VoteGranted = ret.VoteGranted
	return nil
}

func (node *Node) HandleAppendEntriesRPC(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	node.appendEntries <- args
	ret := <-node.appendEntriesReply
	reply.Term = ret.Term
	reply.Success = ret.Success
	return nil
}

func (node *Node) handleStopRunning() {
	node.state = Dead
}

func (node *Node) transitionToFollower(term int) {
	DebuggerLog("Node %v: term out of date in RequestVoteReply", node.id)
	node.state = Follower
	node.currentTerm = term
	node.votedFor = -1
	node.lastElectionTimerResetTime = time.Now()

	go node.runElectionTimer()
}

func (node *Node) transitionToLeader() {
	node.state = Leader
	DebuggerLog("Node %v: transition to leader", node.id)

	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		// Send periodic heartbeats, as long as still leader.
		for {
			node.sendHeartbeats()
			<-ticker.C

			if node.state != Leader {
				return
			}
		}
	}()
}

func (node *Node) sendHeartbeats() {
	if node.state != Leader {
		return
	}
	savedCurrentTerm := node.currentTerm

	for _, peerId := range node.peers {
		args := AppendEntriesArgs{
			Term:     savedCurrentTerm,
			LeaderID: node.id,
		}
		go func(peerId int) {
			DebuggerLog("Node %v: Send AppendEntries to %v", node.id, peerId)
			var reply AppendEntriesReply
			if err := node.server.Call(peerId, "Node.HandleAppendEntriesRPC", args, &reply); err == nil {
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
	return node.id, node.currentTerm, node.state == Leader
}
