package raft

import "time"

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
	nodeInfoWriteCh            chan VolatileStateInfo
	nodeInfoWriteFinishedCh    chan interface{}
	nodeInfoReadCh             chan interface{}
	nodeInfoReadReplyCh        chan VolatileStateInfo
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
		server:                     server,
		id:                         id,
		currentTerm:                0,
		votedFor:                   -1,
		log:                        make([]LogEntry, 0),
		commitIndex:                0,
		lastApplied:                0,
		state:                      Follower,
		leaderID:                   -1,
		peers:                      peers,
		appendEntries:              make(chan AppendEntriesArgs),
		appendEntriesReply:         make(chan AppendEntriesReply),
		requestVote:                make(chan RequestVoteArgs),
		requestVoteReply:           make(chan RequestVoteReply),
		isReadyToRun:               isReadyToRun,
		stopRunning:                make(chan interface{}),
		lastElectionTimerResetTime: time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),
		nodeInfoWriteCh:            make(chan VolatileStateInfo),
		nodeInfoWriteFinishedCh:    make(chan interface{}),
		nodeInfoReadCh:             make(chan interface{}),
		nodeInfoReadReplyCh:        make(chan VolatileStateInfo),
	}

	go node.run()

	return node
}

func (m AppendEntriesArgs) isHeartbeat() bool {
	return 0 == len(m.Entries)
}

func (node *Node) run() {
	<-node.isReadyToRun

	node.writeCurrentVolatileStateInfo(VolatileStateInfo{
		State:             node.state,
		CurrentTerm:       node.currentTerm,
		LastElectionReset: time.Now(),
		VotedFor:          node.votedFor,
	})

	node.runElectionTimer()

	for {
		select {

		case update := <-node.nodeInfoWriteCh:
			DebuggerLog("Node %v: run nodeInfoWriteCh update: %+v", node.id, update)
			node.handleInfoUpdate(update)
			node.nodeInfoWriteFinishedCh <- nil
			DebuggerLog("Node %v: nodeInfoWriteCh nodeInfoWriteFinishedCh: %+v", node.id, update)

		case <-node.nodeInfoReadCh:
			DebuggerLog("Node %v: run nodeInfoReadCh", node.id)
			nodeInfo := node.getVolatileStateInfo()
			DebuggerLog("Node %v: nodeInfoReadCh nodeInfo: %+v", node.id, nodeInfo)
			node.nodeInfoReadReplyCh <- nodeInfo
			DebuggerLog("Node %v: nodeInfoReadCh nodeInfoReadReplyCh: %+v", node.id, node.nodeInfoReadReplyCh)

		case <-node.stopRunning:
			DebuggerLog("Node %v: run stopRunning", node.id)
			node.handleStopRunning()

		case msg := <-node.appendEntries:
			DebuggerLog("Node %v: run appendEntries: %+v", node.id, msg)
			node.handleAppendEntries(msg)

		case msg := <-node.requestVote:
			DebuggerLog("Node %v: run requestVote: %+v", node.id, msg)
			node.handleRequestVote(msg)
		}
	}
}

func (node *Node) handleAppendEntries(args AppendEntriesArgs) {
	nodeInfo := node.readCurrentVolatileStateInfo()

	if nodeInfo.State == Dead {
		return
	}
	DebuggerLog("Node %v: Receive AppendEntries from %v : %+v", node.id, args.LeaderID, args)

	if args.Term > nodeInfo.CurrentTerm {
		DebuggerLog("Node %v: term out of date in AppendEntries", node.id)
		node.transitionToFollower(args.Term)
	}

	var reply AppendEntriesReply

	reply.Success = false
	if args.Term == nodeInfo.CurrentTerm {
		if nodeInfo.State != Follower {
			node.transitionToFollower(args.Term)
		}
		node.writeCurrentVolatileStateInfo(VolatileStateInfo{
			State:             nodeInfo.State,
			CurrentTerm:       nodeInfo.CurrentTerm,
			LastElectionReset: time.Now(),
			VotedFor:          nodeInfo.VotedFor,
		})
		reply.Success = true
	}

	reply.Term = nodeInfo.CurrentTerm
	DebuggerLog("Node %v: Send AppendEntries reply to %v: %+v", node.id, args.LeaderID, reply)
	node.appendEntriesReply <- reply
}

func (node *Node) handleRequestVote(args RequestVoteArgs) {
	nodeInfo := node.readCurrentVolatileStateInfo()

	if nodeInfo.State == Dead {
		return
	}
	DebuggerLog("Node %v: Receive RequestVote from %v", node.id, args.CandidateID)

	if args.Term > nodeInfo.CurrentTerm {
		DebuggerLog("Node %v: term out of date in RequestVote", node.id)
		node.transitionToFollower(args.Term)
	}

	var reply RequestVoteReply

	if nodeInfo.CurrentTerm == args.Term &&
		(nodeInfo.VotedFor == -1 || nodeInfo.VotedFor == args.CandidateID) {
		reply.VoteGranted = true

		node.writeCurrentVolatileStateInfo(
			VolatileStateInfo{
				State:             nodeInfo.State,
				CurrentTerm:       nodeInfo.CurrentTerm,
				LastElectionReset: time.Now(),
				VotedFor:          args.CandidateID,
			})
	} else {
		reply.VoteGranted = false
	}
	reply.Term = node.currentTerm
	DebuggerLog("Node %v: Send RequestVote reply to %v", node.id, args.CandidateID)
	node.requestVoteReply <- reply
}

func (node *Node) runElectionTimer() {
	electionTimeout := getElectionTimeout(150, 300)
	DebuggerLog("Node %v: Election timer started with timeout %v, try to read volatile state info", node.id, electionTimeout)
	nodeInfo := node.readCurrentVolatileStateInfo()
	termStarted := nodeInfo.CurrentTerm
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		<-ticker.C

		if nodeInfo.State != Candidate && nodeInfo.State != Follower {
			DebuggerLog("Node %v: Election timer stopped because node is not candidate or follower", node.id)
			return
		}

		if nodeInfo.CurrentTerm != termStarted {
			DebuggerLog("Node %v: Election timer stopped because term changed", node.id)
			return
		}

		if elapse := time.Since(nodeInfo.LastElectionReset); elapse >= electionTimeout {
			DebuggerLog("Node %v: Election timer timed out without winner, start a new election", node.id)
			node.startElection()
			return
		}

	}
}

func (node *Node) startElection() {

	node.writeCurrentVolatileStateInfo(VolatileStateInfo{
		State:             Candidate,
		CurrentTerm:       node.currentTerm + 1,
		LastElectionReset: time.Now(),
		VotedFor:          node.id,
	})

	DebuggerLog("Node %v: Start election for term %v, try to read current volatile info", node.id, node.currentTerm+1)
	nodeInfo := node.readCurrentVolatileStateInfo()

	savedCurrentTerm := nodeInfo.CurrentTerm

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
			if err := node.server.Call(peerId, "Node.HandleRequestVoteRPC", args, &reply); err == nil {

				DebuggerLog("Node %v: Receive RequestVoteReply from %v : %+v", node.id, peerId, reply)

				if nodeInfo.State != Candidate {
					DebuggerLog("Node %v: state changed to %v, stop sending RequestVote", node.id, nodeInfo.State)
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
	node.writeCurrentVolatileStateInfo(
		VolatileStateInfo{
			State:             Dead,
			CurrentTerm:       node.currentTerm,
			LastElectionReset: node.lastElectionTimerResetTime,
			VotedFor:          node.votedFor,
		})
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
	DebuggerLog("Node %v: send heartbeats, try to read current volatile info", node.id)
	nodeInfo := node.readCurrentVolatileStateInfo()

	if nodeInfo.State != Leader {
		return
	}

	savedCurrentTerm := nodeInfo.CurrentTerm

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
	DebuggerLog("Node %v: report, try to read current volatile info", node.id)
	nodeInfo := node.readCurrentVolatileStateInfo()
	return node.id, nodeInfo.CurrentTerm, nodeInfo.State == Leader
}

func (node *Node) handleInfoUpdate(update VolatileStateInfo) {
	node.state = update.State
	node.currentTerm = update.CurrentTerm
	node.lastElectionTimerResetTime = update.LastElectionReset
	node.votedFor = update.VotedFor
}

func (node *Node) getVolatileStateInfo() VolatileStateInfo {
	return VolatileStateInfo{
		State:             node.state,
		CurrentTerm:       node.currentTerm,
		LastElectionReset: node.lastElectionTimerResetTime,
		VotedFor:          node.votedFor,
	}
}

func (node *Node) readCurrentVolatileStateInfo() VolatileStateInfo {
	DebuggerLog("Node %v: read current volatile state info before sending out signals", node.id)
	node.nodeInfoReadCh <- nil
	DebuggerLog("Node %v: read current volatile state info after node.nodeInfoReadCh <- nil", node.id)
	return <-node.nodeInfoReadReplyCh
}

func (node *Node) writeCurrentVolatileStateInfo(update VolatileStateInfo) {
	DebuggerLog("Node %v: write current volatile state info: %+v", node.id, update)
	node.nodeInfoWriteCh <- update
	<-node.nodeInfoWriteFinishedCh
	DebuggerLog("Node %v: write current volatile state info finished", node.id)
}
