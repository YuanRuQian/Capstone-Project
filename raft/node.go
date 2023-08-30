package raft

import (
	"log"
	"math/rand"
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

// TODO: adjust the heartbeat interval

// TODO: Fine-Grained Locking
// TODO: Check synchronization when launching goroutines to perform tasks concurrently

const (
	HeartbeatInterval = 30
)

const (
	IsDebugMode = true
)

type LogEntry struct {
	Command interface{}
	Term    int
}

// TODO: handle persistent state

type Node struct {
	mu       sync.Mutex
	id       int
	peersIds []int

	// a rpc server representing this node, which could be used to issue rpc calls
	server *Server

	// TODO: handle persistent state
	currentTerm int
	voteForId   int
	logs        []LogEntry

	// volatile states
	state                      State
	lastElectionTimerResetTime time.Time
}

func (n *Node) startElectionTimer() {
	electionTimeout := getElectionTimeout()
	n.mu.Lock()
	termStarted := n.currentTerm
	n.mu.Unlock()

	statusCheckTicker := time.NewTicker(10 * time.Millisecond)
	defer statusCheckTicker.Stop()
	for {
		<-statusCheckTicker.C
		n.mu.Lock()

		if !(n.state == Candidate || n.state == Follower) {
			n.mu.Unlock()
			DebuggerLog("Node %v: Election timer stopped because it is not in Candidate or Follower state", n.id)
			return
		}

		if n.currentTerm != termStarted {
			n.mu.Unlock()
			DebuggerLog("Node %v: Election timer stopped because term has changed", n.id)
			return
		}

		timePassedSinceLastReset := time.Since(n.lastElectionTimerResetTime)
		if timePassedSinceLastReset >= electionTimeout {
			DebuggerLog("Node %v: Election timer timed out", n.id)
			n.mu.Unlock()
			n.startElection()
			return
		}

		n.mu.Unlock()
	}
}

type RequestVoteArgs struct {
	// candidate’s term
	Term int
	// candidate requesting vote
	CandidateId int
	// index of candidate’s last log entry
	LastLogIndex int
	// term of candidate’s last log entry
	LastLogTerm int
}

type RequestVoteReply struct {
	// currentTerm, for candidate to update itself
	Term int
	// true means candidate received vote
	VoteGranted bool
}

func (n *Node) startElection() {
	n.mu.Lock()
	DebuggerLog("Node %v: Election started", n.id)

	n.state = Candidate
	n.currentTerm++

	startedTerm := n.currentTerm

	n.lastElectionTimerResetTime = time.Now()
	n.voteForId = n.id

	votesReceived := 1

	for _, peerId := range n.peersIds {
		go func(peerId int) {
			args := RequestVoteArgs{
				Term:        startedTerm,
				CandidateId: n.id,
			}

			reply := RequestVoteReply{}

			DebuggerLog("Node %v: Sending RequestVote to Node %v", n.id, peerId)

			if err := n.server.Call(peerId, "Node.RequestVote", args, &reply); err != nil {
				DebuggerLog("Node %v: Error while sending RequestVote to Node %v: %v", n.id, peerId, err)

				n.mu.Lock()
				defer n.mu.Unlock()

				if n.state != Candidate {
					DebuggerLog("Node %v: Node %v is not a candidate anymore", n.id, peerId)
					return
				}

				if reply.Term > n.currentTerm {
					DebuggerLog("Node %v: Node %v has a higher term, converting to follower", n.id, peerId)
					n.transitionToFollower(reply.Term)
					return
				} else if reply.Term == startedTerm {
					if reply.VoteGranted {
						votesReceived++
						if votesReceived > len(n.peersIds)/2 {
							DebuggerLog("Node %v: Received majority votes, converting to leader", n.id)
							n.transitionToLeader()
							return
						}
					}
				}

			}

		}(peerId)
	}

	// just in case nobody has won, start a new election
	n.startElectionTimer()
}

func (n *Node) transitionToFollower(term int) {
	n.mu.Lock()
	n.state = Follower
	n.currentTerm = term
	n.voteForId = -1
	n.lastElectionTimerResetTime = time.Now()
	n.mu.Unlock()

	go n.startElection()
}

func (n *Node) transitionToLeader() {
	n.mu.Lock()
	n.state = Leader
	n.mu.Unlock()

	go func() {
		heartbeatTicker := time.NewTicker(HeartbeatInterval * time.Millisecond)
		defer heartbeatTicker.Stop()

		for {
			n.sendHeartbeats()
			<-heartbeatTicker.C

			n.mu.Lock()
			if n.state != Leader {
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()
		}
	}()
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (n *Node) sendHeartbeats() {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}
	savedCurrentTerm := n.currentTerm
	n.mu.Unlock()

	for _, peerId := range n.peersIds {
		args := AppendEntriesArgs{
			Term:     savedCurrentTerm,
			LeaderId: n.id,
		}
		go func(peerId int) {
			DebuggerLog("sending AppendEntries to %v: ni=%d, args=%+v", peerId, 0, args)
			reply := AppendEntriesReply{}
			if err := n.server.Call(peerId, "Node.AppendEntries", args, &reply); err == nil {
				n.mu.Lock()
				defer n.mu.Unlock()
				if reply.Term > savedCurrentTerm {
					DebuggerLog("term out of date in heartbeat reply")
					n.transitionToFollower(reply.Term)
					return
				}
			}
		}(peerId)
	}
}

func (n *Node) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state == Dead {
		DebuggerLog("Node %v: RequestVote received after death", n.id)
		return nil
	}

	DebuggerLog("Node %v: RequestVote received: %v", n.id, args)

	if args.Term > n.currentTerm {
		n.transitionToFollower(args.Term)
	}

	ifCouldVote := n.voteForId == -1 || n.voteForId == args.CandidateId
	if args.Term == n.currentTerm && ifCouldVote {
		n.voteForId = args.CandidateId
		reply.VoteGranted = true
		n.lastElectionTimerResetTime = time.Now()
	} else {
		reply.VoteGranted = false
	}

	reply.Term = n.currentTerm
	DebuggerLog("RequestVote reply: %+v", *reply)
	return nil
}

func (n *Node) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state == Dead {
		DebuggerLog("Node %v: AppendEntries received after death", n.id)
		return nil
	}

	DebuggerLog("Node %v: AppendEntries received: %v", n.id, args)

	if args.Term > n.currentTerm {
		// there is a new leader / incoming entries have new content
		n.transitionToFollower(args.Term)
	}

	reply.Success = false

	if args.Term == n.currentTerm {
		if n.state != Follower {
			n.transitionToFollower(args.Term)
		}
		n.lastElectionTimerResetTime = time.Now()
		reply.Success = true
	}

	reply.Term = n.currentTerm
	DebuggerLog("AppendEntries reply: %+v", *reply)
	return nil

}

func (n *Node) Kill() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.state = Dead
	DebuggerLog("Node %v: Killed", n.id)
}

func DebuggerLog(format string, a ...interface{}) {
	if IsDebugMode {
		log.Printf(format, a...)
	}
}

// To prevent split votes in the first place, election timeouts are chosen randomly from a fixed interval (e.g., 150–300ms).
func getElectionTimeout() time.Duration {
	return time.Duration(150+rand.Int()%151) * time.Millisecond
}

func MakeNewNode(id int, peersIds []int, server *Server) *Node {
	node := &Node{
		id:          id,
		peersIds:    peersIds,
		server:      server,
		currentTerm: 0,
		voteForId:   -1,
		logs:        make([]LogEntry, 0),
		state:       Follower, // Initial state is Follower
	}

	go func() {
		node.mu.Lock()
		node.lastElectionTimerResetTime = time.Now()
		node.mu.Unlock()
		node.startElection()
	}()

	return node
}
