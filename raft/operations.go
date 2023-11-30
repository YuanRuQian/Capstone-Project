package raft

type ReadInfoOp struct {
	reply chan VolatileStateInfo
}

type RequestVoteOp struct {
	args RequestVoteArgs
}

type AppendEntriesOp struct {
	args             AppendEntriesArgs
	receiverId       int
	senderId         int
	currentNextIndex int
}

type RequestVoteReplyOp struct {
	reply RequestVoteReply
}

type CommitEntry struct {
	Command interface{}
	Index   int
	Term    int
}

// LogEntry represents a logEntries entry in Raft.
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

type StopOp struct {
	reply chan bool
}

type AppendEntriesReplyOp struct {
	args             AppendEntriesArgs
	reply            AppendEntriesReply
	receiverId       int
	currentNextIndex int
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
