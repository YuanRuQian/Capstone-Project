package raft

type ReadInfoOp struct {
	reply chan VolatileStateInfo
}

type RequestVoteOp struct {
	args RequestVoteArgs
}

type AppendEntriesOp struct {
	args AppendEntriesArgs
}

type StopOp struct {
	reply chan bool
}

type AppendEntriesReplyOp struct {
	args  AppendEntriesArgs
	reply AppendEntriesReply
}

type RequestVoteReplyOp struct {
	reply RequestVoteReply
}
