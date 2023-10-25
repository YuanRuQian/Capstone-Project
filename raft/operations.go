package raft

type ReadInfoOp struct {
	reply chan VolatileStateInfo
}

type RequestVoteOp struct {
	args  RequestVoteArgs
	reply chan RequestVoteReply
}

type AppendEntriesOp struct {
	args  AppendEntriesArgs
	reply chan AppendEntriesReply
}

type StopOp struct {
	reply chan bool
}
