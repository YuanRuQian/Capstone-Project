package raft

type ReadInfoOp struct {
	reply chan VolatileStateInfo
}

type WriteInfoOp struct {
	info  VolatileStateInfo
	reply chan bool
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
