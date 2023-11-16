package raft

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
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
	State                State
	CurrentTerm          int
	LastElectionReset    time.Time
	VotedFor             int
	votesGrantedReceived int
	votesTotalReceived   int
}

// Node represents a single Raft node.
type Node struct {
	networkInterface                  *NetworkInterface
	id                                int
	volatileStateInfo                 VolatileStateInfo
	log                               []LogEntry
	commitIndex                       int
	lastApplied                       int
	leaderID                          int
	peers                             []int
	appendEntriesOpCh                 chan *AppendEntriesOp
	requestVoteOpCh                   chan *RequestVoteOp
	appendEntriesReplyOpCh            chan *AppendEntriesReplyOp
	requestVoteReplyOpCh              chan *RequestVoteReplyOp
	isReadyToRun                      <-chan interface{}
	stopOpCh                          chan *StopOp
	allNodesAreReadyForIncomingSignal *sync.WaitGroup
	electionStatusCheckerTicker       *time.Ticker
	electionTermStarted               int
	electionTimeout                   time.Duration
	reportToClusterOpCh               chan interface{}
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

func MakeNewNode(id int, peers []int, server *NetworkInterface, isReadyToRun <-chan interface{}, allNodesAreReadyForIncomingSignal *sync.WaitGroup) *Node {
	defaultVolatileStateInfo := VolatileStateInfo{
		State:             Follower,
		CurrentTerm:       0,
		LastElectionReset: time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),
		VotedFor:          -1,
	}
	node := &Node{
		networkInterface:                  server,
		id:                                id,
		volatileStateInfo:                 defaultVolatileStateInfo,
		log:                               make([]LogEntry, 0),
		commitIndex:                       0,
		lastApplied:                       0,
		leaderID:                          -1,
		peers:                             peers,
		appendEntriesOpCh:                 make(chan *AppendEntriesOp),
		requestVoteOpCh:                   make(chan *RequestVoteOp),
		appendEntriesReplyOpCh:            make(chan *AppendEntriesReplyOp),
		requestVoteReplyOpCh:              make(chan *RequestVoteReplyOp),
		isReadyToRun:                      isReadyToRun,
		stopOpCh:                          make(chan *StopOp),
		allNodesAreReadyForIncomingSignal: allNodesAreReadyForIncomingSignal,
		electionStatusCheckerTicker:       time.NewTicker(10 * time.Millisecond),
		reportToClusterOpCh:               make(chan interface{}),
	}

	allNodesAreReadyForIncomingSignal.Add(1)

	go node.run()

	return node
}

func (m AppendEntriesArgs) isHeartbeat() bool {
	return 0 == len(m.Entries)
}

// TODO: recievier end not processing fast enough

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
			startTime := time.Now()

			select {
			case <-node.reportToClusterOpCh:
				node.handleReportToCluster()
				node.printElapsedTime("handleReportToCluster", startTime)

			case <-node.electionStatusCheckerTicker.C:
				node.handleElectionStatusCheck()
				node.printElapsedTime("handleElectionStatusCheck", startTime)

			case <-leaderSendHeartbeatTicker.C:
				node.handleLeaderSendHeartbeatTicker()
				node.printElapsedTime("handleLeaderSendHeartbeatTicker", startTime)

			case stopOp := <-node.stopOpCh:
				node.handleStopRunning(stopOp)
				leaderSendHeartbeatTicker.Stop()
				node.electionStatusCheckerTicker.Stop()
				isRunning = false
				node.printElapsedTime("handleStopRunning", startTime)
				DebuggerLog("Node %v: stop running", node.id)

			case appendEntriesOp := <-node.appendEntriesOpCh:
				DebuggerLog("Node %v: handle append entries in main loop", node.id)
				node.handleAppendEntries(appendEntriesOp)
				node.printElapsedTime("handleAppendEntries", startTime)

			case requestVoteOp := <-node.requestVoteOpCh:
				DebuggerLog("Node %v: handle request vote in main loop", node.id)
				node.handleRequestVote(requestVoteOp)
				node.printElapsedTime("handleRequestVote", startTime)

			case appendEntriesReplyOp := <-node.appendEntriesReplyOpCh:
				DebuggerLog("Node %v: handle append entries reply in main loop", node.id)
				node.handleAppendEntriesReply(appendEntriesReplyOp)
				node.printElapsedTime("handleAppendEntriesReply", startTime)

			case requestVoteReplyOp := <-node.requestVoteReplyOpCh:
				DebuggerLog("Node %v: handle request vote reply in main loop", node.id)
				node.handleRequestVoteReply(requestVoteReplyOp)
				node.printElapsedTime("handleRequestVoteReply", startTime)
			}
		}

		DebuggerLog("Node %v: end single thread listener", node.id)
	}()

}

func (node *Node) printElapsedTime(operation string, startTime time.Time) {
	elapsed := time.Since(startTime)
	DebuggerLog("Node %v: %s took %s", node.id, operation, elapsed)

	// Append the result to the timestamp.csv file
	// node.writeToTimestampFile(operation, elapsed)
}

func (node *Node) writeToTimestampFile(operation string, elapsed time.Duration) {

	fileName := "timestamp.csv"
	file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		DebuggerLog("Error opening file %s: %v", fileName, err)
		return
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			panic(fmt.Sprintf("Error closing file %s: %v", fileName, err))
		}
	}(file)

	// Use a mutex to ensure safe concurrent writes to the file
	var mutex sync.Mutex
	mutex.Lock()
	defer mutex.Unlock()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Convert elapsed time to microseconds
	elapsedMicroseconds := elapsed.Microseconds()

	record := []string{
		strconv.Itoa(node.id),
		operation,
		strconv.FormatInt(elapsedMicroseconds, 10),
	}

	if err := writer.Write(record); err != nil {
		DebuggerLog("Error writing to file %s: %v", fileName, err)
	}
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

	reply := &AppendEntriesReply{}

	reply.Success = false
	if op.args.Term == node.volatileStateInfo.CurrentTerm {
		if node.volatileStateInfo.State != Follower {
			node.transitionToFollower(op.args.Term)
		}

		node.volatileStateInfo.LastElectionReset = time.Now()
		reply.Success = true
	}

	reply.Term = node.volatileStateInfo.CurrentTerm

	err := node.networkInterface.SendAppendEntriesReply(node.id, op.args.LeaderID, op.args, *reply)
	if err != nil {
		panic(fmt.Sprintf("Error in SendAppendEntriesReply RPC: %v", err))
	}
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
	err := node.networkInterface.SendRequestVoteReply(node.id, op.args.CandidateID, reply)
	if err != nil {
		panic(fmt.Sprintf("Error in SendRequestVoteReply RPC: %v", err))
	}
}

func (node *Node) startElection() {
	node.volatileStateInfo.State = Candidate
	node.volatileStateInfo.CurrentTerm += 1
	node.volatileStateInfo.LastElectionReset = time.Now()
	node.volatileStateInfo.VotedFor = node.id

	DebuggerLog("Node %v: Start election for term %v", node.id, node.volatileStateInfo.CurrentTerm)

	node.volatileStateInfo.votesGrantedReceived = 1
	node.volatileStateInfo.votesTotalReceived = 1

	args := RequestVoteArgs{
		Term:        node.volatileStateInfo.CurrentTerm,
		CandidateID: node.id,
	}

	for _, peerId := range node.peers {
		DebuggerLog("Node %v: Send RequestVote to %v", node.id, peerId)

		go node.networkInterface.RequestVote(peerId, args)
	}
}

func (node *Node) HandleStopRPC() {
	stopOp := &StopOp{
		reply: make(chan bool),
	}
	node.stopOpCh <- stopOp
	<-stopOp.reply
}

func (node *Node) HandleRequestVoteRPC(args RequestVoteArgs) error {
	DebuggerLog("Node %v: HandleRequestVoteRPC Receive RequestVote from %v: %+v ", node.id, args.CandidateID, args)
	requestVoteOp := &RequestVoteOp{
		args: args,
	}
	node.requestVoteOpCh <- requestVoteOp
	DebuggerLog("Node %v: HandleRequestVoteRPC end", node.id)
	return nil
}

func (node *Node) HandleAppendEntriesRPC(args AppendEntriesArgs) error {
	DebuggerLog("Node %v: HandleAppendEntriesRPC Receive AppendEntries from %v: %+v", node.id, args.LeaderID, args)
	appendEntriesOp := &AppendEntriesOp{
		args: args,
	}
	node.appendEntriesOpCh <- appendEntriesOp
	DebuggerLog("Node %v: HandleAppendEntriesRPC end", node.id)
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

func (node *Node) Report() {
	// Wait for all nodes in the cluster to be ready for incoming signals
	node.allNodesAreReadyForIncomingSignal.Wait()
	node.reportToClusterOpCh <- true
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
		go node.networkInterface.AppendEntries(peerId, args)
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
		DebuggerLog("Node %v: Election timeout, start election", node.id)
		node.startElection()
		return
	}
}

func (node *Node) startElectionTimer() {
	node.electionTimeout = getElectionTimeout(150, 300)
	node.electionTermStarted = node.volatileStateInfo.CurrentTerm
}

func (node *Node) handleAppendEntriesReply(op *AppendEntriesReplyOp) {
	reply := op.reply

	if op.args.isHeartbeat() {
		if reply.Term > node.volatileStateInfo.CurrentTerm {
			node.transitionToFollower(reply.Term)
			return
		}
	}

	// TODO: handle non heartbeat append entries reply
}

func (node *Node) handleRequestVoteReply(op *RequestVoteReplyOp) {
	reply := op.reply

	if node.volatileStateInfo.State != Candidate {
		DebuggerLog("Node %v: state changed to %v, stop sending RequestVote", node.id, node.volatileStateInfo.State)
		return
	}

	if reply.Term > node.volatileStateInfo.CurrentTerm {
		DebuggerLog("Node %v: term out of date in RequestVoteReply", node.id)
		node.transitionToFollower(reply.Term)
		return
	} else if reply.Term == node.volatileStateInfo.CurrentTerm {
		node.volatileStateInfo.votesTotalReceived += 1
		if reply.VoteGranted {
			node.volatileStateInfo.votesGrantedReceived += 1
			if node.volatileStateInfo.votesGrantedReceived*2 > len(node.peers)+1 {
				DebuggerLog("Node %v: wins election with %d votes", node.id, node.volatileStateInfo.votesGrantedReceived)
				node.transitionToLeader()
				return
			}
		}
	}

	if node.volatileStateInfo.votesTotalReceived*2 > len(node.peers)+1 {
		// no leader selected, start election again
		DebuggerLog("Node %v: no leader selected, start election again", node.id)
		node.startElectionTimer()
	}
}

func (node *Node) HandleAppendEntriesReplyRPC(replierId int, args AppendEntriesArgs, reply AppendEntriesReply) {
	DebuggerLog("Node %v: HandleAppendEntriesReplyRPC Receive AppendEntriesReply from %v: %+v", node.id, replierId, reply)
	appendEntriesOp := &AppendEntriesReplyOp{
		args:  args,
		reply: reply,
	}
	node.appendEntriesReplyOpCh <- appendEntriesOp
	DebuggerLog("Node %v: HandleAppendEntriesReplyRPC end", node.id)
}

func (node *Node) HandleRequestVoteReplyRPC(replierId int, reply RequestVoteReply) {
	DebuggerLog("Node %v: HandleRequestVoteReplyRPC Receive RequestVoteReply from %v: %+v", node.id, replierId, reply)
	requestVoteReplyOp := &RequestVoteReplyOp{
		reply: reply,
	}
	node.requestVoteReplyOpCh <- requestVoteReplyOp
	DebuggerLog("Node %v: HandleRequestVoteReplyRPC end", node.id)
}

func (node *Node) handleReportToCluster() {
	reportReply := &ReportReply{
		id:       node.id,
		term:     node.volatileStateInfo.CurrentTerm,
		isLeader: node.volatileStateInfo.State == Leader,
	}

	go func() {
		node.networkInterface.readyForNewIncomingReport.Wait()
		node.networkInterface.readyForNewIncomingReport.Add(1)
		node.networkInterface.reportReplyCh <- reportReply
		node.networkInterface.readyForNewIncomingReport.Done()
	}()
}
