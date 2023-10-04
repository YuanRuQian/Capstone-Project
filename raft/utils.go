package raft

import (
	"log"
	"math/rand"
	"time"
)

const (
	IsDebugMode = true
)

func DebuggerLog(format string, a ...interface{}) {
	if IsDebugMode {
		log.Printf(format, a...)
	}
}

func getRandomInt(min, max int) int {
	return min + rand.Intn(max-min)
}

func getElectionTimeout(min, max int) time.Duration {
	return time.Duration(getRandomInt(min, max)) * time.Millisecond
}

func getVolatileStateInfoCopyFrom(info VolatileStateInfo) VolatileStateInfo {
	return VolatileStateInfo{
		State:             info.State,
		CurrentTerm:       info.CurrentTerm,
		LastElectionReset: info.LastElectionReset,
		VotedFor:          info.VotedFor,
	}
}
