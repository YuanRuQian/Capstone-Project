package raft

import (
	"log"
	"math/rand"
	"os"
	"time"
)

func DebuggerLog(format string, a ...interface{}) {
	isDebug := os.Getenv("IsDebugMode")
	if isDebug == "true" {
		log.Printf(format, a...)
	}
}

func getRandomInt(min, max int) int {
	return min + rand.Intn(max-min)
}

func getElectionTimeout(min, max int) time.Duration {
	return time.Duration(getRandomInt(min, max)) * time.Millisecond
}

func tlog(format string, a ...interface{}) {
	format = "[TEST] " + format
	log.Printf(format, a...)
}

func sleepWithMilliseconds(n int) {
	time.Sleep(time.Duration(n) * time.Millisecond)
}

func MinInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
