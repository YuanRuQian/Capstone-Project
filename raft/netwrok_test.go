package raft

import (
	"testing"
)

func TestElectionBasic(t *testing.T) {
	h := MakeNewCluster(t, 3)
	defer h.Shutdown()

	h.CheckSingleLeader()
}
