package raft

import (
	"testing"
)

func TestLeaderElection(t *testing.T) {
	cluster := MakeAndStartNewCluster(t, 5)
	defer cluster.KillAll()

	cluster.GetSingleLeaderInfo()
}
