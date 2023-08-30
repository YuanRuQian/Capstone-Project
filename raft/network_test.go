package raft

import "testing"

func TestBasics(t *testing.T) {
	cluster := MakeAndStartNewCluster(t, 3)
	defer cluster.KillAll()

	cluster.GetLeaderIDAndTerm()
}
