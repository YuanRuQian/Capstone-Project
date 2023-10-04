package raft

// command line to see the goroutinue stack dump: go test -v -run TestElectionBasic -bench=. -cpuprofile=cpu.pprof -memprofile=mem.pprof
// see the content at: http://localhost:6060/debug/pprof/goroutine?debug=2

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"testing"
)

func startPProfServer() {
	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()
}

func TestElectionBasic(t *testing.T) {
	startPProfServer()

	h := MakeNewCluster(t, 3)
	defer h.Shutdown()

	h.CheckSingleLeader()
}

func TestInterfaceEncoding(t *testing.T) {
	var network bytes.Buffer // Stand-in for the network.

	// We must register the concrete type for the encoder and decoder (which would
	// normally be on a separate machine from the encoder). On each end, this tells the
	// engine which concrete type is being sent that implements the interface.
	gob.Register(ChannelSignal{})

	// Create an encoder and send some values.
	enc := gob.NewEncoder(&network)

	modes := []Mode{
		ReadInfo,
		WriteInfo,
		StopRunning,
		AppendEntries,
		RequestVote,
	}

	for _, mode := range modes {
		interfaceEncode(enc, ChannelSignal{Mode: mode})
	}

	// Create a decoder and receive some values.
	dec := gob.NewDecoder(&network)
	for _, mode := range modes {
		result := interfaceDecode(dec)
		fmt.Printf("Mode: %s | %+v\n", mode, result.Casting())
	}

}
