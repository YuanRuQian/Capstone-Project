package raft

import (
	"encoding/gob"
	"log"
)

type Mode string

const (
	ReadInfo      Mode = "ReadInfo"
	WriteInfo     Mode = "WriteInfo"
	StopRunning   Mode = "StopRunning"
	AppendEntries Mode = "AppendEntries"
	RequestVote   Mode = "RequestVote"
)

type ChannelSignal struct {
	Mode Mode
}

func (p ChannelSignal) Casting() interface{} {
	if p.Mode == ReadInfo || p.Mode == WriteInfo {
		return VolatileStateInfo{}
	} else if p.Mode == StopRunning {
		return true
	} else if p.Mode == AppendEntries {
		return AppendEntriesArgs{}
	} else if p.Mode == RequestVote {
		return RequestVoteArgs{}
	} else {
		return nil
	}
}

type ChannelSignalInterface interface {
	Casting() interface{}
}

// interfaceEncode encodes the interface value into the encoder.
func interfaceEncode(enc *gob.Encoder, p ChannelSignalInterface) {
	// The encode will fail unless the concrete type has been
	// registered. We registered it in the calling function.

	// Pass pointer to interface so Encode sees (and hence sends) a value of
	// interface type. If we passed p directly it would see the concrete type instead.
	// See the blog post, "The Laws of Reflection" for background.
	err := enc.Encode(&p)
	if err != nil {
		log.Fatal("encode:", err)
	}
}

// interfaceDecode decodes the next interface value from the stream and returns it.
func interfaceDecode(dec *gob.Decoder) ChannelSignalInterface {
	// The decode will fail unless the concrete type on the wire has been
	// registered. We registered it in the calling function.
	var p ChannelSignalInterface
	err := dec.Decode(&p)
	if err != nil {
		log.Fatal("decode:", err)
	}
	return p
}
