// Copyright 2026 livekit-rs contributors. Apache-2.0 original-source observer.
package sfu

import (
	"encoding/json"
	"fmt"
	"github.com/livekit/livekit-server/pkg/sfu/buffer"
	"github.com/livekit/protocol/logger"
	"github.com/pion/rtp"
	"testing"
)

func TestOriginalSourcePacketHolesDroppedSlotsAndLateRepair(t *testing.T) {
	r := NewRTPMunger(logger.GetLogger())
	makePacket := func(sn uint64) *buffer.ExtPacket {
		return &buffer.ExtPacket{ExtSequenceNumber: sn, ExtTimestamp: 3000 + sn, Packet: &rtp.Packet{Header: rtp.Header{SequenceNumber: uint16(sn), Timestamp: uint32(3000 + sn)}, Payload: []byte{1}}}
	}
	r.SetLastSnTs(makePacket(1000))
	inputs := []struct {
		sn       uint64
		selected bool
	}{{1000, true}, {1002, false}, {1001, true}, {1003, false}, {1004, true}, {1003, true}}
	for _, in := range inputs {
		ep := makePacket(in.sn)
		params, err := r.UpdateAndGetSnTs(ep, true)
		kind := "media"
		var out interface{} = nil
		if err != nil {
			kind = "none"
		} else if !in.selected && params.snOrdering == SequenceNumberOrderingContiguous {
			r.PacketDropped(ep)
			kind = "none"
		} else {
			out = params.extSequenceNumber
			if !in.selected {
				kind = "padding"
			}
		}
		raw, marshalErr := json.Marshal(map[string]interface{}{"sequence": in.sn, "selected": in.selected, "kind": kind, "output": out})
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		fmt.Println("SOURCE_PACKET_MUNGER " + string(raw))
	}
}
