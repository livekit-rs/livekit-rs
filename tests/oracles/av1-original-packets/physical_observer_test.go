// Copyright 2026 livekit-rs contributors. Apache-2.0 calls of fixed original consumers.
package sfu_test

import (
	"encoding/json"
	"fmt"
	"github.com/livekit/livekit-server/pkg/sfu/buffer"
	dd "github.com/livekit/livekit-server/pkg/sfu/rtpextension/dependencydescriptor"
	"github.com/livekit/livekit-server/pkg/sfu/testutils"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestOriginalAv1FragmentsUnknownDependencyLateRepairAndDroppedSlotActualSrtp(t *testing.T) {
	for _, dropSlot := range []bool{false, true} {
		t.Run(fmt.Sprint("dropped_slot_", dropSlot), func(t *testing.T) {
			sink := newSourceAv1DownTrack(t, 0)
			templates := []*dd.FrameDependencyTemplate{
				{SpatialId: 0, TemporalId: 0, DecodeTargetIndications: []dd.DecodeTargetIndication{dd.DecodeTargetSwitch}, ChainDiffs: []int{0}},
				{SpatialId: 0, TemporalId: 0, DecodeTargetIndications: []dd.DecodeTargetIndication{dd.DecodeTargetRequired}, ChainDiffs: []int{1}},
				{SpatialId: 0, TemporalId: 0, DecodeTargetIndications: []dd.DecodeTargetIndication{dd.DecodeTargetNotPresent}, ChainDiffs: []int{1}},
				{SpatialId: 0, TemporalId: 0, DecodeTargetIndications: []dd.DecodeTargetIndication{dd.DecodeTargetRequired}, FrameDiffs: []int{1}, ChainDiffs: []int{1}},
			}
			structure := &dd.FrameDependencyStructure{StructureId: 0, NumDecodeTargets: 1, NumChains: 1, DecodeTargetProtectedByChain: []int{0}, Templates: templates}
			extID := 0
			for _, ext := range sink.sender.GetSenders()[0].GetParameters().HeaderExtensions {
				if ext.URI == dd.ExtensionURI {
					extID = ext.ID
				}
			}
			require.NotZero(t, extID)
			sourceFrames := []uint16{10, 11, 12, 13, 14, 16, 15, 17}
			selected := []bool{true, false, false, true, true, true, true, true}
			count := 0
			mediaRows := 0
			for stage, frame := range sourceFrames {
				for part := 0; part < 2; part++ {
					if dropSlot && stage == 1 && part == 1 {
						continue
					}
					template := 0
					if stage == 1 {
						template = 2
					} else if stage == 2 || stage == 4 {
						template = 1
					} else if stage >= 5 {
						template = 3
					}
					descriptor := &dd.DependencyDescriptor{FirstPacketInFrame: part == 0, LastPacketInFrame: part == 1, FrameNumber: frame, FrameDependencies: templates[template].Clone()}
					active := uint32(1)
					if stage == 0 && part == 0 {
						descriptor.AttachedStructure = structure
						descriptor.ActiveDecodeTargetsBitmask = &active
					}
					prefix := byte(0x50)
					if stage == 0 || stage == 3 {
						prefix = 0x58
					}
					payload := []byte{prefix, 0x34, 0}
					if part == 1 {
						payload = []byte{0x90, byte(frame)}
					}
					seq := 1000 + (frame-10)*2 + uint16(part)
					ep, err := testutils.GetTestExtPacket(&testutils.TestExtPacketParams{SequenceNumber: seq, Timestamp: uint32(frame) * 3000, SSRC: 101, PayloadType: 98, PayloadSize: len(payload), Marker: part == 1, IsKeyFrame: stage == 0, VideoLayer: buffer.VideoLayer{Spatial: 0, Temporal: 0}, ArrivalTime: time.Now()})
					require.NoError(t, err)
					ep.Packet.Payload = payload
					ep.DependencyDescriptor = &buffer.ExtDependencyDescriptor{Descriptor: descriptor, DecodeTargets: buffer.ProcessFrameDependencyStructure(structure), StructureUpdated: stage == 0 && part == 0, ActiveDecodeTargetsUpdated: stage == 0 && part == 0, ActiveDecodeTargetsMask: 1, Integrity: part == 1, ExtFrameNum: uint64(frame), ExtKeyFrameNum: 10}
					writes := sink.dt.WriteRTP(ep, 0)
					expected := int32(0)
					if selected[stage] {
						expected = 1
					}
					require.Equal(t, expected, writes, "original per-fragment media write frame%d part%d", frame, part)
					padding := dropSlot && stage == 2 && part == 0
					if expected == 1 || padding {
						count++
						require.Eventually(t, func() bool { return sink.capture.count() == count }, 3*time.Second, 5*time.Millisecond, "original fragment must reach physical receiver before next fragment/codec-frame completion")
						packet := sink.capture.all()[count-1]
						if padding {
							require.True(t, packet.Padding)
							require.Empty(t, packet.Payload)
						} else {
							require.Equal(t, payload, packet.Payload)
							require.Equal(t, part == 1, packet.Marker)
							decoded := &dd.DependencyDescriptor{}
							extension := dd.DependencyDescriptorExtension{Descriptor: decoded, Structure: structure}
							_, err = extension.Unmarshal(packet.GetExtension(uint8(extID)))
							require.NoError(t, err)
							require.Equal(t, frame, decoded.FrameNumber)
							require.Equal(t, part == 0, decoded.FirstPacketInFrame)
							require.Equal(t, part == 1, decoded.LastPacketInFrame)
							raw, err := json.Marshal(map[string]interface{}{"dropped_slot": dropSlot, "frame": frame, "part": part, "sequence": packet.SequenceNumber, "payload": packet.Payload, "frame_diffs": decoded.FrameDependencies.FrameDiffs, "dd_len": len(packet.GetExtension(uint8(extID))), "input_attached": descriptor.AttachedStructure != nil, "output_attached": decoded.AttachedStructure != nil, "active_present": decoded.ActiveDecodeTargetsBitmask != nil})
							require.NoError(t, err)
							fmt.Println("SOURCE_AV1_ORIGINAL_PACKET " + string(raw))
							mediaRows++
						}
					}
				}
			}
			require.Equal(t, 12, mediaRows)
			require.Equal(t, 12+map[bool]int{false: 0, true: 1}[dropSlot], count)
			sink.dt.CloseWithFlush(false, true)
		})
	}
}
