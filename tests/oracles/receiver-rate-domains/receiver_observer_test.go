// Copyright 2026 livekit-rs contributors. Apache-2.0 original production Buffer.Write and ReceiverBase consumers.
package sfu

import (
	"encoding/json"
	"fmt"
	"github.com/livekit/livekit-server/pkg/sfu/buffer"
	dd "github.com/livekit/livekit-server/pkg/sfu/rtpextension/dependencydescriptor"
	"github.com/livekit/protocol/livekit"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"
	"testing"
)

func newSourceRateBuffer(t *testing.T, ssrc uint32) *buffer.Buffer {
	b := buffer.NewBuffer(ssrc, 500, 500)
	codec := webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP9, ClockRate: 90000}
	require.NoError(t, b.Bind(webrtc.RTPParameters{HeaderExtensions: []webrtc.RTPHeaderExtensionParameter{{URI: dd.ExtensionURI, ID: 13}}, Codecs: []webrtc.RTPCodecParameters{{RTPCodecCapability: codec, PayloadType: 98}}}, codec, 0))
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	return b
}
func sourceRateDd(t *testing.T, frame uint16, attached bool) []byte {
	structure := &dd.FrameDependencyStructure{StructureId: 0, NumDecodeTargets: 1, Templates: []*dd.FrameDependencyTemplate{{SpatialId: 0, TemporalId: 0, DecodeTargetIndications: []dd.DecodeTargetIndication{dd.DecodeTargetSwitch}}, {SpatialId: 0, TemporalId: 0, DecodeTargetIndications: []dd.DecodeTargetIndication{dd.DecodeTargetRequired}, FrameDiffs: []int{1}}}}
	template := 1
	if attached {
		template = 0
	}
	descriptor := &dd.DependencyDescriptor{FirstPacketInFrame: true, LastPacketInFrame: true, FrameNumber: frame, FrameDependencies: structure.Templates[template].Clone()}
	if attached {
		descriptor.AttachedStructure = structure
	}
	extension := dd.DependencyDescriptorExtension{Descriptor: descriptor, Structure: structure}
	raw, err := extension.Marshal()
	require.NoError(t, err)
	return raw
}
func writeSourceRatePacket(t *testing.T, b *buffer.Buffer, ssrc uint32, frame uint16, step uint32, withDd bool, rawOverride []byte) {
	payload := []byte{0xac, byte(frame), 0, byte(frame), 0x80, 0}
	if withDd {
		payload = []byte{0xec, 0, 0x60, 0, 0x84, 0}
	}
	packet := &rtp.Packet{Header: rtp.Header{Version: 2, Marker: true, PayloadType: 98, SequenceNumber: 1000 + frame, Timestamp: 3000 + uint32(frame)*step, SSRC: ssrc}, Payload: payload}
	if rawOverride != nil {
		packet.SequenceNumber = 999
		packet.Timestamp = 2999
		require.NoError(t, packet.SetExtension(13, rawOverride))
	} else if withDd {
		require.NoError(t, packet.SetExtension(13, sourceRateDd(t, 100+frame, frame == 0)))
	}
	raw, err := packet.Marshal()
	require.NoError(t, err)
	_, err = b.Write(raw)
	require.NoError(t, err)
}
func TestOriginalVp9DdPriorityAndIndependentReceiverRateDomains(t *testing.T) {
	for _, withDd := range []bool{false, true} {
		receiver := &ReceiverBase{videoLayerMode: livekit.VideoLayer_ONE_SPATIAL_LAYER_PER_STREAM}
		buffers := [3]*buffer.Buffer{}
		steps := [3]uint32{18000, 6000, 3000}
		for slot := 0; slot < 3; slot++ {
			buffers[slot] = newSourceRateBuffer(t, 101+uint32(slot))
			receiver.buffers[slot] = buffers[slot]
		}
		// Present malformed DD must be rejected without becoming extension-not-found.
		if withDd {
			for slot, b := range buffers {
				writeSourceRatePacket(t, b, 101+uint32(slot), 0, steps[slot], false, []byte{0x80, 0})
			}
		}
		for frame := uint16(0); frame < 64; frame++ {
			for slot, b := range buffers {
				writeSourceRatePacket(t, b, 101+uint32(slot), frame, steps[slot], withDd, nil)
			}
		}
		for slot := 0; slot < 3; slot++ {
			rates := receiver.GetTemporalLayerFpsForSpatial(int32(slot))
			require.Len(t, rates, 4)
			if withDd {
				require.Greater(t, rates[0], float32(0), "DD T0 must measure even with constant VP9 payload PID/TID3")
				for _, rate := range rates[1:] {
					require.Zero(t, rate)
				}
			} else {
				require.Equal(t, []float32{[]float32{5, 15, 30}[slot], 0, 0, 0}, rates)
			}
			row, err := json.Marshal(map[string]interface{}{"dd": withDd, "layer": slot, "rates": rates})
			require.NoError(t, err)
			fmt.Println("SOURCE_RECEIVER_RATE " + string(row))
		}
		// Original ReceiverBase selects this buffer's internal spatial zero in ONE
		// mode. Replacing q's real buffer must not reset h/f's measured receivers.
		old := buffers[0]
		buffers[0] = newSourceRateBuffer(t, 101)
		receiver.buffers[0] = buffers[0]
		require.NoError(t, old.Close())
		for frame := uint16(0); frame < 64; frame++ {
			writeSourceRatePacket(t, buffers[0], 101, frame, 1500, withDd, nil)
		}
		for slot := 0; slot < 3; slot++ {
			rates := receiver.GetTemporalLayerFpsForSpatial(int32(slot))
			require.Len(t, rates, 4)
			if slot == 0 {
				require.Greater(t, rates[0], float32(30))
			}
			row, err := json.Marshal(map[string]interface{}{"dd": withDd, "layer": slot, "rates": rates})
			require.NoError(t, err)
			fmt.Println("SOURCE_RECEIVER_RESTART_RATE " + string(row))
		}
		if withDd {
			// Fixed BufferBase recreates payload calculators on DD disappearance but
			// retains frameRateCalculated after a completed DD receiver.
			for frame := uint16(64); frame < 128; frame++ {
				writeSourceRatePacket(t, buffers[0], 101, frame, 1500, false, nil)
			}
			rates := receiver.GetTemporalLayerFpsForSpatial(0)
			require.Equal(t, []float32{0, 0, 0, 0}, rates)
			row, err := json.Marshal(map[string]interface{}{"rates": rates})
			require.NoError(t, err)
			fmt.Println("SOURCE_RECEIVER_COMPLETED_DD_MISSING " + string(row))
		}
	}
}
