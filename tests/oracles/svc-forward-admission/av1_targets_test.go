// Copyright LiveKit contributors. Apache-2.0. Test-only calls of pinned original
// DD selector; no algorithm substitutions. Physical DownTrack proof is separate.
package sfu_test

import (
	"encoding/json"
	"fmt"
	. "github.com/livekit/livekit-server/pkg/sfu"
	"github.com/livekit/livekit-server/pkg/sfu/buffer"
	"github.com/livekit/livekit-server/pkg/sfu/pacer"
	dd "github.com/livekit/livekit-server/pkg/sfu/rtpextension/dependencydescriptor"
	"github.com/livekit/livekit-server/pkg/sfu/sfufakes"
	"github.com/livekit/livekit-server/pkg/sfu/testutils"
	"github.com/livekit/livekit-server/pkg/sfu/videolayerselector"
	"github.com/livekit/livekit-server/pkg/testutils/vnettest"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/pion/interceptor"
	"github.com/pion/transport/v4/vnet"
	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestOriginalAv1IndependentDecodeTargetSelection(t *testing.T) {
	structure := &dd.FrameDependencyStructure{StructureId: 0, NumDecodeTargets: 3, Templates: []*dd.FrameDependencyTemplate{
		{SpatialId: 0, TemporalId: 0, DecodeTargetIndications: []dd.DecodeTargetIndication{dd.DecodeTargetSwitch, dd.DecodeTargetNotPresent, dd.DecodeTargetNotPresent}},
		{SpatialId: 1, TemporalId: 0, DecodeTargetIndications: []dd.DecodeTargetIndication{dd.DecodeTargetNotPresent, dd.DecodeTargetSwitch, dd.DecodeTargetNotPresent}},
		{SpatialId: 2, TemporalId: 0, DecodeTargetIndications: []dd.DecodeTargetIndication{dd.DecodeTargetNotPresent, dd.DecodeTargetNotPresent, dd.DecodeTargetSwitch}},
	}}
	for _, tc := range []struct {
		name   string
		layers []int
	}{{"independent_high_only", []int{2}}, {"independent_full", []int{0, 1, 2}}} {
		t.Run(tc.name, func(t *testing.T) {
			selector := videolayerselector.NewDependencyDescriptor(logger.GetLogger())
			selector.SetMax(buffer.VideoLayer{Spatial: 2, Temporal: 0})
			selector.SetTarget(buffer.VideoLayer{Spatial: 2, Temporal: 0})
			for i, spatial := range tc.layers {
				frame := uint16(10 + i)
				active := uint32(7)
				descriptor := &dd.DependencyDescriptor{FirstPacketInFrame: true, LastPacketInFrame: true, FrameNumber: frame, FrameDependencies: structure.Templates[spatial].Clone()}
				if i == 0 {
					descriptor.AttachedStructure = structure
					descriptor.ActiveDecodeTargetsBitmask = &active
				}
				ep, err := testutils.GetTestExtPacket(&testutils.TestExtPacketParams{SequenceNumber: 1000 + uint16(i), Timestamp: 3000, SSRC: 101, PayloadType: 98, PayloadSize: 5, Marker: true, IsKeyFrame: true, VideoLayer: buffer.VideoLayer{Spatial: int32(spatial), Temporal: 0}, ArrivalTime: time.Now()})
				require.NoError(t, err)
				ep.DependencyDescriptor = &buffer.ExtDependencyDescriptor{Descriptor: descriptor, DecodeTargets: buffer.ProcessFrameDependencyStructure(structure), StructureUpdated: i == 0, ActiveDecodeTargetsUpdated: i == 0, ActiveDecodeTargetsMask: active, Integrity: true, ExtFrameNum: uint64(frame), ExtKeyFrameNum: 10}
				result := selector.Select(ep, int32(spatial))
				require.Equal(t, spatial == 2, result.IsSelected, "actual original DTI target rejects other independent components")
				row, err := json.Marshal(map[string]any{"case": tc.name, "frame": frame, "spatial": spatial, "selected": result.IsSelected})
				require.NoError(t, err)
				fmt.Println("SOURCE_AV1_TARGET " + string(row))
			}
		})
	}
}

func newSourceAv1DownTrack(t *testing.T, target int32) *downTrackHarness {
	t.Helper()
	h := vnettest.NewHosts(t)
	factory := buffer.NewFactoryOfBufferFactory(500, 500).CreateBufferFactory()
	p := pacer.NewPassThrough(logger.GetLogger(), newNullBWE())
	t.Cleanup(p.Stop)
	codec := webrtc.RTPCodecParameters{RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeAV1, ClockRate: 90000, SDPFmtpLine: "", RTCPFeedback: vnettest.VideoRTCPFeedback()}, PayloadType: 98}
	pc := func(net *vnet.Net) *webrtc.PeerConnection {
		me := &webrtc.MediaEngine{}
		require.NoError(t, me.RegisterCodec(codec, webrtc.RTPCodecTypeVideo))
		require.NoError(t, me.RegisterHeaderExtension(webrtc.RTPHeaderExtensionCapability{URI: dd.ExtensionURI}, webrtc.RTPCodecTypeVideo))
		se := vnettest.NewSettingEngine(net)
		se.BufferFactory = factory.GetOrNew
		api := webrtc.NewAPI(webrtc.WithMediaEngine(me), webrtc.WithSettingEngine(se), webrtc.WithInterceptorRegistry(&interceptor.Registry{}))
		conn, err := api.NewPeerConnection(webrtc.Configuration{})
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close() })
		return conn
	}
	sender, sub := pc(h.OfferNet), pc(h.AnswerNet)
	capture := captureTrack(sub)
	receiver := newFakeTrackReceiver(codec)
	receiver.HeaderExtensionsReturns([]webrtc.RTPHeaderExtensionParameter{{URI: dd.ExtensionURI, ID: 13}})
	dt, err := NewDownTrack(DownTrackParams{Codecs: []webrtc.RTPCodecParameters{codec}, Receiver: receiver, BufferFactory: factory, Pacer: p, Logger: logger.GetLogger(), Listener: &sfufakes.FakeDownTrackListener{}, StreamID: "svc-source", SubID: livekit.ParticipantID(fmt.Sprint("svc-", target)), MaxTrack: 500})
	require.NoError(t, err)
	transceiver, err := sender.AddTransceiverFromTrack(dt, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionSendonly})
	require.NoError(t, err)
	dt.SetTransceiver(transceiver)
	vnettest.SignalPair(t, sender, sub)
	dt.SetConnected()
	require.Eventually(t, dt.IsWritableForTest, 10*time.Second, 20*time.Millisecond)
	dt.ForceForwardLayerForTest(buffer.VideoLayer{Spatial: target, Temporal: 0})
	return &downTrackHarness{dt: dt, receiver: receiver, pacer: p, capture: capture, sender: sender, sub: sub}
}

func TestOriginalAv1IndependentTargetsActualDownTrackSrtp(t *testing.T) {
	structure := &dd.FrameDependencyStructure{StructureId: 0, NumDecodeTargets: 3, Templates: []*dd.FrameDependencyTemplate{
		{SpatialId: 0, DecodeTargetIndications: []dd.DecodeTargetIndication{dd.DecodeTargetSwitch, 0, 0}},
		{SpatialId: 1, DecodeTargetIndications: []dd.DecodeTargetIndication{0, dd.DecodeTargetSwitch, 0}},
		{SpatialId: 2, DecodeTargetIndications: []dd.DecodeTargetIndication{0, 0, dd.DecodeTargetSwitch}},
	}}
	for _, highOnly := range []bool{false, true} {
		t.Run(fmt.Sprint("high_only_", highOnly), func(t *testing.T) {
			sink := newSourceAv1DownTrack(t, 2)
			layers := []int{0, 1, 2}
			if highOnly {
				layers = []int{2}
			}
			for i, spatial := range layers {
				frame := uint16(10 + i)
				active := uint32(7)
				descriptor := &dd.DependencyDescriptor{FirstPacketInFrame: true, LastPacketInFrame: true, FrameNumber: frame, FrameDependencies: structure.Templates[spatial].Clone()}
				if i == 0 {
					descriptor.AttachedStructure = structure
					descriptor.ActiveDecodeTargetsBitmask = &active
				}
				payload := []byte{0x18, 0x34, byte(spatial << 3), 0}
				ep, err := testutils.GetTestExtPacket(&testutils.TestExtPacketParams{SequenceNumber: 1000 + uint16(i), Timestamp: 3000, SSRC: 101, PayloadType: 98, PayloadSize: len(payload), Marker: spatial == 2, IsKeyFrame: true, VideoLayer: buffer.VideoLayer{Spatial: int32(spatial), Temporal: 0}, ArrivalTime: time.Now()})
				require.NoError(t, err)
				ep.Packet.Payload = payload
				ep.DependencyDescriptor = &buffer.ExtDependencyDescriptor{Descriptor: descriptor, DecodeTargets: buffer.ProcessFrameDependencyStructure(structure), StructureUpdated: i == 0, ActiveDecodeTargetsUpdated: i == 0, ActiveDecodeTargetsMask: 7, Integrity: true, ExtFrameNum: uint64(frame), ExtKeyFrameNum: 10}
				writes := sink.dt.WriteRTP(ep, int32(spatial))
				require.EqualValues(t, map[bool]int{true: 1, false: 0}[spatial == 2], writes)
				row, err := json.Marshal(map[string]any{"high_only": highOnly, "frame": frame, "spatial": spatial, "writes": writes})
				require.NoError(t, err)
				fmt.Println("SOURCE_AV1_PHYSICAL " + string(row))
			}
			require.Eventually(t, func() bool { return sink.capture.count() == 1 }, 10*time.Second, 20*time.Millisecond)
			require.Equal(t, []byte{0x18, 0x34, 16, 0}, sink.capture.all()[0].Payload)
			sink.dt.CloseWithFlush(false, true)
		})
	}
}
