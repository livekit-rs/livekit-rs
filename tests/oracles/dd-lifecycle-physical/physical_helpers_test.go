// Copyright 2026 livekit-rs contributors. Apache-2.0.
//
// Test-only harness for the DD lifecycle oracle. It wires the ORIGINAL public
// buffer.Buffer.Write -> buffer.ReadExtended -> DownTrack.WriteRTP path (the
// entry point named by the Rust ignore reason) to a real pion PeerConnection
// pair negotiating real DTLS/SRTP over an in-memory pion vnet. No production
// algorithm is replaced, stubbed or reimplemented here: the only authored code
// is the source packet plan (transcribed from the Rust consumer) and the
// far-side capture plumbing, which reuses the pinned
// downtrack_downstream_integration_test.go helpers verbatim.
package sfu_test

import (
	"testing"
	"time"

	. "github.com/livekit/livekit-server/pkg/sfu"
	"github.com/livekit/livekit-server/pkg/sfu/buffer"
	"github.com/livekit/livekit-server/pkg/sfu/pacer"
	dd "github.com/livekit/livekit-server/pkg/sfu/rtpextension/dependencydescriptor"
	"github.com/livekit/livekit-server/pkg/sfu/sfufakes"
	"github.com/livekit/livekit-server/pkg/testutils/vnettest"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/pion/transport/v4/vnet"
	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"
)

const (
	ddLifecyclePT         = 98
	ddLifecycleSourceSSRC = 101
	ddLifecycleSourceExtID = 13
	// ddLifecycleSendSpacing mirrors the Rust consumer's >=200ms source cadence.
	ddLifecycleSendSpacing = 200 * time.Millisecond
)

// ddLifecycleChainStructure is the one-decode-target / one-chain / four-S0T0-template
// structure the Rust consumer's raw_chain_descriptor bit pattern describes
// (structure id 0, 1 decode target, 1 chain, templates S/R/-/R with the fourth
// carrying frame diff 1 and chain diffs 0/1/1/1). It is built with the original
// dd types and serialized by the original DependencyDescriptorExtension.Marshal,
// never hand-assembled from the Rust bit pattern.
func ddLifecycleChainStructure() *dd.FrameDependencyStructure {
	return &dd.FrameDependencyStructure{
		StructureId:                  0,
		NumDecodeTargets:             1,
		NumChains:                    1,
		DecodeTargetProtectedByChain: []int{0},
		Templates: []*dd.FrameDependencyTemplate{
			{SpatialId: 0, TemporalId: 0, DecodeTargetIndications: []dd.DecodeTargetIndication{dd.DecodeTargetSwitch}, ChainDiffs: []int{0}},
			{SpatialId: 0, TemporalId: 0, DecodeTargetIndications: []dd.DecodeTargetIndication{dd.DecodeTargetRequired}, ChainDiffs: []int{1}},
			{SpatialId: 0, TemporalId: 0, DecodeTargetIndications: []dd.DecodeTargetIndication{dd.DecodeTargetNotPresent}, ChainDiffs: []int{1}},
			{SpatialId: 0, TemporalId: 0, DecodeTargetIndications: []dd.DecodeTargetIndication{dd.DecodeTargetRequired}, FrameDiffs: []int{1}, ChainDiffs: []int{1}},
		},
	}
}

// ddLifecycleDescriptorBytes marshals a whole-frame descriptor with the original
// writer and round-trips it through the original reader, so a malformed authored
// structure fails here instead of silently producing a different corpus.
func ddLifecycleDescriptorBytes(t *testing.T, structure *dd.FrameDependencyStructure, frame uint16, attached bool) []byte {
	t.Helper()
	descriptor := &dd.DependencyDescriptor{
		FirstPacketInFrame: true,
		LastPacketInFrame:  true,
		FrameNumber:        frame,
		FrameDependencies:  structure.Templates[0].Clone(),
	}
	if attached {
		descriptor.AttachedStructure = structure
	}
	raw, err := (&dd.DependencyDescriptorExtension{Descriptor: descriptor, Structure: structure}).Marshal()
	require.NoError(t, err)
	decoded := &dd.DependencyDescriptor{}
	_, err = (&dd.DependencyDescriptorExtension{Descriptor: decoded, Structure: structure}).Unmarshal(raw)
	require.NoError(t, err)
	require.Equal(t, frame, decoded.FrameNumber)
	require.True(t, decoded.FirstPacketInFrame)
	require.True(t, decoded.LastPacketInFrame)
	require.NotNil(t, decoded.FrameDependencies)
	require.Equal(t, 0, decoded.FrameDependencies.SpatialId)
	require.Equal(t, 0, decoded.FrameDependencies.TemporalId)
	if attached {
		require.NotNil(t, decoded.AttachedStructure)
		require.Len(t, decoded.AttachedStructure.Templates, 4)
	} else {
		require.Nil(t, decoded.AttachedStructure)
	}
	return raw
}

// ddLifecyclePayload is the coded body of the Rust consumer
// (dd_disappearance.rs:124-138). It is a protocol-shaped fixture, not a decodable
// picture.
func ddLifecyclePayload(mimeType string, frame byte, key bool) []byte {
	if mimeType == webrtc.MimeTypeAV1 {
		if key {
			return []byte{0x28, 1, 8, 0x18, 0}
		}
		return []byte{0x10, 0x30, 0x20, frame}
	}
	value := []byte{0xfc, 0x81, frame, 0}
	if key {
		value[0] = 0xbc
	}
	if !key {
		value = append(value, 2)
	}
	if key {
		return append(value, 0x80, frame)
	}
	return append(value, 0x34, frame)
}

// ddLifecycleSink is one original DownTrack on a real SRTP transport plus the
// original source Buffer that feeds it.
type ddLifecycleSink struct {
	*downTrackHarness
	source    *buffer.Buffer
	ext       chan *buffer.ExtPacket
	done      chan struct{}
	extID     int
	structure *dd.FrameDependencyStructure
}

func newDdLifecycleSink(t *testing.T, mimeType string) *ddLifecycleSink {
	t.Helper()
	h := vnettest.NewHosts(t)
	factory := buffer.NewFactoryOfBufferFactory(500, 500).CreateBufferFactory()
	p := pacer.NewPassThrough(logger.GetLogger(), newNullBWE())
	t.Cleanup(p.Stop)

	codec := webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: mimeType, ClockRate: 90000, SDPFmtpLine: "", RTCPFeedback: vnettest.VideoRTCPFeedback()},
		PayloadType:        ddLifecyclePT,
	}
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
	if mimeType == webrtc.MimeTypeVP9 {
		receiver.VideoLayerModeReturns(livekit.VideoLayer_MULTIPLE_SPATIAL_LAYERS_PER_STREAM)
	}
	receiver.HeaderExtensionsReturns([]webrtc.RTPHeaderExtensionParameter{{URI: dd.ExtensionURI, ID: ddLifecycleSourceExtID}})

	dt, err := NewDownTrack(DownTrackParams{
		Codecs:        []webrtc.RTPCodecParameters{codec},
		Receiver:      receiver,
		BufferFactory: factory,
		Pacer:         p,
		Logger:        logger.GetLogger(),
		Listener:      &sfufakes.FakeDownTrackListener{},
		StreamID:      "svc-source",
		SubID:         livekit.ParticipantID("dd-lifecycle-sink"),
		MaxTrack:      500,
	})
	require.NoError(t, err)
	transceiver, err := sender.AddTransceiverFromTrack(dt, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionSendonly})
	require.NoError(t, err)
	dt.SetTransceiver(transceiver)
	vnettest.SignalPair(t, sender, sub)
	dt.SetConnected()
	require.Eventually(t, dt.IsWritableForTest, 10*time.Second, 20*time.Millisecond)
	dt.ForceForwardLayerForTest(buffer.VideoLayer{Spatial: 0, Temporal: 0})

	extID := 0
	for _, ext := range sender.GetSenders()[0].GetParameters().HeaderExtensions {
		if ext.URI == dd.ExtensionURI {
			extID = ext.ID
		}
	}
	require.NotZero(t, extID, "the negotiated DownTrack must carry the dependency descriptor extension")

	// The original public source entry point named by the Rust ignore reason:
	// buffer.go:159 Write -> buffer_base.go BindLocked/createDDParserAndFrameRateCalculator
	// -> processVideoPacket -> ReadExtended.
	source := buffer.NewBuffer(ddLifecycleSourceSSRC, 500, 500)
	require.NoError(t, source.Bind(webrtc.RTPParameters{
		HeaderExtensions: []webrtc.RTPHeaderExtensionParameter{{URI: dd.ExtensionURI, ID: ddLifecycleSourceExtID}},
		Codecs:           []webrtc.RTPCodecParameters{codec},
	}, codec.RTPCodecCapability, 0))

	sink := &ddLifecycleSink{
		downTrackHarness: &downTrackHarness{dt: dt, receiver: receiver, pacer: p, capture: capture, sender: sender, sub: sub},
		source:           source,
		ext:              make(chan *buffer.ExtPacket, 64),
		done:             make(chan struct{}),
		extID:            extID,
		structure:        ddLifecycleChainStructure(),
	}
	t.Cleanup(func() {
		close(sink.done)
		_ = source.Close()
	})
	go func() {
		for {
			scratch := make([]byte, 4096)
			ep, err := source.ReadExtended(scratch)
			if err != nil {
				return
			}
			if ep == nil {
				continue
			}
			select {
			case sink.ext <- ep:
			case <-sink.done:
				return
			}
		}
	}()
	return sink
}

// write pushes one real serialized RTP packet through the original public
// Buffer.Write and returns the ExtPacket the original buffer actually produced.
// nil means the original buffer rejected the input (for example a malformed
// present dependency descriptor); nothing is invented in that case.
func (s *ddLifecycleSink) write(t *testing.T, pkt *rtp.Packet) *buffer.ExtPacket {
	t.Helper()
	raw, err := pkt.Marshal()
	require.NoError(t, err)
	_, err = s.source.Write(raw)
	require.NoError(t, err)
	select {
	case ep := <-s.ext:
		return ep
	case <-time.After(500 * time.Millisecond):
		return nil
	}
}

// mediaAndPadding splits the far-side SRTP capture exactly the way the Rust
// consumer splits its receipts: the RTP padding bit, not a payload heuristic.
func (s *ddLifecycleSink) mediaAndPadding() (media []*rtp.Packet, padding []*rtp.Packet) {
	for _, pkt := range s.capture.all() {
		if pkt.Header.Padding {
			padding = append(padding, pkt)
		} else {
			media = append(media, pkt)
		}
	}
	return media, padding
}

// outputDD reports whether the far-side packet really carries a decodable
// dependency descriptor, using the original reader on the captured bytes.
func (s *ddLifecycleSink) outputDD(t *testing.T, pkt *rtp.Packet) bool {
	t.Helper()
	raw := pkt.GetExtension(uint8(s.extID))
	if len(raw) == 0 {
		return false
	}
	decoded := &dd.DependencyDescriptor{}
	_, err := (&dd.DependencyDescriptorExtension{Descriptor: decoded, Structure: s.structure}).Unmarshal(raw)
	require.NoError(t, err, "captured dependency descriptor must decode with the original reader")
	return true
}