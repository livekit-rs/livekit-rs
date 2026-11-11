// Copyright 2026 livekit-rs contributors. Apache-2.0.
//
// Test-only harness for the ONE / RID dependency-descriptor physical oracle.
//
// Nothing in the forwarding path is replaced, stubbed or reimplemented here.
// The authored code is only:
//   - the source packet plan (read from source_dd_one_rid_phases.json, which is
//     transcribed from the numbers already pinned in the Rust consumer),
//   - the coded payload fixture and the dependency structure, both built with
//     the ORIGINAL dd types and serialized by the ORIGINAL
//     DependencyDescriptorExtension.Marshal,
//   - the far-side capture plumbing, which reuses the pinned
//     downtrack_downstream_integration_test.go helpers verbatim.
//
// Every source packet travels the original public path
// buffer.Buffer.Write -> buffer.ReadExtended -> DownTrack.WriteRTP, and every
// observed output packet is read back from a real pion DTLS/SRTP transport over
// an in-memory pion vnet. No socket leaves the process.
package sfu_test

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/livekit/mediatransportutil"
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
	ddOneRidPrimaryPT  = 98
	ddOneRidSourceExtID = 13
	// ddOneRidPlanFile is the committed 68-phase table, copied next to this file
	// by prepare.py. Go tests run with the package directory as working directory.
	ddOneRidPlanFile = "source_dd_one_rid_phases.json"
)

// -----------------------------------------------------------------------------
// authored plan (data, never expected output)
// -----------------------------------------------------------------------------

type ddOneRidPhase struct {
	Phase    int    `json:"phase"`
	RID      string `json:"rid"`
	Template int    `json:"template"`
	TID      int    `json:"tid"`
	First    bool   `json:"first"`
	Last     bool   `json:"last"`
	Attached bool   `json:"attached"`
	SN       uint16 `json:"sn"`
	Frame    uint16 `json:"frame"`
	TS       uint32 `json:"ts"`
}

type ddOneRidGroup struct {
	Codec    string          `json:"codec"`
	Mode     string          `json:"mode"`
	Campaign string          `json:"campaign"`
	Prefix   string          `json:"prefix"`
	Phases   []ddOneRidPhase `json:"phases"`
}

type ddOneRidPlan struct {
	PhaseCount   int `json:"phase_count"`
	Warmup       struct {
		Frames    int    `json:"frames"`
		SNBase    uint16 `json:"sn_base"`
		FrameBase uint16 `json:"frame_base"`
		TSBase    uint32 `json:"ts_base"`
		TSStep    uint32 `json:"ts_step"`
	} `json:"warmup"`
	SenderReport struct {
		UnixSeconds int64 `json:"unix_seconds"`
		Packets     uint32 `json:"packets"`
		Octets      uint64 `json:"octets"`
		Layers      []struct {
			Layer        int32  `json:"layer"`
			RID          string `json:"rid"`
			SSRC         uint32 `json:"ssrc"`
			RtpTimestamp uint32 `json:"rtp_timestamp"`
		} `json:"layers"`
	} `json:"sender_report"`
	Pacing struct {
		EnableDelayMS  int `json:"enable_delay_ms"`
		FirstMeasuredMS int `json:"first_measured_ms"`
		MeasuredMS     int `json:"measured_ms"`
		WarmupMS       int `json:"warmup_ms"`
	} `json:"pacing"`
	SourceSSRC    map[string]uint32 `json:"source_ssrc"`
	OuterSpatial  map[string]int32  `json:"outer_spatial"`
	StructureID   map[string]int    `json:"structure_id"`
	Groups        []ddOneRidGroup   `json:"groups"`
}

func loadDdOneRidPlan(t *testing.T) *ddOneRidPlan {
	t.Helper()
	raw, err := os.ReadFile(ddOneRidPlanFile)
	require.NoError(t, err, "prepare.py must copy the committed phase table next to the observer")
	plan := &ddOneRidPlan{}
	require.NoError(t, json.Unmarshal(raw, plan))
	total := 0
	for _, g := range plan.Groups {
		total += len(g.Phases)
		for i, p := range g.Phases {
			require.Equal(t, i, p.Phase, "phase index within group")
			require.Equal(t, p.Template == 4, p.TID == 1, "source TID rule pinned by the Rust consumer")
			if g.Campaign == "rid" {
				require.Contains(t, []string{"q", "f"}, p.RID)
				require.True(t, p.First && p.Last, "RID phases are whole frames")
				require.Equal(t, p.Template == 0, p.Attached)
				require.NotZero(t, p.TS, "RID phases carry an authored source timestamp")
			} else {
				require.Empty(t, p.RID)
				require.Zero(t, p.TS, "non-RID timestamps are derived as frame*3000")
				require.Equal(t, p.Template == 0 && p.First, p.Attached,
					"only the first packet of a key frame may carry a structure")
			}
		}
	}
	require.Equal(t, plan.PhaseCount, total, "the committed table must hold every phase")
	return plan
}

// timestamp is the source RTP timestamp of a phase. Non-RID campaigns derive it
// exactly like the Rust consumer (dd_one_rid_literal.rs:82).
func (p ddOneRidPhase) timestamp(campaign string) uint32 {
	if campaign == "rid" {
		return p.TS
	}
	return uint32(p.Frame) * 3000
}

// -----------------------------------------------------------------------------
// authored source fixture
// -----------------------------------------------------------------------------

// ddOneRidStructure is the two-decode-target / zero-chain / five-template
// structure the Rust consumer's source_descriptor bit pattern describes
// (dd_one_rid_counterexample.rs:194-233). It is built with the original dd types
// and serialized by the original writer, never hand-assembled.
//
//	template 0: S0T0, DTIs [Switch, Switch],       no frame diff   (key frame)
//	template 1: S0T0, DTIs [Required, Required],   frame diff 2
//	template 2: S0T0, DTIs [Required, Required],   frame diff 1
//	template 3: S0T0, DTIs [Required, Required],   frame diff 4
//	template 4: S0T1, DTIs [NotPresent, Switch],   frame diff 1
func ddOneRidStructure(structureID int) *dd.FrameDependencyStructure {
	return &dd.FrameDependencyStructure{
		StructureId:      structureID,
		NumDecodeTargets: 2,
		NumChains:        0,
		Templates: []*dd.FrameDependencyTemplate{
			{SpatialId: 0, TemporalId: 0, DecodeTargetIndications: []dd.DecodeTargetIndication{dd.DecodeTargetSwitch, dd.DecodeTargetSwitch}},
			{SpatialId: 0, TemporalId: 0, DecodeTargetIndications: []dd.DecodeTargetIndication{dd.DecodeTargetRequired, dd.DecodeTargetRequired}, FrameDiffs: []int{2}},
			{SpatialId: 0, TemporalId: 0, DecodeTargetIndications: []dd.DecodeTargetIndication{dd.DecodeTargetRequired, dd.DecodeTargetRequired}, FrameDiffs: []int{1}},
			{SpatialId: 0, TemporalId: 0, DecodeTargetIndications: []dd.DecodeTargetIndication{dd.DecodeTargetRequired, dd.DecodeTargetRequired}, FrameDiffs: []int{4}},
			{SpatialId: 0, TemporalId: 1, DecodeTargetIndications: []dd.DecodeTargetIndication{dd.DecodeTargetNotPresent, dd.DecodeTargetSwitch}, FrameDiffs: []int{1}},
		},
	}
}

// ddOneRidDescriptorBytes marshals one source descriptor with the original
// writer and round-trips it through the original reader, so a malformed authored
// structure fails here instead of silently producing a different corpus.
func ddOneRidDescriptorBytes(t *testing.T, structure *dd.FrameDependencyStructure, p ddOneRidPhase) []byte {
	t.Helper()
	descriptor := &dd.DependencyDescriptor{
		FirstPacketInFrame: p.First,
		LastPacketInFrame:  p.Last,
		FrameNumber:        p.Frame,
		FrameDependencies:  structure.Templates[p.Template].Clone(),
	}
	if p.Attached {
		descriptor.AttachedStructure = structure
	}
	raw, err := (&dd.DependencyDescriptorExtension{Descriptor: descriptor, Structure: structure}).Marshal()
	require.NoError(t, err)
	require.LessOrEqual(t, len(raw), dd.MaxInlineExtensionSize, "the forwarding path reserves inline extension storage")

	decoded := &dd.DependencyDescriptor{}
	_, err = (&dd.DependencyDescriptorExtension{Descriptor: decoded, Structure: structure}).Unmarshal(raw)
	require.NoError(t, err)
	require.Equal(t, p.Frame, decoded.FrameNumber)
	require.Equal(t, p.First, decoded.FirstPacketInFrame)
	require.Equal(t, p.Last, decoded.LastPacketInFrame)
	require.NotNil(t, decoded.FrameDependencies)
	require.Equal(t, 0, decoded.FrameDependencies.SpatialId)
	require.Equal(t, p.TID, decoded.FrameDependencies.TemporalId)
	require.Equal(t, p.Attached, decoded.AttachedStructure != nil)
	// the wire template id is (template index + structure id) % 64, exactly the
	// raw[0]&63 value the Rust consumer asserts on the output.
	require.Equal(t, byte((p.Template+structure.StructureId)%64), raw[0]&63)
	require.Equal(t, byte(boolToInt(p.First)*2+boolToInt(p.Last)), raw[0]>>6)
	return raw
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

// ddOneRidPayload is the coded body of the Rust consumer
// (dd_one_rid_counterexample.rs:158-191), byte for byte. It is a protocol-shaped
// fixture, not a decodable picture; the original key-frame classifiers
// (codec.IsVP9KeyFrame / codec.IsAV1KeyFrame) still read it as real RTP payload.
func ddOneRidPayload(mimeType string, p ddOneRidPhase) []byte {
	label := byte('o')
	if p.RID != "" {
		label = p.RID[0]
	}
	frame := byte(p.Frame)
	if mimeType == webrtc.MimeTypeAV1 {
		if !p.First {
			return []byte{0x90, label, frame}
		}
		fragment := byte(0x40)
		if p.Last {
			fragment = 0
		}
		if p.Template == 0 {
			return []byte{0x28 | fragment, 1, 8, 0x18, 0, label, frame}
		}
		return []byte{0x10 | fragment, 0x30, 0x20, label, frame}
	}
	flags := byte(0xb0)
	if p.First {
		flags |= 8
	}
	if p.Last {
		flags |= 4
	}
	if p.Template != 0 {
		flags |= 0x40
	}
	payload := []byte{flags, 0x81, frame, 0}
	if p.Template == 0 {
		payload = append(payload, 0x80)
	} else {
		diff := []int{0, 2, 1, 4, 1}[p.Template]
		payload = append(payload, byte(diff<<1), 0x34)
	}
	return append(payload, label, frame)
}

// -----------------------------------------------------------------------------
// sink: one original DownTrack on a real SRTP transport, fed by original Buffers
// -----------------------------------------------------------------------------

type ddOneRidSource struct {
	ssrc      uint32
	layer     int32
	rid       string
	structure *dd.FrameDependencyStructure
	buf       *buffer.Buffer
	ext       chan *buffer.ExtPacket
	done      chan struct{}
}

type ddOneRidSink struct {
	*downTrackHarness
	sources map[string]*ddOneRidSource
	order   []string
	extID   int
	codecs  []webrtc.RTPCodecParameters
	// subscriber-side original DD reader, fed with the captured output packets
	outputParser *buffer.DependencyDescriptorParser
}

func newDdOneRidSink(t *testing.T, mimeType string, mode livekit.VideoLayer_Mode, rids []string) *ddOneRidSink {
	t.Helper()
	h := vnettest.NewHosts(t)
	factory := buffer.NewFactoryOfBufferFactory(500, 500).CreateBufferFactory()
	p := pacer.NewPassThrough(logger.GetLogger(), newNullBWE())
	t.Cleanup(p.Stop)

	primary := webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: mimeType, ClockRate: 90000, SDPFmtpLine: "", RTCPFeedback: vnettest.VideoRTCPFeedback()},
		PayloadType:        ddOneRidPrimaryPT,
	}
	codecs := []webrtc.RTPCodecParameters{primary}

	pc := func(net *vnet.Net) *webrtc.PeerConnection {
		me := &webrtc.MediaEngine{}
		for _, c := range codecs {
			require.NoError(t, me.RegisterCodec(c, webrtc.RTPCodecTypeVideo))
		}
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

	receiver := newFakeTrackReceiver(primary)
	// The mode under test. Both ONE modes are simulcast modes in the original
	// server (utils.IsSimulcastMode), so DetermineCodec installs the Simulcast
	// selector, the DD temporal selector, the DDTemporal munger and DDForwarder.
	receiver.VideoLayerModeReturns(mode)
	receiver.HeaderExtensionsReturns([]webrtc.RTPHeaderExtensionParameter{{URI: dd.ExtensionURI, ID: ddOneRidSourceExtID}})

	dt, err := NewDownTrack(DownTrackParams{
		Codecs:        codecs,
		Receiver:      receiver,
		BufferFactory: factory,
		Pacer:         p,
		Logger:        logger.GetLogger(),
		Listener:      &sfufakes.FakeDownTrackListener{},
		StreamID:      "svc-source",
		SubID:         livekit.ParticipantID("dd-one-rid-sink"),
		MaxTrack:      500,
	})
	require.NoError(t, err)
	transceiver, err := sender.AddTransceiverFromTrack(dt, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionSendonly})
	require.NoError(t, err)
	dt.SetTransceiver(transceiver)
	vnettest.SignalPair(t, sender, sub)
	dt.SetConnected()
	require.Eventually(t, dt.IsWritableForTest, 10*time.Second, 20*time.Millisecond)

	extID := 0
	for _, ext := range sender.GetSenders()[0].GetParameters().HeaderExtensions {
		if ext.URI == dd.ExtensionURI {
			extID = ext.ID
		}
	}
	require.NotZero(t, extID, "the negotiated DownTrack must carry the dependency descriptor extension")

	sink := &ddOneRidSink{
		downTrackHarness: &downTrackHarness{dt: dt, receiver: receiver, pacer: p, capture: capture, sender: sender, sub: sub},
		sources:          map[string]*ddOneRidSource{},
		extID:            extID,
		codecs:           codecs,
	}
	for _, rid := range rids {
		sink.addSource(t, factory, primary, rid)
	}
	sink.outputParser = buffer.NewDependencyDescriptorParser(uint8(extID), logger.GetLogger(), func(int32, int32) {}, false)
	t.Cleanup(func() {
		for _, s := range sink.sources {
			close(s.done)
			_ = s.buf.Close()
		}
	})
	return sink
}

func (s *ddOneRidSink) addSource(t *testing.T, factory *buffer.Factory, primary webrtc.RTPCodecParameters, rid string) {
	t.Helper()
	key := rid
	ssrc := uint32(101)
	layer := int32(0)
	structureID := 0
	if rid == "f" {
		ssrc, layer, structureID = 202, 2, 8
	}
	src := &ddOneRidSource{
		ssrc:      ssrc,
		layer:     layer,
		rid:       rid,
		structure: ddOneRidStructure(structureID),
		buf:       buffer.NewBuffer(ssrc, 500, 500),
		ext:       make(chan *buffer.ExtPacket, 64),
		done:      make(chan struct{}),
	}
	require.NoError(t, src.buf.Bind(webrtc.RTPParameters{
		HeaderExtensions: []webrtc.RTPHeaderExtensionParameter{{URI: dd.ExtensionURI, ID: ddOneRidSourceExtID}},
		Codecs:           s.codecs,
	}, primary.RTPCodecCapability, 0))
	s.sources[key] = src
	s.order = append(s.order, key)
	go func() {
		for {
			scratch := make([]byte, 4096)
			ep, err := src.buf.ReadExtended(scratch)
			if err != nil {
				return
			}
			if ep == nil {
				continue
			}
			select {
			case src.ext <- ep:
			case <-src.done:
				return
			}
		}
	}()
}

// write pushes one real serialized RTP packet through the original public
// Buffer.Write of the phase's own source (its own SSRC, its own DD parser
// domain) and returns the ExtPacket the original buffer actually produced.
// nil means the original buffer dropped the input; nothing is invented then.
func (s *ddOneRidSink) write(t *testing.T, p ddOneRidPhase, mimeType string, campaign string) (*buffer.ExtPacket, *ddOneRidSource) {
	t.Helper()
	src := s.sources[p.RID]
	require.NotNil(t, src, "source for rid %q", p.RID)
	pkt := &rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			Marker:         p.Last,
			PayloadType:    ddOneRidPrimaryPT,
			SequenceNumber: p.SN,
			Timestamp:      p.timestamp(campaign),
			SSRC:           src.ssrc,
		},
		Payload: ddOneRidPayload(mimeType, p),
	}
	require.NoError(t, pkt.SetExtension(ddOneRidSourceExtID, ddOneRidDescriptorBytes(t, src.structure, p)))
	raw, err := pkt.Marshal()
	require.NoError(t, err)
	_, err = src.buf.Write(raw)
	require.NoError(t, err)
	select {
	case ep := <-src.ext:
		return ep, src
	case <-time.After(500 * time.Millisecond):
		return nil, src
	}
}

// allocate drives the ORIGINAL public subscriber-settings path: the published
// layer ceiling, the subscribed spatial/temporal maximum and the real
// provisional allocation the stream allocator uses. It sets the forwarder
// TARGET only; the current layer still moves on the packet path, so a layer
// upgrade needs a key frame exactly as in production.
func (s *ddOneRidSink) allocate(t *testing.T, spatial int32) {
	t.Helper()
	s.dt.SetMaxTemporalLayer(0)
	s.dt.SetMaxSpatialLayer(spatial)
	// the original DownTrack.ProvisionalAllocatePrepare reads the published
	// layers/bitrates off the receiver, exactly as the stream allocator does.
	s.receiver.GetLayeredBitrateReturns([]int32{0, 1, 2}, ddOneRidBitrates())
	s.dt.ProvisionalAllocatePrepare()
	ok, _ := s.dt.ProvisionalAllocate(1<<30, buffer.VideoLayer{Spatial: spatial, Temporal: 0}, false, false)
	require.True(t, ok, "the requested layer must be allocatable")
	alloc := s.dt.ProvisionalAllocateCommit()
	want := buffer.VideoLayer{Spatial: spatial, Temporal: 0}
	require.Equal(t, want, alloc.TargetLayer)
	require.False(t, alloc.IsDeficient, "a deficient allocation would pause on downgrade")
	require.Equal(t, want, s.dt.MaxLayer())
}

// ddOneRidBitrates is the published bitrate table handed to the real allocator.
// Only "non-zero at the requested layer" matters to the allocation decision.
func ddOneRidBitrates() Bitrates {
	brs := Bitrates{}
	for sp := 0; sp <= 2; sp++ {
		brs[sp][0] = 10000
	}
	return brs
}

// injectSenderReports feeds the two real source SSRC clocks through the original
// public DownTrack RTCP sender-report entry point, with the same NTP instant and
// the per-RID RTP timestamps the Rust consumer pins.
func (s *ddOneRidSink) injectSenderReports(t *testing.T, plan *ddOneRidPlan) {
	t.Helper()
	ntp := uint64(mediatransportutil.ToNtpTime(time.Unix(plan.SenderReport.UnixSeconds, 0)))
	require.NotZero(t, ntp)
	for _, l := range plan.SenderReport.Layers {
		require.NoError(t, s.dt.HandleRTCPSenderReportData(ddOneRidPrimaryPT, l.Layer, &livekit.RTCPSenderReportState{
			RtpTimestamp:    l.RtpTimestamp,
			RtpTimestampExt: uint64(l.RtpTimestamp),
			NtpTimestamp:    ntp,
			At:              time.Now().UnixNano(),
			Packets:         plan.SenderReport.Packets,
			Octets:          plan.SenderReport.Octets,
		}))
	}
}

// outputDD reads a captured packet with the ORIGINAL subscriber-side dependency
// descriptor parser, so the reported frame number, structure attachment and
// active decode target mask are the original reader's own values.
func (s *ddOneRidSink) outputDD(t *testing.T, pkt *rtp.Packet) *buffer.ExtDependencyDescriptor {
	t.Helper()
	require.NotEmpty(t, pkt.GetExtension(uint8(s.extID)), "every forwarded media packet must carry a dependency descriptor")
	parsed, _, err := s.outputParser.Parse(pkt)
	require.NoError(t, err, "captured dependency descriptor must parse with the original reader")
	require.NotNil(t, parsed)
	require.NotNil(t, parsed.Descriptor)
	return parsed
}

func (s *ddOneRidSink) mediaAndPadding(from int) (media []*rtp.Packet, padding []*rtp.Packet) {
	for _, pkt := range s.capture.all()[from:] {
		if pkt.Header.Padding {
			padding = append(padding, pkt)
		} else {
			media = append(media, pkt)
		}
	}
	return media, padding
}

func ddOneRidMode(name string) livekit.VideoLayer_Mode {
	switch name {
	case "ONE_SPATIAL_LAYER_PER_STREAM":
		return livekit.VideoLayer_ONE_SPATIAL_LAYER_PER_STREAM
	case "ONE_SPATIAL_LAYER_PER_STREAM_INCOMPLETE_RTCP_SR":
		return livekit.VideoLayer_ONE_SPATIAL_LAYER_PER_STREAM_INCOMPLETE_RTCP_SR
	default:
		panic(fmt.Sprintf("unexpected mode %q", name))
	}
}