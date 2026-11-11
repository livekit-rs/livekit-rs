// Copyright LiveKit, Apache-2.0. Test-only observer calls unchanged original
// DownTrack.WriteRTP and real Pion DTLS/SRTP output via existing vnet harness.
package sfu_test

import (
	"encoding/json"
	"fmt"
	. "github.com/livekit/livekit-server/pkg/sfu"
	"github.com/livekit/livekit-server/pkg/sfu/buffer"
	"github.com/livekit/livekit-server/pkg/sfu/pacer"
	"github.com/livekit/livekit-server/pkg/sfu/sfufakes"
	"github.com/livekit/livekit-server/pkg/sfu/testutils"
	"github.com/livekit/livekit-server/pkg/testutils/vnettest"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/pion/interceptor"
	"github.com/pion/rtp/codecs"
	"github.com/pion/transport/v4/vnet"
	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func newSourceSvcDownTrack(t *testing.T, target int32) *downTrackHarness {
	t.Helper()
	h := vnettest.NewHosts(t)
	factory := buffer.NewFactoryOfBufferFactory(500, 500).CreateBufferFactory()
	p := pacer.NewPassThrough(logger.GetLogger(), newNullBWE())
	t.Cleanup(p.Stop)
	codec := webrtc.RTPCodecParameters{RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP9, ClockRate: 90000, SDPFmtpLine: "profile-id=0", RTCPFeedback: vnettest.VideoRTCPFeedback()}, PayloadType: 98}
	pc := func(net *vnet.Net) *webrtc.PeerConnection {
		me := &webrtc.MediaEngine{}
		require.NoError(t, me.RegisterCodec(codec, webrtc.RTPCodecTypeVideo))
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
func TestOriginalSvcPacketSuccessfulAdmissionObserver(t *testing.T) {
	for _, name := range []string{"low_only", "mixed", "mixed_ooo"} {
		t.Run(name, func(t *testing.T) {
			sinks := []*downTrackHarness{newSourceSvcDownTrack(t, 0)}
			if name != "low_only" {
				sinks = append(sinks, newSourceSvcDownTrack(t, 2))
			}
			expectedCaptures := make([]int, len(sinks))
			snbase := uint16(1000)
			for phase := 0; phase < 2; phase++ {
				if phase == 1 && len(sinks) > 1 {
					sinks[1].dt.CloseWithFlush(false, true)
				}
				order := []uint16{0, 1, 2}
				if name == "mixed_ooo" {
					order = []uint16{0, 2, 1}
				}
				type input struct {
					spatial  uint16
					temporal uint8
					sequence uint16
				}
				inputs := make([]input, 0, 4)
				for _, spatial := range order {
					inputs = append(inputs, input{spatial, 0, snbase + spatial})
				}
				inputs = append(inputs, input{0, 2, snbase + 3})
				for _, packet := range inputs {
					spatial, temporal, sequence := packet.spatial, packet.temporal, packet.sequence
					ooo := name == "mixed_ooo" && spatial == 1 && temporal == 0
					payload := []byte{0x3c, byte(spatial<<1) | (temporal << 5), 0x80 + byte(spatial), byte(phase)}
					if temporal > 0 {
						payload = []byte{0x7c, temporal << 5, 2, 0x80, byte(phase)}
					}
					if spatial > 0 {
						payload[1] |= 1
					}
					vp9 := &codecs.VP9Packet{}
					_, err := vp9.Unmarshal(payload)
					require.NoError(t, err)
					ep, err := testutils.GetTestExtPacket(&testutils.TestExtPacketParams{SequenceNumber: sequence, Timestamp: 3000 + uint32(phase)*3000, SSRC: 101, PayloadType: 98, PayloadSize: len(payload), Marker: spatial == 2, IsKeyFrame: spatial == 0 && temporal == 0, VideoLayer: buffer.VideoLayer{Spatial: int32(spatial), Temporal: int32(temporal)}, IsOutOfOrder: ooo, ArrivalTime: time.Now()})
					require.NoError(t, err)
					ep.Packet.Payload = payload
					ep.VP9 = vp9
					writes := make([]int32, len(sinks))
					total := int32(0)
					for i, sink := range sinks {
						writes[i] = sink.dt.WriteRTP(ep, int32(spatial))
						total += writes[i]
						expectedCaptures[i] += int(writes[i])
					}
					row := map[string]any{"case": name, "phase": phase, "spatial": spatial, "temporal": temporal, "sequence": sequence, "out_of_order": ooo, "sink_writes": writes, "writes": total, "eligible": total > 0 && !ep.IsBuffered && !ep.IsOutOfOrder}
					raw, err := json.Marshal(row)
					require.NoError(t, err)
					fmt.Println("SOURCE_SVC_ADMISSION " + string(raw))
				}
				for i, sink := range sinks {
					expected := expectedCaptures[i]
					require.Eventually(t, func() bool { return sink.capture.count() >= expected }, 10*time.Second, 20*time.Millisecond, "phase %d accepted packets must physically arrive before retirement at sink %d", phase, i)
				}
				snbase += 4
			}
			for i, sink := range sinks {
				expected := expectedCaptures[i]
				require.Eventually(t, func() bool { return sink.capture.count() >= expected }, 10*time.Second, 20*time.Millisecond, "actual accepted SRTP must arrive at sink %d", i)
			}
		})
	}
}
