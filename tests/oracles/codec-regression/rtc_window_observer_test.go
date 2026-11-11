//go:build source_codec_observer

package rtc

import (
	"encoding/json"
	"fmt"
	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/rtc/types"
	"github.com/livekit/livekit-server/pkg/sfu"
	"github.com/livekit/livekit-server/pkg/sfu/buffer"
	"github.com/livekit/livekit-server/pkg/telemetry"
	"github.com/livekit/protocol/codecs/mime"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/protocol/observability/roomobs"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/transport/v4/packetio"
	"github.com/pion/webrtc/v4"
	"sync"
	"testing"
	"time"
)

type sourceCodecTrack struct {
	id    string
	ssrc  uint32
	codec webrtc.RTPCodecParameters
}

func (t sourceCodecTrack) ID() string                       { return t.id }
func (t sourceCodecTrack) RID() string                      { return "" }
func (t sourceCodecTrack) Msid() string                     { return "source " + t.id }
func (t sourceCodecTrack) SSRC() webrtc.SSRC                { return webrtc.SSRC(t.ssrc) }
func (t sourceCodecTrack) RtxSSRC() webrtc.SSRC             { return 0 }
func (t sourceCodecTrack) StreamID() string                 { return "source" }
func (t sourceCodecTrack) Kind() webrtc.RTPCodecType        { return webrtc.RTPCodecTypeVideo }
func (t sourceCodecTrack) Codec() webrtc.RTPCodecParameters { return t.codec }
func (t sourceCodecTrack) RTCTrack() *webrtc.TrackRemote    { return nil }

type sourceCodecSink struct {
	types.NullParticipantTelemetryListener
	mu      sync.Mutex
	packets map[string]uint64
	bytes   map[string]uint64
	nacks   uint64
}

func (s *sourceCodecSink) OnTrackStats(_ telemetry.StatsKey, stat *livekit.AnalyticsStat) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, stream := range stat.Streams {
		s.packets[stat.Mime] += uint64(stream.PrimaryPackets + stream.PaddingPackets + stream.RetransmitPackets)
		s.bytes[stat.Mime] += stream.PrimaryBytes + stream.PaddingBytes + stream.RetransmitBytes
		s.nacks += uint64(stream.Nacks)
	}
}
func (s *sourceCodecSink) row(phase string) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := map[string]uint64{}
	b := map[string]uint64{}
	for key, value := range s.packets {
		p[key] = value
	}
	for key, value := range s.bytes {
		b[key] = value
	}
	return map[string]any{"phase": phase, "packets": p, "bytes": b, "nacks": s.nacks}
}
func sourceCodecRow(t *testing.T, row map[string]any) {
	t.Helper()
	wire, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("SOURCE_CODEC_WINDOW %s\n", wire)
}

type sourceCodecFixture struct {
	track    *MediaTrack
	factory  *buffer.Factory
	pc       *webrtc.PeerConnection
	receiver *webrtc.RTPReceiver
	codecs   []webrtc.RTPCodecParameters
	sink     *sourceCodecSink
}

func sourceCodecs(t *testing.T, allows bool) *sourceCodecFixture {
	t.Helper()
	engine := &webrtc.MediaEngine{}
	codecs := []webrtc.RTPCodecParameters{}
	for i, mime := range []string{"video/VP9", "video/VP8", "video/H264"} {
		pt := []webrtc.PayloadType{98, 96, 102}[i]
		codec := webrtc.RTPCodecParameters{RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: mime, ClockRate: 90000, RTCPFeedback: []webrtc.RTCPFeedback{{Type: "nack"}, {Type: "nack", Parameter: "pli"}}}, PayloadType: pt}
		if err := engine.RegisterCodec(codec, webrtc.RTPCodecTypeVideo); err != nil {
			t.Fatal(err)
		}
		codecs = append(codecs, codec)
	}
	if err := engine.RegisterCodec(webrtc.RTPCodecParameters{RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: "video/rtx", ClockRate: 90000, SDPFmtpLine: "apt=98"}, PayloadType: 99}, webrtc.RTPCodecTypeVideo); err != nil {
		t.Fatal(err)
	}
	pc, err := webrtc.NewAPI(webrtc.WithMediaEngine(engine)).NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	transceiver, err := pc.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo)
	if err != nil {
		t.Fatal(err)
	}
	factory := buffer.NewFactoryOfBufferFactory(500, 200).CreateBufferFactory()
	sink := &sourceCodecSink{packets: map[string]uint64{}, bytes: map[string]uint64{}}
	ti := &livekit.TrackInfo{Sid: "TR_original_windows", Type: livekit.TrackType_VIDEO, BackupCodecPolicy: livekit.BackupCodecPolicy_PREFER_REGRESSION,
		Codecs: []*livekit.SimulcastCodecInfo{{MimeType: "video/vp9", Cid: "primary"}, {MimeType: "video/vp8", Cid: "target"}, {MimeType: "video/h264", Cid: "third"}}}
	track := NewMediaTrack(MediaTrackParams{ParticipantID: func() livekit.ParticipantID { return "PA_original" }, BufferFactory: factory,
		ReceiverConfig: ReceiverConfig{PacketBufferSizeVideo: 500, PacketBufferSizeAudio: 200}, VideoConfig: config.DefaultConfig.Video,
		TelemetryListener: func() types.ParticipantTelemetryListener { return sink }, Reporter: roomobs.NewNoopTrackReporter(), Logger: logger.GetLogger(),
		OnRTCP: func([]rtcp.Packet) {}, ShouldRegressCodec: func() bool { return allows }}, ti)
	fixture := &sourceCodecFixture{track, factory, pc, transceiver.Receiver(), codecs, sink}
	t.Cleanup(func() {
		for _, ssrc := range []uint32{101, 102, 103, 201} {
			if buff := factory.GetBuffer(ssrc); buff != nil {
				_ = buff.Close()
			}
		}
		track.Close(false)
		_ = pc.Close()
	})
	return fixture
}
func (f *sourceCodecFixture) add(t *testing.T, index int) *sfu.WebRTCReceiver {
	t.Helper()
	ssrc := uint32(101 + index)
	f.factory.GetOrNew(packetio.RTPBufferPacket, ssrc)
	f.factory.GetOrNew(packetio.RTCPBufferPacket, ssrc)
	added, ok := f.track.AddReceiver(f.receiver, sourceCodecTrack{id: []string{"primary", "target", "third"}[index], ssrc: ssrc, codec: f.codecs[index]}, fmt.Sprint(index))
	if !added || !ok {
		t.Fatalf("original AddReceiver rejected index %d: new=%v ok=%v", index, added, ok)
	}
	receiver, ok := f.track.Receiver(mime.NormalizeMimeType(f.codecs[index].MimeType)).(*sfu.WebRTCReceiver)
	if !ok {
		t.Fatal("original WebRTCReceiver required")
	}
	return receiver
}
func (f *sourceCodecFixture) write(t *testing.T, index int, sequence uint16, pt webrtc.PayloadType) {
	t.Helper()
	payloadIndex := index
	if pt == f.codecs[1].PayloadType {
		payloadIndex = 1
	}
	payload := [][]byte{{0x0c, 0x82, 0x49, 0x83, 0x42, 0, 1, 2}, {0x10, 0, 0, 0, 0x9d, 1, 0x2a, 1, 0, 1, 0}, {0x65, 1, 2, 3}}[payloadIndex]
	packet := &rtp.Packet{Header: rtp.Header{Version: 2, Marker: true, PayloadType: uint8(pt), SequenceNumber: sequence, Timestamp: uint32(sequence) * 3000, SSRC: uint32(101 + index)}, Payload: payload}
	wire, err := packet.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.factory.GetBuffer(uint32(101 + index)).Write(wire); err != nil {
		t.Fatal(err)
	}
}
func sourcePacketCount(s *sourceCodecSink, mime string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.packets[mime]
}
func TestOriginalLiveKitCodecDeltaWindowAndClose(t *testing.T) {
	f := sourceCodecs(t, true)
	primary := f.add(t, 0)
	third := f.add(t, 2)
	// Actual original Buffer refuses initialization by padding-only or repair.
	padding := &rtp.Packet{Header: rtp.Header{Version: 2, Padding: true, PayloadType: 98, SequenceNumber: 65000, Timestamp: 1, SSRC: 101, PaddingSize: 4}}
	paddingWire, err := padding.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.factory.GetBuffer(101).Write(paddingWire); err != nil {
		t.Fatal(err)
	}
	f.factory.GetOrNew(packetio.RTPBufferPacket, 201)
	repair := f.factory.GetBuffer(201)
	repair.SetPrimaryBufferForRTX(f.factory.GetBuffer(101))
	rtx := &rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 99, SequenceNumber: 9, Timestamp: 1, SSRC: 201}, Payload: []byte{0, 1, 0x0c, 0x82, 0x49, 0x83, 0x42, 0, 1, 2}}
	rtxWire, err := rtx.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = repair.Write(rtxWire); err != nil {
		t.Fatal(err)
	}
	sfu.SourceCodecSnapshot(primary)
	if sourcePacketCount(f.sink, mime.MimeTypeVP9.String()) != 0 {
		t.Fatal("pre-start padding/RTX initialized original source")
	}
	f.write(t, 0, 1, f.codecs[0].PayloadType)
	f.write(t, 2, 1, f.codecs[2].PayloadType)
	// Exact original GetDeltaStats -> getStat -> MediaTrack.OnStatsUpdate gate.
	sfu.SourceCodecSnapshot(primary)
	sfu.SourceCodecSnapshot(third)
	if sourcePacketCount(f.sink, mime.MimeTypeVP9.String()) != 1 || sourcePacketCount(f.sink, mime.MimeTypeH264.String()) != 0 {
		t.Fatalf("first original window %v", f.sink.row("first"))
	}
	// A duplicate-only interval consumes its snapshot but exports no delta:
	// original RTPStats.deltaInfo requires positive sequence progress.
	f.write(t, 0, 1, f.codecs[0].PayloadType)
	// Fixed Pion accepts exactly the two OSN bytes. Original Buffer counts
	// this initialized empty repair as a duplicate, then drops media delivery.
	rtx.SequenceNumber = 10
	// Match the original packet timestamp: the original RTPStats rejects
	// a repair timestamp predating the started source before duplicate accounting.
	rtx.Timestamp = 3000
	rtx.Payload = []byte{0, 1}
	rtxWire, err = rtx.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = repair.Write(rtxWire); err != nil {
		t.Fatal(err)
	}
	rawStats := f.factory.GetBuffer(101).GetStats()
	if rawStats.PacketsDuplicate != 2 || rawStats.BytesDuplicate != 32 {
		t.Fatalf("original OSN-only duplicate must count its 12-byte logical RTP header: %v", rawStats)
	}
	rtx.SequenceNumber = 11
	rtx.Payload = []byte{0}
	rtxWire, err = rtx.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = repair.Write(rtxWire); err != nil {
		t.Fatal(err)
	}
	rawStats = f.factory.GetBuffer(101).GetStats()
	if rawStats.PacketsDuplicate != 2 || rawStats.BytesDuplicate != 32 {
		t.Fatal("truncated original repair changed RTPStats")
	}
	sfu.SourceCodecSnapshot(primary)
	if sourcePacketCount(f.sink, mime.MimeTypeVP9.String()) != 1 || f.sink.row("duplicate")["bytes"].(map[string]uint64)[mime.MimeTypeVP9.String()] != 20 {
		t.Fatal("duplicate-only original interval was not discarded")
	}
	first := f.sink.row("discarded_window")
	first["pre_start_ignored"] = true
	first["zero_progress_discarded"] = true
	first["osn_only_duplicate_counted"] = true
	first["short_repair_rejected"] = true
	sourceCodecRow(t, first)
	f.write(t, 0, 2, f.codecs[0].PayloadType)
	f.write(t, 2, 2, f.codecs[2].PayloadType)
	target := f.add(t, 1)
	f.write(t, 1, 1, f.codecs[1].PayloadType)
	if !f.track.regressionTargetCodecReceived.Load() {
		t.Fatal("actual original target receiver must open sticky gate")
	}
	sfu.SourceCodecSnapshot(primary)
	sfu.SourceCodecSnapshot(third)
	sfu.SourceCodecSnapshot(target)
	if sourcePacketCount(f.sink, mime.MimeTypeVP9.String()) != 2 || sourcePacketCount(f.sink, mime.MimeTypeH264.String()) != 1 || sourcePacketCount(f.sink, mime.MimeTypeVP8.String()) != 1 {
		t.Fatalf("current original window %v", f.sink.row("second"))
	}
	sourceCodecRow(t, f.sink.row("current_window_after_target"))
	f.write(t, 0, 3, f.codecs[0].PayloadType)
	f.write(t, 2, 3, f.codecs[2].PayloadType)
	// MediaTrack.Close only clears logical receiver references. Actual RTP
	// transport teardown closes buffers, whose forwarder EOF closes the real
	// WebRTCReceiver and its ConnectionStats worker (with buffers retained).
	for _, ssrc := range []uint32{101, 102, 103, 201} {
		if buff := f.factory.GetBuffer(ssrc); buff != nil {
			if err := buff.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
	f.track.Close(false)
	deadline := time.Now().Add(2 * time.Second)
	for sourcePacketCount(f.sink, mime.MimeTypeVP9.String()) != 3 || sourcePacketCount(f.sink, mime.MimeTypeH264.String()) != 2 {
		if time.Now().After(deadline) {
			t.Fatalf("original close failed to flush pending deltas %v", f.sink.row("close"))
		}
		time.Sleep(time.Millisecond)
	}
	sourceCodecRow(t, f.sink.row("terminal_flush"))
}
func TestOriginalLiveKitCodecInvalidAndGeneratedNack(t *testing.T) {
	f := sourceCodecs(t, false)
	primary := f.add(t, 0)
	_ = f.add(t, 1)
	f.write(t, 0, 1, f.codecs[0].PayloadType)
	f.write(t, 1, 1, f.codecs[1].PayloadType)
	// Same original bound buffer/SSRC switches main PT in order.
	f.write(t, 0, 2, f.codecs[1].PayloadType)
	deadline := time.Now().Add(2 * time.Second)
	var routed bool
	for !routed || primary.CodecState() != sfu.ReceiverCodecStateInvalid {
		for _, receiver := range f.track.receivers {
			if receiver.Mime() == mime.MimeTypeVP9 {
				routed = receiver.IsRegressed()
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("original codec change did not reach Invalid and backup")
		}
		time.Sleep(time.Millisecond)
	}
	if !routed || f.track.regressionTargetCodecReceived.Load() {
		t.Fatal("Invalid route must reach backup without inventing normal target-received flag")
	}
	sourceCodecRow(t, map[string]any{"phase": "in_order_codec_invalid", "invalid": true, "routed": routed, "target_received": f.track.regressionTargetCodecReceived.Load()})
	// Original GetNACKPairs updates RTP stats while no RTCP socket exists.
	f.write(t, 0, 4, f.codecs[1].PayloadType)
	var generated int
	for generated == 0 {
		generated = len(f.factory.GetBuffer(101).GetNACKPairs())
		if time.Now().After(deadline) {
			t.Fatal("original sequence gap did not generate NACK")
		}
		if generated == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}
	sfu.SourceCodecSnapshot(primary)
	row := f.sink.row("generated_untransmitted_nack")
	if row["nacks"].(uint64) == 0 {
		t.Fatalf("generated NACK missing from original accepted stats: %v", row)
	}
	if row["packets"].(map[string]uint64)[mime.MimeTypeVP9.String()] != 4 || row["bytes"].(map[string]uint64)[mime.MimeTypeVP9.String()] != 66 {
		t.Fatalf("original expected SN span must include missing sequence: %v", row)
	}
	row["socket_emissions"] = 0
	sourceCodecRow(t, row)
}
