package observer_test

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/livekit/livekit-server/pkg/routing/routingfakes"
	"github.com/livekit/livekit-server/pkg/rtc"
	"github.com/livekit/livekit-server/pkg/rtc/types"
	"github.com/livekit/livekit-server/pkg/rtc/types/typesfakes"
	"github.com/livekit/livekit-server/pkg/sfu/buffer"
	lkprom "github.com/livekit/livekit-server/pkg/telemetry/prometheus"
	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/protocol/observability/roomobs"
	"github.com/livekit/protocol/utils"
	"github.com/pion/ice/v4"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func TestMain(m *testing.M) {
	// Original upstream test setup; no HTTP server. The process-local sampler
	// terminates with this binary. Every peer, participant and socket is closed.
	if err := lkprom.Init("c63-observer", livekit.NodeType_SERVER); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

type input struct {
	FireOnTrackBySDP bool            `json:"fire_on_track_by_sdp"`
	Request          json.RawMessage `json:"request"`
	EnabledCodecs    []struct {
		Mime string `json:"mime"`
		Fmtp string `json:"fmtp_line"`
	} `json:"enabled_codecs"`
	Browser         string `json:"browser"`
	ProtocolVersion int    `json:"protocol_version"`
	PeerCodec       struct {
		Mime        string `json:"mime_type"`
		ClockRate   uint32 `json:"clock_rate"`
		PayloadType uint8  `json:"payload_type"`
	} `json:"peer_codec"`
	SSRC      uint32   `json:"sdp_ssrc"`
	Mid       string   `json:"sdp_mid"`
	Rids      []string `json:"sdp_rids"`
	Simulcast bool     `json:"sdp_simulcast"`
	RTP       struct {
		Count         int    `json:"count"`
		SequenceStart uint16 `json:"sequence_start"`
		Timestamp     uint32 `json:"timestamp_start"`
		TimestampStep uint32 `json:"timestamp_step"`
		Marker        bool   `json:"marker"`
		PayloadHex    string `json:"payload_hex"`
		IntervalMS    int    `json:"interval_ms"`
	} `json:"rtp"`
}
type fixture struct {
	SchemaVersion int    `json:"schema_version"`
	Case          string `json:"case"`
	Input         input  `json:"input"`
}
type observation struct {
	SchemaVersion int            `json:"schema_version"`
	Case          string         `json:"case"`
	Input         input          `json:"input"`
	Output        map[string]any `json:"output"`
}

func check(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

var jsonOptions = protojson.MarshalOptions{UseProtoNames: true, UseEnumNumbers: true, EmitUnpopulated: true}

// Evidence is written synchronously, including directly inside the publication
// listener. It retains full protobuf JSON (SID/version included), SDP and RTP.
// Errors are stored for the test goroutine rather than calling Fatal in callbacks.
type journal struct {
	mu   sync.Mutex
	file *os.File
	err  error
}

func (j *journal) write(caseName, event string, value any) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.err != nil {
		return
	}
	if message, ok := value.(proto.Message); ok {
		var raw []byte
		raw, j.err = jsonOptions.Marshal(message)
		if j.err != nil {
			return
		}
		value = json.RawMessage(raw)
	}
	j.err = json.NewEncoder(j.file).Encode(map[string]any{"case": caseName, "event": event, "value": value})
	if j.err == nil {
		j.err = j.file.Sync()
	}
}

func projection(t *testing.T, track *livekit.TrackInfo) map[string]any {
	t.Helper()
	raw, err := jsonOptions.Marshal(track)
	check(t, err)
	var result map[string]any
	check(t, json.Unmarshal(raw, &result))
	delete(result, "sid")
	delete(result, "version")
	return result
}

// Listen on loopback port zero and retain the socket: allocation itself proves
// the port is free, without a bind-close-rebind race. No default listener helper,
// wildcard address, STUN/TURN, host reconfiguration or fixed ports are used.
func loopbackSettings(t *testing.T, j *journal, name, side string) webrtc.SettingEngine {
	t.Helper()
	udp, err := net.ListenPacket("udp4", "127.0.0.1:0")
	check(t, err)
	port := udp.LocalAddr().(*net.UDPAddr).Port
	if port == 7880 || port == 7881 {
		check(t, udp.Close())
		t.Fatal("excluded port allocated")
	}
	j.write(name, side+"_socket", udp.LocalAddr().String())
	mux := ice.NewUDPMuxDefault(ice.UDPMuxParams{UDPConn: udp})
	t.Cleanup(func() {
		check(t, mux.Close())
		// Closing the mux must close its owned UDP socket.
		_, err := udp.WriteTo([]byte{0}, udp.LocalAddr())
		if err == nil {
			t.Error("mux left UDP socket open")
		}
		j.write(name, side+"_socket_closed", err != nil)
	})
	var se webrtc.SettingEngine
	se.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4})
	// Pion ICE defaults zero to query-only mDNS, which opens a multicast
	// socket even with an IP filter. Disable it explicitly on BOTH endpoints.
	se.SetICEMulticastDNSMode(ice.MulticastDNSModeDisabled)
	j.write(name, side+"_mdns_disabled", true)
	se.SetIncludeLoopbackCandidate(true)
	se.SetIPFilter(func(ip net.IP) bool { return ip.Equal(net.IPv4(127, 0, 0, 1)) })
	se.SetICEUDPMux(mux)
	return se
}

func await[T any](t *testing.T, channel <-chan T, what string) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-time.After(10 * time.Second):
		t.Fatalf("deadline waiting for %s", what)
		var zero T
		return zero
	}
}

func observe(t *testing.T, f fixture, j *journal) observation {
	t.Helper()
	j.write(f.Case, "input", f.Input)
	var req livekit.AddTrackRequest
	check(t, protojson.Unmarshal(f.Input.Request, &req))
	original := proto.Clone(&req)
	if len(f.Input.Rids) != 0 || f.Input.Simulcast {
		t.Fatal("only declared-SSRC/no-RID stimuli are implemented")
	}

	// Register exactly VP8 and fix SSRC through public RTPSender.Send before
	// creating the offer. No SDP text rewriting or upstream internals are used.
	peerSE := loopbackSettings(t, j, f.Case, "peer")
	me := &webrtc.MediaEngine{}
	capability := webrtc.RTPCodecCapability{MimeType: f.Input.PeerCodec.Mime, ClockRate: f.Input.PeerCodec.ClockRate}
	check(t, me.RegisterCodec(webrtc.RTPCodecParameters{RTPCodecCapability: capability,
		PayloadType: webrtc.PayloadType(f.Input.PeerCodec.PayloadType)}, webrtc.RTPCodecTypeVideo))
	peer, err := webrtc.NewAPI(webrtc.WithSettingEngine(peerSE), webrtc.WithMediaEngine(me)).NewPeerConnection(webrtc.Configuration{})
	check(t, err)
	peerClosed := make(chan struct{}, 1)
	t.Cleanup(func() {
		check(t, peer.Close())
		await(t, peerClosed, "peer closed callback")
		j.write(f.Case, "peer_closed", true)
	})
	connected := make(chan struct{}, 1)
	peer.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		j.write(f.Case, "peer_connection_state", state.String())
		if state == webrtc.PeerConnectionStateClosed {
			peerClosed <- struct{}{}
		}
		if state == webrtc.PeerConnectionStateConnected {
			select {
			case connected <- struct{}{}:
			default:
			}
		}
	})
	local, err := webrtc.NewTrackLocalStaticRTP(capability, req.Cid, req.Stream)
	check(t, err)
	transceiver, err := peer.AddTransceiverFromTrack(local, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionSendonly})
	check(t, err)
	parameters := transceiver.Sender().GetParameters()
	if len(parameters.Encodings) != 1 {
		t.Fatal("expected exactly one sender encoding")
	}
	parameters.Encodings[0].SSRC = webrtc.SSRC(f.Input.SSRC)
	parameters.Encodings[0].RTX.SSRC = 0
	parameters.Encodings[0].FEC.SSRC = 0
	check(t, transceiver.Sender().Send(parameters))

	conf := &rtc.WebRTCConfig{}
	conf.SettingEngine = loopbackSettings(t, j, f.Case, "participant")
	conf.Receiver.PacketBufferSizeVideo = 500
	conf.Receiver.PacketBufferSizeAudio = 200
	conf.SetBufferFactory(buffer.NewFactoryOfBufferFactory(500, 200).CreateBufferFactory())
	conf.UpdatePublisherConfig(false)
	grants := &auth.ClaimGrants{Video: &auth.VideoGrant{}}
	grants.Video.SetCanPublish(true)
	grants.Video.SetCanSubscribe(false)
	grants.Video.SetCanPublishData(false)
	var enabled []*livekit.Codec
	for _, c := range f.Input.EnabledCodecs {
		enabled = append(enabled, &livekit.Codec{Mime: c.Mime, FmtpLine: c.Fmtp})
	}
	answers := make(chan *livekit.SessionDescription, 8)
	acks := make(chan *livekit.TrackPublishedResponse, 8)
	candidates := make(chan *livekit.TrickleRequest, 64)
	sink := &routingfakes.FakeMessageSink{}
	sink.WriteMessageCalls(func(message proto.Message) error {
		j.write(f.Case, "signal_response", message)
		if signal, ok := message.(*livekit.SignalResponse); ok {
			if answer := signal.GetAnswer(); answer != nil {
				answers <- proto.Clone(answer).(*livekit.SessionDescription)
			}
			if ack := signal.GetTrackPublished(); ack != nil {
				acks <- proto.Clone(ack).(*livekit.TrackPublishedResponse)
			}
			if trickle := signal.GetTrickle(); trickle != nil {
				candidates <- proto.Clone(trickle).(*livekit.TrickleRequest)
			}
		}
		return nil
	})
	type published struct {
		track        types.MediaTrack
		snapshot     *livekit.TrackInfo
		writesAtCall int32
	}
	publications := make(chan published, 8)
	var publicationCount, writes atomic.Int32
	listener := &typesfakes.FakeLocalParticipantListener{}
	listener.OnTrackPublishedCalls(func(_ types.Participant, track types.MediaTrack) {
		// This is the actual public listener, not later participant batching or
		// telemetry. ToProto and clone both execute before this callback returns.
		snapshot := proto.Clone(track.ToProto()).(*livekit.TrackInfo)
		n := writes.Load()
		publicationCount.Add(1)
		j.write(f.Case, "on_track_published_track", snapshot)
		j.write(f.Case, "on_track_published_rtp_writes", n)
		publications <- published{track, snapshot, n}
	})
	p, err := rtc.NewParticipant(rtc.ParticipantParams{
		SID: "PA_c63", Identity: "c63", Config: conf, Sink: sink,
		ProtocolVersion: types.ProtocolVersion(f.Input.ProtocolVersion), SessionStartTime: time.Now(),
		Grants: grants, PublishEnabledCodecs: enabled, SubscribeEnabledCodecs: enabled,
		ClientInfo: rtc.ClientInfo{ClientInfo: &livekit.ClientInfo{Browser: f.Input.Browser}},
		Logger:     logger.GetLogger(), Reporter: roomobs.NewNoopParticipantSessionReporter(),
		VersionGenerator: utils.NewDefaultTimedVersionGenerator(), DisableSupervisor: true,
		ParticipantHelper: &typesfakes.FakeLocalParticipantHelper{}, ParticipantListener: listener,
		TelemetryListener: &typesfakes.FakeParticipantTelemetryListener{}, FireOnTrackBySdp: f.Input.FireOnTrackBySDP,
	})
	check(t, err)
	t.Cleanup(func() {
		check(t, p.Close(false, types.ParticipantCloseReasonClientRequestLeave, false))
		j.write(f.Case, "participant_closed", p.IsClosed())
	})
	check(t, p.SendJoinResponse(&livekit.JoinResponse{}))
	if !p.IsReady() {
		t.Fatal("public join did not open signalling")
	}
	// Normal non-migrating participant setup, via the public lifecycle API.
	p.SetMigrateState(types.MigrateStateComplete)
	p.AddTrack(&req)
	ack := await(t, acks, "AddTrack ACK")
	if ack.Cid != req.Cid || ack.Track == nil || ack.Track.Sid == "" {
		t.Fatal("incomplete AddTrack ACK")
	}

	offer, err := peer.CreateOffer(nil)
	check(t, err)
	gathered := webrtc.GatheringCompletePromise(peer)
	check(t, peer.SetLocalDescription(offer))
	await(t, gathered, "loopback peer gathering")
	offer = *peer.LocalDescription()
	j.write(f.Case, "offer", offer)
	parsed, err := offer.Unmarshal()
	check(t, err)
	if len(parsed.MediaDescriptions) != 1 {
		t.Fatal("expected one video media section")
	}
	media := parsed.MediaDescriptions[0]
	mid, _ := media.Attribute("mid")
	if media.MediaName.Media != "video" || mid != f.Input.Mid {
		t.Fatal("unexpected media/MID")
	}
	var ssrcFound, msidFound, candidateFound bool
	for _, attr := range media.Attributes {
		switch attr.Key {
		case "rid", "simulcast":
			t.Fatal("peer unexpectedly declared RID/simulcast")
		case "ssrc":
			if !strings.HasPrefix(attr.Value, fmt.Sprintf("%d ", f.Input.SSRC)) {
				t.Fatal("unexpected SDP SSRC")
			}
			ssrcFound = true
		case "msid":
			msidFound = attr.Value == req.Stream+" "+req.Cid
		case "candidate":
			candidate, err := ice.UnmarshalCandidate(attr.Value)
			check(t, err)
			if candidate.Address() != "127.0.0.1" {
				t.Fatal("non-loopback candidate")
			}
			candidateFound = true
		}
	}
	if !ssrcFound || !msidFound || !candidateFound {
		t.Fatal("missing declared SSRC, matching CID or loopback candidate")
	}
	check(t, p.HandleOffer(&livekit.SessionDescription{Type: "offer", Sdp: offer.SDP, Id: 63}))
	answer := await(t, answers, "participant answer")
	if answer.Id != 63 || answer.Type != "answer" {
		t.Fatal("unexpected answer identity")
	}
	check(t, peer.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: answer.Sdp}))
	// Participant sends normal trickle signalling; feed it to the actual peer.
	// This bounded select exits on connection, with no detached bridge process.
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
connection:
	for {
		select {
		case <-connected:
			break connection
		case candidate := <-candidates:
			if candidate.Target != livekit.SignalTarget_PUBLISHER {
				t.Fatal("unexpected subscriber candidate")
			}
			if candidate.CandidateInit == "" {
				continue
			}
			var init webrtc.ICECandidateInit
			check(t, json.Unmarshal([]byte(candidate.CandidateInit), &init))
			if init.Candidate != "" {
				c, err := ice.UnmarshalCandidate(init.Candidate)
				check(t, err)
				if c.Address() != "127.0.0.1" {
					t.Fatal("non-loopback participant candidate")
				}
			}
			check(t, peer.AddICECandidate(init))
		case <-deadline.C:
			t.Fatal("loopback DTLS/ICE connection deadline")
		}
	}
	j.write(f.Case, "connected_before_rtp", peer.ConnectionState().String())
	var publication published
	if f.Input.FireOnTrackBySDP {
		publication = await(t, publications, "publication before first RTP")
		if publication.writesAtCall != 0 {
			t.Fatal("SDP publication followed RTP")
		}
	} else {
		// Explicit quiet observation window, not a substitute for media receipt.
		select {
		case <-publications:
			t.Fatal("RTP-deferred publication occurred before RTP")
		case <-time.After(100 * time.Millisecond):
			j.write(f.Case, "no_publication_before_rtp", true)
		}
	}

	payload, err := hex.DecodeString(f.Input.RTP.PayloadHex)
	check(t, err)
	ticker := time.NewTicker(time.Duration(f.Input.RTP.IntervalMS) * time.Millisecond)
	defer ticker.Stop()
	for i := 0; i < f.Input.RTP.Count; i++ {
		<-ticker.C
		packet := &rtp.Packet{Header: rtp.Header{Version: 2, Marker: f.Input.RTP.Marker,
			PayloadType: f.Input.PeerCodec.PayloadType, SequenceNumber: f.Input.RTP.SequenceStart + uint16(i),
			Timestamp: f.Input.RTP.Timestamp + uint32(i)*f.Input.RTP.TimestampStep, SSRC: f.Input.SSRC}, Payload: payload}
		raw, err := packet.Marshal()
		check(t, err)
		j.write(f.Case, "rtp_packet_hex", hex.EncodeToString(raw))
		writes.Add(1)
		check(t, local.WriteRTP(packet))
	}
	if !f.Input.FireOnTrackBySDP {
		publication = await(t, publications, "publication after real RTP")
		if publication.writesAtCall == 0 {
			t.Fatal("RTP-deferred callback has no media stimulus")
		}
	}
	// The listener exposes MediaTrack; receiver stats are on its public
	// LocalMediaTrack extension (types/interfaces.go), not a private RTC method.
	localTrack, ok := publication.track.(types.LocalMediaTrack)
	if !ok {
		t.Fatal("publication does not implement public LocalMediaTrack")
	}
	// Public receiver stats prove actual RTP ingestion, not just successful writes.
	statsDeadline := time.NewTimer(5 * time.Second)
	defer statsDeadline.Stop()
	for {
		stats := localTrack.GetTrackStats()
		if stats != nil && stats.Packets >= uint32(f.Input.RTP.Count) {
			j.write(f.Case, "received_rtp_stats", stats)
			break
		}
		select {
		case <-ticker.C:
		case <-statsDeadline.C:
			j.write(f.Case, "received_rtp_stats_deadline", stats)
			t.Fatal("receiver did not ingest all RTP packets")
		}
	}
	settled := publication.track.ToProto()
	j.write(f.Case, "settled_track", settled)
	if !proto.Equal(original, &req) {
		t.Fatal("public methods mutated AddTrack request")
	}
	if publicationCount.Load() != 1 || len(p.GetPublishedTracks()) != 1 {
		t.Fatal("expected one real publication")
	}
	j.write(f.Case, "request_immutable", true)
	return observation{f.SchemaVersion, f.Case, f.Input, map[string]any{
		"ack": projection(t, ack.Track), "callback": projection(t, publication.snapshot),
		"settled": projection(t, settled), "callback_before_rtp": publication.writesAtCall == 0,
		"publication_count": publicationCount.Load(),
	}}
}

func TestObserve(t *testing.T) {
	path := os.Getenv("VIDEO_PUBLICATION_METADATA_GO_JSONL")
	if !filepath.IsAbs(path) {
		t.Fatal("absolute VIDEO_PUBLICATION_METADATA_GO_JSONL required")
	}
	out, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	check(t, err)
	defer func() { check(t, out.Close()) }()
	events, err := os.OpenFile(path+".events", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	check(t, err)
	j := &journal{file: events}
	defer func() {
		j.mu.Lock()
		defer j.mu.Unlock()
		check(t, events.Close())
		check(t, j.err)
	}()
	cwd, err := os.Getwd()
	check(t, err)
	raw, err := os.ReadFile(filepath.Join(cwd, "cases.json"))
	check(t, err)
	var cases []fixture
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	check(t, decoder.Decode(&cases))
	if len(cases) == 0 {
		t.Fatal("no cases")
	}
	for _, f := range cases {
		t.Run(f.Case, func(t *testing.T) {
			row := observe(t, f, j)
			check(t, json.NewEncoder(out).Encode(row))
			check(t, out.Sync())
		})
	}
	after, err := os.ReadFile(filepath.Join(cwd, "cases.json"))
	check(t, err)
	if !bytes.Equal(raw, after) {
		t.Fatal("input fixture changed")
	}
}
