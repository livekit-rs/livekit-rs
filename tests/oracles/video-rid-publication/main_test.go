package observer_test

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
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
	"github.com/pion/sdp/v3"
	"github.com/pion/webrtc/v4"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func TestMain(m *testing.M) {
	if err := lkprom.Init("c65-observer", livekit.NodeType_SERVER); err != nil {
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
	SSRC              uint32   `json:"sdp_ssrc"`
	IdleSSRC          uint32   `json:"idle_ssrc"`
	Mid               string   `json:"sdp_mid"`
	Rids              []string `json:"sdp_rids"`
	Simulcast         bool     `json:"sdp_simulcast"`
	QuietMS           int      `json:"quiet_ms"`
	CallbackTimeoutMS int      `json:"callback_timeout_ms"`
	StatsTimeoutMS    int      `json:"stats_timeout_ms"`
	SettledMS         int      `json:"settled_ms"`
	RTP               struct {
		Count         int    `json:"count"`
		SequenceStart uint16 `json:"sequence_start"`
		Timestamp     uint32 `json:"timestamp_start"`
		Marker        bool   `json:"marker"`
		PayloadHex    string `json:"payload_hex"`
		PaddingSize   uint8  `json:"padding_size,omitempty"`
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

func mediaContract(t *testing.T, media *sdp.MediaDescription, direction string) map[string]uint8 {
	t.Helper()
	mid, _ := media.Attribute("mid")
	if media.MediaName.Media != "video" || mid != "0" || strings.Join(media.MediaName.Formats, " ") != "96" {
		t.Fatal("unexpected media/MID/PT")
	}
	extensions := map[string]uint8{}
	ids := map[uint8]bool{}
	var rids []string
	var simulcast, codec int
	for _, attr := range media.Attributes {
		switch attr.Key {
		case "rid":
			rids = append(rids, attr.Value)
		case "simulcast":
			if attr.Value != direction+" q;f" {
				t.Fatal("unexpected simulcast declaration")
			}
			simulcast++
		case "rtpmap":
			if attr.Value != "96 VP8/90000" {
				t.Fatal("unexpected codec")
			}
			codec++
		case "extmap":
			fields := strings.Fields(attr.Value)
			if len(fields) < 2 {
				t.Fatal("invalid extmap")
			}
			id, err := strconv.Atoi(strings.Split(fields[0], "/")[0])
			check(t, err)
			if fields[1] == sdp.SDESMidURI || fields[1] == sdp.SDESRTPStreamIDURI {
				if id < 1 || id > 14 || ids[uint8(id)] || extensions[fields[1]] != 0 {
					t.Fatal("invalid/duplicate extension")
				}
				if strings.Contains(fields[0], "/") && fields[0] != strconv.Itoa(id)+"/"+direction+"only" && fields[0] != strconv.Itoa(id)+"/sendrecv" {
					t.Fatal("incompatible extmap direction")
				}
				extensions[fields[1]] = uint8(id)
				ids[uint8(id)] = true
			}
		}
	}
	if strings.Join(rids, ";") != "q "+direction+";f "+direction || simulcast != 1 || codec != 1 || len(extensions) != 2 {
		t.Fatal("missing exact RID/codec/header-extension contract")
	}
	return extensions
}

func observe(t *testing.T, f fixture, j *journal) observation {
	t.Helper()
	j.write(f.Case, "input", f.Input)
	var req livekit.AddTrackRequest
	check(t, protojson.Unmarshal(f.Input.Request, &req))
	original := proto.Clone(&req)
	paddingOnly := strings.HasPrefix(f.Case, "c66-")
	if paddingOnly {
		if f.Input.RTP.PaddingSize != 1 || f.Input.RTP.PayloadHex != "" {
			t.Fatal("C66 requires exactly one padding byte and empty payload")
		}
	} else if f.Input.RTP.PaddingSize != 0 || f.Input.RTP.PayloadHex != "1001000000" {
		t.Fatal("unsupported C65 payload/padding stimulus")
	}
	if f.Input.FireOnTrackBySDP || !f.Input.Simulcast || strings.Join(f.Input.Rids, ";") != "q;f" ||
		f.Input.SSRC != 424242 || f.Input.IdleSSRC != 434343 || f.Input.Mid != "0" ||
		req.Cid != "c65-video" || req.Stream != "c65-stream" ||
		f.Input.PeerCodec.Mime != "video/VP8" || f.Input.PeerCodec.ClockRate != 90000 || f.Input.PeerCodec.PayloadType != 96 ||
		f.Input.RTP.Count != 1 || f.Input.RTP.SequenceStart != 1000 || f.Input.RTP.Timestamp != 90000 ||
		f.Input.QuietMS != 100 || f.Input.SettledMS != 100 ||
		f.Input.CallbackTimeoutMS != 5000 || f.Input.StatsTimeoutMS != 5000 {
		t.Fatal("unsupported C65/C66 stimulus")
	}

	peerSE := loopbackSettings(t, j, f.Case, "peer")
	me := &webrtc.MediaEngine{}
	capability := webrtc.RTPCodecCapability{MimeType: f.Input.PeerCodec.Mime, ClockRate: f.Input.PeerCodec.ClockRate}
	check(t, me.RegisterCodec(webrtc.RTPCodecParameters{RTPCodecCapability: capability,
		PayloadType: webrtc.PayloadType(f.Input.PeerCodec.PayloadType)}, webrtc.RTPCodecTypeVideo))
	for _, uri := range []string{sdp.SDESMidURI, sdp.SDESRTPStreamIDURI} {
		check(t, me.RegisterHeaderExtension(webrtc.RTPHeaderExtensionCapability{URI: uri}, webrtc.RTPCodecTypeVideo))
	}
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
	local, err := webrtc.NewTrackLocalStaticRTP(capability, req.Cid, req.Stream, webrtc.WithRTPStreamID("q"))
	check(t, err)
	idle, err := webrtc.NewTrackLocalStaticRTP(capability, req.Cid, req.Stream, webrtc.WithRTPStreamID("f"))
	check(t, err)
	transceiver, err := peer.AddTransceiverFromTrack(local, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionSendonly})
	check(t, err)
	check(t, transceiver.Sender().AddEncoding(idle))
	parameters := transceiver.Sender().GetParameters()
	if len(parameters.Encodings) != 2 {
		t.Fatal("expected exactly two sender encodings")
	}
	for i, ssrc := range []uint32{f.Input.SSRC, f.Input.IdleSSRC} {
		if parameters.Encodings[i].RID != f.Input.Rids[i] {
			t.Fatal("sender RID mismatch")
		}
		parameters.Encodings[i].SSRC = webrtc.SSRC(ssrc)
		parameters.Encodings[i].RTX.SSRC = 0
		parameters.Encodings[i].FEC.SSRC = 0
	}
	check(t, transceiver.Sender().Send(parameters))
	j.write(f.Case, "sender_parameters", transceiver.Sender().GetParameters())

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
	var publicationCount, writes, successfulWrites atomic.Int32
	listener := &typesfakes.FakeLocalParticipantListener{}
	listener.OnTrackPublishedCalls(func(_ types.Participant, track types.MediaTrack) {
		// This is the actual public listener, not later participant batching or
		// telemetry. ToProto and clone both execute before this callback returns.
		snapshot := proto.Clone(track.ToProto()).(*livekit.TrackInfo)
		n := writes.Load()
		publicationCount.Add(1)
		j.write(f.Case, "on_track_published_track", snapshot)
		j.write(f.Case, "on_track_published_rtp_writes", n)
		select {
		case publications <- published{track, snapshot, n}:
		default:
			j.write(f.Case, "publication_channel_overflow", true)
		}
	})
	p, err := rtc.NewParticipant(rtc.ParticipantParams{
		SID: "PA_c65", Identity: "c65", Config: conf, Sink: sink,
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
		j.write(f.Case, "final_counts", map[string]any{"publications": publicationCount.Load(),
			"published_tracks": len(p.GetPublishedTracks()), "rtp_writes": writes.Load(),
			"rtp_write_successes": successfulWrites.Load(), "idle_rtp_writes": 0})
		j.write(f.Case, "final_request_immutable", proto.Equal(original, &req))
		check(t, p.Close(false, types.ParticipantCloseReasonClientRequestLeave, false))
		j.write(f.Case, "participant_closed", p.IsClosed())
		if !p.IsClosed() {
			t.Error("participant remained open")
		}
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
	ssrcs := map[uint32]bool{}
	var msidFound, candidateFound bool
	for _, attr := range media.Attributes {
		switch attr.Key {
		case "ssrc-group":
			t.Fatal("unexpected repair/group SSRC")
		case "ssrc":
			fields := strings.Fields(attr.Value)
			if len(fields) < 2 {
				t.Fatal("invalid SDP SSRC")
			}
			ssrc, err := strconv.ParseUint(fields[0], 10, 32)
			check(t, err)
			if uint32(ssrc) != f.Input.SSRC && uint32(ssrc) != f.Input.IdleSSRC {
				t.Fatal("unexpected SDP SSRC")
			}
			ssrcs[uint32(ssrc)] = true
		case "msid":
			if attr.Value != req.Stream+" "+req.Cid {
				t.Fatal("unexpected CID/stream")
			}
			msidFound = true
		case "candidate":
			candidate, err := ice.UnmarshalCandidate(attr.Value)
			check(t, err)
			if candidate.Address() != "127.0.0.1" {
				t.Fatal("non-loopback candidate")
			}
			candidateFound = true
		}
	}
	if len(ssrcs) != 2 || !msidFound || !candidateFound {
		t.Fatal("missing declared SSRCs, matching CID or loopback candidate")
	}
	offerExtensions := mediaContract(t, media, "send")
	check(t, p.HandleOffer(&livekit.SessionDescription{Type: "offer", Sdp: offer.SDP, Id: 65}))
	answer := await(t, answers, "participant answer")
	j.write(f.Case, "answer", answer)
	if answer.Id != 65 || answer.Type != "answer" {
		t.Fatal("unexpected answer identity")
	}
	answerDescription := webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: answer.Sdp}
	answerParsed, err := answerDescription.Unmarshal()
	check(t, err)
	if len(answerParsed.MediaDescriptions) != 1 {
		t.Fatal("unexpected answer media count")
	}
	answerExtensions := mediaContract(t, answerParsed.MediaDescriptions[0], "recv")
	check(t, peer.SetRemoteDescription(answerDescription))
	extensions := map[string]uint8{}
	for _, ext := range transceiver.Sender().GetParameters().HeaderExtensions {
		if ext.URI == sdp.SDESMidURI || ext.URI == sdp.SDESRTPStreamIDURI {
			if ext.ID < 1 || ext.ID > 14 || uint8(ext.ID) != answerExtensions[ext.URI] || uint8(ext.ID) != offerExtensions[ext.URI] {
				t.Fatal("negotiated extension mismatch")
			}
			extensions[ext.URI] = uint8(ext.ID)
		}
	}
	if len(extensions) != 2 {
		t.Fatal("missing negotiated MID/RID extensions")
	}
	j.write(f.Case, "negotiated_extensions", extensions)
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
	quietStart := time.Now()
	j.write(f.Case, "quiet_start", true)
	<-time.After(time.Duration(f.Input.QuietMS) * time.Millisecond)
	quietCount, quietTracks := publicationCount.Load(), len(p.GetPublishedTracks())
	j.write(f.Case, "quiet_counts", map[string]any{"publications": quietCount, "published_tracks": quietTracks,
		"rtp_writes": writes.Load(), "elapsed_ns": time.Since(quietStart).Nanoseconds()})
	if quietCount != 0 || quietTracks != 0 || writes.Load() != 0 {
		t.Fatal("publication occurred during connected no-packet window")
	}

	payload, err := hex.DecodeString(f.Input.RTP.PayloadHex)
	check(t, err)
	packet := &rtp.Packet{Header: rtp.Header{Version: 2, Marker: f.Input.RTP.Marker,
		Padding: paddingOnly, PaddingSize: f.Input.RTP.PaddingSize,
		PayloadType: f.Input.PeerCodec.PayloadType, SequenceNumber: f.Input.RTP.SequenceStart,
		Timestamp: f.Input.RTP.Timestamp, SSRC: f.Input.SSRC}, Payload: payload}
	check(t, packet.SetExtension(extensions[sdp.SDESMidURI], []byte(f.Input.Mid)))
	check(t, packet.SetExtension(extensions[sdp.SDESRTPStreamIDURI], []byte(local.RID())))
	raw, err := packet.Marshal()
	check(t, err)
	j.write(f.Case, "rtp_packet_hex", hex.EncodeToString(raw))
	writes.Add(1)
	err = local.WriteRTP(packet)
	j.write(f.Case, "rtp_write_result", map[string]any{"success": err == nil, "error": fmt.Sprint(err)})
	check(t, err)
	successfulWrites.Add(1)

	var publication published
	select {
	case publication = <-publications:
	case <-time.After(time.Duration(f.Input.CallbackTimeoutMS) * time.Millisecond):
		j.write(f.Case, "publication_deadline", map[string]any{"publications": publicationCount.Load(),
			"published_tracks": len(p.GetPublishedTracks()), "rtp_writes": writes.Load(), "peer_state": peer.ConnectionState().String()})
		for _, track := range p.GetPublishedTracks() {
			j.write(f.Case, "deadline_track", track.ToProto())
			if localTrack, ok := track.(types.LocalMediaTrack); ok {
				j.write(f.Case, "deadline_track_stats", localTrack.GetTrackStats())
			}
		}
		t.Fatal("one real RID packet did not trigger listener within deadline; no extra packet sent")
	}
	if publication.writesAtCall != 1 {
		t.Fatal("callback did not follow exactly one RTP write")
	}
	localTrack, ok := publication.track.(types.LocalMediaTrack)
	if !ok {
		t.Fatal("publication does not implement public LocalMediaTrack")
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	if paddingOnly {
		// RID discovery precedes the buffer's pre-start padding filter.
		start := time.Now()
		duration := time.Duration(f.Input.SettledMS) * time.Millisecond
		settleDeadline := time.NewTimer(duration)
		defer settleDeadline.Stop()
		samples := 0
		for {
			stats := localTrack.GetTrackStats()
			samples++
			if stats != nil {
				j.write(f.Case, "unexpected_padding_rtp_stats", stats)
				t.Fatal("padding-only first packet initialized public track stats")
			}
			if samples == 1 {
				// Untyped nil ensures JSON null, not an empty protobuf object.
				j.write(f.Case, "received_rtp_stats", nil)
			}
			if time.Since(start) >= duration {
				break
			}
			select {
			case <-ticker.C:
			case <-settleDeadline.C:
			}
		}
		j.write(f.Case, "padding_stats_observation", map[string]any{
			"samples": samples, "elapsed_ns": time.Since(start).Nanoseconds(), "stats": nil,
		})
	} else {
		statsDeadline := time.NewTimer(time.Duration(f.Input.StatsTimeoutMS) * time.Millisecond)
		defer statsDeadline.Stop()
		for {
			stats := localTrack.GetTrackStats()
			if stats != nil && stats.Packets > 0 {
				j.write(f.Case, "received_rtp_stats", stats)
				if stats.Packets != 1 {
					t.Fatal("receiver packet count is not one")
				}
				break
			}
			select {
			case <-ticker.C:
			case <-statsDeadline.C:
				j.write(f.Case, "received_rtp_stats_deadline", stats)
				t.Fatal("receiver did not ingest the sole RTP packet; no extra packet sent")
			}
		}
		<-time.After(time.Duration(f.Input.SettledMS) * time.Millisecond)
	}
	settled := publication.track.ToProto()
	j.write(f.Case, "settled_track", settled)
	j.write(f.Case, "settled_counts", map[string]any{"publications": publicationCount.Load(),
		"published_tracks": len(p.GetPublishedTracks()), "rtp_writes": writes.Load(), "idle_rtp_writes": 0})
	settledStats := localTrack.GetTrackStats()
	if paddingOnly {
		if settledStats != nil {
			j.write(f.Case, "unexpected_padding_rtp_stats", settledStats)
			t.Fatal("padding-only settled public track stats must remain nil")
		}
		j.write(f.Case, "settled_rtp_stats", nil)
	} else {
		j.write(f.Case, "settled_rtp_stats", settledStats)
	}
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
		"callback_rtp_writes": publication.writesAtCall, "rtp_write_attempts": writes.Load(),
		"rtp_write_successes": successfulWrites.Load(), "idle_rtp_writes": 0,
		"quiet_publication_count": quietCount, "quiet_published_track_count": quietTracks,
		"publication_count": publicationCount.Load(), "settled_published_track_count": len(p.GetPublishedTracks()),
	}}
}

func TestObserve(t *testing.T) {
	path := os.Getenv("VIDEO_RID_PUBLICATION_GO_JSONL")
	if !filepath.IsAbs(path) {
		t.Fatal("absolute VIDEO_RID_PUBLICATION_GO_JSONL required")
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
