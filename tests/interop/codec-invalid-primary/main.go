package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/livekit/protocol/livekit"
	sdk "github.com/livekit/server-sdk-go/v2"
	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
)

type wire struct {
	SSRC      uint32
	PT        uint8
	Sequence  uint16
	Timestamp uint32
}

func packet(h *rtp.Header) wire { return wire{h.SSRC, h.PayloadType, h.SequenceNumber, h.Timestamp} }

type mutation struct {
	interceptor.NoOp
	pt                  atomic.Uint32
	payload             []byte
	mu                  sync.Mutex
	before, first, last wire
	count               int
}

func (m *mutation) NewInterceptor(string) (interceptor.Interceptor, error) { return m, nil }
func (m *mutation) BindLocalStream(info *interceptor.StreamInfo, next interceptor.RTPWriter) interceptor.RTPWriter {
	if !strings.EqualFold(info.MimeType, webrtc.MimeTypeVP9) {
		return next
	}
	return interceptor.RTPWriterFunc(func(h *rtp.Header, p []byte, a interceptor.Attributes) (int, error) {
		m.mu.Lock()
		defer m.mu.Unlock()
		if pt := m.pt.Load(); pt != 0 {
			clone := h.Clone()
			clone.PayloadType = uint8(pt)
			n, err := next.Write(&clone, m.payload, a)
			if err == nil {
				if m.count == 0 {
					m.first = packet(&clone)
				}
				m.last = packet(&clone)
				m.count++
			}
			return n, err
		}
		m.before = packet(h)
		return next.Write(h, p, a)
	})
}

type snapshot struct {
	VP8, VP9, Tracks int
	InvalidPayloads  int
	SSRC             uint32
	OtherSSRC        bool
	SID              string
}
type inbound struct {
	sync.Mutex
	row    snapshot
	backup []byte
}

func (i *inbound) read(t *webrtc.TrackRemote, p *sdk.RemoteTrackPublication, _ *sdk.RemoteParticipant) {
	i.Lock()
	i.row.Tracks++
	i.row.SID = p.SID()
	i.Unlock()
	go func() {
		for {
			r, _, err := t.ReadRTP()
			if err != nil {
				return
			}
			if len(r.Payload) == 0 {
				continue
			}
			i.Lock()
			switch strings.ToLower(t.Codec().MimeType) {
			case "video/vp8":
				parsed := &codecs.VP8Packet{}
				b, parseErr := parsed.Unmarshal(r.Payload)
				if parseErr == nil && bytes.Equal(b, i.backup) && r.Marker {
					i.row.VP8++
				} else {
					i.row.InvalidPayloads++
				}
			case "video/vp9":
				i.row.VP9++
			}
			if i.row.SSRC != 0 && i.row.SSRC != r.SSRC {
				i.row.OtherSSRC = true
			}
			i.row.SSRC = r.SSRC
			i.Unlock()
		}
	}()
}
func (i *inbound) snapshot() snapshot { i.Lock(); defer i.Unlock(); return i.row }
func frame(path string) ([]byte, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(b) < 45 || len(b) > 1044 || string(b[:4]) != "DKIF" || int(binary.LittleEndian.Uint32(b[32:36])) != len(b)-44 {
		return nil, fmt.Errorf("expected one complete IVF frame smaller than an RTP packet in %s", path)
	}
	return b[44:], nil
}
func wait(ctx context.Context, label string, ready func() bool) error {
	t := time.NewTicker(30 * time.Millisecond)
	defer t.Stop()
	for !ready() {
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: %w", label, ctx.Err())
		case <-t.C:
		}
	}
	return nil
}
func run(url, vp8File, vp9File, injectedFile, mode string, single bool, e map[string]any) error {
	ctx, cancel := context.WithTimeout(context.Background(), 22*time.Second)
	defer cancel()
	vp8, err := frame(vp8File)
	if err != nil {
		return err
	}
	vp9, err := frame(vp9File)
	if err != nil {
		return err
	}
	injected, err := frame(injectedFile)
	if err != nil {
		return err
	}
	if bytes.Equal(vp8, injected) {
		return fmt.Errorf("injected and backup frames must differ")
	}
	fault := &mutation{payload: (&codecs.VP8Payloader{}).Payload(1100, injected)[0]}
	first, backup := &inbound{backup: vp8}, &inbound{backup: vp8}
	defer func() {
		fault.mu.Lock()
		defer fault.mu.Unlock()
		e["wire"] = map[string]any{"before": fault.before, "first": fault.first, "last": fault.last, "count": fault.count}
		e["after"] = first.snapshot()
		e["backup_after"] = backup.snapshot()
	}()
	api := sdk.NewRoomServiceClient(url, "devkey", "secret")
	name := fmt.Sprintf("invalid-primary-%d", time.Now().UnixNano())
	if _, err = api.CreateRoom(ctx, &livekit.CreateRoomRequest{Name: name, EmptyTimeout: 30}); err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, _ = api.DeleteRoom(cleanup, &livekit.DeleteRoomRequest{Room: name})
	}()
	connect := func(identity string, in *inbound, opts ...sdk.ConnectOption) (*sdk.Room, error) {
		callback := &sdk.RoomCallback{}
		if in != nil {
			callback.OnTrackSubscribed = in.read
		}
		// The VP8-only observer uses a dual-PC join. The fixed SDK's
		// single-PC initial offer retries with a video-only codec list.
		if single && identity != "backup" {
			opts = append(opts, sdk.WithSinglePeerConnection())
		}
		return sdk.ConnectToRoom(url, sdk.ConnectInfo{APIKey: "devkey", APISecret: "secret", RoomName: name, ParticipantIdentity: identity}, callback, opts...)
	}
	pub, err := connect("publisher", nil, sdk.WithInterceptors([]interceptor.Factory{fault}))
	if err != nil {
		return err
	}
	defer pub.Disconnect()
	sub, err := connect("primary", first)
	if err != nil {
		return err
	}
	defer sub.Disconnect()
	mainTrack, err := sdk.NewLocalTrack(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP9, ClockRate: 90000, SDPFmtpLine: "profile-id=0"})
	if err != nil {
		return err
	}
	backupTrack, err := sdk.NewLocalTrack(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000})
	if err != nil {
		return err
	}
	policy := livekit.BackupCodecPolicy_PREFER_REGRESSION
	if mode == "simulcast" {
		policy = livekit.BackupCodecPolicy_SIMULCAST
	}
	if mode == "unknown" {
		policy = livekit.BackupCodecPolicy(99)
	}
	track, err := pub.LocalParticipant.PublishTrack(mainTrack, &sdk.TrackPublicationOptions{
		Name: "invalid-primary", VideoWidth: 160, VideoHeight: 90, BackupCodecPolicy: policy,
	}, sdk.WithBackupCodec(backupTrack))
	if err != nil {
		return err
	}
	e["sid"] = track.SID()
	e["publication"] = track.TrackInfo()
	stream, stop := context.WithCancel(ctx)
	defer stop()
	writes := make(chan error, 2)
	send := func(t *sdk.LocalTrack, b []byte) {
		clock := time.NewTicker(50 * time.Millisecond)
		defer clock.Stop()
		for {
			select {
			case <-stream.Done():
				return
			case <-clock.C:
				if err := t.WriteSample(media.Sample{Data: b, Duration: 50 * time.Millisecond}, nil); err != nil {
					writes <- err
					return
				}
			}
		}
	}
	go send(mainTrack, vp9)
	go send(backupTrack, vp8)
	if err := wait(ctx, "primary VP9 before backup", func() bool { return first.snapshot().VP9 >= 8 }); err != nil {
		return err
	}
	second, err := connect("backup", backup, sdk.WithCodecs([]livekit.Codec{{Mime: "video/VP8"}}))
	if err != nil {
		return err
	}
	defer second.Disconnect()
	if err := wait(ctx, "backup VP8 before fault", func() bool { return backup.snapshot().VP8 >= 8 }); err != nil {
		return err
	}
	before, backupBefore := first.snapshot(), backup.snapshot()
	e["before"] = before
	e["backup_before"] = backupBefore
	if before.VP8 != 0 || before.Tracks != 1 || before.SID != track.SID() || before.SSRC == 0 || before.OtherSSRC ||
		backupBefore.VP9 != 0 || backupBefore.Tracks != 1 || backupBefore.SID != track.SID() || backupBefore.SSRC == 0 || backupBefore.OtherSSRC {
		return fmt.Errorf("expected one VP9 primary and one VP8 backup under the same SID before the fault")
	}
	pc := pub.LocalParticipant.GetPublisherPeerConnection()
	if pc == nil {
		return fmt.Errorf("publisher peer connection unavailable")
	}
	for _, s := range pc.GetSenders() {
		if s.Track() != mainTrack {
			continue
		}
		for _, c := range s.GetParameters().Codecs {
			if strings.EqualFold(c.MimeType, webrtc.MimeTypeVP8) {
				fault.pt.Store(uint32(c.PayloadType))
				break
			}
		}
	}
	if fault.pt.Load() == 0 {
		return fmt.Errorf("VP8 must be negotiated on the VP9 m-line")
	}
	if mode == "simulcast" {
		if err := wait(ctx, "backup active after fault", func() bool { return backup.snapshot().VP8 >= backupBefore.VP8+30 }); err != nil {
			return err
		}
	} else {
		if err := wait(ctx, "primary subscriber migrates to VP8", func() bool { return first.snapshot().VP8 >= 8 }); err != nil {
			return err
		}
	}
	time.Sleep(450 * time.Millisecond)
	after, backupAfter := first.snapshot(), backup.snapshot()
	if after.SID != before.SID || after.InvalidPayloads != 0 || backupAfter.InvalidPayloads != 0 {
		return fmt.Errorf("fallback must preserve SID and cannot forward invalid primary payloads")
	}
	if mode == "simulcast" {
		if after.SSRC != before.SSRC || after.OtherSSRC || after.Tracks != before.Tracks {
			return fmt.Errorf("SIMULCAST must preserve the original subscriber binding")
		}
	} else if after.SSRC == before.SSRC || !after.OtherSSRC || after.Tracks != before.Tracks+1 {
		return fmt.Errorf("Go SDK codec fallback must negotiate one replacement sender with a fresh SSRC")
	}
	if backupAfter.VP8 < backupBefore.VP8+8 || backupAfter.VP9 != 0 || backupAfter.SID != backupBefore.SID || backupAfter.SSRC != backupBefore.SSRC || backupAfter.OtherSSRC || backupAfter.Tracks != backupBefore.Tracks {
		return fmt.Errorf("original backup subscriber must continue receiving VP8")
	}
	if after.VP9 > before.VP9+3 || (mode == "simulcast" && after.VP8 != 0) || (mode != "simulcast" && after.VP8 < 8) {
		return fmt.Errorf("unexpected original-subscriber behavior after invalid primary")
	}
	fault.mu.Lock()
	valid := fault.count > 0 && fault.before.SSRC != 0 && fault.before.SSRC == fault.first.SSRC &&
		fault.before.PT != fault.first.PT && fault.before.Sequence+1 == fault.first.Sequence
	fault.mu.Unlock()
	if !valid {
		return fmt.Errorf("fault must change PT on the same source SSRC and consecutive sequence number")
	}
	select {
	case err := <-writes:
		return err
	default:
	}
	stop()
	second.Disconnect()
	sub.Disconnect()
	pub.Disconnect()
	if _, err := api.DeleteRoom(ctx, &livekit.DeleteRoomRequest{Room: name}); err != nil {
		return err
	}
	rooms, err := api.ListRooms(ctx, &livekit.ListRoomsRequest{Names: []string{name}})
	if err != nil {
		return err
	}
	e["room_removed"] = len(rooms.Rooms) == 0
	if len(rooms.Rooms) != 0 {
		return fmt.Errorf("room survived teardown")
	}
	return nil
}

func main() {
	url := flag.String("url", "", "server URL")
	vp8 := flag.String("vp8", "", "one-frame VP8 IVF")
	vp9 := flag.String("vp9", "", "one-frame VP9 IVF")
	injected := flag.String("injected", "", "distinct one-frame VP8 IVF for the invalid primary")
	policy := flag.String("policy", "prefer", "prefer, unknown, or simulcast")
	single := flag.Bool("single", false, "use one peer connection")
	flag.Parse()
	if *policy != "prefer" && *policy != "unknown" && *policy != "simulcast" {
		fmt.Fprintln(os.Stderr, "unsupported policy")
		os.Exit(2)
	}
	receipt := map[string]any{"policy": *policy, "single": *single, "status": "FAIL"}
	err := run(*url, *vp8, *vp9, *injected, *policy, *single, receipt)
	if err != nil {
		receipt["error"] = err.Error()
	} else {
		receipt["status"] = "PASS"
	}
	out := json.NewEncoder(os.Stdout)
	out.SetIndent("", "  ")
	if err := out.Encode(receipt); err != nil {
		panic(err)
	}
	if err != nil {
		os.Exit(1)
	}
}
