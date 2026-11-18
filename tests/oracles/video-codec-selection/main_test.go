package observer_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"testing/synctest"
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
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func TestMain(m *testing.M) {
	// Match upstream room_test.go's package setup. Close records metrics even
	// for an unconnected participant. No metrics HTTP server is started.
	// Init's process-local CPU sampler must live outside each synctest bubble;
	// it terminates with this test binary, not a persistent test server.
	if err := lkprom.Init("c62-observer", livekit.NodeType_SERVER); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

type enabledCodec struct {
	Mime     string `json:"mime"`
	FmtpLine string `json:"fmtp_line"`
}

type input struct {
	EnabledCodecs []enabledCodec  `json:"enabled_codecs"`
	Browser       string          `json:"browser"`
	Request       json.RawMessage `json:"request"`
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

func fixturePath(t *testing.T) string {
	t.Helper()
	cwd, err := os.Getwd()
	check(t, err)
	return filepath.Join(cwd, "cases.json")
}

func shape(t *testing.T, value any, spec map[string]any) {
	t.Helper()
	if constant, ok := spec["const"]; ok && !reflect.DeepEqual(value, constant) {
		t.Fatal("schema constant mismatch")
	}
	switch spec["type"] {
	case "object":
		object, ok := value.(map[string]any)
		properties := spec["properties"].(map[string]any)
		if !ok || len(object) != len(properties) {
			t.Fatal("schema requires object with all and only declared fields")
		}
		for key, field := range properties {
			child, ok := object[key]
			if !ok {
				t.Fatalf("schema missing field %s", key)
			}
			shape(t, child, field.(map[string]any))
		}
	case "array":
		array, ok := value.([]any)
		if !ok {
			t.Fatal("schema requires non-null array")
		}
		if n, ok := spec["minItems"].(float64); ok && len(array) < int(n) {
			t.Fatal("too few cases")
		}
		if n, ok := spec["maxItems"].(float64); ok && len(array) > int(n) {
			t.Fatal("too many cases")
		}
		for _, child := range array {
			shape(t, child, spec["items"].(map[string]any))
		}
	case "string":
		if _, ok := value.(string); !ok {
			t.Fatal("schema requires string")
		}
	case "boolean":
		if _, ok := value.(bool); !ok {
			t.Fatal("schema requires boolean")
		}
	case "integer":
		n, ok := value.(float64)
		if !ok || n != math.Trunc(n) {
			t.Fatal("schema requires integer")
		}
		if min, ok := spec["minimum"].(float64); ok && n < min {
			t.Fatal("schema integer below minimum")
		}
		if max, ok := spec["maximum"].(float64); ok && n > max {
			t.Fatal("schema integer above maximum")
		}
	default:
		t.Fatal("unsupported schema type")
	}
}

func fixtures(t *testing.T) ([]fixture, []byte) {
	t.Helper()
	raw, err := os.ReadFile(fixturePath(t))
	check(t, err)
	schemaRaw, err := os.ReadFile(filepath.Join(filepath.Dir(fixturePath(t)), "schema.json"))
	check(t, err)
	var schema map[string]any
	var value any
	check(t, json.Unmarshal(schemaRaw, &schema))
	check(t, json.Unmarshal(raw, &value))
	shape(t, value, schema["$defs"].(map[string]any)["cases"].(map[string]any))
	var cases []fixture
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	check(t, d.Decode(&cases))
	var extra any
	if d.Decode(&extra) != io.EOF {
		t.Fatal("trailing JSON input")
	}
	if len(cases) != 48 {
		t.Fatalf("expected 48 cases, got %d", len(cases))
	}
	return cases, raw
}

func observe(t *testing.T, f fixture) observation {
	t.Helper()
	var result observation
	synctest.Test(t, func(t *testing.T) {
		var req livekit.AddTrackRequest
		check(t, protojson.Unmarshal(f.Input.Request, &req))
		original := proto.Clone(&req)
		grants := &auth.ClaimGrants{Video: &auth.VideoGrant{}}
		grants.Video.SetCanPublish(true)
		grants.Video.SetCanSubscribe(false)
		grants.Video.SetCanPublishData(false)
		conf := &rtc.WebRTCConfig{}
		conf.Receiver.PacketBufferSizeVideo = 500
		conf.Receiver.PacketBufferSizeAudio = 200
		conf.SetBufferFactory(buffer.NewFactoryOfBufferFactory(500, 200).CreateBufferFactory())
		// No NewWebRTCConfig: that production helper opens mux/listeners. No SDP,
		// local description, gathering, negotiation, RTP or media receiver is invoked.
		conf.SettingEngine.SetInterfaceFilter(func(string) bool { return false })
		var enabled []*livekit.Codec
		for _, c := range f.Input.EnabledCodecs {
			enabled = append(enabled, &livekit.Codec{Mime: c.Mime, FmtpLine: c.FmtpLine})
		}
		sink := &routingfakes.FakeMessageSink{}
		var mu sync.Mutex
		var captured, source []*livekit.TrackPublishedResponse
		var unexpected []string
		joins := 0
		sink.WriteMessageCalls(func(message proto.Message) error {
			mu.Lock()
			defer mu.Unlock()
			signal, ok := message.(*livekit.SignalResponse)
			if ok && signal.GetJoin() != nil {
				joins++
				return nil
			}
			if !ok || signal.GetTrackPublished() == nil {
				unexpected = append(unexpected, string(message.ProtoReflect().Descriptor().FullName()))
				return nil
			}
			ack := signal.GetTrackPublished()
			source = append(source, ack)
			captured = append(captured, proto.Clone(ack).(*livekit.TrackPublishedResponse))
			return nil
		})
		p, err := rtc.NewParticipant(rtc.ParticipantParams{
			SID: livekit.ParticipantID("PA_c62"), Identity: livekit.ParticipantIdentity("c62"),
			Config: conf, Sink: sink, ProtocolVersion: 6, SessionStartTime: time.Now(),
			Grants: grants, PublishEnabledCodecs: enabled, SubscribeEnabledCodecs: enabled,
			ClientInfo: rtc.ClientInfo{ClientInfo: &livekit.ClientInfo{Browser: f.Input.Browser}},
			Logger:     logger.GetLogger(), Reporter: roomobs.NewNoopParticipantSessionReporter(),
			VersionGenerator: utils.NewDefaultTimedVersionGenerator(), DisableSupervisor: true,
			ParticipantHelper:   &typesfakes.FakeLocalParticipantHelper{},
			ParticipantListener: &typesfakes.FakeLocalParticipantListener{},
			TelemetryListener:   &typesfakes.FakeParticipantTelemetryListener{},
		})
		check(t, err)
		defer func() {
			check(t, p.Close(false, types.ParticipantCloseReasonClientRequestLeave, false))
			synctest.Wait()
			// Original StreamAllocator.Stop sets isStopped; ping observes it only
			// on cPingLong (30s/2). Drain that verified timer AFTER the ACK snapshot.
			// This is virtual time, not a wall-clock sleep or lifecycle assertion.
			time.Sleep(15 * time.Second)
			synctest.Wait()
		}()
		// Public handshake opens the signalling gate without private state mutation
		// or SDP. It is setup evidence, not an AddTrack observation.
		check(t, p.SendJoinResponse(&livekit.JoinResponse{}))
		if !p.IsReady() {
			t.Fatal("public join response did not open signalling")
		}
		p.AddTrack(&req)
		synctest.Wait()
		if p.HasICEConnected() || len(p.GetPublishedTracks()) != 0 {
			t.Fatal("observer crossed ACK-only boundary into connected/published media")
		}
		if !proto.Equal(&req, original) {
			t.Fatal("AddTrack mutated source request")
		}
		mu.Lock()
		defer mu.Unlock()
		if joins != 1 || len(captured) != 1 || len(unexpected) != 0 {
			t.Fatalf("expected exactly one TrackPublished: captured=%d unexpected=%v", len(captured), unexpected)
		}
		ack := captured[0]
		if ack.Cid != req.Cid || ack.Track == nil || ack.Track.Sid == "" {
			t.Fatal("incomplete real ACK")
		}
		// Mutation independence verifies both the actual producer's cloned layer tree
		// and the captured protobuf snapshot, not merely the projected JSON.
		snapshot := proto.Clone(ack)
		mutate := func(layers []*livekit.VideoLayer) {
			for _, l := range layers {
				l.Quality = 123
				l.Width = 7
				l.Height = 8
				l.Bitrate = 9
				l.Ssrc = 10
				l.SpatialLayer = 11
				l.Rid = "mutated"
				l.RepairSsrc = 12
			}
		}
		mutate(req.Layers)
		for _, c := range req.SimulcastCodecs {
			mutate(c.Layers)
			c.Cid = "mutated"
			c.Codec = "mutated"
		}
		if !proto.Equal(ack, snapshot) || !proto.Equal(source[0], snapshot) {
			t.Fatal("ACK aliases mutable request")
		}
		// Preserve every deterministic protobuf field; only random SID and timed
		// version are excluded. Arrays stay explicit and enum values remain numeric.
		raw, err := (protojson.MarshalOptions{UseProtoNames: true, UseEnumNumbers: true, EmitUnpopulated: true}).Marshal(ack.Track)
		check(t, err)
		var track map[string]any
		check(t, json.Unmarshal(raw, &track))
		delete(track, "sid")
		delete(track, "version")
		for _, value := range track["codecs"].([]any) {
			c := value.(map[string]any)
			c["mime"] = c["mime_type"]
			delete(c, "mime_type")
			c["layer_mode"] = c["video_layer_mode"]
			delete(c, "video_layer_mode")
		}
		result = observation{f.SchemaVersion, f.Case, f.Input, map[string]any{"cid": ack.Cid, "track": track}}
		t.Logf("case=%s actual_TrackPublished=1 request_immutable=true ack_independent=true", f.Case)
	})
	return result
}

func runMatrix(t *testing.T) []observation {
	t.Helper()
	cases, raw := fixtures(t)
	rows := make([]observation, 0, len(cases))
	for _, f := range cases {
		rows = append(rows, observe(t, f))
	}
	after, err := os.ReadFile(fixturePath(t))
	check(t, err)
	if !bytes.Equal(raw, after) {
		t.Fatal("raw input source modified")
	}
	t.Logf("raw_input_immutable sha256=%x", sha256.Sum256(raw))
	return rows
}

func TestAddTrack(t *testing.T) {
	first, err := json.Marshal(runMatrix(t))
	check(t, err)
	second, err := json.Marshal(runMatrix(t))
	check(t, err)
	if !bytes.Equal(first, second) {
		t.Fatal("ACK projection is nondeterministic")
	}
}

func TestObserve(t *testing.T) {
	path := os.Getenv("VIDEO_CODEC_SELECTION_GO_JSONL")
	if !filepath.IsAbs(path) {
		t.Fatal("absolute VIDEO_CODEC_SELECTION_GO_JSONL required")
	}
	out, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	check(t, err)
	defer func() { check(t, out.Close()) }()
	enc := json.NewEncoder(out)
	for _, row := range runMatrix(t) {
		check(t, enc.Encode(row))
	}
}
