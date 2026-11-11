// Development-only interoperability oracle; never built into the Rust server.
package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/livekit/psrpc"
	"github.com/livekit/psrpc/internal"
	"github.com/livekit/psrpc/pkg/bus"
	"github.com/livekit/psrpc/pkg/info"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const commit = "e9701ee1b5a981cfa3c7c69503d4775ddff13729"

type capture struct{ payload []byte }

func (c *capture) Publish(_ context.Context, _ bus.Channel, payload []byte) error {
	c.payload = append([]byte(nil), payload...)
	return nil
}
func (*capture) Subscribe(context.Context, bus.Channel, int) (bus.Reader, error) {
	return nil, fmt.Errorf("not used by codec oracle")
}
func (*capture) SubscribeQueue(context.Context, bus.Channel, int) (bus.Reader, error) {
	return nil, fmt.Errorf("not used by codec oracle")
}

type channel struct {
	Legacy string `json:"legacy"`
	Server string `json:"server"`
	Local  string `json:"local"`
}

func channelOf(c bus.Channel) channel { return channel{c.Legacy, c.Server, c.Local} }

type channelCase struct {
	Service       string   `json:"service"`
	Method        string   `json:"method"`
	Topics        []string `json:"topics"`
	Client        string   `json:"client"`
	Queue         bool     `json:"queue"`
	Request       channel  `json:"request"`
	ClaimResponse channel  `json:"claim_response"`
	StreamServer  channel  `json:"stream_server"`
	Response      channel  `json:"response"`
	ClaimRequest  channel  `json:"claim_request"`
	StreamClient  channel  `json:"stream_client"`
	HandlerKey    string   `json:"handler_key"`
}
type messageCase struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Wire        string `json:"wire_base64"`
	Payload     string `json:"payload_base64"`
	Local       string `json:"local"`
	Compression int32  `json:"compression"`
}
type fixture struct {
	Commit   string        `json:"source_commit"`
	Channels []channelCase `json:"channels"`
	Messages []messageCase `json:"messages"`
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func fixtures() fixture {
	f := fixture{Commit: commit}
	for _, service := range []string{"EgressInternal", "service.with space|", "東京😀"} {
		for _, topics := range [][]string{nil, {""}, {"", ""}, {"a", ""}, {"", "a"}, {"a", "", "b"}, {"a", "", ""}, {"room.一", "😀", "|+"}} {
			for _, queue := range []bool{false, true} {
				method := "Start.Egress"
				client := "client.|😀"
				r := info.RequestInfo{RPCInfo: psrpc.RPCInfo{Service: service, Method: method, Topic: topics}, Queue: queue}
				f.Channels = append(f.Channels, channelCase{service, method, topics, client, queue, channelOf(r.GetRPCChannel()), channelOf(r.GetClaimResponseChannel()), channelOf(r.GetStreamServerChannel()), channelOf(info.GetResponseChannel(service, client)), channelOf(info.GetClaimRequestChannel(service, client)), channelOf(info.GetStreamChannel(service, client)), r.GetHandlerKey()})
			}
		}
	}
	inner := wrapperspb.String("safe synthetic value")
	a, err := anypb.New(inner)
	must(err)
	raw, err := proto.Marshal(inner)
	must(err)
	request := &internal.Request{RequestId: "request-1", ClientId: "client-1", SentAt: 1792850000000000000, Expiry: 1792850003000000000, Multi: true, Request: a, Metadata: map[string]string{"trace": "synthetic"}, RawRequest: raw, SkipClaim: true}
	unknown := proto.Clone(request)
	b, err := proto.Marshal(unknown)
	must(err)
	b = protowire.AppendVarint(protowire.AppendTag(b, 123, protowire.VarintType), 7)
	must(proto.Unmarshal(b, unknown))
	messages := []struct {
		name    string
		message proto.Message
		quality int
	}{
		{"request-any-raw", request, 0},
		{"request-unknown-field", unknown, 0},
		{"request-default", &internal.Request{}, 0},
		{"request-gzip", &internal.Request{RequestId: "compressed", RawRequest: bytes.Repeat([]byte("synthetic-compressible-"), 4096)}, 6},
		{"request-threshold-disabled", request, 0},
		{"response-any-raw-error-details", &internal.Response{RequestId: "request-1", ServerId: "server-1", SentAt: 1792850001000000000, Response: a, RawResponse: raw, Error: "safe error", Code: "invalid_argument", ErrorDetails: []*anypb.Any{a}}, 0},
		{"claim-request", &internal.ClaimRequest{RequestId: "request-1", ServerId: "server-1", Affinity: 0.875, Handling: true}, 0},
		{"claim-response", &internal.ClaimResponse{RequestId: "request-1", ServerId: "server-1"}, 0},
		{"stream-message", &internal.Stream{StreamId: "stream-1", RequestId: "request-1", SentAt: 100, Expiry: 200, Body: &internal.Stream_Message{Message: &internal.StreamMessage{Message: a, RawMessage: raw}}}, 0},
	}
	for _, item := range messages {
		c := &capture{}
		b := bus.New(c, bus.WithBusCompression(bus.CompressionOpts{Quality: item.quality, Threshold: 0, MaxDecompressedSize: 1024 * 1024}))
		must(b.Publish(context.Background(), bus.Channel{Local: "Method.REQ"}, item.message))
		// Read using the actual public package, not a handwritten decoder.
		decoded, err := bus.Deserialize(c.payload, 1024*1024)
		must(err)
		if !proto.Equal(decoded, item.message) {
			panic("Go package self-read mismatch")
		}
		envelope := &internal.Msg{}
		must(proto.Unmarshal(c.payload, envelope))
		plain, err := proto.Marshal(item.message)
		must(err)
		f.Messages = append(f.Messages, messageCase{item.name, string(item.message.ProtoReflect().Descriptor().FullName()), base64.StdEncoding.EncodeToString(c.payload), base64.StdEncoding.EncodeToString(plain), envelope.Channel, int32(envelope.Compression)})
	}
	return f
}
func newMessage(name string) proto.Message {
	switch name {
	case "internal.Request":
		return &internal.Request{}
	case "internal.Response":
		return &internal.Response{}
	case "internal.ClaimRequest":
		return &internal.ClaimRequest{}
	case "internal.ClaimResponse":
		return &internal.ClaimResponse{}
	case "internal.Stream":
		return &internal.Stream{}
	default:
		panic("unexpected fixture message type")
	}
}
func main() {
	mode := flag.String("mode", "generate", "generate or verify")
	path := flag.String("file", "", "fixture output or Rust fixture input")
	redisURL := flag.String("redis-url", "redis://127.0.0.1:6379/", "task-owned Redis URL")
	service := flag.String("service", "CompatEcho", "synthetic service")
	method := flag.String("method", "Echo", "synthetic method")
	topic := flag.String("topic", "room.東京😀", "synthetic topic")
	label := flag.String("label", "go", "response label")
	ready := flag.String("ready", "", "readiness JSON file")
	result := flag.String("result", "", "result JSON file")
	value := flag.String("value", "hello", "synthetic request value")
	expectedLabel := flag.String("expect-label", "rust", "expected server label")
	expectedCode := flag.String("expect-code", "", "expected error code")
	queue := flag.Bool("queue", false, "queue RPC")
	claim := flag.Bool("claim", true, "claim RPC")
	skipClaim := flag.Bool("skip-claim", false, "allow queue handling announcement")
	affinity := flag.Float64("affinity", 1, "server affinity")
	timeout := flag.Duration("timeout", 3*time.Second, "call/readiness timeout")
	lifetime := flag.Duration("lifetime", 30*time.Second, "bounded server lifetime")
	payloadBytes := flag.Int("payload-bytes", 0, "compressible synthetic value bytes")
	flag.Parse()
	if strings.HasPrefix(*mode, "redis-") {
		runRedisOracle(*mode, redisOracleOptions{URL: *redisURL, Service: *service, Method: *method, Topic: *topic, Label: *label, Ready: *ready, Result: *result, Value: *value, ExpectedLabel: *expectedLabel, ExpectedCode: *expectedCode, Queue: *queue, Claim: *claim, SkipClaim: *skipClaim, Affinity: *affinity, Timeout: *timeout, Lifetime: *lifetime, PayloadBytes: *payloadBytes})
		return
	}
	if *path == "" {
		panic("-file is required")
	}
	expected := fixtures()
	switch *mode {
	case "generate":
		out, err := json.MarshalIndent(expected, "", "  ")
		must(err)
		out = append(out, '\n')
		must(os.WriteFile(*path, out, 0644))
		fmt.Printf("PASS generated %d channel cases and %d real Go bus fixtures\n", len(expected.Channels), len(expected.Messages))
	case "verify":
		input, err := os.ReadFile(*path)
		must(err)
		var actual fixture
		must(json.Unmarshal(input, &actual))
		if len(actual.Messages) != len(expected.Messages) || len(actual.Channels) != len(expected.Channels) {
			panic("fixture inventory mismatch")
		}
		for i, c := range actual.Channels {
			x, _ := json.Marshal(c)
			y, _ := json.Marshal(expected.Channels[i])
			if !bytes.Equal(x, y) {
				panic(fmt.Sprintf("Rust channel mismatch %d", i))
			}
		}
		for i, item := range actual.Messages {
			want := expected.Messages[i]
			if item.Name != want.Name || item.Type != want.Type || item.Local != want.Local {
				panic("Rust message identity mismatch")
			}
			encoded, err := base64.StdEncoding.DecodeString(item.Wire)
			must(err)
			decoded, err := bus.Deserialize(encoded, 1024*1024)
			must(err)
			known, err := base64.StdEncoding.DecodeString(want.Payload)
			must(err)
			message := newMessage(want.Type)
			must(proto.Unmarshal(known, message))
			message.ProtoReflect().SetUnknown(nil)
			decoded.ProtoReflect().SetUnknown(nil)
			if !proto.Equal(decoded, message) {
				panic("Rust message value mismatch: " + item.Name)
			}
			local, err := bus.DecodeLocalChannel(encoded)
			must(err)
			if local != item.Local {
				panic("Rust local channel mismatch")
			}
		}
		fmt.Printf("PASS real Go bus read %d Rust messages and %d exact channel cases\n", len(actual.Messages), len(actual.Channels))
	default:
		panic("unknown mode")
	}
}
