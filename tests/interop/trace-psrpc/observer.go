// Run from the fixed livekit-stable module: go run -mod=readonly /absolute/observer.go.
// Uses the actual pinned middleware and real localbus Request/Claim/Response.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/livekit/psrpc"
	"github.com/livekit/psrpc/pkg/bus/localbus"
	"github.com/livekit/psrpc/pkg/client"
	"github.com/livekit/psrpc/pkg/info"
	"github.com/livekit/psrpc/pkg/metadata"
	"github.com/livekit/psrpc/pkg/middleware"
	"github.com/livekit/psrpc/pkg/middleware/otelpsrpc"
	"github.com/livekit/psrpc/pkg/server"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type capture struct {
	sync.Mutex
	Spans []map[string]any
}

func attrs(in []attribute.KeyValue) map[string]any {
	out := map[string]any{}
	for _, a := range in {
		out[string(a.Key)] = a.Value.AsInterface()
	}
	return out
}
func (c *capture) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	c.Lock()
	defer c.Unlock()
	for _, s := range spans {
		events := []map[string]any{}
		for _, e := range s.Events() {
			events = append(events, map[string]any{"name": e.Name, "attributes": attrs(e.Attributes)})
		}
		c.Spans = append(c.Spans, map[string]any{"name": s.Name(), "kind": s.SpanKind().String(), "trace_id": s.SpanContext().TraceID().String(), "span_id": s.SpanContext().SpanID().String(), "parent_span_id": s.Parent().SpanID().String(), "tracestate": s.SpanContext().TraceState().String(), "scope": s.InstrumentationScope(), "status": s.Status(), "attributes": attrs(s.Attributes()), "events": events})
	}
	return nil
}
func (*capture) Shutdown(context.Context) error { return nil }
func must(err error) {
	if err != nil {
		panic(err)
	}
}
func main() {
	exporter := &capture{}
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer provider.Shutdown(context.Background())
	bus := localbus.New()
	sd := &info.ServiceDefinition{Name: "TraceProduction", ID: "observer-owner"}
	for _, name := range []string{"Success", "PSRPCError", "PlainError", "WrappedError", "GRPCError", "Retry", "Metadata"} {
		sd.RegisterMethod(name, false, false, true, true)
	}
	sd.RegisterMethod("Discover", false, true, false, false)
	srv := server.NewRPCServer(sd, bus, otelpsrpc.ServerOptions(otelpsrpc.Config{TracerProvider: provider}))
	defer srv.Close(false)
	hits := atomic.Int32{}
	incoming := map[string]map[string]string{}
	for _, name := range []string{"Success", "PSRPCError", "PlainError", "WrappedError", "GRPCError", "Retry", "Metadata", "Discover"} {
		name := name
		must(server.RegisterHandler(srv, name, nil, func(ctx context.Context, request *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			if name == "Metadata" {
				incoming[name] = metadata.IncomingHeader(ctx).Metadata
			}
			switch name {
			case "PSRPCError":
				return nil, psrpc.NewErrorf(psrpc.Unavailable, "real worker unavailable")
			case "PlainError":
				return nil, errors.New("plain handler error")
			case "WrappedError":
				return nil, fmt.Errorf("outer: %w", psrpc.NewErrorf(psrpc.PermissionDenied, "denied"))
			case "GRPCError":
				return nil, status.Error(codes.NotFound, "grpc absent")
			case "Retry":
				if hits.Add(1) == 1 {
					return nil, psrpc.NewErrorf(psrpc.Unavailable, "first actual attempt")
				}
			}
			return request, nil
		}, nil))
	}
	cli, err := client.NewRPCClient(sd, bus, middleware.WithRPCRetries(middleware.RetryOptions{MaxAttempts: 2, Timeout: time.Second}), otelpsrpc.ClientOptions(otelpsrpc.Config{TracerProvider: provider}))
	must(err)
	defer cli.Close()
	tid, err := trace.TraceIDFromHex("03030303030303030303030303030303")
	must(err)
	sid, err := trace.SpanIDFromHex("0909090909090909")
	must(err)
	ts, err := trace.ParseTraceState("vendor=state")
	must(err)
	ctx := trace.ContextWithRemoteSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled, Remote: true, TraceState: ts}))
	ctx = metadata.WithOutgoingMetadata(ctx, metadata.Metadata{"application": "kept", "traceparent": "stale-overridden"})
	outcomes := map[string]any{}
	for _, name := range []string{"Success", "PSRPCError", "PlainError", "WrappedError", "GRPCError", "Retry", "Metadata"} {
		response, err := client.RequestSingle[*wrapperspb.StringValue](ctx, cli, name, nil, wrapperspb.String("actual-request"), psrpc.WithRequestTimeout(time.Second))
		item := map[string]any{}
		if err != nil {
			item["error"] = err.Error()
		} else {
			item["value"] = response.Value
		}
		outcomes[name] = item
	}
	responses, err := client.RequestMulti[*wrapperspb.StringValue](ctx, cli, "Discover", nil, wrapperspb.String("actual-multi"), psrpc.WithRequestTimeout(30*time.Millisecond))
	must(err)
	count := 0
	for response := range responses {
		must(response.Err)
		count++
	}
	outcomes["Discover"] = count
	cli.Close()
	srv.Close(false)
	must(provider.ForceFlush(context.Background()))
	exporter.Lock()
	defer exporter.Unlock()
	must(json.NewEncoder(os.Stdout).Encode(map[string]any{"source": "original pinned otelpsrpc ClientOptions/ServerOptions over actual localbus", "spans": exporter.Spans, "outcomes": outcomes, "incoming": incoming, "retry_handler_calls": hits.Load()}))
}
