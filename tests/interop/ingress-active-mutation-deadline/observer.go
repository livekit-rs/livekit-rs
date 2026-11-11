package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/rpc"
	"github.com/livekit/psrpc/pkg/bus/redisbus"
	"github.com/livekit/psrpc/pkg/metadata"
	"github.com/redis/go-redis/v9"
)

type observer struct {
	rpc.UnimplementedIngressHandlerServer
	output *json.Encoder
	mu     sync.Mutex
	stop   context.Context
}

func (o *observer) event(value map[string]any) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.output.Encode(value); err != nil {
		log.Fatal(err)
	}
}

func (o *observer) hold(ctx context.Context, method, id, room, details string) (*livekit.IngressState, error) {
	deadline, ok := ctx.Deadline()
	header := metadata.IncomingHeader(ctx)
	if !ok || header == nil || id == "" {
		return nil, fmt.Errorf("missing actual ingress identity/deadline metadata")
	}
	o.event(map[string]any{
		"phase": "entered", "method": method, "ingress_id": id, "room": room, "details": details,
		"wire_budget_ns": deadline.Sub(header.SentAt).Nanoseconds(),
	})
	var cause string
	select {
	case <-ctx.Done():
		cause = ctx.Err().Error()
	case <-o.stop.Done():
		cause = "fixture cleanup"
	}
	o.event(map[string]any{"phase": "finished", "method": method, "ingress_id": id, "cause": cause})
	return nil, fmt.Errorf("%s", cause)
}

func (o *observer) UpdateIngress(ctx context.Context, req *livekit.UpdateIngressRequest) (*livekit.IngressState, error) {
	return o.hold(ctx, "UpdateIngress", req.IngressId, req.RoomName, req.ParticipantMetadata)
}

func (o *observer) DeleteIngress(ctx context.Context, req *livekit.DeleteIngressRequest) (*livekit.IngressState, error) {
	return o.hold(ctx, "DeleteIngress", req.IngressId, "", "")
}

func main() {
	if len(os.Args) < 3 {
		log.Fatal("supply owned Redis address and at least one actual ingress ID")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	client := redis.NewClient(&redis.Options{Addr: os.Args[1]})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		log.Fatal(err)
	}
	bus := redisbus.New(client)
	io, err := rpc.NewIOInfoClient(bus)
	if err != nil {
		log.Fatal(err)
	}
	defer io.Close()
	o := &observer{output: json.NewEncoder(os.Stdout), stop: ctx}
	server, err := rpc.NewIngressHandlerServer(o, bus)
	if err != nil {
		log.Fatal(err)
	}
	defer server.Shutdown()
	for _, id := range os.Args[2:] {
		if err := server.RegisterUpdateIngressTopic(id); err != nil {
			log.Fatal(err)
		}
		if err := server.RegisterDeleteIngressTopic(id); err != nil {
			log.Fatal(err)
		}
		now := time.Now().UnixNano()
		_, err := io.UpdateIngressState(ctx, &rpc.UpdateIngressStateRequest{
			IngressId: id,
			State: &livekit.IngressState{Status: livekit.IngressState_ENDPOINT_PUBLISHING,
				StartedAt: now, UpdatedAt: now, RoomId: "RM_owned_active"},
		})
		if err != nil {
			log.Fatal(err)
		}
	}
	o.event(map[string]any{"phase": "ready", "ids": os.Args[2:]})
	<-ctx.Done()
}
