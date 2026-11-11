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
	rpc.UnimplementedEgressInternalServer
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

func (o *observer) StartEgressAffinity(_ context.Context, _ *rpc.StartEgressRequest) float32 {
	return 1
}

func (o *observer) StartEgress(ctx context.Context, req *rpc.StartEgressRequest) (*livekit.EgressInfo, error) {
	deadline, ok := ctx.Deadline()
	header := metadata.IncomingHeader(ctx)
	mode := req.GetWeb().GetUrl()
	if !ok || header == nil || req.GetEgressId() == "" || req.GetRoomId() != "" {
		return nil, fmt.Errorf("missing actual Web request/deadline metadata")
	}
	o.event(map[string]any{
		"phase": "entered", "mode": mode, "egress_id": req.GetEgressId(),
		"deadline_unix_ns": deadline.UnixNano(),
		"sent_at_unix_ns":  header.SentAt.UnixNano(),
		"wire_budget_ns":   deadline.Sub(header.SentAt).Nanoseconds(),
	})
	switch mode {
	case "http://127.0.0.1/owned-held":
		select {
		case <-ctx.Done():
			o.event(map[string]any{"phase": "finished", "mode": mode, "egress_id": req.EgressId, "cause": ctx.Err().Error()})
			return nil, ctx.Err()
		case <-o.stop.Done():
			o.event(map[string]any{"phase": "finished", "mode": mode, "egress_id": req.EgressId, "cause": "fixture cleanup"})
			return nil, o.stop.Err()
		}
	case "http://127.0.0.1/owned-positive":
		o.event(map[string]any{"phase": "finished", "mode": mode, "egress_id": req.EgressId, "cause": "typed positive response"})
		return &livekit.EgressInfo{EgressId: req.EgressId, Status: livekit.EgressStatus_EGRESS_STARTING,
			Request:   &livekit.EgressInfo_Web{Web: req.GetWeb()},
			StartedAt: time.Now().UnixNano(),
		}, nil
	default:
		return nil, fmt.Errorf("unexpected observer request %q", mode)
	}
}

func main() {
	if len(os.Args) != 2 {
		log.Fatal("supply owned Redis address")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	client := redis.NewClient(&redis.Options{Addr: os.Args[1]})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		log.Fatal(err)
	}
	o := &observer{output: json.NewEncoder(os.Stdout), stop: ctx}
	server, err := rpc.NewEgressInternalServer(o, redisbus.New(client))
	if err != nil {
		log.Fatal(err)
	}
	defer server.Shutdown()
	if err := server.RegisterStartEgressTopic(""); err != nil {
		log.Fatal(err)
	}
	o.event(map[string]any{"phase": "ready"})
	<-ctx.Done()
}
