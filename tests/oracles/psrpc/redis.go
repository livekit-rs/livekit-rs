// Genuine pinned Go psrpc over Redis. Synthetic echo is transport evidence only.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/livekit/psrpc"
	"github.com/livekit/psrpc/pkg/bus"
	"github.com/livekit/psrpc/pkg/bus/redisbus"
	"github.com/livekit/psrpc/pkg/client"
	"github.com/livekit/psrpc/pkg/info"
	"github.com/livekit/psrpc/pkg/metadata"
	"github.com/livekit/psrpc/pkg/rand"
	"github.com/livekit/psrpc/pkg/server"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type redisOracleOptions struct {
	URL, Service, Method, Topic, Label, Ready, Result, Value, ExpectedLabel, ExpectedCode string
	Queue, Claim, SkipClaim                                                               bool
	Affinity                                                                              float64
	Timeout, Lifetime                                                                     time.Duration
	PayloadBytes                                                                          int
}

func writeJSON(path string, value any) {
	if path == "" {
		out, _ := json.Marshal(value)
		fmt.Println(string(out))
		return
	}
	out, err := json.MarshalIndent(value, "", "  ")
	must(err)
	must(os.WriteFile(path, append(out, '\n'), 0644))
}
func readySubscriptions(ctx context.Context, rc *redis.Client, channels []string, minimum ...map[string]int64) {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		counts, err := rc.PubSubNumSub(ctx, channels...).Result()
		must(err)
		ready := true
		for _, channel := range channels {
			required := int64(1)
			if len(minimum) > 0 {
				required = minimum[0][channel]
			}
			if counts[channel] < required {
				ready = false
			}
		}
		if ready {
			return
		}
		select {
		case <-ctx.Done():
			panic("subscription readiness deadline")
		case <-ticker.C:
		}
	}
}
func runRedisOracle(mode string, o redisOracleOptions) {
	options, err := redis.ParseURL(o.URL)
	must(err)
	rc := redis.NewClient(options)
	defer rc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), o.Timeout)
	must(rc.Ping(ctx).Err())
	cancel()
	b := redisbus.New(rc, bus.WithBusCompression(bus.CompressionOpts{Quality: 6, Threshold: 128, MaxDecompressedSize: 1024 * 1024}))
	topics := []string{o.Topic}
	sd := &info.ServiceDefinition{Name: o.Service, ID: rand.NewClientID()}
	sd.RegisterMethod(o.Method, o.Claim, false, o.Claim, o.Queue)
	switch mode {
	case "redis-serve":
		sd.ID = rand.NewServerID()
		srv := server.NewRPCServer(sd, b, psrpc.WithServerSkipClaim(func() bool { return o.SkipClaim }))
		defer srv.Close(true)
		var calls atomic.Int64
		var affinity server.AffinityFunc[*wrapperspb.StringValue]
		if !o.Queue {
			affinity = func(context.Context, *wrapperspb.StringValue) float32 { return float32(o.Affinity) }
		}
		requestInfo := sd.GetInfo(o.Method, topics)
		channels := []string{requestInfo.GetRPCChannel().Legacy}
		if o.Claim {
			channels = append(channels, requestInfo.GetClaimResponseChannel().Legacy)
		}
		baselineCtx, baselineDone := context.WithTimeout(context.Background(), o.Timeout)
		minimum, err := rc.PubSubNumSub(baselineCtx, channels...).Result()
		must(err)
		baselineDone()
		for key := range minimum {
			minimum[key]++
		}
		must(server.RegisterHandler(srv, o.Method, topics, func(ctx context.Context, request *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			calls.Add(1)
			header := metadata.IncomingHeader(ctx)
			if header == nil || header.Metadata["synthetic"] != "yes" {
				return nil, psrpc.NewErrorf(psrpc.InvalidArgument, "metadata missing")
			}
			if request.Value == "error" {
				return nil, psrpc.NewErrorf(psrpc.InvalidArgument, "synthetic remote error")
			}
			if strings.HasPrefix(request.Value, "sleep:") {
				n, _ := strconv.Atoi(strings.TrimPrefix(request.Value, "sleep:"))
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(time.Duration(n) * time.Millisecond):
				}
			}
			return wrapperspb.String(o.Label + ":" + request.Value), nil
		}, affinity))
		readyCtx, done := context.WithTimeout(context.Background(), o.Timeout)
		readySubscriptions(readyCtx, rc, channels, minimum)
		done()
		writeJSON(o.Ready, map[string]any{"ready": true, "broker_subscription_count_confirmed": true, "scope": "synthetic psrpc server only"})
		signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		select {
		case <-signalCtx.Done():
		case <-time.After(o.Lifetime):
		}
		writeJSON(o.Result, map[string]any{"status": "STOPPED", "actual_handler_calls": calls.Load(), "scope": "transport, not media/worker execution"})
	case "redis-call":
		rpc, err := client.NewRPCClient(sd, b)
		must(err)
		defer rpc.Close()
		readyCtx, done := context.WithTimeout(context.Background(), o.Timeout)
		readySubscriptions(readyCtx, rc, []string{info.GetResponseChannel(o.Service, sd.ID).Legacy, info.GetClaimRequestChannel(o.Service, sd.ID).Legacy})
		done()
		ctx, cancel := context.WithTimeout(context.Background(), o.Timeout)
		defer cancel()
		ctx = metadata.NewContextWithOutgoingMetadata(ctx, metadata.Metadata{"synthetic": "yes"})
		value := o.Value
		if o.PayloadBytes > 0 {
			value = strings.Repeat("x", o.PayloadBytes)
		}
		selection := psrpc.SelectionOpts{AffinityTimeout: o.Timeout / 2, ShortCircuitTimeout: 50 * time.Millisecond}
		if !o.Queue {
			selection.MaximumAffinity = 1
		}
		response, err := client.RequestSingle[*wrapperspb.StringValue](ctx, rpc, o.Method, topics, wrapperspb.String(value), psrpc.WithRequestTimeout(o.Timeout), psrpc.WithSelectionOpts(selection))
		if err != nil {
			code, _ := psrpc.GetErrorCode(err)
			if string(code) != o.ExpectedCode {
				panic(fmt.Sprintf("unexpected RPC error code %s: %v", code, err))
			}
			writeJSON(o.Result, map[string]any{"status": "PASS", "expected_error_code": code, "scope": "real Go client error path"})
			return
		}
		if o.ExpectedCode != "" {
			panic("expected RPC error, received success")
		}
		expected := o.ExpectedLabel + ":" + value
		if response.Value != expected {
			panic("actual RPC response does not match expected server/value")
		}
		digest := sha256.Sum256([]byte(response.Value))
		writeJSON(o.Result, map[string]any{"status": "PASS", "actual_response_bytes": len(response.Value), "actual_response_sha256": hex.EncodeToString(digest[:]), "scope": "real Go client response"})
	default:
		panic("unknown Redis mode")
	}
}
