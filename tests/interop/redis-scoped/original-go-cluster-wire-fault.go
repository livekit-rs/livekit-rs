// Run from the pinned LiveKit Go module to exercise the production Redis factory.
package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/livekit/livekit-server/pkg/config"
	protocolredis "github.com/livekit/protocol/redis"
	redis "github.com/redis/go-redis/v9"
)

const prefix = "LIVEKIT_CLUSTER_WIRE_FAULT_"

func run() error {
	cfg, err := config.NewConfig(os.Getenv(prefix+"YAML"), true, nil, nil)
	if err != nil {
		return err
	}
	if len(cfg.Redis.SentinelAddresses) != 2 || cfg.Redis.MasterName != "" {
		return fmt.Errorf("fixture requires two empty-master seeds")
	}
	client, err := protocolredis.GetRedisClient(&cfg.Redis)
	if err != nil {
		return err
	}
	defer client.Close()
	cluster, ok := client.(*redis.ClusterClient)
	if !ok {
		return fmt.Errorf("expected production ClusterClient, got %T", client)
	}
	key := os.Getenv(prefix + "KEY")
	profile := os.Getenv(prefix + "CASE")
	payload := "verified-peer"
	if strings.HasPrefix(profile, "write_") {
		payload = strings.Repeat("x", 16<<20)
	}
	ctx := context.Background()
	start := time.Now()
	status, err := cluster.Set(ctx, key, payload, 0).Result()
	fmt.Printf("profile=%s duration=%s status=%q error=%v\n", profile, time.Since(start), status, err)
	switch profile {
	case "write_short":
		var op *net.OpError
		if !errors.As(err, &op) || op.Op != "write" || !op.Timeout() {
			return fmt.Errorf("expected actual write timeout, got %v", err)
		}
		recovery, err := cluster.Set(ctx, key, "recover-after-timeout", 0).Result()
		if err != nil || recovery != "OK" {
			return fmt.Errorf("recovery SET status=%q error=%v", recovery, err)
		}
		value, err := cluster.Exists(ctx, key).Result()
		if err != nil || value != 1 {
			return fmt.Errorf("recovery EXISTS value=%d error=%v", value, err)
		}
	case "tls_short":
		if err == nil {
			return fmt.Errorf("expected actual stalled handshake failure")
		}
	case "write_long", "tls_long":
		if err != nil || status != "OK" {
			return fmt.Errorf("SET status=%q error=%v", status, err)
		}
		value, err := cluster.Exists(ctx, key).Result()
		if err != nil || value != 1 {
			return fmt.Errorf("EXISTS value=%d error=%v", value, err)
		}
	default:
		return fmt.Errorf("unknown fixture case %s", profile)
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
