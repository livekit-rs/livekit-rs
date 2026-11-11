package main

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/livekit/livekit-server/pkg/service"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/redis"
	"github.com/livekit/protocol/xtls"
)

func main() {
	config := &redis.RedisConfig{
		SentinelAddresses: []string{os.Args[1]}, DialTimeout: 1000, ReadTimeout: 2000, WriteTimeout: 180,
	}
	if len(os.Args) > 3 {
		switch os.Args[3] {
		case "cluster":
			config.SentinelAddresses = strings.Split(os.Args[1], ",")
		case "sentinel":
			config.MasterName = "transaction-fixture"
		default:
			panic("unknown Redis topology")
		}
	}
	if len(os.Args) > 4 {
		config.TLS = &xtls.Config{Enabled: true, ServerName: "localhost", CACertFile: os.Args[4]}
	}
	client, err := redis.GetRedisClient(config)
	if err != nil {
		panic(err)
	}
	defer client.Close()
	roomSize, err := strconv.Atoi(os.Args[2])
	if err != nil {
		panic(err)
	}
	store := service.NewRedisStore(client)
	err = store.DeleteIngress(context.Background(), &livekit.IngressInfo{
		IngressId: "IN_fixture", RoomName: strings.Repeat("p", roomSize), StreamKey: "stream",
	})
	if gate := os.Getenv("LK_REDIS_RECOVERY_GATE"); gate != "" {
		deadline := time.Now().Add(25 * time.Second)
		for {
			if _, err := os.Stat(gate); err == nil {
				break
			}
			if time.Now().After(deadline) {
				panic("fixture did not finish Redis transition")
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	recovery := store.StoreEgress(context.Background(), &livekit.EgressInfo{
		EgressId: "EG_recovery", RoomName: "recovery",
	})
	result := map[string]any{"success": err == nil, "error": nil, "recovery": recovery == nil}
	if err != nil {
		result["error"] = err.Error()
	}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		panic(err)
	}
}
