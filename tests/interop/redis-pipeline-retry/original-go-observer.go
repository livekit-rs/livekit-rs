package main

import (
	"context"
	"encoding/json"
	"os"

	"github.com/livekit/livekit-server/pkg/service"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/redis"
)

func main() {
	client, err := redis.GetRedisClient(&redis.RedisConfig{Address: os.Args[1]})
	if err != nil {
		panic(err)
	}
	defer client.Close()
	store := service.NewRedisStore(client)
	switch os.Args[2] {
	case "egress":
		err = store.StoreEgress(context.Background(), &livekit.EgressInfo{EgressId: "EG_fixture", RoomName: "fixture"})
	case "ingress":
		err = store.DeleteIngress(context.Background(), &livekit.IngressInfo{IngressId: "IN_fixture", RoomName: "fixture", StreamKey: "stream"})
	default:
		panic("unrecognized operation")
	}
	result := map[string]any{"success": err == nil, "error": nil}
	if err != nil {
		result["error"] = err.Error()
	}
	if os.Args[2] == "ingress" {
		recovery := store.StoreEgress(context.Background(), &livekit.EgressInfo{EgressId: "EG_recovery", RoomName: "recovery"})
		result["recovery"] = recovery == nil
	}

	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		panic(err)
	}
}
