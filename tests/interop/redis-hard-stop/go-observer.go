// Build against the fixed LiveKit go.mod. Use the same Redis constructor as
// the standalone server, with the default go-redis Cluster state lifetime.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/livekit/protocol/redis"
)

func main() {
	if len(os.Args) != 5 {
		panic("expected addresses, command, key and field")
	}
	addrs, operation, key, field := os.Args[1], os.Args[2], os.Args[3], os.Args[4]
	client, err := redis.GetRedisClient(&redis.RedisConfig{ClusterAddresses: strings.Split(addrs, ",")})
	if err != nil {
		panic(err)
	}
	defer client.Close()
	out := json.NewEncoder(os.Stdout)
	value, err := client.HGet(context.Background(), key, field).Result()
	if err != nil || value != "fixture" {
		panic(fmt.Sprintf("warm HGET: %q %v", value, err))
	}
	if err := out.Encode(map[string]any{"phase": "warm", "value": value}); err != nil {
		panic(err)
	}
	if _, err := bufio.NewReader(os.Stdin).ReadString('\n'); err != nil {
		panic(err)
	}
	start := time.Now()
	var result any
	switch operation {
	case "HGET":
		result, err = client.HGet(context.Background(), key, field).Result()
	case "HDEL":
		result, err = client.HDel(context.Background(), key, field).Result()
	default:
		panic("unknown command")
	}
	row := map[string]any{"phase": "request", "elapsed_ms": time.Since(start).Milliseconds(), "value": result}
	if err != nil {
		row["error"] = err.Error()
	}
	if err := out.Encode(row); err != nil {
		panic(err)
	}
}
