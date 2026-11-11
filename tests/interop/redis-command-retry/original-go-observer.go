package main

import (
	"context"
	"encoding/json"
	"os"
	"strings"

	"github.com/livekit/protocol/redis"
)

func main() {
	client, err := redis.GetRedisClient(&redis.RedisConfig{Address: os.Args[1]})
	if err != nil {
		panic(err)
	}
	defer client.Close()
	out := json.NewEncoder(os.Stdout)
	for _, key := range []string{"retry_target", "recovery"} {
		value, err := client.Get(context.Background(), key).Result()
		if err == nil {
			if err = out.Encode(map[string]string{"key": key, "value": value}); err != nil {
				panic(err)
			}
		} else {
			code, _, _ := strings.Cut(err.Error(), " ")
			if err = out.Encode(map[string]string{"key": key, "error": code}); err != nil {
				panic(err)
			}
		}
	}
}
