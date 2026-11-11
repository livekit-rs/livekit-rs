// Build with the pinned LiveKit server's go.mod and official generated client.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	"github.com/twitchtv/twirp"
)

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintln(os.Stderr, "usage: rpc-client URL ROOM PAYLOAD")
		os.Exit(2)
	}
	room := os.Args[2]
	token, err := auth.NewAccessToken(os.Getenv("LIVEKIT_API_KEY"), os.Getenv("LIVEKIT_API_SECRET")).
		SetVideoGrant(&auth.VideoGrant{Room: room, RoomAdmin: true}).SetValidFor(time.Minute).ToJWT()
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 18*time.Second)
	defer cancel()
	ctx, err = twirp.WithHTTPRequestHeaders(ctx, http.Header{"Authorization": {"Bearer " + token}})
	if err != nil {
		panic(err)
	}
	var status int
	httpClient := &observedClient{status: &status}
	client := livekit.NewRoomServiceJSONClient(os.Args[1], httpClient)
	start := time.Now()
	response, err := client.PerformRpc(ctx, &livekit.PerformRpcRequest{
		Room: room, DestinationIdentity: "observer", Method: "cluster.echo",
		Payload: os.Args[3], ResponseTimeoutMs: 10000,
	})
	row := map[string]any{"elapsed_ms": time.Since(start).Milliseconds(), "http_status": status}
	if err == nil {
		row["payload"] = response.GetPayload()
	} else {
		var wire twirp.Error
		if errors.As(err, &wire) {
			row["code"] = string(wire.Code())
			row["message"] = wire.Msg()
			row["metadata"] = wire.MetaMap()
		} else {
			row["client_error"] = err.Error()
		}
	}
	if err := json.NewEncoder(os.Stdout).Encode(row); err != nil {
		panic(err)
	}
}

type observedClient struct {
	status *int
}

func (c *observedClient) Do(request *http.Request) (*http.Response, error) {
	response, err := http.DefaultClient.Do(request)
	if response != nil {
		*c.status = response.StatusCode
	}
	return response, err
}
