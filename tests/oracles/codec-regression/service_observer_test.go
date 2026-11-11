package service

import (
	"encoding/json"
	"fmt"
	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/rtc"
	"github.com/livekit/livekit-server/pkg/rtc/types"
	"github.com/livekit/livekit-server/pkg/sfu"
	"github.com/livekit/livekit-server/pkg/sfu/audio"
	"github.com/livekit/livekit-server/pkg/telemetry/telemetryfakes"
	"github.com/livekit/protocol/livekit"
	"testing"
)

func TestOriginalLiveKitCodecRoomGate(t *testing.T) {
	room := rtc.NewRoom(&livekit.Room{Name: "source-codec-gate"}, nil, rtc.WebRTCConfig{},
		config.RoomConfig{EmptyTimeout: 300, DepartureTimeout: 20},
		&sfu.AudioConfig{AudioLevelConfig: audio.AudioLevelConfig{UpdateInterval: 400, SmoothIntervals: 2}},
		&livekit.ServerInfo{}, &telemetryfakes.FakeTelemetryService{}, nil, nil, nil)
	defer room.Close(types.RoomCloseReasonUnknown)
	for count := 0; count <= 6; count++ {
		if count > 0 {
			p := rtc.NewMockParticipant(livekit.ParticipantIdentity(fmt.Sprintf("source-%d", count)), types.CurrentProtocol, false, true, room.LocalParticipantListener())
			if err := room.Join(p, nil, &rtc.ParticipantOptions{AutoSubscribe: true}, nil); err != nil {
				t.Fatal(err)
			}
			p.StateReturns(livekit.ParticipantInfo_ACTIVE)
		}
		if actual := room.GetParticipantCount(); actual != count {
			t.Fatalf("real owner count %d != %d", actual, count)
		}
		for _, threshold := range []int{-1, 0, 5} {
			helper := roomManagerParticipantHelper{room: room, codecRegressionThreshold: threshold}
			row := struct {
				Kind      string `json:"kind"`
				Threshold int    `json:"threshold"`
				Count     int    `json:"count"`
				Allows    bool   `json:"allows"`
			}{"room", threshold, count, helper.ShouldRegressCodec()}
			wire, err := json.Marshal(row)
			if err != nil {
				t.Fatal(err)
			}
			fmt.Printf("SOURCE_CODEC_GATE %s\n", wire)
		}
	}
}
