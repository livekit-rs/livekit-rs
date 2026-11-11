// Copyright 2026 livekit-rs contributors. Apache-2.0 original-source observer.
package buffer

import (
	"encoding/json"
	"fmt"
	"github.com/livekit/protocol/logger"
	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
	"testing"
)

func TestOriginalTemporalCalculatorPacketTraces(t *testing.T) {
	for _, name := range []string{"clean", "loss_reorder", "wrap", "tid3_clean", "tid3_loss_reorder", "tid3_wrap"} {
		start := uint16(100)
		if name == "wrap" || name == "tid3_wrap" {
			start = 65520
		}
		vp9 := NewFrameRateCalculatorVP9(90000, logger.GetLogger())
		dd := NewFrameRateCalculatorDD(90000, logger.GetLogger())
		fps := [][]float32{{3.75, 7.5, 15}, {3.75, 7.5, 15}, {3.75, 7.5, 15}}
		if len(name) > 5 && name[:5] == "tid3_" {
			fps = [][]float32{{3.75, 7.5, 11.25, 15}, {3.75, 7.5, 11.25, 15}, {3.75, 7.5, 11.25, 15}}
		}
		frames := createFrames(start, 3000, 1000, 120, fps, true)
		type event struct {
			f       *testFrameInfo
			picture uint16
		}
		events := []event{}
		for index := 0; index < 120; index++ {
			for s := 0; s < 3; s++ {
				if (name == "loss_reorder" || name == "tid3_loss_reorder") && index == 13 {
					continue
				}
				events = append(events, event{frames[s][index], start + uint16(index)})
			}
		}
		if name == "loss_reorder" || name == "tid3_loss_reorder" {
			events[30], events[33] = events[33], events[30]
			events = append(events, events[0])
		}
		for _, event := range events {
			f := event.f
			ep := &ExtPacket{Packet: &rtp.Packet{Header: f.header}, VideoLayer: VideoLayer{Spatial: int32(f.spatial), Temporal: int32(f.temporal)}, VP9: &codecs.VP9Packet{I: true, PictureID: event.picture}}
			vp9.RecvPacket(ep)
			dd.RecvPacket(f.toDD())
			vr := vp9.GetFrameRateForSpatial(int32(f.spatial))
			dr := dd.GetFrameRateForSpatial(int32(f.spatial))
			row := map[string]interface{}{"case": name, "picture": event.picture, "frame": f.framenumber, "spatial": f.spatial, "temporal": f.temporal, "timestamp": f.header.Timestamp, "diffs": f.frameDiff, "vp9": vr, "dd": dr}
			data, err := json.Marshal(row)
			if err != nil {
				t.Fatal(err)
			}
			fmt.Printf("TEMPORAL_PACKET %s\n", data)
		}
		if name == "clean" {
			v := vp9.GetFrameRateForSpatial(0)
			if v[0] != 3.75 || v[1] != 7.5 || v[2] != 15 {
				t.Fatalf("original clean VP9 %v", v)
			}
		}
	}
}
