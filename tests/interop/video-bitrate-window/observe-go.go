// Execute from the fixed LiveKit Go module.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/sfu"
	"github.com/livekit/protocol/codecs/mime"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/pion/rtp"
)

type result struct {
	Source   livekit.TrackSource `json:"source"`
	Duration string              `json:"configured_interval"`
	Present  bool                `json:"tracker_present"`
	Before   int64               `json:"before_bps"`
	After    int64               `json:"after_bps"`
	First    time.Duration       `json:"first_report_ns"`
	Removed  bool                `json:"tracker_removed"`
}

func run(source livekit.TrackSource, intervals string) (result, error) {
	out := result{Source: source, Duration: intervals}
	if source == livekit.TrackSource_CAMERA {
		out.Duration = "125ms"
	}
	body := "rtc: {node_ip: 127.0.0.1}\nvideo:\n  stream_tracker_manager:\n    video:\n      bitrate_report_interval: {0: 125ms, 1: 450ms}\n    screenshare:\n      bitrate_report_interval: " + intervals + "\n"
	cfg, err := config.NewConfig(body, true, nil, nil)
	if err != nil {
		return out, err
	}
	manager := sfu.NewStreamTrackerManager(logger.GetLogger(), &livekit.TrackInfo{Type: livekit.TrackType_VIDEO, Source: source}, mime.MimeTypeVP8, 90000, cfg.Video.StreamTrackerManager)
	defer manager.Close()
	tracker := manager.AddTracker(0)
	out.Present = tracker != nil
	if tracker == nil {
		return out, nil
	}
	start := time.Now()
	tick := time.NewTicker(15 * time.Millisecond)
	defer tick.Stop()
	var seq uint16
	for time.Since(start) < 2*time.Second {
		seq++
		// Send a valid RTP packet's observation through the actual Go
		// stream tracker used by ReceiverBase after packet authentication.
		packet := rtp.Packet{Header: rtp.Header{Version: 2, Marker: true, PayloadType: 96, SequenceNumber: seq, Timestamp: uint32(seq) * 3000, SSRC: 1}, Payload: make([]byte, 900)}
		wire, err := packet.Marshal()
		if err != nil {
			return out, err
		}
		tracker.Observe(0, len(wire), len(packet.Payload), packet.Marker, packet.Timestamp, nil)
		bitrate := tracker.BitrateTemporalCumulative()[0]
		elapsed := time.Since(start)
		if elapsed < 70*time.Millisecond {
			out.Before = bitrate
		}
		if bitrate > 0 {
			out.First, out.After = elapsed, bitrate
			manager.RemoveTracker(0)
			out.Removed = manager.GetTracker(0) == nil
			return out, nil
		}
		<-tick.C
	}
	return out, fmt.Errorf("report timeout")
}

func main() {
	rows := []result{}
	for _, item := range []struct {
		source    livekit.TrackSource
		intervals string
		min, max  time.Duration
		present   bool
	}{
		{livekit.TrackSource_CAMERA, "{0: 450ms}", 100 * time.Millisecond, 280 * time.Millisecond, true},
		{livekit.TrackSource_SCREEN_SHARE, "{0: 450ms}", 400 * time.Millisecond, 700 * time.Millisecond, true},
		{livekit.TrackSource_SCREEN_SHARE, "null", 0, 0, false},
	} {
		row, err := run(item.source, item.intervals)
		rows = append(rows, row)
		if err != nil || row.Present != item.present || (item.present && (row.First < item.min || row.First > item.max || row.Before != 0 || row.After == 0 || !row.Removed)) {
			_ = json.NewEncoder(os.Stdout).Encode(rows)
			fmt.Fprintf(os.Stderr, "unexpected real tracker report %+v error=%v\n", row, err)
			os.Exit(1)
		}
	}
	_ = json.NewEncoder(os.Stdout).Encode(rows)
}
