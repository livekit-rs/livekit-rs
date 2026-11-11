//go:build ignore

// Run from the pinned LiveKit module.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/sfu"
	"github.com/livekit/livekit-server/pkg/sfu/buffer"
	dd "github.com/livekit/livekit-server/pkg/sfu/rtpextension/dependencydescriptor"
	"github.com/livekit/protocol/codecs/mime"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/pion/rtp"
)

type result struct {
	Source                  string
	Intervals               string
	LowPresent, HighPresent bool
	Low, High               int64
	FirstLow, FirstHigh     time.Duration
	Removed                 bool
	WireBytes               *[2]int64   `json:",omitempty"`
	PauseEpochs             []rateEpoch `json:",omitempty"`
}

type rateEpoch struct {
	Phase     string
	Low, High int64
}

func run(source livekit.TrackSource, intervals, mode string) (result, error) {
	result := result{Source: source.String(), Intervals: intervals}
	if source == livekit.TrackSource_CAMERA {
		result.Intervals = "{0: 125ms, 1: 900ms}"
	}
	body := "rtc: {node_ip: 127.0.0.1}\nvideo:\n  stream_tracker_manager:\n    video:\n      bitrate_report_interval: {0: 125ms, 1: 900ms}\n    screenshare:\n      bitrate_report_interval: " + intervals + "\n"
	cfg, err := config.NewConfig(body, true, nil, nil)
	if err != nil {
		return result, err
	}
	l := logger.GetLogger()
	track := &livekit.TrackInfo{
		Type:   livekit.TrackType_VIDEO,
		Source: source,
		Codecs: []*livekit.SimulcastCodecInfo{{
			MimeType:       "video/VP9",
			VideoLayerMode: livekit.VideoLayer_MULTIPLE_SPATIAL_LAYERS_PER_STREAM,
		}},
	}
	manager := sfu.NewStreamTrackerManager(l, track, mime.MimeTypeVP9, 90000, cfg.Video.StreamTrackerManager)
	defer manager.Close()
	defer manager.RemoveAllTrackers()
	manager.AddDependencyDescriptorTrackers()
	low, high := manager.GetTracker(0), manager.GetTracker(1)
	result.LowPresent, result.HighPresent = low != nil, high != nil
	if low == nil || high == nil {
		return result, nil
	}
	t0 := &dd.FrameDependencyTemplate{DecodeTargetIndications: []dd.DecodeTargetIndication{dd.DecodeTargetSwitch, dd.DecodeTargetSwitch}}
	t1 := &dd.FrameDependencyTemplate{SpatialId: 1, DecodeTargetIndications: []dd.DecodeTargetIndication{dd.DecodeTargetNotPresent, dd.DecodeTargetSwitch}}
	structure := &dd.FrameDependencyStructure{NumDecodeTargets: 2, Templates: []*dd.FrameDependencyTemplate{t0, t1}}
	parser := buffer.NewDependencyDescriptorParser(1, l, func(int32, int32) {}, false)
	checkWireBytes := mode == "wire"
	send := func(seq uint16, attached bool) (int, error) {
		template := t0
		if !attached && seq%2 == 0 {
			template = t1
		}
		d := &dd.DependencyDescriptor{FirstPacketInFrame: true, LastPacketInFrame: true, FrameNumber: seq, FrameDependencies: template}
		if attached {
			d.AttachedStructure = structure
		}
		ext, err := (&dd.DependencyDescriptorExtension{Descriptor: d, Structure: structure}).Marshal()
		if err != nil {
			return 0, err
		}
		payloadSize := 900
		if checkWireBytes {
			payloadSize = 20
		}
		pkt := rtp.Packet{Header: rtp.Header{Version: 2, Marker: true, PayloadType: 96, SequenceNumber: seq, Timestamp: uint32(seq) * 3000, SSRC: 1}, Payload: make([]byte, payloadSize)}
		if err := pkt.SetExtension(1, ext); err != nil {
			return 0, err
		}
		if checkWireBytes && seq == 2 {
			// Same payload as packet one, with a different complete RTP size.
			if err := pkt.SetExtension(2, make([]byte, 12)); err != nil {
				return 0, err
			}
		}
		wire, err := pkt.Marshal()
		if err != nil {
			return 0, err
		}
		parsed, layer, err := parser.Parse(&pkt)
		if err != nil || parsed == nil {
			return 0, fmt.Errorf("parse real descriptor: %w", err)
		}
		low.Observe(layer.Temporal, len(wire), len(pkt.Payload), true, pkt.Timestamp, parsed)
		buffer.ReleaseExtDependencyDescriptor(parsed)
		return len(wire), nil
	}
	if mode == "pause" {
		observe := func(phase string) rateEpoch {
			row := rateEpoch{phase, low.BitrateTemporalCumulative()[0], high.BitrateTemporalCumulative()[0]}
			result.PauseEpochs = append(result.PauseEpochs, row)
			return row
		}
		waitReport := func() bool {
			deadline := time.Now().Add(time.Second)
			for time.Now().Before(deadline) {
				if low.BitrateTemporalCumulative()[0] > 0 && high.BitrateTemporalCumulative()[0] > 0 {
					return true
				}
				time.Sleep(time.Millisecond)
			}
			return false
		}
		if _, err := send(1, true); err != nil {
			return result, err
		}
		if !waitReport() {
			return result, fmt.Errorf("initial original epoch did not report")
		}
		observe("original")
		track.Muted = true
		manager.UpdateTrackInfo(track)
		for seq := uint16(2); seq <= 10; seq++ {
			if _, err := send(seq, false); err != nil {
				return result, err
			}
			time.Sleep(15 * time.Millisecond)
		}
		time.Sleep(180 * time.Millisecond)
		if rates := observe("muted-drained"); rates.Low != 0 || rates.High != 0 {
			return result, fmt.Errorf("paused Go tracker counted incoming media")
		}
		track.Muted = false
		manager.UpdateTrackInfo(track)
		if rates := observe("unmuted"); rates.Low != 0 || rates.High != 0 {
			return result, fmt.Errorf("resume did not clear old rate")
		}
		for seq := uint16(11); seq <= 20; seq++ {
			if _, err := send(seq, false); err != nil {
				return result, err
			}
			time.Sleep(15 * time.Millisecond)
		}
		time.Sleep(150 * time.Millisecond)
		if rates := observe("bare-continuations"); rates.Low != 0 || rates.High != 0 {
			return result, fmt.Errorf("bare descriptors restarted Go epoch")
		}
		start := time.Now()
		if _, err := send(21, true); err != nil {
			return result, err
		}
		if !waitReport() || time.Since(start) < 110*time.Millisecond {
			return result, fmt.Errorf("fresh descriptor did not start a new Go window")
		}
		result.FirstLow = time.Since(start)
		observe("fresh-target-epoch")
		manager.RemoveAllTrackers()
		result.Removed = manager.GetTracker(0) == nil && manager.GetTracker(1) == nil
		return result, nil
	}
	ticker := time.NewTicker(15 * time.Millisecond)
	defer ticker.Stop()
	start := time.Now()
	var wireBytes [2]int64
	for seq := uint16(1); time.Since(start) < 2*time.Second; seq++ {
		wireSize, err := send(seq, seq == 1)
		if err != nil {
			return result, err
		}
		if checkWireBytes {
			wireBytes[1] += int64(wireSize)
			if seq == 1 {
				wireBytes[0] += int64(wireSize)
				continue
			}
			// Use only these two packets in one real tracker interval so
			// their rate ratio cancels Go's measured wall-clock duration.
			for time.Since(start) < 2*time.Second {
				result.Low = low.BitrateTemporalCumulative()[0]
				result.High = high.BitrateTemporalCumulative()[0]
				if result.Low > 0 && result.High > 0 {
					break
				}
				time.Sleep(time.Millisecond)
			}
			result.WireBytes = &wireBytes
			difference := result.Low*wireBytes[1] - result.High*wireBytes[0]
			if result.Low <= 0 || result.High <= 2*result.Low ||
				difference >= wireBytes[0]+wireBytes[1] || difference <= -wireBytes[0]-wireBytes[1] {
				return result, fmt.Errorf("original DD rates do not match the wire byte ratio")
			}
			manager.RemoveAllTrackers()
			result.Removed = manager.GetTracker(0) == nil && manager.GetTracker(1) == nil
			return result, nil
		}
		elapsed := time.Since(start)
		if got := low.BitrateTemporalCumulative()[0]; got > 0 && result.FirstLow == 0 {
			result.Low, result.FirstLow = got, elapsed
		}
		if got := high.BitrateTemporalCumulative()[0]; got > 0 && result.FirstHigh == 0 {
			result.High, result.FirstHigh = got, elapsed
		}
		if result.FirstLow != 0 && result.FirstHigh != 0 {
			manager.RemoveAllTrackers()
			result.Removed = manager.GetTracker(0) == nil && manager.GetTracker(1) == nil
			return result, nil
		}
		<-ticker.C
	}
	return result, fmt.Errorf("aggregate bitrate report timeout")
}

func main() {
	rows := []result{}
	for _, tc := range []struct {
		source    livekit.TrackSource
		intervals string
		min, max  time.Duration
	}{
		{livekit.TrackSource_CAMERA, "{0: 450ms, 1: 900ms}", 110 * time.Millisecond, 310 * time.Millisecond},
		{livekit.TrackSource_SCREEN_SHARE, "{0: 450ms, 1: 900ms}", 420 * time.Millisecond, 650 * time.Millisecond},
		{livekit.TrackSource_SCREEN_SHARE, "null", 0, 0},
	} {
		row, err := run(tc.source, tc.intervals, "clock")
		rows = append(rows, row)
		if err != nil || (tc.min == 0 && (row.LowPresent || row.HighPresent)) || (tc.min > 0 && (!row.LowPresent || !row.HighPresent || !row.Removed || row.Low <= 0 || row.High <= row.Low || row.FirstLow < tc.min || row.FirstLow > tc.max || row.FirstHigh != row.FirstLow)) {
			_ = json.NewEncoder(os.Stdout).Encode(rows)
			fmt.Fprintf(os.Stderr, "unexpected original DD report: %+v error=%v\n", row, err)
			os.Exit(1)
		}
	}
	wire, err := run(livekit.TrackSource_CAMERA, "{0: 450ms, 1: 900ms}", "wire")
	rows = append(rows, wire)
	if err != nil || !wire.Removed {
		_ = json.NewEncoder(os.Stdout).Encode(rows)
		fmt.Fprintf(os.Stderr, "unexpected original DD wire rate: %+v error=%v\n", wire, err)
		os.Exit(1)
	}
	paused, err := run(livekit.TrackSource_CAMERA, "{0: 450ms, 1: 900ms}", "pause")
	rows = append(rows, paused)
	_ = json.NewEncoder(os.Stdout).Encode(rows)
	if err != nil || !paused.Removed {
		fmt.Fprintf(os.Stderr, "unexpected original DD pause lifecycle: %+v error=%v\n", paused, err)
		os.Exit(1)
	}
}
