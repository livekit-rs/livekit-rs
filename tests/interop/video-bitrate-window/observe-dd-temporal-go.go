//go:build ignore

// Run from the pinned LiveKit module.
package main

import (
	"encoding/hex"
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
	"github.com/pion/webrtc/v4"
)

func run(includeEnhancement bool) (map[string]any, error) {
	result := map[string]any{"include_enhancement": includeEnhancement}
	cfg, err := config.NewConfig("rtc: {node_ip: 127.0.0.1}\nvideo:\n  stream_tracker_manager:\n    video:\n      bitrate_report_interval: {0: 125ms}\n", true, nil, nil)
	if err != nil {
		return result, err
	}
	l := logger.GetLogger()
	manager := sfu.NewStreamTrackerManager(l, &livekit.TrackInfo{
		Type: livekit.TrackType_VIDEO, Source: livekit.TrackSource_CAMERA,
		Codecs: []*livekit.SimulcastCodecInfo{{
			MimeType: "video/VP9", VideoLayerMode: livekit.VideoLayer_MULTIPLE_SPATIAL_LAYERS_PER_STREAM,
		}},
	}, mime.MimeTypeVP9, 90000, cfg.Video.StreamTrackerManager)
	defer manager.Close()
	defer manager.RemoveAllTrackers()
	manager.AddDependencyDescriptorTrackers()
	track := manager.GetTracker(0)
	if track == nil {
		return result, fmt.Errorf("aggregate tracker is missing")
	}
	templates := []*dd.FrameDependencyTemplate{
		{DecodeTargetIndications: []dd.DecodeTargetIndication{dd.DecodeTargetSwitch, dd.DecodeTargetSwitch, dd.DecodeTargetNotPresent}},
		{TemporalId: 1, DecodeTargetIndications: []dd.DecodeTargetIndication{dd.DecodeTargetNotPresent, dd.DecodeTargetNotPresent, dd.DecodeTargetSwitch}},
	}
	structure := &dd.FrameDependencyStructure{NumDecodeTargets: 3, Templates: templates}
	parser := buffer.NewDependencyDescriptorParser(1, l, func(int32, int32) {}, false)
	var sizes [2]int
	var descriptors [2]string
	for i, size := range []int{90, 140} {
		if i == 1 && !includeEnhancement {
			break
		}
		seq := uint16(i + 1)
		d := &dd.DependencyDescriptor{FirstPacketInFrame: true, LastPacketInFrame: true, FrameNumber: seq, FrameDependencies: templates[i]}
		if i == 0 {
			d.AttachedStructure = structure
		}
		wireDD, err := (&dd.DependencyDescriptorExtension{Descriptor: d, Structure: structure}).Marshal()
		if err != nil {
			return result, err
		}
		descriptors[i] = hex.EncodeToString(wireDD)
		pkt := rtp.Packet{
			Header:  rtp.Header{Version: 2, Marker: true, PayloadType: 96, SequenceNumber: seq, Timestamp: uint32(seq) * 3000, SSRC: 1},
			Payload: make([]byte, size),
		}
		if err := pkt.SetExtension(1, wireDD); err != nil {
			return result, err
		}
		raw, err := pkt.Marshal()
		if err != nil {
			return result, err
		}
		sizes[i] = len(raw)
		parsed, layer, err := parser.Parse(&pkt)
		if err != nil || parsed == nil || int(layer.Temporal) != i {
			return result, fmt.Errorf("unexpected real parsed target %d: %w", i, err)
		}
		track.Observe(layer.Temporal, len(raw), len(pkt.Payload), true, pkt.Timestamp, parsed)
		buffer.ReleaseExtDependencyDescriptor(parsed)
	}
	var rates [4]int64
	var layered sfu.Bitrates
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		_, layered = manager.GetLayeredBitrate()
		rates = layered[0]
		if rates[0] > 0 && (!includeEnhancement || rates[1] > 0) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	// Each of the two distinct T0 decode targets counts packet one. The
	// isolated T1 cell counts only packet two. The same elapsed denominator
	// cancels in this ratio, allowing only integer truncation.
	t0, t1 := int64(2*sizes[0]), int64(sizes[1])
	diff := rates[0]*t1 - rates[1]*t0
	result["descriptor_hex"] = descriptors
	result["wire_sizes"] = sizes
	result["temporal_bytes"] = [4]int64{t0, t1, 0, 0}
	result["temporal_bitrates"] = rates
	if rates[0] <= 0 || (rates[1] > 0) != includeEnhancement ||
		rates[2] != 0 || rates[3] != 0 || diff >= t0+t1 || diff <= -t0-t1 {

		return result, fmt.Errorf("Go did not preserve independent target costs")
	}
	f := sfu.NewForwarder(webrtc.RTPCodecTypeVideo, l, false, false, false, nil)
	f.SetMaxPublishedLayer(0)
	f.SetMaxSpatialLayer(0)
	f.SetMaxTemporalLayer(3)
	result["default_temporal_budget"] = f.GetOptimalBandwidthNeeded(layered)
	f.SetMaxTemporalLayer(0)
	result["base_temporal_budget"] = f.GetOptimalBandwidthNeeded(layered)
	highest := rates[1]
	if !includeEnhancement {
		highest = rates[0]
	}
	if result["default_temporal_budget"] != highest || result["base_temporal_budget"] != rates[0] {
		return result, fmt.Errorf("Go forwarder budget did not select the requested temporal cell")
	}
	manager.RemoveAllTrackers()
	result["retired"] = manager.GetTracker(0) == nil
	if result["retired"] != true {
		return result, fmt.Errorf("Go tracker was not retired")
	}
	return result, nil
}

func main() {
	enc := json.NewEncoder(os.Stdout)
	for _, includeEnhancement := range []bool{true, false} {
		row, err := run(includeEnhancement)
		_ = enc.Encode(row)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}
