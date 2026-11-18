package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/livekit/livekit-server/pkg/sfu"
	"github.com/livekit/livekit-server/pkg/sfu/buffer"
	"github.com/livekit/livekit-server/pkg/sfu/testutils"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/pion/rtp/codecs"
	"github.com/pion/webrtc/v4"
)

type scenario struct {
	CodecMode   string `json:"codec_mode"`
	Reference   int32  `json:"reference"`
	MaxSpatial  int32  `json:"max_spatial"`
	Report      string `json:"report"`
	ReportLayer int32  `json:"report_layer"`
}

type observation struct {
	Stage                string `json:"stage"`
	MaxSubscribedSpatial int32  `json:"max_subscribed_spatial"`
	MaxSpatial           int32  `json:"max_spatial"`
	CurrentSpatial       int32  `json:"current_spatial"`
	TargetSpatial        int32  `json:"target_spatial"`
	Started              bool   `json:"started"`
	ReferenceSpatial     *int32 `json:"reference_spatial"`
	Muted                bool   `json:"muted"`
	PubMuted             bool   `json:"pub_muted"`
}

type row struct {
	Schema       int           `json:"schema"`
	CaseID       int           `json:"case_id"`
	Scenario     scenario      `json:"scenario"`
	Observations []observation `json:"observations"`
}

func newForwarder(mode string) *sfu.Forwarder {
	f := sfu.NewForwarder(webrtc.RTPCodecTypeVideo, logger.GetLogger(), false, true, true, nil)
	codec := webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264, ClockRate: 90000}
	layerMode := livekit.VideoLayer_ONE_SPATIAL_LAYER_PER_STREAM
	if mode == "vp9-svc" {
		codec.MimeType = webrtc.MimeTypeVP9
		layerMode = livekit.VideoLayer_MULTIPLE_SPATIAL_LAYERS_PER_STREAM
	}
	f.DetermineCodec(codec, nil, layerMode)
	return f
}

func observe(f *sfu.Forwarder, stage string) observation {
	o := observation{
		Stage: stage, MaxSubscribedSpatial: f.GetMaxSubscribedSpatial(),
		MaxSpatial: f.MaxLayer().Spatial, CurrentSpatial: f.CurrentLayer().Spatial,
		TargetSpatial: f.TargetLayer().Spatial, Muted: f.IsMuted(), PubMuted: f.IsPubMuted(),
	}
	if state := f.GetState(); state != nil {
		o.Started = state.Started
		reference := state.ReferenceLayerSpatial
		o.ReferenceSpatial = &reference
	}
	return o
}

// Only GetTranslationParams establishes the reference; ForceLayerForTest sets current/target.
func packet(f *sfu.Forwarder, mode string, layer int32, sequence uint16) error {
	videoLayer := buffer.VideoLayer{Spatial: layer, Temporal: 0}
	f.ForceLayerForTest(videoLayer)
	ep, err := testutils.GetTestExtPacket(&testutils.TestExtPacketParams{
		Marker: true, IsKeyFrame: true, PayloadType: 96, SequenceNumber: sequence,
		Timestamp: 90000 + uint32(sequence)*3000, SSRC: 1000 + uint32(layer),
		PayloadSize: 4, ArrivalTime: time.Unix(1700000000, 0), VideoLayer: videoLayer,
	})
	if err != nil {
		return err
	}
	// Minimal parsed codec fixtures; not decoder/SDP/SRTP conformance evidence.
	if mode == "vp9-svc" {
		ep.Packet.Payload = []byte{0x2c, byte(layer << 1), 0, 0}
		vp9 := &codecs.VP9Packet{}
		if _, err := vp9.Unmarshal(ep.Packet.Payload); err != nil {
			return err
		}
		ep.VP9 = vp9
	} else {
		ep.Packet.Payload = []byte{0x65, 0x88, 0x84, 0x00}
	}
	ep.RawPacket, err = ep.Packet.Marshal()
	if err != nil {
		return err
	}
	_, err = f.GetTranslationParams(ep, layer)
	return err
}

func scenarios() []scenario {
	var result []scenario
	for _, mode := range []string{"h264-simulcast", "vp9-svc"} {
		for _, reference := range []int32{0, 1, 2} {
			for _, maximum := range []int32{-1, 0, 2} {
				for _, report := range []string{"none", "wrong-layer", "matching-zero", "matching-valid", "premature-zero"} {
					reportLayer := reference
					if report == "none" {
						reportLayer = -1
					}
					if report == "wrong-layer" {
						reportLayer = (reference + 1) % 3
					}
					result = append(result, scenario{mode, reference, maximum, report, reportLayer})
				}
			}
		}
	}
	return result
}

func run(id int, s scenario) (row, error) {
	r := row{Schema: 1, CaseID: id, Scenario: s}
	f := newForwarder(s.CodecMode)
	add := func(stage string) { r.Observations = append(r.Observations, observe(f, stage)) }
	add("constructed")
	f.SetMaxSpatialLayer(2)
	if s.Report == "premature-zero" {
		// Deliberately after construction, before ForceLayerForTest and first packet.
		f.SetRefSenderReport(s.ReportLayer, &livekit.RTCPSenderReportState{})
	}
	add("before_first_packet")
	if err := packet(f, s.CodecMode, s.Reference, 1); err != nil {
		return r, err
	}
	add("first_packet")
	f.SetMaxSpatialLayer(s.MaxSpatial)
	add("set_max")
	switch s.Report {
	case "matching-zero":
		f.SetRefSenderReport(s.ReportLayer, &livekit.RTCPSenderReportState{})
	case "wrong-layer", "matching-valid":
		f.SetRefSenderReport(s.ReportLayer, &livekit.RTCPSenderReportState{
			NtpTimestamp: 0xe900000000000000, RtpTimestamp: 93000, RtpTimestampExt: 93000,
		})
	}
	add("report")
	f.Resync()
	add("resync")
	f.Resync()
	add("resync_again")
	f.Mute(true, true)
	add("muted")
	f.Mute(false, true)
	add("unmuted")
	f.PubMute(true)
	add("pub_muted")
	f.PubMute(false)
	add("pub_unmuted")
	// Non-nil zero-valued SR is intentional: hold release is not NTP/age validation.
	f.SetRefSenderReport(s.Reference, &livekit.RTCPSenderReportState{})
	add("matching_zero_sr")
	f.Resync()
	add("matching_sr_resync")
	f.Restart()
	add("restart")
	if err := packet(f, s.CodecMode, s.Reference, 2); err != nil {
		return r, err
	}
	add("restart_packet")
	f.Resync()
	add("restart_resync")
	// Leave the old object holding both a reference and a report before replacing it.
	f.SetRefSenderReport(s.Reference, &livekit.RTCPSenderReportState{})
	add("before_fresh")
	// New object must not inherit reference/SR from the earlier one.
	f = newForwarder(s.CodecMode)
	f.SetMaxSpatialLayer(s.MaxSpatial)
	add("fresh")
	if err := packet(f, s.CodecMode, s.Reference, 1); err != nil {
		return r, err
	}
	add("fresh_packet")
	f.Resync()
	add("fresh_resync")
	return r, nil
}

func main() {
	encoder := json.NewEncoder(os.Stdout)
	for id, s := range scenarios() {
		r, err := run(id, s)
		if err == nil {
			err = encoder.Encode(r)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}
