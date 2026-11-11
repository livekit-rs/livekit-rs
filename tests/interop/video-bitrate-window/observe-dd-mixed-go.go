//go:build ignore

// Run from the pinned LiveKit module with go run -mod=readonly.

package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
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

type packetRow struct {
	Sequence  uint16
	Timestamp uint32
	Temporal  int32
	Wire      int
	Payload   int
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	cfg, err := config.NewConfig("rtc: {node_ip: 127.0.0.1}\nvideo:\n  stream_tracker_manager:\n    video:\n      bitrate_report_interval: {0: 500ms}\n", true, nil, nil)
	check(err)
	manager := sfu.NewStreamTrackerManager(logger.GetLogger(), &livekit.TrackInfo{
		Type: livekit.TrackType_VIDEO, Source: livekit.TrackSource_CAMERA,
		Codecs: []*livekit.SimulcastCodecInfo{{
			MimeType: "video/VP9", VideoLayerMode: livekit.VideoLayer_MULTIPLE_SPATIAL_LAYERS_PER_STREAM,
		}},
	}, mime.MimeTypeVP9, 90000, cfg.Video.StreamTrackerManager)
	defer manager.Close()
	defer manager.RemoveAllTrackers()
	manager.AddDependencyDescriptorTrackers()
	tracker := manager.GetTracker(0)

	primary := buffer.NewBuffer(1, 100, 100)
	defer primary.Close()
	repair := buffer.NewBuffer(2, 100, 100)
	defer repair.Close()
	repair.SetPrimaryBufferForRTX(primary)
	vp9 := webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP9, ClockRate: 90000}
	rtx := webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeRTX, ClockRate: 90000, SDPFmtpLine: "apt=96"}
	check(primary.Bind(webrtc.RTPParameters{
		Codecs: []webrtc.RTPCodecParameters{
			{RTPCodecCapability: vp9, PayloadType: 96},
			{RTPCodecCapability: rtx, PayloadType: 97},
		},
		HeaderExtensions: []webrtc.RTPHeaderExtensionParameter{{URI: dd.ExtensionURI, ID: 1}},
	}, vp9, 0))
	trackRows := make(chan packetRow, 12)
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		for {
			ep, err := primary.ReadExtended(make([]byte, 1500))
			if err == io.EOF {
				return
			}
			check(err)
			if ep == nil {
				continue
			}
			row := packetRow{ep.Packet.SequenceNumber, ep.Packet.Timestamp, ep.Temporal, len(ep.RawPacket), len(ep.Packet.Payload)}
			tracker.Observe(ep.Temporal, row.Wire, row.Payload, ep.Packet.Marker, row.Timestamp, ep.DependencyDescriptor)
			buffer.ReleaseExtPacket(ep)
			trackRows <- row
		}
	}()

	wire := map[string]int{}
	send := func(name string, ssrc uint32, pt uint8, seq uint16, timestamp uint32, d []byte, payload []byte) {
		packet := rtp.Packet{
			Header:  rtp.Header{Version: 2, SSRC: ssrc, PayloadType: pt, SequenceNumber: seq, Timestamp: timestamp, Marker: len(payload) > 0},
			Payload: payload,
		}
		if len(payload) == 0 {
			packet.Header.Padding = true
			packet.Header.PaddingSize = 1
		}
		if len(d) != 0 {
			check(packet.SetExtension(1, d))
		}
		raw, err := packet.Marshal()
		check(err)
		wire[name] = len(raw)
		if ssrc == 1 {
			_, err = primary.Write(raw)
		} else {
			_, err = repair.Write(raw)
		}
		check(err)
	}
	payload := func(frame byte, size int) []byte {
		body := make([]byte, size)
		copy(body, []byte{0x9c, 0x81, frame, 0, 0x34, 0})
		return body
	}
	send("base-first", 1, 96, 100, 3000, []byte{0xc0, 0, 1, 0x80, 2, 0x7a, 2, 0}, payload(1, 90))
	send("base-ahead", 1, 96, 102, 9000, []byte{0xc0, 0, 3}, payload(3, 90))
	repaired := make([]byte, 2)
	binary.BigEndian.PutUint16(repaired, 101)
	repaired = append(repaired, payload(2, 140)...)
	send("actual-repair", 2, 97, 500, 6000, []byte{0xc1, 0, 2}, repaired)
	send("duplicate-repair", 2, 97, 501, 6000, []byte{0xc1, 0, 2}, repaired)
	send("primary-padding", 1, 96, 103, 12000, nil, nil)
	send("rtx-padding", 2, 97, 502, 12000, nil, nil)
	var rows []packetRow
	for range 3 {
		select {
		case row := <-trackRows:
			rows = append(rows, row)
		case <-time.After(time.Second):
			panic(fmt.Sprintf("missing actual packet %v, wire %v", rows, wire))
		}
	}
	var rates [4]int64
	deadline := time.After(time.Second)
	for rates[0] == 0 || rates[1] == 0 {
		select {
		case row := <-trackRows:
			panic(fmt.Sprintf("extra packet passed buffer %v", row))
		case <-deadline:
			panic(fmt.Sprintf("missing rates %v rows %v", rates, rows))
		case <-time.After(time.Millisecond):
			_, br := manager.GetLayeredBitrate()
			rates = br[0]
		}
	}
	check(primary.Close())
	<-readDone
	want0 := int64(2 * (wire["base-first"] + wire["base-ahead"]))
	want1 := int64(wire["actual-repair"] - 2)
	out := map[string]any{"wire_sizes": wire, "buffer_rows": rows, "logical_temporal_bytes": []int64{want0, want1}, "rates": rates}
	check(json.NewEncoder(os.Stdout).Encode(out))
	if rows[0].Sequence != 100 || rows[1].Sequence != 102 || rows[2].Sequence != 101 || rows[2].Wire != int(want1) || rows[2].Payload != 140 {
		panic("unexpected actual buffer RTX normalization")
	}
	difference := rates[0]*want1 - rates[1]*want0
	if difference >= want0+want1 || difference <= -want0-want1 || rates[2] != 0 || rates[3] != 0 {
		panic("padding or duplicated/outer RTX bytes affected DD temporal cost")
	}
}
