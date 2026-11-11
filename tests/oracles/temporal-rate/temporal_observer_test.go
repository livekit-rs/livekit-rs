// Copyright 2026 livekit-rs contributors. Apache-2.0 source observer.
package rtc

import (
	"fmt"
	"github.com/livekit/livekit-server/pkg/sfu"
	"github.com/livekit/livekit-server/pkg/sfu/buffer"
	"github.com/livekit/livekit-server/pkg/sfu/videolayerselector"
	"github.com/livekit/protocol/codecs/mime"
	"github.com/livekit/protocol/logger"
	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
	"testing"
)

type sourceTemporalReceiver struct {
	sfu.TrackReceiver
	rates []float32
}

func (r sourceTemporalReceiver) Mime() mime.MimeType                             { return mime.MimeTypeVP9 }
func (r sourceTemporalReceiver) GetTemporalLayerFpsForSpatial(_ int32) []float32 { return r.rates }
func TestOriginalPublicTemporalCeilConsumer(t *testing.T) {
	for _, rates := range [][]float32{{3.75, 7.5, 15, 0}, {7.5, 15, 30, 0}, {0, 0, 0, 0}, {3.75, 0, 15, 30}} {
		receiver := &MediaTrackReceiver{receivers: []*simulcastReceiver{{TrackReceiver: sourceTemporalReceiver{rates: rates}}}}
		for _, fps := range []uint32{0, 1, 4, 5, 8, 9, 15, 17, 30, 60} {
			actual := receiver.GetTemporalLayerForSpatialFps(mime.MimeTypeVP9, 0, fps)
			expected := int32(buffer.DefaultMaxLayerTemporal)
			for i, rate := range rates {
				if float32(fps)*0.9 <= rate {
					expected = int32(i)
					break
				}
			}
			if actual != expected {
				t.Fatalf("public consumer %v %d got%d want%d", rates, fps, actual, expected)
			}
			fmt.Printf("TEMPORAL_CEIL rates=%v requested=%d actual=%d\n", rates, fps, actual)
		}
	}
}
func TestOriginalVP9CalculatorFeedsPublicTemporalConsumer(t *testing.T) {
	calc := buffer.NewFrameRateCalculatorVP9(90000, logger.GetLogger())
	for frame := 0; frame < 64; frame++ {
		temporal := int32(2)
		if frame%4 == 0 {
			temporal = 0
		} else if frame%2 == 0 {
			temporal = 1
		}
		for spatial := int32(0); spatial < 3; spatial++ {
			calc.RecvPacket(&buffer.ExtPacket{VideoLayer: buffer.VideoLayer{Spatial: spatial, Temporal: temporal}, Packet: &rtp.Packet{Header: rtp.Header{Timestamp: uint32(3000 + frame*6000), SequenceNumber: uint16(frame*3 + int(spatial))}}, VP9: &codecs.VP9Packet{I: true, PictureID: uint16(frame)}})
		}
	}
	rates := calc.GetFrameRateCalculatorForSpatial(0).GetFrameRate()
	if len(rates) != 4 || rates[0] != 3.75 || rates[1] != 7.5 || rates[2] != 15 || rates[3] != 0 {
		t.Fatalf("original measured rates %v", rates)
	}
	receiver := &MediaTrackReceiver{receivers: []*simulcastReceiver{{TrackReceiver: sourceTemporalReceiver{rates: rates}}}}
	for _, row := range []struct {
		request uint32
		want    int32
	}{{1, 0}, {5, 1}, {15, 2}, {17, 3}} {
		got := receiver.GetTemporalLayerForSpatialFps(mime.MimeTypeVP9, 0, row.request)
		if got != row.want {
			t.Fatalf("actual rates %v req%d got%d want%d", rates, row.request, got, row.want)
		}
		fmt.Printf("MEASURED_TEMPORAL rates=%v requested=%d actual=%d\n", rates, row.request, got)
	}
}

func TestOriginalVP9TemporalOnlyChangeDoesNotRequestKeyframe(t *testing.T) {
	selector := videolayerselector.NewVP9(logger.GetLogger())
	selector.SetRequestSpatial(2)
	selector.SetTarget(buffer.VideoLayer{Spatial: 2, Temporal: 3})
	selector.SetCurrent(buffer.VideoLayer{Spatial: 2, Temporal: 0})
	locked, layer := selector.CheckSync()
	if !locked || layer != 2 {
		t.Fatalf("original temporal-only CheckSync %v %d", locked, layer)
	}
	fmt.Printf("TEMPORAL_SYNC requested=3 current=0 spatial=2 locked=%t\n", locked)
	selector.SetRequestSpatial(0)
	locked, _ = selector.CheckSync()
	if locked {
		t.Fatal("actual spatial mismatch must remain unlocked")
	}
}
