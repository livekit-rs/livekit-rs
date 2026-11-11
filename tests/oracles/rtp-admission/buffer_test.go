// Copyright LiveKit, Apache-2.0. Original BufferBase consumer, not a reimplemented restart.
package buffer

import (
	"encoding/json"
	"fmt"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"testing"
)

func TestOriginalRtpAdmissionBufferRestartSource(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			b := NewBuffer(123, 100, 100)
			b.SetStreamRestartDetection(enabled)
			b.OnRtcpFeedback(func(_ []rtcp.Packet) {})
			if err := b.Bind(webrtc.RTPParameters{Codecs: []webrtc.RTPCodecParameters{opusCodec}}, opusCodec.RTPCodecCapability, 0); err != nil {
				t.Fatal(err)
			}
			defer b.Close()

			for i := 0; i < 7; i++ {
				sn := uint16(1000 + i)
				ts := uint32(i * 1000)
				if i == 0 {
					ts = 90000
				}
				if i == 6 {
					ts = 8000
				}
				p := &rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 111, SequenceNumber: sn, Timestamp: ts, SSRC: 123}, Payload: []byte{0xf8, 0xff, 0xfe}}
				raw, e := p.Marshal()
				if e != nil {
					t.Fatal(e)
				}
				old := b.rtpStats
				seq, err := b.HandleIncomingPacket(raw, p, 1000000000+int64(i)*10000000, false, false, nil, 0)
				msg := ""
				if err != nil {
					msg = err.Error()
				}
				row := map[string]any{"enabled": enabled, "index": i, "sn": sn, "ts": ts, "seq": seq, "error": msg, "stats_replaced": old != b.rtpStats, "highest": b.rtpStats.ExtendedHighestSequenceNumber()}
				data, e := json.Marshal(row)
				if e != nil {
					t.Fatal(e)
				}
				fmt.Println("SOURCE_RTP_BUFFER_RESTART " + string(data))
				if i == 5 && (old != b.rtpStats) != enabled {
					t.Fatal("original enabled consumer did not match actual stats replacement")
				}
			}
		})
	}
}
