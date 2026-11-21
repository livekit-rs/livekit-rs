// Copyright LiveKit, Apache-2.0. Observer for the unchanged locked RED encoder: it calls
// RedReceiver.ForwardRTP and records every byte actually forwarded. No expectation is baked
// into the Go side; the Rust differential lives in vendor/str0m/src/packet/red.rs.
package sfu

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"

	"github.com/livekit/livekit-server/pkg/sfu/buffer"
	"github.com/livekit/protocol/logger"
)

type sourceRedVector struct {
	name  string
	steps []*rtp.Packet
}

// newSourceRedReceiver builds the wrapper exactly like the locked redreceiver_test.go does: a
// WebRTCReceiver whose isRED flag flips makes GetRedReceiver return a *RedReceiver encoder.
func newSourceRedReceiver(t *testing.T) (*RedReceiver, *dummyDowntrack) {
	t.Helper()
	dt := &dummyDowntrack{TrackSender: &DownTrack{}}
	w := &WebRTCReceiver{ReceiverBase: &ReceiverBase{
		params: ReceiverBaseParams{
			Kind:   webrtc.RTPCodecTypeAudio,
			Logger: logger.GetLogger(),
		},
		isRED: true,
	}}
	if w.GetRedReceiver() != w.ReceiverBase {
		t.Fatal("locked receiver already claimed to be RED")
	}
	w.isRED = false
	red, ok := w.GetRedReceiver().(*RedReceiver)
	if !ok {
		t.Fatalf("locked GetRedReceiver returned %T, want *RedReceiver", w.GetRedReceiver())
	}
	if err := red.AddDownTrack(dt); err != nil {
		t.Fatal(err)
	}
	return red, dt
}

// sourceRedVectors are the frozen vectors of TestRedReceiver (normal, packet lost and jump,
// unorder and repeat, encoding exceed space, large timestamp gap), reconstructed with the
// locked generatePkts helper, plus one vector for the >= maxRedPayload broadcast branch that the
// frozen table does not cover.
func sourceRedVectors() []sourceRedVector {
	base := rtp.Header{SequenceNumber: 65534, Timestamp: (uint32(1) << 31) - 2*tsStep, PayloadType: opusPT}
	vectors := []sourceRedVector{{name: "normal", steps: generatePkts(base, 10, tsStep)}}

	// Frozen "packet lost and jump": every even header advances without being forwarded, then a
	// 10-sequence jump follows the last sent packet.
	header := base
	lost := make([]*rtp.Packet, 0, 8)
	for i := 0; i < 10; i++ {
		if i%2 == 0 {
			header.SequenceNumber++
			header.Timestamp += tsStep
			continue
		}
		hbuf, err := header.Marshal()
		if err != nil {
			panic(err)
		}
		lost = append(lost, &rtp.Packet{Header: header, Payload: hbuf})
		header.SequenceNumber++
		header.Timestamp += tsStep
	}
	header.SequenceNumber += 10
	header.Timestamp += 10 * tsStep
	lost = append(lost, generatePkts(header, 3, tsStep)...)
	vectors = append(vectors, sourceRedVector{name: "packet_lost_and_jump", steps: lost})

	// Frozen "unorder and repeat": after ten consecutive packets an out-of-window old packet is
	// replayed, then the newest packet is replayed.
	pkts := generatePkts(base, 10, tsStep)
	unorder := append([]*rtp.Packet{}, pkts...)
	unorder = append(unorder, pkts[len(pkts)-3], pkts[len(pkts)-1])
	vectors = append(vectors, sourceRedVector{name: "unorder_and_repeat", steps: unorder})

	// Frozen "encoding exceed space": 1000-byte payloads leave no MTU budget for redundancy.
	header = base
	budget := make([]*rtp.Packet, 0, 5)
	for i := 0; i < 5; i++ {
		pkt := rtp.Packet{Header: header, Payload: bytes.Repeat([]byte{byte(i)}, 1000)}
		budget = append(budget, &pkt)
		header.SequenceNumber++
		header.Timestamp += tsStep
	}
	vectors = append(vectors, sourceRedVector{name: "encoding_exceed_space", steps: budget})

	// Frozen "large timestamp gap": generatePkts takes the header by value, so the second batch
	// restarts at the base sequence number with a 40x timestamp step.
	gap := generatePkts(base, 4, tsStep)
	gap = append(gap, generatePkts(base, 4, 40*tsStep)...)
	vectors = append(vectors, sourceRedVector{name: "large_timestamp_gap", steps: gap})

	// Not frozen: a payload of maxRedPayload bytes or more must bypass RED and keep the primary
	// payload type; the two frames after it prove the bypassed frame is not window history.
	header = base
	oversized := make([]*rtp.Packet, 0, 4)
	for i, size := range []int{maxRedPayload, maxRedPayload - 1, 20, 20} {
		pkt := rtp.Packet{Header: header, Payload: bytes.Repeat([]byte{byte(i)}, size)}
		oversized = append(oversized, &pkt)
		header.SequenceNumber++
		header.Timestamp += tsStep
	}
	vectors = append(vectors, sourceRedVector{name: "oversized_frame", steps: oversized})

	return vectors
}

func TestOriginalRedEncodeSource(t *testing.T) {
	// The Rust sender pins its distances and buffer to these locked constants; if upstream moves
	// either one, this observation no longer describes the behaviour being compared.
	if maxRedCount != 2 {
		t.Fatalf("locked maxRedCount = %d, Rust RED distances are pinned to 2 levels", maxRedCount)
	}
	if mtuSize != 1500 {
		t.Fatalf("locked mtuSize = %d, Rust RED budget is pinned to 1500 bytes", mtuSize)
	}
	for _, vector := range sourceRedVectors() {
		t.Run(vector.name, func(t *testing.T) {
			red, dt := newSourceRedReceiver(t)
			for i, pkt := range vector.steps {
				before := len(dt.receivedPkts)
				forwarded := red.ForwardRTP(&buffer.ExtPacket{Packet: pkt}, 0)
				sent := len(dt.receivedPkts) - before
				if sent != 1 {
					t.Fatalf("%s frame %d: locked RED forwarded %d downtrack writes, want 1 (n=%d)",
						vector.name, i, sent, forwarded)
				}
				// RedReceiver reuses one rtp.Packet and one scratch buffer, so the row must be
				// materialised before the next ForwardRTP call.
				out := dt.receivedPkts[len(dt.receivedPkts)-1]
				row := map[string]any{
					"case":            vector.name,
					"index":           i,
					"seq":             pkt.SequenceNumber,
					"ts":              pkt.Timestamp,
					"payload_hex":     hex.EncodeToString(pkt.Payload),
					"marker":          pkt.Marker,
					"out_seq":         out.SequenceNumber,
					"out_ts":          out.Timestamp,
					"out_marker":      out.Marker,
					"out_pt":          out.PayloadType,
					"out_payload_hex": hex.EncodeToString(out.Payload),
				}
				raw, err := json.Marshal(row)
				if err != nil {
					t.Fatal(err)
				}
				fmt.Println("SOURCE_RED_ENCODE " + string(raw))
			}
		})
	}
}
