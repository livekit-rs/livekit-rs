// Copyright 2026 livekit-rs contributors. Apache-2.0.
//
// NEWLY AUTHORED oracle producer. The source packet plan below is transcribed
// from the Rust consumer crates/rtc-media/src/regression/dd_disappearance.rs:300-385
// (pre-roll, seven media sends, DD attachment/keyframe schedule, retired-source
// phase) and :124-138 (coded body). Every emitted value is measured from real Go
// execution: `writes` is the original DownTrack.WriteRTP return value, and
// `sequence` / `timestamp` / `payload_hex` / `output_dd` come from the packet
// captured on the far side of a real pion DTLS/SRTP transport over an in-memory
// vnet. `sequence` is therefore the OUTPUT (post-munger) RTP sequence number,
// never the source sequence number.
//
// One group per (codec, startup_missing) filter. `timing` is deliberately NOT a
// dimension: the Rust driver loops timing false/true over the same rows.
//
// Row identity recommendation (see README): one row per source stage 0..5, the
// trigger stage carrying writes==0. The malformed pre-roll and the retired send
// (stage 6) are executed and asserted but produce no row, because the Rust
// parser requires exactly six rows per group.
package sfu_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"
)

func TestOriginalDdLifecyclePhysicalPayloadRecovery(t *testing.T) {
	for _, codec := range []struct {
		label string
		mime  string
	}{{"vp9", webrtc.MimeTypeVP9}, {"av1", webrtc.MimeTypeAV1}} {
		for _, startupMissing := range []bool{false, true} {
			t.Run(fmt.Sprintf("lifecycle_%s_startup_missing_%t", codec.label, startupMissing), func(t *testing.T) {
				runDdLifecycleCase(t, codec.mime, startupMissing)
			})
		}
	}
}

func runDdLifecycleCase(t *testing.T, mimeType string, startupMissing bool) {
	sink := newDdLifecycleSink(t, mimeType)

	// Phase 1: malformed present DD pre-roll (dd_disappearance.rs:309-324). The
	// original buffer must reject it, so it can never reach a subscriber and can
	// never be mistaken for "extension not found".
	preRoll := &rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			Marker:         true,
			PayloadType:    ddLifecyclePT,
			SequenceNumber: 999,
			Timestamp:      27000,
			SSRC:           ddLifecycleSourceSSRC,
		},
		Payload: ddLifecyclePayload(mimeType, 9, true),
	}
	require.NoError(t, preRoll.SetExtension(ddLifecycleSourceExtID, []byte{0x80, 0x00}))
	preEp := sink.write(t, preRoll)
	t.Logf("malformed pre-roll produced ext packet: %t", preEp != nil)

	trigger := 2
	if startupMissing {
		trigger = 0
	}

	rows := make([]map[string]any, 0, 6)
	mediaExpected := 0
	retiredWrites := int32(-1)
	for sent := 0; sent < 7; sent++ {
		time.Sleep(ddLifecycleSendSpacing)
		if sent == 6 {
			// Retired-source phase: the Rust consumer issues UnpublishTrack BEFORE
			// this write. The Go analogue on this harness is closing the original
			// DownTrack, which mutes its original forwarder.
			sink.dt.CloseWithFlush(false, true)
		}
		hasDD := sent == 4
		if !startupMissing {
			hasDD = sent == 0 || sent == 1 || sent == 4 || sent == 6
		}
		key := sent == 0 || (startupMissing && sent == 1)
		frame := uint16(10 + sent)
		pkt := &rtp.Packet{
			Header: rtp.Header{
				Version:        2,
				Marker:         true,
				PayloadType:    ddLifecyclePT,
				SequenceNumber: uint16(1000 + sent),
				Timestamp:      3000 * uint32(frame),
				SSRC:           ddLifecycleSourceSSRC,
			},
			Payload: ddLifecyclePayload(mimeType, byte(frame), key),
		}
		if hasDD {
			attached := sent == 0 || sent == 4 || sent == 6
			require.NoError(t, pkt.SetExtension(ddLifecycleSourceExtID, ddLifecycleDescriptorBytes(t, sink.structure, frame, attached)))
		}

		ep := sink.write(t, pkt)
		writes := int32(0)
		if ep != nil {
			writes = sink.dt.WriteRTP(ep, 0)
		}
		if writes == 1 {
			mediaExpected++
		}
		require.Eventually(t, func() bool { return sink.capture.count() >= mediaExpected }, 10*time.Second, 10*time.Millisecond,
			"sent %d accepted original media must physically arrive before the next source phase", sent)
		t.Logf("sent=%d sn=%d has_dd=%t key=%t trigger=%t buffer_ext_packet=%t writes=%d captures=%d",
			sent, 1000+sent, hasDD, key, sent == trigger, ep != nil, writes, sink.capture.count())

		if sent == 6 {
			retiredWrites = writes
			continue
		}
		rows = append(rows, map[string]any{
			"codec":           mimeType,
			"startup_missing": startupMissing,
			"stage":           sent,
			"source_sequence": 1000 + sent,
			"writes":          writes,
			"input_dd":        hasDD,
			"trigger":         sent == trigger,
		})
	}

	// Let the last accepted packet physically arrive before measuring.
	time.Sleep(300 * time.Millisecond)
	media, padding := sink.mediaAndPadding()
	require.Len(t, media, mediaExpected, "every accepted original write must reach the far-side SRTP capture")

	accepted := 0
	for _, row := range rows {
		if row["writes"].(int32) != 1 {
			row["emitted"] = false
			continue
		}
		pkt := media[accepted]
		accepted++
		row["emitted"] = true
		row["sequence"] = pkt.SequenceNumber
		row["timestamp"] = pkt.Timestamp
		row["output_dd"] = sink.outputDD(t, pkt)
		row["payload_hex"] = fmt.Sprintf("%x", pkt.Payload)
		row["marker"] = pkt.Marker
	}
	require.Equal(t, mediaExpected, accepted)

	caseRow := map[string]any{
		"codec":             mimeType,
		"startup_missing":   startupMissing,
		"trigger_stage":     trigger,
		"malformed_preroll_rejected": preEp == nil,
		"media_receipts":    len(media),
		"padding_receipts":  len(padding),
		"retired_writes":    retiredWrites,
		"retired_captures":  len(media) + len(padding),
	}
	caseRaw, err := json.Marshal(caseRow)
	require.NoError(t, err)
	fmt.Println("SOURCE_DD_LIFECYCLE_CASE " + string(caseRaw))
	for _, row := range rows {
		raw, err := json.Marshal(row)
		require.NoError(t, err)
		fmt.Println("SOURCE_DD_LIFECYCLE_PHYSICAL " + string(raw))
	}

	// Invariants of the behaviour under test, asserted on the real capture after
	// the measured rows have been emitted so a genuine divergence stays visible.
	require.Nil(t, preEp, "the original buffer must reject the malformed present DD pre-roll")
	require.Len(t, rows, 6, "one row per source stage 0..5")
	require.Equal(t, 5, accepted, "exactly five of the six stages are accepted")
	require.Empty(t, padding, "the contiguous trigger is a real dropped slot, not invented padding")
	require.Zero(t, retiredWrites, "a retired publication cannot forward")
	require.Len(t, media, 5, "the retired send adds no far-side receipt")
	for i := 1; i < len(media); i++ {
		require.Equal(t, uint16(i), media[i].SequenceNumber-media[0].SequenceNumber, "the dropped trigger slot is compacted exactly once")
		require.True(t, media[i].Marker, "forwarded whole frames keep the marker")
	}
	for _, row := range rows {
		if row["trigger"].(bool) {
			require.Equal(t, int32(0), row["writes"], "the trigger stage is the single dropped slot")
		}
	}
}