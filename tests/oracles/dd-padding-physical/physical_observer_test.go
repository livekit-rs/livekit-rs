// Copyright 2026 livekit-rs contributors. Apache-2.0.
//
// NEWLY AUTHORED oracle producer. The source packet plan below is transcribed
// from the Rust consumer crates/rtc-media/src/regression/dd_padding.rs:333-345
// (plan), :397-425 (per-send header/DD/payload construction) and :102-116
// (coded body). Every emitted number is measured from real Go execution:
// `writes` is the original DownTrack.WriteRTP return value, `output_sequence`
// is the RTP sequence number of the packet captured on the far side of a real
// pion DTLS/SRTP transport over an in-memory vnet.
//
// One group per (codec, alternate_pt[, gap]) filter. `timing` is deliberately
// NOT a dimension: the Rust driver loops timing false/true over the same rows.
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

// ddPaddingInput is one authored source send: (sequence, is_padding_only, has_dd).
type ddPaddingInput struct {
	sequence    uint16
	paddingOnly bool
	hasDD       bool
}

func ddPaddingPlan(outOfOrder bool, gap bool) []ddPaddingInput {
	if outOfOrder {
		// dd_padding.rs:334-341
		return []ddPaddingInput{
			{1000, false, true},
			{1002, false, true},
			{1001, true, false},
			{1003, false, true},
			{1004, false, false},
			{1005, false, true},
		}
	}
	// dd_padding.rs:342-344
	p := uint16(1001)
	if gap {
		p = 1002
	}
	return []ddPaddingInput{{1000, false, true}, {p, true, false}, {p + 1, false, true}}
}

func ddPaddingPrefix(outOfOrder bool, alternatePT bool) string {
	switch {
	case outOfOrder && alternatePT:
		return "SOURCE_DD_OOO_PADDING_ALT_PT "
	case outOfOrder:
		return "SOURCE_DD_OOO_PADDING "
	case alternatePT:
		return "SOURCE_DD_INORDER_PADDING_ALT_PT "
	default:
		return "SOURCE_DD_INORDER_PADDING "
	}
}

func TestOriginalDdPaddingPhysicalOffsetsAndLatches(t *testing.T) {
	for _, codec := range []struct {
		label string
		mime  string
	}{{"vp9", webrtc.MimeTypeVP9}, {"av1", webrtc.MimeTypeAV1}} {
		for _, alternatePT := range []bool{false, true} {
			t.Run(fmt.Sprintf("ooo_%s_alt_%t", codec.label, alternatePT), func(t *testing.T) {
				runDdPaddingCase(t, codec.mime, alternatePT, true, false)
			})
			for _, gap := range []bool{false, true} {
				t.Run(fmt.Sprintf("inorder_%s_alt_%t_gap_%t", codec.label, alternatePT, gap), func(t *testing.T) {
					runDdPaddingCase(t, codec.mime, alternatePT, false, gap)
				})
			}
		}
	}
}

func runDdPaddingCase(t *testing.T, mimeType string, alternatePT bool, outOfOrder bool, gap bool) {
	sink := newDdPaddingSink(t, mimeType, alternatePT)
	plan := ddPaddingPlan(outOfOrder, gap)
	prefix := ddPaddingPrefix(outOfOrder, alternatePT)

	paddingPT := uint8(ddPaddingPrimaryPT)
	if alternatePT {
		paddingPT = ddPaddingAlternatePT
		require.NotEqual(t, uint8(ddPaddingPrimaryPT), paddingPT, "alternate PT variant needs two distinct registered payload types")
	}

	rows := make([]map[string]any, 0, len(plan))
	mediaExpected := 0
	for stage, item := range plan {
		if stage > 0 {
			time.Sleep(ddPaddingSendSpacing)
		}
		frame := item.sequence - 990
		pkt := &rtp.Packet{
			Header: rtp.Header{
				Version:        2,
				Marker:         true,
				PayloadType:    ddPaddingPrimaryPT,
				SequenceNumber: item.sequence,
				Timestamp:      3000 * uint32(frame),
				SSRC:           ddPaddingSourceSSRC,
			},
		}
		if item.paddingOnly {
			pkt.Header.PayloadType = paddingPT
			pkt.Header.Marker = false
			pkt.Header.Padding = true
			pkt.Header.PaddingSize = ddPaddingInputPaddingSize
		} else {
			pkt.Payload = ddPaddingPayload(mimeType, byte(frame), stage == 0)
			if item.hasDD {
				// dd_padding.rs:399-403: the descriptor is attached on EVERY media
				// packet that carries DD, not only on the first one.
				require.NoError(t, pkt.SetExtension(ddPaddingSourceExtID, ddPaddingDescriptorBytes(t, sink.structure, frame, true)))
			}
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
			"stage %d accepted original media must physically arrive before the next source phase", stage)
		t.Logf("stage=%d sn=%d padding_only=%t has_dd=%t buffer_ext_packet=%t writes=%d captures=%d",
			stage, item.sequence, item.paddingOnly, item.hasDD, ep != nil, writes, sink.capture.count())

		if ep == nil {
			// The original buffer dropped this input (in-order padding-only packet).
			// It never reached the DownTrack, so it is not an observation row.
			continue
		}
		row := map[string]any{
			"codec":           mimeType,
			"stage":           stage,
			"source_sequence": item.sequence,
			"writes":          writes,
			"input_dd":        item.hasDD,
			"padding_only":    item.paddingOnly,
		}
		if !outOfOrder {
			row["gap"] = gap
		}
		rows = append(rows, row)
	}

	// Let any dropped-slot padding physically arrive before measuring the capture.
	time.Sleep(300 * time.Millisecond)
	media, padding := sink.mediaAndPadding()
	require.Len(t, media, mediaExpected, "every accepted original write must reach the far-side SRTP capture")
	if outOfOrder {
		require.Len(t, padding, 1, "an out-of-order missing slot is filled with one real RTP padding packet")
		require.Empty(t, padding[0].Payload, "dropped slot padding carries no payload")
		require.Equal(t, media[0].SequenceNumber+1, padding[0].SequenceNumber, "padding occupies the missing outgoing slot")
		require.Len(t, rows, 6, "one row per original source stage, ascending")
	} else {
		require.Empty(t, padding, "the original buffer drops in-order padding-only input; it is never re-emitted downstream")
		require.Len(t, rows, 2, "only the two accepted media packets are observation rows")
	}

	accepted := 0
	for _, row := range rows {
		if row["writes"].(int32) != 1 {
			continue
		}
		pkt := media[accepted]
		accepted++
		row["output_sequence"] = pkt.SequenceNumber
		row["output_timestamp"] = pkt.Timestamp
		row["output_payload_hex"] = fmt.Sprintf("%x", pkt.Payload)
		row["output_marker"] = pkt.Marker
		row["output_dd"] = sink.outputDD(t, pkt)
	}
	require.Equal(t, mediaExpected, accepted)
	for i := 1; i < len(media); i++ {
		require.Greater(t, media[i].SequenceNumber, media[i-1].SequenceNumber, "captured output sequence numbers must ascend")
	}

	caseRow := map[string]any{
		"codec":             mimeType,
		"alternate_pt":      alternatePT,
		"out_of_order":      outOfOrder,
		"gap":               gap,
		"media_receipts":    len(media),
		"padding_receipts":  len(padding),
		"primary_pt":        ddPaddingPrimaryPT,
		"padding_input_pt":  paddingPT,
		"output_dd_present": []bool{},
	}
	ddFlags := make([]bool, 0, len(media))
	for _, pkt := range media {
		ddFlags = append(ddFlags, sink.outputDD(t, pkt))
	}
	caseRow["output_dd_present"] = ddFlags
	if len(padding) == 1 {
		caseRow["padding_sequence_offset"] = int(padding[0].SequenceNumber) - int(media[0].SequenceNumber)
		caseRow["padding_size"] = padding[0].Header.PaddingSize
	}
	caseRaw, err := json.Marshal(caseRow)
	require.NoError(t, err)
	fmt.Println("SOURCE_DD_PADDING_CASE " + string(caseRaw))

	for _, row := range rows {
		raw, err := json.Marshal(row)
		require.NoError(t, err)
		fmt.Println(prefix + string(raw))
	}
	sink.dt.CloseWithFlush(false, true)
}