// Copyright 2026 livekit-rs contributors. Apache-2.0.
//
// Observers for the ONE / RID dependency-descriptor physical oracle.
//
// Each test walks the four (codec, mode) groups of its campaign in the exact
// order the Rust consumer replays them, and prints one JSON observation row per
// measured phase. Every reported value is read back from a packet that really
// arrived on the far side of a pion DTLS/SRTP transport over an in-memory vnet.
//
// Nothing here computes an expected forwarding outcome. The Go test asserts only
// Go-internal invariants (the buffer accepted the input, WriteRTP's return value
// equals the number of media packets physically received, the payload survives
// the DDTemporal munger unchanged, the output descriptor parses with the original
// subscriber-side reader). Whether those observations agree with the Rust
// reimplementation is decided by the Rust consumer, not here.
package sfu_test

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"
)

// ddOneRidRow is the exact 19-field schema the Rust consumer deserializes with
// deny_unknown_fields. Optional fields are pointers so that a present zero
// (internal_sid 0, outer_spatial 0, key false, out_sn_offset 0) is emitted and an
// absent field is omitted entirely.
type ddOneRidRow struct {
	Codec       string `json:"codec"`
	Mode        string `json:"mode"`
	Phase       int    `json:"phase"`
	SourceFrame uint16 `json:"source_frame"`
	SourceSN    uint16 `json:"source_sn"`
	TID         uint8  `json:"tid"`
	Writes      int    `json:"writes"`

	Marker          *bool   `json:"marker,omitempty"`
	OutFrame        *uint16 `json:"out_frame,omitempty"`
	OutSNOffset     *uint16 `json:"out_sn_offset,omitempty"`
	PayloadHex      *string `json:"payload_hex,omitempty"`
	PaddingSNOffset *uint16 `json:"padding_sn_offset,omitempty"`
	InternalSID     *uint8  `json:"internal_sid,omitempty"`
	Key             *bool   `json:"key,omitempty"`
	OutTSOffset     *uint32 `json:"out_ts_offset,omitempty"`
	OuterSpatial    *uint8  `json:"outer_spatial,omitempty"`
	RID             *string `json:"rid,omitempty"`
	SourceSSRC      *uint32 `json:"source_ssrc,omitempty"`
	SourceTS        *uint32 `json:"source_ts,omitempty"`
}

func ddOneRidBool(v bool) *bool    { return &v }
func ddOneRidU8(v uint8) *uint8    { return &v }
func ddOneRidU16(v uint16) *uint16 { return &v }
func ddOneRidU32(v uint32) *uint32 { return &v }
func ddOneRidStr(v string) *string { return &v }

// ddOneRidWarmup feeds the training frames through the original public source
// path while the subscriber target is still invalid, exactly like the Rust
// consumer, which sends them before it enables the sink. It asserts that not one
// of them reaches the subscriber: that is what makes the Rust assertion
// indices[0] == 0 meaningful.
func ddOneRidWarmup(t *testing.T, sink *ddOneRidSink, plan *ddOneRidPlan, mimeType string) {
	t.Helper()
	w := plan.Warmup
	for n := 0; n < w.Frames; n++ {
		for _, rid := range sink.order {
			template := 1
			if n == 0 {
				template = 0
			} else if n%2 == 1 {
				template = 4
			}
			p := ddOneRidPhase{
				RID:      rid,
				Template: template,
				TID:      boolToInt(template == 4),
				First:    true,
				Last:     true,
				Attached: template == 0,
				SN:       w.SNBase + uint16(n),
				Frame:    w.FrameBase + uint16(n),
				TS:       w.TSBase + uint32(n)*w.TSStep,
			}
			ep, src := sink.write(t, p, mimeType, "rid")
			require.NotNil(t, ep, "warmup frame %d rid %q must be accepted by the original buffer", n, rid)
			require.Equal(t, 0, int(sink.dt.WriteRTP(ep, src.layer)),
				"a paused subscriber must not forward warmup frame %d rid %q", n, rid)
			time.Sleep(time.Duration(plan.Pacing.WarmupMS) * time.Millisecond)
		}
	}
	require.Equal(t, 0, sink.capture.count(), "no warmup packet may reach the subscriber")
}

// ddOneRidRunGroup measures one (codec, mode) group and prints its rows.
func ddOneRidRunGroup(t *testing.T, plan *ddOneRidPlan, g ddOneRidGroup) {
	t.Helper()
	mimeType := webrtc.MimeTypeVP9
	if g.Codec == "video/AV1" {
		mimeType = webrtc.MimeTypeAV1
	}
	require.Contains(t, []string{"video/VP9", "video/AV1"}, g.Codec)
	mode := ddOneRidMode(g.Mode)
	isRID := g.Campaign == "rid"
	rids := []string{""}
	if isRID {
		rids = []string{"q", "f"}
	}

	sink := newDdOneRidSink(t, mimeType, mode, rids)
	// the published ceiling, as the real receiver reports it. This only moves
	// maxSeen; it does not set a target.
	sink.dt.UpTrackMaxPublishedLayerChange(2)
	sink.dt.UpTrackMaxTemporalLayerSeenChange(1)

	ddOneRidWarmup(t, sink, plan, mimeType)

	if isRID && mode.String() == "ONE_SPATIAL_LAYER_PER_STREAM" {
		// same-NTP reports for the two real source SSRC clocks, through the
		// original public DownTrack RTCP entry point.
		sink.injectSenderReports(t, plan)
	}
	sink.allocate(t, 0)
	time.Sleep(time.Duration(plan.Pacing.EnableDelayMS) * time.Millisecond)

	var firstSN uint16
	var firstTS uint32
	haveFirst := false

	for i, p := range g.Phases {
		if isRID && i == 5 {
			// the subscriber asks for the high layer. This is the real
			// target-only allocation path; the current layer still moves on
			// the packet path, so the upgrade waits for a key frame.
			sink.allocate(t, 2)
			time.Sleep(time.Duration(plan.Pacing.EnableDelayMS) * time.Millisecond)
		}

		before := sink.capture.count()
		ep, src := sink.write(t, p, mimeType, g.Campaign)
		require.NotNil(t, ep, "phase %d must be accepted by the original buffer", i)
		// IsKeyFrame is a first-packet-of-frame property, not a whole-frame property:
		// the original classifiers (codec.IsVP9KeyFrame / codec.IsAV1KeyFrame) only
		// report a key frame on the packet that STARTS it (the VP9 B bit / the AV1
		// sequence-header OBU). A later packet of the same key frame belongs to a key
		// frame but is not itself the key-frame start, so the classifier correctly
		// reports false there (e.g. the ONE campaign's phase 1, the last packet of key
		// frame 10). Every RID phase is a whole frame (first=true), so for RID this
		// reduces to template==0. Assert the true invariant on EVERY phase, including
		// that non-first packets of a key frame are not themselves key frames.
		require.Equal(t, p.Template == 0 && p.First, ep.IsKeyFrame,
			"the original key-frame classifier must read the authored fixture at phase %d", i)
		writes := int(sink.dt.WriteRTP(ep, src.layer))
		require.Contains(t, []int{0, 1}, writes, "WriteRTP writes at most one media packet")

		pacing := plan.Pacing.MeasuredMS
		if i == 0 {
			pacing = plan.Pacing.FirstMeasuredMS
		}
		time.Sleep(time.Duration(pacing) * time.Millisecond)
		require.Eventually(t, func() bool {
			return sink.capture.count() >= before+writes
		}, 5*time.Second, 10*time.Millisecond, "phase %d media never arrived", i)
		time.Sleep(40 * time.Millisecond)

		media, padding := sink.mediaAndPadding(before)
		require.Len(t, media, writes, "phase %d: WriteRTP return value must equal the physical media receipts", i)
		require.LessOrEqual(t, len(padding), 1, "phase %d emits at most one dropped-slot padding packet", i)

		row := ddOneRidRow{
			Codec:       g.Codec,
			Mode:        g.Mode,
			Phase:       i,
			SourceFrame: p.Frame,
			SourceSN:    p.SN,
			TID:         uint8(p.TID),
			Writes:      writes,
		}
		if isRID {
			rid := p.RID
			row.RID = ddOneRidStr(rid)
			row.InternalSID = ddOneRidU8(0)
			row.Key = ddOneRidBool(ep.IsKeyFrame)
			row.OuterSpatial = ddOneRidU8(uint8(src.layer))
			row.SourceSSRC = ddOneRidU32(ep.Packet.SSRC)
			row.SourceTS = ddOneRidU32(ep.Packet.Timestamp)
		}

		if writes > 0 {
			out := media[0]
			if !haveFirst {
				firstSN, firstTS, haveFirst = out.SequenceNumber, out.Timestamp, true
			}
			require.Equal(t, ep.Packet.Payload, out.Payload,
				"the DDTemporal munger leaves the %s payload unchanged at phase %d", g.Codec, i)
			row.OutSNOffset = ddOneRidU16(out.SequenceNumber - firstSN)
			row.PayloadHex = ddOneRidStr(hex.EncodeToString(out.Payload))
			if !isRID {
				row.Marker = ddOneRidBool(out.Marker)
			} else {
				row.OutTSOffset = ddOneRidU32(out.Timestamp - firstTS)
			}
			dd := sink.outputDD(t, out)
			row.OutFrame = ddOneRidU16(dd.Descriptor.FrameNumber)
			require.Equal(t, 0, dd.Descriptor.FrameDependencies.SpatialId, "phase %d output spatial", i)
			require.Equal(t, p.TID, dd.Descriptor.FrameDependencies.TemporalId, "phase %d output temporal", i)
			require.Equal(t, p.Attached, dd.Descriptor.AttachedStructure != nil, "phase %d output structure attachment", i)
			require.NotNil(t, dd.Descriptor.ActiveDecodeTargetsBitmask, "phase %d must name its active decode targets", i)
			require.Equal(t, uint32(1), *dd.Descriptor.ActiveDecodeTargetsBitmask, "phase %d admits only the T0 decode target", i)
		}
		if len(padding) == 1 {
			require.True(t, haveFirst, "padding can only follow a forwarded packet")
			require.Empty(t, padding[0].Payload, "a dropped-slot padding packet carries no payload")
			row.PaddingSNOffset = ddOneRidU16(padding[0].SequenceNumber - firstSN)
		}

		encoded, err := json.Marshal(row)
		require.NoError(t, err)
		fmt.Println(g.Prefix + string(encoded))
	}

	require.True(t, haveFirst, "every group must produce at least one physical media packet")
	sink.dt.CloseWithFlush(false, true)
}

func ddOneRidCampaign(t *testing.T, campaign string) {
	t.Helper()
	plan := loadDdOneRidPlan(t)
	groups := 0
	for _, g := range plan.Groups {
		if g.Campaign != campaign {
			continue
		}
		groups++
		t.Run(fmt.Sprintf("%s/%s", g.Codec, g.Mode), func(t *testing.T) {
			ddOneRidRunGroup(t, plan, g)
		})
	}
	require.Equal(t, 4, groups, "each campaign has four (codec, mode) groups")
}

// TestOriginalDdOnePhysicalLiteralRows emits the 24 non-RID observations.
func TestOriginalDdOnePhysicalLiteralRows(t *testing.T) {
	ddOneRidCampaign(t, "one")
}

// TestOriginalDdRidPhysicalLiteralRows emits the 44 RID observations.
func TestOriginalDdRidPhysicalLiteralRows(t *testing.T) {
	ddOneRidCampaign(t, "rid")
}