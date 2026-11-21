// Copyright LiveKit, Inc. Apache-2.0. Observer calls unchanged locked production algorithms.
//
// This observer pins the media counting boundary that livekit-rs has to reproduce before
// RUNTIME:process_lifetime_source_telemetry can claim media-boundary parity. It drives the real
// locked objects in the order the production downtrack does:
//
// RTPStatsSender.Update             (pkg/sfu/rtpstats/rtpstats_sender.go:459; called by
//
//	                                 pkg/sfu/downtrack.go:1185 before d.pacer.Enqueue at :1195 and
//	                                 by writeDroppedSlotPadding at :1266 for padding-only slots)
//	-> DeltaInfoSender              (sender view + receiver view, rtpstats_sender.go:1047)
//	-> ReconcileRTPDeltaInfoWithRTX (rtpstats_base.go:974, wired at downtrack.go:2679)
//	-> toAnalyticsStream            (pkg/sfu/connectionquality/connectionstats.go:455)
//
// Nothing here re-implements an algorithm: every number in a row is a value the locked code
// produced. The file lives in package connectionquality because toAnalyticsStream is unexported
// and is the function that turns a delta into the Primary/Retransmit/Padding series that telemetry
// then folds (pkg/telemetry/stats.go:83-92).
package connectionquality

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/livekit/livekit-server/pkg/sfu/rtpstats"
	"github.com/livekit/protocol/livekit"
)

const (
	// boundarySSRC only labels the emitted AnalyticsStream row.
	boundarySSRC = uint32(0x12345678)
	// boundaryClockRate: deltaInfo returns early when the clock rate is zero
	// (rtpstats_base.go:477); production sets it from the codec.
	boundaryClockRate = uint32(90000)
	// boundaryHdr is a bare RTP header; production passes hdr.MarshalSize().
	boundaryHdr = 12
	// boundaryCacheSize/boundaryCacheRTX mirror pkg/sfu/downtrack.go:391-402.
	boundaryCacheSize = 8192
	boundaryCacheRTX  = 1024
	// boundarySlotPadding stands in for droppedSlotPaddingSize that
	// writeDroppedSlotPadding (downtrack.go:1266) hands to Update.
	boundarySlotPadding = 12
)

// boundaryPacket is one physical packet handed to rtpStats.Update.
type boundaryPacket struct {
	ms      int64
	sn      uint64
	ts      uint64
	marker  bool
	payload int
	padding int
	ooo     bool
}

func boundaryPacketTime(ms int64) int64 {
	return 1000000000 + ms*1000000
}

func boundaryEmit(t *testing.T, row map[string]any) {
	t.Helper()
	raw, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("SOURCE_MEDIA_BOUNDARY " + string(raw))
}

func boundaryDelta(d *rtpstats.RTPDeltaInfo) map[string]any {
	if d == nil {
		return nil
	}
	return map[string]any{
		"packets":              d.Packets,
		"bytes":                d.Bytes,
		"header_bytes":         d.HeaderBytes,
		"packets_duplicate":    d.PacketsDuplicate,
		"bytes_duplicate":      d.BytesDuplicate,
		"packets_padding":      d.PacketsPadding,
		"bytes_padding":        d.BytesPadding,
		"packets_lost":         d.PacketsLost,
		"packets_missing":      d.PacketsMissing,
		"packets_out_of_order": d.PacketsOutOfOrder,
		"frames":               d.Frames,
		"rtt_max":              d.RttMax,
		"jitter_max":           d.JitterMax,
		"nacks":                d.Nacks,
		"nack_repeated":        d.NackRepeated,
		"plis":                 d.Plis,
		"firs":                 d.Firs,
	}
}

func boundaryStream(s *livekit.AnalyticsStream) map[string]any {
	if s == nil {
		return nil
	}
	return map[string]any{
		"ssrc":               s.Ssrc,
		"primary_packets":    s.PrimaryPackets,
		"primary_bytes":      s.PrimaryBytes,
		"retransmit_packets": s.RetransmitPackets,
		"retransmit_bytes":   s.RetransmitBytes,
		"padding_packets":    s.PaddingPackets,
		"padding_bytes":      s.PaddingBytes,
		"packets_lost":       s.PacketsLost,
		"out_of_order":       s.PacketsOutOfOrder,
		"frames":             s.Frames,
		"rtt":                s.Rtt,
		"jitter":             s.Jitter,
		"nacks":              s.Nacks,
		"plis":               s.Plis,
		"firs":               s.Firs,
	}
}

// boundaryLayer records the one place in the locked tree where the three packet classes are added
// together before they leave the process: toAnalyticsVideoLayer
// (connectionstats.go:506-521) folds primary+retransmit+padding, which is the same shape as the
// UPSTREAM telemetry fold (pkg/telemetry/stats.go:83-92) and the Rust MediaTraffic accumulation.
func boundaryLayer(l *livekit.AnalyticsVideoLayer) map[string]any {
	if l == nil {
		return nil
	}
	return map[string]any{"packets": l.Packets, "bytes": l.Bytes, "frames": l.Frames}
}

// boundaryPhysical describes what the stimulus actually put on the wire, so a reader can tell a
// counting rule apart from a different packet mix.
func boundaryPhysical(packets []boundaryPacket) map[string]any {
	var primary, padding, frames, logicalBytes, spanLo, spanHi, duplicate uint64
	seen := map[uint64]bool{}
	first := true
	for _, p := range packets {
		logicalBytes += uint64(boundaryHdr + p.payload + p.padding)
		if p.payload == 0 {
			padding++
		} else {
			primary++
			if p.marker {
				frames++
			}
		}
		if seen[p.sn] {
			duplicate++
		}
		seen[p.sn] = true
		if first {
			spanLo, spanHi, first = p.sn, p.sn, false
			continue
		}
		if p.sn < spanLo {
			spanLo = p.sn
		}
		if p.sn > spanHi {
			spanHi = p.sn
		}
	}
	span := uint64(0)
	if !first {
		span = spanHi - spanLo + 1
	}
	return map[string]any{
		"sent_primary":   primary,
		"sent_padding":   padding,
		"sent_duplicate": duplicate,
		"sent_frames":    frames,
		"logical_bytes":  logicalBytes,
		"sn_span":        span,
	}
}

func boundarySender(isRTX bool, cacheSize int) *rtpstats.RTPStatsSender {
	r := rtpstats.NewRTPStatsSender(rtpstats.RTPStatsParams{IsRTX: isRTX}, cacheSize)
	r.SetClockRate(boundaryClockRate)
	return r
}

func boundarySendSender(r *rtpstats.RTPStatsSender, packets []boundaryPacket) {
	for _, p := range packets {
		r.Update(boundaryPacketTime(p.ms), p.sn, p.ts, p.marker, boundaryHdr, p.payload, p.padding, p.ooo)
	}
}

func boundarySendReceiver(r *rtpstats.RTPStatsReceiver, packets []boundaryPacket) {
	for _, p := range packets {
		r.Update(boundaryPacketTime(p.ms), uint16(p.sn), uint32(p.ts), p.marker, boundaryHdr, p.payload, p.padding)
	}
}

// boundaryWindow reads every delta view of one window twice. The second read uses the same
// snapshot id, which is exactly what production sees when a sampling interval makes no progress
// (or when an interval was already consumed): the boundary pairs with the Rust
// `delta.expected > 0` filter in crates/rtc-media/src/receiver_traffic.rs.
func boundaryWindow(t *testing.T, name string, r *rtpstats.RTPStatsSender, stimulus []boundaryPacket) {
	baseID := r.NewSnapshotId()
	senderID := r.NewSenderSnapshotId()
	// A snapshot id created after a packet anchors packetsExpected on that packet's own
	// position (newSnapshotID passes extHighestSN), so the window covers
	// anchor..highest inclusive. The Rust window baseline is the source start for the same
	// first window, which is why the identity is asserted as highest-anchor+1.
	anchor := r.ExtHighestSequenceNumber()
	boundarySendSender(r, stimulus)
	for window := 1; window <= 2; window++ {
		physical := []boundaryPacket{}
		if window == 1 {
			physical = stimulus
		}
		sv, rv := r.DeltaInfoSender(senderID)
		boundaryEmit(t, map[string]any{
			"case":                  name,
			"window":                window,
			"is_rtx":                false,
			"direction":             "downstream",
			"physical":              boundaryPhysical(physical),
			"sender_view":           boundaryDelta(sv),
			"receiver_view":         boundaryDelta(rv),
			"base_delta":            boundaryDelta(r.DeltaInfo(baseID)),
			"stream_down":           boundaryStream(toAnalyticsStream(boundarySSRC, sv, rv, true)),
			"layer_fold":            boundaryLayer(toAnalyticsVideoLayer(0, sv)),
			"seen_minus_padding":    r.GetPacketsSeenMinusPadding(),
			"anchor_ext_highest_sn": anchor,
			"ext_highest_sn":        r.ExtHighestSequenceNumber(),
		})
	}
}

// TestOriginalMediaCountingBoundaryDownstream covers the publisher->subscriber (sender) side: the
// class partition of a physical packet, the SN-span basis of PrimaryPackets and the window rules.
func TestOriginalMediaCountingBoundaryDownstream(t *testing.T) {
	// A padding-only packet before the stream is initialised returns before initialising
	// (rtpstats_sender.go:476-480), so it must leave no trace at all.
	pre := boundarySender(false, boundaryCacheSize)
	defer pre.Stop()
	preID := pre.NewSenderSnapshotId()
	prePadding := []boundaryPacket{{ms: 0, sn: 100, ts: 3000, padding: 200}}
	boundarySendSender(pre, prePadding)
	sv0, rv0 := pre.DeltaInfoSender(preID)
	boundaryEmit(t, map[string]any{
		"case":               "downstream_padding_only_before_start",
		"window":             1,
		"direction":          "downstream",
		"physical":           boundaryPhysical(prePadding),
		"sender_view":        boundaryDelta(sv0),
		"receiver_view":      boundaryDelta(rv0),
		"stream_down":        boundaryStream(toAnalyticsStream(boundarySSRC, sv0, rv0, true)),
		"seen_minus_padding": pre.GetPacketsSeenMinusPadding(),
		"ext_highest_sn":     pre.ExtHighestSequenceNumber(),
	})
	preFirst := []boundaryPacket{{ms: 10, sn: 101, ts: 3000, marker: true, payload: 20}}
	boundarySendSender(pre, preFirst)
	sv1, rv1 := pre.DeltaInfoSender(preID)
	boundaryEmit(t, map[string]any{
		"case":               "downstream_padding_only_before_start",
		"window":             2,
		"direction":          "downstream",
		"physical":           boundaryPhysical(preFirst),
		"sender_view":        boundaryDelta(sv1),
		"receiver_view":      boundaryDelta(rv1),
		"stream_down":        boundaryStream(toAnalyticsStream(boundarySSRC, sv1, rv1, true)),
		"seen_minus_padding": pre.GetPacketsSeenMinusPadding(),
		"ext_highest_sn":     pre.ExtHighestSequenceNumber(),
	})

	cases := []struct {
		name     string
		cache    int
		start    []boundaryPacket
		stimulus []boundaryPacket
	}{
		{
			// Primary, then a dropped-slot padding packet, then primary again.
			name:     "downstream_padding_inside_window",
			cache:    boundaryCacheSize,
			start:    []boundaryPacket{{ms: 0, sn: 200, ts: 3000, marker: true, payload: 20}},
			stimulus: []boundaryPacket{{ms: 10, sn: 201, ts: 6000, marker: true, payload: 20}, {ms: 20, sn: 202, ts: 6000, padding: boundarySlotPadding}, {ms: 30, sn: 203, ts: 9000, marker: true, payload: 20}},
		},
		{
			// A gap of three sequence numbers inside one window: the gap is charged to
			// packetsLost (rtpstats_sender.go:601-602) and still counted as primary packets.
			name:     "downstream_sequence_gap",
			cache:    boundaryCacheSize,
			start:    []boundaryPacket{{ms: 0, sn: 300, ts: 3000, marker: true, payload: 20}},
			stimulus: []boundaryPacket{{ms: 10, sn: 301, ts: 6000, marker: true, payload: 20}, {ms: 20, sn: 305, ts: 9000, marker: true, payload: 20}},
		},
		{
			// The same sequence number twice: gapSN == 0, so this is a duplicate and not
			// out-of-order (:559).
			name:     "downstream_exact_duplicate",
			cache:    boundaryCacheSize,
			start:    []boundaryPacket{{ms: 0, sn: 400, ts: 3000, marker: true, payload: 20}},
			stimulus: []boundaryPacket{{ms: 10, sn: 401, ts: 6000, marker: true, payload: 20}, {ms: 20, sn: 401, ts: 6000, marker: true, payload: 20}},
		},
		{
			// A late packet that lands inside the metadata window repairs a hole instead of
			// counting as a duplicate (:563-571).
			name:     "downstream_late_repair_in_window",
			cache:    boundaryCacheSize,
			start:    []boundaryPacket{{ms: 0, sn: 500, ts: 3000, marker: true, payload: 20}},
			stimulus: []boundaryPacket{{ms: 10, sn: 501, ts: 6000, marker: true, payload: 20}, {ms: 20, sn: 503, ts: 9000, marker: true, payload: 20}, {ms: 30, sn: 502, ts: 9000, marker: true, payload: 20, ooo: true}},
		},
		{
			// A late packet below the start sequence number moves extStartSN (:523-557),
			// which is the quantity the primary packet count is derived from.
			name:     "downstream_late_below_start",
			cache:    boundaryCacheSize,
			start:    []boundaryPacket{{ms: 0, sn: 600, ts: 3000, marker: true, payload: 20}},
			stimulus: []boundaryPacket{{ms: 10, sn: 601, ts: 6000, marker: true, payload: 20}, {ms: 20, sn: 599, ts: 3000, marker: true, payload: 20, ooo: true}},
		},
		{
			// Too old to be in the metadata cache, so isSnInfoLost says no and the packet is
			// booked as a duplicate instead of a repair (:1317-1325, :1368-1375).
			name:     "downstream_late_beyond_cache",
			cache:    4,
			start:    []boundaryPacket{{ms: 0, sn: 700, ts: 3000, marker: true, payload: 20}},
			stimulus: []boundaryPacket{{ms: 10, sn: 701, ts: 6000, payload: 20}, {ms: 20, sn: 702, ts: 6000, payload: 20}, {ms: 30, sn: 703, ts: 9000, payload: 20}, {ms: 40, sn: 704, ts: 9000, marker: true, payload: 20}, {ms: 50, sn: 700, ts: 3000, marker: true, payload: 20, ooo: true}},
		},
		{
			// More than 65536 sequence numbers in one window: both views are dropped
			// (rtpstats_sender.go:1062 and rtpstats_base.go:495-504).
			name:     "downstream_too_many_packets_expected",
			cache:    boundaryCacheSize,
			start:    []boundaryPacket{{ms: 0, sn: 1000, ts: 3000, marker: true, payload: 20}},
			stimulus: []boundaryPacket{{ms: 10, sn: 71000, ts: 6000, marker: true, payload: 20}},
		},
		{
			// No progress at all between the two reads of one window.
			name:     "downstream_zero_progress",
			cache:    boundaryCacheSize,
			start:    []boundaryPacket{{ms: 0, sn: 800, ts: 3000, marker: true, payload: 20}},
			stimulus: []boundaryPacket{{ms: 10, sn: 801, ts: 6000, marker: true, payload: 20}},
		},
	}
	for _, c := range cases {
		r := boundarySender(false, c.cache)
		boundarySendSender(r, c.start)
		boundaryWindow(t, c.name, r, c.stimulus)
		r.Stop()
	}

	// Feedback counters reach the delta the way the downtrack feeds them
	// (downtrack.go:2202 UpdateNack, :2498 UpdateNackProcessed, :2081 UpdatePliAndTime).
	nacked := boundarySender(false, boundaryCacheSize)
	defer nacked.Stop()
	boundarySendSender(nacked, []boundaryPacket{{ms: 0, sn: 900, ts: 3000, marker: true, payload: 20}})
	nackBaseID := nacked.NewSnapshotId()
	nackSenderID := nacked.NewSenderSnapshotId()
	nackAnchor := nacked.ExtHighestSequenceNumber()
	nacked.UpdateNack(3)
	nacked.UpdateNackProcessed(1, 0, 1)
	nacked.UpdatePliAndTime(2)
	nacked.UpdateFir(1)
	boundarySendSender(nacked, []boundaryPacket{{ms: 10, sn: 901, ts: 6000, marker: true, payload: 20}})
	svN, rvN := nacked.DeltaInfoSender(nackSenderID)
	boundaryEmit(t, map[string]any{
		"case":                  "downstream_feedback_counters",
		"window":                1,
		"direction":             "downstream",
		"physical":              boundaryPhysical([]boundaryPacket{{ms: 10, sn: 901, ts: 6000, marker: true, payload: 20}}),
		"sender_view":           boundaryDelta(svN),
		"base_delta":            boundaryDelta(nacked.DeltaInfo(nackBaseID)),
		"stream_down":           boundaryStream(toAnalyticsStream(boundarySSRC, svN, rvN, true)),
		"seen_minus_padding":    nacked.GetPacketsSeenMinusPadding(),
		"anchor_ext_highest_sn": nackAnchor,
		"ext_highest_sn":        nacked.ExtHighestSequenceNumber(),
	})

	// An RTX-typed sender may initialise on a padding-only packet, because the start guard is
	// `payloadSize == 0 && !IsRTX` (rtpstats_sender.go:476-480). Both types get the same
	// two-packet stimulus; only the RTX stream exists afterwards.
	paddingStart := []boundaryPacket{{ms: 0, sn: 1000, ts: 3000, padding: boundarySlotPadding}}
	paddingWindow := []boundaryPacket{{ms: 10, sn: 1001, ts: 3000, padding: boundarySlotPadding}}
	for _, isRTX := range []bool{true, false} {
		r := boundarySender(isRTX, boundaryCacheRTX)
		boundarySendSender(r, paddingStart)
		paddingBaseID := r.NewSnapshotId()
		paddingAnchor := r.ExtHighestSequenceNumber()
		paddingSenderID := r.NewSenderSnapshotId()
		boundarySendSender(r, paddingWindow)
		svP, rvP := r.DeltaInfoSender(paddingSenderID)
		name := "non_rtx_never_starts_on_padding"
		if isRTX {
			name = "rtx_starts_on_padding_only"
		}
		boundaryEmit(t, map[string]any{
			"case":                  name,
			"window":                1,
			"is_rtx":                isRTX,
			"direction":             "downstream",
			"physical":              boundaryPhysical(paddingWindow),
			"sender_view":           boundaryDelta(svP),
			"receiver_view":         boundaryDelta(rvP),
			"base_delta":            boundaryDelta(r.DeltaInfo(paddingBaseID)),
			"stream_down":           boundaryStream(toAnalyticsStream(boundarySSRC, svP, rvP, true)),
			"layer_fold":            boundaryLayer(toAnalyticsVideoLayer(0, svP)),
			"seen_minus_padding":    r.GetPacketsSeenMinusPadding(),
			"anchor_ext_highest_sn": paddingAnchor,
			"ext_highest_sn":        r.ExtHighestSequenceNumber(),
		})
		r.Stop()
	}

	// Padding packets that arrive as repairs for holes do not advance the highest sequence
	// number, so one window can hold more padding packets than the positions it opened. The two
	// paths react differently: the sender view subtracts the padding delta unguarded
	// (rtpstats_sender.go:1096) so its packet count wraps to a huge uint32, while the base path
	// has the `padding packets more than expected` guard (rtpstats_base.go:517-533), which zeroes
	// only Packets and still returns a non-nil delta because the `DeltaInfo` wrapper logs the
	// error and returns the object anyway (rtpstats_sender.go:1040-1044, rtpstats_receiver.go:896-900).
	// Both paths see the same narrow window here: a snapshot id created after the stream is
	// initialised anchors packetsExpected on that packet's own position for the base path too
	// (rtpstats_base.go:311-324 with rtpstats_sender.go:437), while the *counter* fields of the
	// seeded snapshot stay zero, so the padding delta is the lifetime total.
	repairs := boundarySender(false, boundaryCacheSize)
	defer repairs.Stop()
	boundarySendSender(repairs, []boundaryPacket{
		{ms: 0, sn: 100, ts: 3000, marker: true, payload: 20},
		{ms: 10, sn: 105, ts: 6000, marker: true, payload: 20},
	})
	repairBaseID := repairs.NewSnapshotId()
	repairAnchor := repairs.ExtHighestSequenceNumber()
	repairSenderID := repairs.NewSenderSnapshotId()
	repairWindow := []boundaryPacket{
		{ms: 20, sn: 101, ts: 6000, padding: boundarySlotPadding, ooo: true},
		{ms: 30, sn: 102, ts: 6000, padding: boundarySlotPadding, ooo: true},
		{ms: 40, sn: 103, ts: 6000, padding: boundarySlotPadding, ooo: true},
		{ms: 50, sn: 104, ts: 6000, padding: boundarySlotPadding, ooo: true},
	}
	boundarySendSender(repairs, repairWindow)
	svR, rvR := repairs.DeltaInfoSender(repairSenderID)
	boundaryEmit(t, map[string]any{
		"case":                  "downstream_padding_repairs_exceed_span",
		"window":                1,
		"direction":             "downstream",
		"physical":              boundaryPhysical(repairWindow),
		"sender_view":           boundaryDelta(svR),
		"receiver_view":         boundaryDelta(rvR),
		"base_delta":            boundaryDelta(repairs.DeltaInfo(repairBaseID)),
		"stream_down":           boundaryStream(toAnalyticsStream(boundarySSRC, svR, rvR, true)),
		"seen_minus_padding":    repairs.GetPacketsSeenMinusPadding(),
		"anchor_ext_highest_sn": repairAnchor,
		"ext_highest_sn":        repairs.ExtHighestSequenceNumber(),
	})
}

// TestOriginalMediaCountingBoundaryRTXReconcile pins ReconcileRTPDeltaInfoWithRTX followed by
// toAnalyticsStream, i.e. how a separate RTX SSRC becomes RetransmitPackets downstream.
func TestOriginalMediaCountingBoundaryRTXReconcile(t *testing.T) {
	primary := boundarySender(false, boundaryCacheSize)
	defer primary.Stop()
	boundarySendSender(primary, []boundaryPacket{{ms: 0, sn: 2000, ts: 3000, marker: true, payload: 20}})
	primaryBaseID := primary.NewSnapshotId()
	primarySenderID := primary.NewSenderSnapshotId()
	boundarySendSender(primary, []boundaryPacket{
		{ms: 10, sn: 2001, ts: 6000, marker: true, payload: 20},
		{ms: 20, sn: 2002, ts: 6000, padding: boundarySlotPadding},
		{ms: 30, sn: 2003, ts: 9000, marker: true, payload: 20},
	})
	primary.UpdateNack(2)
	primary.UpdateNackProcessed(1, 0, 1)
	primarySV, primaryRV := primary.DeltaInfoSender(primarySenderID)
	primaryBase := primary.DeltaInfo(primaryBaseID)

	rtx := boundarySender(true, boundaryCacheRTX)
	defer rtx.Stop()
	boundarySendSender(rtx, []boundaryPacket{{ms: 5, sn: 5000, ts: 3000, payload: 18}})
	rtxBaseID := rtx.NewSnapshotId()
	rtxSenderID := rtx.NewSenderSnapshotId()
	boundarySendSender(rtx, []boundaryPacket{
		{ms: 15, sn: 5001, ts: 6000, payload: 18},
		{ms: 25, sn: 5002, ts: 6000, payload: 18},
		{ms: 35, sn: 5003, ts: 6000, padding: boundarySlotPadding},
	})
	rtxSV, rtxRV := rtx.DeltaInfoSender(rtxSenderID)
	rtxBase := rtx.DeltaInfo(rtxBaseID)

	// Reconcile mutates its first argument (rtpstats_base.go:974-1001), which production tolerates
	// because it re-reads the snapshot every interval; copyDelta keeps the inputs printable.
	reconciledSV := rtpstats.ReconcileRTPDeltaInfoWithRTX(copyDelta(primarySV), copyDelta(rtxSV))
	reconciledRV := rtpstats.ReconcileRTPDeltaInfoWithRTX(copyDelta(primaryRV), copyDelta(rtxRV))
	reconciledBase := rtpstats.ReconcileRTPDeltaInfoWithRTX(copyDelta(primaryBase), copyDelta(rtxBase))
	boundaryEmit(t, map[string]any{
		"case":                     "rtx_reconcile",
		"window":                   1,
		"direction":                "downstream",
		"primary_sender_view":      boundaryDelta(primarySV),
		"primary_receiver_view":    boundaryDelta(primaryRV),
		"primary_base_delta":       boundaryDelta(primaryBase),
		"rtx_sender_view":          boundaryDelta(rtxSV),
		"rtx_base_delta":           boundaryDelta(rtxBase),
		"reconciled_sender_view":   boundaryDelta(reconciledSV),
		"reconciled_receiver_view": boundaryDelta(reconciledRV),
		"reconciled_base_delta":    boundaryDelta(reconciledBase),
		"stream_down":              boundaryStream(toAnalyticsStream(boundarySSRC, reconciledSV, reconciledRV, true)),
		"stream_down_unreconciled": boundaryStream(toAnalyticsStream(boundarySSRC, primarySV, primaryRV, true)),
		"stream_up":                boundaryStream(toAnalyticsStream(boundarySSRC, reconciledBase, nil, false)),
		"layer_fold":               boundaryLayer(toAnalyticsVideoLayer(0, reconciledSV)),
	})
}

// copyDelta keeps the pre-reconcile values printable.
func copyDelta(d *rtpstats.RTPDeltaInfo) *rtpstats.RTPDeltaInfo {
	if d == nil {
		return nil
	}
	c := *d
	return &c
}

// TestOriginalMediaCountingBoundaryUpstream covers the publisher side, which uses
// RTPStatsReceiver plus the base deltaInfo (buffer_base.go:469, connectionstats.go:290) and whose
// telemetry fold adds retransmissions back into the packet total (pkg/telemetry/stats.go:88-92).
// TestOriginalMediaCountingBoundaryUpstream covers the publisher-side receiver. Timestamps follow
// the packet clock (ts = 3000 + 90 ms at 90 kHz) so the receiver's own rejection paths
// (pre-start timestamp at rtpstats_receiver.go:335-346, old timestamp at :373-403, old sequence
// number at :406-455) cannot be what explains a row: the only gate left standing in these cases is
// the sequence-number rule under test.
func TestOriginalMediaCountingBoundaryUpstream(t *testing.T) {
	cases := []struct {
		name     string
		start    []boundaryPacket
		stimulus []boundaryPacket
	}{
		{
			// A padding-only packet after the stream is initialised is counted in the padding
			// series (rtpstats_receiver.go:597-601); the sender-side tail uses the same rule but
			// gates on !isDuplicate.
			name:     "upstream_primary_and_padding",
			start:    []boundaryPacket{{ms: 0, sn: 100, ts: 3000, marker: true, payload: 20}},
			stimulus: []boundaryPacket{{ms: 10, sn: 101, ts: 3900, padding: boundarySlotPadding}, {ms: 20, sn: 102, ts: 4800, marker: true, payload: 20}},
		},
		{
			// The stream is not initialised when the anchor is taken, so the padding-only
			// first packet is dropped (rtpstats_receiver.go:270-278) and the anchor slot is
			// initialised lazily from the state at first read.
			name:     "upstream_padding_only_first",
			start:    nil,
			stimulus: []boundaryPacket{{ms: 0, sn: 110, ts: 3000, padding: boundarySlotPadding}, {ms: 10, sn: 111, ts: 3900, marker: true, payload: 20}},
		},
		{
			// The receiver's sequence number WrapAround is created with IsRestartAllowed: false
			// (rtpstats_receiver.go:203), so a packet below the stream start is IsUnhandled and
			// is dropped outright (:490-523) instead of repairing packetsLost the way the
			// sender's below-start branch does (rtpstats_sender.go:523-557). In-range late
			// packets (121) still take the repair path at :534-541.
			name:     "upstream_gap_late_and_pre_start",
			start:    []boundaryPacket{{ms: 0, sn: 120, ts: 3000, marker: true, payload: 20}},
			stimulus: []boundaryPacket{{ms: 10, sn: 122, ts: 3900, marker: true, payload: 20}, {ms: 20, sn: 121, ts: 3900, marker: true, payload: 20}, {ms: 30, sn: 119, ts: 5700, marker: true, payload: 20}},
		},
		{
			// gapSN == 0 is a duplicate, not out-of-order (:529-531), and the duplicate series is
			// what the telemetry fold keeps separate from primary bytes (:597-612 skips bytes).
			name:     "upstream_duplicate",
			start:    []boundaryPacket{{ms: 0, sn: 130, ts: 3000, marker: true, payload: 20}},
			stimulus: []boundaryPacket{{ms: 10, sn: 131, ts: 3900, marker: true, payload: 20}, {ms: 20, sn: 131, ts: 3900, marker: true, payload: 20}},
		},
	}
	for _, c := range cases {
		r := rtpstats.NewRTPStatsReceiver(rtpstats.RTPStatsParams{})
		r.SetClockRate(boundaryClockRate)
		boundarySendReceiver(r, c.start)
		anchor := r.NewSnapshotId()
		anchorHighest := r.ExtendedHighestSequenceNumber()
		boundarySendReceiver(r, c.stimulus)
		for window := 1; window <= 2; window++ {
			physical := []boundaryPacket{}
			if window == 1 {
				physical = c.stimulus
			}
			d := r.DeltaInfo(anchor)
			boundaryEmit(t, map[string]any{
				"case":                  c.name,
				"window":                window,
				"direction":             "upstream",
				"physical":              boundaryPhysical(physical),
				"base_delta":            boundaryDelta(d),
				"stream_up":             boundaryStream(toAnalyticsStream(boundarySSRC, d, nil, false)),
				"layer_fold":            boundaryLayer(toAnalyticsVideoLayer(0, d)),
				"anchor_ext_highest_sn": anchorHighest,
				"ext_highest_sn":        r.ExtendedHighestSequenceNumber(),
			})
		}
		r.Stop()
	}
}
