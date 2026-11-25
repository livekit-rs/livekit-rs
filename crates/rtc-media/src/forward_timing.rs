//! Original-packet observations after actual successful broadcast admission.
use crate::engine::MediaEngine;
use std::time::Instant;
use str0m::media::ForwardingPacket;
struct PacketAdmission {
    packet: ForwardingPacket,
    writes: u32,
    layer: i32,
    classified: bool,
}
pub(crate) struct ForwardingAdmissions {
    packets: Vec<PacketAdmission>,
}
impl ForwardingAdmissions {
    pub(crate) fn new(packets: &[ForwardingPacket]) -> Self {
        Self {
            packets: packets
                .iter()
                .copied()
                .map(|packet| PacketAdmission {
                    packet,
                    writes: 0,
                    layer: 0,
                    classified: true,
                })
                .collect(),
        }
    }
    pub(crate) fn apply_av1_layers(&mut self, layers: &[Option<(u8, u8)>]) {
        for sample in &mut self.packets {
            sample.classified = false;
            if let Some(Some((spatial, temporal))) = layers.get(sample.packet.extension_index) {
                sample.packet.spatial = Some(*spatial);
                sample.packet.temporal = Some(*temporal);
                sample.classified = true;
            }
        }
    }
    /// Called only after this target's actual Writer::write returned Ok. A
    /// rejected/failed target cannot admit any original packet into the union.
    pub(crate) fn accepted(&mut self, spatial_cap: i32, temporal_cap: u8, source_spatial: i32) {
        for sample in &mut self.packets {
            if !sample.classified {
                continue;
            }
            let spatial = sample.packet.spatial.map_or(source_spatial, i32::from);
            if spatial > spatial_cap || sample.packet.temporal.is_some_and(|t| t > temporal_cap) {
                continue;
            }
            sample.writes = sample.writes.saturating_add(1);
            sample.layer = spatial;
        }
    }
    /// VP9's fixed original selector is spatial-first lexicographic. This
    /// separate predicate must not change AV1's exact selected-frame union.
    pub(crate) fn accepted_vp9(&mut self, spatial_cap: i32, temporal_cap: u8, source_spatial: i32) {
        for sample in &mut self.packets {
            if !sample.classified {
                continue;
            }
            let spatial = sample.packet.spatial.map_or(source_spatial, i32::from);
            if spatial > spatial_cap
                || (spatial == spatial_cap
                    && sample.packet.temporal.is_some_and(|t| t > temporal_cap))
            {
                continue;
            }
            sample.writes = sample.writes.saturating_add(1);
            sample.layer = spatial;
        }
    }
    /// AV1 DTI selection need not retain a spatial prefix. Count exactly the
    /// original DD frame IDs retained by this successful writer admission.
    pub(crate) fn accepted_av1(&mut self, packet_frames: &[Option<u16>], retained_frames: &[u16]) {
        for sample in &mut self.packets {
            if !sample.classified {
                continue;
            }
            if packet_frames
                .get(sample.packet.extension_index)
                .copied()
                .flatten()
                .is_some_and(|frame| retained_frames.contains(&frame))
            {
                sample.writes = sample.writes.saturating_add(1);
                sample.layer = sample.packet.spatial.map_or(0, i32::from);
            }
        }
    }
    /// Test-only: set one packet's successful subscriber-write count directly,
    /// standing in for a broadcast far larger than a fixture should loop.
    #[cfg(test)]
    pub(crate) fn set_write_count(&mut self, index: usize, writes: u32) {
        self.packets[index].writes = writes;
    }
    #[cfg(test)]
    pub(crate) fn write_counts(&self) -> Vec<u32> {
        self.packets.iter().map(|s| s.writes).collect()
    }
}
fn signed_ns(left: Instant, arrival: Instant) -> i64 {
    if left >= arrival {
        left.duration_since(arrival)
            .as_nanos()
            .min(i64::MAX as u128) as i64
    } else {
        -(arrival
            .duration_since(left)
            .as_nanos()
            .min(i64::MAX as u128) as i64)
    }
}

/// Upstream `MediaTrack.getReceiverLBThreshold()` (`pkg/rtc/mediatrack.go:276-286`),
/// fed to `sfu.WithLoadBalanceThreshold` at receiver creation
/// (`pkg/rtc/mediatrack.go:417`) and on to `ReceiverBase.SetLBThreshold`
/// (`pkg/sfu/receiver_base.go:240-246`, `NewDownTrackSpreader` at `:171-174`).
/// With the batched UDP mux installed, `WriteRTP` only enqueues (the send runs
/// on the flush goroutine), so Go defers the last-subscriber wake-up to a much
/// larger down-track count: 300 instead of 20. The values are literals in the
/// pinned oracle; nothing here re-derives them.
pub(crate) const LB_THRESHOLD_BATCH_IO: usize = 300;
pub(crate) const LB_THRESHOLD_DEFAULT: usize = 20;

impl MediaEngine {
    /// The one place the projected batch-IO condition is read, mirroring Go's
    /// single call site (`pkg/rtc/mediatrack.go:417`).
    pub(crate) fn receiver_lb_threshold(&self) -> usize {
        if self.cfg.rtc.media_batch_io_enabled {
            LB_THRESHOLD_BATCH_IO
        } else {
            LB_THRESHOLD_DEFAULT
        }
    }

    /// Actual downstream delivery cost of one received frame, measured per
    /// subscriber write like Go's serial forwarder
    /// (`pkg/sfu/receiver_base.go:968-990`, where `writeCount` is the number of
    /// down tracks the packet was written to and the transit sample is taken
    /// once per admitted original packet). Every admitted packet costs one
    /// write for each successful subscriber until the cohort has been written
    /// to more subscribers than the receiver load-balance threshold allows;
    /// past that point Go's spreader hands the remaining writes to its worker
    /// pool (`DownTrackSpreader` with `Threshold > 0`), which this synchronous
    /// engine does not run, so they no longer contribute to the measured cost.
    /// The observable consequence is the queueing term of the observation: with
    /// the small default threshold a broadcast cohort stops accumulating
    /// latency after 20 subscriber writes, with the batch-IO threshold it keeps
    /// measuring up to 300.
    fn record_forwarding_cohort_with_threshold(
        &self,
        admissions: &ForwardingAdmissions,
        left: Instant,
        dequeued_at: Instant,
        lb_threshold: usize,
    ) {
        let Some(stats) = self.cfg.rtc.forward_stats.as_ref() else {
            return;
        };
        let eligible = |sample: &&PacketAdmission| {
            sample.writes > 0 && !sample.packet.buffered && !sample.packet.out_of_order
        };
        let mut measured_writes = 0usize;
        let mut sampled = false;
        for sample in &admissions.packets {
            if !eligible(&sample) {
                continue;
            }
            stats.record_forward_samples(left, [sample.packet.received]);
            sampled = true;
            let remaining = lb_threshold.saturating_sub(measured_writes);
            if remaining == 0 {
                continue;
            }
            let counted = sample.writes as usize;
            measured_writes += counted.min(remaining);
            if counted > remaining {
                break;
            }
        }
        if !sampled {
            return;
        }
        for sample in &admissions.packets {
            if !eligible(&sample) {
                continue;
            }
            let packet = &sample.packet;
            let latency = signed_ns(left, packet.received);
            if latency > 20_000_000 {
                tracing::debug!(
                    latency,
                    queuingLatency = signed_ns(dequeued_at, packet.received),
                    writeCount = sample.writes,
                    isOutOfOrder = packet.out_of_order,
                    layer = sample.layer,
                    "high forwarding latency"
                );
            }
        }
    }

    pub(crate) fn record_forwarding_cohort(
        &self,
        admissions: &ForwardingAdmissions,
        left: Instant,
        dequeued_at: Instant,
    ) {
        self.record_forwarding_cohort_with_threshold(
            admissions,
            left,
            dequeued_at,
            self.receiver_lb_threshold(),
        );
    }
}
