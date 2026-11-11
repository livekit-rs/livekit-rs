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
pub(crate) struct ForwardingAdmissions(Vec<PacketAdmission>);
impl ForwardingAdmissions {
    pub(crate) fn new(packets: &[ForwardingPacket]) -> Self {
        Self(
            packets
                .iter()
                .copied()
                .map(|packet| PacketAdmission {
                    packet,
                    writes: 0,
                    layer: 0,
                    classified: true,
                })
                .collect(),
        )
    }
    pub(crate) fn apply_av1_layers(&mut self, layers: &[Option<(u8, u8)>]) {
        for sample in &mut self.0 {
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
        for sample in &mut self.0 {
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
        for sample in &mut self.0 {
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
        for sample in &mut self.0 {
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
    #[cfg(test)]
    pub(crate) fn write_counts(&self) -> Vec<u32> {
        self.0.iter().map(|s| s.writes).collect()
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
impl MediaEngine {
    pub(crate) fn record_forwarding_cohort(
        &self,
        admissions: &ForwardingAdmissions,
        left: Instant,
        dequeued_at: Instant,
    ) {
        let Some(stats) = self.cfg.rtc.forward_stats.as_ref() else {
            return;
        };
        let eligible = || {
            admissions
                .0
                .iter()
                .filter(|s| s.writes > 0 && !s.packet.buffered && !s.packet.out_of_order)
        };
        stats.record_forward_samples(left, eligible().map(|s| s.packet.received));
        for sample in eligible() {
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
}
