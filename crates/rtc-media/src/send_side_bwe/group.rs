// Copyright 2023 LiveKit, Inc.
// SPDX-License-Identifier: Apache-2.0
// Port of e5655ac20267fa5005dccaf3f0e3bfc3ea688c07 pkg/sfu/bwe/sendsidebwe/packet_group.go
use rtc_core::config::{PacketGroupConfig, WeightedLossConfig};

use super::bitmap::Bitmap;
use super::tracker::PacketInfo;
use super::traffic::{TrafficData, TrafficStats, zero_weighted_loss};

/// Upstream returns `errGroupFinalized` / `errOldPacket`; both abort the update
/// without changing any state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum GroupError {
    Finalized,
    OldPacket,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Stat {
    num_packets: i64,
    num_bytes: i64,
}

impl Stat {
    fn add(&mut self, size: i64) {
        self.num_packets += 1;
        self.num_bytes += size;
    }

    fn remove(&mut self, size: i64) {
        self.num_packets -= 1;
        self.num_bytes -= size;
    }

    pub fn packets(&self) -> i64 {
        self.num_packets
    }

    pub fn bytes(&self) -> i64 {
        self.num_bytes
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct ClassStat {
    primary: Stat,
    rtx: Stat,
    probe: Stat,
}

impl ClassStat {
    fn add(&mut self, size: i64, is_rtx: bool, is_probe: bool) {
        if is_rtx {
            self.rtx.add(size);
        } else if is_probe {
            self.probe.add(size);
        } else {
            self.primary.add(size);
        }
    }

    fn remove(&mut self, size: i64, is_rtx: bool, is_probe: bool) {
        if is_rtx {
            self.rtx.remove(size);
        } else if is_probe {
            self.probe.remove(size);
        } else {
            self.primary.remove(size);
        }
    }

    pub fn packets(&self) -> i64 {
        self.primary.num_packets + self.rtx.num_packets + self.probe.num_packets
    }

    pub fn bytes(&self) -> i64 {
        self.primary.num_bytes + self.rtx.num_bytes + self.probe.num_bytes
    }

    pub fn primary(&self) -> &Stat {
        &self.primary
    }

    pub fn rtx(&self) -> &Stat {
        &self.rtx
    }

    pub fn probe(&self) -> &Stat {
        &self.probe
    }
}

pub(super) struct PacketGroup {
    min_packets: i64,
    max_window_duration_us: i64,
    weighted_loss: WeightedLossConfig,

    min_sequence_number: u64,
    max_sequence_number: u64,
    min_send_time: i64,
    max_send_time: i64,
    min_recv_time: i64,
    max_recv_time: i64,

    acked: ClassStat,
    lost: ClassStat,
    sn_bitmap: Bitmap,

    aggregate_send_delta: i64,
    aggregate_recv_delta: i64,
    inherited_queuing_delay: i64,

    is_finalized: bool,
}

impl PacketGroup {
    /// `inherited_queuing_delay` is the queue the previous group already built up
    /// and that this group's packets still carry.
    pub fn new(
        config: PacketGroupConfig,
        weighted_loss: WeightedLossConfig,
        inherited_queuing_delay: i64,
    ) -> Self {
        let min_packets =
            usize::try_from(config.min_packets).expect("negative min_packets panics upstream");
        Self {
            min_packets: config.min_packets,
            max_window_duration_us: config.max_window_duration.as_micros_i64(),
            weighted_loss,
            min_sequence_number: 0,
            max_sequence_number: 0,
            min_send_time: 0,
            max_send_time: 0,
            min_recv_time: 0,
            max_recv_time: 0,
            acked: ClassStat::default(),
            lost: ClassStat::default(),
            sn_bitmap: Bitmap::new(min_packets),
            aggregate_send_delta: 0,
            aggregate_recv_delta: 0,
            inherited_queuing_delay,
            is_finalized: false,
        }
    }

    pub fn add(
        &mut self,
        pi: &PacketInfo,
        send_delta: i64,
        recv_delta: i64,
        is_lost: bool,
    ) -> Result<(), GroupError> {
        if is_lost {
            return self.lost_packet(pi);
        }

        self.in_group(pi.sequence_number)?;

        if self.min_sequence_number == 0 || pi.sequence_number < self.min_sequence_number {
            self.min_sequence_number = pi.sequence_number;
        }
        self.max_sequence_number = self.max_sequence_number.max(pi.sequence_number);

        if self.min_send_time == 0 || pi.send_time - send_delta < self.min_send_time {
            self.min_send_time = pi.send_time - send_delta;
        }
        self.max_send_time = self.max_send_time.max(pi.send_time);

        if self.min_recv_time == 0 || pi.recv_time - recv_delta < self.min_recv_time {
            self.min_recv_time = pi.recv_time - recv_delta;
        }
        self.max_recv_time = self.max_recv_time.max(pi.recv_time);

        let size = i64::from(pi.size);
        self.acked.add(size, pi.is_rtx, pi.is_probe);
        let offset = (pi.sequence_number - self.min_sequence_number) as usize;
        if offset < self.sn_bitmap.len() && self.sn_bitmap.is_set(offset) {
            // an earlier packet reported as lost has been received
            self.sn_bitmap.clear(offset);
            self.lost.remove(size, pi.is_rtx, pi.is_probe);
        }

        // note that out-of-order deliveries will amplify the queueing delay.
        self.aggregate_send_delta += send_delta;
        self.aggregate_recv_delta += recv_delta;

        if self.acked.packets() == self.min_packets
            || pi.send_time - self.min_send_time > self.max_window_duration_us
        {
            self.is_finalized = true;
        }
        Ok(())
    }

    pub fn lost_packet(&mut self, pi: &PacketInfo) -> Result<(), GroupError> {
        if pi.recv_time != 0 {
            // previously received packet, so not lost
            return Ok(());
        }

        self.in_group(pi.sequence_number)?;

        if self.min_sequence_number == 0 || pi.sequence_number < self.min_sequence_number {
            self.min_sequence_number = pi.sequence_number;
        }
        self.max_sequence_number = self.max_sequence_number.max(pi.sequence_number);
        let offset = (pi.sequence_number - self.min_sequence_number) as usize;
        self.sn_bitmap.set(offset);

        self.lost.add(i64::from(pi.size), pi.is_rtx, pi.is_probe);
        Ok(())
    }

    pub fn min_send_time(&self) -> i64 {
        self.min_send_time
    }

    pub fn send_window(&self) -> (i64, i64) {
        (self.min_send_time, self.max_send_time)
    }

    pub fn recv_window(&self) -> (i64, i64) {
        (self.min_recv_time, self.max_recv_time)
    }

    pub fn sequence_window(&self) -> (u64, u64) {
        (self.min_sequence_number, self.max_sequence_number)
    }

    pub fn aggregate_deltas(&self) -> (i64, i64) {
        (self.aggregate_send_delta, self.aggregate_recv_delta)
    }

    /// `inheritedQueuingDelay`, which the campaign echoes as `g_inherited_us` /
    /// the group it seeded.
    #[cfg(test)]
    pub fn inherited_queuing_delay(&self) -> i64 {
        self.inherited_queuing_delay
    }

    pub fn propagated_queuing_delay(&self) -> i64 {
        let with_inherited =
            self.inherited_queuing_delay + self.aggregate_recv_delta - self.aggregate_send_delta;
        if with_inherited > 0 {
            return with_inherited;
        }

        (self.aggregate_recv_delta - self.aggregate_send_delta).max(0)
    }

    pub fn finalized_propagated_queuing_delay(&self) -> Option<i64> {
        if !self.is_finalized {
            return None;
        }

        Some(self.propagated_queuing_delay())
    }

    pub fn is_finalized(&self) -> bool {
        self.is_finalized
    }

    /// Upstream `Traffic()` returns a `trafficStats` literal built with **no**
    /// params, so its weighted loss runs on the zero config: oracle `lg_wl_raw`
    /// is `NaN`/`+Inf` where the configured group stats answer `0`.
    pub fn traffic(&self) -> TrafficStats {
        let mut stats = TrafficStats::new(zero_weighted_loss());
        stats.merge(&self.traffic_data());
        stats
    }

    /// Upstream logging path: stats built with the group's own weighted loss config.
    pub fn configured_traffic_stats(&self) -> TrafficStats {
        let mut stats = TrafficStats::new(self.weighted_loss.clone());
        stats.merge(&self.traffic_data());
        stats
    }

    pub fn traffic_data(&self) -> TrafficData {
        TrafficData {
            min_send_time: self.min_send_time,
            max_send_time: self.max_send_time,
            send_delta: self.aggregate_send_delta,
            recv_delta: self.aggregate_recv_delta,
            acked_packets: self.acked.packets(),
            acked_bytes: self.acked.bytes(),
            lost_packets: self.lost.packets(),
            lost_bytes: self.lost.bytes(),
        }
    }

    pub fn acked(&self) -> &ClassStat {
        &self.acked
    }

    pub fn lost(&self) -> &ClassStat {
        &self.lost
    }

    fn in_group(&self, sequence_number: u64) -> Result<(), GroupError> {
        if self.is_finalized && sequence_number > self.max_sequence_number {
            return Err(GroupError::Finalized);
        }

        if sequence_number < self.min_sequence_number {
            return Err(GroupError::OldPacket);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtc_core::config::PolicyDuration;

    /// The window is a count of microseconds, the unit the campaign stimulus
    /// declares (`grp_win=500000` is the upstream default half-second group).
    fn config(min_packets: i64, max_window_us: i64) -> PacketGroupConfig {
        PacketGroupConfig {
            min_packets,
            max_window_duration: PolicyDuration::from_micros(max_window_us),
        }
    }

    fn pi(sn: u64, send_time: i64, recv_time: i64, size: u16) -> PacketInfo {
        PacketInfo {
            sequence_number: sn,
            send_time,
            recv_time,
            probe_cluster_id: 0,
            size,
            is_rtx: false,
            is_probe: false,
        }
    }

    #[test]
    fn finalized_group_accepts_late_sequence_updates_but_rejects_newer_ones() {
        let mut group = PacketGroup::new(config(2, 500_000), WeightedLossConfig::default(), 0);
        group
            .add(&pi(10, 100, 180, 10), 0, 0, false)
            .expect("first packet");
        group
            .add(&pi(11, 120, 200, 10), 0, 0, false)
            .expect("second packet finalizes");
        assert!(group.is_finalized());
        assert_eq!(group.sequence_window(), (10, 11));
        assert_eq!(
            group.add(&pi(12, 140, 220, 10), 0, 0, false),
            Err(GroupError::Finalized)
        );
        assert_eq!(
            group.add(&pi(9, 90, 170, 10), 0, 0, false),
            Err(GroupError::OldPacket)
        );
        assert_eq!(
            group.acked().packets(),
            2,
            "rejected packets change nothing"
        );
    }

    #[test]
    fn window_extremes_use_delta_adjusted_min_and_raw_max() {
        let mut group = PacketGroup::new(config(5, 500_000), WeightedLossConfig::default(), 0);
        group
            .add(&pi(10, 100, 150, 10), 20, 40, false)
            .expect("first");
        group
            .add(&pi(11, 300, 320, 20), 500, 10, false)
            .expect("second");
        // Oracle `grpwin`: the minimum is re-taken whenever the delta-adjusted
        // send time is lower, so a large send delta pushes it below zero.
        assert_eq!(group.send_window(), (-200, 300));
        assert_eq!(group.recv_window(), (110, 320));
        assert_eq!(group.aggregate_deltas(), (520, 50));
        assert_eq!(group.traffic().num_bytes(), 30);
        // Oracle `grpwin` second row: the 500us send span is what the bitrate
        // divides by, and the capped ratio makes it a plain byte rate.
        assert_eq!(group.traffic().captured_traffic_ratio(), 1.0);
        assert_eq!(group.traffic().acknowledged_bitrate(), 480_000);
        assert_eq!(group.traffic().weighted_loss(), 0.0);
        assert_eq!(group.propagated_queuing_delay(), 0);
    }

    #[test]
    fn late_arrival_clears_the_lost_bookkeeping_for_the_same_class() {
        let mut group = PacketGroup::new(config(30, 500_000), WeightedLossConfig::default(), 0);
        group
            .lost_packet(&pi(12, 0, 0, 7))
            .expect("loss before any ack sets min to this packet");
        assert_eq!(group.lost().packets(), 1);
        assert_eq!(group.lost().bytes(), 7);

        group
            .add(&pi(12, 100, 130, 7), 0, 0, true)
            .expect("already received");
        assert_eq!(
            group.lost().packets(),
            1,
            "a packet with a receive time is not lost"
        );

        group
            .add(&pi(12, 100, 130, 7), 0, 0, false)
            .expect("the late arrival is acknowledged");
        assert_eq!(group.lost().packets(), 0);
        assert_eq!(group.lost().bytes(), 0);
        assert_eq!(group.acked().packets(), 1);
        assert_eq!(group.acked().primary().packets(), 1);
    }

    #[test]
    fn class_stat_routes_rtx_before_probe() {
        let mut group = PacketGroup::new(config(30, 500_000), WeightedLossConfig::default(), 0);
        let rtx = PacketInfo {
            is_rtx: true,
            ..pi(10, 100, 150, 10)
        };
        let probe_rtx = PacketInfo {
            is_probe: true,
            ..rtx
        };
        group.add(&rtx, 0, 0, false).expect("rtx");
        group
            .add(&probe_rtx, 0, 0, false)
            .expect("rtx classification wins over probe");
        assert_eq!(group.acked().rtx().packets(), 2);
        assert_eq!(group.acked().probe().packets(), 0);

        let probe = PacketInfo {
            is_probe: true,
            ..pi(11, 110, 160, 20)
        };
        group.lost_packet(&probe).expect("already received");
        assert_eq!(
            group.lost().packets(),
            0,
            "oracle grpcls: a loss report for a packet with a receive time is a no-op"
        );

        let probe = PacketInfo {
            is_probe: true,
            ..pi(11, 110, 0, 20)
        };
        group.lost_packet(&probe).expect("probe loss");
        assert_eq!(group.lost().probe().packets(), 1);
        assert_eq!(group.lost().bytes(), 20);
        assert_eq!(group.traffic().weighted_loss(), f64::INFINITY);
    }

    #[test]
    fn a_lost_sequence_far_above_the_min_stays_lost_beyond_the_bitmap_extent() {
        // min_packets 30 allocates 2 words, so only 128 offsets are trackable.
        let mut group = PacketGroup::new(config(30, 500_000), WeightedLossConfig::default(), 0);
        group
            .lost_packet(&pi(10, 0, 0, 5))
            .expect("first loss fixes the minimum");
        group
            .lost_packet(&pi(200, 0, 0, 5))
            .expect("offset 190 aliases");
        assert_eq!(group.lost().packets(), 2);

        group
            .add(&pi(10, 100, 120, 5), 0, 0, false)
            .expect("offset 0 is recovered");
        assert_eq!(group.lost().packets(), 1);
        group
            .add(&pi(200, 110, 130, 5), 0, 0, false)
            .expect("acknowledged anyway");
        assert_eq!(
            group.lost().packets(),
            1,
            "offset 190 is beyond the 128-slot extent, so the loss is never undone"
        );
        assert_eq!(group.acked().packets(), 2);
        // Oracle `grpgap`: the uncounted loss still counts towards the loss
        // ratio, and the 10us span turns it into a large weighted loss.
        assert!((group.traffic().weighted_loss() - 1.825_707_084_906_553_8).abs() < 1e-9);
    }

    #[test]
    fn propagated_queuing_delay_inherits_only_when_it_keeps_the_total_positive() {
        let mut group = PacketGroup::new(config(30, 500_000), WeightedLossConfig::default(), 100);
        assert_eq!(group.finalized_propagated_queuing_delay(), None);
        group
            .add(&pi(10, 100, 260, 10), 20, 40, false)
            .expect("one packet");
        assert_eq!(group.propagated_queuing_delay(), 120);

        let mut negative =
            PacketGroup::new(config(30, 500_000), WeightedLossConfig::default(), 100);
        negative
            .add(&pi(10, 100, 110, 10), 400, 10, false)
            .expect("one packet");
        assert_eq!(negative.propagated_queuing_delay(), 0);
    }

    #[test]
    fn traffic_uses_the_zero_weighted_loss_config_and_group_stats_use_its_own() {
        let weighted = WeightedLossConfig {
            min_duration_for_loss_validity: PolicyDuration::from_millis(500),
            ..WeightedLossConfig::default()
        };
        let mut group = PacketGroup::new(config(30, 500_000), weighted, 0);
        group
            .add(&pi(10, 0, 10, 10), 0, 100, false)
            .expect("first, oracle grpwl row 1");
        assert_eq!(group.send_window(), (0, 0));
        assert_eq!(group.recv_window(), (-90, 10));
        // A zero send span divides by zero, so the unconfigured stats answer NaN.
        assert!(group.traffic().weighted_loss().is_nan());
        assert_eq!(group.configured_traffic_stats().weighted_loss(), 0.0);

        group
            .lost_packet(&pi(11, 0, 0, 10))
            .expect("lost, oracle grpwl row 2");
        assert_eq!(group.traffic().weighted_loss(), f64::INFINITY);

        group
            .add(&pi(12, 200, 210, 10), 0, 100, false)
            .expect("second, oracle grpwl row 3");
        assert_eq!(group.send_window(), (200, 200));
        assert_eq!(group.acked().packets(), 2);
        assert_eq!(group.lost().packets(), 1);

        // 0us of send span is below the configured validity floor, so the
        // configured stats report no loss at all...
        assert_eq!(group.configured_traffic_stats().weighted_loss(), 0.0);
        assert_eq!(group.configured_traffic_stats().acknowledged_bitrate(), 0);
        // ... while upstream's unconfigured `Traffic()` divides by a zero span.
        assert_eq!(group.traffic().weighted_loss(), f64::INFINITY);
        assert_eq!(group.traffic().acknowledged_bitrate(), 0);
    }
}
