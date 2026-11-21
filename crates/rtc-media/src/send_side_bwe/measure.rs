// Copyright 2023 LiveKit, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Port of the two measurements inside
//! `pkg/sfu/bwe/sendsidebwe/congestion_detector.go`: `qdMeasurement`,
//! `lossMeasurement`, `queuingRegion`, `congestionReason` and
//! `CongestionSignalConfig.IsTriggered`.
//! Upstream at commit `e5655ac20267fa5005dccaf3f0e3bfc3ea688c07`.
//!
//! Both measurements are fed the same packet groups from newest to oldest and
//! each keeps its own sealed state, so the caller must keep replaying groups
//! until both answer. Delays are microseconds, exactly like upstream's
//! `JQRMinDelay.Microseconds()` pre-conversion.

use rtc_core::config::{CongestionSignalConfig, WeightedLossConfig};

use super::group::PacketGroup;
use super::traffic::TrafficStats;

/// `queuingRegion`. Upstream's zero value is `queuingRegionDQR`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum QueuingRegion {
    #[default]
    Dqr,
    Indeterminate,
    Jqr,
}

/// `congestionReason`. Upstream's zero value is `congestionReasonNone`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum CongestionReason {
    #[default]
    None,
    QueuingDelay,
    Loss,
}

/// `CongestionSignalConfig.IsTriggered(numGroups, duration)`, where `duration`
/// is already in microseconds like every other delay in this port.
pub(super) fn is_triggered(
    config: &CongestionSignalConfig,
    num_groups: i64,
    duration_us: i64,
) -> bool {
    num_groups >= config.min_number_of_groups && duration_us >= config.min_duration.as_micros_i64()
}

/// `qdMeasurement`.
pub(super) struct QdMeasurement {
    jqr_config: CongestionSignalConfig,
    dqr_config: CongestionSignalConfig,
    jqr_min_delay_us: i64,
    jqr_min_trend_coefficient: f64,
    dqr_max_delay_us: i64,

    num_groups: i64,
    num_jqr_groups: i64,
    num_dqr_groups: i64,
    min_send_time: i64,
    max_send_time: i64,
    propagated_queuing_delays: Vec<i64>,

    is_sealed: bool,
    min_group_idx: i64,
    max_group_idx: i64,
    queuing_region: QueuingRegion,
}

impl QdMeasurement {
    pub(super) fn new(
        jqr_config: CongestionSignalConfig,
        dqr_config: CongestionSignalConfig,
        jqr_min_delay_us: i64,
        jqr_min_trend_coefficient: f64,
        dqr_max_delay_us: i64,
    ) -> Self {
        Self {
            jqr_config,
            dqr_config,
            jqr_min_delay_us,
            jqr_min_trend_coefficient,
            dqr_max_delay_us,
            num_groups: 0,
            num_jqr_groups: 0,
            num_dqr_groups: 0,
            min_send_time: 0,
            max_send_time: 0,
            propagated_queuing_delays: Vec::new(),
            is_sealed: false,
            min_group_idx: 0,
            max_group_idx: 0,
            // upstream leaves this at the zero value, which is Indeterminate
            // because newQDMeasurement sets it explicitly.
            queuing_region: QueuingRegion::Indeterminate,
        }
    }

    /// `ProcessPacketGroup(pg, groupIdx)`.
    pub(super) fn process_packet_group(&mut self, pg: &PacketGroup, group_idx: i64) {
        if self.is_sealed {
            return;
        }
        let Some(pqd) = pg.finalized_propagated_queuing_delay() else {
            return;
        };

        self.num_groups += 1;
        if self.min_group_idx == 0 || self.min_group_idx > group_idx {
            self.min_group_idx = group_idx;
        }
        self.max_group_idx = self.max_group_idx.max(group_idx);

        let (min_send_time, max_send_time) = pg.send_window();
        if self.min_send_time == 0 || min_send_time < self.min_send_time {
            self.min_send_time = min_send_time;
        }
        self.max_send_time = self.max_send_time.max(max_send_time);

        self.propagated_queuing_delays.push(pqd);

        if pqd < self.dqr_max_delay_us {
            self.num_dqr_groups += 1;
            if self.num_jqr_groups > 0 {
                // broken continuity, seal
                self.is_sealed = true;
            } else if is_triggered(
                &self.dqr_config,
                self.num_dqr_groups,
                self.max_send_time - self.min_send_time,
            ) {
                self.is_sealed = true;
                self.queuing_region = QueuingRegion::Dqr;
            }
        } else if pqd > self.jqr_min_delay_us {
            self.num_jqr_groups += 1;
            if self.num_dqr_groups > 0 {
                // broken continuity, seal
                self.is_sealed = true;
            } else if is_triggered(
                &self.jqr_config,
                self.num_jqr_groups,
                self.max_send_time - self.min_send_time,
            ) && self.trend_coefficient() > self.jqr_min_trend_coefficient
            {
                self.is_sealed = true;
                self.queuing_region = QueuingRegion::Jqr;
            }
        } else if self.num_dqr_groups > 0 || self.num_jqr_groups > 0 {
            // broken continuity, seal
            self.is_sealed = true;
        }
    }

    pub(super) fn is_sealed(&self) -> bool {
        self.is_sealed
    }

    pub(super) fn queuing_region(&self) -> QueuingRegion {
        self.queuing_region
    }

    /// `GroupRange()`.
    pub(super) fn group_range(&self) -> (i64, i64) {
        (self.min_group_idx.max(0), self.max_group_idx.max(0))
    }

    /// `trendCoefficient()`: Kendall tau over the recorded delays in the order
    /// the groups were processed, which is newest first, so a *decreasing* delay
    /// sequence is the concordant direction here.
    pub(super) fn trend_coefficient(&self) -> f64 {
        let mut concordant = 0i64;
        let mut discordant = 0i64;
        let len = self.propagated_queuing_delays.len();
        for i in 0..len.saturating_sub(1) {
            for j in i + 1..len {
                let first = self.propagated_queuing_delays[i];
                let last = self.propagated_queuing_delays[j];
                if first > last {
                    concordant += 1;
                } else if first < last {
                    discordant += 1;
                }
            }
        }
        if concordant + discordant == 0 {
            // a single sample cannot form a trend, so upstream declares the
            // highest trend value rather than the impossible zero.
            if len == 1 {
                return 1.0;
            }
            return 0.0;
        }
        (concordant - discordant) as f64 / (concordant + discordant) as f64
    }

    // Diagnostics accessors, mirroring upstream's MarshalLogObject fields.
    pub(super) fn counts(&self) -> (i64, i64, i64) {
        (self.num_groups, self.num_jqr_groups, self.num_dqr_groups)
    }

    pub(super) fn send_span_us(&self) -> i64 {
        self.max_send_time - self.min_send_time
    }

    pub(super) fn delays(&self) -> &[i64] {
        &self.propagated_queuing_delays
    }
}

/// `lossMeasurement`.
pub(super) struct LossMeasurement {
    jqr_config: CongestionSignalConfig,
    dqr_config: CongestionSignalConfig,
    jqr_min_loss: f64,
    dqr_max_loss: f64,

    num_groups: i64,
    stats: TrafficStats,

    is_jqr_sealed: bool,
    is_dqr_sealed: bool,
    min_group_idx: i64,
    max_group_idx: i64,

    weighted_loss: f64,
    queuing_region: QueuingRegion,
}

impl LossMeasurement {
    pub(super) fn new(
        jqr_config: CongestionSignalConfig,
        dqr_config: CongestionSignalConfig,
        weighted_loss: WeightedLossConfig,
        jqr_min_loss: f64,
        dqr_max_loss: f64,
    ) -> Self {
        Self {
            jqr_config,
            dqr_config,
            jqr_min_loss,
            dqr_max_loss,
            num_groups: 0,
            stats: TrafficStats::new(weighted_loss),
            is_jqr_sealed: false,
            is_dqr_sealed: false,
            min_group_idx: 0,
            max_group_idx: 0,
            weighted_loss: 0.0,
            queuing_region: QueuingRegion::Indeterminate,
        }
    }

    /// `ProcessPacketGroup(pg, groupIdx)`.
    pub(super) fn process_packet_group(&mut self, pg: &PacketGroup, group_idx: i64) {
        if (self.is_jqr_sealed && self.is_dqr_sealed) || !pg.is_finalized() {
            return;
        }

        self.num_groups += 1;
        if self.min_group_idx == 0 || self.min_group_idx > group_idx {
            self.min_group_idx = group_idx;
        }
        self.max_group_idx = self.max_group_idx.max(group_idx);

        self.stats.merge(&pg.traffic_data());

        if !self.is_jqr_sealed
            && is_triggered(&self.jqr_config, self.num_groups, self.stats.duration_us())
        {
            self.is_jqr_sealed = true;
            let loss = self.stats.weighted_loss();
            if loss > self.jqr_min_loss {
                self.weighted_loss = loss;
                self.queuing_region = QueuingRegion::Jqr;
                // seal DQR also as the queuing region has been determined
                self.is_dqr_sealed = true;
                return;
            }
        }

        if !self.is_dqr_sealed
            && is_triggered(&self.dqr_config, self.num_groups, self.stats.duration_us())
        {
            self.is_dqr_sealed = true;
            let loss = self.stats.weighted_loss();
            if loss < self.dqr_max_loss {
                self.weighted_loss = loss;
                self.queuing_region = QueuingRegion::Dqr;
                // seal JQR also as the queuing region has been determined
                self.is_jqr_sealed = true;
                return;
            }
        }
    }

    /// `IsSealed()`: both regions must have settled.
    pub(super) fn is_sealed(&self) -> bool {
        self.is_jqr_sealed && self.is_dqr_sealed
    }

    pub(super) fn queuing_region(&self) -> QueuingRegion {
        self.queuing_region
    }

    /// `GroupRange()`.
    pub(super) fn group_range(&self) -> (i64, i64) {
        (self.min_group_idx.max(0), self.max_group_idx.max(0))
    }

    pub(super) fn num_groups(&self) -> i64 {
        self.num_groups
    }

    pub(super) fn weighted_loss(&self) -> f64 {
        self.weighted_loss
    }

    /// The two seal flags separately. The campaign echoes the upstream fields
    /// `isJQRSealed`/`isDQRSealed`, not the `IsSealed()` conjunction.
    #[cfg(test)]
    pub(super) fn seals(&self) -> (bool, bool) {
        (self.is_jqr_sealed, self.is_dqr_sealed)
    }

    /// `ts.AcknowledgedBitrate()` of the measurement's own accumulated stats.
    #[cfg(test)]
    pub(super) fn acknowledged_bitrate(&self) -> i64 {
        self.stats.acknowledged_bitrate()
    }

    /// The measurement's accumulated stats, as the campaign echo reads them. The
    /// echo has to reach the whole triple, not just the bitrate, because upstream
    /// prints `weightedLoss` from the field and the rate pair from `ts`.
    #[cfg(test)]
    pub(super) fn traffic_stats(&self) -> TrafficStats {
        self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::super::tracker::PacketInfo;
    use super::*;
    use rtc_core::config::{CongestionDetectorConfig, PacketGroupConfig, PolicyDuration};

    fn config() -> CongestionDetectorConfig {
        CongestionDetectorConfig::default()
    }

    /// A finalised group whose propagated queuing delay is exactly `pqd_us`:
    /// packets leave `send_gap` apart and arrive `send_gap + pqd_us` apart, so
    /// the aggregate receive delta exceeds the aggregate send delta by `pqd_us`.
    fn group_with_delay(min_seq: u64, min_send: i64, pqd_us: i64, num_packets: i64) -> PacketGroup {
        let send_gap = 1_000i64;
        let recv_gap = send_gap + pqd_us;
        let mut group = PacketGroup::new(
            PacketGroupConfig {
                min_packets: num_packets,
                max_window_duration: PolicyDuration::from_secs(1),
            },
            WeightedLossConfig::default(),
            0,
        );
        for index in 0..num_packets {
            let send_time = min_send + index * send_gap;
            let pi = PacketInfo {
                sequence_number: min_seq + index as u64,
                send_time,
                recv_time: send_time + pqd_us,
                probe_cluster_id: 0,
                size: 1_000,
                is_rtx: false,
                is_probe: false,
            };
            let deltas = if index == 0 {
                (0, 0)
            } else {
                (send_gap, recv_gap)
            };
            group.add(&pi, deltas.0, deltas.1, false).unwrap();
        }
        assert!(group.is_finalized(), "test fixture must finalise by count");
        assert_eq!(group.propagated_queuing_delay(), pqd_us);
        group
    }

    fn qd(jqr: (i64, i64), dqr: (i64, i64)) -> QdMeasurement {
        QdMeasurement::new(
            CongestionSignalConfig {
                min_number_of_groups: jqr.0,
                min_duration: PolicyDuration::from_millis(jqr.1),
            },
            CongestionSignalConfig {
                min_number_of_groups: dqr.0,
                min_duration: PolicyDuration::from_millis(dqr.1),
            },
            config().jqr_min_delay.as_micros_i64(),
            config().jqr_min_trend_coefficient,
            config().dqr_max_delay.as_micros_i64(),
        )
    }

    #[test]
    fn an_unfinalized_group_is_ignored_and_leaves_the_range_at_zero() {
        let mut group = PacketGroup::new(config().packet_group, WeightedLossConfig::default(), 0);
        let pi = PacketInfo {
            sequence_number: 1,
            send_time: 10,
            recv_time: 20,
            ..PacketInfo::default()
        };
        group.add(&pi, 0, 0, false).unwrap();
        let mut m = qd((2, 200), (3, 300));
        m.process_packet_group(&group, 0);
        assert_eq!(m.counts(), (0, 0, 0));
        assert_eq!(m.group_range(), (0, 0));
        assert!(!m.is_sealed());
    }

    #[test]
    fn a_jqr_region_needs_the_group_count_and_a_downward_trend() {
        let mut m = qd((2, 0), (99, 0));
        m.process_packet_group(&group_with_delay(100, 1_000_000, 70_000, 2), 3);
        assert_eq!(
            m.queuing_region(),
            QueuingRegion::Indeterminate,
            "one sample has the maximum trend but only one JQR group"
        );
        assert_eq!(m.counts(), (1, 1, 0));

        m.process_packet_group(&group_with_delay(200, 900_000, 60_000, 2), 2);
        assert_eq!(m.queuing_region(), QueuingRegion::Jqr);
        assert!(m.is_sealed());
        assert_eq!(m.counts(), (2, 2, 0));
        assert_eq!(m.group_range(), (2, 3));
    }

    #[test]
    fn an_increasing_delay_sequence_fails_the_jqr_trend_gate() {
        // Groups are processed newest first, so 60ms followed by 70ms means the
        // delay is rising: tau = -1.0 must veto the otherwise triggered JQR.
        let mut m = qd((2, 0), (99, 0));
        m.process_packet_group(&group_with_delay(100, 1_000_000, 60_000, 2), 3);
        m.process_packet_group(&group_with_delay(200, 900_000, 70_000, 2), 2);
        assert_eq!(m.trend_coefficient(), -1.0);
        assert_eq!(m.queuing_region(), QueuingRegion::Indeterminate);
        assert!(
            !m.is_sealed(),
            "a vetoed JQR must stay open, not seal an indeterminate region"
        );
    }

    #[test]
    fn a_single_delay_sample_is_declared_the_highest_trend() {
        let mut m = qd((1, 0), (99, 0));
        m.process_packet_group(&group_with_delay(100, 1_000_000, 60_000, 2), 0);
        assert_eq!(m.trend_coefficient(), 1.0);
        assert_eq!(m.queuing_region(), QueuingRegion::Jqr);
    }

    #[test]
    fn a_dqr_region_seals_on_its_own_threshold() {
        let mut m = qd((99, 0), (2, 0));
        m.process_packet_group(&group_with_delay(100, 1_000_000, 0, 2), 4);
        assert_eq!(m.counts(), (1, 0, 1));
        assert!(!m.is_sealed(), "one DQR group is below the two group floor");
        m.process_packet_group(&group_with_delay(200, 900_000, 0, 2), 3);
        assert!(m.is_sealed());
        assert_eq!(m.queuing_region(), QueuingRegion::Dqr);
        assert_eq!(m.group_range(), (3, 4));
    }

    #[test]
    fn broken_continuity_seals_without_naming_a_region() {
        let mut m = qd((99, 0), (99, 0));
        m.process_packet_group(&group_with_delay(100, 1_000_000, 0, 2), 2);
        assert_eq!(m.counts(), (1, 0, 1));
        m.process_packet_group(&group_with_delay(200, 900_000, 60_000, 2), 1);
        assert!(m.is_sealed());
        assert_eq!(
            m.queuing_region(),
            QueuingRegion::Indeterminate,
            "a JQR after a DQR breaks continuity instead of switching region"
        );
    }

    #[test]
    fn an_indeterminate_delay_group_seals_any_started_region() {
        // 20ms is not below the 20ms DQR ceiling and not above the 50ms JQR floor.
        let mut m = qd((2, 0), (2, 0));
        m.process_packet_group(&group_with_delay(100, 1_000_000, 60_000, 2), 2);
        assert_eq!(m.counts(), (1, 1, 0));
        m.process_packet_group(&group_with_delay(200, 900_000, 20_000, 2), 1);
        assert!(m.is_sealed());
        assert_eq!(m.queuing_region(), QueuingRegion::Indeterminate);
    }

    #[test]
    fn the_send_span_uses_the_earliest_group_minimum_and_the_latest_maximum() {
        let mut m = qd((99, 0), (99, 0));
        m.process_packet_group(&group_with_delay(100, 500_000, 0, 2), 2);
        assert_eq!(m.send_span_us(), 1_000);
        m.process_packet_group(&group_with_delay(200, 900_000, 0, 2), 1);
        assert_eq!(m.send_span_us(), 401_000);
    }

    #[test]
    fn the_jqr_gate_uses_the_configured_duration_floor() {
        // Two JQR groups with a 100ms span must not clear a 200ms floor.
        let mut narrow = qd((2, 200), (99, 0));
        narrow.process_packet_group(&group_with_delay(100, 1_000_000, 70_000, 2), 3);
        narrow.process_packet_group(&group_with_delay(200, 900_000, 60_000, 2), 2);
        assert_eq!(narrow.send_span_us(), 101_000);
        assert_eq!(narrow.queuing_region(), QueuingRegion::Indeterminate);

        // The same groups clear a 100ms floor.
        let mut wide = qd((2, 101), (99, 0));
        wide.process_packet_group(&group_with_delay(100, 1_000_000, 70_000, 2), 3);
        wide.process_packet_group(&group_with_delay(200, 900_000, 60_000, 2), 2);
        assert_eq!(wide.queuing_region(), QueuingRegion::Jqr);
    }

    fn loss(
        jqr_groups: i64,
        dqr_groups: i64,
        jqr_min_loss: f64,
        dqr_max_loss: f64,
    ) -> LossMeasurement {
        LossMeasurement::new(
            CongestionSignalConfig {
                min_number_of_groups: jqr_groups,
                min_duration: PolicyDuration::ZERO,
            },
            CongestionSignalConfig {
                min_number_of_groups: dqr_groups,
                min_duration: PolicyDuration::ZERO,
            },
            config().weighted_loss,
            jqr_min_loss,
            dqr_max_loss,
        )
    }

    /// 100 acknowledged packets plus `lost` reported losses across a 601ms
    /// span: the configured weighted loss of that mix is about 0.38.
    fn lossy_group(min_seq: u64, lost: i64) -> PacketGroup {
        let mut group = PacketGroup::new(
            PacketGroupConfig {
                min_packets: 200,
                max_window_duration: PolicyDuration::from_millis(500),
            },
            WeightedLossConfig::default(),
            0,
        );
        for index in 0..100 {
            let send_time = 1_000_000 + index * 1_000;
            let pi = PacketInfo {
                sequence_number: min_seq + index as u64 * 2,
                send_time,
                recv_time: send_time + 1_000,
                size: 1_000,
                ..PacketInfo::default()
            };
            group.add(&pi, 0, 0, false).unwrap();
        }
        for index in 0..lost {
            let pi = PacketInfo {
                sequence_number: min_seq + 1 + index as u64 * 2,
                send_time: 1_000_000 + index * 2_000,
                recv_time: 0,
                size: 1_000,
                ..PacketInfo::default()
            };
            group.add(&pi, 0, 0, true).unwrap();
        }
        // The window duration, not min_packets, finalises the group.
        let late = PacketInfo {
            sequence_number: min_seq + 601,
            send_time: 1_601_000,
            recv_time: 1_602_000,
            size: 1_000,
            ..PacketInfo::default()
        };
        group.add(&late, 0, 0, false).unwrap();
        assert!(group.is_finalized());
        group
    }

    #[test]
    fn a_loss_jqr_above_the_floor_determines_the_region_and_seals_both() {
        let mut m = loss(1, 99, 0.0, 1.0);
        m.process_packet_group(&lossy_group(10, 20), 3);
        assert!(m.is_sealed());
        assert_eq!(m.queuing_region(), QueuingRegion::Jqr);
        assert!(
            (m.weighted_loss() - 0.38).abs() < 0.01,
            "{}",
            m.weighted_loss()
        );
        assert_eq!(m.num_groups(), 1);
        assert_eq!(m.group_range(), (3, 3));
    }

    #[test]
    fn a_jqr_trigger_below_the_loss_floor_leaves_dqr_to_name_the_region() {
        let mut m = loss(1, 2, 10.0, 1.0);
        m.process_packet_group(&lossy_group(10, 20), 3);
        assert_eq!(
            m.queuing_region(),
            QueuingRegion::Indeterminate,
            "a JQR count with too little loss must not claim a congested region"
        );
        assert!(!m.is_sealed(), "the JQR seal alone is not a decision");

        m.process_packet_group(&lossy_group(900, 20), 2);
        assert_eq!(m.queuing_region(), QueuingRegion::Dqr);
        assert!(
            (m.weighted_loss() - 0.43).abs() < 0.01,
            "the merged pair must report its own weighted loss, got {}",
            m.weighted_loss()
        );
        assert!(m.is_sealed());
        assert_eq!(m.num_groups(), 2);
        assert_eq!(m.group_range(), (2, 3));
    }

    #[test]
    fn a_dqr_trigger_with_acceptable_loss_names_the_dqr_region() {
        let mut m = loss(1, 1, 10.0, 1.0);
        m.process_packet_group(&lossy_group(10, 0), 1);
        assert!(m.is_sealed());
        assert_eq!(m.queuing_region(), QueuingRegion::Dqr);
        assert_eq!(m.weighted_loss(), 0.0);
    }

    #[test]
    fn a_dqr_trigger_with_high_loss_leaves_the_weighted_loss_unset() {
        let mut m = loss(99, 1, 0.0, 0.0001);
        m.process_packet_group(&lossy_group(10, 20), 1);
        assert_eq!(
            m.queuing_region(),
            QueuingRegion::Indeterminate,
            "loss above dqrMaxLoss must not be reported as a clear channel"
        );
        assert_eq!(m.weighted_loss(), 0.0);
        assert!(
            !m.is_sealed(),
            "the JQR side never triggered, so the pair stays open"
        );
    }

    #[test]
    fn the_sealed_measurement_stops_absorbing_groups() {
        let mut m = loss(1, 1, 0.0, 1.0);
        m.process_packet_group(&lossy_group(10, 20), 5);
        let groups = m.num_groups();
        m.process_packet_group(&lossy_group(800, 20), 4);
        assert_eq!(m.num_groups(), groups);
        assert_eq!(m.group_range(), (5, 5));
    }

    #[test]
    fn an_unfinalized_group_is_skipped_before_any_counting() {
        let mut group = PacketGroup::new(config().packet_group, WeightedLossConfig::default(), 0);
        let pi = PacketInfo {
            sequence_number: 1,
            send_time: 1_000,
            recv_time: 2_000,
            size: 1_000,
            ..PacketInfo::default()
        };
        group.add(&pi, 0, 0, false).unwrap();
        let mut m = loss(1, 1, 0.0, 1.0);
        m.process_packet_group(&group, 0);
        assert_eq!(m.num_groups(), 0);
        assert_eq!(m.group_range(), (0, 0));
    }

    #[test]
    fn the_default_signal_gates_are_the_frozen_upstream_pairs() {
        let c = config();
        let gates = [
            (c.queuing_delay_early_warning_jqr.clone(), 2, 200),
            (c.queuing_delay_early_warning_dqr.clone(), 3, 300),
            (c.loss_early_warning_jqr.clone(), 3, 300),
            (c.loss_early_warning_dqr.clone(), 4, 400),
            (c.queuing_delay_congested_jqr.clone(), 4, 400),
            (c.queuing_delay_congested_dqr.clone(), 5, 500),
            (c.loss_congested_jqr.clone(), 6, 600),
            (c.loss_congested_dqr.clone(), 6, 600),
        ];
        for (gate, groups, millis) in gates {
            assert_eq!(gate.min_number_of_groups, groups);
            assert_eq!(gate.min_duration, PolicyDuration::from_millis(millis));
            assert!(
                !is_triggered(&gate, groups - 1, millis * 1_000),
                "one group short must never trigger"
            );
            assert!(
                !is_triggered(&gate, groups, millis * 1_000 - 1),
                "one microsecond short of the floor must never trigger"
            );
            assert!(is_triggered(&gate, groups, millis * 1_000));
        }
    }

    #[test]
    fn zero_gates_trigger_immediately() {
        let gate = CongestionSignalConfig::default();
        assert!(is_triggered(&gate, 0, 0));
        assert!(
            !is_triggered(&gate, -1, 0),
            "a negative group count is still below the floor"
        );
        assert!(!is_triggered(&gate, 0, -1));
    }

    #[test]
    fn measurements_start_at_the_upstream_constructor_state() {
        let m = qd((2, 200), (3, 300));
        assert_eq!(m.counts(), (0, 0, 0));
        assert_eq!(m.group_range(), (0, 0));
        assert_eq!(m.queuing_region(), QueuingRegion::Indeterminate);
        assert!(!m.is_sealed());
        assert!(m.delays().is_empty());
        assert_eq!(m.send_span_us(), 0);

        let l = loss(3, 4, 0.25, 0.1);
        assert_eq!(l.num_groups(), 0);
        assert!(!l.is_sealed());
        assert_eq!(l.queuing_region(), QueuingRegion::Indeterminate);
        assert_eq!(l.weighted_loss(), 0.0);
    }

    #[test]
    fn the_queuing_region_and_reason_zero_values_follow_the_go_iota_order() {
        assert_eq!(QueuingRegion::default(), QueuingRegion::Dqr);
        assert_eq!(CongestionReason::default(), CongestionReason::None);
        assert_ne!(QueuingRegion::Dqr, QueuingRegion::Jqr);
    }
}
