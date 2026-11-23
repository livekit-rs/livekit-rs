// Copyright 2023 LiveKit, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Port of `pkg/sfu/bwe/sendsidebwe/probe_packet_group.go` plus the two
//! `ProbeSignalConfig` methods inside `congestion_detector.go`
//! (`IsValid`, `ProbeSignal`). Upstream at commit
//! `e5655ac20267fa5005dccaf3f0e3bfc3ea688c07`.
//!
//! Deliberate porting differences:
//!
//! * Upstream embeds `*packetGroup`; here the group is a named field, because
//!   the embedded methods would otherwise be reachable through a second name.
//! * `doneAt` is Go's zero `time.Time`; the sentinel becomes `Option<Instant>`,
//!   and the clock is supplied by the caller like everywhere else in this port.
//! * `MaybeFinalizeProbe` returns `(ProbeClusterInfoInvalid, false)` for the
//!   invalid case. The value is never read when the flag is false, so the
//!   result collapses to `Option`.

use std::time::Instant;

use rtc_core::config::{
    PolicyDuration, ProbePacketGroupConfig, ProbeSignalConfig, WeightedLossConfig,
};

use super::ccutils::{ProbeClusterInfo, ProbeSignal};
use super::group::{GroupError, PacketGroup};
use super::time::{elapsed_nanos, float_i64};
use super::tracker::PacketInfo;
use super::traffic::TrafficStats;

/// `probePacketGroup`.
pub(super) struct ProbePacketGroup {
    #[cfg_attr(not(test), allow(dead_code))]
    // no shipped caller yet: the `#[cfg(test)]` oracle replay executes it; C74-e wires the pacer/prober/allocator consumer
    config: ProbePacketGroupConfig,
    group: PacketGroup,
    pci: ProbeClusterInfo,
    max_sequence_number: u64,
    #[cfg_attr(not(test), allow(dead_code))]
    // no shipped caller yet: the `#[cfg(test)]` oracle replay executes it; C74-e wires the pacer/prober/allocator consumer
    done_at: Option<Instant>,
}

impl ProbePacketGroup {
    /// `newProbePacketGroup`: the group is built from the probe packet group
    /// config and the detector's weighted loss, with no inherited queuing delay.
    #[cfg_attr(not(test), allow(dead_code))] // no shipped caller yet: the `#[cfg(test)]` oracle replay executes it; C74-e wires the pacer/prober/allocator consumer
    pub(super) fn new(
        config: ProbePacketGroupConfig,
        weighted_loss: WeightedLossConfig,
        pci: ProbeClusterInfo,
    ) -> Self {
        let group = PacketGroup::new(config.packet_group.clone(), weighted_loss, 0);
        Self {
            config,
            group,
            pci,
            max_sequence_number: 0,
            done_at: None,
        }
    }

    /// `ProbeClusterInfo()`.
    #[cfg_attr(not(test), allow(dead_code))] // no shipped caller yet: the `#[cfg(test)]` oracle replay executes it; C74-e wires the pacer/prober/allocator consumer
    pub(super) fn pci(&self) -> ProbeClusterInfo {
        self.pci
    }

    /// The embedded `packetGroup`, which upstream reaches through promotion.
    #[cfg_attr(not(test), allow(dead_code))] // no shipped caller yet: the `#[cfg(test)]` oracle replay executes it; C74-e wires the pacer/prober/allocator consumer
    pub(super) fn group(&self) -> &PacketGroup {
        &self.group
    }

    #[cfg_attr(not(test), allow(dead_code))] // no shipped caller yet: the `#[cfg(test)]` oracle replay executes it; C74-e wires the pacer/prober/allocator consumer
    pub(super) fn max_sequence_number(&self) -> u64 {
        self.max_sequence_number
    }

    /// `doneAt`, for the campaign differential. Go emits `doneAt - origin` in
    /// microseconds and 0 while the probe is still running.
    #[cfg(test)]
    pub(super) fn done_at(&self) -> Option<Instant> {
        self.done_at
    }

    /// `Add`: a packet of another cluster is not an error, it is simply not
    /// part of this probe, and it does not advance the probe sequence either.
    pub(super) fn add(
        &mut self,
        pi: &PacketInfo,
        send_delta: i64,
        recv_delta: i64,
        is_lost: bool,
    ) -> Result<(), GroupError> {
        if pi.probe_cluster_id != self.pci.id {
            return Ok(());
        }
        self.max_sequence_number = self.max_sequence_number.max(pi.sequence_number);
        self.group.add(pi, send_delta, recv_delta, is_lost)
    }

    /// `ProbeClusterDone`: only the matching cluster can close the probe, and
    /// it stamps the completion time with the caller's clock.
    #[cfg_attr(not(test), allow(dead_code))] // no shipped caller yet: the `#[cfg(test)]` oracle replay executes it; C74-e wires the pacer/prober/allocator consumer
    pub(super) fn probe_cluster_done(&mut self, pci: &ProbeClusterInfo, now: Instant) {
        if self.pci.id != pci.id {
            return;
        }
        self.pci.result = pci.result;
        self.done_at = Some(now);
    }

    /// `MaybeFinalizeProbe(maxSequenceNumber, rtt)` at `now`.
    #[cfg_attr(not(test), allow(dead_code))] // no shipped caller yet: the `#[cfg(test)]` oracle replay executes it; C74-e wires the pacer/prober/allocator consumer
    pub(super) fn maybe_finalize_probe(
        &self,
        max_sequence_number: u64,
        rtt: f64,
        now: Instant,
    ) -> Option<ProbeClusterInfo> {
        let done_at = self.done_at?;

        if max_sequence_number != 0 && self.max_sequence_number >= max_sequence_number {
            return Some(self.pci);
        }

        let settle_wait = settle_wait(
            self.config.settle_wait_num_rtt,
            rtt,
            self.config.settle_wait_min,
            self.config.settle_wait_max,
        );
        if elapsed_nanos(now, done_at) < settle_wait.as_nanos_i64() {
            return None;
        }

        Some(self.pci)
    }
}

/// The settle wait of `MaybeFinalizeProbe`: `SettleWaitNumRTT * rtt(seconds)`,
/// clamped from below first and then from above, exactly like the two
/// consecutive Go comparisons. A non-positive RTT therefore clamps to the
/// configured minimum instead of producing a negative wait.
#[cfg_attr(not(test), allow(dead_code))] // no shipped caller yet: the `#[cfg(test)]` oracle replay executes it; C74-e wires the pacer/prober/allocator consumer
fn settle_wait(num_rtt: u32, rtt: f64, min: PolicyDuration, max: PolicyDuration) -> PolicyDuration {
    let scaled = PolicyDuration::from_nanos(float_i64(f64::from(num_rtt) * rtt * 1_000_000_000.0));
    let mut settle_wait = scaled;
    if settle_wait < min {
        settle_wait = min;
    }
    if settle_wait > max {
        settle_wait = max;
    }
    settle_wait
}

/// `ProbeSignalConfig.IsValid(pci)`: both the byte and the duration floor are
/// strict, and both thresholds truncate like Go's `int()`/`time.Duration()`
/// conversions of the scaled goal.
#[cfg_attr(not(test), allow(dead_code))] // no shipped caller yet: the `#[cfg(test)]` oracle replay executes it; C74-e wires the pacer/prober/allocator consumer
pub(super) fn probe_cluster_is_valid(config: &ProbeSignalConfig, pci: &ProbeClusterInfo) -> bool {
    let min_bytes = float_i64(config.min_bytes_ratio * pci.goal.desired_bytes as f64);
    if pci.result.bytes() <= min_bytes {
        return false;
    }
    let min_duration = PolicyDuration::from_nanos(float_i64(
        config.min_duration_ratio * pci.goal.duration.as_nanos_i64() as f64,
    ));
    pci.result.duration() > min_duration
}

/// `ProbeSignalConfig.ProbeSignal(ppg)`: the delay/loss lattice is evaluated on
/// a fresh stats object built from the probe group's own traffic.
#[cfg_attr(not(test), allow(dead_code))] // no shipped caller yet: the `#[cfg(test)]` oracle replay executes it; C74-e wires the pacer/prober/allocator consumer
pub(super) fn probe_cluster_signal(
    config: &ProbeSignalConfig,
    group: &PacketGroup,
) -> (ProbeSignal, i64) {
    let mut stats = TrafficStats::new(config.weighted_loss);
    stats.merge(&group.traffic_data());

    let pqd = group.propagated_queuing_delay();
    if pqd > config.jqr_min_delay.as_micros_i64()
        || stats.weighted_loss() > config.jqr_min_weighted_loss
    {
        return (ProbeSignal::Congesting, stats.acknowledged_bitrate());
    }

    if pqd < config.dqr_max_delay.as_micros_i64()
        && stats.weighted_loss() < config.dqr_max_weighted_loss
    {
        return (ProbeSignal::NotCongesting, stats.acknowledged_bitrate());
    }

    (ProbeSignal::Inconclusive, stats.acknowledged_bitrate())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtc_core::config::{CongestionDetectorConfig, PacketGroupConfig, PolicyDuration};

    use super::super::ccutils::{ProbeClusterGoal, ProbeClusterResult};

    fn detector() -> CongestionDetectorConfig {
        CongestionDetectorConfig::default()
    }

    /// `duration_us` / `start_us` / `end_us` are the campaign's declared
    /// microsecond digits, so a test line carries the same number the pinned Go
    /// sources received (`pstart dur_us=1000000`, `pdone start_us=0;end_us=600000`).
    /// The result stamps are nanoseconds inside Go; the multiply is the only place
    /// that crosses that boundary.
    fn goal(desired_bytes: i64, duration_us: i64) -> ProbeClusterGoal {
        ProbeClusterGoal {
            desired_bytes,
            duration: PolicyDuration::from_micros(duration_us),
            ..ProbeClusterGoal::default()
        }
    }

    fn result(bytes: i64, start_us: i64, end_us: i64) -> ProbeClusterResult {
        ProbeClusterResult {
            start_time_ns: start_us * 1_000,
            end_time_ns: end_us * 1_000,
            bytes_probe: bytes,
            ..ProbeClusterResult::default()
        }
    }

    fn pci(id: u32, goal: ProbeClusterGoal, result: ProbeClusterResult) -> ProbeClusterInfo {
        ProbeClusterInfo {
            id,
            created_at: None,
            goal,
            result,
        }
    }

    /// A two-packet cluster whose propagated queuing delay is `pqd_us` and whose
    /// receive delta is non-zero, so the captured traffic ratio is meaningful.
    fn delayed_group(pqd_us: i64) -> PacketGroup {
        let mut group = PacketGroup::new(
            PacketGroupConfig {
                min_packets: 2,
                max_window_duration: PolicyDuration::from_secs(1),
            },
            WeightedLossConfig::default(),
            0,
        );
        let sends = [1_000_000i64, 1_001_000];
        let recv_deltas = [0i64, 1_000 + pqd_us];
        for index in 0..2 {
            let pi = PacketInfo {
                sequence_number: 10 + index,
                send_time: sends[index as usize],
                recv_time: sends[index as usize] + 1_000 + pqd_us * (index == 1) as i64,
                probe_cluster_id: 3,
                size: 1_000,
                ..PacketInfo::default()
            };
            group
                .add(
                    &pi,
                    if index == 0 { 0 } else { 1_000 },
                    recv_deltas[index as usize],
                    false,
                )
                .unwrap();
        }
        assert_eq!(group.propagated_queuing_delay(), pqd_us);
        group
    }

    fn new_probe_group() -> ProbePacketGroup {
        ProbePacketGroup::new(
            detector().probe_packet_group,
            detector().weighted_loss,
            pci(3, goal(0, 0), ProbeClusterResult::default()),
        )
    }

    #[test]
    fn a_foreign_cluster_packet_is_ignored_without_an_error_or_a_sequence_move() {
        let mut group = new_probe_group();
        let foreign = PacketInfo {
            sequence_number: 500,
            send_time: 1_000,
            recv_time: 1_100,
            probe_cluster_id: 4,
            size: 1_000,
            ..PacketInfo::default()
        };
        assert_eq!(group.add(&foreign, 0, 0, false), Ok(()));
        assert_eq!(group.max_sequence_number(), 0);
        assert_eq!(group.group().acked().packets(), 0);

        let own = PacketInfo {
            sequence_number: 20,
            send_time: 1_000,
            recv_time: 1_100,
            probe_cluster_id: 3,
            size: 1_000,
            ..PacketInfo::default()
        };
        group.add(&own, 0, 0, false).unwrap();
        assert_eq!(group.max_sequence_number(), 20);

        let older = PacketInfo {
            sequence_number: 15,
            ..own
        };
        assert_eq!(
            group.add(&older, 0, 0, false),
            Err(GroupError::OldPacket),
            "oracle pgforeign: the probe group forwards to the plain group, which rejects it"
        );
        assert_eq!(
            group.max_sequence_number(),
            20,
            "only a larger sequence advances"
        );
        assert_eq!(group.group().acked().packets(), 1);
    }

    #[test]
    fn a_mismatched_done_neither_stamps_time_nor_copies_the_result() {
        let mut group = new_probe_group();
        let other = pci(9, goal(1_000, 1_000_000), result(900, 0, 900_000));
        group.probe_cluster_done(&other, Instant::now());
        assert_eq!(group.pci().result, ProbeClusterResult::default());
        assert_eq!(
            group.maybe_finalize_probe(0, 0.07, Instant::now()),
            None,
            "an unstamped probe can never finalise"
        );

        let created = group.pci().goal;
        let same = pci(3, goal(1_000, 1_000_000), result(900, 5_000, 700_000));
        assert_ne!(
            created, same.goal,
            "the closing cluster carries a different goal"
        );
        let done_at = Instant::now();
        group.probe_cluster_done(&same, done_at);
        assert_eq!(group.pci().result, same.result);
        assert_eq!(
            group.pci().goal,
            created,
            "oracle pgdone: only the result is copied, the goal keeps the created cluster's"
        );
        assert_eq!(
            group.maybe_finalize_probe(0, 0.07, done_at + std::time::Duration::from_secs(6)),
            Some(group.pci())
        );
    }

    #[test]
    fn the_sequence_shortcut_finalizes_before_any_settle_wait() {
        let mut group = new_probe_group();
        let done_at = Instant::now();
        group.probe_cluster_done(&pci(3, goal(0, 0), result(0, 0, 0)), done_at);
        // Upstream compares the tracker's probe high sequence against the
        // group's own; a zero tracker sequence never takes that shortcut.
        assert_eq!(
            group.maybe_finalize_probe(0, 100.0, done_at),
            None,
            "the default ceiling is a five second settle wait"
        );

        let pi = PacketInfo {
            sequence_number: 100,
            send_time: 1,
            recv_time: 2,
            probe_cluster_id: 3,
            size: 10,
            ..PacketInfo::default()
        };
        group.add(&pi, 0, 0, false).unwrap();
        assert_eq!(
            group.maybe_finalize_probe(100, 100.0, done_at),
            Some(group.pci()),
            "having reached the tracker sequence finalizes with no wait at all"
        );
        assert_eq!(
            group.maybe_finalize_probe(101, 100.0, done_at),
            None,
            "one short of the tracker sequence must fall through to the settle wait"
        );
    }

    #[test]
    fn the_settle_wait_scales_with_rtt_and_clamps_at_both_ends() {
        assert_eq!(
            settle_wait(
                5,
                100.0,
                PolicyDuration::from_secs(1),
                PolicyDuration::from_secs(2)
            ),
            PolicyDuration::from_secs(2),
            "500s of nominal wait collapses to the ceiling"
        );
        assert_eq!(
            settle_wait(
                5,
                0.010,
                PolicyDuration::from_millis(250),
                PolicyDuration::from_secs(5)
            ),
            PolicyDuration::from_millis(250),
            "50ms of nominal wait rises to the floor"
        );
        assert_eq!(
            settle_wait(
                5,
                0.020,
                PolicyDuration::from_millis(250),
                PolicyDuration::from_secs(5)
            ),
            PolicyDuration::from_millis(250),
            "oracle pgsettle: 5 * 20ms is below the floor, so the floor is the wait"
        );
        assert_eq!(
            settle_wait(
                5,
                0.100,
                PolicyDuration::from_millis(250),
                PolicyDuration::from_secs(5)
            ),
            PolicyDuration::from_millis(500),
            "oracle pgsettle: 5 * 100ms is inside the band and is used as is"
        );
        assert_eq!(
            settle_wait(
                5,
                0.0,
                PolicyDuration::from_millis(250),
                PolicyDuration::from_secs(5)
            ),
            PolicyDuration::from_millis(250)
        );
        assert_eq!(
            settle_wait(
                5,
                -1.0,
                PolicyDuration::from_millis(250),
                PolicyDuration::from_secs(5)
            ),
            PolicyDuration::from_millis(250),
            "a negative rtt clamps to the floor, not to a negative wait"
        );
    }

    #[test]
    fn the_finalization_is_exclusive_of_the_settle_wait_boundary() {
        let mut group = new_probe_group();
        let done_at = Instant::now();
        group.probe_cluster_done(&pci(3, goal(0, 0), result(0, 0, 0)), done_at);
        // default settle wait: 5 * 100us rtt -> 0.5ms -> floored to 250ms.
        let floor = std::time::Duration::from_millis(250);
        assert_eq!(
            group.maybe_finalize_probe(
                0,
                0.0001,
                done_at + floor - std::time::Duration::from_nanos(1)
            ),
            None
        );
        assert_eq!(
            group.maybe_finalize_probe(0, 0.0001, done_at + floor),
            Some(group.pci()),
            "time.Since(doneAt) < settleWait is false exactly at the floor"
        );
    }

    #[test]
    fn validity_needs_both_the_byte_and_the_duration_floor() {
        let config = detector().probe_signal;
        let strict = |bytes: i64, end_us: i64| {
            probe_cluster_is_valid(
                &config,
                &pci(1, goal(1_000, 1_000_000), result(bytes, 0, end_us)),
            )
        };
        assert!(strict(501, 501_000));
        assert!(
            !strict(500, 501_000),
            "0.5 * 1000 is 500 and the comparison is strict"
        );
        assert!(
            !strict(501, 500_000),
            "0.5 * 1s is 500ms and the comparison is strict"
        );
        assert!(!strict(500, 500_000));
        assert!(
            !strict(0, 10_000_000),
            "no bytes at all is never a valid probe"
        );

        // A zero goal has a zero floor, so any traffic at all is valid.
        assert!(probe_cluster_is_valid(
            &config,
            &pci(1, goal(0, 0), result(1, 0, 1_000))
        ));
        assert!(!probe_cluster_is_valid(
            &config,
            &pci(1, goal(0, 0), result(0, 0, 1_000))
        ));
    }

    #[test]
    fn the_probe_signal_lattice_uses_delay_first_and_then_loss() {
        let config = detector().probe_signal;

        let (signal, bitrate) = probe_cluster_signal(&config, &delayed_group(60_000));
        assert_eq!(signal, ProbeSignal::Congesting);
        // 2000 acked bytes over 1000us is 16Mbps, scaled by 1000/61000.
        assert_eq!(bitrate, 262_295);

        let (signal, bitrate) = probe_cluster_signal(&config, &delayed_group(10_000));
        assert_eq!(signal, ProbeSignal::NotCongesting);
        assert_eq!(bitrate, 1_454_545, "1000/11000 of 16Mbps");

        let (signal, bitrate) = probe_cluster_signal(&config, &delayed_group(30_000));
        assert_eq!(
            signal,
            ProbeSignal::Inconclusive,
            "between the two ceilings the probe says nothing"
        );
        assert_eq!(bitrate, 516_129);

        // The delay ceiling is exclusive on both sides.
        assert_eq!(
            probe_cluster_signal(&config, &delayed_group(50_000)).0,
            ProbeSignal::Inconclusive,
            "pqd == jqrMinDelay is not congesting"
        );
        assert_eq!(
            probe_cluster_signal(&config, &delayed_group(20_000)).0,
            ProbeSignal::Inconclusive,
            "pqd == dqrMaxDelay is not clear"
        );
    }

    #[test]
    fn heavy_loss_congests_a_probe_even_without_any_queuing_delay() {
        let config = detector().probe_signal;
        let mut group = PacketGroup::new(
            PacketGroupConfig {
                min_packets: 200,
                max_window_duration: PolicyDuration::from_millis(500),
            },
            WeightedLossConfig::default(),
            0,
        );
        for index in 0..100_i64 {
            let pi = PacketInfo {
                sequence_number: u64::try_from(10 + index * 2).unwrap(),
                send_time: 1_000_000 + index * 1_000,
                recv_time: 1_001_000 + index * 1_000,
                probe_cluster_id: 3,
                size: 1_000,
                ..PacketInfo::default()
            };
            group.add(&pi, 0, 0, false).unwrap();
        }
        for index in 0..40_i64 {
            let pi = PacketInfo {
                sequence_number: u64::try_from(11 + index * 2).unwrap(),
                send_time: 1_000_000 + index * 2_000,
                ..PacketInfo::default()
            };
            group.add(&pi, 0, 0, true).unwrap();
        }
        let late = PacketInfo {
            sequence_number: 611,
            send_time: 1_601_000,
            recv_time: 1_602_000,
            probe_cluster_id: 3,
            size: 1_000,
            ..PacketInfo::default()
        };
        group.add(&late, 0, 0, false).unwrap();

        assert_eq!(group.propagated_queuing_delay(), 0);
        let (signal, bitrate) = probe_cluster_signal(&config, &group);
        assert_eq!(signal, ProbeSignal::Congesting);
        assert_eq!(
            bitrate, 0,
            "a zero receive delta zeroes the captured ratio, as upstream does"
        );
    }

    #[test]
    fn the_probe_group_defaults_are_the_frozen_upstream_values() {
        let config = detector().probe_packet_group;
        assert_eq!(config.packet_group.min_packets, 16_384);
        assert_eq!(
            config.packet_group.max_window_duration,
            PolicyDuration::from_secs(60)
        );
        assert_eq!(config.settle_wait_num_rtt, 5);
        assert_eq!(config.settle_wait_min, PolicyDuration::from_millis(250));
        assert_eq!(config.settle_wait_max, PolicyDuration::from_secs(5));

        let group = new_probe_group();
        assert_eq!(group.pci().id, 3);
        assert_eq!(group.max_sequence_number(), 0);
        assert_eq!(group.group().traffic_data().acked_packets, 0);
    }
}
