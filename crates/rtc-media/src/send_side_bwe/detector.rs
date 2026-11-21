// Copyright 2023 LiveKit, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Port of `pkg/sfu/bwe/sendsidebwe/congestion_detector.go`. Upstream at commit
//! `e5655ac20267fa5005dccaf3f0e3bfc3ea688c07`.
//!
//! This is the whole send-side estimator: it turns a TWCC feedback report into
//! per-packet receive indications through the [`super::tracker`], groups those
//! indications into [`PacketGroup`]s, replays the groups newest-first through the
//! queuing-delay and loss [`super::measure`] gates, runs the three-state
//! `None -> EarlyWarning -> Congested` machine, estimates the available channel
//! capacity from the contributing groups, and drives the probe lifecycle through
//! [`super::probe_group`] and [`super::regulator`].
//!
//! Porting differences that are deliberate:
//!
//! * Upstream reads `mono.Now()` inside `Reset`, `HandleTWCCFeedback`,
//!   `updateCongestionState` and `TrendDetector.AddValue`. Here every clock read
//!   is one caller-supplied `at` per feedback report (or per reset/probe call),
//!   so the whole detector is a pure function of its inputs. The report's single
//!   clock also covers the CTR trend stamp, which upstream reads a few
//!   instructions later on the same monotonic clock.
//! * Upstream notifies a `bwe.BWEListener` outside the lock. The listener is the
//!   caller's own concern, so the state machine result is returned as
//!   `Option<StateChange>` and emitted once per report.
//! * Upstream's logger calls carry no decision. The two paths that would drop a
//!   packet on the floor are counted instead, see [`CongestionDetector::dropped`]
//!   and [`CongestionDetector::older_group_finalized`]; an empty implementation
//!   would make the branch unfalsifiable.
//! * The Go `switch` in `updateCongestionSignal` leaves `congestionReason`
//!   untouched when no case matches, so a report that names no region keeps the
//!   reason of the last report that did. That retention is reproduced, see the
//!   test with the same name.

use std::time::Instant;

use rtc_core::config::{CongestionDetectorConfig, CongestionSignalConfig, PolicyDuration};

use super::ccutils::{ProbeClusterId, ProbeClusterInfo, ProbeSignal};
use super::feedback::TwccFeedback;
use super::group::{GroupError, PacketGroup};
use super::measure::{CongestionReason, LossMeasurement, QdMeasurement, QueuingRegion};
use super::probe_group::{ProbePacketGroup, probe_cluster_is_valid, probe_cluster_signal};
use super::regulator::ProbeRegulator;
use super::time::float_i64;
use super::tracker::{PacketInfo, PacketTracker};
use super::traffic::TrafficStats;
use super::trend::{TrendDetector, TrendDirection};

/// `bwe.DefaultRTT`.
pub(crate) const DEFAULT_RTT: f64 = 0.070;

/// `bwe.RTTSmoothingFactor`.
pub(crate) const RTT_SMOOTHING_FACTOR: f64 = 0.5;

/// `c.estimatedAvailableChannelCapacity` at reset.
pub(crate) const INITIAL_CHANNEL_CAPACITY_BPS: i64 = 100_000_000;

/// `rtcp.TypeTCCPacketNotReceived`.
pub(crate) const SYMBOL_NOT_RECEIVED: u8 = 0;

/// `rtcp.TypeTCCPacketReceivedSmallDelta`.
pub(crate) const SYMBOL_SMALL_DELTA: u8 = 1;

/// `rtcp.TypeTCCPacketReceivedLargeDelta`.
pub(crate) const SYMBOL_LARGE_DELTA: u8 = 2;

/// `rtcp.TypeTCCPacketReceivedWithoutDelta`, and the value a status vector can
/// hold for the two reserved symbols.
pub(crate) const SYMBOL_WITHOUT_DELTA: u8 = 3;

/// `bwe.CongestionState`, in upstream `iota` order. The send-side detector has
/// one more state than the receiver-side `remote_bwe` enum, so it is its own type.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum CongestionState {
    #[default]
    None,
    EarlyWarning,
    Congested,
}

/// The report as the detector consumes it: the header fields plus the packet
/// status symbols in wire order.
///
/// Upstream walks `report.PacketChunks` and expands each run-length or status
/// vector chunk in place, breaking as soon as the sequence number reaches
/// `base + packet_status_count`. Because a chunk only ever contributes symbols,
/// that walk is exactly "the first `packet_status_count` symbols of the expanded
/// chunk stream", so the expansion happens at the wire boundary and this port
/// walks a flat symbol list. [`super::super::pion_gcc`] does that expansion and
/// keeps the pinned Pion chunk accounting; see `twcc_report`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct TwccFeedbackReport {
    pub(super) base_sequence_number: u16,
    pub(super) packet_status_count: u16,
    pub(super) reference_time: u32,
    pub(super) fb_pkt_count: u8,
    /// One entry per packet status symbol: 0 not received, 1 small delta,
    /// 2 large delta, 3 received without delta.
    pub(super) symbols: Vec<u8>,
    /// `RecvDeltas` in microseconds, in the order the symbols that carry one
    /// appear. pion keeps these already scaled by `TypeTCCDeltaScaleFactor`.
    pub(super) deltas_us: Vec<i64>,
}

/// The listener callback upstream makes after unlocking.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct StateChange {
    pub(super) from: CongestionState,
    pub(super) to: CongestionState,
    pub(super) capacity_bps: i64,
}

/// `congestionDetector`.
pub(super) struct CongestionDetector {
    config: CongestionDetectorConfig,
    packet_tracker: PacketTracker,
    twcc_feedback: TwccFeedback,

    rtt: f64,
    packet_groups: Vec<PacketGroup>,

    probe_packet_group: Option<ProbePacketGroup>,
    probe_regulator: ProbeRegulator,

    estimated_available_channel_capacity: i64,
    estimate_traffic_stats: Option<TrafficStats>,

    congestion_state: CongestionState,
    congestion_state_switched_at: Instant,

    congested_ctr_trend: Option<TrendDetector>,
    congested_traffic_stats: Option<TrafficStats>,
    congested_packet_group: Option<PacketGroup>,

    congestion_reason: CongestionReason,
    qd_measurement: Option<QdMeasurement>,
    loss_measurement: Option<LossMeasurement>,

    /// Upstream logs and drops the packet when a freshly created group refuses
    /// it. Counted here so the branch is observable.
    dropped_after_finalize: u64,
    /// Upstream logs "unexpected finalized group" while falling back to an older
    /// group; the scan then continues to the next older group.
    older_group_finalized: u64,
}

impl CongestionDetector {
    /// `newCongestionDetector` followed by its `Reset` call.
    pub(super) fn new(config: CongestionDetectorConfig, at: Instant) -> Self {
        let probe_regulator = ProbeRegulator::new(config.probe_regulator.clone(), at);
        let mut detector = Self {
            packet_tracker: PacketTracker::new(),
            twcc_feedback: TwccFeedback::new(),
            probe_regulator,
            congestion_state_switched_at: at,
            estimated_available_channel_capacity: INITIAL_CHANNEL_CAPACITY_BPS,
            rtt: DEFAULT_RTT,
            config,
            packet_groups: Vec::new(),
            probe_packet_group: None,
            estimate_traffic_stats: None,
            congestion_state: CongestionState::None,
            congested_ctr_trend: None,
            congested_traffic_stats: None,
            congested_packet_group: None,
            congestion_reason: CongestionReason::None,
            qd_measurement: None,
            loss_measurement: None,
            dropped_after_finalize: 0,
            older_group_finalized: 0,
        };
        detector.reset(at);
        detector
    }

    /// `Reset`. The packet tracker and the TWCC report bookkeeping survive:
    /// upstream rebuilds neither, so sequence reconstruction and the feedback
    /// interval estimate keep their state across a stream restart.
    pub(super) fn reset(&mut self, at: Instant) {
        self.rtt = DEFAULT_RTT;
        self.packet_groups.clear();
        self.probe_packet_group = None;
        self.probe_regulator = ProbeRegulator::new(self.config.probe_regulator.clone(), at);
        self.estimated_available_channel_capacity = INITIAL_CHANNEL_CAPACITY_BPS;
        self.estimate_traffic_stats = None;
        self.congestion_state = CongestionState::None;
        self.congestion_state_switched_at = at;
        self.clear_ctr_trend();
        self.congestion_reason = CongestionReason::None;
        self.qd_measurement = None;
        self.loss_measurement = None;
    }

    /// `HandleTWCCFeedback`: returns the listener notification upstream would
    /// have made, if any.
    pub(super) fn handle_twcc_feedback(
        &mut self,
        report: &TwccFeedbackReport,
        at: Instant,
    ) -> Option<StateChange> {
        let (mut recv_ref_time, _is_out_of_order) =
            self.twcc_feedback
                .process_report(report.reference_time, report.fb_pkt_count, at);

        if self.packet_groups.is_empty() {
            self.packet_groups.push(self.new_packet_group(0));
        }

        // 1. record every receive time the remote reports
        // 2. hand each acknowledged packet to a packet group
        //
        // A lost feedback report is not reconstructed: the RFC says to ignore the
        // packets that report would have carried, which is what walking the
        // symbols of the reports that did arrive amounts to.
        let mut sequence_number = report.base_sequence_number;
        let end_exclusive = sequence_number.wrapping_add(report.packet_status_count);
        let mut delta_idx = 0usize;
        for &symbol in &report.symbols {
            if sequence_number == end_exclusive {
                break;
            }

            let mut recv_time = 0i64;
            let mut is_lost = false;
            if symbol == SYMBOL_NOT_RECEIVED {
                is_lost = true;
            } else if (symbol == SYMBOL_SMALL_DELTA || symbol == SYMBOL_LARGE_DELTA)
                && delta_idx < report.deltas_us.len()
            {
                // The report's reference time is the anchor and each delta is
                // relative to the previous reported packet, so the receive clock
                // accumulates across the whole report.
                recv_ref_time = recv_ref_time.wrapping_add(report.deltas_us[delta_idx]);
                delta_idx += 1;
                recv_time = recv_ref_time;
            } else {
                // A reserved symbol, or a delta-carrying symbol the report has no
                // delta left for: nothing to record, just move on.
                sequence_number = sequence_number.wrapping_add(1);
                continue;
            }

            if let Some((pi, send_delta, recv_delta)) = self
                .packet_tracker
                .record_packet_indication_from_remote(sequence_number, recv_time)
            {
                // `sendTime == 0` is upstream's "never sent" sentinel, which also
                // swallows the very first packet of a stream because its send time
                // is the reference of the clock. That is replayed, not fixed.
                if pi.send_time != 0 {
                    self.track_packet_group(&pi, send_delta, recv_delta, is_lost, at);
                }
            }
            sequence_number = sequence_number.wrapping_add(1);
        }

        self.prune_packet_groups(at);
        self.congestion_detection_state_machine(at)
    }

    /// The `trackPacketGroup` closure of `HandleTWCCFeedback`. Upstream keeps a
    /// pointer to the newest group; because new groups are only ever appended,
    /// that pointer is the last element, so the closure works on the index here.
    fn track_packet_group(
        &mut self,
        pi: &PacketInfo,
        send_delta: i64,
        recv_delta: i64,
        is_lost: bool,
        at: Instant,
    ) {
        self.update_ctr_trend(pi, send_delta, recv_delta, is_lost, at);

        if let Some(probe_group) = self.probe_packet_group.as_mut() {
            // Upstream discards the error: a packet of another probe cluster, or
            // one the probe group already has, is simply not part of the probe.
            let _ = probe_group.add(pi, send_delta, recv_delta, is_lost);
        }

        let last = self.packet_groups.len() - 1;
        match self.packet_groups[last].add(pi, send_delta, recv_delta, is_lost) {
            Ok(()) => return,
            Err(GroupError::OldPacket) => {}
            Err(GroupError::Finalized) => {
                // The group ended; the queue it built up is inherited by the next
                // one, because those packets are still in flight.
                let inherited = self.packet_groups[last].propagated_queuing_delay();
                let mut group = self.new_packet_group(inherited);
                // Upstream appends the new group before retrying the add, so a
                // group that refuses the packet is still part of the list.
                let refused = group.add(pi, send_delta, recv_delta, is_lost).is_err();
                self.packet_groups.push(group);
                if refused {
                    self.dropped_after_finalize += 1;
                }
                return;
            }
        }

        // An old packet: try the older groups, newest first, excluding the last.
        for idx in (0..self.packet_groups.len() - 1).rev() {
            match self.packet_groups[idx].add(pi, send_delta, recv_delta, is_lost) {
                Ok(()) => return,
                Err(GroupError::Finalized) => self.older_group_finalized += 1,
                Err(GroupError::OldPacket) => {}
            }
        }
    }

    /// `newPacketGroup` with the detector's own group and loss configuration.
    fn new_packet_group(&self, inherited_queuing_delay: i64) -> PacketGroup {
        PacketGroup::new(
            self.config.packet_group.clone(),
            self.config.weighted_loss.clone(),
            inherited_queuing_delay,
        )
    }

    /// `prunePacketGroups`. Upstream leaves the list untouched when no group is
    /// newer than the threshold, which is why a list of entirely stale groups is
    /// retained until a fresh one appears.
    fn prune_packet_groups(&mut self, at: Instant) {
        if self.packet_groups.is_empty() {
            return;
        }
        let Some(threshold) = self
            .packet_tracker
            .base_send_time_threshold(at, self.config.packet_group_max_age.as_micros_i64())
        else {
            return;
        };

        for idx in 0..self.packet_groups.len() {
            if self.packet_groups[idx].min_send_time() > threshold {
                self.packet_groups.drain(..idx);
                return;
            }
        }
    }

    /// `congestionDetectionStateMachine`: returns the notification upstream hands
    /// to the listener, or `None` when it stays silent.
    fn congestion_detection_state_machine(&mut self, at: Instant) -> Option<StateChange> {
        let from_state = self.congestion_state;
        let mut to_state = self.congestion_state;

        match from_state {
            CongestionState::None => {
                if self.update_early_warning_signal() == QueuingRegion::Jqr {
                    to_state = CongestionState::EarlyWarning;
                }
            }
            CongestionState::EarlyWarning => {
                if self.update_congested_signal() == QueuingRegion::Jqr {
                    to_state = CongestionState::Congested;
                } else if self.update_early_warning_signal() == QueuingRegion::Dqr {
                    to_state = CongestionState::None;
                }
            }
            CongestionState::Congested => {
                if self.update_congested_signal() == QueuingRegion::Dqr {
                    to_state = CongestionState::None;
                }
            }
        }

        let mut should_notify = false;
        let mut from = from_state;
        let mut to = to_state;
        if to_state != from_state {
            self.estimate_available_channel_capacity();
            from = self.update_congestion_state(to_state, at);
            to = self.congestion_state;
            should_notify = true;
        }

        // A downward captured-traffic trend means the congested estimate was too
        // generous, so it is lowered even though the state did not change.
        if self.congested_ctr_trend_active_and_downward() {
            let acknowledged = self
                .congested_traffic_stats
                .as_ref()
                .map(TrafficStats::acknowledged_bitrate)
                .unwrap_or(0);
            if acknowledged < self.estimated_available_channel_capacity {
                self.estimated_available_channel_capacity = acknowledged;
                should_notify = true;
            }
            // Reset so the next trend is measured on a fresh set of samples.
            self.reset_ctr_trend();
        }

        if !should_notify {
            return None;
        }
        Some(StateChange {
            from,
            to,
            capacity_bps: self.estimated_available_channel_capacity,
        })
    }

    /// `Config.QueuingDelayEarlyWarning*` and `Config.LossEarlyWarning*`.
    fn update_early_warning_signal(&mut self) -> QueuingRegion {
        let (qd_jqr, qd_dqr, loss_jqr, loss_dqr) = (
            self.config.queuing_delay_early_warning_jqr.clone(),
            self.config.queuing_delay_early_warning_dqr.clone(),
            self.config.loss_early_warning_jqr.clone(),
            self.config.loss_early_warning_dqr.clone(),
        );
        self.update_congestion_signal(qd_jqr, qd_dqr, loss_jqr, loss_dqr)
    }

    /// `Config.QueuingDelayCongested*` and `Config.LossCongested*`.
    fn update_congested_signal(&mut self) -> QueuingRegion {
        let (qd_jqr, qd_dqr, loss_jqr, loss_dqr) = (
            self.config.queuing_delay_congested_jqr.clone(),
            self.config.queuing_delay_congested_dqr.clone(),
            self.config.loss_congested_jqr.clone(),
            self.config.loss_congested_dqr.clone(),
        );
        self.update_congestion_signal(qd_jqr, qd_dqr, loss_jqr, loss_dqr)
    }

    /// `updateCongestionSignal`: rebuild both measurements and replay the groups
    /// from newest to oldest until both have decided.
    fn update_congestion_signal(
        &mut self,
        qd_jqr: CongestionSignalConfig,
        qd_dqr: CongestionSignalConfig,
        loss_jqr: CongestionSignalConfig,
        loss_dqr: CongestionSignalConfig,
    ) -> QueuingRegion {
        self.qd_measurement = Some(QdMeasurement::new(
            qd_jqr,
            qd_dqr,
            self.config.jqr_min_delay.as_micros_i64(),
            self.config.jqr_min_trend_coefficient,
            self.config.dqr_max_delay.as_micros_i64(),
        ));
        self.loss_measurement = Some(LossMeasurement::new(
            loss_jqr,
            loss_dqr,
            self.config.weighted_loss.clone(),
            self.config.jqr_min_weighted_loss,
            self.config.dqr_max_weighted_loss,
        ));

        for idx in (0..self.packet_groups.len()).rev() {
            let group_idx = idx as i64;
            let group = &self.packet_groups[idx];
            if let Some(qd) = self.qd_measurement.as_mut() {
                qd.process_packet_group(group, group_idx);
            }
            if let Some(loss) = self.loss_measurement.as_mut() {
                loss.process_packet_group(group, group_idx);
            }
            let both_sealed = self
                .qd_measurement
                .as_ref()
                .is_some_and(QdMeasurement::is_sealed)
                && self
                    .loss_measurement
                    .as_ref()
                    .is_some_and(LossMeasurement::is_sealed);
            if both_sealed {
                break;
            }
        }

        let qd_region = self
            .qd_measurement
            .as_ref()
            .map(QdMeasurement::queuing_region)
            .unwrap_or(QueuingRegion::Indeterminate);
        let loss_region = self
            .loss_measurement
            .as_ref()
            .map(LossMeasurement::queuing_region)
            .unwrap_or(QueuingRegion::Indeterminate);

        let mut region = QueuingRegion::Indeterminate;
        if qd_region == QueuingRegion::Jqr {
            region = QueuingRegion::Jqr;
            self.congestion_reason = CongestionReason::QueuingDelay;
        } else if loss_region == QueuingRegion::Jqr {
            region = QueuingRegion::Jqr;
            self.congestion_reason = CongestionReason::Loss;
        } else if qd_region == QueuingRegion::Dqr && loss_region == QueuingRegion::Dqr {
            region = QueuingRegion::Dqr;
            self.congestion_reason = CongestionReason::None;
        }
        region
    }

    /// `updateCTRTrend`: while congested, the captured traffic ratio of each
    /// congested packet group window is quantised and fed to a trend detector.
    fn update_ctr_trend(
        &mut self,
        pi: &PacketInfo,
        send_delta: i64,
        recv_delta: i64,
        is_lost: bool,
        at: Instant,
    ) {
        if self.congested_ctr_trend.is_none() {
            return;
        }

        let mut group = self
            .congested_packet_group
            .take()
            .unwrap_or_else(|| self.new_congested_packet_group(0));
        if group.add(pi, send_delta, recv_delta, is_lost).err() != Some(GroupError::Finalized) {
            self.congested_packet_group = Some(group);
            return;
        }

        // Keep growing the window and measure over longer spans: if the
        // congestion is not relieving, the ratio trends down.
        let data = group.traffic_data();
        if let Some(stats) = self.congested_traffic_stats.as_mut() {
            stats.merge(&data);
        }
        let mut ts = TrafficStats::new(self.config.weighted_loss.clone());
        ts.merge(&data);
        let ctr = ts.captured_traffic_ratio();
        // Quantise to filter out small changes.
        let epsilon = self.config.congested_ctr_epsilon;
        let quantised = float_i64((ctr + epsilon / 2.0) / epsilon) as f64 * epsilon;
        if let Some(trend) = self.congested_ctr_trend.as_mut() {
            trend.add_value(quantised, at);
        }

        let inherited = group.propagated_queuing_delay();
        self.congested_packet_group = Some(self.new_congested_packet_group(inherited));
    }

    /// `newPacketGroup(Config.CongestedPacketGroup, ...)`.
    fn new_congested_packet_group(&self, inherited_queuing_delay: i64) -> PacketGroup {
        PacketGroup::new(
            self.config.congested_packet_group.clone(),
            self.config.weighted_loss.clone(),
            inherited_queuing_delay,
        )
    }

    fn congested_ctr_trend_active_and_downward(&self) -> bool {
        self.congested_ctr_trend
            .as_ref()
            .is_some_and(|trend| trend.direction() == TrendDirection::Downward)
    }

    /// `estimateAvailableChannelCapacity`.
    fn estimate_available_channel_capacity(&mut self) {
        self.estimate_traffic_stats = None;
        if self.packet_groups.is_empty() || self.probe_packet_group.is_some() {
            return;
        }

        // When congested, only the groups that contributed to the signal count;
        // otherwise measure over a time window.
        let mut use_window = false;
        let mut is_agg_valid = true;
        let mut min_group_idx = 0usize;
        let mut max_group_idx = self.packet_groups.len() - 1;
        match self.congestion_reason {
            CongestionReason::QueuingDelay => {
                if let Some(qd) = self.qd_measurement.as_ref() {
                    let (min, max) = qd.group_range();
                    min_group_idx = min as usize;
                    max_group_idx = max as usize;
                }
            }
            CongestionReason::Loss => {
                if let Some(loss) = self.loss_measurement.as_ref() {
                    let (min, max) = loss.group_range();
                    min_group_idx = min as usize;
                    max_group_idx = max as usize;
                }
            }
            CongestionReason::None => {
                use_window = true;
                is_agg_valid = false;
            }
        }

        let mut agg = TrafficStats::new(self.config.weighted_loss.clone());
        let window_us = self.config.estimation_window_duration.as_micros_i64();
        let mut idx = max_group_idx.min(self.packet_groups.len() - 1);
        loop {
            let group = &self.packet_groups[idx];
            if group.is_finalized() {
                agg.merge(&group.traffic_data());
                if use_window && agg.duration_us() > window_us {
                    is_agg_valid = true;
                    break;
                }
            }
            if idx == 0 || idx <= min_group_idx {
                break;
            }
            idx -= 1;
        }

        if is_agg_valid {
            self.estimated_available_channel_capacity = agg.acknowledged_bitrate();
            self.estimate_traffic_stats = Some(agg);
        }
    }

    /// `updateCongestionState`: returns the state it switched away from.
    fn update_congestion_state(&mut self, state: CongestionState, at: Instant) -> CongestionState {
        if state != self.congestion_state {
            self.congestion_state_switched_at = at;
        }
        let from_state = self.congestion_state;
        self.congestion_state = state;

        // While congested, keep watching the captured traffic ratio: the estimate
        // at the moment congestion starts can be too high, and the allocation
        // then never relieves the congestion.
        if state == CongestionState::Congested && from_state != CongestionState::Congested {
            self.create_ctr_trend();
        } else if state != CongestionState::Congested {
            self.clear_ctr_trend();
        }

        from_state
    }

    /// `createCTRTrend`.
    fn create_ctr_trend(&mut self) {
        self.reset_ctr_trend();
        self.congested_packet_group = None;
    }

    /// `resetCTRTrend`.
    fn reset_ctr_trend(&mut self) {
        self.congested_ctr_trend =
            Some(TrendDetector::new(self.config.congested_ctr_trend.clone()));
        self.congested_traffic_stats = Some(TrafficStats::new(self.config.weighted_loss.clone()));
    }

    /// `clearCTRTrend`, which is also the constructor state.
    fn clear_ctr_trend(&mut self) {
        self.congested_ctr_trend = None;
        self.congested_traffic_stats = None;
        self.congested_packet_group = None;
    }

    /// `UpdateRTT`. A zero argument is upstream's "no measurement yet" and
    /// restores the 70 ms default rather than smoothing towards zero.
    pub(super) fn update_rtt(&mut self, rtt: f64) {
        if rtt == 0.0 {
            self.rtt = DEFAULT_RTT;
        } else if self.rtt == 0.0 {
            self.rtt = rtt;
        } else {
            self.rtt = RTT_SMOOTHING_FACTOR * rtt + (1.0 - RTT_SMOOTHING_FACTOR) * self.rtt;
        }
    }

    /// `CanProbe`.
    pub(super) fn can_probe(&self, at: Instant) -> bool {
        self.congestion_state == CongestionState::None
            && self.probe_packet_group.is_none()
            && self.probe_regulator.can_probe(at)
    }

    /// `ProbeDuration`.
    pub(super) fn probe_duration(&self) -> PolicyDuration {
        self.probe_regulator.duration()
    }

    /// `ProbeClusterStarting`: a new cluster replaces the running one, exactly as
    /// upstream's unconditional assignment does.
    pub(super) fn probe_cluster_starting(&mut self, pci: ProbeClusterInfo) {
        self.probe_packet_group = Some(ProbePacketGroup::new(
            self.config.probe_packet_group.clone(),
            self.config.weighted_loss.clone(),
            pci,
        ));
        self.packet_tracker.probe_cluster_starting(pci.id);
    }

    /// `ProbeClusterDone`: the tracker stops attributing sends to the cluster and
    /// the probe group records the result and the completion clock, which starts
    /// the settle wait.
    pub(super) fn probe_cluster_done(&mut self, pci: &ProbeClusterInfo, at: Instant) {
        self.packet_tracker.probe_cluster_done(pci.id);
        if let Some(probe_group) = self.probe_packet_group.as_mut() {
            probe_group.probe_cluster_done(pci, at);
        }
    }

    /// `ProbeClusterIsGoalReached`.
    pub(super) fn probe_cluster_is_goal_reached(&self) -> bool {
        let Some(probe_group) = self.probe_packet_group.as_ref() else {
            return false;
        };
        if self.congestion_state != CongestionState::None {
            return false;
        }

        let pci = probe_group.pci();
        if !probe_cluster_is_valid(&self.config.probe_signal, &pci) {
            return false;
        }

        let (signal, estimated) =
            probe_cluster_signal(&self.config.probe_signal, probe_group.group());
        signal != ProbeSignal::NotCongesting && estimated > pci.goal.desired_bps
    }

    /// `ProbeClusterFinalize`. `None` is upstream's `isFinalized == false`, for
    /// which the `(Inconclusive, 0)` pair is never read.
    pub(super) fn probe_cluster_finalize(&mut self, at: Instant) -> Option<(ProbeSignal, i64)> {
        let probe_group = self.probe_packet_group.as_ref()?;
        let max_sequence_number = self.packet_tracker.probe_max_sequence_number();
        let Some(pci) = probe_group.maybe_finalize_probe(max_sequence_number, self.rtt, at) else {
            return None;
        };

        // A congestion signal that moved while the probe ran outranks the probe.
        if self.congestion_state != CongestionState::None {
            let signal = ProbeSignal::Congesting;
            self.probe_regulator
                .probe_signal(signal, pci.created_at, at);
            self.probe_packet_group = None;
            return Some((signal, self.estimated_available_channel_capacity));
        }

        let (signal, estimated) =
            probe_cluster_signal(&self.config.probe_signal, probe_group.group());
        if signal == ProbeSignal::NotCongesting
            && estimated > self.estimated_available_channel_capacity
        {
            self.estimated_available_channel_capacity = estimated;
        }
        self.probe_regulator
            .probe_signal(signal, pci.created_at, at);
        self.probe_packet_group = None;
        Some((signal, self.estimated_available_channel_capacity))
    }

    /// `CongestionState`.
    pub(super) fn congestion_state(&self) -> CongestionState {
        self.congestion_state
    }

    /// `EstimatedAvailableChannelCapacity`, the value the listener receives.
    pub(super) fn estimated_available_channel_capacity(&self) -> i64 {
        self.estimated_available_channel_capacity
    }

    pub(super) fn rtt(&self) -> f64 {
        self.rtt
    }

    /// The pacer's send path: every transmitted packet has to be recorded before
    /// a feedback report can make sense of it.
    pub(super) fn packet_tracker_mut(&mut self) -> &mut PacketTracker {
        &mut self.packet_tracker
    }

    /// The campaign differential reads the tracker's reconstructed counter and its
    /// two base stamps back out, exactly as the Go harness prints
    /// `packetTracker.sequenceNumber` and the two bases.
    #[cfg(test)]
    pub(super) fn packet_tracker(&self) -> &PacketTracker {
        &self.packet_tracker
    }

    /// Diagnostics for the two upstream log-only drop paths.
    pub(super) fn dropped(&self) -> (u64, u64) {
        (self.dropped_after_finalize, self.older_group_finalized)
    }

    /// Test and differential inspection surface.
    pub(super) fn packet_group_count(&self) -> usize {
        self.packet_groups.len()
    }

    pub(super) fn packet_group(&self, idx: usize) -> &PacketGroup {
        &self.packet_groups[idx]
    }

    pub(super) fn congestion_reason(&self) -> CongestionReason {
        self.congestion_reason
    }

    pub(super) fn probe_packet_group_id(&self) -> Option<ProbeClusterId> {
        self.probe_packet_group
            .as_ref()
            .map(ProbePacketGroup::pci)
            .map(|pci| pci.id)
    }

    /// The live probe group, for the campaign differential, which reads the
    /// group's own sequence high-water, completion stamp and cluster info back
    /// out of the detector the way the Go harness reads its fields.
    #[cfg(test)]
    pub(super) fn probe_packet_group(&self) -> Option<&ProbePacketGroup> {
        self.probe_packet_group.as_ref()
    }

    #[cfg(test)]
    pub(super) fn probe_packet_group_mut(&mut self) -> Option<&mut ProbePacketGroup> {
        self.probe_packet_group.as_mut()
    }

    /// The oracle's `settle` op overwrites the detector RTT directly instead of
    /// running it through `UpdateRTT`, so an exact stimulus RTT is expressible.
    #[cfg(test)]
    pub(super) fn set_rtt(&mut self, rtt: f64) {
        self.rtt = rtt;
    }

    pub(super) fn probe_is_running(&self) -> bool {
        self.probe_packet_group.is_some()
    }

    pub(super) fn ctr_trend_is_active(&self) -> bool {
        self.congested_ctr_trend.is_some()
    }

    pub(super) fn estimate_is_aggregated(&self) -> bool {
        self.estimate_traffic_stats.is_some()
    }

    pub(super) fn congestion_state_switched_at(&self) -> Instant {
        self.congestion_state_switched_at
    }

    /// The campaign differential reads the measurement objects, the TWCC
    /// bookkeeping and the trend/estimate sidecars the way the Go harness reads
    /// its unexported fields.
    #[cfg(test)]
    pub(super) fn config(&self) -> &CongestionDetectorConfig {
        &self.config
    }

    #[cfg(test)]
    pub(super) fn twcc_feedback(&self) -> &TwccFeedback {
        &self.twcc_feedback
    }

    #[cfg(test)]
    pub(super) fn qd_measurement(&self) -> Option<&QdMeasurement> {
        self.qd_measurement.as_ref()
    }

    #[cfg(test)]
    pub(super) fn loss_measurement(&self) -> Option<&LossMeasurement> {
        self.loss_measurement.as_ref()
    }

    #[cfg(test)]
    pub(super) fn estimate_traffic_stats(&self) -> Option<&TrafficStats> {
        self.estimate_traffic_stats.as_ref()
    }

    #[cfg(test)]
    pub(super) fn congested_ctr_trend(&self) -> Option<&TrendDetector> {
        self.congested_ctr_trend.as_ref()
    }

    #[cfg(test)]
    pub(super) fn packet_groups(&self) -> &[PacketGroup] {
        &self.packet_groups
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtc_core::config::{PacketGroupConfig, TrendDetectorConfig};
    use std::time::Duration;

    use super::super::ccutils::{ProbeClusterGoal, ProbeClusterResult};

    /// The test clock in milliseconds, named so that no call site can hand it a
    /// microsecond digit from the campaign by accident (`at_us` is the oracle's
    /// unit, `at_ms` is 1/1000 of it).
    fn at_ms(base: Instant, millis: u64) -> Instant {
        base + Duration::from_millis(millis)
    }

    /// The group configuration every hand-computed expectation below assumes:
    /// a group finalises on its second acknowledgement, and the 5 s window never
    /// interferes with the 100 ms cadence. Everything else stays at the frozen
    /// upstream default because the expected capacities are derived from it.
    fn test_config() -> CongestionDetectorConfig {
        CongestionDetectorConfig {
            packet_group: PacketGroupConfig {
                min_packets: 2,
                max_window_duration: PolicyDuration::from_secs(5),
            },
            ..CongestionDetectorConfig::default()
        }
    }

    fn detector(base: Instant) -> CongestionDetector {
        CongestionDetector::new(test_config(), base)
    }

    /// `count` sends starting at packet ordinal `first`, 100 ms apart, on wire
    /// sequences `ordinal + 1`. Ordinal 0 is sent exactly at `base`, which is the
    /// reference of the send clock, so it carries `send_time == 0`.
    fn send(det: &mut CongestionDetector, base: Instant, first: usize, count: usize) {
        send_stamped(det, base, first, count, 100_000, 0);
    }

    /// As `send`, with an explicit cadence and probe cluster stamp. A non-zero
    /// cluster marks the packet as probe traffic, which is how the pacer does it.
    fn send_stamped(
        det: &mut CongestionDetector,
        base: Instant,
        first: usize,
        count: usize,
        cadence_us: i64,
        cluster: ProbeClusterId,
    ) {
        for ordinal in first..first + count {
            det.packet_tracker_mut().record_packet_send(
                base + Duration::from_micros((ordinal as i64 * cadence_us) as u64),
                1_200,
                false,
                cluster,
                cluster != 0,
                (ordinal + 1) as u16,
            );
        }
    }

    /// A report whose every symbol acknowledges a consecutive packet. The
    /// reference time is constant, so the reconstructed receive clock of each
    /// report restarts at `1_000 * 64_000` microseconds.
    fn received(base_sequence: u16, deltas_us: &[i64]) -> TwccFeedbackReport {
        report(
            base_sequence,
            &vec![SYMBOL_LARGE_DELTA; deltas_us.len()],
            deltas_us,
        )
    }

    fn report(base_sequence: u16, symbols: &[u8], deltas_us: &[i64]) -> TwccFeedbackReport {
        TwccFeedbackReport {
            base_sequence_number: base_sequence,
            packet_status_count: symbols.len() as u16,
            reference_time: 1_000,
            fb_pkt_count: 0,
            symbols: symbols.to_vec(),
            deltas_us: deltas_us.to_vec(),
        }
    }

    /// Both helpers take the campaign's declared unit: `duration_us` is the
    /// `pstart dur_us=` digit and `start_us`/`end_us` are the `pdone
    /// start_us`/`end_us` digits, so a test line reads as the same number the
    /// pinned Go sources received. The result stamps are nanoseconds inside Go
    /// and this is the only place that says so.
    fn goal(desired_bps: i64, desired_bytes: i64, duration_us: i64) -> ProbeClusterGoal {
        ProbeClusterGoal {
            desired_bps,
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

    fn pci(
        id: ProbeClusterId,
        goal: ProbeClusterGoal,
        result: ProbeClusterResult,
        created_at: Option<Instant>,
    ) -> ProbeClusterInfo {
        ProbeClusterInfo {
            id,
            created_at,
            goal,
            result,
        }
    }

    /// A trend that can go downward inside a single feedback report: upstream
    /// stamps every sample of one report with the same clock, so the
    /// `downward_trend_max_wait` escape hatch cannot reach `2 s` unless the
    /// required sample count is lowered.
    fn ctr_trend(required_samples: i64) -> TrendDetectorConfig {
        TrendDetectorConfig {
            required_samples,
            required_samples_min: required_samples,
            ..CongestionDetectorConfig::default().congested_ctr_trend
        }
    }

    #[test]
    fn the_fresh_detector_is_the_documented_upstream_initial_state() {
        let base = Instant::now();
        let det = detector(base);
        assert_eq!(det.congestion_state(), CongestionState::None);
        assert_eq!(det.congestion_reason(), CongestionReason::None);
        assert_eq!(det.estimated_available_channel_capacity(), 100_000_000);
        assert_eq!(det.rtt(), DEFAULT_RTT);
        assert_eq!(det.packet_group_count(), 0);
        assert_eq!(det.congestion_state_switched_at(), base);
        assert_eq!(det.dropped(), (0, 0));
        assert!(!det.probe_is_running());
        assert!(!det.ctr_trend_is_active());
        assert!(!det.estimate_is_aggregated());
        assert_eq!(det.probe_duration(), PolicyDuration::from_millis(200));
        assert!(
            !det.can_probe(base),
            "the regulator schedules the first probe from the creation clock"
        );
        assert!(det.can_probe(at_ms(base, 1)));
    }

    #[test]
    fn the_first_packet_of_a_stream_is_never_tracked_because_its_send_time_is_zero() {
        let base = Instant::now();
        let mut det = detector(base);
        send(&mut det, base, 0, 2);

        // Sequence 1 carries the reference send time, so upstream's
        // `sendTime == 0` sentinel hides it from every group.
        let r = received(1, &[0, 100_000]);
        assert_eq!(det.handle_twcc_feedback(&r, at_ms(base, 200)), None);
        assert_eq!(det.packet_group_count(), 1);
        let group = det.packet_group(0);
        assert_eq!(group.acked().packets(), 1);
        assert_eq!(group.sequence_window(), (2, 2));
        // oracle firstpkt: the only acknowledged packet sets the group send window
        // to (sendTime - sendDelta, sendTime) = (0, 100_000), i.e. the group's
        // minimum send time lands exactly on the clock zero of the stream.
        assert_eq!(group.min_send_time(), 0);
        assert_eq!(group.send_window(), (0, 100_000));
        assert_eq!(group.traffic().acknowledged_bitrate(), 96_000);
    }

    #[test]
    fn symbol_classes_decide_what_is_recorded_and_what_is_only_skipped() {
        let base = Instant::now();
        let mut det = detector(base);
        send(&mut det, base, 0, 5);

        // seq2 small delta, seq3 finalises the first group, seq4 has no delta at
        // all and is skipped, seq5 is reported lost and opens the next group.
        let r = report(
            2,
            &[
                SYMBOL_SMALL_DELTA,
                SYMBOL_SMALL_DELTA,
                SYMBOL_WITHOUT_DELTA,
                SYMBOL_NOT_RECEIVED,
            ],
            &[0, 100_000],
        );
        assert_eq!(det.handle_twcc_feedback(&r, at_ms(base, 500)), None);
        assert_eq!(det.packet_group_count(), 2);
        assert_eq!(det.packet_group(0).acked().packets(), 2);
        assert!(det.packet_group(0).is_finalized());
        assert_eq!(det.packet_group(1).acked().packets(), 0);
        assert_eq!(det.packet_group(1).lost().packets(), 1);
        assert_eq!(det.packet_group(1).sequence_window(), (5, 5));

        // seq3 arriving late is acknowledged by the older group, which is already
        // finalised but still accepts a sequence inside its window.
        let late = report(3, &[SYMBOL_LARGE_DELTA], &[150_000]);
        assert_eq!(det.handle_twcc_feedback(&late, at_ms(base, 600)), None);
        assert_eq!(det.packet_group(0).acked().packets(), 3);
        assert_eq!(det.packet_group(1).acked().packets(), 0);
        assert_eq!(det.dropped(), (0, 0), "the older group accepted the packet");

        // The status count truncates the symbol stream, so seq4 is not walked.
        let mut truncated = report(
            4,
            &[SYMBOL_LARGE_DELTA, SYMBOL_LARGE_DELTA],
            &[100_000, 100_000],
        );
        truncated.packet_status_count = 1;
        assert_eq!(det.handle_twcc_feedback(&truncated, at_ms(base, 700)), None);
        assert_eq!(det.packet_group(1).acked().packets(), 0);
        assert_eq!(det.packet_group(1).lost().packets(), 1);
    }

    #[test]
    fn a_delta_stream_that_runs_out_ends_the_walk_without_recording() {
        let base = Instant::now();
        let mut det = detector(base);
        send(&mut det, base, 0, 4);

        // Three large-delta symbols but only two deltas: the third packet cannot
        // be placed in time, so upstream leaves it unrecorded.
        let r = report(
            2,
            &[SYMBOL_LARGE_DELTA, SYMBOL_LARGE_DELTA, SYMBOL_LARGE_DELTA],
            &[0, 100_000],
        );
        assert_eq!(det.handle_twcc_feedback(&r, at_ms(base, 400)), None);
        assert_eq!(det.packet_group_count(), 1);
        assert_eq!(det.packet_group(0).acked().packets(), 2);
        assert_eq!(det.packet_group(0).sequence_window(), (2, 3));
    }

    #[test]
    fn a_wrapped_wire_sequence_extends_the_reconstructed_sequence_of_the_group() {
        let base = Instant::now();
        let mut det = detector(base);
        // Eight sends on wire sequences 65_530..65_537, i.e. across the wrap.
        for index in 0..8u16 {
            det.packet_tracker_mut().record_packet_send(
                base + Duration::from_micros(u64::from(index) * 100_000),
                1_200,
                false,
                0,
                false,
                65_530u16.wrapping_add(index),
            );
        }

        let r = received(
            65_531,
            &[0, 100_000, 100_000, 100_000, 100_000, 100_000, 100_000],
        );
        assert_eq!(det.handle_twcc_feedback(&r, at_ms(base, 800)), None);
        // oracle wrap16: two packets per group closes three groups and leaves the
        // eighth packet alone in the fourth.
        assert_eq!(det.packet_group_count(), 4);
        assert_eq!(
            (0..det.packet_group_count())
                .filter(|&i| det.packet_group(i).is_finalized())
                .count(),
            3
        );
        let group = det.packet_group(3);
        assert_eq!(group.acked().packets(), 1);
        // The wire wrap is invisible inside the reconstructed 64-bit space: the
        // last wire sequence 65_537 after 65_535 is 65_537 - 65_536 + 65_536 = the
        // second sequence past the wrap boundary.
        assert_eq!(group.sequence_window(), (65_537, 65_537));
        assert_eq!(group.send_window(), (600_000, 700_000));
        assert_eq!(group.traffic().acknowledged_bitrate(), 96_000);
        assert_eq!(group.propagated_queuing_delay(), 0);
        assert_eq!(
            (0..det.packet_group_count())
                .map(|i| det.packet_group(i).send_window())
                .fold((i64::MAX, i64::MIN), |(lo, hi), (a, b)| (
                    lo.min(a),
                    hi.max(b)
                )),
            (100_000, 700_000)
        );
    }

    #[test]
    fn a_repeated_report_counts_the_same_packets_again_like_upstream() {
        let base = Instant::now();
        let mut det = detector(base);
        send(&mut det, base, 0, 3);

        let r = received(2, &[0, 100_000]);
        assert_eq!(det.handle_twcc_feedback(&r, at_ms(base, 300)), None);
        assert_eq!(det.packet_group(0).acked().packets(), 2);
        assert_eq!(
            det.packet_group(0).traffic().acknowledged_bitrate(),
            192_000
        );

        // Upstream has no duplicate suppression: the second copy of the same
        // report inflates the acknowledged counter and therefore the bitrate.
        assert_eq!(det.handle_twcc_feedback(&r, at_ms(base, 400)), None);
        let group = det.packet_group(0);
        assert_eq!(group.acked().packets(), 4);
        assert_eq!(group.acked().bytes(), 4_800);
        assert_eq!(group.traffic().acknowledged_bitrate(), 384_000);
        assert!(!group.is_finalized() || group.acked().packets() == 4);
    }

    #[test]
    fn a_report_for_a_gap_skips_every_finalized_older_group_and_counts_the_refusal() {
        let base = Instant::now();
        let mut det = detector(base);
        for (index, wire) in [9u16, 10, 11, 15, 20, 21].into_iter().enumerate() {
            det.packet_tracker_mut().record_packet_send(
                base + Duration::from_micros(index as u64 * 100_000),
                1_200,
                false,
                0,
                false,
                wire,
            );
        }

        // Two groups that are not adjacent in sequence space.
        assert_eq!(
            det.handle_twcc_feedback(&received(10, &[100_000, 100_000]), at_ms(base, 300)),
            None
        );
        assert_eq!(det.packet_group_count(), 1);
        assert_eq!(det.packet_group(0).sequence_window(), (10, 11));
        assert_eq!(
            det.handle_twcc_feedback(&received(20, &[300_000, 100_000]), at_ms(base, 600)),
            None
        );
        assert_eq!(det.packet_group_count(), 2);
        assert_eq!(det.packet_group(1).sequence_window(), (20, 21));

        // Sequence 15 is older than the newest group, so the fallback scan reaches
        // the older group, which is finalised and refuses it. Upstream only logs
        // that; the counter is the observable.
        assert_eq!(
            det.handle_twcc_feedback(&received(15, &[350_000]), at_ms(base, 700)),
            None
        );
        assert_eq!(det.dropped(), (0, 1));
        assert_eq!(det.packet_group(0).acked().packets(), 2);
        assert_eq!(det.packet_group(1).acked().packets(), 2);
        assert_eq!(
            det.packet_group_count(),
            2,
            "nothing is appended for a refused packet"
        );
    }

    #[test]
    fn pruning_drains_up_to_the_first_group_that_is_still_fresh() {
        let mut config = test_config();
        config.packet_group_max_age = PolicyDuration::from_millis(300);
        let base = Instant::now();
        let mut det = CongestionDetector::new(config, base);
        send(&mut det, base, 0, 8);

        // seq2..6 make three groups (two packets, two packets, one packet). The
        // age threshold at 600 ms is now - baseSendTime - maxAge = 300_000us, so
        // the two groups whose minimum send time does not exceed it are drained
        // and only the group holding seq6 is left.
        assert_eq!(
            det.handle_twcc_feedback(
                &received(2, &[0, 100_000, 100_000, 100_000, 100_000]),
                at_ms(base, 600)
            ),
            None
        );
        assert_eq!(det.packet_group_count(), 1);
        // oracle prune: the surviving group is the one that was opened by seq6,
        // whose send window minimum is `sendTime - sendDelta` = 500_000 - 100_000.
        let group = det.packet_group(0);
        assert_eq!(group.sequence_window(), (6, 6));
        assert_eq!(group.send_window(), (400_000, 500_000));
        assert_eq!(group.acked().packets(), 1);
        assert_eq!(group.traffic().acknowledged_bitrate(), 96_000);

        // seq7 closes that group, seq8 opens the next one; the threshold is now
        // 500_000us, so the just-finalised group (minimum 400_000us) is drained too.
        assert_eq!(
            det.handle_twcc_feedback(&received(7, &[100_000, 100_000]), at_ms(base, 800)),
            None
        );
        assert_eq!(det.packet_group_count(), 1);
        assert_eq!(det.packet_group(0).sequence_window(), (8, 8));
        assert_eq!(det.packet_group(0).send_window(), (600_000, 700_000));

        // A report that carries no status blocks is rejected before the walk, so
        // upstream never reaches the drain: even a five-second-old list survives.
        assert_eq!(
            det.handle_twcc_feedback(&report(9, &[], &[]), at_ms(base, 5_000)),
            None
        );
        assert_eq!(
            det.packet_group_count(),
            1,
            "an empty report never reaches the drain"
        );
        assert_eq!(det.packet_group(0).sequence_window(), (8, 8));
    }

    #[test]
    fn one_report_moves_the_state_machine_by_a_single_transition() {
        let base = Instant::now();
        let mut det = detector(base);
        send(&mut det, base, 0, 13);

        // Seven tracked packets (seq 2..8) in four groups whose propagated queuing
        // delay keeps growing: this already satisfies the congested queuing delay
        // gate, but from `None` only the early warning signal is evaluated.
        let escalate = received(
            2,
            &[0, 155_000, 105_000, 105_000, 105_000, 105_000, 105_000],
        );
        assert_eq!(
            det.handle_twcc_feedback(&escalate, at_ms(base, 900)),
            Some(StateChange {
                from: CongestionState::None,
                to: CongestionState::EarlyWarning,
                // First transition: `congestionReason` is only assigned inside
                // updateCongestionState(), which runs *after* the estimation, so
                // this merges the time-windowed path — newest finalized group down
                // until the send window passes the 1 s estimation window, i. e.
                // 12_000 bytes over 1.05 s at a captured ratio of 1 = 91_428.
                capacity_bps: 91_428,
            })
        );
        assert_eq!(det.congestion_state(), CongestionState::EarlyWarning);
        assert_eq!(det.congestion_reason(), CongestionReason::QueuingDelay);
        assert_eq!(det.packet_group_count(), 4);
        assert!(
            !det.ctr_trend_is_active(),
            "only a congested state tracks the CTR"
        );

        // The next two groups push the oldest four past the congested gate, so
        // this report is the one that escalates.
        let congested = received(10, &[890_000, 105_000, 105_000, 145_000]);
        assert_eq!(
            det.handle_twcc_feedback(&congested, at_ms(base, 1_300)),
            Some(StateChange {
                from: CongestionState::EarlyWarning,
                to: CongestionState::Congested,
                // oracle esm13 row 2 (state=2, capacity=81269, num_groups=6,
                // qd_groups=4, qd_min_idx=1, qd_max_idx=4): the congested group
                // range 1..4 merges 8 packets / 9_600 bytes over a 900 ms send
                // window, then the CTR penalty scales it by 400/420, i. e.
                // 9_600 * 8 * 1e6 / 900_000 * 0.952380952 = 81_269.
                capacity_bps: 81_269,
            })
        );
        assert!(det.ctr_trend_is_active());
        assert_eq!(det.packet_group_count(), 6);
    }

    #[test]
    fn a_downward_captured_traffic_ratio_lowers_the_estimate_without_a_state_change() {
        let mut config = test_config();
        config.congested_ctr_trend = ctr_trend(2);
        config.congested_packet_group = PacketGroupConfig {
            min_packets: 2,
            max_window_duration: PolicyDuration::from_millis(150),
        };
        let base = Instant::now();
        let mut det = CongestionDetector::new(config, base);
        send(&mut det, base, 0, 15);

        assert_eq!(
            det.handle_twcc_feedback(
                &received(2, &[0, 155_000, 105_000, 105_000]),
                at_ms(base, 500)
            ),
            Some(StateChange {
                from: CongestionState::None,
                to: CongestionState::EarlyWarning,
                capacity_bps: 105_205,
            })
        );
        let congested_at = at_ms(base, 900);
        assert_eq!(
            det.handle_twcc_feedback(
                &received(6, &[465_000, 110_000, 105_000, 105_000]),
                congested_at
            ),
            Some(StateChange {
                from: CongestionState::EarlyWarning,
                to: CongestionState::Congested,
                capacity_bps: 97_834,
            })
        );
        assert_eq!(det.congestion_state_switched_at(), congested_at);

        // Two congested windows of two packets each: the ratio of the first is
        // 200_000/400_000 = 0.5 and the second 200_000/500_000 = 0.4, both exactly
        // on the 0.05 quantisation grid, so the trend turns downward.
        let lower = received(10, &[885_000, 300_000, 100_000, 250_000, 250_000, 100_000]);
        let at3 = at_ms(base, 1_400);
        assert_eq!(
            det.handle_twcc_feedback(&lower, at3),
            Some(StateChange {
                from: CongestionState::Congested,
                to: CongestionState::Congested,
                // the two congested windows merged: 4_800 bytes over the 500 ms
                // they span, ratio 400_000/900_000.
                capacity_bps: 34_133,
            })
        );
        assert_eq!(det.congestion_state(), CongestionState::Congested);
        assert_eq!(
            det.congestion_state_switched_at(),
            congested_at,
            "a capacity-only notification is not a state switch"
        );
        assert!(
            det.ctr_trend_is_active(),
            "the trend is reset for a fresh set of samples, not cleared"
        );
    }

    #[test]
    fn a_contiguous_drain_releases_early_warning_over_the_estimation_window() {
        let mut config = test_config();
        config.estimation_window_duration = PolicyDuration::from_millis(500);
        let base = Instant::now();
        let mut det = CongestionDetector::new(config, base);
        send(&mut det, base, 0, 11);

        assert_eq!(
            det.handle_twcc_feedback(
                &received(2, &[0, 155_000, 105_000, 105_000]),
                at_ms(base, 500)
            ),
            Some(StateChange {
                from: CongestionState::None,
                to: CongestionState::EarlyWarning,
                capacity_bps: 105_205,
            })
        );

        // Six packets that arrive 25 ms sooner than they were sent apart: each
        // group sheds 50 ms, so the propagated delay falls 15 ms, 0, 0 and the
        // queuing region becomes contiguous DQR.
        let relieve = received(6, &[440_000, 75_000, 75_000, 75_000, 75_000, 75_000]);
        assert_eq!(
            det.handle_twcc_feedback(&relieve, at_ms(base, 1_100)),
            Some(StateChange {
                from: CongestionState::EarlyWarning,
                to: CongestionState::None,
                // the window path stops at the first aggregate wider than 500 ms,
                // which is the three relieving groups: 7_200 bytes over 600 ms at
                // a captured ratio capped at one.
                capacity_bps: 96_000,
            })
        );
        assert_eq!(det.congestion_reason(), CongestionReason::None);
        assert!(det.estimate_is_aggregated());
        assert_eq!(det.packet_group_count(), 5);
        assert_eq!(det.dropped(), (0, 0));
    }

    #[test]
    fn an_estimation_window_nobody_reaches_leaves_the_capacity_alone() {
        let mut config = test_config();
        config.estimation_window_duration = PolicyDuration::from_secs(2);
        let base = Instant::now();
        let mut det = CongestionDetector::new(config, base);
        send(&mut det, base, 0, 11);

        det.handle_twcc_feedback(
            &received(2, &[0, 155_000, 105_000, 105_000]),
            at_ms(base, 500),
        )
        .expect("early warning");
        // The same relieving stimulus, but the whole group list only spans
        // 900 ms, which never exceeds the 2 s window.
        let relieve = received(6, &[440_000, 75_000, 75_000, 75_000, 75_000, 75_000]);
        assert_eq!(
            det.handle_twcc_feedback(&relieve, at_ms(base, 1_100)),
            Some(StateChange {
                from: CongestionState::EarlyWarning,
                to: CongestionState::None,
                capacity_bps: 105_205,
            })
        );
        assert!(
            !det.estimate_is_aggregated(),
            "an aggregate below the window is not an estimate"
        );
    }

    #[test]
    fn a_report_that_names_no_region_keeps_the_reason_of_the_report_that_did() {
        let base = Instant::now();
        let mut det = detector(base);
        send(&mut det, base, 0, 13);

        det.handle_twcc_feedback(
            &received(2, &[0, 155_000, 105_000, 105_000]),
            at_ms(base, 500),
        )
        .expect("early warning");
        assert_eq!(det.congestion_reason(), CongestionReason::QueuingDelay);

        // Two groups whose propagated delay sits in the dead band between the DQR
        // and JQR thresholds: the loss gate settles on DQR, but the queuing delay
        // gate cannot, so Go's switch matches no case.
        assert_eq!(
            det.handle_twcc_feedback(
                &received(6, &[450_000, 85_000, 100_000, 100_000]),
                at_ms(base, 900)
            ),
            None
        );
        assert_eq!(det.congestion_state(), CongestionState::EarlyWarning);
        assert_eq!(
            det.congestion_reason(),
            CongestionReason::QueuingDelay,
            "upstream leaves congestionReason untouched when no case matches"
        );

        assert_eq!(
            det.handle_twcc_feedback(
                &received(10, &[535_000, 100_000, 100_000, 100_000]),
                at_ms(base, 1_300)
            ),
            None
        );
        assert_eq!(det.congestion_reason(), CongestionReason::QueuingDelay);
        assert_eq!(det.estimated_available_channel_capacity(), 105_205);
    }

    #[test]
    fn a_loss_region_escalates_to_early_warning_and_penalises_the_estimate() {
        let mut config = test_config();
        config.packet_group = PacketGroupConfig {
            min_packets: 2,
            max_window_duration: PolicyDuration::from_secs(5),
        };
        let base = Instant::now();
        let mut det = CongestionDetector::new(config, base);
        // A 35 ms cadence keeps the whole report inside 420 ms of send window, so
        // the weighted loss stays valid but the queuing delay stays in the dead
        // band. Every other packet is reported lost.
        for index in 0..13usize {
            det.packet_tracker_mut().record_packet_send(
                base + Duration::from_micros((index as i64 * 35_000) as u64),
                1_200,
                false,
                0,
                false,
                (index + 1) as u16,
            );
        }

        // oracle loss_region twcc row
        // states=0,2:0,0,2:100000,0,2:72500,0,2:72500,0,2:72500,0,2:72500 —
        // twelve statuses of which six carry a delta, and `deltas_us` is
        // consumed once per delta-carrying status.
        let symbols = [SYMBOL_NOT_RECEIVED, SYMBOL_LARGE_DELTA].repeat(6);
        let r = report(2, &symbols, &[0, 100_000, 72_500, 72_500, 72_500, 72_500]);
        let change = det
            .handle_twcc_feedback(&r, at_ms(base, 600))
            .expect("the loss region escalates");
        assert_eq!(change.from, CongestionState::None);
        assert_eq!(change.to, CongestionState::EarlyWarning);
        assert_eq!(det.congestion_reason(), CongestionReason::Loss);
        assert_eq!(det.packet_group_count(), 3);

        // oracle loss_region twcc row: capacity=123914, loss_groups=3,
        // loss_wl=0.7675566008486745. The 35 ms cadence keeps the report inside
        // the 420 ms window, so the weighted loss stays valid and its simulated
        // queuing contribution penalises the captured ratio; without that
        // penalty the same traffic is worth 164_571 bps.
        assert_eq!(change.capacity_bps, 123_914);
        assert!(
            change.capacity_bps < 164_571,
            "the simulated queuing contribution of the lost packets must be counted"
        );

        // oracle loss_region group columns for the newest group: sequences
        // 10..13 over a 280..420 ms send window, two acknowledged and two lost
        // 1_200 byte packets, a 40 ms propagated delay, and the merged traffic
        // worth 132_413 bps at a captured ratio of 140/145.
        let last = det.packet_group(2);
        assert_eq!(last.sequence_window(), (10, 13));
        assert_eq!(last.send_window(), (280_000, 420_000));
        assert_eq!(last.aggregate_deltas(), (140_000, 145_000));
        assert_eq!(last.propagated_queuing_delay(), 40_000);
        assert!(last.is_finalized());
        assert_eq!(last.acked().primary().packets(), 2);
        assert_eq!(last.lost().packets(), 2);
        assert_eq!(last.traffic().acknowledged_bitrate(), 132_413);
    }

    #[test]
    fn a_not_congesting_probe_below_the_estimate_keeps_the_estimate_and_grows_the_next_probe() {
        let base = Instant::now();
        let mut det = detector(base);
        send(&mut det, base, 0, 1);
        det.probe_cluster_starting(pci(
            7,
            goal(100_000, 20_000, 1_000_000),
            ProbeClusterResult::default(),
            Some(base),
        ));
        assert!(det.probe_is_running());
        assert_eq!(det.probe_packet_group_id(), Some(7));
        assert!(
            !det.can_probe(at_ms(base, 400)),
            "a running probe cannot be overlapped"
        );

        // Probe traffic on the exact send cadence: no queue builds up.
        send_stamped(&mut det, base, 1, 4, 100_000, 7);
        assert_eq!(
            det.handle_twcc_feedback(
                &received(2, &[0, 100_000, 100_000, 100_000]),
                at_ms(base, 500)
            ),
            None
        );

        // A result that has not been reported yet makes the probe invalid.
        assert!(!det.probe_cluster_is_goal_reached());
        // A cluster id that does not match the running probe closes nothing.
        let wrong = pci(
            8,
            goal(100_000, 20_000, 1_000_000),
            result(12_000, 0, 600_000),
            Some(base),
        );
        det.probe_cluster_done(&wrong, at_ms(base, 600));
        assert!(det.probe_cluster_finalize(at_ms(base, 600)).is_none());

        let done = pci(
            7,
            goal(100_000, 20_000, 1_000_000),
            result(12_000, 0, 600_000),
            Some(base),
        );
        det.probe_cluster_done(&done, at_ms(base, 700));
        // 4_800 bytes over the 300 ms probe send window is only 128 kbps, and the
        // probe covers the tracker's last cluster packet, so there is no settle wait.
        assert!(
            !det.probe_cluster_is_goal_reached(),
            "NotCongesting never reaches a goal"
        );
        // oracle probe_nc pfin row: fin_signal=2 (NotCongesting), fin_ok=1 and
        // fin_capacity=100000000 — upstream reports and keeps the *detector*
        // estimate, not the probe bitrate. The probe bitrate (pg_abr=128000) is
        // below the 100 Mbps estimate, so the raise arm does not fire.
        assert_eq!(
            det.probe_cluster_finalize(at_ms(base, 700)),
            Some((ProbeSignal::NotCongesting, 100_000_000))
        );
        assert_eq!(det.estimated_available_channel_capacity(), 100_000_000);
        assert!(!det.probe_is_running());
        assert_eq!(det.probe_duration(), PolicyDuration::from_millis(300));
        assert!(!det.can_probe(at_ms(base, 2_999)));
        assert!(det.can_probe(at_ms(base, 3_001)));
    }

    /// The other arm of the same upstream comparison: the raise only happens when
    /// the probe is worth more than the current estimate, which needs a probe
    /// cadence far above the 100 Mbps default.
    #[test]
    fn a_not_congesting_probe_above_the_estimate_raises_the_capacity_to_the_probe_bitrate() {
        let base = Instant::now();
        let mut det = detector(base);
        send(&mut det, base, 0, 1);
        det.probe_cluster_starting(pci(
            7,
            goal(100_000, 20_000, 1_000_000),
            ProbeClusterResult::default(),
            Some(base),
        ));

        // 100 us apart instead of 100 ms: the same 4_800 bytes now span a 300 us
        // send window, so the probe group is worth 128 Mbps.
        send_stamped(&mut det, base, 1, 4, 100, 7);
        assert_eq!(
            det.handle_twcc_feedback(&received(2, &[0, 100, 100, 100]), at_ms(base, 500)),
            None,
            "the probe never changes the congestion state by itself"
        );

        let foreign = pci(
            8,
            goal(100_000, 20_000, 1_000_000),
            result(12_000, 0, 600_000),
            Some(base),
        );
        det.probe_cluster_done(&foreign, at_ms(base, 600));
        assert!(det.probe_cluster_finalize(at_ms(base, 600)).is_none());

        let done = pci(
            7,
            goal(100_000, 20_000, 1_000_000),
            result(12_000, 0, 600_000),
            Some(base),
        );
        det.probe_cluster_done(&done, at_ms(base, 700));
        // oracle probe_raise pfin row: fin_signal=2 (NotCongesting), fin_ok=1 and
        // fin_capacity=128000000, because pg_abr=128000000 exceeds the 100 Mbps
        // estimate and the raise arm in ProbeClusterFinalize fires.
        assert_eq!(
            det.probe_cluster_finalize(at_ms(base, 700)),
            Some((ProbeSignal::NotCongesting, 128_000_000))
        );
        assert_eq!(det.estimated_available_channel_capacity(), 128_000_000);
        assert!(
            !det.probe_cluster_is_goal_reached(),
            "the probe group is gone"
        );
        assert!(!det.probe_is_running());
        assert_eq!(det.probe_duration(), PolicyDuration::from_millis(300));
        assert!(!det.can_probe(at_ms(base, 2_999)));
        assert!(det.can_probe(at_ms(base, 3_001)));
    }

    #[test]
    fn an_inconclusive_probe_that_beats_the_goal_leaves_the_capacity_alone() {
        let base = Instant::now();
        let mut det = detector(base);
        send(&mut det, base, 0, 1);
        det.probe_cluster_starting(pci(
            7,
            goal(100_000, 20_000, 1_000_000),
            ProbeClusterResult::default(),
            Some(base),
        ));
        // 110 ms apart for 100 ms of send spacing: 30 ms of propagated delay, so
        // the probe is neither congesting nor relieving.
        send_stamped(&mut det, base, 1, 4, 100_000, 7);
        assert_eq!(
            det.handle_twcc_feedback(
                &received(2, &[0, 110_000, 110_000, 110_000]),
                at_ms(base, 500)
            ),
            None
        );
        det.probe_cluster_done(
            &pci(
                7,
                goal(100_000, 20_000, 1_000_000),
                result(12_000, 0, 600_000),
                Some(base),
            ),
            at_ms(base, 600),
        );

        assert!(det.probe_cluster_is_goal_reached());
        // oracle probe_inc pfin row: fin_signal=0 (Inconclusive), fin_ok=1,
        // fin_capacity=100000000. The probe group itself is worth
        // pg_abr=116363 (4_800 bytes over 300 ms scaled by the 300/330 captured
        // ratio), which is above the 100 kbps goal — that is what makes the goal
        // reached — but an inconclusive signal never raises the estimate.
        assert_eq!(
            det.probe_cluster_finalize(at_ms(base, 600)),
            Some((ProbeSignal::Inconclusive, 100_000_000)),
        );
        assert_eq!(det.estimated_available_channel_capacity(), 100_000_000);
        assert_eq!(det.probe_duration(), PolicyDuration::from_millis(300));
    }

    #[test]
    fn an_insufficient_probe_result_is_invalid_and_a_state_move_outranks_the_probe() {
        let base = Instant::now();
        let mut det = detector(base);
        send(&mut det, base, 0, 9);
        det.probe_cluster_starting(pci(
            8,
            goal(100_000, 20_000, 1_000_000),
            ProbeClusterResult::default(),
            Some(base),
        ));

        // Media traffic, not probe traffic: the probe group stays empty and the
        // tracker's probe window stays at zero.
        assert_eq!(
            det.handle_twcc_feedback(
                &received(
                    2,
                    &[0, 155_000, 105_000, 105_000, 105_000, 105_000, 105_000]
                ),
                at_ms(base, 900)
            ),
            Some(StateChange {
                from: CongestionState::None,
                to: CongestionState::EarlyWarning,
                // oracle probe_state twcc row: state=1 but capacity stays at the
                // 100 Mbps default and est_ts_abr=0, while the *same* report in the
                // probe-free esm13 case yields 91_428. A live probe group
                // short-circuits the estimation before any group is merged.
                capacity_bps: 100_000_000,
            })
        );
        assert_eq!(det.probe_packet_group_id(), Some(8));

        let short = pci(
            8,
            goal(100_000, 20_000, 1_000_000),
            result(10_000, 0, 600_000),
            Some(base),
        );
        det.probe_cluster_done(&short, at_ms(base, 1_000));
        assert!(
            !det.probe_cluster_is_goal_reached(),
            "10_000 bytes is not above half of the desired 20_000"
        );

        // The tracker never saw a cluster packet, so the settle wait of
        // 5 * 70 ms = 350 ms has to elapse first.
        assert!(det.probe_cluster_finalize(at_ms(base, 1_349)).is_none());
        let done = pci(
            8,
            goal(100_000, 20_000, 1_000_000),
            result(12_000, 0, 600_000),
            Some(base),
        );
        det.probe_cluster_done(&done, at_ms(base, 1_000));
        assert_eq!(
            det.probe_cluster_finalize(at_ms(base, 1_350)),
            // oracle probe_state pfin row: fin_signal=1 (Congesting),
            // fin_ok=1, fin_capacity=100000000. The empty probe group would be
            // NotCongesting, but the congestion signal that moved while it ran
            // outranks the probe, and the value reported upstream is the
            // detector estimate — which the live probe kept at its default.
            Some((ProbeSignal::Congesting, 100_000_000))
        );
        assert_eq!(det.estimated_available_channel_capacity(), 100_000_000);
        assert_eq!(
            det.probe_duration(),
            PolicyDuration::from_millis(200),
            "a congesting probe restarts at the minimum duration"
        );
    }

    #[test]
    fn the_settle_wait_follows_the_smoothed_rtt() {
        let base = Instant::now();
        let mut det = detector(base);
        send(&mut det, base, 0, 1);
        det.probe_cluster_starting(pci(
            7,
            goal(100_000, 20_000, 1_000_000),
            ProbeClusterResult::default(),
            Some(base),
        ));
        det.probe_cluster_done(
            &pci(
                7,
                goal(100_000, 20_000, 1_000_000),
                result(12_000, 0, 600_000),
                Some(base),
            ),
            at_ms(base, 1_000),
        );
        assert!(det.probe_cluster_finalize(at_ms(base, 1_349)).is_none());
        assert!(det.probe_cluster_finalize(at_ms(base, 1_350)).is_some());

        // 0.5 * 0.1 + 0.5 * 0.07 = 0.085 s, so five RTT is 425 ms.
        det.update_rtt(0.1);
        assert_eq!(det.rtt(), 0.085);
        det.probe_cluster_starting(pci(
            9,
            goal(100_000, 20_000, 1_000_000),
            ProbeClusterResult::default(),
            Some(base),
        ));
        det.probe_cluster_done(
            &pci(
                9,
                goal(100_000, 20_000, 1_000_000),
                result(12_000, 0, 600_000),
                Some(base),
            ),
            at_ms(base, 2_000),
        );
        assert!(det.probe_cluster_finalize(at_ms(base, 2_400)).is_none());
        assert!(det.probe_cluster_finalize(at_ms(base, 2_430)).is_some());
    }

    #[test]
    fn the_rtt_estimate_smooths_and_a_zero_measurement_restores_the_default() {
        let base = Instant::now();
        let mut det = detector(base);
        assert_eq!(det.rtt(), DEFAULT_RTT);
        det.update_rtt(0.1);
        assert_eq!(det.rtt(), 0.085);
        det.update_rtt(0.2);
        // oracle lrtt row: 0.14250000000000002 — `0.5 * 0.2 + 0.5 * 0.085` is one
        // ulp above the literal 0.1425, so the exact double has to be compared.
        assert_eq!(det.rtt(), 0.142_500_000_000_000_002);
        det.update_rtt(0.0);
        assert_eq!(det.rtt(), DEFAULT_RTT, "no measurement is not a zero delay");
    }

    #[test]
    fn reset_restores_the_policy_state_and_keeps_the_send_clock_and_report_history() {
        let base = Instant::now();
        let mut det = detector(base);
        send(&mut det, base, 0, 3);
        det.handle_twcc_feedback(&received(2, &[0, 155_000, 105_000]), at_ms(base, 300));
        // oracle zrst twcc row: one group, already finalized, carrying the
        // 55 ms propagated delay and a delta-adjusted minimum send time of
        // 100 ms (the packet sent at 100 ms with a 0 us report delta).
        assert_eq!(det.packet_group_count(), 1);
        assert!(det.packet_group(0).is_finalized());
        assert_eq!(det.packet_group(0).propagated_queuing_delay(), 55_000);
        assert_eq!(det.packet_group(0).min_send_time(), 100_000);
        det.update_rtt(0.5);
        det.probe_cluster_starting(pci(
            7,
            goal(100_000, 20_000, 1_000_000),
            ProbeClusterResult::default(),
            Some(base),
        ));

        let reset_at = at_ms(base, 1_000);
        det.reset(reset_at);
        assert_eq!(det.congestion_state(), CongestionState::None);
        assert_eq!(det.congestion_reason(), CongestionReason::None);
        assert_eq!(det.estimated_available_channel_capacity(), 100_000_000);
        assert_eq!(det.rtt(), DEFAULT_RTT);
        assert_eq!(det.packet_group_count(), 0);
        assert!(!det.probe_is_running());
        assert!(!det.ctr_trend_is_active());
        assert!(!det.estimate_is_aggregated());
        assert_eq!(det.congestion_state_switched_at(), reset_at);
        assert_eq!(det.probe_duration(), PolicyDuration::from_millis(200));
        assert!(!det.can_probe(reset_at));
        assert!(det.can_probe(at_ms(reset_at, 1)));

        // The tracker is not rebuilt, so a later send is still timestamped
        // against the pre-reset reference and the feedback base is unchanged.
        send(&mut det, base, 3, 1);
        assert_eq!(
            det.handle_twcc_feedback(&received(4, &[200_000]), at_ms(base, 1_100)),
            None
        );
        assert_eq!(det.packet_group_count(), 1);
        assert_eq!(det.packet_group(0).acked().packets(), 1);
        assert_eq!(det.packet_group(0).min_send_time(), 200_000);
    }

    #[test]
    fn an_out_of_order_report_is_consumed_but_freezes_the_interval_estimate() {
        let base = Instant::now();
        let mut det = detector(base);
        send(&mut det, base, 0, 11);

        let mut first = received(2, &[0, 100_000, 100_000, 100_000]);
        first.fb_pkt_count = 10;
        assert_eq!(det.handle_twcc_feedback(&first, at_ms(base, 400)), None);
        assert_eq!(det.packet_group_count(), 2);

        // 200 - 10 is above the half of the sequence-number space, so this report
        // is judged to come from behind even though its packets are newer.
        let mut late = received(6, &[400_000, 100_000, 100_000, 100_000]);
        late.fb_pkt_count = 200;
        assert_eq!(det.handle_twcc_feedback(&late, at_ms(base, 800)), None);
        assert_eq!(det.twcc_feedback.report_counts(), (2, 1));
        assert_eq!(
            det.twcc_feedback.estimated_feedback_interval(),
            PolicyDuration::ZERO,
            "the rejected report may not seed the interval"
        );
        assert_eq!(det.packet_group_count(), 4, "its packets are still tracked");

        // The next accepted report is measured against the last accepted clock.
        let mut next = received(10, &[800_000, 100_000]);
        next.fb_pkt_count = 11;
        assert_eq!(det.handle_twcc_feedback(&next, at_ms(base, 1_300)), None);
        assert_eq!(
            det.twcc_feedback.estimated_feedback_interval(),
            PolicyDuration::from_millis(900)
        );
        assert_eq!(det.twcc_feedback.report_counts(), (3, 1));
    }
}
