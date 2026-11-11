//! Google Congestion Control (GoogCC) Bandwidth Estimation based on TWCC feedback.
//!
//! This implementation is ported from libWebRTC's GoogCC and goes beyond the simplified
//! IETF draft (<https://datatracker.ietf.org/doc/html/draft-ietf-rmcat-gcc-02>) to include
//! WebRTC's production features:
//!
//! - Delay-based control (trendline estimator with AIMD rate control)
//! - Loss-based control (with inherent loss rate estimation)
//! - Probe controller with state machine and multi-stage probing strategy
//! - ALR (Application Limited Region) detection and periodic probing
//! - Link capacity estimation from ALR probes
//!
//! The probe controller in particular closely matches WebRTC's `ProbeController` behavior
//! and default constants, enabling compatible bandwidth discovery with WebRTC endpoints.

use std::cmp::Ordering;
use std::fmt;
use std::time::{Duration, Instant};

use crate::Reason;
use crate::rtp_::{Bitrate, DataSize, TwccClusterId, TwccSendRecord, TwccSeq};
use crate::util::Soonest;

#[cfg(test)]
mod acked_bitrate_estimator;
mod alr_detector;
pub(crate) mod api;
mod delay;
mod feedback_rtt;
mod link_capacity_estimator;
mod loss_controller;
mod macros;
mod probe;
mod robust_throughput_estimator;
mod smoother;
mod time;
mod validation;

use alr_detector::AlrDetector;
use api::{BweEstimateCause, BweFeedbackObservation, BweProbeObservation, BweSnapshot};
use delay::DelayController;
use link_capacity_estimator::LinkCapacityEstimator;
use loss_controller::{LossController, LossControllerState};
use macros::log_loss;
use robust_throughput_estimator::RobustThroughputEstimator as AckedBitrateEstimator;
use smoother::EstimateSmoother;
use validation::{BweValidationStatus, Validation};

pub(crate) use macros::{log_pacer_media_debt, log_pacer_padding_debt};
#[cfg(test)]
pub(crate) use probe::ProbeKind;
pub(crate) use probe::{BandwidthLimitedCause, ProbeEstimator};
pub(crate) use probe::{ProbeClusterState, ProbeControl};

#[cfg(feature = "_internal_test_exports")]
pub use probe::ProbeClusterConfig;
#[cfg(not(feature = "_internal_test_exports"))]
pub(crate) use probe::ProbeClusterConfig;

const STARTUP_PHASE: Duration = Duration::from_secs(2);

pub struct Bwe {
    bwe: SendSideBandwidthEstimator,
    desired_bitrate: Bitrate,
    smoother: EstimateSmoother,
}

impl Bwe {
    pub fn new(initial: Bitrate) -> Self {
        let send_side_bwe = SendSideBandwidthEstimator::new(initial);
        Bwe {
            bwe: send_side_bwe,
            desired_bitrate: Bitrate::ZERO,
            smoother: EstimateSmoother::new(),
        }
    }

    pub fn handle_timeout(&mut self, now: Instant, do_probe: bool) -> Option<ProbeClusterConfig> {
        let result = self.bwe.handle_timeout(self.desired_bitrate, do_probe, now);
        if let Some(estimate) = self.bwe.last_estimate() {
            self.smoother.record(now, estimate);
        }
        result
    }

    pub fn start_probe(&mut self, config: ProbeClusterConfig, now: Instant) -> bool {
        self.bwe.start_probe(config, now)
    }

    pub fn end_probe(&mut self, now: Instant, cluster_id: TwccClusterId) {
        self.bwe.end_probe(now, cluster_id);
    }

    pub fn set_automatic_probing(&mut self, enabled: bool) -> bool {
        self.bwe.probe_control.set_automatic_probing(enabled)
    }

    pub fn set_periodic_alr_probing(&mut self, enabled: bool, now: Instant) {
        self.bwe
            .probe_control
            .set_periodic_alr_probing(enabled, now);
    }

    pub fn snapshot(&self) -> BweSnapshot {
        self.bwe.snapshot()
    }

    pub fn reset(&mut self, init_bitrate: Bitrate) {
        self.bwe.reset(init_bitrate);
    }

    pub fn update<'t>(
        &mut self,
        records: impl Iterator<Item = &'t crate::rtp_::TwccSendRecord>,
        now: Instant,
    ) {
        self.bwe.update(records, now);
    }

    pub fn poll_estimate(&mut self) -> Option<Bitrate> {
        self.smoother.poll()
    }

    pub fn poll_timeout(&self) -> (Option<Instant>, Reason) {
        self.bwe.poll_timeout()
    }

    pub fn last_estimate(&self) -> Option<Bitrate> {
        self.bwe.last_estimate()
    }

    pub fn on_media_sent(&mut self, payload_size: DataSize, _is_padding: bool, now: Instant) {
        // Google GoogCC OnSentPacket accounts every tracked RTP send, including
        // padding/probe packets. They consume link capacity and can end ALR.
        self.bwe.on_media_sent(payload_size, now);
    }

    pub fn begin_validation(
        &mut self,
        target: Bitrate,
        now: Instant,
    ) -> Option<ProbeClusterConfig> {
        if self.bwe.validation.as_ref().is_some_and(Validation::active) {
            return None;
        }
        let estimate = self.bwe.last_estimate()?;
        let cause = self.bwe.bandwidth_limited_cause();
        // Explicitly prime after reset; this does not disable/reinitialize the
        // normal controller or depend on cached propagate_estimate equality.
        self.bwe
            .probe_control
            .set_estimated_bitrate(estimate, cause);
        let config = self.bwe.probe_control.validation_config(target, now)?;
        self.bwe.validation = Some(Validation::new(*config.cluster(), target, now));
        Some(config)
    }
    pub fn begin_validation_with_pacing(
        &mut self,
        floor: Bitrate,
        pacing: Bitrate,
        now: Instant,
    ) -> Option<ProbeClusterConfig> {
        if !Validation::valid_pacing_pair(floor, pacing)
            || self.bwe.validation.as_ref().is_some_and(Validation::active)
        {
            return None;
        }
        let estimate = self.bwe.last_estimate()?;
        let cause = self.bwe.bandwidth_limited_cause();
        self.bwe
            .probe_control
            .set_estimated_bitrate(estimate, cause);
        let config = self.bwe.probe_control.validation_config(pacing, now)?;
        self.bwe.validation = Validation::with_pacing(*config.cluster(), floor, pacing, now);
        Some(config)
    }
    pub fn cancel_validation(&mut self, now: Instant) {
        if let Some(v) = &mut self.bwe.validation {
            v.terminate(BweValidationStatus::Cancelled, now);
        }
    }
    pub fn is_validation_cluster(&self, id: Option<TwccClusterId>) -> bool {
        self.bwe
            .validation
            .as_ref()
            .is_some_and(|v| id.is_some_and(|id| *id == v.snapshot().id))
    }

    pub fn on_validation_sent(&mut self, id: TwccClusterId, seq: u64, at: Instant) {
        if let Some(v) = &mut self.bwe.validation {
            v.on_sent(*id, seq, at);
        }
    }

    pub fn on_validation_credit_discard(
        &mut self,
        id: TwccClusterId,
        seq: u64,
        sent_at: Instant,
        discarded: Duration,
    ) {
        if let Some(v) = &mut self.bwe.validation {
            v.on_credit_discard_at_seq(*id, seq, sent_at, discarded);
        }
    }

    pub fn validation_should_stop(&self) -> bool {
        self.bwe.validation.as_ref().is_some_and(|v| !v.active())
    }

    pub fn is_overusing(&self) -> bool {
        self.bwe.is_overusing()
    }

    pub fn set_desired_bitrate(&mut self, v: Bitrate) {
        self.desired_bitrate = v;
    }
}

struct SendSideBandwidthEstimator {
    delay_controller: DelayController,
    loss_controller: LossController,
    acked_bitrate_estimator: AckedBitrateEstimator,
    probe_control: ProbeControl,
    probe_estimator: ProbeEstimator,
    started_at: Option<Instant>,
    alr_detector: AlrDetector,
    link_capacity_estimator: LinkCapacityEstimator,
    last_updated_estimate: Option<Bitrate>,
    last_twcc_feedback_at: Option<Instant>,
    latest_feedback: Option<BweFeedbackObservation>,
    last_successful_probe: Option<BweProbeObservation>,
    latest_scheduled_probe: Option<BweProbeObservation>,
    validation: Option<Validation>,
}

impl SendSideBandwidthEstimator {
    pub fn new(initial_bitrate: Bitrate) -> Self {
        let mut alr_detector = AlrDetector::new();
        alr_detector.set_estimated_bitrate(initial_bitrate);

        let mut loss_controller = LossController::new();
        loss_controller.set_bandwidth_estimate(initial_bitrate);

        Self {
            delay_controller: DelayController::new(initial_bitrate),
            loss_controller,
            acked_bitrate_estimator: AckedBitrateEstimator::new(),
            probe_control: ProbeControl::new(),
            probe_estimator: ProbeEstimator::new(),
            started_at: None,
            alr_detector,
            link_capacity_estimator: LinkCapacityEstimator::new(),
            last_updated_estimate: None,
            last_twcc_feedback_at: None,
            latest_feedback: None,
            last_successful_probe: None,
            latest_scheduled_probe: None,
            validation: None,
        }
    }

    /// Whether the delay-based detector currently signals overuse.
    ///
    /// This is useful for gating behaviors (like padding/probing) that would otherwise
    /// re-excite the system while we're already congested.
    pub fn is_overusing(&self) -> bool {
        self.delay_controller.is_overusing()
    }

    /// Update ALR detector with actual bytes sent.
    ///
    /// Accounts all tracked RTP packets, including padding and probes.
    /// This is typically called from the session's packet sending logic.
    pub fn on_media_sent(&mut self, bytes: DataSize, now: Instant) {
        self.alr_detector.on_bytes_sent(bytes, now);
    }

    /// Record a packet from a TWCC report.
    pub fn update<'t>(&mut self, records: impl Iterator<Item = &'t TwccSendRecord>, now: Instant) {
        let _ = self.started_at.get_or_insert(now);

        let send_records: Vec<_> = records.collect();
        if !send_records.is_empty() {
            self.last_twcc_feedback_at = Some(now);
        }

        let batch_has_loss = send_records
            .iter()
            .any(|record| record.remote_recv_time().is_none());
        let mut validation_feedback = false;
        if let Some(v) = &mut self.validation {
            let request = v.snapshot();
            for record in &send_records {
                if record.cluster().is_some_and(|id| *id == request.id)
                    && record.local_send_time() >= request.requested_at
                {
                    validation_feedback = true;
                    if let Some(bytes) = record.validation_packet_size() {
                        v.record_packet(
                            *record.seq(),
                            record.local_send_time(),
                            record.remote_recv_time(),
                            bytes,
                            record.size(),
                            now,
                        );
                    }
                }
            }
        }

        // Google's default robust throughput path has no ALR variance hook.
        // Only actual, unique acknowledged send/receive records enter its window.

        // Feed records to probe estimator for analysis and process any new probe results
        let mut latest_probe_result = None;
        for (config, bitrate) in self.probe_estimator.update(send_records.iter().copied()) {
            latest_probe_result = Some(bitrate);
            self.last_successful_probe = Some(BweProbeObservation {
                at: now,
                bitrate,
                cluster_id: *config.cluster(),
                kind: config.kind(),
                started_at: config.started_at(),
            });
            debug!(cluster_id = *config.cluster(), kind = ?config.kind(), bitrate_bps = bitrate.as_u64(), "native probe measured");

            // Update link capacity estimator for every successful ALR probe, not just the latest.
            // The estimator internally takes the max of all probe results, building up knowledge
            // of proven link capacity. This differs from the delay controller, which only receives
            // the latest probe result (matching WebRTC's FetchAndResetLastEstimatedBitrate behavior).
            if config.is_alr_probe() {
                self.link_capacity_estimator.update_from_probe(bitrate, now);
            }
        }

        let mut acked_packets: Vec<AckedPacket> = vec![];

        let mut max_rtt = None;
        let mut count = 0;
        let mut lost = 0;
        for record in send_records.iter() {
            count += 1;
            let Ok(acked_packet) = (*record).try_into() else {
                lost += 1;
                continue;
            };
            acked_packets.push(acked_packet);
            max_rtt = max_rtt.max(record.rtt());
        }
        if count > 0 {
            self.latest_feedback = Some(BweFeedbackObservation {
                at: now,
                received_packets: count - lost,
                lost_packets: lost,
                max_rtt,
                min_propagation_rtt: feedback_rtt::min_propagation_rtt(
                    now,
                    acked_packets
                        .iter()
                        .map(|packet| (packet.local_send_time, packet.remote_recv_time)),
                ),
            });
        }
        acked_packets.sort_by(AckedPacket::order_by_receive_time);

        self.acked_bitrate_estimator.incoming_feedback(
            acked_packets
                .iter()
                .map(|packet| (packet.local_send_time, packet.remote_recv_time, packet.size)),
        );

        let acked_bitrate = self.acked_bitrate_estimator.current_estimate();

        // Use the latest probe result from this update, if any
        let probe_result = latest_probe_result;

        let is_probe_result = probe_result.is_some();

        // Update delay controller with the latest probe result
        let maybe_estimate =
            self.delay_controller
                .update(&acked_packets, acked_bitrate, probe_result, now);

        let Some(delay_estimate) = maybe_estimate else {
            self.finish_validation_feedback(validation_feedback, batch_has_loss, now);
            return;
        };

        let loss = if count == 0 {
            0.0
        } else {
            lost as f64 / count as f64
        };
        log_loss!(loss);

        // During startup with no loss, use delay-based estimate directly
        if in_startup_phase(self.started_at, now) && loss <= 0.001 {
            self.loss_controller.set_bandwidth_estimate(delay_estimate);
            self.finish_validation_feedback(validation_feedback, batch_has_loss, now);
            return;
        }

        // When probe succeeds, set bandwidth directly
        if is_probe_result {
            self.loss_controller.set_bandwidth_estimate(delay_estimate);
        }

        if let Some(acked_bitrate) = acked_bitrate {
            self.loss_controller.set_acknowledged_bitrate(acked_bitrate);
        }

        // This corresponds to UpdateLossBasedEstimator + UpdateEstimate
        self.loss_controller
            .update_bandwidth_estimate(&send_records, delay_estimate);

        // Loss-based result is capped by delay_based_limit
        let loss_result = self.loss_controller.loss_based_result();
        if let Some(loss_estimate) = loss_result.bandwidth_estimate {
            if loss_estimate > delay_estimate {
                // Loss controller produced higher estimate than delay controller
                // Cap it at delay estimate (delay controller is the upper limit)
                self.loss_controller.set_bandwidth_estimate(delay_estimate);
            }
        }

        self.finish_validation_feedback(validation_feedback, batch_has_loss, now);

        // Feed the (possibly combined) estimate into subcomponents wanting it.
        self.propagate_estimate();
    }

    fn finish_validation_feedback(&mut self, matching: bool, batch_has_loss: bool, now: Instant) {
        if !matching {
            return;
        }
        // Evaluate after this batch's actual ACK, trendline and loss updates.
        // A cached Passed observation is never an input a second time.
        let overuse = self.delay_controller.is_overusing();
        let measured_payload = self
            .validation
            .as_mut()
            .and_then(|v| v.finish_feedback(overuse, batch_has_loss, now));
        if let Some(rate) = measured_payload {
            if self.delay_controller.apply_validated_probe(rate, now) {
                if let Some(estimate) = self.delay_controller.last_estimate() {
                    self.loss_controller.set_bandwidth_estimate(estimate);
                    debug!(
                        payload_bps = rate.as_f64(),
                        estimate_bps = estimate.as_f64(),
                        "native sustained measured payload accepted"
                    );
                    self.propagate_estimate();
                }
            }
        }
    }

    pub fn poll_timeout(&self) -> (Option<Instant>, Reason) {
        let delay_timeout = Some(self.delay_controller.poll_timeout());
        let validation = self.validation.as_ref().filter(|v| v.active());
        let probe_timeout = validation
            .map(|v| v.snapshot().requested_at + Duration::from_secs(3))
            .or_else(|| Some(self.probe_control.poll_timeout()));
        let probe_estimator_timeout = Some(self.probe_estimator.poll_timeout());
        (delay_timeout, Reason::BweDelayControl)
            .soonest((probe_timeout, Reason::BweProbeControl))
            .soonest((probe_estimator_timeout, Reason::BweProbeEstimator))
    }

    /// Handle periodic timeout for BWE components.
    pub fn handle_timeout(
        &mut self,
        desired_bitrate: Bitrate,
        do_probe: bool,
        now: Instant,
    ) -> Option<ProbeClusterConfig> {
        self.delay_controller
            .handle_timeout(self.acked_bitrate_estimator.current_estimate(), now);

        // Update probe control with desired bitrate.
        self.probe_control.set_desired_bitrate(desired_bitrate);

        // Empty media queues are application-limited too. Advance the budget
        // without charging probe/padding bytes, including before the first media.
        if do_probe {
            self.alr_detector.handle_timeout(now);
        }

        // Get ALR state and forward to both probe control and loss controller
        let alr_start_time = self.alr_detector.alr_start_time();
        if let Some(t) = alr_start_time {
            self.probe_control.set_alr_start_time(t);
        } else {
            self.probe_control.set_alr_stop_time(now);
        }

        self.loss_controller.set_alr_start_time(alr_start_time);

        // Get link capacity estimate and forward to loss controller.
        let link_capacity = self.link_capacity_estimator.capacity_estimate(now);
        self.loss_controller
            .set_link_capacity_estimate(link_capacity);

        // Clean up expired probe cluster state
        self.probe_estimator.handle_timeout(now);

        // Feed the current estimate into subcontrollers, if it changed.
        self.propagate_estimate();

        // If we can't probe, clear any pending/active probes
        if !do_probe {
            self.probe_estimator.clear_probes();
        }

        self.probe_control.enable(do_probe);

        if let Some(v) = &mut self.validation {
            v.timeout(now);
            if !do_probe {
                v.terminate(BweValidationStatus::Failed, now);
            }
            // Preserve the ordinary controller state while serializing probe
            // transmission. A timeout or terminal feedback resumes it naturally.
            if v.active() {
                return None;
            }
        }

        // Timeout-driven probe logic (WebRTC `Process()` equivalent).
        let cluster = self.probe_control.handle_timeout(now);
        if let Some(config) = &cluster {
            self.latest_scheduled_probe = Some(BweProbeObservation {
                at: now,
                bitrate: config.target_bitrate(),
                cluster_id: *config.cluster(),
                kind: config.kind(),
                started_at: config.started_at(),
            });
            debug!(cluster_id = *config.cluster(), kind = ?config.kind(), target_bps = config.target_bitrate().as_u64(), desired_bps = desired_bitrate.as_u64(), "native probe scheduled");
        }
        cluster
    }

    fn snapshot(&self) -> BweSnapshot {
        BweSnapshot {
            raw_estimate: self.last_estimate(),
            acknowledged_bitrate: self.acked_bitrate_estimator.current_estimate(),
            cause: match self.bandwidth_limited_cause() {
                BandwidthLimitedCause::DelayBasedLimited => BweEstimateCause::Normal,
                BandwidthLimitedCause::DelayBasedLimitedDelayIncreased => {
                    BweEstimateCause::DelayOveruse
                }
                BandwidthLimitedCause::LossLimitedBwe => BweEstimateCause::LossLimited,
                BandwidthLimitedCause::LossLimitedBweIncreasing => BweEstimateCause::LossRecovering,
            },
            last_twcc_feedback_at: self.last_twcc_feedback_at,
            latest_feedback: self.latest_feedback,
            last_successful_probe: self.last_successful_probe,
            latest_scheduled_probe: self.latest_scheduled_probe,
            application_limited: self.alr_detector.alr_start_time().is_some(),
            // Legacy classic-path diagnostic. Robust ALR setters are no-ops.
            ack_fast_rate_changes: 0,
            last_validation: self.validation.as_ref().map(Validation::snapshot),
        }
    }

    fn propagate_estimate(&mut self) {
        // Do we have a value?
        let Some(estimate) = self.last_estimate() else {
            return;
        };
        // Did it change?
        if self.last_updated_estimate == Some(estimate) {
            return;
        }

        let cause = self.bandwidth_limited_cause();

        self.probe_control.set_estimated_bitrate(estimate, cause);
        self.alr_detector.set_estimated_bitrate(estimate);

        // Don't update until this changes.
        self.last_updated_estimate = Some(estimate);
    }

    fn bandwidth_limited_cause(&self) -> BandwidthLimitedCause {
        if self.delay_controller.is_overusing() {
            return BandwidthLimitedCause::DelayBasedLimitedDelayIncreased;
        }

        match self.loss_controller.loss_based_result().state {
            LossControllerState::DelayBased => BandwidthLimitedCause::DelayBasedLimited,
            LossControllerState::Increasing => BandwidthLimitedCause::LossLimitedBweIncreasing,
            LossControllerState::Decreasing => BandwidthLimitedCause::LossLimitedBwe,
        }
    }

    /// Get the latest estimate.
    pub fn last_estimate(&self) -> Option<Bitrate> {
        let delay_estimate = self.delay_controller.last_estimate();

        let loss_result = self.loss_controller.loss_based_result();

        // Only apply loss-based limiting when actively in a loss-limiting state
        match loss_result.state {
            LossControllerState::DelayBased => {
                // Loss controller defers to delay-based estimate
                delay_estimate
            }
            LossControllerState::Decreasing | LossControllerState::Increasing => {
                // Loss controller is actively limiting or recovering
                match (delay_estimate, loss_result.bandwidth_estimate) {
                    (Some(de), Some(le)) => Some(de.min(le)),
                    (None, le @ Some(_)) => le,
                    (de @ Some(_), None) => de,
                    (None, None) => None,
                }
            }
        }
    }

    /// Start analyzing a probe cluster.
    ///
    /// This should be called when the pacer starts sending a probe cluster,
    /// to tell the estimator which cluster to watch for in TWCC feedback.
    /// Returns `true` if the probe was started, `false` if rejected.
    pub fn start_probe(&mut self, config: ProbeClusterConfig, now: Instant) -> bool {
        if !self.probe_control.automatic_probing_enabled()
            && config.kind() != probe::ProbeKind::Validation
        {
            return false;
        }
        self.probe_estimator.probe_start(config, now)
    }

    /// End a probe cluster and mark it for cleanup.
    ///
    /// This should be called when the pacer finishes sending a probe cluster.
    /// The estimator will continue collecting feedback for a cluster history period
    /// to allow late-arriving TWCC reports to refine the estimate.
    pub fn end_probe(&mut self, now: Instant, cluster_id: TwccClusterId) {
        self.probe_estimator.end_probe(now, cluster_id);
    }

    pub fn reset(&mut self, init_bitrate: Bitrate) {
        // TWCC send records outlive a BWE reset. Never reuse a cluster identity
        // within this transport, including packets sent at the same Instant.
        let next_id = self.probe_control.next_cluster_id();
        let periodic = self.probe_control.periodic_alr_policy();
        let automatic = self.probe_control.automatic_probing_enabled();
        *self = Self::new(init_bitrate);
        self.probe_control.continue_cluster_ids(next_id);
        self.probe_control.restore_periodic_alr_policy(periodic);
        self.probe_control.set_automatic_probing(automatic);
    }
}

/// A RTP packet that has been sent and acknowledged by the receiver in a TWCC report.
#[derive(Debug, Copy, Clone)]
pub struct AckedPacket {
    /// The TWCC sequence number
    seq_no: TwccSeq,
    /// The size of the packets in bytes.
    size: DataSize,
    /// When we sent the packet
    local_send_time: Instant,
    /// When the packet was received at the remote, note this Instant is only usable with other
    /// instants of the same type i.e. those that represent a TWCC reported receive time for this
    /// session.
    remote_recv_time: Instant,
    /// The local time when received confirmation that the other side received the seq i.e. when we
    /// received the TWCC report for this packet.
    local_recv_time: Instant,
}

impl AckedPacket {
    fn rtt(&self) -> Duration {
        self.local_recv_time - self.local_send_time
    }

    fn order_by_receive_time(lhs: &Self, rhs: &Self) -> Ordering {
        if lhs.remote_recv_time != rhs.remote_recv_time {
            lhs.remote_recv_time.cmp(&rhs.remote_recv_time)
        } else if lhs.local_send_time != rhs.local_send_time {
            lhs.local_send_time.cmp(&rhs.local_send_time)
        } else {
            lhs.seq_no.cmp(&rhs.seq_no)
        }
    }
}

// NB: Extracted for lifetime reasons
fn in_startup_phase(started_at: Option<Instant>, now: Instant) -> bool {
    started_at
        .map(|s| now.duration_since(s) <= STARTUP_PHASE)
        .unwrap_or(false)
}

impl TryFrom<&TwccSendRecord> for AckedPacket {
    type Error = ();

    fn try_from(value: &TwccSendRecord) -> Result<Self, Self::Error> {
        let Some(remote_recv_time) = value.remote_recv_time() else {
            return Err(());
        };
        let Some(local_recv_time) = value.local_recv_time() else {
            return Err(());
        };

        Ok(Self {
            seq_no: value.seq(),
            size: value.size().into(),
            local_send_time: value.local_send_time(),
            remote_recv_time,
            local_recv_time,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BandwidthUsage {
    Overuse,
    Normal,
    Underuse,
}

impl fmt::Display for BandwidthUsage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BandwidthUsage::Overuse => write!(f, "overuse"),
            BandwidthUsage::Normal => write!(f, "normal"),
            BandwidthUsage::Underuse => write!(f, "underuse"),
        }
    }
}
