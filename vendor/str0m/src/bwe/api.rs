//! Bandwidth estimation.

use std::time::{Duration, Instant};

use crate::{Rtc, rtp_::Mid};

pub use crate::bwe_::probe::ProbeKind;
pub use crate::bwe_::validation::{
    BweValidationObservation, BweValidationProof, BweValidationProofScope, BweValidationStatus,
    BweValidationTotals,
};

pub use crate::rtp_::Bitrate;
pub use crate::rtp_::BweIncomingTwccObservation;

#[derive(Debug, PartialEq)]
#[non_exhaustive]
/// Bandwidth estimation kind.
pub enum BweKind {
    /// Transport wide congestion control.
    ///
    /// Emits the current estimate with `can_probe: true` when probing becomes
    /// available, initially using the configured starting bitrate. Estimate
    /// updates follow while probing is available. On loss of probing capability,
    /// emits the last reported estimate once with `can_probe: false`, then
    /// suppresses updates until probing becomes available again.
    Twcc {
        /// Estimated available bitrate. Initially this is the configured starting bitrate.
        /// When probing becomes unavailable, this retains the last estimate.
        estimate: Bitrate,
        /// Whether the local BWE can currently probe for capacity.
        ///
        /// Requires BWE to be enabled, SRTP keys, and a sending media section
        /// supporting TWCC feedback and the transport sequence extension.
        /// This does not guarantee that feedback is arriving.
        can_probe: bool,
    },
    /// A real received REMB with its complete source identity, enabled by the
    /// explicit remote feedback meter. One event per received RTCP report;
    /// unknown source identities must be ignored by the application.
    ReceiverEstimate {
        /// Actual receiver-reported available bitrate, never a requested rate.
        estimate: Bitrate,
        /// Complete actual report SSRC list, in its original wire order.
        ssrcs: Vec<u32>,
    },
    /// REMB (Receiver Estimated Maximum Bitrate)
    Remb {
        /// Bitrate reported by the remote receiver.
        estimate: Bitrate,
        /// Media section associated with the report.
        mid: Mid,
    },
}

/// The estimator's current limiting cause, independent of probing capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum BweEstimateCause {
    /// No current native congestion signal (not proof of unused capacity).
    Normal,
    /// Delay-based detector currently signals overuse.
    DelayOveruse,
    /// Loss-based estimator is actively reducing its estimate.
    LossLimited,
    /// Loss-based estimator is recovering, still limiting the estimate.
    LossRecovering,
}

/// One native probe result or scheduled cluster. A scheduled target is not capacity.
#[derive(Debug, Clone, Copy)]
pub struct BweProbeObservation {
    /// Local observation time (feedback receipt or cluster scheduling).
    pub at: Instant,
    /// Measured received bitrate or the scheduled cluster target, respectively.
    pub bitrate: Bitrate,
    /// Native cluster identifier, unique until an estimator reset.
    pub cluster_id: u64,
    /// Earliest local send time included in a measured result; absent for a
    /// merely scheduled cluster. Late feedback receipt cannot move this forward.
    pub started_at: Option<Instant>,
    /// Native reason for scheduling the cluster.
    pub kind: ProbeKind,
}

/// Quality of the latest received TWCC batch containing matched send records.
/// Counts cover that batch, not an assumed fixed-duration loss window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BweFeedbackObservation {
    /// Local receipt time, equal to the snapshot's `last_twcc_feedback_at`.
    pub at: Instant,
    /// Matched records acknowledged as received by the remote endpoint.
    pub received_packets: u64,
    /// Matched records reported without a remote receive timestamp.
    pub lost_packets: u64,
    /// Largest feedback RTT, including receiver report waiting; diagnostic only.
    pub max_rtt: Option<Duration>,
    /// Minimum report-local propagation estimate after removing receiver wait
    /// evidenced by this batch's receive span. Unknown for no received samples
    /// or any invalid negative/future-send candidate; never falls back to max.
    /// Waiting after the last received packet remains unobservable and included.
    pub min_propagation_rtt: Option<Duration>,
}

impl BweFeedbackObservation {
    /// Reported loss fraction, absent for an empty batch. Native observations
    /// always contain at least one matched record; local polling never adds one.
    pub fn loss_fraction(&self) -> Option<f32> {
        let total = self.received_packets as f64 + self.lost_packets as f64;
        (total > 0.0).then(|| (self.lost_packets as f64 / total) as f32)
    }
}

/// Read-only native bandwidth observations; polling never advances feedback time.
#[derive(Debug, Clone, Copy)]
pub struct BweSnapshot {
    /// Unsmooth native estimate, which may fall merely because media is application-limited.
    pub raw_estimate: Option<Bitrate>,
    /// Current native acknowledged-throughput estimate.
    pub acknowledged_bitrate: Option<Bitrate>,
    /// Current native delay/loss limiting cause.
    pub cause: BweEstimateCause,
    /// Local receipt time of the latest TWCC report containing matched send records.
    /// Bootstrap estimates, periodic stats and BWE timeouts do not update this field.
    pub last_twcc_feedback_at: Option<Instant>,
    /// Latest matched TWCC batch quality, retained unchanged across silence.
    pub latest_feedback: Option<BweFeedbackObservation>,
    /// Latest feedback-validated probe result, not a requested sending rate.
    pub last_successful_probe: Option<BweProbeObservation>,
    /// Latest cluster returned to the pacer; useful for correlating actual probe rates.
    pub latest_scheduled_probe: Option<BweProbeObservation>,
    /// Whether the native application-limited-region detector is active.
    pub application_limited: bool,
    /// Latest bounded sustained validation; short probe results never populate this.
    pub last_validation: Option<BweValidationObservation>,
    /// Legacy classic-path diagnostic, retained for observation compatibility.
    /// Always zero on the default robust throughput path, whose ALR hooks are no-ops.
    pub ack_fast_rate_changes: u64,
}

/// Access to the Bandwidth Estimate subsystem.
pub struct Bwe<'a>(pub(crate) &'a mut Rtc);

impl<'a> Bwe<'a> {
    /// Enable bounded incoming-TWCC metadata collection (disabled by default).
    /// This changes diagnostics only, not feedback bytes, timing or BWE policy.
    /// Repeating the same value is a no-op; disabling clears diagnostic history.
    pub fn set_incoming_twcc_observations(&mut self, enabled: bool) {
        self.0.session.set_incoming_twcc_observations(enabled);
    }

    /// Read-only metadata for server-generated incoming-RTP TWCC feedback.
    /// Returns `None` without explicit opt-in or before a report is recorded.
    /// Serialization-poll timestamps are not kernel/network send timestamps.
    pub fn incoming_twcc_snapshot(&self) -> Option<BweIncomingTwccObservation> {
        self.0.session.incoming_twcc_snapshot()
    }

    /// Start a bounded sustained validation at the required target. Returns its
    /// identity only if transport capabilities and current congestion permit it.
    /// Repeated requests while one is active never extend the fixed deadline.
    pub fn start_validation(&mut self, target: Bitrate, now: Instant) -> Option<u64> {
        self.0.session.start_bwe_validation(target, now)
    }

    /// Explicit experiment with protected proof floor and bounded pacing strength.
    /// Requires finite representable 0 < floor <= pacing <= 1.2 * floor. Success proves
    /// only actual selected evidence at floor, never the requested pacing rate.
    /// Whole-request proof fallback is available only through this opt-in API.
    pub fn start_validation_with_pacing(
        &mut self,
        floor: Bitrate,
        pacing: Bitrate,
        now: Instant,
    ) -> Option<u64> {
        self.0
            .session
            .start_bwe_validation_with_pacing(floor, pacing, now)
    }

    /// Cancel this validation and its generated padding, preserving media/RTX.
    pub fn cancel_validation(&mut self, now: Instant) {
        self.0.session.cancel_bwe_validation(now);
    }

    /// Read native state without changing the estimator or generating feedback.
    pub fn snapshot(&self) -> Option<BweSnapshot> {
        self.0.session.bwe_snapshot()
    }

    /// Enable native automatic short-probe discovery (enabled by default).
    ///
    /// Disabling cancels only non-validation probe trains and their generated
    /// padding. Native feedback, congestion control, media/RTX, physical probing
    /// capability and explicit sustained validation continue. No estimate or
    /// capacity proof is added. The policy survives estimator reset; identical
    /// repeated settings do not cancel validation or renew any deadline.
    pub fn set_automatic_probing(&mut self, enabled: bool) {
        let now = self.0.last_now;
        self.0.session.set_automatic_probing(enabled, now);
    }

    /// Read the configured normal-media pacing rate without changing state.
    ///
    /// This excludes temporary probe rates and queue-drain acceleration, and is
    /// not measured link capacity. None means no bandwidth-estimating pacer.
    pub fn configured_pacing_rate(&self) -> Option<Bitrate> {
        self.0.session.configured_bwe_pacing_rate()
    }

    /// Enable periodic ALR exploration (enabled by default).
    ///
    /// Applications may disable this when their current allocation covers all
    /// demand. This cancels only periodic ALR probes and their pending padding;
    /// it never increases capacity or suppresses congestion feedback. Other
    /// probing and sustained validation remain enabled. Re-enabling respects a
    /// fresh normal periodic interval; repeated identical settings do not renew it.
    pub fn set_periodic_alr_probing(&mut self, enabled: bool) {
        let now = self.0.last_now;
        self.0.session.set_periodic_alr_probing(enabled, now);
    }

    /// Configure the desired bitrate.
    ///
    /// Configure the bandwidth estimation system with the desired bitrate.
    /// **Note:** This only has an effect if BWE has been enabled via
    /// [`RtcConfig::enable_bwe`][crate::RtcConfig::enable_bwe].
    ///
    /// * `desired_bitrate` The bitrate you would like to eventually send at. The BWE system will try
    ///   to reach this bitrate by probing with padding packets. You should allocate your media bitrate
    ///   based on the estimated the BWE system produces via
    ///   [`Event::EgressBitrateEstimate`][crate::Event::EgressBitrateEstimate]. This rate might not
    ///   be reached if the network link cannot sustain the desired bitrate.
    ///
    /// ## Example
    ///
    /// Say you have three simulcast video tracks each with a high layer configured at 1.5Mbit/s.
    /// You should then set the desired bitrate to 4.5Mbit/s (or slightly higher). If the network
    /// link can sustain 4.5Mbit/s there will eventually be an
    /// [`Event::EgressBitrateEstimate`][crate::Event::EgressBitrateEstimate] with this estimate.
    pub fn set_desired_bitrate(&mut self, desired_bitrate: Bitrate) {
        self.0.session.set_bwe_desired_bitrate(desired_bitrate);
    }

    /// Reset the BWE with a new init_bitrate
    ///
    /// This discards the current estimator state and starts estimation again
    /// from `init_bitrate`. Normal transitions from audio-only to video sending
    /// do not require a reset: with TWCC negotiated, probing can discover
    /// capacity before video starts. The desired bitrate controls the capacity
    /// the estimator attempts to discover.
    pub fn reset(&mut self, init_bitrate: Bitrate) {
        self.0.session.reset_bwe(init_bitrate);
    }
}
