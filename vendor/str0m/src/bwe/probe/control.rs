//! Bandwidth probing controller - decides when and how to probe network capacity.
//!
//! This module implements WebRTC's `ProbeController` state machine for discovering available
//! bandwidth through intentional bursts of packets at rates higher than current estimates.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use super::{ProbeClusterConfig, ProbeKind};
use crate::rtp_::{Bitrate, TwccClusterId};
use crate::util::{already_happened, not_happening};

// Port notes:
// This module ports WebRTC's `ProbeController` behavior from:
// `webrtc/modules/congestion_controller/goog_cc/probe_controller.cc`
//
// Key integration difference: WebRTC returns vectors of probe clusters, while str0m
// returns a single `ProbeClusterConfig` per `handle_timeout()` call. Configs are queued
// internally and `poll_timeout()` returns `already_happened()` until the queue is drained.

/// WebRTC: `kMaxWaitingTimeForProbingResult`.
const MAX_WAITING_TIME_FOR_PROBING_RESULT: Duration = Duration::from_secs(1);

/// WebRTC: `kBitrateDropThreshold`, `kBitrateDropTimeout`, `kProbeFractionAfterDrop`,
/// `kProbeUncertainty`, `kAlrEndedTimeout`, `kMinTimeBetweenAlrProbes`.
const BITRATE_DROP_THRESHOLD: f64 = 0.66;
const BITRATE_DROP_TIMEOUT: Duration = Duration::from_secs(5);
const PROBE_FRACTION_AFTER_DROP: f64 = 0.85;
const PROBE_UNCERTAINTY: f64 = 0.05;
const ALR_ENDED_TIMEOUT: Duration = Duration::from_secs(3);
const MIN_TIME_BETWEEN_ALR_PROBES: Duration = Duration::from_secs(5);

/// WebRTC: inline `* 2` in probe_controller.cc InitiateProbing().
/// Allows probing up to 2x max_bitrate to account for bursty streams.
const MAX_PROBE_BITRATE_FACTOR: f64 = 2.0;

/// Minimum time between stagnant periodic probes to avoid excessive probing when at capacity.
const MIN_TIME_BETWEEN_STAGNANT_PROBES: Duration = Duration::from_secs(15);

/// Threshold for considering an estimate change significant (5%).
const ESTIMATE_CHANGE_THRESHOLD: f64 = 0.05;

/// Probe rate scale for stagnation probes (2× current estimate).
const STAGNANT_PROBE_SCALE: f64 = 2.0;

/// WebRTC's `BandwidthLimitedCause` (subset used by probing gating).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BandwidthLimitedCause {
    LossLimitedBweIncreasing,
    LossLimitedBwe,
    DelayBasedLimited,
    DelayBasedLimitedDelayIncreased,
}

pub struct ProbeControl {
    config: Config,
    next_timeout: Instant,
    enabled: bool,

    desired_bitrate: Option<Bitrate>,
    prev_desired: Option<Bitrate>,

    last_estimate: Option<Bitrate>,
    last_estimate_change: Option<Instant>,
    last_cause: BandwidthLimitedCause,

    prev_estimate: Option<Bitrate>,

    alr_start: Option<Instant>,
    alr_stop: Option<Instant>,

    last_probe: Option<LastProbe>,

    large_drop: Option<LargeDrop>,

    last_stagnant: Option<Instant>,

    next_cluster_id: TwccClusterId,
    pending: VecDeque<ProbeClusterConfig>,

    scheduled_exponential: Option<Instant>,
    scheduled_periodic_alr: Option<Instant>,
    periodic_alr_enabled: bool,
    automatic_probing_enabled: bool,
    periodic_alr_not_before: Option<Instant>,
    scheduled_stagnant: Option<Instant>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct LastProbe {
    when: Instant,
    kind: ProbeKind,
    further: Bitrate,
    was_estimate: Option<Bitrate>,
}

struct LargeDrop {
    when: Instant,
    bitrate_before: Bitrate,
}

impl Default for ProbeControl {
    fn default() -> Self {
        Self {
            config: Config::default(),
            enabled: false,
            next_timeout: not_happening(),
            desired_bitrate: None,
            prev_desired: None,
            last_estimate: None,
            last_estimate_change: None,
            last_cause: BandwidthLimitedCause::DelayBasedLimited,
            prev_estimate: None,
            alr_start: None,
            alr_stop: None,
            next_cluster_id: 0.into(),
            last_probe: None,
            large_drop: None,
            last_stagnant: None,
            pending: VecDeque::new(),
            scheduled_exponential: None,
            scheduled_periodic_alr: None,
            periodic_alr_enabled: true,
            automatic_probing_enabled: true,
            periodic_alr_not_before: None,
            scheduled_stagnant: None,
        }
    }
}

impl ProbeControl {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn next_cluster_id(&self) -> TwccClusterId {
        self.next_cluster_id
    }
    pub(crate) fn continue_cluster_ids(&mut self, next: TwccClusterId) {
        self.next_cluster_id = next;
    }

    pub(crate) fn validation_config(
        &mut self,
        target: Bitrate,
        now: Instant,
    ) -> Option<ProbeClusterConfig> {
        if target < Bitrate::kbps(5) || !self.can_probe(self.last_estimate?) {
            return None;
        }
        let id = self.next_cluster_id.inc();
        Some(
            ProbeClusterConfig::new(id, target, ProbeKind::Validation)
                .with_duration(Duration::from_secs(2))
                .with_requested_at(now),
        )
    }

    pub(crate) fn automatic_probing_enabled(&self) -> bool {
        self.automatic_probing_enabled
    }

    pub(crate) fn set_automatic_probing(&mut self, enabled: bool) -> bool {
        if self.automatic_probing_enabled == enabled {
            return false;
        }
        self.automatic_probing_enabled = enabled;
        if !enabled {
            self.pending.retain(|p| p.kind() == ProbeKind::Validation);
        }
        self.request_immediate();
        true
    }

    pub(crate) fn periodic_alr_policy(&self) -> (bool, Option<Instant>) {
        (self.periodic_alr_enabled, self.periodic_alr_not_before)
    }

    pub(crate) fn restore_periodic_alr_policy(&mut self, policy: (bool, Option<Instant>)) {
        self.periodic_alr_enabled = policy.0;
        self.periodic_alr_not_before = policy.1;
        self.request_immediate();
    }

    pub(crate) fn set_periodic_alr_probing(&mut self, enabled: bool, now: Instant) {
        if enabled == self.periodic_alr_enabled {
            return;
        }
        self.periodic_alr_enabled = enabled;
        self.periodic_alr_not_before = enabled.then_some(now + MIN_TIME_BETWEEN_ALR_PROBES);
        if !enabled {
            self.pending.retain(|p| p.kind() != ProbeKind::PeriodicAlr);
        }
        self.request_immediate();
    }

    pub fn enable(&mut self, v: bool) {
        if !self.enabled && v {
            self.enabled = true;
            self.request_immediate();
        } else if self.enabled && !v {
            self.enabled = false;
            self.pending.clear();
            self.last_estimate = None;
            self.desired_bitrate = None;
            self.last_estimate_change = None;
            self.last_stagnant = None;
            self.last_probe = None;
            self.prev_estimate = None;
            self.scheduled_exponential = None;
            self.scheduled_periodic_alr = None;
            self.scheduled_stagnant = None;
            self.next_timeout = not_happening();
        }
    }

    pub fn set_desired_bitrate(&mut self, v: Bitrate) {
        // Don't accept Bitrate::ZERO as first ever value.
        if self.desired_bitrate.is_none() && v.is_zero() {
            return;
        }
        self.desired_bitrate = Some(v);
        self.request_immediate();
    }

    pub fn set_estimated_bitrate(&mut self, v: Bitrate, cause: BandwidthLimitedCause) {
        // Don't accept Bitrate::ZERO as first ever value.
        if self.last_estimate.is_none() && v.is_zero() {
            return;
        }

        // Check if estimate changed significantly (>5%) or cause changed.
        let dominated_by_last = self.last_estimate.is_some_and(|last| {
            let upper = last * (1.0 + ESTIMATE_CHANGE_THRESHOLD);
            let lower = last * (1.0 - ESTIMATE_CHANGE_THRESHOLD);
            v <= upper && v >= lower
        });

        if dominated_by_last && self.last_cause == cause {
            return;
        }

        self.last_estimate = Some(v);
        self.last_cause = cause;
        self.request_immediate();
    }

    pub fn set_alr_start_time(&mut self, t: Instant) {
        if self.alr_start.is_some() {
            return;
        }
        self.alr_start = Some(t);
        self.alr_stop = None;
        self.request_immediate();
    }

    pub fn set_alr_stop_time(&mut self, t: Instant) {
        if self.alr_start.is_none() || self.alr_stop.is_some() {
            return;
        }
        self.alr_start = None;
        self.alr_stop = Some(t);
        self.request_immediate();
    }

    fn request_immediate(&mut self) {
        self.next_timeout = already_happened();
        self.scheduled_exponential = None;
        self.scheduled_periodic_alr = None;
        self.scheduled_stagnant = None;
    }

    pub fn poll_timeout(&self) -> Instant {
        if self.automatic_probing_enabled {
            self.next_timeout
        } else {
            not_happening()
        }
    }

    pub fn handle_timeout(&mut self, now: Instant) -> Option<ProbeClusterConfig> {
        // Spurious call before timeout is due - ignore.
        if now < self.next_timeout {
            return None;
        }

        // Timeout fired - reset to not_happening until we compute the next one.
        self.next_timeout = not_happening();

        // Automatic discovery is independent of physical capability and explicit
        // sustained validation. Keep estimates/ALR history while it is suspended.
        if !self.automatic_probing_enabled {
            return None;
        }
        // Probing is disabled until first packet sent and padding queue exists.
        if !self.enabled {
            return None;
        }

        // We need to have both desired AND last_estimate set to
        // start considering probing.
        let desired = self.desired_bitrate?;
        let estimate = self.last_estimate?;

        // Unlike Google's vector return, this adapter dispatches a vector one
        // cluster per timeout. Revalidate deferred work against CURRENT limits,
        // not the congestion/demand state when the vector was originally built.
        if !self.can_probe(estimate) {
            self.pending.clear();
            return None;
        }
        let current_max = desired * MAX_PROBE_BITRATE_FACTOR;
        self.pending.retain(|config| {
            (self.periodic_alr_enabled || config.kind() != ProbeKind::PeriodicAlr)
                && config.target_bitrate() <= current_max
                && config
                    .requested_at()
                    .is_none_or(|at| now.saturating_duration_since(at) <= Duration::from_secs(5))
        });
        if let Some(config) = self.pending.pop_front() {
            self.request_immediate();
            return Some(config);
        }

        // Try each probe type in order - only one fires per timeout.
        let _ = self.maybe_initial(now, desired, estimate)
            || self.maybe_exponential(now, desired, estimate)
            || self.maybe_increase_alr(now, desired, estimate)
            || self.maybe_large_drop(now, desired, estimate)
            || self.maybe_periodic_alr(now, desired)
            || self.maybe_stagnant(now, desired, estimate);

        self.update_estimate_change(now, estimate);

        // Update prev_estimate for next cycle (used by large drop and stagnation detection).
        self.prev_estimate = Some(estimate);

        // Update timeout based on current state.
        self.next_timeout = self.compute_next_timeout(now);

        if !self.pending.is_empty() {
            self.request_immediate();
        }

        self.pending.pop_front()
    }

    fn update_estimate_change(&mut self, now: Instant, estimate: Bitrate) {
        // Track when estimate last changed significantly (>5%).
        if let Some(prev) = self.prev_estimate {
            if estimate != prev {
                self.last_estimate_change = Some(now);
            }
        }

        // Initialize baseline if not set yet.
        if self.last_estimate_change.is_none() {
            self.last_estimate_change = Some(now);
        }
    }

    fn maybe_initial(&mut self, now: Instant, desired: Bitrate, estimate: Bitrate) -> bool {
        // Initial probes only fire once at startup.
        if self.last_probe.is_some() {
            return false;
        }

        // Queue 3× and 6× of estimate.
        let p1 = estimate * self.config.first_exponential_probe_scale;
        let p2 = estimate * self.config.second_exponential_probe_scale;

        self.queue_probe(p1, ProbeKind::Initial, desired, now);
        self.queue_probe(p2, ProbeKind::Initial, desired, now);
        true
    }

    fn maybe_exponential(&mut self, now: Instant, desired: Bitrate, estimate: Bitrate) -> bool {
        // Wait for pending probes to be dispatched first.
        if !self.pending.is_empty() {
            return false;
        }

        // Need a previous probe to continue from.
        let Some(last) = self.last_probe else {
            return false;
        };

        // Estimate must exceed 70% of last probe rate to trigger further probing.
        if estimate < last.further {
            return false;
        }

        let is_same = Some(estimate) == last.was_estimate;
        let time_since = self.time_since_last_probe(now);

        // Don't re-probe at the same estimate; wait for new result or timeout.
        if is_same && time_since < MAX_WAITING_TIME_FOR_PROBING_RESULT {
            return false;
        }

        let scale = self.last_cause.probe_scale(&self.config);
        let target = estimate * scale;

        // Already probed at max rate; no point probing again.
        let max = desired * MAX_PROBE_BITRATE_FACTOR;
        if target >= max && last.further >= max * self.config.further_probe_threshold {
            return false;
        }

        self.queue_probe(target, ProbeKind::Exponential, desired, now);

        true
    }

    fn maybe_increase_alr(&mut self, now: Instant, desired: Bitrate, estimate: Bitrate) -> bool {
        // Don't interfere with initial probing phase.
        if self.is_during_initial(now) {
            return false;
        }

        // Allocation probes only fire in ALR (application-limited region).
        if !self.in_alr() {
            return false;
        }

        let prev = self.prev_desired;
        self.prev_desired = Some(desired);

        // Need a previous desired value to compare against.
        let Some(prev) = prev else {
            return false;
        };

        // Only probe if desired increased
        if desired <= prev {
            return false;
        }

        // No point probing if we already have enough bandwidth.
        if desired <= estimate {
            return false;
        }

        // Allocation probes at 1× and 2× of desired, capped by 2× estimate
        let current_bwe_limit = estimate * self.config.allocation_probe_limit_by_current_scale;

        let p1 = (desired * self.config.first_allocation_probe_scale).min(current_bwe_limit);
        self.queue_probe(p1, ProbeKind::IncreaseAlr, desired, now);

        let p2 = desired * self.config.second_allocation_probe_scale;
        if p2 <= current_bwe_limit && p2 > p1 {
            self.queue_probe(p2, ProbeKind::IncreaseAlr, desired, now);
        }

        true
    }

    fn maybe_periodic_alr(&mut self, now: Instant, desired: Bitrate) -> bool {
        if !self.periodic_alr_enabled || self.periodic_alr_not_before.is_some_and(|at| now < at) {
            return false;
        }
        // Don't interfere with initial probing phase.
        if self.is_during_initial(now) {
            return false;
        }

        // Periodic probes only fire in ALR (application-limited region).
        if !self.in_alr() {
            return false;
        }

        // Respect minimum interval between ALR probes.
        if self.time_since_last_probe(now) < MIN_TIME_BETWEEN_ALR_PROBES {
            return false;
        }

        // Periodic ALR probe at 2× desired (capped by queue_probe to 2× desired anyway).
        // Using desired rather than estimate allows discovering higher capacity when
        // the app wants more bandwidth than currently estimated.
        let target = desired * self.config.further_exponential_probe_scale;
        self.queue_probe(target, ProbeKind::PeriodicAlr, desired, now);
        true
    }

    /// Probe when estimate has stagnated (no change for 15+ seconds) despite unmet demand.
    ///
    /// ## Why This Exists (str0m Addition)
    ///
    /// This probe type addresses a deadlock scenario in the BWE system where AIMD recovery
    /// cannot make progress after network capacity is restored:
    ///
    /// **The Deadlock:**
    /// 1. Network degrades from 5 Mbps → 1 Mbps, estimate drops to ~900 kbps
    /// 2. Application reduces send rate to ~500 kbps (below estimate)
    /// 3. Network recovers to 5 Mbps
    /// 4. AIMD tries to increase but is capped at 1.5× observed throughput:
    ///    500 kbps × 1.5 = 750 kbps maximum
    /// 5. Sending at 500 kbps = 71% of estimate, which is above ALR threshold (65%)
    /// 6. ALR never triggers → no periodic probing
    /// 7. Large-drop probe requires ALR or recent ALR exit (see `maybe_large_drop`)
    /// 8. System is stuck: estimate ~700 kbps on a 5 Mbps network
    ///
    /// **AIMD's 1.5× Cap (line 191 in rate_control.rs):**
    /// `observed_bitrate * 1.5 + Bitrate::kbps(10)`
    /// This prevents runaway growth beyond actual sending rate. It's conservative but
    /// necessary - without it, the estimate could grow unbounded even when we're barely
    /// sending anything.
    ///
    /// **ALR Detection Threshold:**
    /// ALR triggers when sending < 65% of estimate consistently for 500ms with budget
    /// accumulation > 80%. At 60-70% send rate, you're in the deadlock zone: too high
    /// to trigger ALR, too low for AIMD to help much.
    ///
    /// **Loss Controller's 1.5× Cap:**
    /// The loss controller also applies a 1.5× cap during recovery (line 303 in
    /// loss_controller.rs), compounding the AIMD limitation.
    ///
    /// ## How This Differs from WebRTC
    ///
    /// WebRTC does not have stagnation-based probing. They rely on:
    /// 1. Large-drop recovery probe (requires ALR or recent ALR exit)
    /// 2. Rapid recovery field trial (`WebRTC-BweRapidRecoveryExperiment`) which removes
    ///    the ALR requirement from large-drop probes
    ///
    /// str0m adds stagnant probing as a complementary mechanism that:
    /// - Catches deadlock regardless of whether a drop was detected
    /// - Provides periodic escape from any stagnation scenario, not just post-drop
    /// - Uses a conservative 15-second wait to avoid probing at convergence
    /// - Rate-limited to once per 30 seconds to prevent oscillation
    fn maybe_stagnant(&mut self, now: Instant, desired: Bitrate, estimate: Bitrate) -> bool {
        // Don't interfere with initial probing phase.
        if self.is_during_initial(now) {
            return false;
        }

        // Don't probe in ALR (periodic ALR handles that).
        if self.in_alr() {
            return false;
        }

        let Some(last_change) = self.last_estimate_change else {
            return false;
        };

        if now.saturating_duration_since(last_change) < MIN_TIME_BETWEEN_STAGNANT_PROBES {
            return false;
        }

        // Only if there's unmet demand.
        if desired <= estimate {
            return false;
        }

        // Rate limit: at least 30 seconds between stagnation probes.
        if let Some(last_probe) = self.last_stagnant {
            if now.saturating_duration_since(last_probe) < MIN_TIME_BETWEEN_STAGNANT_PROBES {
                return false;
            }
        }

        // Probe at 2× estimate (conservative, won't overwhelm if at capacity).
        let probe_rate = estimate * STAGNANT_PROBE_SCALE;
        self.queue_probe(probe_rate, ProbeKind::Stagnant, desired, now);
        self.last_stagnant = Some(now);

        true
    }

    fn maybe_large_drop(&mut self, now: Instant, desired: Bitrate, estimate: Bitrate) -> bool {
        // Don't interfere with initial probing phase.
        if self.is_during_initial(now) {
            return false;
        }

        // Detect large drops: estimate fell below 66% of previous.
        if self.large_drop.is_none() {
            if let Some(prev) = self.prev_estimate {
                if estimate < prev * BITRATE_DROP_THRESHOLD {
                    self.large_drop = Some(LargeDrop {
                        when: now,
                        bitrate_before: prev,
                    });
                }
            }
        }

        // No large drop detected.
        let Some(drop) = &self.large_drop else {
            return false;
        };

        // Drop expires after 5 seconds.
        if now.saturating_duration_since(drop.when) > BITRATE_DROP_TIMEOUT {
            self.large_drop = None;
            return false;
        }

        // Large-drop probing requires ALR context (in ALR or recently exited).
        if !self.in_alr() && !self.alr_ended_recently(now) {
            return false;
        }

        // Respect minimum interval between ALR probes.
        if self.time_since_last_probe(now) < MIN_TIME_BETWEEN_ALR_PROBES {
            return false;
        }

        // Probe at 85% of pre-drop bitrate.
        let target = drop.bitrate_before * PROBE_FRACTION_AFTER_DROP;
        self.queue_probe(target, ProbeKind::LargeDrop, desired, now);

        self.large_drop = None;
        true
    }

    fn queue_probe(&mut self, bitrate: Bitrate, kind: ProbeKind, desired: Bitrate, now: Instant) {
        // Cap at 2× desired bitrate.
        let max = desired * MAX_PROBE_BITRATE_FACTOR;
        let bitrate = bitrate.min(max);

        // No probe at too small values.
        if bitrate < Bitrate::kbps(5) {
            return;
        }

        let cluster_id = self.next_cluster_id.inc();

        let config = ProbeClusterConfig::new(cluster_id, bitrate, kind)
            .with_min_packet_count(self.config.min_probe_packets_sent)
            .with_duration(self.config.min_probe_duration)
            .with_min_probe_delta(self.config.min_probe_delta)
            .with_requested_at(now);

        // Threshold for further exponential probing (probe_bitrate * 0.7).
        let probe_further = bitrate * self.config.further_probe_threshold;

        self.pending.push_back(config);
        self.last_probe = Some(LastProbe {
            when: now,
            kind,
            further: probe_further,
            was_estimate: self.last_estimate,
        });
    }

    fn compute_next_timeout(&mut self, now: Instant) -> Instant {
        // Exponential probing: wait for probe result before re-probing at same estimate.
        // This handles the case where we sent a probe but haven't received updated estimate yet.
        if let Some(last) = &self.last_probe {
            if matches!(last.kind, ProbeKind::Initial | ProbeKind::Exponential) {
                if self.scheduled_exponential.is_none() {
                    self.scheduled_exponential = Some(now + MAX_WAITING_TIME_FOR_PROBING_RESULT);
                }
                return self.scheduled_exponential.unwrap();
            }
        }

        // ALR periodic probing is an independent application policy.
        if self.in_alr() && self.periodic_alr_enabled {
            if self.scheduled_periodic_alr.is_none() {
                self.scheduled_periodic_alr = Some(
                    (now + MIN_TIME_BETWEEN_ALR_PROBES)
                        .max(self.periodic_alr_not_before.unwrap_or(now)),
                );
            }
            return self.scheduled_periodic_alr.unwrap();
        }

        // Stagnant probing (only when not in ALR)
        if !self.in_alr() {
            if self.scheduled_stagnant.is_none() {
                self.scheduled_stagnant = Some(now + MIN_TIME_BETWEEN_STAGNANT_PROBES);
            }
            return self.scheduled_stagnant.unwrap();
        }

        not_happening()
    }

    fn can_probe(&self, estimate: Bitrate) -> bool {
        // Infinite estimate indicates no valid measurement yet.
        if estimate == Bitrate::INFINITY {
            return false;
        }

        // Only probe when delay-limited or loss-limited-but-increasing.
        // Don't probe during active congestion (loss-limited, delay-increased).
        matches!(
            self.last_cause,
            BandwidthLimitedCause::LossLimitedBweIncreasing
                | BandwidthLimitedCause::DelayBasedLimited
        )
    }

    fn in_alr(&self) -> bool {
        self.alr_start.is_some() && self.alr_stop.is_none()
    }

    fn alr_ended_recently(&self, now: Instant) -> bool {
        self.alr_stop
            .map(|stop| now.saturating_duration_since(stop) < ALR_ENDED_TIMEOUT)
            .unwrap_or(false)
    }

    fn is_during_initial(&self, now: Instant) -> bool {
        let is_initial = matches!(
            self.last_probe.map(|p| p.kind),
            Some(ProbeKind::Initial) | Some(ProbeKind::Exponential)
        );
        is_initial && self.time_since_last_probe(now) <= MAX_WAITING_TIME_FOR_PROBING_RESULT
    }

    fn last_when(&self) -> Option<Instant> {
        self.last_probe.map(|p| p.when)
    }

    fn time_since_last_probe(&self, now: Instant) -> Duration {
        self.last_when()
            .map(|t| now.saturating_duration_since(t))
            .unwrap_or(Duration::MAX)
    }
}

/// Configuration using WebRTC default constants (no field-trial plumbing).
#[derive(Debug, Clone, Copy)]
struct Config {
    // Initial/exponential probing
    first_exponential_probe_scale: f64,   // p1 = 3.0
    second_exponential_probe_scale: f64,  // p2 = 6.0
    further_exponential_probe_scale: f64, // step_size = 2.0
    further_probe_threshold: f64,         // 0.7

    // Allocation probing
    first_allocation_probe_scale: f64,            // 1.0
    second_allocation_probe_scale: f64,           // 2.0
    allocation_probe_limit_by_current_scale: f64, // 2.0

    // Probe cluster config defaults
    min_probe_packets_sent: usize, // 5
    min_probe_duration: Duration,  // 15ms
    min_probe_delta: Duration,     // 2ms

    // Gating / limits
    loss_limited_probe_scale: f64, // 1.5
}

impl Default for Config {
    fn default() -> Self {
        Self {
            first_exponential_probe_scale: 3.0,
            second_exponential_probe_scale: 6.0,
            further_exponential_probe_scale: 2.0,
            further_probe_threshold: 0.7,

            first_allocation_probe_scale: 1.0,
            second_allocation_probe_scale: 2.0,
            allocation_probe_limit_by_current_scale: 2.0,

            min_probe_packets_sent: 5,
            min_probe_duration: Duration::from_millis(15),
            min_probe_delta: Duration::from_millis(2),

            loss_limited_probe_scale: 1.5,
        }
    }
}

impl BandwidthLimitedCause {
    /// Probe scale factor for exponential probing.
    ///
    /// When loss-limited but increasing, use a more conservative 1.575× (1.5 * 1.05).
    /// Otherwise use the standard 2× scale.
    fn probe_scale(&self, config: &Config) -> f64 {
        match self {
            BandwidthLimitedCause::LossLimitedBweIncreasing => {
                config.loss_limited_probe_scale * (1.0 + PROBE_UNCERTAINTY)
            }
            _ => config.further_exponential_probe_scale,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn deferred_probe_is_rejected_when_congestion_started_after_vector_creation() {
        let now = Instant::now();
        for cause in [
            BandwidthLimitedCause::LossLimitedBwe,
            BandwidthLimitedCause::DelayBasedLimitedDelayIncreased,
        ] {
            let mut pc = ProbeControl::new();
            pc.enable(true);
            pc.set_desired_bitrate(Bitrate::mbps(10));
            pc.set_estimated_bitrate(Bitrate::kbps(300), BandwidthLimitedCause::DelayBasedLimited);
            assert!(pc.handle_timeout(now).is_some());
            assert_eq!(pc.pending.len(), 1);
            pc.set_estimated_bitrate(Bitrate::kbps(300), cause);
            assert!(pc.handle_timeout(now).is_none());
            assert!(pc.pending.is_empty());
        }
    }

    #[test]
    fn deferred_probe_rechecks_shrinking_demand_instead_of_sending_stale_target() {
        let now = Instant::now();
        let mut pc = ProbeControl::new();
        pc.enable(true);
        pc.set_desired_bitrate(Bitrate::mbps(10));
        pc.set_estimated_bitrate(Bitrate::kbps(300), BandwidthLimitedCause::DelayBasedLimited);
        assert!(pc.handle_timeout(now).is_some());
        assert_eq!(
            pc.pending.front().unwrap().target_bitrate(),
            Bitrate::kbps(1800)
        );
        pc.set_desired_bitrate(Bitrate::kbps(200));
        if let Some(next) = pc.handle_timeout(now) {
            assert!(next.target_bitrate() <= Bitrate::kbps(400));
        }
        assert!(
            pc.pending
                .iter()
                .all(|p| p.target_bitrate() <= Bitrate::kbps(400))
        );
    }

    #[test]
    fn deferred_probe_older_than_five_seconds_cannot_retain_original_request() {
        let now = Instant::now();
        let mut pc = ProbeControl::new();
        pc.enable(true);
        pc.set_desired_bitrate(Bitrate::mbps(10));
        pc.set_estimated_bitrate(Bitrate::kbps(300), BandwidthLimitedCause::DelayBasedLimited);
        assert!(pc.handle_timeout(now).is_some());
        let stale_id = pc.pending.front().unwrap().cluster();
        let resumed = now + Duration::from_secs(5) + Duration::from_nanos(1);
        if let Some(next) = pc.handle_timeout(resumed) {
            assert_ne!(next.cluster(), stale_id);
            assert_eq!(next.requested_at(), Some(resumed));
        }
        assert!(pc.pending.iter().all(|p| p.cluster() != stale_id));
    }

    #[test]
    fn initial_exponential_probes_are_queued_and_emitted_one_per_tick() {
        let mut pc = ProbeControl::new();
        pc.enable(true);
        let now = Instant::now();

        pc.set_desired_bitrate(Bitrate::mbps(50));
        pc.set_estimated_bitrate(Bitrate::kbps(300), BandwidthLimitedCause::DelayBasedLimited);

        // First handle_timeout triggers initial probing and returns first probe.
        let p1 = pc.handle_timeout(now).unwrap();

        // poll_timeout returns already_happened while there are pending probes.
        assert_eq!(pc.poll_timeout(), already_happened());

        // Second handle_timeout returns the second queued probe.
        let p2 = pc.handle_timeout(now).unwrap();

        assert_eq!(p1.target_bitrate(), Bitrate::kbps(900));
        assert_eq!(p2.target_bitrate(), Bitrate::kbps(1800));
        assert_eq!(p1.min_packet_count(), 5);
        assert_eq!(p1.min_probe_delta(), Duration::from_millis(2));
        assert!(!p1.is_alr_probe());

        // Queue drained - no more probes.
        assert!(pc.handle_timeout(now).is_none());
    }

    #[test]
    fn further_probe_is_triggered_when_probe_result_is_high_enough() {
        let mut pc = ProbeControl::new();
        pc.enable(true);
        let now = Instant::now();

        pc.enable(true);
        pc.set_desired_bitrate(Bitrate::mbps(50));
        pc.set_estimated_bitrate(Bitrate::mbps(1), BandwidthLimitedCause::DelayBasedLimited);

        // Drain initial two probes.
        let _ = pc.handle_timeout(now).unwrap();
        let _ = pc.handle_timeout(now).unwrap();

        // WebRTC rule: if measured bitrate > min_bitrate_to_probe_further, probe at 2x measured.
        // min_bitrate_to_probe_further is 0.7 * last_probe_rate (6x start) = 4.2 Mbps.
        pc.set_estimated_bitrate(Bitrate::mbps(5), BandwidthLimitedCause::DelayBasedLimited);

        let p = pc.handle_timeout(now + Duration::from_millis(10)).unwrap();
        assert_eq!(p.target_bitrate(), Bitrate::mbps(10));
    }

    #[test]
    fn allocation_probe_is_triggered_in_alr_when_allocation_increases() {
        let mut pc = ProbeControl::new();
        pc.enable(true);
        let now = Instant::now();

        pc.set_desired_bitrate(Bitrate::mbps(1));
        pc.set_estimated_bitrate(Bitrate::mbps(1), BandwidthLimitedCause::DelayBasedLimited);

        // Drain initial probes.
        let _ = pc.handle_timeout(now).unwrap();
        let _ = pc.handle_timeout(now).unwrap();

        // Time out waiting for probing result -> probing complete.
        assert!(pc.handle_timeout(now + Duration::from_secs(2)).is_none());

        // Enter ALR
        pc.set_alr_start_time(now + Duration::from_secs(2));

        // No probe yet - desired hasn't increased
        assert!(pc.handle_timeout(now + Duration::from_secs(2)).is_none());

        // Increase desired bitrate while in ALR (desired > prev AND desired > estimate)
        pc.set_desired_bitrate(Bitrate::mbps(4));

        // Should trigger allocation probe: p1 = 4 Mbps * 1.0 = 4 Mbps, capped by 2× estimate = 2 Mbps
        let p = pc.handle_timeout(now + Duration::from_secs(2)).unwrap();
        assert_eq!(p.target_bitrate(), Bitrate::mbps(2));
    }

    #[test]
    fn handles_bitrate_infinity_without_panic() {
        let mut pc = ProbeControl::new();
        pc.enable(true);
        let now = Instant::now();

        pc.set_desired_bitrate(Bitrate::mbps(50));

        // Should not panic with Infinity
        pc.set_estimated_bitrate(Bitrate::INFINITY, BandwidthLimitedCause::DelayBasedLimited);

        // Verify behavior is reasonable (no probing with infinite estimate)
        assert!(pc.handle_timeout(now).is_none());
    }

    #[test]
    fn handles_clock_skew_gracefully() {
        let mut pc = ProbeControl::new();
        pc.enable(true);
        let now = Instant::now();

        pc.set_desired_bitrate(Bitrate::mbps(50));
        pc.set_estimated_bitrate(Bitrate::kbps(300), BandwidthLimitedCause::DelayBasedLimited);

        // Drain initial probes
        let _ = pc.handle_timeout(now);
        let _ = pc.handle_timeout(now);

        // Simulate time going backwards (clock skew)
        let earlier = now - Duration::from_secs(5);

        // Should handle gracefully with saturating_duration_since
        let _ = pc.handle_timeout(earlier);

        // Should still be able to continue normally
        let _ = pc.handle_timeout(now + Duration::from_secs(1));
    }

    #[test]
    fn handles_max_bitrate_zero() {
        let mut pc = ProbeControl::new();
        pc.enable(true);
        let now = Instant::now();

        // Set max_bitrate to zero - this is rejected as first value to avoid
        // creating probes with zero cap.
        pc.set_desired_bitrate(Bitrate::ZERO);
        pc.set_estimated_bitrate(Bitrate::kbps(300), BandwidthLimitedCause::DelayBasedLimited);

        // No probes should be created since desired was rejected.
        let p1 = pc.handle_timeout(now);
        assert!(p1.is_none(), "Should not create probes with zero desired");
    }

    #[test]
    fn allocation_probe_fires_when_desired_increases_in_alr() {
        let mut pc = ProbeControl::new();
        pc.enable(true);
        let now = Instant::now();

        pc.set_desired_bitrate(Bitrate::kbps(500));
        pc.set_estimated_bitrate(Bitrate::kbps(500), BandwidthLimitedCause::DelayBasedLimited);

        // Drain initial probes
        let _ = pc.handle_timeout(now);
        let _ = pc.handle_timeout(now);

        // Timeout to reach probing complete
        assert!(pc.handle_timeout(now + Duration::from_secs(2)).is_none());

        // Enter ALR
        pc.set_alr_start_time(now + Duration::from_secs(3));

        // No probe on ALR entry alone
        assert!(pc.handle_timeout(now + Duration::from_secs(3)).is_none());

        // Increase desired while in ALR (desired > prev AND desired > estimate)
        pc.set_desired_bitrate(Bitrate::mbps(4));

        // Should trigger allocation probe
        let probe = pc.handle_timeout(now + Duration::from_secs(3));
        assert!(
            probe.is_some(),
            "Allocation probe should trigger when desired increases in ALR"
        );
    }

    #[test]
    fn large_drop_probing_after_alr_ended() {
        let mut pc = ProbeControl::new();
        pc.enable(true);
        let now = Instant::now();

        pc.set_desired_bitrate(Bitrate::mbps(5));
        pc.set_estimated_bitrate(Bitrate::mbps(5), BandwidthLimitedCause::DelayBasedLimited);

        // Drain initial probes
        let _ = pc.handle_timeout(now);
        let _ = pc.handle_timeout(now);

        // Timeout to probing complete
        assert!(pc.handle_timeout(now + Duration::from_secs(2)).is_none());

        // Enter and exit ALR (large-drop works when ALR ended recently)
        pc.set_alr_start_time(now + Duration::from_secs(2));
        pc.set_alr_stop_time(now + Duration::from_secs(3));

        // Simulate large drop (to 60% of original = 3 Mbps, below 66% threshold)
        pc.set_estimated_bitrate(Bitrate::mbps(3), BandwidthLimitedCause::DelayBasedLimited);

        // Check at now+5s (within 3s of ALR ending, so alr_ended_recently is true)
        let later = now + Duration::from_secs(5);

        // Should trigger large-drop recovery probe at 85% of pre-drop rate (4.25 Mbps)
        let p = pc.handle_timeout(later);
        assert!(p.is_some(), "Large-drop recovery should schedule probe");
        if let Some(probe) = p {
            // 85% of 5 Mbps = 4.25 Mbps
            assert!(probe.target_bitrate() >= Bitrate::mbps(4));
            assert!(probe.target_bitrate() <= Bitrate::mbps(5));
        }
    }

    #[test]
    fn allocation_probe_requires_desired_increase_in_alr() {
        let mut pc = ProbeControl::new();
        pc.enable(true);
        let now = Instant::now();

        pc.set_desired_bitrate(Bitrate::mbps(5));
        pc.set_estimated_bitrate(Bitrate::mbps(1), BandwidthLimitedCause::DelayBasedLimited);

        // Drain initial probes
        let _ = pc.handle_timeout(now);
        let _ = pc.handle_timeout(now);

        // Timeout to probing complete
        assert!(pc.handle_timeout(now + Duration::from_secs(2)).is_none());

        // Enter ALR with estimate < max_bitrate
        pc.set_alr_start_time(now + Duration::from_secs(2));

        // No allocation probe on ALR entry - need desired to increase
        let probe = pc.handle_timeout(now + Duration::from_secs(2));
        assert!(
            probe.is_none(),
            "Should NOT trigger allocation probe on ALR entry alone"
        );

        // Increase desired while in ALR
        pc.set_desired_bitrate(Bitrate::mbps(10));

        // Now should trigger allocation probe
        let probe = pc.handle_timeout(now + Duration::from_secs(2));
        assert!(
            probe.is_some(),
            "Should trigger allocation probe when desired increases in ALR"
        );
    }

    #[test]
    fn managed_discovery_keeps_capability_estimates_and_explicit_validation() {
        let now = Instant::now();
        let mut pc = ProbeControl::new();
        assert!(pc.automatic_probing_enabled());
        pc.enable(true);
        pc.set_desired_bitrate(Bitrate::mbps(2));
        pc.set_estimated_bitrate(Bitrate::kbps(32), BandwidthLimitedCause::DelayBasedLimited);
        for (id, kind) in [
            (1, ProbeKind::Initial),
            (2, ProbeKind::Exponential),
            (3, ProbeKind::IncreaseAlr),
            (4, ProbeKind::PeriodicAlr),
            (5, ProbeKind::LargeDrop),
            (6, ProbeKind::Stagnant),
        ] {
            pc.pending
                .push_back(ProbeClusterConfig::new(id.into(), Bitrate::mbps(1), kind));
        }
        assert!(pc.set_automatic_probing(false));
        assert!(pc.pending.is_empty());
        assert!(pc.enabled);
        assert_eq!(pc.last_estimate, Some(Bitrate::kbps(32)));
        assert_eq!(pc.desired_bitrate, Some(Bitrate::mbps(2)));
        assert!(pc.poll_timeout() > now + Duration::from_secs(100));
        assert!(pc.handle_timeout(now + Duration::from_secs(10)).is_none());
        let validation = pc.validation_config(Bitrate::mbps(2), now).unwrap();
        assert_eq!(validation.kind(), ProbeKind::Validation);
        let identity = pc.next_cluster_id();
        assert!(!pc.set_automatic_probing(false));
        assert_eq!(pc.next_cluster_id(), identity);
        assert!(pc.set_automatic_probing(true));
        assert_eq!(pc.handle_timeout(now).unwrap().kind(), ProbeKind::Initial);
    }

    #[test]
    fn periodic_policy_removes_only_periodic_pending_and_preserves_validation_initial() {
        let now = Instant::now();
        let mut pc = ProbeControl::new();
        for (id, kind) in [
            (1, ProbeKind::Initial),
            (2, ProbeKind::PeriodicAlr),
            (3, ProbeKind::Validation),
        ] {
            pc.pending
                .push_back(ProbeClusterConfig::new(id.into(), Bitrate::mbps(1), kind));
        }
        pc.set_periodic_alr_probing(false, now);
        assert_eq!(pc.pending.len(), 2);
        assert_eq!(pc.pending[0].kind(), ProbeKind::Initial);
        assert_eq!(pc.pending[1].kind(), ProbeKind::Validation);
        assert!(!pc.periodic_alr_policy().0);
        // A deferred entry introduced before dispatch is rechecked too.
        pc.pending.push_front(ProbeClusterConfig::new(
            4.into(),
            Bitrate::mbps(1),
            ProbeKind::PeriodicAlr,
        ));
        pc.enable(true);
        pc.set_desired_bitrate(Bitrate::mbps(2));
        pc.set_estimated_bitrate(Bitrate::mbps(1), BandwidthLimitedCause::DelayBasedLimited);
        assert_eq!(pc.handle_timeout(now).unwrap().kind(), ProbeKind::Initial);
    }

    #[test]
    fn periodic_policy_disabled_timeout_does_not_spin_and_reenable_has_fresh_interval() {
        let now = Instant::now();
        let mut pc = ProbeControl::new();
        pc.enable(true);
        pc.set_desired_bitrate(Bitrate::mbps(2));
        pc.set_estimated_bitrate(Bitrate::mbps(1), BandwidthLimitedCause::DelayBasedLimited);
        pc.last_probe = Some(LastProbe {
            when: now,
            kind: ProbeKind::PeriodicAlr,
            further: Bitrate::mbps(2),
            was_estimate: Some(Bitrate::mbps(1)),
        });
        pc.set_alr_start_time(now);
        pc.set_periodic_alr_probing(false, now);
        let later = now + Duration::from_secs(30);
        assert!(pc.handle_timeout(later).is_none());
        assert!(pc.poll_timeout() > later);
        pc.set_periodic_alr_probing(true, later);
        assert!(pc.handle_timeout(later).is_none());
        pc.set_periodic_alr_probing(true, later + Duration::from_secs(4));
        assert!(pc.handle_timeout(later + Duration::from_secs(4)).is_none());
        assert_eq!(
            pc.handle_timeout(later + MIN_TIME_BETWEEN_ALR_PROBES)
                .unwrap()
                .kind(),
            ProbeKind::PeriodicAlr
        );
    }

    #[test]
    fn periodic_reenable_deadline_survives_estimator_reset_without_renewal() {
        let now = Instant::now();
        let mut original = ProbeControl::new();
        original.set_periodic_alr_probing(false, now);
        original.set_periodic_alr_probing(true, now + Duration::from_secs(1));
        let deadline = now + Duration::from_secs(6);
        let policy = original.periodic_alr_policy();
        assert_eq!(policy, (true, Some(deadline)));
        let mut restored = ProbeControl::new();
        restored.restore_periodic_alr_policy(policy);
        assert_eq!(restored.periodic_alr_policy(), policy);
        restored.last_probe = Some(LastProbe {
            when: now,
            kind: ProbeKind::PeriodicAlr,
            further: Bitrate::mbps(2),
            was_estimate: Some(Bitrate::mbps(1)),
        });
        restored.set_alr_start_time(now);
        restored.set_periodic_alr_probing(true, now + Duration::from_secs(2));
        assert_eq!(restored.periodic_alr_policy(), policy);
        assert!(!restored.maybe_periodic_alr(deadline - Duration::from_nanos(1), Bitrate::mbps(2)));
        assert!(restored.maybe_periodic_alr(deadline, Bitrate::mbps(2)));
        // Disabled policy also remains disabled, without manufacturing a deadline.
        restored.set_periodic_alr_probing(false, deadline);
        let mut disabled = ProbeControl::new();
        disabled.restore_periodic_alr_policy(restored.periodic_alr_policy());
        assert_eq!(disabled.periodic_alr_policy(), (false, None));
    }

    #[test]
    fn periodic_alr_probing() {
        let mut pc = ProbeControl::new();
        pc.enable(true);
        let now = Instant::now();

        pc.set_desired_bitrate(Bitrate::mbps(5));
        pc.set_estimated_bitrate(Bitrate::mbps(1), BandwidthLimitedCause::DelayBasedLimited);

        // Drain initial probes
        let _ = pc.handle_timeout(now);
        let _ = pc.handle_timeout(now);

        // Timeout to probing complete
        assert!(pc.handle_timeout(now + Duration::from_secs(2)).is_none());

        // Enter ALR
        pc.set_alr_start_time(now + Duration::from_secs(2));

        // No immediate probe on ALR entry
        assert!(pc.handle_timeout(now + Duration::from_secs(2)).is_none());

        // Wait 5 seconds for periodic probe (2s to complete initial + 5s = 7s)
        let probe = pc.handle_timeout(now + Duration::from_secs(7));
        assert!(
            probe.is_some(),
            "Should trigger periodic ALR probe after 5 seconds in ALR"
        );
        assert!(probe.unwrap().is_alr_probe());
    }

    #[test]
    fn periodic_alr_probing_continues_even_when_estimate_reaches_max() {
        let mut pc = ProbeControl::new();
        pc.enable(true);
        let now = Instant::now();

        pc.set_desired_bitrate(Bitrate::mbps(2));
        pc.set_estimated_bitrate(Bitrate::mbps(1), BandwidthLimitedCause::DelayBasedLimited);

        // Drain initial probes
        let _ = pc.handle_timeout(now);
        let _ = pc.handle_timeout(now);

        // Timeout to probing complete
        assert!(pc.handle_timeout(now + Duration::from_secs(2)).is_none());

        // Enter ALR
        pc.set_alr_start_time(now + Duration::from_secs(2));

        // No immediate probe on ALR entry
        assert!(pc.handle_timeout(now + Duration::from_secs(2)).is_none());

        // Now increase estimate to match max_bitrate
        pc.set_estimated_bitrate(Bitrate::mbps(2), BandwidthLimitedCause::DelayBasedLimited);

        // Wait 5 seconds - should still trigger periodic probe in ALR
        // even though estimate >= max_bitrate, to maintain confidence in the estimate
        let probe = pc.handle_timeout(now + Duration::from_secs(7));
        assert!(
            probe.is_some(),
            "Should continue periodic probing in ALR even when estimate >= max_bitrate"
        );
    }
}
