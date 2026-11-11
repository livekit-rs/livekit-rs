//! Subscriber-side admission around str0m's actual TWCC/sustained padding.
//! The managed server disables automatic short trains: unmet demand directly
//! authorizes one bounded validation, never an unmeasured media budget.
//!
//! This module never synthesizes media or treats a requested rate as measured
//! capacity. Only sustained native validation or receiver REMB reports can
//! increase an allocation budget; local GCC estimate events cannot grant it.
//! Probe admission is bounded by actual requested wire demand. It is an
//! allocation budget, not a claim about the link's sustained physical capacity.
//! A proven allocation budget is distinct from application-limited GCC rate.
//! Failed discovery backs off 1.5x; requested padding never grants capacity.
use std::time::{Duration, Instant};
use str0m::bwe::{BweEstimateCause, BweSnapshot, BweValidationStatus};

const BASE_INTERVAL: Duration = Duration::from_secs(3);
const MAX_INTERVAL: Duration = Duration::from_secs(120);
// Preserve an unmet allocation goal independently of the native validation's
// shorter immutable deadline; failure backoff still bounds every new request.
const PROBE_WINDOW: Duration = Duration::from_secs(18);
const STALE_AFTER: Duration = Duration::from_secs(10);
const RECOVERY_WAIT: Duration = Duration::from_secs(3);
const PAUSED_RESET_AFTER: Duration = Duration::from_secs(5);
const MIN_CAPACITY: u64 = 32_000;
const VERIFIED_FOR: Duration = Duration::from_secs(60);
// Native validation sends for at least one real second and has a fixed
// three-second request-to-feedback deadline. Neither polling nor another short
// discovery train may renew that authorization.
const VALIDATION_WINDOW: Duration = Duration::from_secs(3);
const VALIDATION_SPAN: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug)]
struct Probe {
    target: u64,
    required_bps: u64,
    until: Instant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DiscoveryGoal {
    NextLayer,
    TotalDemand,
}

#[derive(Clone, Copy, Debug)]
struct Validation {
    goal: DiscoveryGoal,
    id: Option<u64>,
    target_bps: u64,
    requested_at: Instant,
    until: Instant,
}

#[derive(Clone, Copy, Debug)]
struct NativeSafetyReduction {
    receipt: Instant,
    raw_target: u64,
    // Same-batch qualified delivery; never recomputed from later ALR usage.
    budget: u64,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ProbeCounters {
    pub attempts: u64,
    pub successes: u64,
    pub failures: u64,
    pub congestion_events: u64,
    pub stale_feedback_events: u64,
    pub paused_resets: u64,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ProbeAction {
    /// Passed to str0m's built-in pacer/prober; never used as capacity.
    pub desired_bps: u64,
    /// Actual capped demand is uncovered; diagnostic only. Explicit sustained
    /// validation is the sole discovery owner in the managed server profile.
    pub discovery_needed: bool,
    pub reset_bwe_to: Option<u64>,
    pub start_validation_bps: Option<u64>,
    pub cancel_validation: bool,
}

#[derive(Debug)]
pub(crate) struct ProbeController {
    can_probe: bool,
    discovery_goal: DiscoveryGoal,
    upgrade_goal: Option<u64>,
    demand_bps: u64,
    admission_hard_limit: Option<u64>,
    admitted_probe: Option<(u64, Instant)>,
    validation: Option<Validation>,
    cancel_validation_pending: bool,
    estimate: Option<u64>,
    feedback_at: Instant,
    rtcp_feedback_at: Option<Instant>,
    committed_capacity: u64,
    verified_at: Instant,
    last_twcc_at: Option<Instant>,
    last_stats_twcc_at: Option<Instant>,
    last_matched_raw: Option<u64>,
    matched_rtt: Option<(Instant, Duration)>,
    pending_loss_receipt: Option<Instant>,
    pending_safety_native_reduction: Option<NativeSafetyReduction>,
    congestion_at: Option<Instant>,
    min_rtt: Option<Duration>,
    safety_congested: bool,
    last_bad_safety_twcc_at: Option<Instant>,
    next_probe_at: Instant,
    reset_allowed_at: Instant,
    interval: Duration,
    probe: Option<Probe>,
    last_adjustment: Instant,
    last_growth_at: Instant,
    last_reset_at: Option<Instant>,
    pub counters: ProbeCounters,
}

impl ProbeController {
    pub fn new(now: Instant) -> Self {
        Self {
            can_probe: false,
            discovery_goal: DiscoveryGoal::NextLayer,
            upgrade_goal: None,
            demand_bps: 0,
            admission_hard_limit: None,
            admitted_probe: None,
            validation: None,
            cancel_validation_pending: false,
            estimate: None,
            feedback_at: now,
            rtcp_feedback_at: None,
            committed_capacity: crate::allocator::INITIAL_CAPACITY_BPS,
            verified_at: now,
            last_twcc_at: None,
            last_stats_twcc_at: None,
            last_matched_raw: None,
            matched_rtt: None,
            pending_loss_receipt: None,
            pending_safety_native_reduction: None,
            congestion_at: None,
            min_rtt: None,
            safety_congested: false,
            last_bad_safety_twcc_at: None,
            next_probe_at: now + BASE_INTERVAL,
            reset_allowed_at: now + BASE_INTERVAL,
            interval: BASE_INTERVAL,
            probe: None,
            last_adjustment: now,
            last_growth_at: now,
            last_reset_at: None,
            counters: ProbeCounters::default(),
        }
    }

    pub fn set_upgrade_goal(&mut self, goal: Option<u64>) {
        self.upgrade_goal = goal;
    }

    /// Apply this allocation cycle's explicit controls before consuming a
    /// validation result. Layer-cost noise must not restart an in-flight train.
    pub fn prepare_admission(
        &mut self,
        demand_bps: u64,
        goal: Option<u64>,
        hard_limit: Option<u64>,
        connected: bool,
    ) {
        self.demand_bps = demand_bps.min(hard_limit.unwrap_or(u64::MAX));
        self.upgrade_goal = goal;
        self.admission_hard_limit = hard_limit;
        if !connected
            || self.demand_bps == 0
            || self.demand_bps <= self.committed_capacity
            || self
                .validation
                .is_some_and(|v| hard_limit.is_some_and(|limit| v.target_bps > limit))
        {
            self.cancel_validation();
        }
    }

    fn probe_target(&self, capacity: u64, desired_bps: u64) -> u64 {
        // Return immutable proof/admission demand P. The explicit dual API
        // independently selects bounded sending strength Q; Q is never budget.
        // Managed transports keep ordinary automatic short probes disabled.
        match self.discovery_goal {
            DiscoveryGoal::NextLayer => self.upgrade_goal.unwrap_or(desired_bps),
            DiscoveryGoal::TotalDemand => desired_bps,
        }
        .max(capacity)
    }

    // Total current demand is the ceiling for a completed immutable proof.
    // A temporary allocator rollback to an intermediate layer must not discard
    // a higher target that the same validation actually measured successfully.
    fn admission_limit(&self) -> u64 {
        self.demand_bps
            .min(self.admission_hard_limit.unwrap_or(u64::MAX))
    }

    // New requests still discover only the next missing layer, bounded by the
    // actual total demand and any explicit hard cap.
    fn validation_target(&self) -> u64 {
        match self.discovery_goal {
            DiscoveryGoal::NextLayer => self.upgrade_goal.unwrap_or(self.demand_bps),
            DiscoveryGoal::TotalDemand => self.demand_bps,
        }
        .min(self.admission_limit())
    }

    pub fn on_estimate(&mut self, estimate: u64, can_probe: bool, now: Instant) {
        self.can_probe = can_probe;
        self.estimate = Some(estimate);
        if !can_probe {
            self.probe = None;
            self.cancel_validation();
        }
        // Configuration/capability and timer-produced estimates are not feedback.
        let _ = now;
    }

    /// A receiver-reported REMB is a direct observation, unlike a local GCC tick.
    pub fn on_remb(&mut self, estimate: u64, now: Instant) {
        self.on_estimate(estimate, false, now);
        self.committed_capacity = estimate;
        self.feedback_at = now;
        self.verified_at = now;
    }

    /// An expired safety observation is unknown, not clean. Its memory remains
    /// set until real clean stats arrive; expiry only permits bounded paused
    /// discovery and stops replaying that old observation as new congestion.
    fn safety_episode_expired(&self, now: Instant) -> bool {
        self.safety_congested
            && self
                .last_bad_safety_twcc_at
                .is_some_and(|at| at <= now && now.saturating_duration_since(at) >= STALE_AFTER)
    }

    /// Consume actual native feedback without confusing a scheduled probe with
    /// its measured result. Repeated snapshots cannot refresh or reduce budgets.
    pub fn observe_native(&mut self, snapshot: BweSnapshot, now: Instant) {
        let Some(at) = snapshot.last_twcc_feedback_at.filter(|at| *at <= now) else {
            return;
        };
        if self.last_twcc_at.is_some_and(|previous| at <= previous) {
            return;
        }
        self.last_twcc_at = Some(at);
        self.feedback_at = at;
        self.rtcp_feedback_at = Some(at);
        let raw = snapshot.raw_estimate.map(|rate| rate.as_u64());
        // Periodic estimate callbacks can run before this snapshot; compare
        // only independently identified matched-feedback samples.
        let raw_reduced = raw
            .zip(self.last_matched_raw)
            .is_some_and(|(current, previous)| current < previous);
        self.last_matched_raw = raw;
        self.matched_rtt = snapshot
            .latest_feedback
            .filter(|feedback| feedback.at == at)
            .and_then(|feedback| feedback.min_propagation_rtt.map(|rtt| (at, rtt)));
        if let Some(raw) = raw {
            self.estimate = Some(raw);
        }
        // Aggregate TWCC loss/RTT can include packets sent before this
        // validation. Only its native id-scoped result may attribute those
        // observations to this train; global delay/RTCP safety still applies.
        // LossLimited is a persistent model state, not a new loss event for
        // every TWCC report. A constant historical target plus low ALR audio
        // throughput must not renew congestion or suppress rediscovery forever.
        // raw < committed also retains a real reduction that first arrives
        // inside the one-second adjustment gate: commit it once the gate opens.
        let native_loss_reduction = snapshot.cause == BweEstimateCause::LossLimited
            && raw.is_some_and(|rate| rate < self.committed_capacity);
        let safety_active = self.safety_congested && !self.safety_episode_expired(now);
        let current_matched_loss = snapshot
            .latest_feedback
            .is_some_and(|feedback| feedback.at == at && feedback.lost_packets > 0);
        let delivery_eligible = !snapshot.application_limited || current_matched_loss;
        if safety_active && raw_reduced {
            self.pending_safety_native_reduction = raw.map(|target| {
                let observed = if delivery_eligible {
                    snapshot
                        .acknowledged_bitrate
                        .map_or(target, |acked| target.min(acked.as_u64()))
                } else {
                    target
                };
                let candidate = NativeSafetyReduction {
                    receipt: at,
                    raw_target: target,
                    budget: observed,
                };
                self.pending_safety_native_reduction
                    .filter(|old| {
                        now.saturating_duration_since(old.receipt) < Duration::from_secs(2)
                            && old.budget <= candidate.budget
                            && target <= old.raw_target
                    })
                    // Retaining old delivery means retaining its complete
                    // identity and expiry; no new raw drop renews old ACKs.
                    .unwrap_or(candidate)
            });
        }
        let safety_native_reduction = safety_active
            && self.pending_safety_native_reduction.is_some_and(|pending| {
                pending.receipt <= at
                    && now.saturating_duration_since(pending.receipt) < Duration::from_secs(2)
                    && raw.is_some_and(|rate| rate <= pending.raw_target)
            });

        // A real rolling bad-loss receipt may authorize one later native loss
        // delivery bound. Queue-only history and a new Normal/ALR report do
        // not acquire this permission merely by receiving another TWCC batch.
        let loss_delivery_receipt = matches!(
            snapshot.cause,
            BweEstimateCause::LossLimited | BweEstimateCause::LossRecovering
        ) && delivery_eligible
            && self.pending_loss_receipt.is_some_and(|receipt| {
                receipt <= at && now.saturating_duration_since(receipt) < Duration::from_secs(2)
            });
        let explicit_congestion = snapshot.cause == BweEstimateCause::DelayOveruse
            || safety_native_reduction
            || loss_delivery_receipt;
        let congested = explicit_congestion || native_loss_reduction;
        if congested {
            // Cancel real padding immediately even if the existing one-second
            // capacity-adjustment gate delays committing a native reduction.
            if self.validation.is_some() {
                self.fail_validation(now);
            } else {
                self.cancel_validation();
            }
            if now.saturating_duration_since(self.last_adjustment) >= Duration::from_secs(1) {
                // Native GCC has already applied delay/loss backoff. Applying
                // another multiplicative cut on each snapshot compounds one
                // congestion episode below even the native safe sending rate.
                let native = self.estimate.unwrap_or(self.committed_capacity);
                // Native raw always constrains congestion. A delayed reduction
                // uses its own receipt's budget, not the next report's ACK.
                let mut budget = native;
                if safety_native_reduction {
                    budget = budget.min(self.pending_safety_native_reduction.unwrap().budget);
                }
                if delivery_eligible
                    && (snapshot.cause == BweEstimateCause::DelayOveruse || loss_delivery_receipt)
                {
                    budget = snapshot
                        .acknowledged_bitrate
                        .map_or(budget, |acked| budget.min(acked.as_u64()));
                }
                if budget < self.committed_capacity || self.probe.is_some() {
                    if budget < self.committed_capacity {
                        // One successful native reduction processes prior loss
                        // authority from this episode as well; it cannot be
                        // replayed as a separate ALR-delivery permission.
                        self.pending_loss_receipt = None;
                        self.pending_safety_native_reduction = None;
                    }
                    self.congest(budget, now);
                }
            }
            return;
        }
        if safety_active {
            // Historical safety can block discovery, but is not a fresh
            // throughput measurement that cuts a stable budget to ALR usage.
            if self.validation.is_some() {
                self.fail_validation(now);
            } else {
                self.cancel_validation();
            }
            return;
        }
        // Ordinary short probe observations remain diagnostics only. Managed
        // discovery does not need a short train before its sustained proof.
        // A room driver stall must not consume a cached terminal result as a
        // new allocation authorization after this request's fixed deadline.
        if self.validation.is_some_and(|active| now >= active.until) {
            self.fail_validation(now);
        }
        if let (Some(active), Some(validation)) = (self.validation, snapshot.last_validation)
            && active.id == Some(validation.id)
            && validation.requested_at == active.requested_at
            && validation.at <= at
            && validation.at <= active.until
        {
            if validation.lost_packets > 0 {
                self.fail_validation(now);
                return;
            }
            match validation.status {
                BweValidationStatus::Passed => {
                    let proof = validation.selected_proof;
                    let measured = proof.map(|proof| proof.measured_bitrate.as_u64());
                    let valid = proof.is_some_and(|proof| {
                        proof.send_span >= VALIDATION_SPAN
                            && proof.recv_span >= VALIDATION_SPAN
                            && proof.received_packets >= 2
                            && proof.send_bytes > 0
                            && proof.recv_bytes > 0
                            && proof.started_at >= active.requested_at
                            && self.congestion_at.is_none_or(|at| proof.started_at > at)
                    }) && validation.lost_packets == 0
                        && validation.pacing_bitrate.as_f64().is_finite()
                        && validation.pacing_bitrate.as_f64() >= validation.target_bitrate.as_f64()
                        && validation.pacing_bitrate.as_f64()
                            <= validation.target_bitrate.as_f64() * 1.2
                        && validation.target_bitrate.as_u64() == active.target_bps
                        && validation.started_at.is_some_and(|started| {
                            started >= active.requested_at
                                && self.congestion_at.is_none_or(|at| started > at)
                        })
                        && measured.is_some_and(|rate| rate >= active.target_bps);
                    if valid {
                        let admitted = measured
                            .unwrap()
                            .min(self.admission_limit())
                            .min(active.target_bps);
                        self.admitted_probe = Some((admitted, validation.at));
                        if admitted > self.committed_capacity {
                            self.committed_capacity = admitted;
                            self.last_growth_at = at;
                        }
                        if admitted >= self.committed_capacity {
                            self.verified_at = at;
                        }
                        self.validation = None;
                        // Only this actual completed immutable attempt can
                        // authorize trying the requested full quality next.
                        // A partial admission after total-demand shrink cannot.
                        if active.goal == DiscoveryGoal::NextLayer
                            && admitted >= active.target_bps
                            && self.demand_bps > self.committed_capacity
                        {
                            self.discovery_goal = DiscoveryGoal::TotalDemand;
                        }
                        if let Some(probe) = self.probe
                            && now <= probe.until
                            && admitted >= probe.required_bps
                        {
                            self.counters.successes += 1;
                            self.probe = None;
                            self.interval = BASE_INTERVAL;
                            self.next_probe_at = now + self.interval;
                        }
                    } else {
                        self.fail_validation(now);
                    }
                }
                BweValidationStatus::Failed | BweValidationStatus::Cancelled => {
                    self.fail_validation(now);
                }
                _ => {}
            }
        }
        // Acknowledged traffic covering 80% revalidates an existing budget;
        // this existing allocation tolerance permits budget / ACK up to 1.25,
        // not a claim of physical-capacity accuracy. Low-rate ALR feedback and
        // local estimate events cannot keep an uncovered budget forever.
        if snapshot
            .acknowledged_bitrate
            .is_some_and(|rate| rate.as_u64() >= self.committed_capacity.saturating_mul(80) / 100)
        {
            self.verified_at = at;
        }
        if now.saturating_duration_since(self.verified_at) >= VERIFIED_FOR {
            // Retire an old high-water mark using the newest still-fresh
            // admitted proof if available. ALR's audio-only local rate must not
            // replace a recent, real lower-bandwidth probe with mere usage.
            let observed = self
                .admitted_probe
                .filter(|(_, at)| now.saturating_duration_since(*at) < STALE_AFTER)
                .map(|(rate, _)| rate.min(self.admission_limit()))
                .unwrap_or_else(|| self.estimate.unwrap_or(MIN_CAPACITY));
            self.committed_capacity = self.committed_capacity.min(observed);
            self.verified_at = now;
        }
    }

    /// Native control still awaiting the allocation/RTC action consumer.
    /// Read-only level: only tick may consume it, including cached snapshots.
    pub(crate) fn has_pending_control(&self) -> bool {
        self.cancel_validation_pending
    }

    fn cancel_validation(&mut self) {
        self.discovery_goal = DiscoveryGoal::NextLayer;
        self.cancel_validation_pending |= self.validation.take().is_some();
    }

    fn fail_validation(&mut self, now: Instant) {
        let active = self.validation.is_some() || self.probe.is_some();
        self.cancel_validation();
        self.probe = None;
        if active {
            self.counters.failures += 1;
            self.interval = self.interval.mul_f32(1.5).min(MAX_INTERVAL);
            self.next_probe_at = now + self.interval;
            self.reset_allowed_at = self.next_probe_at;
        }
    }

    /// Bind the native request identity returned by the real padding scheduler.
    pub fn validation_started(&mut self, id: Option<u64>, now: Instant) {
        if let (Some(active), Some(id)) = (self.validation.as_mut(), id) {
            active.id = Some(id);
        } else if self.validation.is_some() {
            self.fail_validation(now);
        }
    }

    /// Transport-wide reports, once per stats interval (not once per track).
    pub fn on_feedback(
        &mut self,
        loss: Option<f32>,
        _rtt: Option<Duration>,
        _media_bytes: u64,
        now: Instant,
    ) -> bool {
        // RTT snapshots may retain an old sample indefinitely. Only a new,
        // recent native TWCC receipt authorizes these safety observations.
        let Some(at) = self
            .last_twcc_at
            .filter(|at| now.saturating_duration_since(*at) < Duration::from_secs(2))
        else {
            return false;
        };
        if self.last_stats_twcc_at == Some(at) {
            return false;
        }
        self.last_stats_twcc_at = Some(at);
        let rtt = self
            .matched_rtt
            .filter(|(receipt, _)| *receipt == at)
            .map(|(_, rtt)| rtt);
        // Unknown RTT is neither a new queue event nor evidence of recovery.
        let queued = rtt.map(|current| {
            self.min_rtt.is_some_and(|base| {
                current > base + Duration::from_millis(100) && current > base.mul_f32(1.5)
            })
        });
        if let Some(current) = rtt {
            self.min_rtt = Some(self.min_rtt.map_or(current, |old| old.min(current)));
        }
        let bad_loss = loss.is_some_and(|loss| loss.is_finite() && loss >= 0.10);
        if bad_loss {
            self.pending_loss_receipt = Some(at);
        }
        if bad_loss || queued == Some(true) {
            // Safety backoff is once per actual loss/queueing episode, not
            // current_budget * 0.8 every stats tick while the same RTT persists.
            let first = !self.safety_congested || self.safety_episode_expired(now);
            self.safety_congested = true;
            self.last_bad_safety_twcc_at = Some(at);
            if self.validation.is_some() {
                self.fail_validation(now);
            }
            if first
                && now.saturating_duration_since(self.last_adjustment) >= Duration::from_secs(1)
            {
                let capacity = self.capacity(self.estimate).unwrap_or(MIN_CAPACITY);
                self.congest(capacity.saturating_mul(80) / 100, now);
                return true;
            }
        } else if loss.is_some_and(|loss| loss.is_finite() && loss <= 0.02) && queued == Some(false)
        {
            self.safety_congested = false;
            self.pending_loss_receipt = None;
            self.pending_safety_native_reduction = None;
        }
        false
    }

    fn congest(&mut self, capacity: u64, now: Instant) {
        self.counters.congestion_events += 1;
        self.committed_capacity = self.committed_capacity.min(capacity);
        self.admitted_probe = None;
        self.cancel_validation();
        self.congestion_at = Some(now);
        self.last_adjustment = now;
        if self.probe.take().is_some() {
            self.counters.failures += 1;
            self.interval = self.interval.mul_f32(1.5).min(MAX_INTERVAL);
        }
        self.next_probe_at = now + self.interval;
        self.reset_allowed_at = self.next_probe_at;
    }

    pub fn capacity(&self, _estimate: Option<u64>) -> Option<u64> {
        Some(self.committed_capacity)
    }

    /// `desired_bps` is demand before allocation; `allocated_bps` is committed
    /// media cost. A hard override caps *both* media allocation and probe demand.
    pub fn tick(
        &mut self,
        now: Instant,
        desired_bps: u64,
        allocated_bps: u64,
        hard_limit: Option<u64>,
        connected: bool,
        all_video_paused: bool,
    ) -> ProbeAction {
        let desired_bps = desired_bps.min(hard_limit.unwrap_or(u64::MAX));
        self.demand_bps = desired_bps;
        self.admission_hard_limit = hard_limit;
        if !connected || desired_bps == 0 {
            self.probe = None;
            self.cancel_validation();
            return ProbeAction {
                desired_bps: 0,
                discovery_needed: false,
                reset_bwe_to: None,
                start_validation_bps: None,
                cancel_validation: std::mem::take(&mut self.cancel_validation_pending),
            };
        }
        if self.validation.is_some_and(|v| now >= v.until) {
            self.fail_validation(now);
        }
        if self
            .validation
            .is_some_and(|v| hard_limit.is_some_and(|limit| v.target_bps > limit))
        {
            self.cancel_validation();
        }
        if now.saturating_duration_since(self.feedback_at) >= STALE_AFTER
            && now.saturating_duration_since(self.last_adjustment) >= Duration::from_secs(1)
        {
            let previous = self.capacity(self.estimate).unwrap_or(MIN_CAPACITY);
            self.committed_capacity =
                (previous.saturating_mul(80) / 100).max(previous.min(MIN_CAPACITY));
            self.last_adjustment = now;
            if !all_video_paused {
                self.probe = None;
                self.cancel_validation();
                self.next_probe_at = now + self.interval;
            }
            // With video entirely paused and no audio there may be no packet
            // to elicit fresh feedback. Keep the existing bounded discovery
            // deadline/backoff while still reducing this stale media budget.
            self.counters.stale_feedback_events += 1;
        }
        let capacity = self.capacity(self.estimate).unwrap_or(allocated_bps);
        let deficient = desired_bps > allocated_bps && desired_bps > capacity;
        if !deficient {
            // Demand can shrink while native discovery is still pending. Stop
            // requesting the obsolete rate without claiming a measured success,
            // changing the budget, or bypassing an existing failure backoff.
            self.probe = None;
            self.cancel_validation();
        }
        if self.probe.is_some_and(|p| now >= p.until) {
            self.cancel_validation();
            self.probe = None;
            self.counters.failures += 1;
            self.interval = self.interval.mul_f32(1.5).min(MAX_INTERVAL);
            self.next_probe_at = now + self.interval;
            self.reset_allowed_at = self.next_probe_at;
        }
        let feedback_fresh = now.saturating_duration_since(self.feedback_at) < STALE_AFTER;
        if self.can_probe
            && deficient
            && (feedback_fresh || all_video_paused)
            && self.probe.is_none()
            && now >= self.next_probe_at
        {
            let target = self.probe_target(capacity, desired_bps);
            self.probe = Some(Probe {
                target,
                required_bps: target,
                until: now + PROBE_WINDOW,
            });
            self.counters.attempts += 1;
        }
        let reset_due = self
            .last_reset_at
            .is_none_or(|t| now.saturating_duration_since(t) >= STALE_AFTER);
        let reset_bwe_to = if self.can_probe
            && self.validation.is_none()
            && deficient
            && reset_due
            && now >= self.reset_allowed_at
            && (all_video_paused
                || self
                    .rtcp_feedback_at
                    .is_some_and(|at| now.saturating_duration_since(at) < STALE_AFTER))
            && now.saturating_duration_since(self.last_growth_at) >= PAUSED_RESET_AFTER
            && self
                .congestion_at
                .is_none_or(|at| now.saturating_duration_since(at) >= RECOVERY_WAIT)
        {
            // Audio may keep flowing while every video layer is paused or an
            // upgrade is stuck. Restart native initial/exponential discovery
            // after a clean, stagnant interval, at or below verified capacity.
            // The reset is never treated as proof of increased bandwidth.
            self.last_reset_at = Some(now);
            self.counters.paused_resets += 1;
            if self.probe.is_none() {
                self.probe = Some(Probe {
                    target: self.probe_target(capacity, desired_bps),
                    required_bps: self.probe_target(capacity, desired_bps),
                    until: now + PROBE_WINDOW,
                });
                self.counters.attempts += 1;
            }
            let initial = capacity
                .clamp(MIN_CAPACITY, 300_000)
                .min(hard_limit.unwrap_or(u64::MAX));
            // Reset emits its initial estimate before new feedback. Compare
            // subsequent reports against that reset baseline, not the old GCC value.
            self.estimate = Some(initial);
            Some(initial)
        } else {
            None
        };
        let start_validation_bps = if self.can_probe
            && deficient
            // The native cancellation must be applied before authorizing a
            // replacement request, even when a hard cap still leaves demand.
            && !self.cancel_validation_pending
            && self.validation.is_none()
            && self.probe.is_some()
            && (!self.safety_congested || (all_video_paused && self.safety_episode_expired(now)))
            && now >= self.next_probe_at
            && (self
                .last_twcc_at
                .is_some_and(|at| at <= now && now.saturating_duration_since(at) < STALE_AFTER)
                || all_video_paused)
        {
            let target_bps = self.validation_target();
            self.validation = Some(Validation {
                goal: self.discovery_goal,
                id: None,
                target_bps,
                requested_at: now,
                until: now + VALIDATION_WINDOW,
            });
            Some(target_bps)
        } else {
            None
        };
        ProbeAction {
            desired_bps: self
                .validation
                .map(|v| v.target_bps)
                .unwrap_or_else(|| self.probe.map_or(desired_bps.min(capacity), |p| p.target))
                .max(reset_bwe_to.unwrap_or(0))
                .min(hard_limit.unwrap_or(u64::MAX)),
            discovery_needed: desired_bps > capacity,
            reset_bwe_to,
            start_validation_bps,
            cancel_validation: std::mem::take(&mut self.cancel_validation_pending),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use str0m::bwe::{Bitrate, BweProbeObservation, BweValidationObservation, ProbeKind};

    fn snapshot(at: Instant, raw: u64) -> BweSnapshot {
        BweSnapshot {
            raw_estimate: Some(Bitrate::bps(raw)),
            acknowledged_bitrate: Some(Bitrate::bps(raw)),
            cause: BweEstimateCause::Normal,
            last_twcc_feedback_at: Some(at),
            latest_feedback: Some(str0m::bwe::BweFeedbackObservation {
                at,
                received_packets: 10,
                lost_packets: 0,
                max_rtt: Some(Duration::from_millis(20)),
                min_propagation_rtt: Some(Duration::from_millis(20)),
            }),
            last_successful_probe: None,
            last_validation: None,
            latest_scheduled_probe: None,
            application_limited: false,
            ack_fast_rate_changes: 0,
        }
    }
    fn controller(now: Instant, capacity: u64) -> ProbeController {
        let mut c = ProbeController::new(now);
        c.committed_capacity = capacity;
        c.on_estimate(capacity, true, now);
        c.observe_native(snapshot(now, capacity), now);
        c
    }
    fn action(c: &mut ProbeController, at: Instant) -> ProbeAction {
        c.tick(at, 1_000_000, 150_000, None, true, false)
    }
    fn proof(at: Instant, rate: u64, id: u64) -> BweProbeObservation {
        BweProbeObservation {
            at,
            started_at: Some(at),
            bitrate: Bitrate::bps(rate),
            cluster_id: id,
            kind: ProbeKind::Initial,
        }
    }

    fn sustained_proof(
        at: Instant,
        requested_at: Instant,
        target: u64,
        measured: u64,
        id: u64,
    ) -> BweValidationObservation {
        BweValidationObservation {
            request_totals: Default::default(),
            measurement_started_at: Some(requested_at),
            measurement_segments: 1,
            cancelled_local_credit: Duration::ZERO,
            last_credit_discard_at: None,
            id,
            at,
            requested_at,
            started_at: Some(requested_at),
            target_bitrate: Bitrate::bps(target),
            pacing_bitrate: Bitrate::bps(target),
            selected_proof: Some(str0m::bwe::BweValidationProof {
                scope: str0m::bwe::BweValidationProofScope::CurrentSegment,
                started_at: requested_at,
                send_span: VALIDATION_SPAN,
                recv_span: VALIDATION_SPAN,
                send_bytes: measured / 8,
                recv_bytes: measured / 8,
                received_packets: 100,
                measured_bitrate: Bitrate::bps(measured),
                measured_payload_bitrate: Bitrate::bps(measured),
            }),
            status: BweValidationStatus::Passed,
            send_span: VALIDATION_SPAN,
            recv_span: VALIDATION_SPAN,
            send_bytes: measured / 8,
            recv_bytes: measured / 8,
            received_packets: 100,
            lost_packets: 0,
            measured_bitrate: Some(Bitrate::bps(measured)),
        }
    }

    fn validating(now: Instant) -> (ProbeController, Instant) {
        let mut c = controller(now, 400_000);
        let requested = now + BASE_INTERVAL;
        assert_eq!(
            action(&mut c, requested).start_validation_bps,
            Some(1_000_000)
        );
        c.validation_started(Some(7), requested);
        (c, requested)
    }

    #[test]
    fn fresh_feedback_starts_direct_validation_without_any_short_probe_proof() {
        let now = Instant::now();
        let mut c = controller(now, 400_000);
        c.set_upgrade_goal(Some(704_445));
        let at = now + BASE_INTERVAL;
        let result = c.tick(at, 1_000_000, 150_000, None, true, false);
        assert_eq!(result.start_validation_bps, Some(704_445));
        assert_eq!(c.capacity(None), Some(400_000));
        assert_eq!(c.counters.successes, 0);
        assert_eq!(c.counters.attempts, 1);
        assert_eq!(c.last_twcc_at, Some(now));
    }

    #[test]
    fn initialized_feedback_time_local_estimates_and_cached_rtt_cannot_start_direct_validation() {
        let now = Instant::now();
        let mut c = ProbeController::new(now);
        c.committed_capacity = 400_000;
        c.on_estimate(400_000, true, now);
        c.on_feedback(None, Some(Duration::from_millis(10)), 0, now);
        let at = now + BASE_INTERVAL;
        assert!(action(&mut c, at).start_validation_bps.is_none());
        assert_eq!(c.last_twcc_at, None);
        assert!(c.validation.is_none());
        let received = at + Duration::from_millis(10);
        c.observe_native(snapshot(received, 100_000), received);
        assert_eq!(
            action(&mut c, received).start_validation_bps,
            Some(1_000_000)
        );
        assert_eq!(c.capacity(None), Some(400_000));
    }

    #[test]
    fn stale_feedback_cannot_start_unpaused_direct_validation() {
        let now = Instant::now();
        let mut c = controller(now, 400_000);
        let at = now + STALE_AFTER;
        let result = c.tick(at, 1_000_000, 150_000, None, true, false);
        assert!(result.start_validation_bps.is_none());
        assert!(c.validation.is_none());
        assert!(c.capacity(None).unwrap() < 400_000);
        assert_eq!(c.last_twcc_at, Some(now));
    }

    #[test]
    fn silent_video_only_pause_can_explore_boundedly_without_fabricating_feedback_or_budget() {
        let now = Instant::now();
        let mut c = ProbeController::new(now);
        c.committed_capacity = MIN_CAPACITY;
        c.on_estimate(MIN_CAPACITY, true, now);
        let requested = now + BASE_INTERVAL;
        let result = c.tick(requested, 1_000_000, 0, None, true, true);
        assert_eq!(result.start_validation_bps, Some(1_000_000));
        assert_eq!(c.last_twcc_at, None);
        c.validation_started(Some(7), requested);
        let until = requested + VALIDATION_WINDOW;
        let result = c.tick(until, 1_000_000, 0, None, true, true);
        assert!(result.cancel_validation);
        assert!(result.start_validation_bps.is_none());
        assert_eq!(c.counters.failures, 1);
        assert_eq!(c.next_probe_at, until + BASE_INTERVAL.mul_f32(1.5));
        let retry = c.next_probe_at;
        assert!(
            c.tick(
                retry - Duration::from_millis(1),
                1_000_000,
                0,
                None,
                true,
                true
            )
            .start_validation_bps
            .is_none()
        );
        let result = c.tick(retry, 1_000_000, 0, None, true, true);
        assert_eq!(result.start_validation_bps, Some(1_000_000));
        c.validation_started(Some(8), retry);
        for elapsed in 1..3 {
            let at = retry + Duration::from_secs(elapsed);
            let result = c.tick(at, 1_000_000, 0, None, true, true);
            assert!(result.start_validation_bps.is_none());
            assert!(!result.cancel_validation);
            assert_eq!(c.validation.unwrap().until, retry + VALIDATION_WINDOW);
        }
        let result = c.tick(retry + VALIDATION_WINDOW, 1_000_000, 0, None, true, true);
        assert!(result.cancel_validation);
        assert_eq!(c.counters.failures, 2);
        assert_eq!(c.capacity(None), Some(MIN_CAPACITY));
        assert_eq!(c.last_twcc_at, None);
        assert_eq!(c.counters.successes, 0);
        assert_eq!(c.verified_at, now);
    }

    #[test]
    fn paused_discovery_still_requires_connection_capability_and_no_safety_congestion() {
        let now = Instant::now();
        let mut c = ProbeController::new(now);
        c.committed_capacity = MIN_CAPACITY;
        let at = now + BASE_INTERVAL;
        assert!(
            c.tick(at, 1_000_000, 0, None, true, true)
                .start_validation_bps
                .is_none()
        );
        c.on_estimate(MIN_CAPACITY, true, at);
        assert!(
            c.tick(at, 1_000_000, 0, None, false, true)
                .start_validation_bps
                .is_none()
        );
        c.safety_congested = true;
        assert!(
            c.tick(at, 1_000_000, 0, None, true, true)
                .start_validation_bps
                .is_none()
        );
        assert_eq!(c.capacity(None), Some(MIN_CAPACITY));
        assert_eq!(c.last_twcc_at, None);
        assert_eq!(c.counters.successes, 0);
    }

    fn loss_then_silent_pause(now: Instant) -> (ProbeController, Instant) {
        let mut c = controller(now, 100_000);
        let bad_at = now + Duration::from_secs(1);
        c.observe_native(snapshot(bad_at, 100_000), bad_at);
        assert!(c.on_feedback(Some(0.15), Some(Duration::from_millis(50)), 0, bad_at));
        assert_eq!(c.capacity(None), Some(80_000));
        assert!(c.safety_congested);
        assert_eq!(c.last_bad_safety_twcc_at, Some(bad_at));
        (c, bad_at)
    }

    #[test]
    fn old_dated_loss_allows_only_bounded_paused_discovery_without_budget_or_fake_clean() {
        let now = Instant::now();
        let (mut c, bad_at) = loss_then_silent_pause(now);
        let verified = c.verified_at;
        let before = bad_at + STALE_AFTER - Duration::from_millis(1);
        assert!(
            c.tick(before, 1_000_000, 0, None, true, true)
                .start_validation_bps
                .is_none()
        );
        let at = bad_at + STALE_AFTER;
        assert!(
            c.tick(at, 1_000_000, 64_000, None, true, false)
                .start_validation_bps
                .is_none()
        );
        // The unpaused stale path preserves its ordinary backoff. Once that
        // backoff elapses, only the explicitly paused path may explore.
        let retry = c.next_probe_at;
        let result = c.tick(retry, 1_000_000, 0, None, true, true);
        assert_eq!(result.start_validation_bps, Some(1_000_000));
        c.validation_started(Some(9), retry);
        assert!(
            c.safety_congested,
            "expiry does not claim the path is clean"
        );
        assert_eq!(c.last_bad_safety_twcc_at, Some(bad_at));
        assert_eq!(c.last_twcc_at, Some(bad_at));
        assert_eq!(c.verified_at, verified);
        assert!(c.capacity(None).unwrap() <= 80_000);
        let before_budget = c.capacity(None);
        let clean_batch_at = retry + Duration::from_millis(100);
        let mut actual_batch = snapshot(clean_batch_at, 50_000);
        actual_batch.acknowledged_bitrate = Some(Bitrate::bps(1));
        c.observe_native(actual_batch, clean_batch_at);
        assert!(
            c.validation.is_some(),
            "old loss cannot kill the first new clean batch"
        );
        assert_eq!(c.capacity(None), before_budget);
        assert!(c.safety_congested);
        let result = c.tick(retry + VALIDATION_WINDOW, 1_000_000, 0, None, true, true);
        assert!(result.cancel_validation);
        assert!(result.start_validation_bps.is_none());
        assert_eq!(c.counters.successes, 0);
        assert_eq!(c.counters.failures, 1);
    }

    #[test]
    fn fresh_new_loss_recuts_and_cancels_an_expired_episode_recovery_train() {
        let now = Instant::now();
        let (mut c, bad_at) = loss_then_silent_pause(now);
        let retry = bad_at + STALE_AFTER;
        assert_eq!(
            c.tick(retry, 1_000_000, 0, None, true, true)
                .start_validation_bps,
            Some(1_000_000)
        );
        c.validation_started(Some(9), retry);
        let fresh_bad = retry + Duration::from_secs(1);
        let mut sample = snapshot(fresh_bad, 50_000);
        sample.acknowledged_bitrate = Some(Bitrate::bps(1));
        c.observe_native(sample, fresh_bad);
        let before = c.capacity(None).unwrap();
        assert!(c.on_feedback(Some(0.15), Some(Duration::from_millis(50)), 0, fresh_bad));
        assert_eq!(c.capacity(None), Some(before * 80 / 100));
        assert!(c.validation.is_none());
        assert_eq!(c.last_bad_safety_twcc_at, Some(fresh_bad));
        let result = c.tick(fresh_bad, 1_000_000, 0, None, true, true);
        assert!(result.cancel_validation);
        assert!(result.start_validation_bps.is_none());
        assert_eq!(c.counters.successes, 0);
    }

    #[test]
    fn cached_bad_stats_and_estimates_do_not_renew_a_real_safety_receipt() {
        let now = Instant::now();
        let (mut c, bad_at) = loss_then_silent_pause(now);
        for millis in [100, 900, 1900] {
            let at = bad_at + Duration::from_millis(millis);
            c.on_feedback(Some(0.15), Some(Duration::from_millis(500)), 0, at);
            c.on_estimate(1_000_000, true, at);
            assert_eq!(c.last_bad_safety_twcc_at, Some(bad_at));
        }
        let fresh_bad = bad_at + Duration::from_secs(9);
        c.observe_native(snapshot(fresh_bad, 100_000), fresh_bad);
        c.on_feedback(Some(0.15), Some(Duration::from_millis(50)), 0, fresh_bad);
        assert_eq!(c.last_bad_safety_twcc_at, Some(fresh_bad));
        let old_expiry = bad_at + STALE_AFTER;
        assert!(!c.safety_episode_expired(old_expiry));
        assert!(
            c.tick(old_expiry, 1_000_000, 0, None, true, true)
                .start_validation_bps
                .is_none()
        );
    }

    #[test]
    fn real_clean_stats_restore_unpaused_discovery_but_native_overuse_still_cancels() {
        let now = Instant::now();
        let (mut c, bad_at) = loss_then_silent_pause(now);
        let clean_at = bad_at + Duration::from_secs(3);
        c.observe_native(snapshot(clean_at, 100_000), clean_at);
        c.on_feedback(Some(0.0), Some(Duration::from_millis(50)), 0, clean_at);
        assert!(!c.safety_congested);
        assert_eq!(
            c.tick(clean_at, 1_000_000, 64_000, None, true, false)
                .start_validation_bps,
            Some(1_000_000)
        );
        c.validation_started(Some(9), clean_at);
        let overuse = clean_at + Duration::from_millis(100);
        let mut sample = snapshot(overuse, 50_000);
        sample.cause = BweEstimateCause::DelayOveruse;
        c.observe_native(sample, overuse);
        assert!(c.validation.is_none());
        assert!(
            c.tick(overuse, 1_000_000, 0, None, true, true)
                .cancel_validation
        );
        assert_eq!(c.counters.successes, 0);
    }

    #[test]
    fn bootstrap_coverage_needs_no_managed_discovery_without_claiming_new_proof() {
        let now = Instant::now();
        let mut c = ProbeController::new(now);
        let verified = c.verified_at;
        let at = now + Duration::from_millis(100);
        let result = c.tick(at, 2_000_000, 1_800_000, None, true, false);
        assert!(!result.discovery_needed);
        assert_eq!(
            c.capacity(None),
            Some(crate::allocator::INITIAL_CAPACITY_BPS)
        );
        assert_eq!(c.verified_at, verified);
        assert_eq!(c.last_twcc_at, None);
        assert_eq!(c.counters.successes, 0);
    }

    #[test]
    fn managed_discovery_tracks_total_demand_not_current_allocation_or_old_probe() {
        let now = Instant::now();
        let mut c = controller(now, 400_000);
        let at = now + BASE_INTERVAL;
        let initial = c.tick(at, 1_000_000, 150_000, None, true, false);
        assert!(initial.discovery_needed);
        assert!(c.probe.is_some());
        let satisfied = c.tick(
            at + Duration::from_millis(1),
            350_000,
            150_000,
            None,
            true,
            false,
        );
        assert!(!satisfied.discovery_needed);
        assert!(c.probe.is_none());
        let raised = c.tick(
            at + Duration::from_millis(2),
            900_000,
            150_000,
            None,
            true,
            false,
        );
        assert!(raised.discovery_needed);
        assert_eq!(c.capacity(None), Some(400_000));
        assert_eq!(c.counters.successes, 0);
    }

    #[test]
    fn hard_limits_zero_demand_and_disconnect_bound_managed_discovery() {
        let now = Instant::now();
        let mut c = controller(now, 400_000);
        assert!(
            !c.tick(now, 1_000_000, 150_000, Some(350_000), true, false)
                .discovery_needed
        );
        assert!(
            c.tick(now, 1_000_000, 150_000, Some(700_000), true, false)
                .discovery_needed
        );
        assert!(
            !c.tick(now, 1_000_000, 150_000, None, false, false)
                .discovery_needed
        );
        assert!(!c.tick(now, 0, 0, None, true, false).discovery_needed);
        assert!(
            !c.tick(now, 1_000_000, 0, Some(0), true, false)
                .discovery_needed
        );
        assert_eq!(c.capacity(None), Some(400_000));
    }

    #[test]
    fn real_congestion_reopens_discovery_only_when_budget_no_longer_covers_demand() {
        let now = Instant::now();
        let mut c = controller(now, 1_000_000);
        assert!(
            !c.tick(now, 800_000, 500_000, None, true, false)
                .discovery_needed
        );
        let at = now + Duration::from_secs(1);
        let mut sample = snapshot(at, 600_000);
        sample.cause = BweEstimateCause::DelayOveruse;
        c.observe_native(sample, at);
        assert_eq!(c.capacity(None), Some(600_000));
        assert!(
            c.tick(at, 800_000, 500_000, None, true, false)
                .discovery_needed
        );
        assert!(
            !c.tick(at, 500_000, 400_000, None, true, false)
                .discovery_needed
        );
        assert_eq!(c.counters.congestion_events, 1);
        assert_eq!(c.counters.successes, 0);
    }

    #[test]
    fn disconnection_cancels_validation_and_recovery_reopens_unmet_discovery() {
        let (mut c, requested) = validating(Instant::now());
        let disconnected = requested + Duration::from_millis(100);
        let result = c.tick(disconnected, 1_000_000, 150_000, None, false, false);
        assert!(result.cancel_validation);
        assert!(!result.discovery_needed);
        assert!(c.validation.is_none());
        assert!(c.probe.is_none());
        assert_eq!(c.capacity(None), Some(400_000));
        assert_eq!(c.counters.successes, 0);
        assert_eq!(c.counters.failures, 0);
        let recovered = c.tick(
            disconnected + Duration::from_millis(1),
            1_000_000,
            150_000,
            None,
            true,
            false,
        );
        assert!(recovered.discovery_needed);
        assert!(!recovered.cancel_validation);
        assert_eq!(c.capacity(None), Some(400_000));
    }

    #[test]
    fn expired_coverage_reopens_discovery_without_renewing_from_alr_usage() {
        let now = Instant::now();
        let mut c = controller(now, 1_000_000);
        for second in 1..=60 {
            let at = now + Duration::from_secs(second);
            let mut sample = snapshot(at, 100_000);
            sample.application_limited = true;
            c.observe_native(sample, at);
            let result = c.tick(at, 800_000, 150_000, None, true, false);
            assert_eq!(result.discovery_needed, second == 60);
            assert_eq!(c.counters.successes, 0);
        }
        assert_eq!(c.capacity(None), Some(100_000));
    }

    #[test]
    fn fresh_feedback_and_new_short_trains_never_renew_validation_deadline() {
        let (mut c, requested) = validating(Instant::now());
        let deadline = c.validation.unwrap().until;
        for tick in 1..30u64 {
            let at = requested + Duration::from_millis(tick * 100);
            let mut sample = snapshot(at, 100_000);
            sample.last_successful_probe = Some(proof(at, 5_000_000, tick + 10));
            c.observe_native(sample, at);
            let result = action(&mut c, at);
            assert!(result.start_validation_bps.is_none());
            assert!(!result.cancel_validation);
            assert_eq!(c.validation.unwrap().until, deadline);
            assert_eq!(c.capacity(None), Some(400_000));
        }
        let expired = action(&mut c, deadline);
        assert!(expired.cancel_validation);
        assert!(expired.start_validation_bps.is_none());
        assert!(c.validation.is_none());
        assert_eq!(c.counters.failures, 1);
        assert_eq!(c.counters.successes, 0);
        assert_eq!(c.next_probe_at, deadline + BASE_INTERVAL.mul_f32(1.5));
        assert_eq!(c.capacity(None), Some(400_000));
    }

    #[test]
    fn driver_stall_cannot_admit_a_cached_pass_after_the_fixed_deadline() {
        let (mut c, requested) = validating(Instant::now());
        let completed = requested + Duration::from_millis(1100);
        let mut sample = snapshot(completed, 400_000);
        sample.last_validation = Some(sustained_proof(
            completed, requested, 1_000_000, 1_000_000, 7,
        ));
        let resumed = requested + VALIDATION_WINDOW;
        c.observe_native(sample, resumed);
        assert_eq!(c.capacity(None), Some(400_000));
        assert!(c.validation.is_none());
        assert_eq!(c.counters.successes, 0);
        assert_eq!(c.counters.failures, 1);
        let result = action(&mut c, resumed);
        assert!(result.cancel_validation);
        assert!(result.start_validation_bps.is_none());
        assert_eq!(c.next_probe_at, resumed + BASE_INTERVAL.mul_f32(1.5));
    }

    #[test]
    fn reused_native_id_does_not_accept_a_different_request_generation() {
        let (mut c, requested) = validating(Instant::now());
        for offset in [-1i64, 1] {
            let at = requested + Duration::from_millis((1100 + offset) as u64);
            let other_request = if offset < 0 {
                requested - Duration::from_millis(1)
            } else {
                requested + Duration::from_millis(1)
            };
            let mut sample = snapshot(at, 400_000);
            sample.last_validation =
                Some(sustained_proof(at, other_request, 1_000_000, 1_000_000, 7));
            c.observe_native(sample, at);
            assert_eq!(c.capacity(None), Some(400_000));
            assert!(c.validation.is_some());
            assert_eq!(c.counters.successes, 0);
            assert_eq!(c.counters.failures, 0);
        }
        let at = requested + Duration::from_millis(1200);
        let mut sample = snapshot(at, 400_000);
        sample.last_validation = Some(sustained_proof(at, requested, 1_000_000, 1_000_000, 7));
        c.observe_native(sample, at);
        assert_eq!(c.capacity(None), Some(1_000_000));
        assert_eq!(c.counters.successes, 1);
    }

    #[test]
    fn loss_queue_or_native_overuse_cancels_validation_without_waiting_for_adjustment() {
        use str0m::bwe::BweFeedbackObservation;
        for mode in 0..3 {
            let (mut c, requested) = validating(Instant::now());
            c.last_adjustment = requested;
            c.min_rtt = Some(Duration::from_millis(50));
            let at = requested + Duration::from_millis(100);
            let mut sample = snapshot(at, 400_000);
            sample.latest_feedback = Some(BweFeedbackObservation {
                at,
                received_packets: 10,
                lost_packets: u64::from(mode == 0),
                max_rtt: Some(Duration::from_millis(if mode == 1 { 200 } else { 50 })),
                min_propagation_rtt: Some(Duration::from_millis(if mode == 1 { 200 } else { 50 })),
            });
            if mode == 2 {
                sample.cause = BweEstimateCause::DelayOveruse;
            } else {
                let mut own = sustained_proof(at, requested, 1_000_000, 0, 7);
                own.status = BweValidationStatus::Failed;
                own.lost_packets = u64::from(mode == 0);
                sample.last_validation = Some(own);
            }
            c.observe_native(sample, at);
            assert!(c.validation.is_none());
            assert_eq!(c.counters.failures, 1);
            let result = action(&mut c, at);
            assert!(result.cancel_validation);
            assert!(result.start_validation_bps.is_none());
            assert_eq!(c.counters.successes, 0);
        }
    }

    #[test]
    fn aggregate_old_loss_and_rtt_do_not_cancel_a_clean_current_validation() {
        use str0m::bwe::BweFeedbackObservation;
        let (mut c, requested) = validating(Instant::now());
        c.min_rtt = Some(Duration::from_millis(50));
        let at = requested + Duration::from_millis(100);
        let mut sample = snapshot(at, 400_000);
        // Aggregate feedback may contain an old lost or queued packet. Native
        // validation filters those using its id and actual send timestamp.
        sample.latest_feedback = Some(BweFeedbackObservation {
            at,
            received_packets: 10,
            lost_packets: 1,
            max_rtt: Some(Duration::from_millis(250)),
            min_propagation_rtt: Some(Duration::from_millis(250)),
        });
        let mut own = sustained_proof(at, requested, 1_000_000, 0, 7);
        own.status = BweValidationStatus::Running;
        own.lost_packets = 0;
        sample.last_validation = Some(own);
        c.observe_native(sample, at);
        assert!(c.validation.is_some());
        assert_eq!(c.counters.failures, 0);
        assert!(!action(&mut c, at).cancel_validation);
        own.at = at + Duration::from_millis(1);
        own.status = BweValidationStatus::Failed;
        own.lost_packets = 1;
        sample.last_validation = Some(own);
        sample.last_twcc_feedback_at = Some(own.at);
        c.observe_native(sample, own.at);
        assert!(c.validation.is_none());
        assert_eq!(c.counters.failures, 1);
        assert!(action(&mut c, own.at).cancel_validation);
    }

    #[test]
    fn explicit_controls_cancel_validation_before_accepting_a_late_passed_result() {
        for (demand, hard_limit, connected) in [
            (350_000, None, true),
            (1_000_000, Some(800_000), true),
            (1_000_000, None, false),
        ] {
            let (mut c, requested) = validating(Instant::now());
            let at = requested + Duration::from_millis(1100);
            c.prepare_admission(demand, Some(1_000_000), hard_limit, connected);
            let mut sample = snapshot(at, 400_000);
            sample.last_validation = Some(sustained_proof(at, requested, 1_000_000, 1_000_000, 7));
            c.observe_native(sample, at);
            let result = c.tick(at, demand, 150_000, hard_limit, connected, false);
            assert!(result.cancel_validation);
            assert!(result.start_validation_bps.is_none());
            assert_eq!(c.capacity(None), Some(400_000));
            assert_eq!(c.counters.successes, 0);
            assert_eq!(c.counters.failures, 0);
        }
    }

    #[test]
    fn cancellation_is_serialized_before_a_new_hard_capped_validation_generation() {
        let (mut c, requested) = validating(Instant::now());
        let at = requested + Duration::from_millis(1100);
        c.prepare_admission(1_000_000, Some(1_000_000), Some(800_000), true);
        let mut sample = snapshot(at, 400_000);
        sample.last_validation = Some(sustained_proof(at, requested, 1_000_000, 1_000_000, 7));
        c.observe_native(sample, at);
        let cancelled = c.tick(at, 1_000_000, 150_000, Some(800_000), true, false);
        assert!(cancelled.cancel_validation);
        assert!(cancelled.start_validation_bps.is_none());
        assert!(c.validation.is_none());
        assert_eq!(c.capacity(None), Some(400_000));
        let next = at + Duration::from_millis(1);
        let replacement = c.tick(next, 1_000_000, 150_000, Some(800_000), true, false);
        assert!(!replacement.cancel_validation);
        assert_eq!(replacement.start_validation_bps, Some(800_000));
        c.validation_started(Some(8), next);
        let repeated = next + Duration::from_millis(100);
        sample.last_twcc_feedback_at = Some(repeated);
        c.observe_native(sample, repeated);
        assert_eq!(c.capacity(None), Some(400_000));
        let completed = next + Duration::from_millis(1100);
        sample = snapshot(completed, 400_000);
        sample.last_validation = Some(sustained_proof(completed, next, 800_000, 800_000, 8));
        c.observe_native(sample, completed);
        assert_eq!(c.capacity(None), Some(800_000));
        assert_eq!(c.counters.failures, 0);
        assert_eq!(
            c.counters.successes, 0,
            "the obsolete original goal was1Mbps"
        );
    }

    #[test]
    fn intermediate_layer_rollback_cannot_discard_a_validated_high_goal_with_unchanged_total_demand()
     {
        let now = Instant::now();
        let mut c = controller(now, 615_352);
        let mut tracks = [crate::allocator::AllocTrack {
            priority: 1,
            max_spatial: 2,
            bitrates: [154_136, 489_816, 1_876_324],
            exempt: false,
        }];
        let before = crate::allocator::plan(Some(615_352), &tracks, 1, true);
        assert_eq!(before.caps, [1]);
        let goal = crate::allocator::next_probe_budget(&tracks, &before);
        assert_eq!(goal, Some(2_155_916));
        let total = 2_155_916;
        c.prepare_admission(total, goal, None, true);
        let requested = now + BASE_INTERVAL;
        let result = c.tick(requested, total, before.allocated_bps, None, true, false);
        assert_eq!(result.start_validation_bps, goal);
        c.validation_started(Some(7), requested);
        tracks[0].bitrates[0] = 149_497;
        tracks[0].bitrates[1] = 499_541;
        let intermediate = crate::allocator::plan(Some(615_352), &tracks, 1, true);
        assert_eq!(intermediate.caps, [0]);
        let next = crate::allocator::next_probe_budget(&tracks, &intermediate);
        assert_eq!(next, Some(626_157));
        c.prepare_admission(total, next, None, true);
        let completed = requested + Duration::from_millis(1100);
        let mut sample = snapshot(completed, 2_155_916);
        sample.last_validation = Some(sustained_proof(
            completed, requested, 2_155_916, 2_175_762, 7,
        ));
        c.observe_native(sample, completed);
        assert_eq!(c.capacity(None), Some(2_155_916));
        assert_eq!(c.counters.successes, 1);
        assert_eq!(
            crate::allocator::plan(c.capacity(None), &tracks, 1, true).caps,
            [2]
        );
    }

    #[test]
    fn successful_next_layer_discovery_reaches_total_demand_despite_repeated_low_cost_growth() {
        let now = Instant::now();
        let mut c = controller(now, 64_000);
        let total = 2_200_000;
        let request = now + BASE_INTERVAL;
        c.prepare_admission(total, Some(242_984), None, true);
        assert_eq!(
            c.tick(request, total, 64_000, None, true, true)
                .start_validation_bps,
            Some(242_984)
        );
        c.validation_started(Some(7), request);
        let completed = request + Duration::from_millis(1100);
        let mut sample = snapshot(completed, 242_984);
        sample.last_validation = Some(sustained_proof(completed, request, 242_984, 243_500, 7));
        c.observe_native(sample, completed);
        assert_eq!(c.capacity(None), Some(242_984));
        for (offset, goal) in [(100, 267_713), (200, 302_812)] {
            let at = completed + Duration::from_millis(offset);
            c.prepare_admission(total, Some(goal), None, true);
            assert!(
                c.tick(at, total, 64_000, None, true, true)
                    .start_validation_bps
                    .is_none()
            );
        }
        let retry = completed + BASE_INTERVAL;
        c.observe_native(snapshot(retry, 242_984), retry);
        let next = c.tick(retry, total, 64_000, None, true, true);
        assert_eq!(next.start_validation_bps, Some(total));
        assert_eq!(next.desired_bps, total);
        assert_eq!(
            c.capacity(None),
            Some(242_984),
            "requesting total never grants it"
        );
    }

    #[test]
    fn total_demand_failure_falls_back_to_a_feasible_layer_without_erasing_proof_or_backoff() {
        let now = Instant::now();
        let mut c = controller(now, 400_000);
        let total = 2_000_000;
        let request = now + BASE_INTERVAL;
        c.prepare_admission(total, Some(650_000), None, true);
        assert_eq!(
            c.tick(request, total, 200_000, None, true, false)
                .start_validation_bps,
            Some(650_000)
        );
        c.validation_started(Some(7), request);
        let passed = request + Duration::from_millis(1100);
        let mut sample = snapshot(passed, 650_000);
        sample.last_validation = Some(sustained_proof(passed, request, 650_000, 651_000, 7));
        c.observe_native(sample, passed);
        assert_eq!(c.discovery_goal, DiscoveryGoal::TotalDemand);
        let request_total = passed + BASE_INTERVAL;
        c.observe_native(snapshot(request_total, 650_000), request_total);
        c.prepare_admission(total, Some(700_000), None, true);
        let next = c.tick(request_total, total, 600_000, None, true, false);
        assert_eq!(next.start_validation_bps, Some(total));
        assert_eq!(next.desired_bps, total);
        c.validation_started(Some(8), request_total);
        let failed = request_total + VALIDATION_WINDOW;
        let result = c.tick(failed, total, 600_000, None, true, false);
        assert!(result.cancel_validation);
        assert!(result.start_validation_bps.is_none());
        assert_eq!(c.discovery_goal, DiscoveryGoal::NextLayer);
        assert_eq!(
            c.capacity(None),
            Some(650_000),
            "timeout is not proof of new congestion"
        );
        let retry = c.next_probe_at;
        assert!(
            retry > failed + BASE_INTERVAL,
            "failure preserves the original 1.5x backoff"
        );
        let early = retry - Duration::from_millis(1);
        c.observe_native(snapshot(early, 650_000), early);
        assert!(
            c.tick(early, total, 600_000, None, true, false)
                .start_validation_bps
                .is_none()
        );
        c.observe_native(snapshot(retry, 650_000), retry);
        assert_eq!(
            c.tick(retry, total, 600_000, None, true, false)
                .start_validation_bps,
            Some(700_000)
        );
        c.validation_started(Some(9), retry);
        let at = retry + Duration::from_millis(1100);
        sample = snapshot(at, 700_000);
        sample.last_validation = Some(sustained_proof(at, retry, 700_000, 701_000, 9));
        c.observe_native(sample, at);
        assert_eq!(
            c.capacity(None),
            Some(700_000),
            "a link unable to prove TOTAL can still acquire MID"
        );
        assert_eq!(c.counters.successes, 2);
    }

    #[test]
    fn total_discovery_qualification_is_cleared_by_controls_and_real_congestion() {
        for mode in 0..4 {
            let now = Instant::now();
            let mut c = controller(now, 650_000);
            c.discovery_goal = DiscoveryGoal::TotalDemand;
            let at = now + BASE_INTERVAL;
            c.prepare_admission(2_000_000, Some(700_000), None, true);
            assert_eq!(
                c.tick(at, 2_000_000, 600_000, None, true, false)
                    .start_validation_bps,
                Some(2_000_000)
            );
            c.validation_started(Some(7), at);
            match mode {
                0 => c.prepare_admission(0, None, None, true),
                1 => c.prepare_admission(2_000_000, Some(700_000), None, false),
                2 => c.prepare_admission(2_000_000, Some(700_000), Some(1_000_000), true),
                _ => c.congest(500_000, at + Duration::from_millis(100)),
            }
            assert_eq!(c.discovery_goal, DiscoveryGoal::NextLayer);
            assert!(c.validation.is_none());
            assert!(c.cancel_validation_pending);
            let mut stale = snapshot(at + Duration::from_millis(1100), 650_000);
            stale.last_validation = Some(sustained_proof(
                stale.last_twcc_feedback_at.unwrap(),
                at,
                2_000_000,
                2_010_000,
                7,
            ));
            c.observe_native(stale, stale.last_twcc_feedback_at.unwrap());
            assert!(c.capacity(None).unwrap() <= 650_000);
            assert_eq!(c.discovery_goal, DiscoveryGoal::NextLayer);
        }
    }

    #[test]
    fn dynamic_layer_costs_do_not_restart_or_extend_the_frozen_validation_goal() {
        let (mut c, requested) = validating(Instant::now());
        let deadline = c.validation.unwrap().until;
        for (tick, goal) in [950_000, 1_100_000, 980_000].into_iter().enumerate() {
            let at = requested + Duration::from_millis((tick as u64 + 1) * 200);
            c.prepare_admission(2_000_000, Some(goal), None, true);
            let result = c.tick(at, 2_000_000, 150_000, None, true, false);
            assert!(!result.cancel_validation);
            assert!(result.start_validation_bps.is_none());
            let active = c.validation.unwrap();
            assert_eq!(active.id, Some(7));
            assert_eq!(active.target_bps, 1_000_000);
            assert_eq!(active.until, deadline);
        }
        let at = requested + Duration::from_millis(1100);
        let mut sample = snapshot(at, 400_000);
        sample.last_validation = Some(sustained_proof(at, requested, 1_000_000, 1_000_000, 7));
        c.observe_native(sample, at);
        assert_eq!(c.capacity(None), Some(1_000_000));
        assert_eq!(c.counters.successes, 1);
    }

    #[test]
    fn true_total_demand_shrink_limits_proof_without_completing_the_original_goal() {
        for total in [980_000, 700_000] {
            let (mut c, requested) = validating(Instant::now());
            c.prepare_admission(total, Some(total), None, true);
            let at = requested + Duration::from_millis(1100);
            let mut sample = snapshot(at, 400_000);
            sample.last_validation = Some(sustained_proof(at, requested, 1_000_000, 1_000_000, 7));
            c.observe_native(sample, at);
            assert_eq!(c.capacity(None), Some(total));
            assert_eq!(
                c.counters.successes, 0,
                "the original goal still required 1 Mbps"
            );
        }
    }

    #[test]
    fn raised_goal_cannot_turn_old_validation_into_proof_of_the_new_layer() {
        let (mut c, requested) = validating(Instant::now());
        c.prepare_admission(2_000_000, Some(2_000_000), None, true);
        let at = requested + Duration::from_millis(1100);
        let mut sample = snapshot(at, 400_000);
        sample.last_validation = Some(sustained_proof(at, requested, 1_000_000, 1_500_000, 7));
        c.observe_native(sample, at);
        assert_eq!(c.capacity(None), Some(1_000_000));
        assert_eq!(
            c.counters.successes, 1,
            "only the original 1 Mbps attempt completed"
        );
        assert!(c.capacity(None).unwrap() < c.admission_limit());
    }

    #[test]
    fn dual_whole_proof_uses_selected_raw_span_and_admits_only_floor() {
        let (mut c, requested) = validating(Instant::now());
        let at = requested + Duration::from_millis(1500);
        let mut result = sustained_proof(at, requested, 1_000_000, 1_200_000, 7);
        result.pacing_bitrate = Bitrate::bps(1_200_000);
        result.send_span = Duration::from_millis(200);
        result.recv_span = Duration::from_millis(200);
        result.measured_bitrate = None;
        result.selected_proof.as_mut().unwrap().scope =
            str0m::bwe::BweValidationProofScope::WholeRequest;
        let mut sample = snapshot(at, 400_000);
        sample.last_validation = Some(result);
        c.observe_native(sample, at);
        assert_eq!(c.capacity(None), Some(1_000_000));
        assert_eq!(c.counters.successes, 1);
    }

    #[test]
    fn pacing_strength_and_legacy_summary_cannot_replace_selected_actual_proof() {
        for mode in 0..3 {
            let (mut c, requested) = validating(Instant::now());
            let at = requested + Duration::from_millis(1500);
            let mut result = sustained_proof(at, requested, 1_000_000, 1_200_000, 7);
            result.pacing_bitrate = Bitrate::bps(1_200_000);
            match mode {
                0 => result.selected_proof = None,
                1 => {
                    result.selected_proof.as_mut().unwrap().measured_bitrate = Bitrate::bps(999_999)
                }
                _ => {
                    result.selected_proof.as_mut().unwrap().started_at =
                        requested - Duration::from_millis(1)
                }
            }
            let mut sample = snapshot(at, 400_000);
            sample.last_validation = Some(result);
            c.observe_native(sample, at);
            assert_eq!(c.capacity(None), Some(400_000));
            assert_eq!(c.counters.successes, 0);
        }
    }

    #[test]
    fn incomplete_or_foreign_or_lost_validation_never_grants_budget() {
        for mode in 0..5 {
            let (mut c, requested) = validating(Instant::now());
            let at = requested + Duration::from_millis(1100);
            let mut result = sustained_proof(at, requested, 1_000_000, 1_000_000, 7);
            match mode {
                0 => {
                    result.send_span = Duration::from_millis(999);
                    result.selected_proof.as_mut().unwrap().send_span = result.send_span;
                }
                1 => {
                    result.recv_span = Duration::from_millis(999);
                    result.selected_proof.as_mut().unwrap().recv_span = result.recv_span;
                }
                2 => result.lost_packets = 1,
                3 => result.id = 8,
                _ => result.started_at = Some(requested - Duration::from_millis(1)),
            }
            let mut sample = snapshot(at, 400_000);
            sample.last_validation = Some(result);
            c.observe_native(sample, at);
            assert_eq!(c.capacity(None), Some(400_000));
            assert_eq!(c.counters.successes, 0);
            if mode == 3 {
                assert!(
                    c.validation.is_some(),
                    "unrelated ids are ignored, not accepted"
                );
            } else {
                assert!(c.validation.is_none());
                assert_eq!(c.counters.failures, 1);
            }
        }
    }

    #[test]
    fn lower_application_limited_rate_preserves_proven_allocation() {
        let now = Instant::now();
        let mut c = controller(now, 1_000_000);
        let at = now + Duration::from_secs(1);
        c.on_estimate(180_000, true, at);
        let mut sample = snapshot(at, 180_000);
        sample.application_limited = true;
        c.observe_native(sample, at);
        assert_eq!(c.capacity(Some(180_000)), Some(1_000_000));
        assert_eq!(c.counters.congestion_events, 0);
    }

    #[test]
    fn estimate_or_scheduled_goal_cannot_grant_capacity_or_probe_success() {
        let now = Instant::now();
        let mut c = controller(now, 400_000);
        assert_eq!(action(&mut c, now + BASE_INTERVAL).desired_bps, 1_000_000);
        let at = now + BASE_INTERVAL + Duration::from_millis(100);
        c.on_estimate(10_000_000, true, at);
        let mut sample = snapshot(at, 10_000_000);
        sample.latest_scheduled_probe = Some(proof(at, 20_000_000, 1));
        sample.acknowledged_bitrate = Some(Bitrate::bps(200_000));
        c.observe_native(sample, at);
        assert_eq!(c.capacity(Some(10_000_000)), Some(400_000));
        assert_eq!(c.counters.successes, 0);
    }

    #[test]
    fn feedback_validated_probe_commits_capacity_and_completes_once() {
        let now = Instant::now();
        let (mut c, requested) = validating(now);
        let at = now + BASE_INTERVAL + Duration::from_millis(100);
        let mut sample = snapshot(at, 250_000);
        sample.last_successful_probe = Some(proof(at, 1_000_000, 1));
        c.observe_native(sample, at);
        assert_eq!(
            c.capacity(None),
            Some(400_000),
            "short discovery is not admission"
        );
        assert_eq!(c.counters.successes, 0);
        assert!(action(&mut c, at).start_validation_bps.is_none());
        let completed = at + Duration::from_millis(1100);
        sample = snapshot(completed, 250_000);
        sample.last_validation = Some(sustained_proof(
            completed, requested, 1_000_000, 1_000_000, 7,
        ));
        c.observe_native(sample, completed);
        c.observe_native(sample, completed + Duration::from_secs(1));
        assert_eq!(c.capacity(Some(250_000)), Some(1_000_000));
        assert_eq!(c.counters.successes, 1);
    }

    #[test]
    fn upgrade_needs_measured_wire_cost_but_not_padding_headroom() {
        for measured in [700_000, 708_924] {
            let now = Instant::now();
            let mut c = controller(now, 615_620);
            c.set_upgrade_goal(Some(704_445));
            // Full room demand exceeds this intermediate next-layer goal.
            // The exact 704445-bps measured success boundary is unchanged.
            let started = now + BASE_INTERVAL;
            let initial = c.tick(started, 1_000_000, 580_800, None, true, false);
            assert_eq!(initial.desired_bps, 704_445);
            assert_eq!(initial.start_validation_bps, Some(704_445));
            c.validation_started(Some(7), started);
            let requested = started + Duration::from_millis(100);
            let mut sample = snapshot(requested, 700_000);
            sample.last_successful_probe = Some(proof(requested, measured, 1));
            c.observe_native(sample, requested);
            assert_eq!(c.capacity(None), Some(615_620));
            assert!(
                c.tick(requested, 1_000_000, 580_800, None, true, false)
                    .start_validation_bps
                    .is_none()
            );
            let at = requested + Duration::from_millis(1100);
            sample = snapshot(at, measured);
            sample.last_validation = Some(sustained_proof(at, started, 704_445, measured, 7));
            c.observe_native(sample, at);
            if measured == 700_000 {
                assert_eq!(c.counters.successes, 0, "below actual layer wire cost");
                assert_eq!(c.capacity(None), Some(615_620));
            } else {
                assert_eq!(c.counters.successes, 1);
                assert_eq!(
                    sample
                        .last_validation
                        .unwrap()
                        .measured_bitrate
                        .unwrap()
                        .as_u64(),
                    708_924
                );
                assert_eq!(c.capacity(Some(708_924)), Some(704_445));
            }
        }
    }

    #[test]
    fn short_train_peak_never_grants_a_temporary_budget_before_sustained_validation() {
        let now = Instant::now();
        let mut c = controller(now, 400_000);
        c.set_upgrade_goal(Some(2_100_000));
        let start = now + BASE_INTERVAL;
        let initial = c.tick(start, 3_000_000, 300_000, None, true, false);
        assert_eq!(initial.start_validation_bps, Some(2_100_000));
        c.validation_started(Some(7), start);
        let requested = start + Duration::from_millis(100);
        let mut sample = snapshot(requested, 680_589);
        sample.acknowledged_bitrate = Some(Bitrate::bps(92_056));
        sample.last_successful_probe = Some(proof(requested, 18_331_200, 1));
        c.observe_native(sample, requested);
        assert_eq!(c.capacity(None), Some(400_000));
        assert_eq!(c.counters.successes, 0);
        assert_eq!(c.last_growth_at, now);
        assert!(
            c.tick(requested, 3_000_000, 300_000, None, true, false)
                .start_validation_bps
                .is_none()
        );
        let at = requested + Duration::from_millis(1100);
        sample = snapshot(at, 680_589);
        sample.last_validation = Some(sustained_proof(at, start, 2_100_000, 2_200_000, 7));
        c.observe_native(sample, at);
        assert_eq!(c.capacity(None), Some(2_100_000));
        assert_eq!(c.counters.successes, 1);
    }

    #[test]
    fn raw_peak_beyond_admission_cannot_renew_larger_bootstrap_budget() {
        let now = Instant::now();
        let mut c = controller(now, 2_500_000);
        c.set_upgrade_goal(Some(2_100_000));
        c.tick(now, 3_000_000, 1_800_000, None, true, false);
        let at = now + Duration::from_secs(1);
        let mut sample = snapshot(at, 680_589);
        sample.last_successful_probe = Some(proof(at, 18_331_200, 1));
        c.observe_native(sample, at);
        assert_eq!(c.capacity(None), Some(2_500_000));
        assert_eq!(c.verified_at, now);
        assert_eq!(c.last_growth_at, now);
        assert!(
            c.admitted_probe.is_none(),
            "a raw short peak is not an admitted proof"
        );
    }

    #[test]
    fn repeated_weak_proofs_expire_old_peak_to_fresh_admitted_evidence_not_alr_usage() {
        let now = Instant::now();
        let mut c = controller(now, 18_331_200);
        c.set_upgrade_goal(Some(2_100_000));
        c.tick(now, 3_000_000, 1_800_000, None, true, false);
        for second in 1..=60 {
            let at = now + Duration::from_secs(second);
            let mut sample = snapshot(at, 80_000);
            sample.application_limited = true;
            sample.last_successful_probe = Some(proof(at, 1_451_813, second));
            c.observe_native(sample, at);
            if second == 59 {
                // Previously validated sustained delivery is an independent
                // lease fallback; the weak short samples cannot replace it.
                c.admitted_probe = Some((1_451_813, at));
            }
            assert_eq!(c.last_growth_at, now);
            if second < 60 {
                assert_eq!(c.capacity(None), Some(18_331_200));
                assert_eq!(c.verified_at, now);
            }
        }
        assert_eq!(c.capacity(None), Some(1_451_813));
        assert_eq!(c.counters.congestion_events, 0);
    }

    #[test]
    fn expired_proof_freshness_cannot_restore_budget_from_an_old_cluster() {
        let now = Instant::now();
        let mut c = controller(now, 18_331_200);
        c.set_upgrade_goal(Some(2_100_000));
        c.tick(now, 3_000_000, 1_800_000, None, true, false);
        let measured = now + VERIFIED_FOR - STALE_AFTER;
        c.admitted_probe = Some((1_451_813, measured));
        let mut sample = snapshot(measured, 80_000);
        sample.last_successful_probe = Some(proof(measured, 1_451_813, 1));
        c.observe_native(sample, measured);
        let at = now + VERIFIED_FOR;
        // Re-reading the same native result with fresh TWCC is not new proof.
        sample.last_twcc_feedback_at = Some(at);
        c.observe_native(sample, at);
        assert_eq!(c.capacity(None), Some(80_000));
    }

    #[test]
    fn newly_observed_but_old_probe_cannot_grant_or_revalidate_capacity() {
        let now = Instant::now();
        let mut c = controller(now, 400_000);
        action(&mut c, now + BASE_INTERVAL);
        let at = now + STALE_AFTER;
        let mut sample = snapshot(at, 80_000);
        sample.last_successful_probe = Some(proof(now, 18_331_200, 1));
        c.observe_native(sample, at);
        assert_eq!(c.capacity(None), Some(400_000));
        assert_eq!(c.verified_at, now);
        assert_eq!(c.last_growth_at, now);
        assert!(c.admitted_probe.is_none());
        assert_eq!(c.counters.successes, 0);
    }

    #[test]
    fn fresh_proof_expiry_fallback_respects_current_demand_and_hard_override() {
        let now = Instant::now();
        let mut c = controller(now, 18_331_200);
        c.set_upgrade_goal(Some(2_100_000));
        c.tick(now, 3_000_000, 1_800_000, None, true, false);
        let measured = now + VERIFIED_FOR - Duration::from_secs(1);
        c.admitted_probe = Some((1_451_813, measured));
        let mut sample = snapshot(measured, 80_000);
        sample.last_successful_probe = Some(proof(measured, 1_451_813, 1));
        c.observe_native(sample, measured);
        c.set_upgrade_goal(Some(800_000));
        c.tick(measured, 900_000, 600_000, Some(700_000), true, false);
        let at = now + VERIFIED_FOR;
        sample.last_twcc_feedback_at = Some(at);
        c.observe_native(sample, at);
        assert_eq!(c.capacity(None), Some(700_000));
    }

    #[test]
    fn actual_congestion_invalidates_admitted_probe_before_expiry_fallback() {
        let now = Instant::now();
        let mut c = controller(now, 2_000_000);
        c.tick(now, 2_100_000, 1_800_000, None, true, false);
        let at = now + Duration::from_secs(1);
        let mut sample = snapshot(at, 1_000_000);
        sample.last_successful_probe = Some(proof(at, 1_000_000, 1));
        // Retained history from an earlier sustained admission must be
        // invalidated by new real congestion, regardless of short discovery.
        c.admitted_probe = Some((1_000_000, at));
        c.observe_native(sample, at);
        assert!(c.admitted_probe.is_some());
        let at = at + Duration::from_secs(1);
        sample = snapshot(at, 350_000);
        sample.cause = BweEstimateCause::DelayOveruse;
        c.observe_native(sample, at);
        assert_eq!(c.capacity(None), Some(350_000));
        assert!(c.admitted_probe.is_none());
        assert_eq!(c.counters.congestion_events, 1);
    }

    #[test]
    fn managed_validation_receives_actual_demand_without_short_probe_multiplier() {
        let now = Instant::now();
        let mut c = controller(now, 1_000_000);
        c.set_upgrade_goal(Some(1_838_559));
        let action = c.tick(now + BASE_INTERVAL, 1_838_559, 429_530, None, true, false);
        assert_eq!(action.desired_bps, 1_838_559);
        assert_eq!(action.start_validation_bps, Some(1_838_559));
        assert_eq!(c.capacity(Some(1_000_000)), Some(1_000_000));
    }

    #[test]
    fn fresh_overuse_or_loss_reduces_capacity_once_per_feedback_and_interval() {
        let now = Instant::now();
        for cause in [
            BweEstimateCause::DelayOveruse,
            BweEstimateCause::LossLimited,
        ] {
            let mut c = controller(now, 1_000_000);
            let at = now + Duration::from_secs(1);
            let mut sample = snapshot(at, 350_000);
            sample.cause = cause;
            c.observe_native(sample, at);
            assert_eq!(c.capacity(Some(350_000)), Some(350_000));
            c.observe_native(sample, at + Duration::from_secs(2));
            assert_eq!(c.capacity(Some(350_000)), Some(350_000));
            assert_eq!(c.counters.congestion_events, 1);
        }
    }

    #[test]
    fn congested_delivery_rate_bounds_a_lagging_native_target() {
        let now = Instant::now();
        let mut c = controller(now, 2_500_000);
        let at = now + Duration::from_secs(1);
        let mut sample = snapshot(at, 737_707);
        sample.acknowledged_bitrate = Some(Bitrate::bps(90_000));
        sample.cause = BweEstimateCause::DelayOveruse;
        c.observe_native(sample, at);
        assert_eq!(c.capacity(Some(737_707)), Some(90_000));
    }

    #[test]
    fn actual_loss_episode_uses_delivery_even_if_native_cause_has_recovered() {
        let now = Instant::now();
        let mut c = controller(now, 2_500_000);
        let loss = now + Duration::from_secs(1);
        c.observe_native(snapshot(loss, 1_000_000), loss);
        assert!(c.on_feedback(Some(0.15), Some(Duration::from_millis(20)), 1000, loss));
        let at = now + Duration::from_secs(2);
        let mut sample = snapshot(at, 737_707);
        sample.acknowledged_bitrate = Some(Bitrate::bps(90_000));
        sample.cause = BweEstimateCause::LossRecovering;
        c.observe_native(sample, at);
        assert_eq!(c.capacity(Some(737_707)), Some(90_000));
    }

    #[test]
    fn persistent_native_loss_state_does_not_compound_a_second_backoff() {
        let now = Instant::now();
        let mut c = controller(now, 1_000_000);
        for second in 1..=15 {
            let at = now + Duration::from_secs(second);
            let mut sample = snapshot(at, 60_000);
            sample.cause = BweEstimateCause::LossLimited;
            c.observe_native(sample, at);
            assert_eq!(c.capacity(Some(60_000)), Some(60_000));
        }
        let at = now + Duration::from_secs(16);
        let mut sample = snapshot(at, 80_000);
        sample.cause = BweEstimateCause::LossRecovering;
        c.observe_native(sample, at);
        assert_eq!(c.capacity(Some(80_000)), Some(60_000));
    }

    #[test]
    fn historical_loss_limited_audio_ack_cannot_renew_congestion_or_block_rediscovery() {
        let now = Instant::now();
        let mut c = controller(now, 1_311_849);
        c.set_upgrade_goal(Some(2_038_314));
        let mut recovery_reset = false;
        let mut direct_requests = 0;
        for (index, acked) in [
            596_758, 419_286, 196_930, 174_637, 117_187, 60_714, 49_335, 49_335, 49_335, 48_518,
            48_518, 46_617, 46_617, 46_617, 46_617,
        ]
        .into_iter()
        .enumerate()
        {
            let at = now + Duration::from_secs(index as u64 + 1);
            let mut sample = snapshot(at, 964_448);
            sample.cause = BweEstimateCause::LossLimited;
            sample.application_limited = true;
            sample.acknowledged_bitrate = Some(Bitrate::bps(acked));
            sample.latest_feedback = Some(str0m::bwe::BweFeedbackObservation {
                at,
                received_packets: 3,
                lost_packets: 0,
                max_rtt: Some(Duration::from_millis(60)),
                min_propagation_rtt: Some(Duration::from_millis(60)),
            });
            c.observe_native(sample, at);
            let action = c.tick(at, 2_038_314, 455_639, None, true, false);
            recovery_reset |= action.reset_bwe_to.is_some();
            direct_requests += u64::from(action.start_validation_bps.is_some());
            assert_eq!(c.capacity(Some(964_448)), Some(964_448));
            assert_eq!(c.congestion_at, Some(now + Duration::from_secs(1)));
            assert_eq!(c.counters.congestion_events, 1);
            assert_eq!(c.counters.successes, 0);
        }
        assert!(
            recovery_reset,
            "historical loss state must permit the existing recovery reset"
        );
        // Unlike the old automatic-probe timer, each direct train must fail
        // at three seconds without a sustained result. Historical LossLimited
        // must still allow real bounded attempts, then preserve their backoff.
        assert_eq!(direct_requests, 2);
        assert_eq!(c.counters.attempts, 2);
        assert_eq!(c.counters.failures, 2);
        assert!(c.validation.is_none());
        assert_eq!(c.next_probe_at, now + Duration::from_millis(21_750));
    }

    #[test]
    fn same_loss_state_with_a_real_new_raw_reduction_still_reduces_capacity() {
        let now = Instant::now();
        let mut c = controller(now, 1_311_849);
        for (second, raw, expected) in [
            (1, 964_448, 964_448),
            (2, 964_448, 964_448),
            (3, 600_000, 600_000),
        ] {
            let at = now + Duration::from_secs(second);
            let mut sample = snapshot(at, raw);
            sample.cause = BweEstimateCause::LossLimited;
            sample.application_limited = true;
            sample.acknowledged_bitrate = Some(Bitrate::bps(49_335));
            c.observe_native(sample, at);
            assert_eq!(c.capacity(Some(raw)), Some(expected));
        }
        assert_eq!(c.counters.congestion_events, 2);
        assert_eq!(c.congestion_at, Some(now + Duration::from_secs(3)));
        let loss = now + Duration::from_secs(4);
        let mut sample = snapshot(loss, 600_000);
        sample.cause = BweEstimateCause::LossLimited;
        c.observe_native(sample, loss);
        assert!(c.on_feedback(Some(0.15), Some(Duration::from_millis(20)), 1000, loss));
        assert_eq!(c.capacity(Some(600_000)), Some(480_000));
        let at = now + Duration::from_secs(5);
        sample = snapshot(at, 600_000);
        sample.cause = BweEstimateCause::LossLimited;
        sample.acknowledged_bitrate = Some(Bitrate::bps(90_000));
        c.observe_native(sample, at);
        assert_eq!(c.capacity(Some(600_000)), Some(90_000));
    }

    #[test]
    fn native_loss_reduction_inside_adjustment_gate_is_applied_after_gate_opens() {
        let now = Instant::now();
        let mut c = controller(now, 1_000_000);
        let early = now + Duration::from_millis(200);
        let mut sample = snapshot(early, 500_000);
        sample.cause = BweEstimateCause::LossLimited;
        sample.application_limited = true;
        sample.acknowledged_bitrate = Some(Bitrate::bps(100_000));
        c.observe_native(sample, early);
        assert_eq!(c.capacity(Some(500_000)), Some(1_000_000));
        assert_eq!(c.counters.congestion_events, 0);
        let ready = now + Duration::from_millis(1100);
        sample.last_twcc_feedback_at = Some(ready);
        c.observe_native(sample, ready);
        assert_eq!(c.capacity(Some(500_000)), Some(500_000));
        assert_eq!(c.congestion_at, Some(ready));
        assert_eq!(c.counters.congestion_events, 1);
        let later = now + Duration::from_millis(2200);
        sample.last_twcc_feedback_at = Some(later);
        c.observe_native(sample, later);
        assert_eq!(c.capacity(Some(500_000)), Some(500_000));
        assert_eq!(c.congestion_at, Some(ready));
        assert_eq!(c.counters.congestion_events, 1);
    }

    #[test]
    fn clean_feedback_rearms_queue_safety_without_a_native_loss_transition() {
        let now = Instant::now();
        let mut c = controller(now, 1_000_000);
        c.on_feedback(Some(0.0), Some(Duration::from_millis(20)), 0, now);
        for (second, rtt_ms, expected) in [(1, 180, 800_000), (2, 20, 800_000), (3, 180, 640_000)] {
            let at = now + Duration::from_secs(second);
            let mut actual = snapshot(at, 1_000_000);
            actual.latest_feedback.as_mut().unwrap().max_rtt = Some(Duration::from_millis(rtt_ms));
            actual.latest_feedback.as_mut().unwrap().min_propagation_rtt =
                Some(Duration::from_millis(rtt_ms));
            c.observe_native(actual, at);
            c.on_feedback(
                Some(0.0),
                Some(Duration::from_millis(rtt_ms)),
                second * 1000,
                at,
            );
            assert_eq!(c.capacity(Some(1_000_000)), Some(expected));
        }
        assert_eq!(c.counters.congestion_events, 2);
    }

    #[test]
    fn current_matched_rtt_does_not_authorize_a_different_cached_stats_rtt() {
        use str0m::bwe::BweFeedbackObservation;
        let now = Instant::now();
        let mut c = controller(now, 1_000_000);
        c.min_rtt = Some(Duration::from_millis(50));
        let fresh = now + Duration::from_secs(1);
        let mut actual = snapshot(fresh, 1_000_000);
        actual.latest_feedback = Some(BweFeedbackObservation {
            at: fresh,
            received_packets: 10,
            lost_packets: 0,
            max_rtt: Some(Duration::from_millis(50)),
            min_propagation_rtt: Some(Duration::from_millis(50)),
        });
        c.observe_native(actual, fresh);
        // PeerStats may still expose an earlier record's300ms RTT. The
        // current real matched batch has50ms RTT; its receipt does not give
        // the different cached sample a new queue-event identity.
        assert!(!c.on_feedback(Some(0.0), Some(Duration::from_millis(300)), 1000, fresh));
        assert_eq!(c.capacity(None), Some(1_000_000));
        assert!(!c.safety_congested);
        assert_eq!(c.counters.congestion_events, 0);

        // Preserve the independent one-second rolling loss source: a clean
        // instantaneous batch cannot erase15% rolling transport loss.
        let bad_loss = fresh + Duration::from_secs(1);
        actual.last_twcc_feedback_at = Some(bad_loss);
        actual.latest_feedback.as_mut().unwrap().at = bad_loss;
        c.observe_native(actual, bad_loss);
        assert!(c.on_feedback(Some(0.15), Some(Duration::from_millis(300)), 2000, bad_loss));
        assert_eq!(c.capacity(None), Some(800_000));
        assert_eq!(c.counters.congestion_events, 1);
    }

    #[test]
    fn historical_safety_episode_cannot_recut_clean_normal_alr_usage() {
        use str0m::bwe::BweFeedbackObservation;
        let now = Instant::now();
        let mut c = controller(now, 1_000_000);
        c.min_rtt = Some(Duration::from_millis(50));
        let bad_at = now + Duration::from_secs(1);
        let mut actual = snapshot(bad_at, 1_000_000);
        actual.latest_feedback = Some(BweFeedbackObservation {
            at: bad_at,
            received_packets: 10,
            lost_packets: 0,
            max_rtt: Some(Duration::from_millis(300)),
            min_propagation_rtt: Some(Duration::from_millis(300)),
        });
        c.observe_native(actual, bad_at);
        assert!(c.on_feedback(None, Some(Duration::from_millis(300)), 1000, bad_at));
        assert_eq!(c.capacity(None), Some(800_000));
        assert!(c.safety_congested);
        for second in 2..=8 {
            let at = now + Duration::from_secs(second);
            let mut clean = snapshot(at, 1_000_000);
            clean.acknowledged_bitrate = Some(Bitrate::bps(48_000));
            clean.application_limited = true;
            clean.latest_feedback = Some(BweFeedbackObservation {
                at,
                received_packets: 10,
                lost_packets: 0,
                max_rtt: Some(Duration::from_millis(50)),
                min_propagation_rtt: Some(Duration::from_millis(50)),
            });
            // Production consumes this snapshot before the next PeerStats.
            // There is new real feedback, but no new delay/loss reduction;
            //48kbps is paused-media usage, not newly lost link capacity.
            c.observe_native(clean, at);
            assert_eq!(c.capacity(None), Some(800_000), "second{second}");
            assert_eq!(c.counters.congestion_events, 1);
        }
    }

    #[test]
    fn unknown_matched_rtt_cannot_clear_or_renew_a_queue_episode() {
        let now = Instant::now();
        let mut c = controller(now, 1_000_000);
        c.min_rtt = Some(Duration::from_millis(50));
        let at = now + Duration::from_secs(1);
        let mut actual = snapshot(at, 1_000_000);
        actual.latest_feedback.as_mut().unwrap().max_rtt = Some(Duration::from_millis(300));
        actual.latest_feedback.as_mut().unwrap().min_propagation_rtt =
            Some(Duration::from_millis(300));
        c.observe_native(actual, at);
        assert!(c.on_feedback(Some(0.0), Some(Duration::from_millis(300)), 1000, at));
        let unknown_at = at + Duration::from_secs(1);
        actual = snapshot(unknown_at, 1_000_000);
        actual.latest_feedback.as_mut().unwrap().max_rtt = None;
        actual.latest_feedback.as_mut().unwrap().min_propagation_rtt = None;
        c.observe_native(actual, unknown_at);
        assert!(!c.on_feedback(Some(0.0), Some(Duration::from_millis(50)), 2000, unknown_at));
        assert!(c.safety_congested);
        assert_eq!(c.last_bad_safety_twcc_at, Some(at));
        assert_eq!(c.capacity(None), Some(800_000));
    }

    #[test]
    fn rolling_loss_does_not_authorize_normal_stable_raw_alr_delivery_cut() {
        let now = Instant::now();
        let mut c = controller(now, 1_000_000);
        let at = now + Duration::from_secs(1);
        c.observe_native(snapshot(at, 1_000_000), at);
        assert!(c.on_feedback(Some(0.15), Some(Duration::from_millis(20)), 1000, at));
        assert_eq!(c.capacity(None), Some(800_000));
        let next = at + Duration::from_secs(1);
        let mut actual = snapshot(next, 1_000_000);
        actual.acknowledged_bitrate = Some(Bitrate::bps(48_000));
        actual.application_limited = true;
        c.observe_native(actual, next);
        assert_eq!(c.capacity(None), Some(800_000));
        assert_eq!(c.counters.congestion_events, 1);
    }

    #[test]
    fn periodic_estimate_callback_cannot_erase_a_matched_native_reduction() {
        let now = Instant::now();
        let mut c = controller(now, 1_000_000);
        let at = now + Duration::from_secs(1);
        c.observe_native(snapshot(at, 1_000_000), at);
        assert!(c.on_feedback(Some(0.15), Some(Duration::from_millis(20)), 1000, at));
        let next = at + Duration::from_secs(1);
        c.on_estimate(737_707, true, next);
        let mut actual = snapshot(next, 737_707);
        actual.acknowledged_bitrate = Some(Bitrate::bps(90_000));
        actual.cause = BweEstimateCause::Normal;
        c.observe_native(actual, next);
        assert_eq!(c.capacity(None), Some(90_000));
    }

    #[test]
    fn real_loss_delivery_permission_survives_feedback_before_adjustment_gate() {
        let now = Instant::now();
        let mut c = controller(now, 1_000_000);
        let at = now + Duration::from_secs(1);
        c.observe_native(snapshot(at, 1_000_000), at);
        assert!(c.on_feedback(Some(0.15), Some(Duration::from_millis(20)), 1000, at));
        let early = at + Duration::from_millis(50);
        let mut actual = snapshot(early, 1_000_000);
        actual.cause = BweEstimateCause::LossLimited;
        actual.acknowledged_bitrate = Some(Bitrate::bps(90_000));
        c.observe_native(actual, early);
        assert_eq!(c.capacity(None), Some(800_000));
        assert_eq!(c.pending_loss_receipt, Some(at));
        let ready = at + Duration::from_secs(1);
        actual.last_twcc_feedback_at = Some(ready);
        actual.latest_feedback.as_mut().unwrap().at = ready;
        c.observe_native(actual, ready);
        assert_eq!(c.capacity(None), Some(90_000));
        assert!(c.pending_loss_receipt.is_none());
    }

    #[test]
    fn matched_safety_native_reduction_survives_feedback_before_adjustment_gate() {
        let now = Instant::now();
        let mut c = controller(now, 1_000_000);
        let at = now + Duration::from_secs(1);
        c.observe_native(snapshot(at, 1_000_000), at);
        assert!(c.on_feedback(Some(0.15), Some(Duration::from_millis(20)), 1000, at));
        let early = at + Duration::from_millis(50);
        c.on_estimate(737_707, true, early);
        let mut actual = snapshot(early, 737_707);
        actual.acknowledged_bitrate = Some(Bitrate::bps(90_000));
        c.observe_native(actual, early);
        assert_eq!(c.capacity(None), Some(800_000));
        let ready = at + Duration::from_secs(1);
        actual.last_twcc_feedback_at = Some(ready);
        actual.latest_feedback.as_mut().unwrap().at = ready;
        c.observe_native(actual, ready);
        assert_eq!(c.capacity(None), Some(90_000));
        assert!(c.pending_safety_native_reduction.is_none());
    }

    #[test]
    fn delayed_native_reduction_uses_its_own_delivery_not_later_alr_ack() {
        let now = Instant::now();
        let mut c = controller(now, 1_000_000);
        let bad_at = now + Duration::from_millis(100);
        c.observe_native(snapshot(bad_at, 1_000_000), bad_at);
        assert!(!c.on_feedback(Some(0.15), Some(Duration::from_millis(20)), 1000, bad_at));
        let early = now + Duration::from_millis(200);
        let mut reduction = snapshot(early, 737_707);
        reduction.acknowledged_bitrate = Some(Bitrate::bps(600_000));
        c.observe_native(reduction, early);
        assert_eq!(c.capacity(None), Some(1_000_000));
        let ready = now + Duration::from_millis(1100);
        let mut later = snapshot(ready, 737_707);
        later.acknowledged_bitrate = Some(Bitrate::bps(48_000));
        later.application_limited = true;
        c.observe_native(later, ready);
        assert_eq!(c.capacity(None), Some(600_000));
    }

    #[test]
    fn persistent_delay_cannot_treat_lossless_alr_usage_as_capacity_but_still_cancels() {
        let now = Instant::now();
        let mut c = controller(now, 1_000_000);
        let first = now + Duration::from_secs(1);
        let mut actual = snapshot(first, 737_707);
        actual.cause = BweEstimateCause::DelayOveruse;
        actual.acknowledged_bitrate = Some(Bitrate::bps(600_000));
        c.observe_native(actual, first);
        assert_eq!(c.capacity(None), Some(600_000));
        let next = now + Duration::from_secs(2);
        c.validation = Some(Validation {
            goal: DiscoveryGoal::NextLayer,
            id: Some(7),
            target_bps: 1_000_000,
            requested_at: next - Duration::from_millis(10),
            until: next + VALIDATION_WINDOW,
        });
        actual = snapshot(next, 737_707);
        actual.cause = BweEstimateCause::DelayOveruse;
        actual.acknowledged_bitrate = Some(Bitrate::bps(48_000));
        actual.application_limited = true;
        c.observe_native(actual, next);
        assert!(c.validation.is_none());
        assert!(c.cancel_validation_pending);
        assert_eq!(c.capacity(None), Some(600_000));
        let decrease = now + Duration::from_secs(3);
        actual = snapshot(decrease, 350_000);
        actual.cause = BweEstimateCause::DelayOveruse;
        actual.acknowledged_bitrate = Some(Bitrate::bps(48_000));
        actual.application_limited = true;
        c.observe_native(actual, decrease);
        assert_eq!(
            c.capacity(None),
            Some(350_000),
            "real native target still constrains capacity"
        );
    }

    #[test]
    fn successful_native_reduction_consumes_earlier_same_episode_loss_permission() {
        let now = Instant::now();
        let mut c = controller(now, 1_000_000);
        let bad_at = now + Duration::from_millis(100);
        c.observe_native(snapshot(bad_at, 1_000_000), bad_at);
        assert!(!c.on_feedback(Some(0.15), Some(Duration::from_millis(20)), 1000, bad_at));
        let early = now + Duration::from_millis(200);
        let mut actual = snapshot(early, 700_000);
        actual.acknowledged_bitrate = Some(Bitrate::bps(650_000));
        c.observe_native(actual, early);
        let ready = now + Duration::from_secs(1);
        actual = snapshot(ready, 700_000);
        actual.acknowledged_bitrate = Some(Bitrate::bps(650_000));
        c.observe_native(actual, ready);
        assert_eq!(c.capacity(None), Some(650_000));
        let replay = now + Duration::from_secs(2);
        actual = snapshot(replay, 700_000);
        actual.cause = BweEstimateCause::LossRecovering;
        actual.acknowledged_bitrate = Some(Bitrate::bps(48_000));
        c.observe_native(actual, replay);
        assert_eq!(c.capacity(None), Some(650_000));
    }

    #[test]
    fn rolling_loss_permission_cannot_bound_alr_usage_without_current_matched_loss() {
        let now = Instant::now();
        let mut c = controller(now, 1_000_000);
        let first = now + Duration::from_secs(1);
        c.observe_native(snapshot(first, 1_000_000), first);
        assert!(c.on_feedback(Some(0.15), Some(Duration::from_millis(20)), 1000, first));
        let next = now + Duration::from_secs(2);
        let mut actual = snapshot(next, 1_000_000);
        actual.cause = BweEstimateCause::LossRecovering;
        actual.acknowledged_bitrate = Some(Bitrate::bps(48_000));
        actual.application_limited = true;
        c.observe_native(actual, next);
        assert_eq!(c.capacity(None), Some(800_000));
    }

    #[test]
    fn current_matched_loss_keeps_real_loss_delivery_bound_even_during_alr() {
        let now = Instant::now();
        let mut c = controller(now, 1_000_000);
        let first = now + Duration::from_secs(1);
        c.observe_native(snapshot(first, 1_000_000), first);
        assert!(c.on_feedback(Some(0.15), Some(Duration::from_millis(20)), 1000, first));
        let next = now + Duration::from_secs(2);
        let mut actual = snapshot(next, 1_000_000);
        actual.cause = BweEstimateCause::LossRecovering;
        actual.acknowledged_bitrate = Some(Bitrate::bps(90_000));
        actual.application_limited = true;
        actual.latest_feedback.as_mut().unwrap().lost_packets = 1;
        c.observe_native(actual, next);
        assert_eq!(c.capacity(None), Some(90_000));
    }

    #[test]
    fn a_new_larger_delivery_candidate_cannot_renew_older_low_budget_identity() {
        let now = Instant::now();
        let mut c = controller(now, 1_000_000);
        let bad_at = now + Duration::from_millis(100);
        c.observe_native(snapshot(bad_at, 1_000_000), bad_at);
        assert!(!c.on_feedback(Some(0.15), Some(Duration::from_millis(20)), 1000, bad_at));
        let old_at = now + Duration::from_millis(200);
        let mut actual = snapshot(old_at, 737_707);
        actual.acknowledged_bitrate = Some(Bitrate::bps(90_000));
        c.observe_native(actual, old_at);
        let newer_at = now + Duration::from_millis(800);
        actual = snapshot(newer_at, 600_000);
        actual.acknowledged_bitrate = Some(Bitrate::bps(600_000));
        c.observe_native(actual, newer_at);
        let pending = c.pending_safety_native_reduction.unwrap();
        assert_eq!(pending.budget, 90_000);
        assert_eq!(
            pending.receipt, old_at,
            "preserved budget keeps the exact receipt that measured it"
        );
        assert_eq!(pending.raw_target, 737_707);
        assert_eq!(c.capacity(None), Some(1_000_000));
    }

    #[test]
    fn actual_clean_feedback_retires_old_episode_pending_permissions() {
        let now = Instant::now();
        let mut c = controller(now, 1_000_000);
        let bad_at = now + Duration::from_millis(100);
        c.observe_native(snapshot(bad_at, 1_000_000), bad_at);
        assert!(!c.on_feedback(Some(0.15), Some(Duration::from_millis(20)), 1000, bad_at));
        let early = now + Duration::from_millis(200);
        let mut actual = snapshot(early, 737_707);
        actual.acknowledged_bitrate = Some(Bitrate::bps(90_000));
        c.observe_native(actual, early);
        assert!(c.pending_loss_receipt.is_some());
        assert!(c.pending_safety_native_reduction.is_some());
        let clean_at = now + Duration::from_millis(300);
        c.observe_native(snapshot(clean_at, 737_707), clean_at);
        assert!(!c.on_feedback(Some(0.0), Some(Duration::from_millis(20)), 2000, clean_at));
        assert!(!c.safety_congested);
        assert!(c.pending_loss_receipt.is_none());
        assert!(c.pending_safety_native_reduction.is_none());
        let new_bad = now + Duration::from_millis(400);
        c.observe_native(snapshot(new_bad, 737_707), new_bad);
        assert!(!c.on_feedback(Some(0.15), Some(Duration::from_millis(20)), 3000, new_bad));
        let ready = now + Duration::from_millis(1100);
        actual = snapshot(ready, 600_000);
        actual.cause = BweEstimateCause::LossLimited;
        c.observe_native(actual, ready);
        assert_eq!(
            c.capacity(None),
            Some(600_000),
            "new loss cannot revive the old90kbps delivery"
        );
    }

    #[test]
    fn inapplicable_older_pending_target_cannot_hide_a_new_real_native_reduction() {
        let now = Instant::now();
        let mut c = controller(now, 1_000_000);
        let bad_at = now + Duration::from_millis(100);
        c.observe_native(snapshot(bad_at, 1_000_000), bad_at);
        assert!(!c.on_feedback(Some(0.15), Some(Duration::from_millis(20)), 1000, bad_at));
        let old_at = now + Duration::from_millis(200);
        let mut actual = snapshot(old_at, 600_000);
        actual.acknowledged_bitrate = Some(Bitrate::bps(90_000));
        c.observe_native(actual, old_at);
        let rebound = now + Duration::from_millis(400);
        c.observe_native(snapshot(rebound, 1_000_000), rebound);
        let new_at = now + Duration::from_millis(800);
        actual = snapshot(new_at, 700_000);
        actual.acknowledged_bitrate = Some(Bitrate::bps(600_000));
        c.observe_native(actual, new_at);
        let ready = now + Duration::from_millis(1100);
        actual = snapshot(ready, 700_000);
        actual.acknowledged_bitrate = Some(Bitrate::bps(600_000));
        c.observe_native(actual, ready);
        assert_eq!(
            c.capacity(None),
            Some(600_000),
            "old600k target is inapplicable to the real new700k target"
        );
        assert_eq!(c.counters.congestion_events, 1);
    }

    #[test]
    fn receiver_report_batch_span_is_not_a_propagation_queue_event() {
        let now = Instant::now();
        let mut c = controller(now, 1_000_000);
        c.min_rtt = Some(Duration::from_millis(50));
        let at = now + Duration::from_secs(1);
        let mut actual = snapshot(at, 1_000_000);
        // Fixed source vector (not an encrypted-packet test): local sends
        //0..160ms/20ms; remote receipts10..170ms; local feedback180ms.
        // Actual max feedback RTT180ms includes160ms report-batch waiting;
        // the propagation formula's minimum is20ms, not queue growth.
        actual.latest_feedback.as_mut().unwrap().max_rtt = Some(Duration::from_millis(180));
        actual.latest_feedback.as_mut().unwrap().min_propagation_rtt =
            Some(Duration::from_millis(20));
        c.observe_native(actual, at);
        assert!(!c.on_feedback(Some(0.0), Some(Duration::from_millis(20)), 1000, at));
        assert_eq!(c.capacity(None), Some(1_000_000));
        assert_eq!(c.counters.congestion_events, 0);
    }

    #[test]
    fn actual_return_path_queue_keeps_original_safety_threshold() {
        let now = Instant::now();
        let mut c = controller(now, 1_000_000);
        c.min_rtt = Some(Duration::from_millis(50));
        let at = now + Duration::from_secs(1);
        let mut actual = snapshot(at, 1_000_000);
        // Same fixed source vector plus150ms real return-path waiting:
        // max feedbackRTT330ms; minimum propagation estimate170ms.
        actual.latest_feedback.as_mut().unwrap().max_rtt = Some(Duration::from_millis(330));
        actual.latest_feedback.as_mut().unwrap().min_propagation_rtt =
            Some(Duration::from_millis(170));
        c.observe_native(actual, at);
        assert!(c.on_feedback(Some(0.0), Some(Duration::from_millis(170)), 1000, at));
        assert_eq!(c.capacity(None), Some(800_000));
        assert_eq!(c.counters.congestion_events, 1);
    }

    #[test]
    fn one_real_loss_receipt_cannot_authorize_repeated_loss_state_alr_cuts() {
        let now = Instant::now();
        let mut c = controller(now, 1_000_000);
        let at = now + Duration::from_secs(1);
        c.observe_native(snapshot(at, 1_000_000), at);
        assert!(c.on_feedback(Some(0.15), Some(Duration::from_millis(20)), 1000, at));
        for (second, ack, expected) in [(2, 90_000, 90_000), (3, 48_000, 90_000)] {
            let at = now + Duration::from_secs(second);
            let mut actual = snapshot(at, 1_000_000);
            actual.cause = BweEstimateCause::LossRecovering;
            actual.acknowledged_bitrate = Some(Bitrate::bps(ack));
            c.observe_native(actual, at);
            assert_eq!(c.capacity(None), Some(expected));
        }
        assert_eq!(c.counters.congestion_events, 2);
    }

    #[test]
    fn repeated_loss_or_queue_safety_stats_cut_only_once_per_episode() {
        let now = Instant::now();
        let mut c = controller(now, 1_000_000);
        for second in 1..=8 {
            let at = now + Duration::from_secs(second);
            c.observe_native(snapshot(at, 1_000_000), at);
            c.on_feedback(
                Some(0.15),
                Some(Duration::from_millis(20)),
                second * 1000,
                at,
            );
            assert_eq!(c.capacity(Some(1_000_000)), Some(800_000));
        }
        let clean = now + Duration::from_secs(9);
        c.observe_native(snapshot(clean, 1_000_000), clean);
        c.on_feedback(Some(0.0), Some(Duration::from_millis(20)), 9000, clean);
        let loss = now + Duration::from_secs(10);
        c.observe_native(snapshot(loss, 1_000_000), loss);
        c.on_feedback(Some(0.15), Some(Duration::from_millis(20)), 10000, loss);
        assert_eq!(c.capacity(Some(1_000_000)), Some(640_000));
    }

    #[test]
    fn pre_congestion_probe_cannot_reinflate_capacity() {
        let now = Instant::now();
        let mut c = controller(now, 1_000_000);
        c.congest(100_000, now + Duration::from_secs(1));
        let at = now + Duration::from_secs(2);
        let mut sample = snapshot(at, 80_000);
        let mut late = proof(at, 3_000_000, 1);
        late.started_at = Some(now);
        sample.last_successful_probe = Some(late);
        c.observe_native(sample, at);
        assert_eq!(c.capacity(Some(80_000)), Some(100_000));
    }

    #[test]
    fn repeated_rtt_and_periodic_estimates_do_not_refresh_real_feedback() {
        let now = Instant::now();
        let mut c = controller(now, 400_000);
        for second in 1..=11 {
            let at = now + Duration::from_secs(second);
            c.on_estimate(400_000, true, at);
            c.on_feedback(None, Some(Duration::from_millis(20)), 100, at);
        }
        let at = now + Duration::from_secs(11);
        c.tick(at, 1_000_000, 150_000, None, true, false);
        assert_eq!(c.feedback_at, now);
        assert_eq!(c.counters.stale_feedback_events, 1);
        assert_eq!(c.capacity(Some(400_000)), Some(320_000));
    }

    #[test]
    fn continuous_low_rate_feedback_does_not_preserve_capacity_forever() {
        let now = Instant::now();
        let mut c = controller(now, 2_000_000);
        for second in 1..=61 {
            let at = now + Duration::from_secs(second);
            c.observe_native(snapshot(at, 100_000), at);
        }
        assert_eq!(c.capacity(Some(100_000)), Some(100_000));
    }

    #[test]
    fn acknowledged_sustained_media_revalidates_existing_capacity() {
        let now = Instant::now();
        let mut c = controller(now, 2_000_000);
        for second in 1..=61 {
            let at = now + Duration::from_secs(second);
            let mut sample = snapshot(at, 100_000);
            sample.acknowledged_bitrate = Some(Bitrate::bps(1_800_000));
            c.observe_native(sample, at);
        }
        assert_eq!(c.capacity(Some(100_000)), Some(2_000_000));
    }

    #[test]
    fn ack_revalidation_preserves_exact_existing_eighty_percent_boundary() {
        let now = Instant::now();
        let at = now + Duration::from_secs(1);
        for (acked, revalidates) in [(1_599_999, false), (1_600_000, true)] {
            let mut c = controller(now, 2_000_000);
            let mut sample = snapshot(at, 100_000);
            sample.acknowledged_bitrate = Some(Bitrate::bps(acked));
            c.observe_native(sample, at);
            assert_eq!(c.verified_at, if revalidates { at } else { now });
            assert_eq!(c.capacity(None), Some(2_000_000));
            assert_eq!(c.last_growth_at, now);
            assert_eq!(c.counters.congestion_events, 0);
        }
    }

    #[test]
    fn direct_validation_keeps_its_deadline_through_alr_rate_fall() {
        let (mut c, start) = validating(Instant::now());
        let deadline = c.validation.unwrap().until;
        for elapsed in 1..3 {
            let at = start + Duration::from_secs(elapsed);
            c.on_estimate(250_000, true, at);
            c.observe_native(snapshot(at, 250_000), at);
            assert_eq!(action(&mut c, at).desired_bps, 1_000_000);
            assert_eq!(c.validation.unwrap().until, deadline);
            assert_eq!(c.counters.failures, 0);
            assert_eq!(c.capacity(None), Some(400_000));
        }
        assert!(action(&mut c, deadline).cancel_validation);
        assert_eq!(c.counters.failures, 1);
        assert_eq!(c.counters.successes, 0);
    }

    #[test]
    fn satisfied_demand_cancels_obsolete_probe_without_faking_success_or_resetting_backoff() {
        let now = Instant::now();
        let mut c = controller(now, 400_000);
        c.set_upgrade_goal(Some(1_437_147));
        let start = now + BASE_INTERVAL;
        assert_eq!(
            c.tick(start, 1_437_147, 315_000, None, true, false)
                .desired_bps,
            1_437_147
        );
        assert!(c.probe.is_some());
        let backoff = (c.interval, c.next_probe_at, c.reset_allowed_at);
        let at = start + Duration::from_secs(1);
        c.observe_native(snapshot(at, 400_000), at);
        c.set_upgrade_goal(None);
        let action = c.tick(at, 350_000, 315_000, None, true, false);
        assert_eq!(action.desired_bps, 350_000);
        assert!(action.reset_bwe_to.is_none());
        assert!(c.probe.is_none());
        assert_eq!(c.capacity(Some(400_000)), Some(400_000));
        assert_eq!(c.counters.attempts, 1);
        assert_eq!(c.counters.successes, 0);
        assert_eq!(c.counters.failures, 0);
        assert_eq!((c.interval, c.next_probe_at, c.reset_allowed_at), backoff);
    }

    #[test]
    fn direct_goal_does_not_wait_for_a_disabled_fifteen_second_native_timer() {
        let now = Instant::now();
        let mut c = controller(now, 400_000);
        c.set_upgrade_goal(Some(1_437_147));
        let requested = now + BASE_INTERVAL;
        let initial = c.tick(requested, 1_437_147, 315_000, None, true, false);
        assert_eq!(initial.start_validation_bps, Some(1_437_147));
        c.validation_started(Some(7), requested);
        let deadline = c.validation.unwrap().until;
        let at = requested + Duration::from_millis(1100);
        let mut sample = snapshot(at, 180_000);
        sample.application_limited = true;
        sample.last_validation = Some(sustained_proof(at, requested, 1_437_147, 1_437_147, 7));
        c.observe_native(sample, at);
        assert!(at < deadline);
        assert_eq!(c.counters.successes, 1);
        assert_eq!(c.capacity(None), Some(1_437_147));
        assert!(c.probe.is_none());
    }

    #[test]
    fn failed_probe_backs_off_without_granting_capacity() {
        let now = Instant::now();
        let mut c = controller(now, 400_000);
        action(&mut c, now + BASE_INTERVAL);
        let at = now + BASE_INTERVAL + PROBE_WINDOW;
        c.observe_native(snapshot(at, 200_000), at);
        action(&mut c, at);
        assert_eq!(c.counters.failures, 1);
        assert_eq!(c.capacity(Some(200_000)), Some(400_000));
        assert_eq!(
            action(&mut c, at + Duration::from_secs(1)).desired_bps,
            400_000
        );
    }

    #[test]
    fn target_and_reset_remain_hard_capped_and_never_count_as_proof() {
        let now = Instant::now();
        let mut c = controller(now, 400_000);
        c.set_upgrade_goal(Some(1_960_000));
        let at = now + BASE_INTERVAL;
        assert_eq!(
            c.tick(at, 1_960_000, 100_000, Some(450_000), true, false)
                .desired_bps,
            450_000
        );
        assert_eq!(c.capacity(Some(400_000)), Some(400_000));
        // Let the first explicit validation reach its immutable deadline,
        // then keep the existing bounded failure backoff before recovery reset.
        let expired = at + VALIDATION_WINDOW;
        assert!(
            c.tick(expired, 1_960_000, 0, Some(450_000), true, true)
                .cancel_validation
        );
        let later = now + Duration::from_secs(11);
        let action = c.tick(later, 1_960_000, 0, Some(450_000), true, true);
        assert!(action.reset_bwe_to.is_some());
        assert!(action.desired_bps <= 450_000);
        assert_eq!(c.counters.successes, 0);
    }

    #[test]
    fn remb_receiver_estimate_can_raise_or_lower_capacity_without_twcc() {
        let now = Instant::now();
        let mut c = ProbeController::new(now);
        c.on_remb(300_000, now);
        assert_eq!(c.capacity(Some(300_000)), Some(300_000));
        c.on_remb(500_000, now + Duration::from_secs(1));
        assert_eq!(c.capacity(Some(500_000)), Some(500_000));
    }
}
