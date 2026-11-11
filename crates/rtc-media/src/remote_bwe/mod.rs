// Copyright 2023 LiveKit, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Sender policy port of LiveKit e5655ac20267fa5005dccaf3f0e3bfc3ea688c07.
//! REMB is the remote receiver's observation. The initial 100 Mbps is optimistic
//! policy state, never measured capacity. All clocks are explicitly supplied.
mod observer;
mod probe;
#[cfg(test)]
mod tests;
mod time;
mod trend;

use observer::{ChannelTrend, Observer, Reason};
use probe::{ProbeController, ProbeInfo};
pub use probe::{ProbeGoal, ProbeResult, ProbeSignal};
use rtc_core::config::{PolicyDuration, RemoteBweConfig};
use std::time::Instant;
use time::float_i64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Sender-side policy congestion state, independent of receiver estimator internals.
pub enum CongestionState {
    /// No current policy congestion signal.
    None,
    /// A falling estimate trend or repeated-NACK window signals congestion.
    Congested,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Notification returned by a REMB update, replacing the upstream listener callback.
pub struct StateChange {
    /// State before processing this update.
    pub from: CongestionState,
    /// State after processing this update; may equal `from` for a capacity update.
    pub to: CongestionState,
    /// Committed policy capacity in bits per second, not a guaranteed physical limit.
    pub capacity: i64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Settled policy outcome containing caller-reported probe execution accounting.
pub struct ProbeOutcome {
    /// Caller-owned probe generation identifier.
    pub id: u32,
    /// Settled congestion classification.
    pub signal: ProbeSignal,
    /// Committed policy capacity in bits per second, not a guaranteed physical limit.
    pub capacity: i64,
    /// Actual execution counters supplied by the caller, not independently verified here.
    pub result: ProbeResult,
}
/// Pure LiveKit sender REMB policy with explicit caller-owned time and execution.
///
/// Inputs are bits per second, packet deltas, and monotonic timestamps. Configuration
/// durations retain signed nanoseconds. Supply nonnegative required sample counts,
/// finite factors/RTT, and arithmetic results representable as i64. This foundation
/// neither estimates ABS receive timing nor sends padding or schedules tasks.
///
/// # Panics
/// Policy updates panic for non-finite/out-of-range float-to-i64 arithmetic, negative
/// required sample counts when the first sample is pruned, or timestamps/durations
/// outside the platform `Instant` range. Those invalid domains are not claimed
/// equivalent to architecture-dependent Go conversions.
pub struct RemoteController {
    config: RemoteBweConfig,
    received_estimate: i64,
    expected_usage: i64,
    committed_capacity: i64,
    probe: ProbeController,
    observer: Observer,
    congestion: CongestionState,
    switched_at: Instant,
}
impl RemoteController {
    /// Creates policy state with an optimistic 100 Mbps capacity, not a measurement.
    pub fn new(config: RemoteBweConfig, now: Instant) -> Self {
        Self {
            received_estimate: 0,
            expected_usage: 0,
            committed_capacity: 100_000_000,
            probe: ProbeController::new(config.probe_controller.clone(), now),
            observer: Observer::new(config.channel_observer_non_probe.clone()),
            config,
            congestion: CongestionState::None,
            switched_at: now,
        }
    }
    /// Resets observers, probes, RTT and optimistic capacity while retaining configuration.
    pub fn reset(&mut self, now: Instant) {
        *self = Self::new(self.config.clone(), now);
    }
    /// Returns committed policy capacity in bits per second.
    pub fn capacity(&self) -> i64 {
        self.committed_capacity
    }
    /// Returns creation/reset time or the latest congestion-state/capacity notification time.
    pub fn state_changed_at(&self) -> Instant {
        self.switched_at
    }
    /// Channel observer view for diagnostics: trend, reason and NACK ratio.
    pub fn observation(&self, now: Instant) -> String {
        let (trend, reason) = self.observer.state(now);
        format!(
            "{trend:?}/{reason:?} nack_ratio={:.3}",
            self.observer.ratio()
        )
    }
    /// Returns the current sender policy congestion state.
    pub fn state(&self) -> CongestionState {
        self.congestion
    }
    /// Processes a received REMB and actual sent/repeated-NACK packet deltas.
    ///
    /// `estimate` and `expected_usage` are bits per second. Returns a notification
    /// only when upstream would notify its listener. Congested probes freeze the
    /// observer until probe settlement. See the controller-level supported domain.
    pub fn on_remb(
        &mut self,
        estimate: i64,
        expected_usage: i64,
        sent_delta: u32,
        repeated_nack_delta: u32,
        now: Instant,
    ) -> Option<StateChange> {
        self.received_estimate = estimate;
        self.expected_usage = expected_usage;
        if self.congestion != CongestionState::None && self.probe.in_probe() {
            return None;
        }
        self.observer
            .add(estimate, sent_delta, repeated_nack_delta, now);
        let from = self.congestion;
        let mut to = from;
        let mut update = false;
        let (trend, reason) = self.observer.state(now);
        match from {
            CongestionState::None => {
                if trend == ChannelTrend::Congesting
                    && (self.probe.in_probe() || self.estimate_capacity(reason))
                {
                    to = CongestionState::Congested;
                }
            }
            CongestionState::Congested => {
                if trend == ChannelTrend::Congesting {
                    update = self.estimate_capacity(reason);
                } else {
                    to = CongestionState::None;
                }
            }
        }
        if to != from || update {
            self.congestion = to;
            self.switched_at = now;
            Some(StateChange {
                from,
                to,
                capacity: self.committed_capacity,
            })
        } else {
            None
        }
    }
    fn estimate_capacity(&mut self, reason: Reason) -> bool {
        let mut estimate = if reason == Reason::Loss {
            float_i64(
                self.expected_usage as f64
                    * (1.0 - self.config.nack_ratio_attenuator * self.observer.ratio()),
            )
        } else {
            self.received_estimate
        };
        if estimate > self.received_estimate {
            estimate = self.received_estimate;
        }
        let threshold =
            float_i64(self.config.expected_usage_threshold * self.expected_usage as f64);
        if estimate > threshold || self.committed_capacity == estimate {
            return false;
        }
        self.committed_capacity = estimate;
        true
    }
    /// Updates smoothed RTT in seconds; zero restores the upstream 70 ms default.
    pub fn update_rtt(&mut self, seconds: f64) {
        self.probe.update_rtt(seconds);
    }
    /// Tests congestion, probe lifecycle and the strict regulator deadline.
    pub fn can_probe(&self, now: Instant) -> bool {
        self.congestion == CongestionState::None && self.probe.can_probe(now)
    }
    /// Returns the signed duration selected by the probe regulator.
    pub fn probe_duration(&self) -> PolicyDuration {
        self.probe.duration()
    }
    /// Starts a caller-owned probe, using `now` as its creation time.
    /// Starting does not itself send packets or commit the desired rate.
    pub fn start_probe(&mut self, id: u32, goal: ProbeGoal, now: Instant) {
        self.start_probe_created_at(id, goal, now, now);
    }
    /// Starts a probe with its original creation time for regulator backoff.
    /// As upstream does, this replaces any previous probe without executing it.
    pub fn start_probe_created_at(
        &mut self,
        id: u32,
        goal: ProbeGoal,
        created_at: Instant,
        now: Instant,
    ) {
        self.expected_usage = goal.expected_usage_bps;
        self.probe.start(
            ProbeInfo {
                id,
                created_at,
                goal,
                result: Default::default(),
            },
            now,
        );
        self.new_observer();
    }
    /// Reports actual completion accounting for a matching caller generation.
    ///
    /// Upstream retains the last generation after finalization, so repeated
    /// same-ID completion can re-enter hangover. The execution bridge must emit
    /// completion at most once and prevent stale ID reuse, including across reset.
    pub fn finish_probe(&mut self, id: u32, actual: ProbeResult, now: Instant) {
        self.probe.done(id, actual, now);
    }
    /// Tests sample sufficiency and a highest received estimate strictly above the goal.
    pub fn goal_reached(&self) -> bool {
        self.probe.in_probe()
            && self.congestion == CongestionState::None
            && self.observer.enough()
            && self.probe.goal_reached(self.observer.highest())
    }
    /// Settles a completed probe after its RTT-derived wait, returning at most one
    /// outcome per completion transition. Capacity growth uses received estimates,
    /// never the requested goal or caller byte counters.
    pub fn finalize(&mut self, now: Instant) -> Option<ProbeOutcome> {
        let info = self.probe.finalize(now)?;
        let congested = self.congestion != CongestionState::None;
        let enough = self.observer.enough();
        let highest = self.observer.highest();
        self.congestion = CongestionState::None;
        self.new_observer();
        let signal = if congested {
            ProbeSignal::Congesting
        } else if !enough {
            ProbeSignal::Inconclusive
        } else {
            if highest > self.committed_capacity {
                self.committed_capacity = highest;
            }
            ProbeSignal::NotCongesting
        };
        self.probe.signal(signal, Some(info.created_at), now);
        Some(ProbeOutcome {
            id: info.id,
            signal,
            capacity: self.committed_capacity,
            result: info.result,
        })
    }
    fn new_observer(&mut self) {
        self.observer = Observer::new(if self.probe.in_probe() {
            self.config.channel_observer_probe.clone()
        } else {
            self.config.channel_observer_non_probe.clone()
        });
    }
}
