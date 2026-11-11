// Copyright 2023 LiveKit, Inc.
// SPDX-License-Identifier: Apache-2.0
// Fixed LiveKit remotebwe/probe_controller.go and ccutils/probe_regulator.go.
use super::time::{elapsed, float_i64, offset, scaled_millis};
use rtc_core::config::{PolicyDuration, RemoteProbeControllerConfig};
use std::time::Instant;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Upstream regulator classification of a settled probe.
pub enum ProbeSignal {
    /// Too few received estimates to classify the probe.
    Inconclusive,
    /// Probe observation reached a congestion state.
    Congesting,
    /// Sufficient estimates without probe congestion; not a physical-capacity guarantee.
    NotCongesting,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
/// Caller-owned execution goal; rates are bits per second and counts are bytes.
pub struct ProbeGoal {
    /// Policy capacity when planning the probe.
    pub available_bandwidth_bps: i64,
    /// Expected normal media usage in bits per second.
    pub expected_usage_bps: i64,
    /// Goal rate used for the strict received-estimate goal comparison.
    pub desired_bps: i64,
    /// Signed requested execution duration; this policy does not schedule execution.
    pub duration: PolicyDuration,
    /// Requested probe byte amount, retained as caller execution metadata.
    pub desired_bytes: i64,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
/// Actual counters reported by a probe executor, not an independent bandwidth proof.
pub struct ProbeResult {
    /// Executor start timestamp in its nanosecond clock domain.
    pub start_time_ns: i64,
    /// Executor end timestamp in the same nanosecond clock domain.
    pub end_time_ns: i64,
    /// Actual probe packet count.
    pub packets_probe: i64,
    /// Actual probe byte count in the executor accounting domain.
    pub bytes_probe: i64,
    /// Actual primary media packets sent during execution.
    pub packets_non_probe_primary: i64,
    /// Actual primary media bytes sent during execution.
    pub bytes_non_probe_primary: i64,
    /// Actual retransmission packets sent during execution.
    pub packets_non_probe_rtx: i64,
    /// Actual retransmission bytes sent during execution.
    pub bytes_non_probe_rtx: i64,
    /// Executor completion marker, preserved as upstream metadata.
    pub is_completed: bool,
}
#[derive(Clone, Copy, Debug)]
pub(super) struct ProbeInfo {
    pub id: u32,
    pub created_at: Instant,
    pub goal: ProbeGoal,
    pub result: ProbeResult,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    None,
    Probing,
    Hangover,
}
pub(super) struct ProbeController {
    config: RemoteProbeControllerConfig,
    state: State,
    switched: Instant,
    info: Option<ProbeInfo>,
    rtt: f64,
    interval: PolicyDuration,
    duration: PolicyDuration,
    earliest: Instant,
}
impl ProbeController {
    pub fn new(config: RemoteProbeControllerConfig, now: Instant) -> Self {
        Self {
            interval: config.probe_regulator.base_interval,
            duration: config.probe_regulator.min_duration,
            config,
            state: State::None,
            switched: now,
            info: None,
            rtt: 0.070,
            earliest: now,
        }
    }
    pub fn update_rtt(&mut self, rtt: f64) {
        if rtt == 0.0 {
            self.rtt = 0.070;
        } else if self.rtt == 0.0 {
            self.rtt = rtt;
        } else {
            self.rtt = 0.5 * rtt + 0.5 * self.rtt;
        }
    }
    pub fn can_probe(&self, now: Instant) -> bool {
        self.state == State::None && now > self.earliest
    }
    pub fn in_probe(&self) -> bool {
        self.state != State::None
    }
    pub fn duration(&self) -> PolicyDuration {
        self.duration
    }
    pub fn start(&mut self, info: ProbeInfo, now: Instant) {
        self.set_state(State::Probing, now);
        self.info = Some(info);
    }
    pub fn done(&mut self, id: u32, result: ProbeResult, now: Instant) {
        if let Some(info) = self.info.as_mut()
            && info.id == id
        {
            info.result = result;
            self.set_state(State::Hangover, now);
        }
    }
    fn set_state(&mut self, state: State, now: Instant) {
        if self.state != state {
            self.state = state;
            self.switched = now;
        }
    }
    pub fn goal_reached(&self, estimate: i64) -> bool {
        self.info
            .is_some_and(|info| info.id != 0 && estimate > info.goal.desired_bps)
    }
    pub fn finalize(&mut self, now: Instant) -> Option<ProbeInfo> {
        if self.state != State::Hangover {
            return None;
        }
        let mut wait =
            float_i64(self.config.settle_wait_num_rtt as f64 * self.rtt * 1_000_000_000.0);
        if wait < self.config.settle_wait_min.as_nanos_i64() {
            wait = self.config.settle_wait_min.as_nanos_i64();
        }
        if wait > self.config.settle_wait_max.as_nanos_i64() {
            wait = self.config.settle_wait_max.as_nanos_i64();
        }
        if elapsed(now, self.switched) < wait {
            return None;
        }
        self.set_state(State::None, now);
        self.info
    }
    pub fn signal(&mut self, signal: ProbeSignal, base: Option<Instant>, now: Instant) {
        let cfg = &self.config.probe_regulator;
        if signal == ProbeSignal::Congesting {
            self.interval = PolicyDuration::from_nanos(scaled_millis(
                self.interval.as_nanos_i64(),
                cfg.backoff_factor,
            ));
            if self.interval.as_nanos_i64() > cfg.max_interval.as_nanos_i64() {
                self.interval = cfg.max_interval;
            }
            self.duration = cfg.min_duration;
        } else {
            self.interval = cfg.base_interval;
            self.duration = PolicyDuration::from_nanos(scaled_millis(
                self.duration.as_nanos_i64(),
                cfg.duration_increase_factor,
            ));
            if self.duration.as_nanos_i64() > cfg.max_duration.as_nanos_i64() {
                self.duration = cfg.max_duration;
            }
        }
        self.earliest = offset(base.unwrap_or(now), self.interval.as_nanos_i64());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtc_core::config::RemoteBweConfig;
    use std::time::Duration;

    #[test]
    fn regulator_backoff_is_based_on_creation_and_strict_deadline() {
        let now = Instant::now();
        let mut p = ProbeController::new(RemoteBweConfig::default().probe_controller, now);
        p.signal(
            ProbeSignal::Congesting,
            Some(now),
            now + Duration::from_secs(1),
        );
        assert_eq!(p.duration().as_nanos_i64(), 200_000_000);
        assert!(!p.can_probe(now + Duration::from_millis(4500)));
        assert!(p.can_probe(now + Duration::from_millis(4500) + Duration::from_nanos(1)));
        p.signal(
            ProbeSignal::NotCongesting,
            Some(now),
            now + Duration::from_secs(2),
        );
        assert_eq!(p.duration().as_nanos_i64(), 300_000_000);
        assert!(!p.can_probe(now + Duration::from_secs(3)));
        assert!(p.can_probe(now + Duration::from_secs(3) + Duration::from_nanos(1)));
    }

    #[test]
    fn stale_completion_does_not_start_hangover_and_settle_is_inclusive() {
        let now = Instant::now();
        let mut p = ProbeController::new(RemoteBweConfig::default().probe_controller, now);
        p.start(
            ProbeInfo {
                id: 2,
                created_at: now,
                goal: ProbeGoal::default(),
                result: ProbeResult::default(),
            },
            now,
        );
        p.done(1, ProbeResult::default(), now);
        assert!(p.finalize(now + Duration::from_secs(1)).is_none());
        p.done(
            2,
            ProbeResult {
                bytes_probe: 37,
                ..Default::default()
            },
            now,
        );
        assert!(p.finalize(now + Duration::from_millis(349)).is_none());
        assert_eq!(
            p.finalize(now + Duration::from_millis(350))
                .unwrap()
                .result
                .bytes_probe,
            37
        );
    }
}
