// Copyright 2023 LiveKit, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Port of `ccutils/probe_regulator.go`.
//!
//! Upstream reads `mono.Now()` in the constructor, in `CanProbe` and again when a
//! signal carries a zero base time; every one of those reads is an explicit `now`
//! parameter here, so the scheduling state is reproducible from the caller clock.
//! Interval and duration growth keeps Go's millisecond quantisation: the factor is
//! applied to `Duration.Milliseconds()` and the product is re-multiplied by
//! `time.Millisecond`, so sub-millisecond history is discarded on every update.

use rtc_core::config::{PolicyDuration, ProbeRegulatorConfig};
use std::time::Instant;

use super::ccutils::ProbeSignal;
use super::time::{float_i64, offset};

/// `ccutils.ProbeRegulator`.
pub(crate) struct ProbeRegulator {
    config: ProbeRegulatorConfig,
    probe_interval: PolicyDuration,
    probe_duration: PolicyDuration,
    next_probe_earliest_at: Instant,
}

impl ProbeRegulator {
    /// `NewProbeRegulator`: the initial interval and duration are the configured
    /// base interval and minimum duration, and the first probe is allowed to start
    /// from the supplied creation clock.
    pub(crate) fn new(config: ProbeRegulatorConfig, now: Instant) -> Self {
        Self {
            probe_interval: config.base_interval,
            probe_duration: config.min_duration,
            next_probe_earliest_at: now,
            config,
        }
    }

    /// `CanProbe()`: `time.After` is a strict comparison, so a probe exactly at the
    /// earliest instant is still refused.
    pub(crate) fn can_probe(&self, now: Instant) -> bool {
        now > self.next_probe_earliest_at
    }

    /// `ProbeDuration()`.
    pub(crate) fn duration(&self) -> PolicyDuration {
        self.probe_duration
    }

    /// The scheduling instant, exposed for the owner's diagnostics and tests.
    pub(crate) fn next_probe_earliest_at(&self) -> Instant {
        self.next_probe_earliest_at
    }

    /// `ProbeSignal(signal, baseTime)`. `base_time` is `None` for Go's zero
    /// `time.Time`, which upstream replaces with a fresh clock read, so the caller
    /// must pass `now` in that case rather than a fabricated epoch.
    pub(crate) fn probe_signal(
        &mut self,
        signal: ProbeSignal,
        base_time: Option<Instant>,
        now: Instant,
    ) {
        if signal == ProbeSignal::Congesting {
            self.probe_interval = scaled_millis(
                self.probe_interval,
                self.config.backoff_factor,
                self.config.max_interval,
            );
            self.probe_duration = self.config.min_duration;
        } else {
            self.probe_interval = self.config.base_interval;
            self.probe_duration = scaled_millis(
                self.probe_duration,
                self.config.duration_increase_factor,
                self.config.max_duration,
            );
        }

        let from = base_time.unwrap_or(now);
        self.next_probe_earliest_at = offset(from, self.probe_interval.as_nanos_i64());
    }
}

/// `time.Duration(float64(d.Milliseconds()) * factor) * time.Millisecond`,
/// truncated to whole milliseconds then clamped when strictly above the maximum.
fn scaled_millis(value: PolicyDuration, factor: f64, max: PolicyDuration) -> PolicyDuration {
    let scaled = PolicyDuration::from_millis(float_i64(value.as_millis_i64() as f64 * factor));
    if scaled > max { max } else { scaled }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn regulator(now: Instant) -> ProbeRegulator {
        ProbeRegulator::new(ProbeRegulatorConfig::default(), now)
    }

    #[test]
    fn a_fresh_regulator_uses_the_base_interval_and_min_duration() {
        let now = Instant::now();
        let r = regulator(now);
        assert_eq!(r.duration(), PolicyDuration::from_millis(200));
        assert_eq!(r.next_probe_earliest_at(), now);
        assert!(
            !r.can_probe(now),
            "time.After is strict, so the creation instant itself cannot probe"
        );
        assert!(r.can_probe(now + Duration::from_micros(1)));
    }

    #[test]
    fn a_congesting_signal_backs_off_the_interval_and_reverts_the_duration() {
        let now = Instant::now();
        let mut r = regulator(now);
        r.probe_signal(ProbeSignal::NotCongesting, None, now);
        assert_eq!(r.duration(), PolicyDuration::from_millis(300));
        assert_eq!(r.next_probe_earliest_at(), now + Duration::from_secs(3));

        let at = now + Duration::from_secs(4);
        r.probe_signal(ProbeSignal::Congesting, Some(at), at);
        assert_eq!(
            r.duration(),
            PolicyDuration::from_millis(200),
            "a congesting probe restarts at the shortest duration"
        );
        assert_eq!(
            r.next_probe_earliest_at(),
            at + Duration::from_millis(4_500),
            "the backoff compounds on the millisecond-truncated base interval"
        );

        let at = at + Duration::from_secs(5);
        r.probe_signal(ProbeSignal::Congesting, Some(at), at);
        assert_eq!(
            r.next_probe_earliest_at(),
            at + Duration::from_millis(6_750),
            "4500ms * 1.5"
        );
    }

    #[test]
    fn growth_is_quantised_to_whole_milliseconds_like_the_go_cast() {
        let now = Instant::now();
        let mut r = regulator(now);
        // 200ms * 1.5 = 300ms, 300 * 1.5 = 450, 450 * 1.5 = 675, 675 * 1.5 = 1012
        // after four NotCongesting signals: the .5 truncation is observable.
        for _ in 0..4 {
            r.probe_signal(ProbeSignal::NotCongesting, Some(now), now);
        }
        assert_eq!(r.duration(), PolicyDuration::from_millis(1_012));
    }

    #[test]
    fn both_scales_saturate_at_their_configured_maxima() {
        let now = Instant::now();
        let mut config = ProbeRegulatorConfig::default();
        config.max_duration = PolicyDuration::from_millis(1_000);
        config.max_interval = PolicyDuration::from_secs(6);
        let mut r = ProbeRegulator::new(config, now);
        for _ in 0..40 {
            r.probe_signal(ProbeSignal::NotCongesting, Some(now), now);
        }
        assert_eq!(r.duration(), PolicyDuration::from_millis(1_000));
        assert_eq!(r.next_probe_earliest_at(), now + Duration::from_secs(3));
        for _ in 0..40 {
            r.probe_signal(ProbeSignal::Congesting, Some(now), now);
        }
        assert_eq!(r.next_probe_earliest_at(), now + Duration::from_secs(6));
        assert_eq!(r.duration(), PolicyDuration::from_millis(200));
    }

    #[test]
    fn an_inconclusive_signal_clears_backoff_the_way_upstream_branches() {
        let now = Instant::now();
        let mut r = regulator(now);
        r.probe_signal(ProbeSignal::Congesting, Some(now), now);
        assert_eq!(
            r.next_probe_earliest_at(),
            now + Duration::from_secs_f64(4.5)
        );
        r.probe_signal(ProbeSignal::Inconclusive, Some(now), now);
        assert_eq!(
            r.next_probe_earliest_at(),
            now + Duration::from_secs(3),
            "only Congesting takes the backoff branch"
        );
    }

    #[test]
    fn a_zero_base_time_reschedules_from_the_supplied_clock() {
        let now = Instant::now();
        let mut r = regulator(now);
        let later = now + Duration::from_secs(20);
        r.probe_signal(ProbeSignal::NotCongesting, None, later);
        assert_eq!(r.next_probe_earliest_at(), later + Duration::from_secs(3));
    }
}
