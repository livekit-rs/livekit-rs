// Copyright 2023 LiveKit, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Port of `ccutils/trenddetector.go` instantiated with `float64`, which is what
//! the congested-CTR trend of the send-side detector uses. The integer estimator
//! trend used by the remote path lives in `remote_bwe::trend`; both replay the
//! same upstream sample collapse, validity-window and Kendall-tau rules, so this
//! file keeps the float instantiation instead of widening that one to a generic.
//!
//! Upstream reads `time.Now()` inside `AddValue`; every clock read here is an
//! explicit `now` parameter supplied by the owner. `TrendDetector.Seed` is kept
//! out: the only upstream caller is the remote channel observer, which uses the
//! integer instantiation in `remote_bwe::trend`.

use super::time::{elapsed_nanos, offset};
// The campaign echoes the sample-list span, so that one reader only exists in the
// test build; naming it here keeps the release-shape import set exact.
#[cfg(test)]
use super::time::elapsed_micros;
use rtc_core::config::TrendDetectorConfig;
use std::time::Instant;

/// `ccutils.TrendDirection`. Upstream's zero value is `TrendDirectionInconclusive`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum TrendDirection {
    #[default]
    Inconclusive,
    Upward,
    Downward,
}

#[derive(Clone, Copy, Debug)]
struct Sample {
    value: f64,
    at: Instant,
}

/// `ccutils.TrendDetector[float64]`.
pub(crate) struct TrendDetector {
    config: TrendDetectorConfig,
    samples: Vec<Sample>,
    num_samples: i64,
    lowest_value: f64,
    highest_value: f64,
    direction: TrendDirection,
}

impl TrendDetector {
    pub(crate) fn new(config: TrendDetectorConfig) -> Self {
        Self {
            config,
            samples: Vec::new(),
            num_samples: 0,
            lowest_value: 0.0,
            highest_value: 0.0,
            direction: TrendDirection::Inconclusive,
        }
    }

    /// `Reset()`: upstream replaces the detector with a fresh one built from the
    /// same parameters, so lifetime extrema and the sample count restart too.
    #[cfg_attr(not(test), allow(dead_code))] // no shipped caller yet: the `#[cfg(test)]` oracle replay executes it; C74-e wires the pacer/prober/allocator consumer
    pub(crate) fn reset(&mut self) {
        *self = Self::new(self.config);
    }

    pub(crate) fn direction(&self) -> TrendDirection {
        self.direction
    }

    #[cfg_attr(not(test), allow(dead_code))] // no shipped caller yet: the `#[cfg(test)]` oracle replay executes it; C74-e wires the pacer/prober/allocator consumer
    pub(crate) fn highest(&self) -> f64 {
        self.highest_value
    }

    #[cfg_attr(not(test), allow(dead_code))] // no shipped caller yet: the `#[cfg(test)]` oracle replay executes it; C74-e wires the pacer/prober/allocator consumer
    pub(crate) fn lowest(&self) -> f64 {
        self.lowest_value
    }

    /// `HasEnoughSamples()`.
    #[cfg_attr(not(test), allow(dead_code))] // no shipped caller yet: the `#[cfg(test)]` oracle replay executes it; C74-e wires the pacer/prober/allocator consumer
    pub(crate) fn has_enough_samples(&self) -> bool {
        self.num_samples >= self.config.required_samples
    }

    /// `numSamples`, which counts every value offered, including the ones the
    /// collapse window refused to record, so it is not the sample list length.
    #[cfg(test)]
    pub(crate) fn num_samples(&self) -> i64 {
        self.num_samples
    }

    /// The recorded sample list, oldest first, as the campaign echoes it.
    #[cfg(test)]
    pub(crate) fn sample_values(&self) -> Vec<f64> {
        self.samples.iter().map(|sample| sample.value).collect()
    }

    /// The span between the first and last recorded sample in microseconds, which
    /// is the only way the campaign can see the stamps the collapse window and
    /// the validity window left in the list. Zero when the list is empty, like
    /// the oracle's `spanUs`.
    #[cfg(test)]
    pub(crate) fn sample_span_us(&self) -> i64 {
        match (self.samples.first(), self.samples.last()) {
            (Some(first), Some(last)) => elapsed_micros(last.at, first.at),
            _ => 0,
        }
    }

    /// `AddValue(value)`.
    pub(crate) fn add_value(&mut self, value: f64, now: Instant) {
        self.num_samples = self.num_samples.wrapping_add(1);
        if self.lowest_value == 0.0 || value < self.lowest_value {
            self.lowest_value = value;
        }
        if value > self.highest_value {
            self.highest_value = value;
        }

        let collapse = self.config.collapse_threshold.as_nanos_i64();
        if self.samples.last().is_some_and(|last| {
            last.value == value && collapse > 0 && elapsed_nanos(now, last.at) < collapse
        }) {
            return;
        }

        self.samples.push(Sample { value, at: now });
        self.prune(now);
        self.update_direction();
    }

    fn prune(&mut self, now: Instant) {
        // 1. keep at most `required_samples` newest values.
        let required = usize::try_from(self.config.required_samples)
            .expect("negative required_samples is invalid upstream");
        if self.samples.len() > required {
            self.samples.drain(..self.samples.len() - required);
        }

        // 2. drop samples that are too old, retaining the source's unusual
        // all-expired behaviour of keeping the whole set.
        let validity = self.config.validity_window.as_nanos_i64();
        if !self.samples.is_empty() && validity > 0 {
            let cutoff = offset(now, -validity);
            if let Some(index) = self.samples.iter().position(|s| s.at > cutoff) {
                self.samples.drain(..index);
            }
        }

        // 3. collapse a same-value front down to its last sample.
        if let Some(first_value) = self.samples.first().map(|s| s.value) {
            let changed = self
                .samples
                .iter()
                .skip(1)
                .position(|s| s.value != first_value)
                .map(|index| index + 1);
            match changed {
                Some(index) => {
                    self.samples.drain(..index - 1);
                }
                None if self.samples.len() > 1 => {
                    let keep = self.samples.len() - 1;
                    self.samples.drain(..keep);
                }
                _ => {}
            }
        }
    }

    /// `kendallsTau()`.
    fn kendalls_tau(&self) -> f64 {
        let mut concordant = 0i64;
        let mut discordant = 0i64;
        for (i, first) in self.samples.iter().enumerate() {
            for last in self.samples.iter().skip(i + 1) {
                if first.value < last.value {
                    concordant += 1;
                } else if first.value > last.value {
                    discordant += 1;
                }
            }
        }
        if concordant + discordant == 0 {
            return 0.0;
        }
        (concordant - discordant) as f64 / (concordant + discordant) as f64
    }

    fn update_direction(&mut self) {
        let len = self.samples.len() as i64;
        if len < self.config.required_samples_min {
            self.direction = TrendDirection::Inconclusive;
            return;
        }
        let tau = self.kendalls_tau();
        self.direction = TrendDirection::Inconclusive;
        if tau > 0.0 && len >= self.config.required_samples {
            self.direction = TrendDirection::Upward;
        } else if tau < self.config.downward_trend_threshold
            && (len >= self.config.required_samples
                || elapsed_nanos(self.samples[self.samples.len() - 1].at, self.samples[0].at)
                    > self.config.downward_trend_max_wait.as_nanos_i64())
        {
            self.direction = TrendDirection::Downward;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtc_core::config::{CongestionDetectorConfig, PolicyDuration};
    use std::time::Duration;

    fn ctr_config() -> TrendDetectorConfig {
        CongestionDetectorConfig::default().congested_ctr_trend
    }

    #[test]
    fn default_ctr_config_values_are_the_frozen_upstream_defaults() {
        let config = ctr_config();
        assert_eq!(config.required_samples, 4);
        assert_eq!(config.required_samples_min, 2);
        assert_eq!(config.downward_trend_threshold, -0.5);
        assert_eq!(config.downward_trend_max_wait, PolicyDuration::from_secs(2));
        assert_eq!(config.collapse_threshold, PolicyDuration::from_millis(500));
        assert_eq!(config.validity_window, PolicyDuration::from_secs(10));
    }

    #[test]
    fn two_descending_values_need_the_max_wait_before_they_are_downward() {
        // Oracle tr_dw: two samples 600ms apart are not enough, neither the
        // sample count nor the span reaches the congested-trend floor.
        let now = Instant::now();
        let mut t = TrendDetector::new(ctr_config());
        t.add_value(1.0, now);
        t.add_value(0.9, now + Duration::from_millis(600));
        assert_eq!(t.samples.len(), 2);
        assert_eq!(t.direction(), TrendDirection::Inconclusive);
        assert!(!t.has_enough_samples());

        // Oracle tr_dw2: the same two samples 2001ms apart cross
        // DownwardTrendMaxWait and become a downward trend on their own.
        let mut t = TrendDetector::new(ctr_config());
        t.add_value(1.0, now);
        t.add_value(0.9, now + Duration::from_millis(2_001));
        assert_eq!(t.direction(), TrendDirection::Downward);
        assert!(
            !t.has_enough_samples(),
            "the direction does not need the count"
        );
    }

    #[test]
    fn collapse_window_skips_repeats_until_it_elapses() {
        let now = Instant::now();
        let mut t = TrendDetector::new(ctr_config());
        t.add_value(1.0, now);
        t.add_value(1.0, now + Duration::from_millis(499));
        assert_eq!(t.samples.len(), 1);
        assert_eq!(t.num_samples, 2);
        // Oracle tr_clp: one second later the value is recorded, but the prune
        // pass collapses the all-equal front down to its newest sample.
        t.add_value(1.0, now + Duration::from_millis(500));
        assert_eq!(t.samples.len(), 1);
        assert_eq!(t.num_samples, 3);
        assert_eq!(
            t.samples[0].at,
            now + Duration::from_millis(500),
            "the surviving sample is the last one of the run"
        );
    }

    #[test]
    fn same_value_front_collapses_to_the_last_sample_of_the_run() {
        let now = Instant::now();
        // Oracle tr_cfront runs with the collapse and validity windows off, so
        // only the front-collapse rule can shorten the sample list.
        let mut config = ctr_config();
        config.collapse_threshold = PolicyDuration::ZERO;
        config.validity_window = PolicyDuration::ZERO;
        let mut t = TrendDetector::new(config);
        for (index, value) in [1.0, 1.0, 1.0, 0.5].into_iter().enumerate() {
            t.add_value(value, now + Duration::from_millis(index as u64 * 600));
        }
        assert_eq!(
            t.samples.iter().map(|s| s.value).collect::<Vec<_>>(),
            vec![1.0, 0.5]
        );
        assert_eq!(t.samples[1].at, now + Duration::from_millis(1_800));
        assert_eq!(
            elapsed_nanos(t.samples[1].at, t.samples[0].at),
            600_000_000,
            "the collapsed front keeps the newest timestamp, so the span is 600ms"
        );
        assert!(t.has_enough_samples(), "the raw sample counter is 4");
    }

    #[test]
    fn reset_restarts_direction_and_lifetime_extrema() {
        let now = Instant::now();
        let mut t = TrendDetector::new(ctr_config());
        t.add_value(1.0, now);
        t.add_value(0.5, now + Duration::from_millis(2_001));
        assert_eq!(t.direction(), TrendDirection::Downward);
        assert_eq!(t.highest(), 1.0);
        t.reset();
        assert_eq!(t.direction(), TrendDirection::Inconclusive);
        assert_eq!(t.highest(), 0.0);
        assert_eq!(t.lowest(), 0.0);
        assert_eq!(t.num_samples, 0);
        assert!(t.samples.is_empty());
    }

    #[test]
    fn upward_requires_the_full_required_sample_window() {
        let now = Instant::now();
        let mut t = TrendDetector::new(ctr_config());
        for (index, value) in [1.0, 1.1].into_iter().enumerate() {
            t.add_value(value, now + Duration::from_millis(index as u64 * 600));
        }
        assert_eq!(t.direction(), TrendDirection::Inconclusive);
        for (index, value) in [1.2, 1.3].into_iter().enumerate() {
            t.add_value(value, now + Duration::from_millis((index as u64 + 2) * 600));
        }
        assert_eq!(t.direction(), TrendDirection::Upward);
        assert!(t.has_enough_samples());
    }
}
