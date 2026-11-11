// Copyright 2023 LiveKit, Inc.
// SPDX-License-Identifier: Apache-2.0
// Port of e5655ac20267fa5005dccaf3f0e3bfc3ea688c07 ccutils/trenddetector.go.
use super::time::{elapsed, offset};
use rtc_core::config::TrendDetectorConfig;
use std::time::Instant;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Direction {
    Inconclusive,
    Upward,
    Downward,
}
#[derive(Clone, Copy, Debug)]
struct Sample {
    value: i64,
    at: Instant,
}
pub(super) struct Trend {
    config: TrendDetectorConfig,
    samples: Vec<Sample>,
    total_samples: i64,
    lowest: i64,
    highest: i64,
    direction: Direction,
}
impl Trend {
    pub fn new(config: TrendDetectorConfig) -> Self {
        Self {
            config,
            samples: Vec::new(),
            total_samples: 0,
            lowest: 0,
            highest: 0,
            direction: Direction::Inconclusive,
        }
    }
    pub fn add(&mut self, value: i64, now: Instant) {
        self.total_samples = self.total_samples.wrapping_add(1);
        if self.lowest == 0 || value < self.lowest {
            self.lowest = value;
        }
        if value > self.highest {
            self.highest = value;
        }
        if self.samples.last().is_some_and(|last| {
            last.value == value
                && self.config.collapse_threshold.as_nanos_i64() > 0
                && elapsed(now, last.at) < self.config.collapse_threshold.as_nanos_i64()
        }) {
            return;
        }
        self.samples.push(Sample { value, at: now });
        let required = usize::try_from(self.config.required_samples)
            .expect("negative required_samples is invalid upstream");
        if self.samples.len() > required {
            self.samples.drain(..self.samples.len() - required);
        }
        if self.config.validity_window.as_nanos_i64() > 0
            && !self.samples.is_empty()
            && let Some(index) = self
                .samples
                .iter()
                .position(|s| s.at > offset(now, -self.config.validity_window.as_nanos_i64()))
        {
            // Preserve the source's unusual all-expired behavior: it retains
            // the existing set when no sample is strictly after the cutoff.
            self.samples.drain(..index);
        }
        if let Some(first) = self.samples.first() {
            let cutoff = self
                .samples
                .iter()
                .skip(1)
                .position(|s| s.value != first.value)
                .unwrap_or_else(|| self.samples.len() - 1);
            self.samples.drain(..cutoff);
        }
        self.update();
    }
    fn tau(&self) -> f64 {
        let mut concordant = 0u64;
        let mut discordant = 0u64;
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
            0.0
        } else {
            (concordant as f64 - discordant as f64) / (concordant + discordant) as f64
        }
    }
    fn update(&mut self) {
        self.direction = Direction::Inconclusive;
        if (self.samples.len() as i64) < self.config.required_samples_min {
            return;
        }
        let tau = self.tau();
        if tau > 0.0 && (self.samples.len() as i64) >= self.config.required_samples {
            self.direction = Direction::Upward;
        } else if tau < self.config.downward_trend_threshold
            && ((self.samples.len() as i64) >= self.config.required_samples
                || self
                    .samples
                    .last()
                    .zip(self.samples.first())
                    .is_some_and(|(last, first)| {
                        elapsed(last.at, first.at)
                            > self.config.downward_trend_max_wait.as_nanos_i64()
                    }))
        {
            self.direction = Direction::Downward;
        }
    }
    pub fn direction(&self) -> Direction {
        self.direction
    }
    pub fn highest(&self) -> i64 {
        self.highest
    }
    pub fn enough(&self) -> bool {
        self.total_samples >= self.config.required_samples
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtc_core::config::{PolicyDuration, RemoteBweConfig};
    use std::time::Duration;

    #[test]
    fn collapsed_values_count_for_probe_sufficiency_without_inventing_trend() {
        let mut cfg = RemoteBweConfig::default().channel_observer_probe.estimate;
        cfg.required_samples = 3;
        cfg.required_samples_min = 3;
        cfg.collapse_threshold = PolicyDuration::from_secs(1);
        let now = Instant::now();
        let mut trend = Trend::new(cfg);
        for millis in [0, 1, 2] {
            trend.add(123, now + Duration::from_millis(millis));
        }
        assert!(trend.enough());
        assert_eq!(trend.samples.len(), 1);
        assert_eq!(trend.direction(), Direction::Inconclusive);
    }

    #[test]
    fn tied_front_pruning_and_lifetime_extrema_do_not_follow_sliding_window() {
        let mut cfg = RemoteBweConfig::default().channel_observer_probe.estimate;
        cfg.required_samples = 3;
        cfg.required_samples_min = 3;
        cfg.collapse_threshold = PolicyDuration::ZERO;
        cfg.validity_window = PolicyDuration::ZERO;
        let now = Instant::now();
        let mut trend = Trend::new(cfg);
        for (i, value) in [100, 100, 90, 80, 70].into_iter().enumerate() {
            trend.add(value, now + Duration::from_millis(i as u64));
        }
        assert_eq!(
            trend.samples.iter().map(|s| s.value).collect::<Vec<_>>(),
            [90, 80, 70]
        );
        assert_eq!(trend.highest(), 100);
        assert_eq!(trend.lowest, 70);
        assert_eq!(trend.direction(), Direction::Downward);
        assert_eq!(trend.tau(), -1.0);
    }
}
