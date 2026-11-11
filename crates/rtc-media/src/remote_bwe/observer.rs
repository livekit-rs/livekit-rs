// Copyright 2023 LiveKit, Inc.
// SPDX-License-Identifier: Apache-2.0
// Port of fixed LiveKit remotebwe/channel_observer.go and nack_tracker.go.
use super::time::elapsed;
use super::trend::{Direction, Trend};
use rtc_core::config::{ChannelObserverConfig, NackTrackerConfig};
use std::time::Instant;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Reason {
    None,
    Estimate,
    Loss,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ChannelTrend {
    Inconclusive,
    Clearing,
    Congesting,
}
struct Nacks {
    config: NackTrackerConfig,
    start: Option<Instant>,
    packets: u32,
    repeated: u32,
}
impl Nacks {
    fn new(config: NackTrackerConfig) -> Self {
        Self {
            config,
            start: None,
            packets: 0,
            repeated: 0,
        }
    }
    fn add(&mut self, packets: u32, repeated: u32, now: Instant) {
        if self.config.window_max_duration.as_nanos_i64() != 0
            && self
                .start
                .is_some_and(|t| elapsed(now, t) > self.config.window_max_duration.as_nanos_i64())
        {
            self.start = None;
            self.packets = 0;
            self.repeated = 0;
        }
        if self.repeated == 0 && repeated != 0 {
            self.start = Some(now);
        }
        if self.start.is_some() {
            self.packets = self.packets.wrapping_add(packets);
            self.repeated = self.repeated.wrapping_add(repeated);
        }
    }
    fn ratio(&self) -> f64 {
        if self.packets == 0 {
            0.0
        } else {
            (self.repeated as f64 / self.packets as f64).min(1.0)
        }
    }
    fn triggered(&self, now: Instant) -> bool {
        self.config.window_min_duration.as_nanos_i64() != 0
            && self
                .start
                .is_some_and(|t| elapsed(now, t) > self.config.window_min_duration.as_nanos_i64())
            && self.ratio() > self.config.ratio_threshold
    }
}
pub(super) struct Observer {
    trend: Trend,
    nacks: Nacks,
}
impl Observer {
    pub fn new(config: ChannelObserverConfig) -> Self {
        Self {
            trend: Trend::new(config.estimate),
            nacks: Nacks::new(config.nack),
        }
    }
    pub fn add(&mut self, estimate: i64, packets: u32, repeated: u32, now: Instant) {
        self.trend.add(estimate, now);
        self.nacks.add(packets, repeated, now);
    }
    pub fn state(&self, now: Instant) -> (ChannelTrend, Reason) {
        if self.trend.direction() == Direction::Downward {
            (ChannelTrend::Congesting, Reason::Estimate)
        } else if self.nacks.triggered(now) {
            (ChannelTrend::Congesting, Reason::Loss)
        } else if self.trend.direction() == Direction::Upward {
            (ChannelTrend::Clearing, Reason::None)
        } else {
            (ChannelTrend::Inconclusive, Reason::None)
        }
    }
    pub fn ratio(&self) -> f64 {
        self.nacks.ratio()
    }
    pub fn highest(&self) -> i64 {
        self.trend.highest()
    }
    pub fn enough(&self) -> bool {
        self.trend.enough()
    }
}
