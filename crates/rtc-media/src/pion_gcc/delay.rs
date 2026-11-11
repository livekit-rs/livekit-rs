// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT
//! Source port: interceptor v0.1.49 pkg/gcc/{arrival_group*,kalman,
//! slope_estimator,adaptive_threshold,overuse_detector,rate_controller}.go.
//! Durations are signed nanoseconds. Wall clocks are explicit; no timer/task.
use super::{
    Ack, MS, RateState, SECOND, Stats, Timestamp, Usage, duration, go_int, source_mul_add,
};
use std::collections::VecDeque;
#[derive(Clone, Copy, Debug)]
pub(super) struct Group {
    pub departure: Timestamp,
    pub arrival: Timestamp,
    pub packets: usize,
}
pub(super) struct Groups {
    current: Option<Group>,
}
impl Groups {
    pub fn new() -> Self {
        Self { current: None }
    }
    pub fn push(&mut self, ack: Ack) -> Option<Group> {
        // Go time.Time{} is an actual zero-arrival sentinel, not filtered here.
        let arrival = ack.arrival.unwrap_or(super::ZERO_TIME);
        let departure = ack.departure.unwrap_or(super::ZERO_TIME);
        let Some(group) = self.current.as_mut() else {
            self.current = Some(Group {
                departure,
                arrival,
                packets: 1,
            });
            return None;
        };
        if arrival < group.arrival {
            return None;
        }
        if departure > group.departure {
            let dd = duration(departure, group.departure);
            let da = duration(arrival, group.arrival);
            if dd <= 5 * MS || (da <= 5 * MS && da - dd < 0) {
                group.arrival = arrival;
                group.packets += 1;
                return None;
            }
            return self.current.replace(Group {
                departure,
                arrival,
                packets: 1,
            });
        }
        None
    }
}
#[derive(Debug)]
pub(super) struct Kalman {
    pub estimate: i64,
    pub error: f64,
    pub uncertainty: f64,
    pub gain: f64,
}
impl Kalman {
    pub fn new() -> Self {
        Self {
            estimate: 0,
            error: 0.1,
            uncertainty: 0.0,
            gain: 0.0,
        }
    }
    pub fn update(&mut self, measurement: i64) -> i64 {
        let z = measurement.wrapping_sub(self.estimate);
        let zms = (z / 1000) as f64 / 1000.0;
        let alpha = (1.0f64 - 0.001).powf(30.0 / (1000.0 * 5.0 * MS as f64));
        let root3 = 3.0 * self.uncertainty.sqrt();
        // Preserve Go's left-associative (1-alpha)*sample*sample, not
        // (1-alpha)*(sample*sample). Actual Go1.26.3 ARM64 selects the
        // alpha*previous left product for FMA when both sides are products;
        // default GOAMD64=v1 leaves the products/addition separate.
        let sample = if zms > root3 { root3 } else { zms };
        let weighted = (1.0 - alpha) * sample;
        self.uncertainty = source_mul_add(alpha, self.uncertainty, weighted * sample).max(1.0);
        let error = self.error + 1e-3;
        self.gain = error / (error + self.uncertainty);
        self.estimate = self
            .estimate
            .wrapping_add(go_int(self.gain * zms * MS as f64));
        self.error = (1.0 - self.gain) * error;
        self.estimate
    }
}
#[derive(Debug)]
pub(super) struct Threshold {
    pub threshold: i64,
    pub deltas: i64,
    last: Option<Timestamp>,
}
impl Threshold {
    pub fn new() -> Self {
        Self {
            threshold: 12_500_000,
            deltas: 0,
            last: None,
        }
    }
    pub fn compare(&mut self, estimate: i64, now: Timestamp) -> (Usage, i64, i64) {
        self.deltas = self.deltas.wrapping_add(1);
        if self.deltas < 2 {
            return (Usage::Normal, estimate, 600 * MS);
        }
        let scaled = self.deltas.min(60).wrapping_mul(estimate);
        let usage = if scaled > self.threshold {
            Usage::Over
        } else if scaled < -self.threshold {
            Usage::Under
        } else {
            Usage::Normal
        };
        let threshold = self.threshold;
        let last = *self.last.get_or_insert(now);
        let abs = estimate_scaled_abs_us(scaled).wrapping_mul(1000);
        if abs > self.threshold + 15 * MS {
            self.last = Some(now);
            return (usage, scaled, threshold);
        }
        let k = if abs < self.threshold { 0.00018 } else { 0.01 };
        let dt = (duration(now, last) / MS).min(100);
        let difference = abs.wrapping_sub(self.threshold) / MS;
        self.threshold = self
            .threshold
            .wrapping_add(go_int(k * difference as f64 * dt as f64 * 1000.0).wrapping_mul(1000));
        self.threshold = self.threshold.clamp(6 * MS, 600 * MS);
        self.last = Some(now);
        (usage, scaled, threshold)
    }
}
fn estimate_scaled_abs_us(value: i64) -> i64 {
    go_int(((value / 1000) as f64).abs())
}
#[derive(Debug)]
pub(super) struct Overuse {
    threshold: Threshold,
    last: Timestamp,
    pub last_estimate: i64,
    pub increasing: i64,
    pub count: i64,
}
impl Overuse {
    pub fn new(now: Timestamp) -> Self {
        Self {
            threshold: Threshold::new(),
            last: now,
            last_estimate: 0,
            increasing: 0,
            count: 0,
        }
    }
    pub fn update(&mut self, mut stats: Stats, now: Timestamp) -> Stats {
        let delta = duration(now, self.last);
        self.last = now;
        let (threshold_use, estimate, threshold) = self.threshold.compare(stats.estimate, now);
        let mut usage = Usage::Normal;
        match threshold_use {
            Usage::Over => {
                if self.increasing == 0 {
                    self.increasing = delta / 2
                } else {
                    self.increasing = self.increasing.wrapping_add(delta)
                };
                self.count = self.count.wrapping_add(1);
                if self.increasing > 10 * MS && self.count > 1 && estimate > self.last_estimate {
                    usage = Usage::Over
                }
            }
            Usage::Under => {
                self.count = 0;
                self.increasing = 0;
                usage = Usage::Under
            }
            Usage::Normal => {
                self.count = 0;
                self.increasing = 0
            }
        }
        self.last_estimate = estimate;
        stats.usage = usage;
        stats.estimate = estimate;
        stats.threshold = threshold;
        stats
    }
}
#[derive(Debug, Default)]
struct Ema {
    mean: f64,
    variance: f64,
    deviation: f64,
}
impl Ema {
    fn update(&mut self, value: f64) {
        if self.mean == 0.0 {
            self.mean = value
        } else {
            let x = value - self.mean;
            self.mean = source_mul_add(0.95, x, self.mean);
            self.variance = 0.05 * source_mul_add(0.95 * x, x, self.variance);
            self.deviation = self.variance.sqrt()
        }
    }
}
#[derive(Debug)]
pub(super) struct Rate {
    pub target: i64,
    pub received: i64,
    pub rtt: i64,
    pub last_stats: Stats,
    initialized: bool,
    last: Option<Timestamp>,
    decrease: Ema,
    min: i64,
    max: i64,
}
impl Rate {
    pub fn new(initial: i64, min: i64, max: i64) -> Self {
        Self {
            target: initial,
            received: 0,
            rtt: 0,
            last_stats: Stats::default(),
            initialized: false,
            last: None,
            decrease: Ema::default(),
            min,
            max,
        }
    }
    pub fn update(&mut self, mut stats: Stats, now: Timestamp) -> Option<Stats> {
        if !self.initialized {
            stats.state = RateState::Increase;
            self.last_stats = stats;
            self.initialized = true;
            return None;
        }
        // Preserve upstream onDelayStats assignment: incoming State is zero Increase,
        // before transition. Do not silently repair it to previous state.
        stats.state = stats.state.transition(stats.usage);
        self.last_stats = stats;
        if stats.state == RateState::Hold {
            return None;
        }
        self.target = match stats.state {
            RateState::Increase => self.increase(now),
            RateState::Decrease => {
                self.decrease.update(self.received as f64);
                self.last = Some(now);
                go_int(0.85 * self.received as f64)
            }
            RateState::Hold => unreachable!(),
        }
        .clamp(self.min, self.max);
        stats.target = self.target;
        Some(stats)
    }
    fn increase(&mut self, now: Timestamp) -> i64 {
        let elapsed = self.last.map_or(i64::MAX, |at| duration(now, at));
        let result = if self.decrease.mean > 0.0
            && self.received as f64 > self.decrease.mean - 3.0 * self.decrease.deviation
            && (self.received as f64) < self.decrease.mean + 3.0 * self.decrease.deviation
        {
            let bits = self.target as f64 / 30.0;
            let packets = (bits / (1200.0 * 8.0)).ceil();
            let expected = bits / packets;
            let alpha = 0.5
                * ((elapsed / MS) as f64 / ((100 * MS).wrapping_add(self.rtt) / MS) as f64)
                    .min(1.0);
            go_int(
                ((self
                    .target
                    .wrapping_add(go_int((alpha * expected).max(1000.0)))) as f64)
                    .min(1.5 * self.received as f64),
            )
        } else {
            let eta = 1.08f64.powf(((elapsed / MS) as f64 / 1000.0).min(1.0));
            let rate = go_int(eta * self.target as f64);
            let received = go_int(1.5 * self.received as f64);
            if rate > received && received > self.target {
                received
            } else {
                rate.max(self.target)
            }
        };
        self.last = Some(now);
        result
    }
}
/// Exact received-byte window, including each endpoint packet as upstream.
pub(super) struct Received {
    history: VecDeque<Ack>,
    sum: i64,
    initialized: bool,
}
impl Received {
    pub fn new() -> Self {
        Self {
            history: VecDeque::new(),
            sum: 0,
            initialized: false,
        }
    }
    pub fn push(&mut self, ack: Ack) -> Option<i64> {
        let arrival = ack.arrival?;
        self.history.push_back(ack);
        self.sum = self.sum.wrapping_add(ack.size);
        if !self.initialized {
            self.initialized = true;
            return Some(ack.size.wrapping_mul(8));
        }
        while self
            .history
            .front()
            .is_some_and(|v| v.arrival.unwrap() < arrival - i128::from(SECOND / 2))
        {
            self.sum = self
                .sum
                .wrapping_sub(self.history.pop_front().unwrap().size)
        }
        Some(self.history.front().map_or(0, |first| {
            go_int(
                self.sum.wrapping_mul(8) as f64
                    / (duration(arrival, first.arrival.unwrap()) as f64 / SECOND as f64),
            )
        }))
    }
}
/// Complete source delay stages. Arrival grouping retains its pending last group.
pub(super) struct Delay {
    groups: Groups,
    previous: Option<Group>,
    kalman: Kalman,
    overuse: Overuse,
    pub rate: Rate,
    received: Received,
}
impl Delay {
    pub fn new(initial: i64, min: i64, max: i64, now: Timestamp) -> Self {
        Self {
            groups: Groups::new(),
            previous: None,
            kalman: Kalman::new(),
            overuse: Overuse::new(now),
            rate: Rate::new(initial, min, max),
            received: Received::new(),
        }
    }
    pub fn groups(&mut self, acks: &[Ack], now: Timestamp) -> Vec<Stats> {
        let mut output = Vec::new();
        for &ack in acks {
            if let Some(group) = self.groups.push(ack)
                && let Some(stats) = self.group(group, now)
            {
                output.push(stats)
            }
        }
        output
    }
    pub fn rates(&mut self, acks: &[Ack]) {
        for &ack in acks {
            if let Some(rate) = self.received.push(ack) {
                self.rate.received = rate
            }
        }
    }
    pub fn group(&mut self, group: Group, now: Timestamp) -> Option<Stats> {
        let previous = self.previous.replace(group)?;
        let delta = duration(group.arrival, previous.arrival);
        let measurement = delta.wrapping_sub(duration(group.departure, previous.departure));
        let estimate = self.kalman.update(measurement);
        let stats = self.overuse.update(
            Stats {
                measurement,
                estimate,
                receive_delta: delta,
                ..Stats::default()
            },
            now,
        );
        self.rate.update(stats, now)
    }
}

#[cfg(test)]
#[path = "numeric_oracle.rs"]
mod numeric_oracle;
