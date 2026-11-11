// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT
//! Actual Pion interceptor v0.1.49 estimator arithmetic, separate from str0m GCC.
//! Explicit signed-nanosecond clocks replace source wall-clock reads only.
//! Stage ordering is exposed because the original two delay goroutines do not
//! define a unique interleaving. Runtime chooses a documented serial order;
//! independent stage-source oracles must validate it before activation.
mod adapter;
mod delay;
pub use adapter::{Adapter, Chunk, FeedbackError, Rfc8888Block};
const MS: i64 = 1_000_000;
const SECOND: i64 = 1_000_000_000;
/// Nanoseconds since Go's year-one zero. i128 avoids overflow in real local
/// dates; duration differences saturate exactly as time.Time.Sub.
pub type Timestamp = i128;
const ZERO_TIME: Timestamp = 0;
fn duration(a: Timestamp, b: Timestamp) -> i64 {
    (a - b).clamp(i64::MIN as i128, i64::MAX as i128) as i64
}
/// Fixed Go1.26.3 default ARM64 SSA uses a fused right-product addition.
/// Default GOAMD64=v1 uses separate operations. Other custom compiler profiles
/// require their own source oracle; this is not a cross-compiler bit promise.
fn source_mul_add(x: f64, y: f64, add: f64) -> f64 {
    #[cfg(target_arch = "aarch64")]
    {
        x.mul_add(y, add)
    }
    #[cfg(not(target_arch = "aarch64"))]
    {
        add + x * y
    }
}
/// Source acknowledgment, including its real unmatched/default and lost states.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Ack {
    pub sequence: u16,
    pub ssrc: u32,
    pub size: i64,
    pub departure: Option<Timestamp>,
    pub arrival: Option<Timestamp>,
    pub ecn: u8,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Usage {
    #[default]
    Over,
    Under,
    Normal,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RateState {
    #[default]
    Increase,
    Decrease,
    Hold,
}
impl RateState {
    fn transition(self, usage: Usage) -> Self {
        match (self, usage) {
            (_, Usage::Over) => Self::Decrease,
            (Self::Decrease, _) | (_, Usage::Under) => Self::Hold,
            (_, Usage::Normal) => Self::Increase,
        }
    }
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    pub measurement: i64,
    pub estimate: i64,
    pub threshold: i64,
    pub receive_delta: i64,
    pub usage: Usage,
    pub state: RateState,
    pub target: i64,
}
/// Go implementation-defined out-of-range float->int behavior must be replayed
/// on each target. Normal finite representable values simply truncate.
fn go_int(value: f64) -> i64 {
    #[cfg(target_arch = "x86_64")]
    {
        if !value.is_finite()
            || value >= 9_223_372_036_854_775_808.0
            || value < -9_223_372_036_854_775_808.0
        {
            return i64::MIN;
        }
    }
    value as i64
}
#[derive(Debug)]
struct Loss {
    bitrate: i64,
    average: f64,
    last: Option<Timestamp>,
    increase: Option<Timestamp>,
    decrease: Option<Timestamp>,
}
impl Loss {
    fn new(initial: i64) -> Self {
        Self {
            bitrate: initial,
            average: 0.0,
            last: None,
            increase: None,
            decrease: None,
        }
    }
    fn update(&mut self, acks: &[Ack], now: Timestamp) {
        if acks.is_empty() {
            return;
        }
        let ratio = acks.iter().filter(|v| v.arrival.is_none()).count() as f64 / acks.len() as f64;
        let delta = self.last.map_or(i64::MAX, |last| duration(now, last));
        let weight = (-(delta / MS) as f64 / 200.0).exp();
        self.average = source_mul_add(weight, self.average - ratio, ratio);
        self.last = Some(now);
        if self.average.max(ratio) < 0.02
            && self.increase.is_none_or(|at| duration(now, at) > 200 * MS)
        {
            self.increase = Some(now);
            self.bitrate = go_int(1.05 * self.bitrate as f64).clamp(100_000, 100_000_000);
        } else if self.average.min(ratio) > 0.1
            && self.decrease.is_none_or(|at| duration(now, at) > 200 * MS)
        {
            self.decrease = Some(now);
            self.bitrate = go_int(self.bitrate as f64 * (1.0 - 0.5 * self.average.min(ratio)))
                .clamp(100_000, 100_000_000);
        }
    }
    fn estimate(&mut self, wanted: i64) -> i64 {
        if self.bitrate <= 0 {
            self.bitrate = wanted.clamp(100_000, 100_000_000)
        }
        self.bitrate = self.bitrate.min(wanted);
        self.bitrate
    }
}
/// Protocol-specific report-sent reference used by the source RTT equation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FeedbackTime {
    /// TWCC uses the maximum acknowledgment arrival, including source defaults.
    Twcc,
    /// RFC8888 uses the actual report NTP timestamp, not max packet arrival.
    Rfc8888 { report_time: Timestamp },
}
/// Original source controllers with 1Mbps LiveKit startup and 5kbps/50Mbps
/// delay bounds. This type contains no native estimator or normal-media pacer.
pub struct Estimator {
    delay: delay::Delay,
    loss: Loss,
    target: i64,
    pub latest: Stats,
    closed: bool,
}
impl Estimator {
    pub fn new(now_ns: Timestamp) -> Self {
        Self {
            delay: delay::Delay::new(1_000_000, 5_000, 50_000_000, now_ns),
            loss: Loss::new(1_000_000),
            target: 1_000_000,
            latest: Stats::default(),
            closed: false,
        }
    }
    pub fn target(&self) -> i64 {
        self.target
    }
    pub fn close(&mut self) {
        self.closed = true
    }
    /// TWCC report: source min RTT uses maximum ACK arrival. For RFC8888,
    /// callers must use `feedback_timed` with the actual report reference.
    pub fn feedback(&mut self, acks: &[Ack], now_ns: Timestamp) -> Result<Vec<i64>, FeedbackError> {
        self.feedback_timed(acks, FeedbackTime::Twcc, now_ns)
    }
    /// Last matched report RTT, retaining signed source arithmetic.
    pub fn latest_rtt(&self) -> i64 {
        self.delay.rate.rtt
    }
    /// Full source report. Callback list contains only actual target changes.
    /// RFC8888 report timing is explicit and never inferred from ACK arrivals.
    pub fn feedback_timed(
        &mut self,
        acks: &[Ack],
        timing: FeedbackTime,
        now_ns: Timestamp,
    ) -> Result<Vec<i64>, FeedbackError> {
        if self.closed {
            return Err(FeedbackError::Closed);
        }
        let feedback_sent = match timing {
            FeedbackTime::Twcc => acks.iter().map(|a| a.arrival.unwrap_or(ZERO_TIME)).max(),
            FeedbackTime::Rfc8888 { report_time } => Some(report_time),
        };
        if let Some(last) = feedback_sent
            && let Some(rtt) = acks
                .iter()
                .filter_map(|a| {
                    Some(
                        duration(now_ns, a.departure.unwrap_or(ZERO_TIME))
                            .wrapping_sub(duration(last, a.arrival?)),
                    )
                })
                .min()
        {
            self.delay.rate.rtt = rtt
        }
        self.loss.update(acks, now_ns);
        // Source run() pipelines race; serialize received-rate updates before each
        // group update in the owner. No numeric trajectory parity claim yet.
        self.delay.rates(acks);
        let stats = self.delay.groups(acks, now_ns);
        let mut changes = Vec::new();
        for ds in stats {
            let target = self.loss.estimate(ds.target).min(ds.target);
            if target != self.target {
                self.target = target;
                changes.push(target)
            }
            self.latest = ds
        }
        Ok(changes)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn real_algorithm_has_source_startup_loss_and_change_only_lifecycle() {
        let mut e = Estimator::new(SECOND.into());
        assert_eq!(e.target(), 1_000_000);
        assert!(e.feedback(&[], SECOND.into()).unwrap().is_empty());
        let a = Ack {
            sequence: 1,
            size: 1200,
            departure: Some(SECOND.into()),
            arrival: None,
            ..Ack::default()
        };
        e.loss.update(&[a], SECOND.into());
        assert_eq!(e.loss.bitrate, 500_000);
        assert_eq!(e.loss.average, 1.0);
        e.close();
        assert_eq!(e.feedback(&[], SECOND.into()), Err(FeedbackError::Closed));
    }
    #[test]
    fn source_grouping_waits_for_next_group_and_rejects_arrival_reordering() {
        let mut g = delay::Groups::new();
        let a = |d, r| Ack {
            departure: Some(i128::from(d * MS)),
            arrival: Some(i128::from(r * MS)),
            ..Ack::default()
        };
        assert!(g.push(a(0, 10)).is_none());
        assert!(g.push(a(4, 14)).is_none());
        assert!(g.push(a(20, 13)).is_none());
        let group = g.push(a(20, 30)).unwrap();
        assert_eq!(group.departure, 0);
        assert_eq!(group.arrival, i128::from(14 * MS));
    }
    #[test]
    fn loss_exact_time_and_ratio_boundaries_are_not_fuzzy() {
        let mut l = Loss::new(1_000_000);
        let ack = Ack {
            arrival: Some(1),
            ..Ack::default()
        };
        l.update(&[ack], SECOND.into());
        assert_eq!(l.bitrate, 1_050_000);
        l.update(&[ack], (SECOND + 200 * MS).into());
        assert_eq!(l.bitrate, 1_050_000);
        l.update(&[ack], (SECOND + 200 * MS + 1).into());
        assert_eq!(l.bitrate, 1_102_500);
    }
}
#[cfg(test)]
mod oracle;

mod wire;
pub use wire::{Rfc8888Report, TwccReport, parse_rfc8888, parse_twcc, rfc8888_report_time};
#[cfg(test)]
mod adapter_oracle;

#[cfg(test)]
mod rfc8888_oracle;

mod compound;
pub use compound::{CompoundFeedback, CompoundReport, admit_compound, parse_compound};
