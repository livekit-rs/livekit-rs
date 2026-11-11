//! Source-compatible forwarding transit summaries, shared by every room on a node.
//! Producers report successful original-packet forwarding admission, not socket TX.
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const SUB: usize = 8;
const OCTAVES: usize = 27;
const BUCKETS: usize = SUB * OCTAVES;
const SHARDS: usize = 16;
const SHARD_CAP: usize = 8192;
/// Original Prometheus forwarding-latency histogram boundaries in nanoseconds.
pub const HISTOGRAM_BOUNDS_NS: [i64; 10] = [
    50_000, 100_000, 250_000, 500_000, 1_000_000, 2_000_000, 3_000_000, 5_000_000, 10_000_000,
    20_000_000,
];
#[derive(Clone, Copy, Debug, Default)]
struct Bucket {
    count: i64,
    min: i64,
    max: i64,
}
impl Bucket {
    fn add(&mut self, value: i64) {
        if self.count == 0 {
            self.min = value;
            self.max = value;
        } else {
            self.min = self.min.min(value);
            self.max = self.max.max(value);
        }
        self.count += 1;
    }
    fn merge(&mut self, other: Self) {
        if other.count == 0 {
            return;
        }
        if self.count == 0 {
            *self = other;
            return;
        }
        self.count += other.count;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
    }
}
#[derive(Clone, Debug)]
struct Summary {
    count: i64,
    min: i64,
    max: i64,
    buckets: [Bucket; BUCKETS],
}
impl Default for Summary {
    fn default() -> Self {
        Self {
            count: 0,
            min: 0,
            max: 0,
            buckets: [Bucket::default(); BUCKETS],
        }
    }
}
fn bucket(ns: i64) -> usize {
    let us = ns / 1000;
    if us <= 0 {
        return 0;
    }
    let e = 63 - (us as u64).leading_zeros() as usize;
    if e >= OCTAVES {
        return BUCKETS - 1;
    }
    let octave = 1i64 << e;
    e * SUB + (((us - octave) * SUB as i64) / octave) as usize
}
impl Summary {
    fn add(&mut self, ns: i64) {
        if self.count == 0 {
            self.min = ns;
            self.max = ns
        } else {
            self.min = self.min.min(ns);
            self.max = self.max.max(ns)
        }
        self.count += 1;
        self.buckets[bucket(ns)].add(ns);
    }
    fn merge(&mut self, other: &Self) {
        if other.count == 0 {
            return;
        }
        if self.count == 0 {
            *self = other.clone();
            return;
        }
        self.count += other.count;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        for (a, b) in self.buckets.iter_mut().zip(other.buckets) {
            a.merge(b);
        }
    }
    fn percentile(&self, p: f64) -> i64 {
        if self.count == 0 {
            return 0;
        }
        let target = p * self.count as f64;
        let mut cum = 0f64;
        for b in self.buckets {
            let c = b.count as f64;
            if c == 0.0 {
                continue;
            }
            if cum + c >= target {
                return (b.min as f64 + (b.max as f64 - b.min as f64) * (target - cum) / c) as i64;
            }
            cum += c;
        }
        self.max
    }
}
/// Actual cumulative histogram output plus the last reported rolling p90.
#[derive(Clone, Debug, Default)]
pub struct ForwardStatsSnapshot {
    pub latency_ns: u32,
    pub histogram_buckets: [u64; 10],
    pub histogram_count: u64,
    pub histogram_sum_ns: f64,
    pub dropped_samples: u64,
}
#[derive(Debug)]
struct State {
    samples: [VecDeque<i64>; SHARDS],
    dropped: u64,
    ring: Vec<Summary>,
    head: usize,
    len: usize,
    next_summary: Instant,
    next_report: Instant,
    output: ForwardStatsSnapshot,
}
/// A process/manager-scoped collector. Clone the Arc across room media engines;
/// never sum room percentiles. The runtime owns its periodic worker and stop/join.
#[derive(Debug)]
pub struct ForwardStats {
    summary: Duration,
    report: Duration,
    origin: Instant,
    state: Mutex<State>,
}
fn next_tick(deadline: Instant, period: Duration, now: Instant) -> Instant {
    let period_ns = period.as_nanos();
    let steps = now.saturating_duration_since(deadline).as_nanos() / period_ns + 1;
    let advance = period_ns * steps;
    let seconds = (advance / 1_000_000_000).min(u64::MAX as u128) as u64;
    let duration = Duration::new(seconds, (advance % 1_000_000_000) as u32);
    deadline
        .checked_add(duration)
        .or_else(|| now.checked_add(period))
        .unwrap_or(now)
}

impl ForwardStats {
    /// Match original activation and signed window arithmetic. Either ticker must
    /// be positive; a nonpositive report window still creates the original one slot.
    pub fn new(
        summary_ns: i64,
        report_ns: i64,
        window_ns: i64,
        now: Instant,
    ) -> Result<Arc<Self>, &'static str> {
        if summary_ns <= 0 || report_ns <= 0 {
            return Err("non-positive interval for forward stats ticker");
        }
        let cap = (window_ns.wrapping_add(summary_ns).wrapping_sub(1) / summary_ns).max(1) as usize;
        let mut ring = Vec::new();
        ring.try_reserve_exact(cap)
            .map_err(|_| "forward stats window capacity overflow")?;
        ring.resize_with(cap, Summary::default);
        let summary = Duration::from_nanos(summary_ns as u64);
        let report = Duration::from_nanos(report_ns as u64);
        let next_summary = now
            .checked_add(summary)
            .ok_or("forward stats summary deadline overflow")?;
        let next_report = now
            .checked_add(report)
            .ok_or("forward stats report deadline overflow")?;
        Ok(Arc::new(Self {
            summary,
            report,
            origin: now,
            state: Mutex::new(State {
                samples: std::array::from_fn(|_| VecDeque::new()),
                dropped: 0,
                ring,
                head: 0,
                len: 0,
                next_summary,
                next_report,
                output: Default::default(),
            }),
        }))
    }
    /// One sample for each admitted original packet, retaining its own arrival.
    /// Filtering source buffered/out-of-order/recovered packets is the caller's duty.
    pub fn record_forward_samples(
        &self,
        left: Instant,
        arrivals: impl IntoIterator<Item = Instant>,
    ) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        for arrival in arrivals {
            let ns = if left >= arrival {
                left.duration_since(arrival)
                    .as_nanos()
                    .min(i64::MAX as u128) as i64
            } else {
                -(arrival
                    .duration_since(left)
                    .as_nanos()
                    .min(i64::MAX as u128) as i64)
            };
            let key = arrival
                .checked_duration_since(self.origin)
                .map_or(0, |d| d.as_nanos() as u64);
            let shard = ((key >> 6) & 15) as usize;
            if state.samples[shard].len() == SHARD_CAP {
                state.samples[shard].pop_front();
                state.dropped += 1;
            }
            state.samples[shard].push_back(ns);
        }
    }
    fn flush(state: &mut State) {
        let mut summary = Summary::default();
        for shard in &mut state.samples {
            for ns in shard.drain(..) {
                summary.add(ns);
                state.output.histogram_count += 1;
                state.output.histogram_sum_ns += ns as f64;
                for (i, bound) in HISTOGRAM_BOUNDS_NS.iter().enumerate() {
                    if ns <= *bound {
                        state.output.histogram_buckets[i] += 1;
                    }
                }
            }
        }
        let head = state.head;
        state.ring[head] = summary;
        state.head = (head + 1) % state.ring.len();
        state.len = (state.len + 1).min(state.ring.len());
    }
    fn summarize(&self, state: &State, window_ns: i64) -> Summary {
        let mut n = state.len;
        if window_ns > 0 {
            let summary_ns = self.summary.as_nanos() as i64;
            let want =
                (window_ns.wrapping_add(summary_ns).wrapping_sub(1) / summary_ns).max(1) as usize;
            n = n.min(want);
        }
        let mut total = Summary::default();
        for i in 0..n {
            total.merge(&state.ring[(state.head + state.ring.len() - 1 - i) % state.ring.len()]);
        }
        total
    }
    /// Run due ticks. Like time.Ticker, late consumers coalesce elapsed ticks.
    pub fn tick(&self, now: Instant) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if now >= state.next_summary {
            Self::flush(&mut state);
            state.next_summary = next_tick(state.next_summary, self.summary, now);
        }
        if now >= state.next_report {
            let window = self.summarize(&state, 0);
            let p90 = window.percentile(0.9);
            state.output.latency_ns = p90 as u32;
            if state.dropped > 0 {
                tracing::warn!(
                    dropped = state.dropped,
                    "forward stats sample buffer overflow"
                );
                state.output.dropped_samples =
                    state.output.dropped_samples.saturating_add(state.dropped);
                state.dropped = 0;
            }
            if window.count > 0 && window.max > p90.wrapping_mul(10) {
                tracing::info!(
                    lowest_ns = window.min,
                    highest_ns = window.max,
                    count = window.count,
                    p90_ns = p90,
                    "high spread in forwarding path"
                );
            }
            state.next_report = next_tick(state.next_report, self.report, now);
        }
    }
    /// Duration is rounded up to complete summary spans; <=0 selects the full ring.
    pub fn get_stats_ns(&self, window_ns: i64) -> i64 {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        self.summarize(&state, window_ns).percentile(0.9)
    }
    pub fn snapshot(&self) -> ForwardStatsSnapshot {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .output
            .clone()
    }
    pub fn next_deadline(&self) -> Instant {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.next_summary.min(state.next_report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn original_bucket_interpolation_and_signed_extremes() {
        let mut s = Summary::default();
        for ns in [1_000_000, 1_000_000, 2_000_000, 3_000_000, 5_000_000] {
            s.add(ns)
        }
        assert_eq!(s.percentile(0.9), 5_000_000);
        assert_eq!(bucket(-1), 0);
        assert_eq!(bucket(134_217_728_000), 215);
        let mut crossing = Summary::default();
        crossing.add(1_000_000);
        crossing.add(1_010_000);
        assert_eq!(crossing.percentile(0.9), 1_009_000);
    }
    #[test]
    fn individual_arrivals_window_aging_and_real_histogram_count() {
        let now = Instant::now();
        let s = ForwardStats::new(100_000_000, 100_000_000, 250_000_000, now).unwrap();
        s.record_forward_samples(
            now + Duration::from_millis(5),
            [now, now + Duration::from_millis(4)],
        );
        s.tick(now + Duration::from_millis(100));
        assert_eq!(s.snapshot().histogram_count, 2);
        assert_eq!(s.snapshot().histogram_sum_ns, 6_000_000.0);
        assert_eq!(s.get_stats_ns(1), 5_000_000);
        for t in [200, 300, 400] {
            s.tick(now + Duration::from_millis(t));
        }
        assert_eq!(s.get_stats_ns(0), 0);
        assert_eq!(s.snapshot().latency_ns, 0);
        assert_eq!(s.snapshot().histogram_count, 2);
    }
    #[test]
    fn bounded_shard_reports_actual_overwritten_samples() {
        let now = Instant::now();
        let s = ForwardStats::new(1, 1, -1, now).unwrap();
        s.record_forward_samples(now, std::iter::repeat_n(now, SHARD_CAP + 3));
        s.tick(now + Duration::from_nanos(1));
        let output = s.snapshot();
        assert_eq!(output.histogram_count, SHARD_CAP as u64);
        assert_eq!(output.dropped_samples, 3);
        assert!(ForwardStats::new(-1, 1, 1, now).is_err());
        assert!(ForwardStats::new(1, 0, 1, now).is_err());
    }
    #[test]
    fn original_window_ceiling_and_uint32_node_output_are_preserved() {
        let now = Instant::now();
        let stats = ForwardStats::new(100_000_000, 100_000_000, 500_000_000, now).unwrap();
        for (slot, value) in (1..=5).zip((1..=5).rev()) {
            let left = now + Duration::from_millis(slot * 100);
            stats.record_forward_samples(left, [left - Duration::from_millis(value)]);
            stats.tick(left);
        }
        assert_eq!(stats.get_stats_ns(0), 5_000_000);
        assert_eq!(stats.get_stats_ns(1), 1_000_000);
        assert_eq!(stats.get_stats_ns(200_000_000), 2_000_000);
        let large = ForwardStats::new(1, 1, 1, now).unwrap();
        let left = now + Duration::from_secs(10);
        large.record_forward_samples(left, [now]);
        large.tick(left);
        assert_eq!(
            large.snapshot().latency_ns,
            1_410_065_408,
            "original RecordForwardLatency narrows nanoseconds to uint32"
        );
    }
    #[test]
    fn late_ticks_coalesce_without_drifting_the_original_timer_phase() {
        let now = Instant::now();
        let stats = ForwardStats::new(100_000_000, 100_000_000, 1_000_000_000, now).unwrap();
        stats.tick(now + Duration::from_millis(150));
        assert_eq!(stats.next_deadline(), now + Duration::from_millis(200));
        stats.tick(now + Duration::from_millis(260));
        assert_eq!(stats.next_deadline(), now + Duration::from_millis(300));
    }
}
