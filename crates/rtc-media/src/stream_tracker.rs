//! Port of fixed Go `pkg/sfu/streamtracker` packet tracker (v1.13.10,
//! `streamtracker.go` + `streamtracker_packet.go`), per simulcast spatial layer.
//!
//! A layer becomes active on its first packet after creation or `reset`. A
//! check runs every `cycle` from that moment: a cycle with at least `samples`
//! packets increments the cycle count, a shorter one clears it. A cleared
//! count marks the layer stopped; it only becomes active again after
//! `cycles` consecutive good cycles. With the video defaults a stopped base
//! layer needs 2 s and an enhancement layer 10 s of steady packets, which is
//! why fixed Go settles on the base layer after a publisher outage.
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PacketTrackerConfig {
    pub samples: u32,
    pub cycles: u32,
    pub cycle: Duration,
}

impl From<rtc_core::config::PacketTrackerSpec> for PacketTrackerConfig {
    fn from(spec: rtc_core::config::PacketTrackerSpec) -> Self {
        Self {
            samples: spec.samples_required,
            cycles: spec.cycles_required,
            cycle: spec.cycle_duration,
        }
    }
}

/// `DefaultStreamTrackerPacketConfigVideo`.
pub(crate) const VIDEO: [PacketTrackerConfig; 3] = [
    PacketTrackerConfig {
        samples: 1,
        cycles: 4,
        cycle: Duration::from_millis(500),
    },
    PacketTrackerConfig {
        samples: 5,
        cycles: 20,
        cycle: Duration::from_millis(500),
    },
    PacketTrackerConfig {
        samples: 5,
        cycles: 20,
        cycle: Duration::from_millis(500),
    },
];

#[derive(Clone, Copy, Debug)]
pub(crate) struct PacketTracker {
    config: PacketTrackerConfig,
    initialized: bool,
    count_since_last: u32,
    cycle_count: u32,
    active: bool,
    next_check: Option<Instant>,
}

impl PacketTracker {
    pub(crate) fn new(config: PacketTrackerConfig) -> Self {
        Self {
            config,
            initialized: false,
            count_since_last: 0,
            cycle_count: 0,
            active: false,
            next_check: None,
        }
    }

    pub(crate) fn set_config(&mut self, config: PacketTrackerConfig) {
        self.config = config;
    }

    /// `StreamTracker.Reset`: stopped until the next packet, which activates it.
    pub(crate) fn reset(&mut self) {
        *self = Self::new(self.config);
    }

    /// Run the due checks. Only the first interval can contain packets when no
    /// observation occurred between checks. Fold later empty intervals in one
    /// step so a tiny configured cycle cannot stall the room after an outage.
    fn advance(&mut self, now: Instant) {
        let Some(at) = self.next_check else {
            return;
        };
        if now < at || self.config.cycle.is_zero() {
            return;
        }
        let elapsed = now.duration_since(at).as_nanos();
        let cycle = self.config.cycle.as_nanos();
        let empty_cycles = elapsed / cycle;
        let remainder = elapsed % cycle;
        let remainder = Duration::new(
            (remainder / 1_000_000_000) as u64,
            (remainder % 1_000_000_000) as u32,
        );
        self.next_check = now.checked_add(self.config.cycle - remainder);

        if self.config.samples == 0 {
            let due = u32::try_from(empty_cycles + 1).unwrap_or(u32::MAX);
            self.cycle_count = self.cycle_count.saturating_add(due);
        } else if empty_cycles > 0 || self.count_since_last < self.config.samples {
            self.cycle_count = 0;
        } else {
            self.cycle_count = self.cycle_count.saturating_add(1);
        }
        if self.cycle_count == 0 {
            self.active = false;
        } else if self.cycle_count >= self.config.cycles {
            self.active = true;
        }
        self.count_since_last = 0;
    }

    /// `StreamTracker.Observe` for a packet with payload.
    pub(crate) fn observe(&mut self, now: Instant, packets: u64) {
        if packets == 0 {
            return;
        }
        self.advance(now);
        if !self.initialized {
            self.initialized = true;
            self.active = true;
            self.next_check = now.checked_add(self.config.cycle);
        }
        self.count_since_last = self
            .count_since_last
            .saturating_add(u32::try_from(packets).unwrap_or(u32::MAX));
    }

    /// Status at `now` without mutating: pending checks are applied to a copy.
    pub(crate) fn is_active(&self, now: Instant) -> bool {
        let mut copy = *self;
        copy.advance(now);
        copy.active
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn every(t: &mut PacketTracker, from: Instant, to: Instant, step: Duration) {
        let mut at = from;
        while at < to {
            t.observe(at, 1);
            at += step;
        }
    }

    #[test]
    fn first_packet_activates_and_one_short_cycle_stops() {
        let t0 = Instant::now();
        let mut t = PacketTracker::new(VIDEO[0]);
        assert!(!t.is_active(t0));
        t.observe(t0, 1);
        assert!(t.is_active(t0));
        // The activating packet counts for the first cycle (count 1 >= 1); the next,
        // empty cycle clears the count and stops the layer.
        assert!(t.is_active(t0 + Duration::from_millis(999)));
        assert!(!t.is_active(t0 + Duration::from_millis(1000)));
    }

    #[test]
    fn base_layer_reactivates_after_four_good_cycles() {
        let t0 = Instant::now();
        let mut t = PacketTracker::new(VIDEO[0]);
        every(
            &mut t,
            t0,
            t0 + Duration::from_secs(2),
            Duration::from_millis(33),
        );
        assert!(t.is_active(t0 + Duration::from_secs(2)));
        // 3 s outage.
        let back = t0 + Duration::from_secs(5);
        assert!(!t.is_active(back));
        // Ticks fall on t0 + k*500ms: good cycles end at 5.5, 6.0, 6.5 and 7.0 s.
        every(
            &mut t,
            back,
            back + Duration::from_millis(1600),
            Duration::from_millis(33),
        );
        assert!(!t.is_active(back + Duration::from_millis(1600)));
        every(
            &mut t,
            back + Duration::from_millis(1600),
            back + Duration::from_millis(2100),
            Duration::from_millis(33),
        );
        assert!(t.is_active(back + Duration::from_millis(2100)));
    }

    #[test]
    fn enhancement_layer_needs_ten_seconds_of_five_packets_per_cycle() {
        let t0 = Instant::now();
        let mut t = PacketTracker::new(VIDEO[2]);
        every(
            &mut t,
            t0,
            t0 + Duration::from_secs(2),
            Duration::from_millis(10),
        );
        let back = t0 + Duration::from_secs(5);
        every(
            &mut t,
            back,
            back + Duration::from_millis(9_400),
            Duration::from_millis(10),
        );
        assert!(!t.is_active(back + Duration::from_millis(9_400)));
        every(
            &mut t,
            back + Duration::from_millis(9_400),
            back + Duration::from_secs(12),
            Duration::from_millis(10),
        );
        assert!(t.is_active(back + Duration::from_millis(12_000)));
        // Fewer than five packets in a cycle clears the count again.
        let thin = back + Duration::from_secs(12);
        every(
            &mut t,
            thin,
            thin + Duration::from_secs(1),
            Duration::from_millis(300),
        );
        assert!(!t.is_active(thin + Duration::from_millis(1000)));
    }

    #[test]
    fn reset_makes_the_next_packet_active_immediately() {
        let t0 = Instant::now();
        let mut t = PacketTracker::new(VIDEO[2]);
        t.observe(t0, 1);
        assert!(!t.is_active(t0 + Duration::from_secs(1)));
        t.reset();
        let again = t0 + Duration::from_secs(2);
        t.observe(again, 1);
        assert!(t.is_active(again));
    }

    #[test]
    fn elapsed_empty_cycles_are_bounded_and_keep_the_check_phase() {
        let t0 = Instant::now();
        let mut tracker = PacketTracker::new(PacketTrackerConfig {
            samples: 5,
            cycles: 2,
            cycle: Duration::from_nanos(2),
        });
        tracker.observe(t0, 10);
        let after_gap = t0 + Duration::from_secs(10) + Duration::from_nanos(1);
        assert!(!tracker.is_active(after_gap));
        tracker.observe(after_gap, 5);
        assert!(!tracker.is_active(after_gap + Duration::from_nanos(1)));
        tracker.observe(after_gap + Duration::from_nanos(1), 5);
        assert!(tracker.is_active(after_gap + Duration::from_nanos(3)));
    }
}

/// Fixed Go `StreamTrackerFrame` operates on RTP end-of-frame markers,
/// including packets received out of order and incomplete codec pictures.
#[derive(Clone, Copy, Debug)]
pub(crate) struct FrameTracker {
    min_fps: f64,
    last_check: Option<Instant>,
    frames: u64,
    oldest_ts: u32,
    newest_ts: u32,
    estimated_fps: f64,
    active: bool,
}

impl FrameTracker {
    const TICK: Duration = Duration::from_millis(500);

    pub(crate) fn new(min_fps: f64) -> Self {
        Self {
            min_fps,
            last_check: None,
            frames: 0,
            oldest_ts: 0,
            newest_ts: 0,
            estimated_fps: 0.0,
            active: false,
        }
    }

    pub(crate) fn reset(&mut self) {
        *self = Self::new(self.min_fps);
    }

    pub(crate) fn observe(&mut self, now: Instant, marker: bool, ts: u32) {
        self.advance(now);
        if self.last_check.is_none() {
            // The first packet activates a layer even without an RTP marker.
            self.last_check = Some(now);
            self.active = true;
        }
        if !marker {
            return;
        }
        if self.frames == 0 || ts.wrapping_sub(self.oldest_ts) > (1 << 31) {
            self.oldest_ts = ts;
        }
        if self.frames == 0 || ts.wrapping_sub(self.newest_ts) < (1 << 31) {
            self.newest_ts = ts;
        }
        self.frames = self.frames.saturating_add(1);
    }

    pub(crate) fn is_active(&self, now: Instant) -> bool {
        let mut copy = *self;
        copy.advance(now);
        copy.active
    }

    fn period(&self) -> Duration {
        let mut eval_ns = Self::TICK.as_nanos() as f64;
        for fps in [self.estimated_fps, self.min_fps] {
            if fps > 0.0 {
                let interval = 1_000_000_000.0 / fps;
                // Go ignores a duration which falls outside its signed range.
                if interval < i64::MAX as f64 {
                    eval_ns = eval_ns.max(interval.trunc());
                }
            }
        }
        let threshold_ns = (0.98 * eval_ns) as u64;
        let tick_ns = Self::TICK.as_nanos() as u64;
        Duration::from_nanos(threshold_ns.div_ceil(tick_ns) * tick_ns)
    }

    fn advance(&mut self, now: Instant) {
        let Some(mut last_check) = self.last_check else {
            return;
        };
        // A valid estimate consumes all but the last marker. Therefore, after
        // at most one successful check the next empty interval stops the layer.
        loop {
            let Some(next) = last_check.checked_add(self.period()) else {
                return;
            };
            if now < next {
                return;
            }

            let round_rate = |fps: f64| (fps / 0.01).round() * 0.01;
            let span = self.newest_ts.wrapping_sub(self.oldest_ts);
            // All Go video RTP codecs use the 90 kHz timestamp clock.
            let rate = if span > 0 && self.frames > 1 {
                round_rate(90_000.0 / f64::from(span) * (self.frames - 1) as f64)
            } else {
                0.0
            };
            if rate == 0.0 {
                self.active = false;
                self.frames = 0;
                self.estimated_fps = 0.0;
                // Fold overdue empty checks in one step, without moving the
                // original ticker phase across a long pause or stalled room.
                let elapsed = now.duration_since(next);
                let remainder = elapsed.as_nanos() % self.period().as_nanos();
                self.last_check = Some(now - Duration::from_nanos(remainder as u64));
                return;
            }

            let factor = if rate < self.estimated_fps { 0.9 } else { 0.6 };
            self.estimated_fps = round_rate(rate * factor + self.estimated_fps * (1.0 - factor));
            self.active = true;
            self.oldest_ts = self.newest_ts;
            self.frames = 1;
            self.last_check = Some(next);
            last_check = next;
        }
    }
}

#[cfg(test)]
mod frame_tests {
    use super::*;

    #[test]
    fn markers_and_two_distinct_timestamps_are_needed_to_keep_or_resume_a_layer() {
        let t0 = Instant::now();
        let mut tracker = FrameTracker::new(5.0);
        tracker.observe(t0, false, 0);
        assert!(tracker.is_active(t0));
        tracker.observe(t0 + Duration::from_millis(10), true, 3_000);
        tracker.observe(t0 + Duration::from_millis(20), true, 3_000);
        assert!(!tracker.is_active(t0 + Duration::from_millis(500)));

        tracker.observe(t0 + Duration::from_millis(550), true, 90_000);
        tracker.observe(t0 + Duration::from_millis(600), true, 93_000);
        assert!(!tracker.is_active(t0 + Duration::from_millis(999)));
        assert!(tracker.is_active(t0 + Duration::from_millis(1000)));
        assert!(!tracker.is_active(t0 + Duration::from_millis(1500)));

        tracker.reset();
        tracker.observe(t0 + Duration::from_secs(2), false, 0);
        assert!(tracker.is_active(t0 + Duration::from_secs(2)));
    }

    #[test]
    fn screenshare_window_preserves_reordered_markers_across_timestamp_wrap() {
        let t0 = Instant::now();
        let mut tracker = FrameTracker::new(0.5);
        tracker.observe(t0, true, u32::MAX - 3_000);
        tracker.observe(t0 + Duration::from_millis(100), true, 3_000);
        tracker.observe(t0 + Duration::from_millis(200), true, u32::MAX - 6_000);
        assert!(tracker.is_active(t0 + Duration::from_secs(2)));
        assert!(!tracker.is_active(t0 + Duration::from_secs(4)));
        assert!(!tracker.is_active(t0 + Duration::from_secs(86400)));
    }
}
