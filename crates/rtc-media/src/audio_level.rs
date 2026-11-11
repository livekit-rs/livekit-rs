//! Active speaker level per audio track (sfu-media.md §5.3, audiolevel.go:88-197).
//!
//! Input: RFC 6464 level (0..=127, -dBov; lower is louder) per packet/frame with
//! its duration (RTP timestamp delta / clock rate).

use std::time::{Duration, Instant};

use rtc_core::config::AudioConfig;

/// Silence level (no observation).
const SILENT_LEVEL: u8 = 127;

#[derive(Clone, Debug)]
pub(crate) struct AudioLevelParams {
    pub active_level: u8,
    pub min_percentile: u8,
    pub update_interval: Duration,
    pub smooth_intervals: u32,
}

impl From<&AudioConfig> for AudioLevelParams {
    fn from(c: &AudioConfig) -> Self {
        Self {
            active_level: c.active_level,
            min_percentile: c.min_percentile,
            update_interval: c.update_interval,
            smooth_intervals: c.smooth_intervals,
        }
    }
}

/// Durations are kept in integer microseconds so interval boundaries are exact.
pub(crate) struct AudioLevel {
    active_level: u8,
    update_interval_us: u64,
    min_active_us: u64,
    smooth_factor: f64,
    active_threshold: f64,

    smoothed: f64,
    loudest: u8,
    active_us: u64,
    observed_us: u64,
    last_observed: Option<Instant>,
}

impl AudioLevel {
    pub(crate) fn new(p: &AudioLevelParams) -> Self {
        let interval_us = p.update_interval.as_micros() as u64;
        let smooth_intervals = p.smooth_intervals.max(1) as f64;
        Self {
            active_level: p.active_level,
            update_interval_us: interval_us,
            // minActiveDuration = min_percentile * update_interval / 100
            min_active_us: p.min_percentile as u64 * interval_us / 100,
            // smoothFactor = 2 / (smooth_intervals + 1)
            smooth_factor: 2.0 / (smooth_intervals + 1.0),
            // activeThreshold = 10^(-active_level/20)
            active_threshold: 10f64.powf(-(p.active_level as f64) / 20.0),
            smoothed: 0.0,
            loudest: SILENT_LEVEL,
            active_us: 0,
            observed_us: 0,
            last_observed: None,
        }
    }

    /// One observation of `level` (-dBov) lasting `duration_us` microseconds.
    pub(crate) fn observe(&mut self, level: u8, duration_us: u64, now: Instant) {
        // Fixed upstream Observe returns before all mutations when disabled.
        if self.update_interval_us == 0 {
            return;
        }
        self.last_observed = Some(now);
        self.observed_us += duration_us;
        if level <= self.active_level {
            self.active_us += duration_us;
            self.loudest = self.loudest.min(level);
        }
        if self.observed_us >= self.update_interval_us {
            if self.active_us >= self.min_active_us {
                // Adjust the loudest level by the fraction of the interval that was active.
                let ratio = self.active_us as f64 / self.update_interval_us as f64;
                let adjusted = self.loudest as f64 - 20.0 * ratio.log10();
                let linear = 10f64.powf(-adjusted / 20.0);
                self.smoothed += (linear - self.smoothed) * self.smooth_factor;
            } else {
                self.smoothed = 0.0;
            }
            self.loudest = SILENT_LEVEL;
            self.active_us = 0;
            self.observed_us = 0;
        }
    }

    /// `(linear smoothed level, active)`. Stale for ≥ 2 update intervals → 0.
    pub(crate) fn level(&mut self, now: Instant) -> (f32, bool) {
        if self.update_interval_us == 0 {
            return (0.0, false);
        }
        let stale = match self.last_observed {
            Some(t) => {
                now.saturating_duration_since(t).as_micros() as u64 >= 2 * self.update_interval_us
            }
            None => true,
        };
        if stale {
            self.smoothed = 0.0;
        }
        // Fixed Go GetLevel returns the actual EMA without clipping. A frame
        // can cover more than a short configured update interval, giving an
        // activity weight above one; that value reaches speaker notifications.
        let level = self.smoothed;
        (
            level as f32,
            self.smoothed > 0.0 && self.smoothed >= self.active_threshold,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params() -> AudioLevelParams {
        AudioLevelParams {
            active_level: 35,
            min_percentile: 40,
            update_interval: Duration::from_millis(400),
            smooth_intervals: 2,
        }
    }

    #[test]
    fn fixed_go_source_vectors_cover_disabled_and_short_intervals() {
        let vectors = include_str!(
            "../../../tests/fixtures/upstream/evidence/2026-10-25/f02b-oracle/audio.tsv"
        );
        for line in vectors
            .lines()
            .filter(|line| !line.starts_with('#') && !line.is_empty())
        {
            let fields: Vec<_> = line.split_whitespace().collect();
            let number = |index: usize| fields[index].parse::<u64>().unwrap();
            let start = Instant::now();
            let mut params = params();
            params.update_interval = Duration::from_millis(number(0));
            params.smooth_intervals = number(3) as u32;
            let mut detector = AudioLevel::new(&params);
            detector.observe(
                number(2) as u8,
                number(1) * 1000,
                start + Duration::from_millis(number(1)),
            );
            let (level, active) = detector.level(start + Duration::from_millis(number(4)));
            let expected: f32 = fields[5].parse().unwrap();
            assert!((level - expected).abs() < 1e-6, "{line}: {level}");
            assert_eq!(active, fields[6] == "true", "{line}");
            assert_eq!(
                detector.last_observed.is_some(),
                fields[7] == "true",
                "{line}"
            );
            if number(0) == 0 {
                assert_eq!(detector.observed_us, 0);
                assert_eq!(detector.active_us, 0);
                assert_eq!(detector.loudest, SILENT_LEVEL);
            }
        }
    }

    #[test]
    fn loud_speech_becomes_active() {
        let t0 = Instant::now();
        let mut a = AudioLevel::new(&params());
        for i in 0..40 {
            a.observe(20, 20_000, t0 + Duration::from_millis(20 * i));
        }
        let (level, active) = a.level(t0 + Duration::from_millis(800));
        assert!(active);
        // 10^(-20/20) = 0.1, smoothed over two intervals: 0.0667 then 0.0889
        assert!((level - 0.0889).abs() < 0.001, "level {level}");
    }

    #[test]
    fn short_activity_is_ignored() {
        let t0 = Instant::now();
        let mut a = AudioLevel::new(&params());
        // 100 ms of speech < 160 ms min active duration
        for i in 0..20 {
            let lvl = if i < 5 { 10 } else { 90 };
            a.observe(lvl, 20_000, t0 + Duration::from_millis(20 * i));
        }
        assert_eq!(a.level(t0 + Duration::from_millis(400)), (0.0, false));
    }

    #[test]
    fn stale_level_resets() {
        let t0 = Instant::now();
        let mut a = AudioLevel::new(&params());
        for i in 0..20 {
            a.observe(10, 20_000, t0 + Duration::from_millis(20 * i));
        }
        assert!(a.level(t0 + Duration::from_millis(400)).1);
        assert_eq!(a.level(t0 + Duration::from_millis(1300)), (0.0, false));
    }
}
