// Copyright 2023 LiveKit, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Port of `twccFeedback` from
//! `pkg/sfu/bwe/sendsidebwe/twcc_feedback.go`. Upstream at commit
//! `e5655ac20267fa5005dccaf3f0e3bfc3ea688c07`.
//!
//! The reference time of a TWCC report is a 24-bit compact-NTP timestamp in
//! 64 ms units, so it wraps every `2**24 * 64ms` (about 30.4 hours). This
//! module reconstructs the monotone microsecond value by tracking an era cycle
//! counter, and it estimates the feedback interval so that an out-of-order
//! report can be recognised from its `FbPktCount` alone.
//!
//! Porting differences that are deliberate:
//!
//! * `lastFeedbackTime` is Go's zero `time.Time`; here it is `Option<Instant>`
//!   and every clock read is supplied by the caller.
//! * The interval estimate is kept as a `PolicyDuration` (signed nanoseconds),
//!   which is what `time.Duration` is, so the division and multiplication of
//!   the outlier window stay integer, like Go.

use std::time::Instant;

use rtc_core::config::PolicyDuration;

use super::time::float_i64;

/// `cReferenceTimeMask`: the 24-bit width of a compact-NTP reference time.
const REFERENCE_TIME_MASK: u32 = (1 << 24) - 1;

/// `cReferenceTimeResolution`: microseconds contributed by one reference tick.
const REFERENCE_TIME_RESOLUTION_US: i64 = 64 * 1_000;

/// `cOutlierReportFactor`.
const OUTLIER_REPORT_FACTOR: i64 = 3;

/// `cEstimatedFeedbackIntervalAlpha`.
const ESTIMATED_FEEDBACK_INTERVAL_ALPHA: f64 = 0.9;

/// `twccFeedback`.
pub(super) struct TwccFeedback {
    last_feedback_at: Option<Instant>,
    estimated_feedback_interval: PolicyDuration,
    num_reports: i64,
    num_reports_out_of_order: i64,
    highest_feedback_count: u8,
    cycles: i64,
    highest_reference_time: u32,
}

impl Default for TwccFeedback {
    fn default() -> Self {
        Self::new()
    }
}

impl TwccFeedback {
    /// `newTWCCFeedback`: everything is the Go zero value.
    pub(super) fn new() -> Self {
        Self {
            last_feedback_at: None,
            estimated_feedback_interval: PolicyDuration::ZERO,
            num_reports: 0,
            num_reports_out_of_order: 0,
            highest_feedback_count: 0,
            cycles: 0,
            highest_reference_time: 0,
        }
    }

    /// `ProcessReport`: returns the reconstructed receive reference time in
    /// microseconds and whether the report was out of order. The flag is the
    /// second return value upstream discards at the call site, but it is the
    /// reason the interval estimate stops advancing, so it stays visible.
    pub(super) fn process_report(
        &mut self,
        reference_time: u32,
        fb_pkt_count: u8,
        at: Instant,
    ) -> (i64, bool) {
        self.num_reports += 1;
        if self.last_feedback_at.is_none() {
            self.last_feedback_at = Some(at);
            self.highest_reference_time = reference_time;
            self.highest_feedback_count = fb_pkt_count;
            return (
                (self.cycles + i64::from(reference_time)) * REFERENCE_TIME_RESOLUTION_US,
                false,
            );
        }

        let is_out_of_order = fb_pkt_count.wrapping_sub(self.highest_feedback_count) > (1 << 7);
        if is_out_of_order {
            self.num_reports_out_of_order += 1;
        }

        let reference = self.unwrap_reference_time(reference_time);

        if !is_out_of_order {
            self.smooth_interval(at);
            self.last_feedback_at = Some(at);
            self.highest_feedback_count = fb_pkt_count;
        }

        (reference * REFERENCE_TIME_RESOLUTION_US, is_out_of_order)
    }

    /// The 24-bit wrap handling of `ProcessReport`, split out so the era
    /// bookkeeping can be read on its own.
    fn unwrap_reference_time(&mut self, reference_time: u32) -> i64 {
        let absolute = i64::from(reference_time);
        if (reference_time.wrapping_sub(self.highest_reference_time) & REFERENCE_TIME_MASK)
            < (1 << 23)
        {
            if reference_time < self.highest_reference_time {
                self.cycles += 1 << 24;
            }
            self.highest_reference_time = reference_time;
            self.cycles + absolute
        } else {
            let mut cycles = self.cycles;
            if reference_time > self.highest_reference_time && cycles >= (1 << 24) {
                cycles -= 1 << 24;
            }
            cycles + absolute
        }
    }

    /// The interval estimate of `ProcessReport`, which only runs for an
    /// in-order report: the first sample seeds it, later samples must fall
    /// inside the outlier window before the exponentially weighted average
    /// absorbs them.
    fn smooth_interval(&mut self, at: Instant) {
        let last = self
            .last_feedback_at
            .expect("the caller only smooths after a seeded report");
        let since_last = PolicyDuration::from_nanos(super::time::elapsed_nanos(at, last));
        if self.estimated_feedback_interval == PolicyDuration::ZERO {
            self.estimated_feedback_interval = since_last;
            return;
        }

        let interval_ns = self.estimated_feedback_interval.as_nanos_i64();
        let lower = interval_ns / OUTLIER_REPORT_FACTOR;
        let upper = interval_ns.wrapping_mul(OUTLIER_REPORT_FACTOR);
        if since_last.as_nanos_i64() > lower && since_last.as_nanos_i64() < upper {
            self.estimated_feedback_interval = PolicyDuration::from_nanos(float_i64(
                ESTIMATED_FEEDBACK_INTERVAL_ALPHA * interval_ns as f64
                    + (1.0 - ESTIMATED_FEEDBACK_INTERVAL_ALPHA) * since_last.as_nanos_i64() as f64,
            ));
        }
    }

    /// `estimatedFeedbackInterval`.
    pub(super) fn estimated_feedback_interval(&self) -> PolicyDuration {
        self.estimated_feedback_interval
    }

    /// `cycles/(1<<24)`, the era count upstream logs.
    pub(super) fn era(&self) -> i64 {
        self.cycles / (1 << 24)
    }

    pub(super) fn report_counts(&self) -> (i64, i64) {
        (self.num_reports, self.num_reports_out_of_order)
    }

    /// The raw wrap bookkeeping, which is what the campaign differential echoes:
    /// `cycles` accumulates whole 24-bit laps and `highestReferenceTime` keeps the
    /// last in-order 24-bit value, so the two together are the un-wrapped clock.
    #[cfg(test)]
    pub(super) fn cycles_and_highest_reference(&self) -> (i64, u32) {
        (self.cycles, self.highest_reference_time)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// The test clock in milliseconds, named so that no call site can hand it a
    /// microsecond digit from the campaign by accident (`at_us` is the oracle's
    /// unit, `at_ms` is 1/1000 of it).
    fn at_ms(base: Instant, millis: u64) -> Instant {
        base + Duration::from_millis(millis)
    }

    #[test]
    fn the_first_report_only_seeds_the_state_and_is_never_out_of_order() {
        let base = Instant::now();
        let mut feedback = TwccFeedback::new();
        assert_eq!(feedback.process_report(1_000, 7, base), (64_000_000, false));
        assert_eq!(feedback.report_counts(), (1, 0));
        assert_eq!(feedback.era(), 0);
        assert_eq!(feedback.estimated_feedback_interval(), PolicyDuration::ZERO);

        // A second report far in the future is judged against the seed, which
        // is still zero, so the first non-zero interval is the raw gap.
        assert_eq!(
            feedback.process_report(1_001, 8, at_ms(base, 100)).0,
            64_064_000
        );
        assert_eq!(
            feedback.estimated_feedback_interval(),
            PolicyDuration::from_millis(100),
            "the very first gap seeds the estimate untouched"
        );
    }

    #[test]
    fn the_reference_time_is_sixty_four_milliseconds_per_tick() {
        let base = Instant::now();
        let mut feedback = TwccFeedback::new();
        feedback.process_report(0, 0, base);
        assert_eq!(feedback.process_report(2, 1, at_ms(base, 10)).0, 128_000);
        assert_eq!(feedback.process_report(3, 2, at_ms(base, 20)).0, 192_000);
    }

    #[test]
    fn a_monotone_reference_time_never_moves_the_era() {
        let base = Instant::now();
        let mut feedback = TwccFeedback::new();
        feedback.process_report(100, 0, base);
        for (index, reference) in [8_000_000u32, 16_000_000, 16_777_211].iter().enumerate() {
            let (got, out_of_order) = feedback.process_report(
                *reference,
                (index + 1) as u8,
                at_ms(base, 10 + index as u64 * 10),
            );
            assert!(!out_of_order);
            assert_eq!(got, i64::from(*reference) * 64_000);
        }
        assert_eq!(feedback.era(), 0);
    }

    #[test]
    fn a_wrapped_reference_time_advances_the_era_and_keeps_counting() {
        let base = Instant::now();
        let mut feedback = TwccFeedback::new();
        feedback.process_report(16_777_211, 0, base);
        // 3 is 8 ticks "ahead" modulo 2**24, so this is the wrap, not a late report.
        assert_eq!(
            feedback.process_report(3, 1, at_ms(base, 10)).0,
            16_777_219 * 64_000
        );
        assert_eq!(feedback.era(), 1);
        // A small backwards step now looks like a stale report: it is reported
        // in the current era without moving the high water mark.
        assert_eq!(
            feedback.process_report(2, 2, at_ms(base, 20)).0,
            (16_777_216 + 2) * 64_000
        );
        assert_eq!(feedback.era(), 1);
    }

    #[test]
    fn a_large_forward_jump_is_reported_one_era_down_when_the_era_is_ahead() {
        let base = Instant::now();
        let mut feedback = TwccFeedback::new();
        feedback.process_report(16_777_211, 0, base);
        feedback.process_report(3, 1, at_ms(base, 10));
        assert_eq!(feedback.era(), 1);
        // 9_000_000 is more than half the space ahead of the high water mark of
        // 3, so upstream treats it as a report from before the wrap.
        assert_eq!(
            feedback.process_report(9_000_000, 2, at_ms(base, 20)).0,
            9_000_000 * 64_000
        );
        assert_eq!(
            feedback.era(),
            1,
            "the stored era is not rewritten by a stale report"
        );
    }

    #[test]
    fn an_out_of_order_report_is_counted_and_freezes_the_time_tracking_state() {
        let base = Instant::now();
        let mut feedback = TwccFeedback::new();
        feedback.process_report(1_000, 10, base);
        assert_eq!(
            feedback.process_report(1_010, 200, at_ms(base, 100)),
            (1_010 * 64_000, true),
            "200 - 10 wraps to 190, which is above the 128 half-space"
        );
        assert_eq!(feedback.report_counts(), (2, 1));
        assert_eq!(
            feedback.estimated_feedback_interval(),
            PolicyDuration::ZERO,
            "an out-of-order report may not seed the interval"
        );

        // The next in-order report is still measured from the last in-order time.
        assert!(!feedback.process_report(1_020, 11, at_ms(base, 200)).1);
        assert_eq!(
            feedback.estimated_feedback_interval(),
            PolicyDuration::from_millis(200),
            "the seed comes from the gap to the last accepted report"
        );
        assert_eq!(feedback.report_counts(), (3, 1));
    }

    #[test]
    fn the_feedback_count_window_is_exactly_half_the_u8_space() {
        let base = Instant::now();
        let mut cases = Vec::new();
        for (highest, next) in [
            (10u8, 139u8),
            (10, 138),
            (10, 11),
            (10, 9),
            (250, 252),
            (250, 248),
        ] {
            let mut feedback = TwccFeedback::new();
            feedback.process_report(1_000, highest, base);
            let (_, out_of_order) = feedback.process_report(1_000, next, at_ms(base, 10));
            cases.push((highest, next, out_of_order));
        }
        assert_eq!(
            cases,
            vec![
                // 129 ahead: strictly above the half space.
                (10, 139, true),
                // exactly 128 ahead is still inside it.
                (10, 138, false),
                (10, 11, false),
                // 9 - 10 wraps to 255, which is the out-of-order direction.
                (10, 9, true),
                // 252 - 250 is two ahead: in order.
                (250, 252, false),
                // 248 - 250 wraps to 254: a report from behind.
                (250, 248, true),
            ]
        );
    }

    #[test]
    fn the_interval_estimate_absorbs_samples_inside_the_outlier_window_only() {
        let base = Instant::now();
        let mut feedback = TwccFeedback::new();
        feedback.process_report(1_000, 0, base);
        // seed: exactly 100ms
        feedback.process_report(1_001, 1, at_ms(base, 100));
        assert_eq!(
            feedback.estimated_feedback_interval(),
            PolicyDuration::from_millis(100)
        );

        // 150ms is inside (33ms, 300ms): 0.9*100 + 0.1*150 = 105ms
        feedback.process_report(1_002, 2, at_ms(base, 250));
        assert_eq!(
            feedback.estimated_feedback_interval(),
            PolicyDuration::from_millis(105)
        );

        // 400ms is at or beyond three times the current 105ms estimate, so the
        // estimate must not move even though the clock does.
        feedback.process_report(1_003, 3, at_ms(base, 650));
        assert_eq!(
            feedback.estimated_feedback_interval(),
            PolicyDuration::from_millis(105)
        );

        // 10ms is below the third, so it is rejected as well.
        feedback.process_report(1_004, 4, at_ms(base, 660));
        assert_eq!(
            feedback.estimated_feedback_interval(),
            PolicyDuration::from_millis(105)
        );

        // An exactly-one-third gap is not greater than the lower bound.
        feedback.process_report(1_005, 5, at_ms(base, 695));
        assert_eq!(
            feedback.estimated_feedback_interval(),
            PolicyDuration::from_millis(105)
        );

        // 36ms is one millisecond above the truncated third of 105ms
        // (35ms), so it is absorbed: 0.9*105 + 0.1*36 = 98.1ms.
        feedback.process_report(1_006, 6, at_ms(base, 731));
        assert_interval(&feedback, 98_100_000);
    }

    /// The smoothed estimate is a float product, so compare it within the one
    /// nanosecond that float-to-int truncation can move it.
    fn assert_interval(feedback: &TwccFeedback, expect_ns: i64) {
        let got = feedback.estimated_feedback_interval().as_nanos_i64();
        assert!(
            (got - expect_ns).abs() <= 1,
            "expected {expect_ns}ns, got {got}ns"
        );
    }

    #[test]
    fn the_outlier_bounds_are_integer_divisions_of_the_nanosecond_estimate() {
        let base = Instant::now();
        let mut feedback = TwccFeedback::new();
        feedback.process_report(1_000, 0, base);
        // A seed that is not divisible by three shows the truncating bound.
        feedback.process_report(1_001, 1, at_ms(base, 101));
        assert_eq!(
            feedback.estimated_feedback_interval(),
            PolicyDuration::from_millis(101)
        );
        // 33ms is not above 101ms/3 == 33.666ms truncated, so it is filtered out.
        feedback.process_report(1_002, 2, at_ms(base, 134));
        assert_eq!(
            feedback.estimated_feedback_interval(),
            PolicyDuration::from_millis(101)
        );
        // 34ms is above it and is absorbed: 0.9*101 + 0.1*34 = 94.3ms
        feedback.process_report(1_003, 3, at_ms(base, 168));
        assert_interval(&feedback, 94_300_000);
    }

    #[test]
    fn a_zero_gap_seeds_a_zero_interval_that_still_lets_any_gap_smooth_in() {
        let base = Instant::now();
        let mut feedback = TwccFeedback::new();
        feedback.process_report(1_000, 0, base);
        feedback.process_report(1_001, 1, base);
        assert_eq!(feedback.estimated_feedback_interval(), PolicyDuration::ZERO);
        // The estimate is still zero, so the next gap re-seeds it rather than
        // dividing by a zero lower bound.
        feedback.process_report(1_002, 2, at_ms(base, 50));
        assert_eq!(
            feedback.estimated_feedback_interval(),
            PolicyDuration::from_millis(50)
        );
    }

    #[test]
    fn the_era_is_reported_as_a_whole_number_of_wraps() {
        let mut feedback = TwccFeedback::new();
        assert_eq!(feedback.era(), 0);
        feedback.cycles = 2 * (1 << 24);
        assert_eq!(feedback.era(), 2);
        feedback.cycles = (1 << 24) + 5;
        assert_eq!(feedback.era(), 1, "the logged era drops the sub-era part");
    }
}
