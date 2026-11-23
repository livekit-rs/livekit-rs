// Copyright 2023 LiveKit, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Explicit monotonic-clock arithmetic for the send-side detector.
//!
//! Upstream works in signed microseconds derived from `mono.UnixMicro()` and
//! `time.Duration.Microseconds()`. This port keeps an absolute `Instant` at the
//! boundary and converts to microseconds relative to a base clock exactly once,
//! so all arithmetic happens on the same integer quantities upstream uses.

use std::time::{Duration, Instant};

/// Signed nanoseconds from `then` to `now`, negative when `now` is earlier.
pub(crate) fn elapsed_nanos(now: Instant, then: Instant) -> i64 {
    if let Some(d) = now.checked_duration_since(then) {
        d.as_nanos() as i64
    } else {
        -(then.duration_since(now).as_nanos() as i64)
    }
}

/// `time.Since(then).Microseconds()` at `now`: truncating integer division of
/// the signed nanosecond difference, like Go.
pub(crate) fn elapsed_micros(now: Instant, then: Instant) -> i64 {
    elapsed_nanos(now, then) / 1_000
}

/// Nanoseconds of a whole-microsecond offset, used to rebuild an `Instant` from
/// a microsecond timestamp, e.g. a probe window start.
#[cfg_attr(not(test), allow(dead_code))] // no shipped caller yet: the `#[cfg(test)]` oracle replay executes it; C74-e wires the pacer/prober/allocator consumer
pub(crate) fn at_instant(base: Instant, micros: i64) -> Instant {
    offset(base, micros.wrapping_mul(1_000))
}

/// Signed nanosecond offset of a clock, used for validity-window cutoffs.
pub(crate) fn offset(now: Instant, nanos: i64) -> Instant {
    let amount = Duration::from_nanos(nanos.unsigned_abs());
    if nanos < 0 {
        now.checked_sub(amount)
    } else {
        now.checked_add(amount)
    }
    .expect("signed policy duration exceeds platform Instant range")
}

/// Finite, representable Go float-to-int conversion truncates toward zero.
/// Non-finite or out-of-range policy arithmetic is outside this port's
/// supported domain, where Go's result is architecture-dependent.
pub(crate) fn float_i64(value: f64) -> i64 {
    assert!(
        value.is_finite()
            && (-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&value),
        "non-finite or out-of-range policy arithmetic"
    );
    value as i64
}

/// `math.Round(x)` rounds half away from zero, which upstream uses to build
/// probe bucket counts.
#[cfg_attr(not(test), allow(dead_code))] // no shipped caller yet: the `#[cfg(test)]` oracle replay executes it; C74-e wires the pacer/prober/allocator consumer
pub(crate) fn go_round(value: f64) -> f64 {
    if value == 0.5 {
        1.0
    } else if value == -0.5 {
        -1.0
    } else {
        value.round()
    }
}

/// `int(math.Round(value))`.
#[cfg_attr(not(test), allow(dead_code))] // no shipped caller yet: the `#[cfg(test)]` oracle replay executes it; C74-e wires the pacer/prober/allocator consumer
pub(crate) fn go_round_i64(value: f64) -> i64 {
    float_i64(go_round(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signed_microsecond_conversion_truncates_like_go() {
        let base = Instant::now();
        let later = base + Duration::from_micros(1_500);
        assert_eq!(elapsed_micros(later, base), 1_500);
        assert_eq!(elapsed_micros(base, later), -1_500);
        assert_eq!(elapsed_nanos(base, base), 0);
    }

    #[test]
    fn instant_round_trip_is_exact_for_whole_microseconds() {
        let base = Instant::now();
        assert_eq!(elapsed_micros(at_instant(base, 7_250), base), 7_250);
        assert_eq!(elapsed_micros(at_instant(base, -7_250), base), -7_250);
    }

    #[test]
    fn rounding_matches_go_half_away_from_zero() {
        assert_eq!(go_round_i64(2.5), 3.0 as i64);
        assert_eq!(go_round_i64(-2.5), -3);
        assert_eq!(go_round_i64(2.4), 2);
        assert_eq!(go_round_i64(0.5), 1);
    }

    #[test]
    #[should_panic(expected = "non-finite or out-of-range policy arithmetic")]
    fn non_finite_policy_arithmetic_is_rejected_not_silently_clamped() {
        float_i64(f64::NAN);
    }
}
