// Copyright 2023 LiveKit, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Explicit monotonic-clock and signed-duration arithmetic for upstream policy.
use std::time::{Duration, Instant};
pub(super) fn elapsed(now: Instant, then: Instant) -> i64 {
    if let Some(d) = now.checked_duration_since(then) {
        d.as_nanos() as i64
    } else {
        -(then.duration_since(now).as_nanos() as i64)
    }
}
pub(super) fn offset(now: Instant, nanos: i64) -> Instant {
    let amount = Duration::from_nanos(nanos.unsigned_abs());
    if nanos < 0 {
        now.checked_sub(amount)
    } else {
        now.checked_add(amount)
    }
    .expect("signed policy duration exceeds platform Instant range")
}
/// Finite, representable Go float-to-int64 conversion truncates toward zero.
/// Out-of-range and non-finite policy configurations are outside this port's
/// supported domain: Go's result there is architecture-dependent.
pub(super) fn float_i64(value: f64) -> i64 {
    assert!(
        value.is_finite()
            && (-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&value),
        "non-finite or out-of-range policy arithmetic"
    );
    value as i64
}
pub(super) fn scaled_millis(nanos: i64, factor: f64) -> i64 {
    float_i64((nanos / 1_000_000) as f64 * factor).wrapping_mul(1_000_000)
}
