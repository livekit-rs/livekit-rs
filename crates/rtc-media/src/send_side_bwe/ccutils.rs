// Copyright 2023 LiveKit, Inc.
// SPDX-License-Identifier: Apache-2.0
//! `pkg/sfu/ccutils` values consumed by the send-side detector: probe cluster
//! identity, goal and result, plus the `ccutils.ProbeSignal` lattice.
//! Upstream at commit `e5655ac20267fa5005dccaf3f0e3bfc3ea688c07`:
//! `ccutils/prober.go` and `ccutils/probe_signal.go`.
//!
//! Upstream keeps two different clock units in these two structs and this port
//! preserves both: `pacer/probe_observer.go` stamps `Result.StartTime`/`EndTime`
//! with `mono.UnixNano()`, so a result duration is a nanosecond `time.Duration`,
//! while a goal duration is a configured `time.Duration` too. Packet-level
//! timestamps are separate and stay in microseconds, see `tracker.rs`.

use rtc_core::config::PolicyDuration;
use std::time::Instant;

/// `ccutils.ProbeClusterId`; the zero value is `ProbeClusterIdInvalid`.
pub(crate) type ProbeClusterId = u32;
pub(crate) const PROBE_CLUSTER_ID_INVALID: ProbeClusterId = 0;

/// `ccutils.ProbeClusterGoal`. Upstream overwrites `desired_bytes` inside
/// `initProbes()`, so callers pass zero and read the computed value back.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) struct ProbeClusterGoal {
    pub(crate) available_bandwidth_bps: i64,
    pub(crate) expected_usage_bps: i64,
    pub(crate) desired_bps: i64,
    pub(crate) duration: PolicyDuration,
    pub(crate) desired_bytes: i64,
}

/// `ccutils.ProbeClusterResult`. `start_time_ns` and `end_time_ns` are the raw
/// `mono.UnixNano()` stamps the pacer's probe observer records.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) struct ProbeClusterResult {
    pub(crate) start_time_ns: i64,
    pub(crate) end_time_ns: i64,
    pub(crate) packets_probe: i64,
    pub(crate) bytes_probe: i64,
    pub(crate) packets_non_probe_primary: i64,
    pub(crate) bytes_non_probe_primary: i64,
    pub(crate) packets_non_probe_rtx: i64,
    pub(crate) bytes_non_probe_rtx: i64,
    pub(crate) is_completed: bool,
}

impl ProbeClusterResult {
    /// `Bytes()`: probe traffic plus non-probe primary plus non-probe RTX.
    pub(crate) fn bytes(&self) -> i64 {
        self.bytes_probe + self.bytes_non_probe_primary + self.bytes_non_probe_rtx
    }

    /// `Duration()`: `time.Duration(EndTime - StartTime)`, which wraps like
    /// Go's int64 subtraction instead of trapping.
    pub(crate) fn duration(&self) -> PolicyDuration {
        PolicyDuration::from_nanos(self.end_time_ns.wrapping_sub(self.start_time_ns))
    }
}

/// `ccutils.ProbeClusterInfo`. `created_at` is `None` for Go's zero `time.Time`,
/// which the probe regulator treats as "schedule from now".
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) struct ProbeClusterInfo {
    pub(crate) id: ProbeClusterId,
    pub(crate) created_at: Option<Instant>,
    pub(crate) goal: ProbeClusterGoal,
    pub(crate) result: ProbeClusterResult,
}

/// `ccutils.ProbeSignal`. The zero value is `ProbeSignalInconclusive`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum ProbeSignal {
    #[default]
    Inconclusive,
    Congesting,
    NotCongesting,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_result_bytes_add_the_three_upstream_classes() {
        let result = ProbeClusterResult {
            start_time_ns: 1_000,
            end_time_ns: 5_000,
            bytes_probe: 300,
            bytes_non_probe_primary: 400,
            bytes_non_probe_rtx: 50,
            ..ProbeClusterResult::default()
        };
        assert_eq!(result.bytes(), 750);
        assert_eq!(result.duration(), PolicyDuration::from_nanos(4_000));
        let empty = ProbeClusterResult::default();
        assert_eq!(empty.bytes(), 0);
        assert_eq!(empty.duration(), PolicyDuration::ZERO);
    }

    #[test]
    fn a_result_duration_is_the_nanosecond_difference_of_the_observer_stamps() {
        let result = ProbeClusterResult {
            start_time_ns: 5_000_000_000,
            end_time_ns: 5_250_000_000,
            ..ProbeClusterResult::default()
        };
        assert_eq!(result.duration(), PolicyDuration::from_millis(250));
    }

    #[test]
    fn zero_is_the_invalid_cluster_sentinel() {
        assert_eq!(PROBE_CLUSTER_ID_INVALID, 0);
        assert_eq!(ProbeClusterInfo::default().id, PROBE_CLUSTER_ID_INVALID);
    }

    #[test]
    fn inconclusive_is_the_zero_value_probe_signal() {
        assert_eq!(ProbeSignal::default(), ProbeSignal::Inconclusive);
    }
}
