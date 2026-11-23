// Copyright 2023 LiveKit, Inc.
// SPDX-License-Identifier: Apache-2.0
//! `ccutils/prober.go` — the padding-only probe cluster scheduler the stream
//! allocator owns per peer connection.
//!
//! Upstream runs the schedule in its own goroutine (`Prober.run()`, waking on a
//! resettable ticker that `Cluster.Process()` re-arms to `cSleepDurationMin`).
//! This port keeps the same arithmetic but is pull-driven: the owner calls
//! [`Prober::process`] when its deadline is due and acts on the returned
//! [`ProberEvent`]s, which are exactly upstream's `ProberListener` callbacks
//! (`OnProbeClusterSwitch`, `OnSendProbe`).
//!
//! Upstream properties the driver depends on, each pinned by a test below:
//!
//! * `AddCluster` never hands out `ProbeClusterIdInvalid`: the id is `Inc()`-ed
//!   before use, and a goal with `DesiredBps <= 0` is rejected without touching
//!   the queue or the counter.
//! * `DesiredBytes` is computed here by `initProbes`, so a caller passes the
//!   goal with `desired_bytes: 0` and reads the value back from the returned
//!   `ProbeClusterInfo` — which is what `ccutils::ProbeClusterGoal` documents.
//! * The bucket clock is absolute: elapsed time is compared against the
//!   *cumulative* expected duration of the bucket, and overflowing the last
//!   bucket repeats it forever instead of ending the window.

use std::collections::VecDeque;
use std::time::Instant;

use rtc_core::config::PolicyDuration;

use super::ccutils::{ProbeClusterGoal, ProbeClusterId, ProbeClusterInfo, ProbeClusterResult};

#[cfg(test)]
use super::ccutils::PROBE_CLUSTER_ID_INVALID;
use super::time::go_round_i64;

/// `cBytesPerProbe`: padding-only packets are 255 bytes payload plus a 20-byte
/// header, so this is 4 packets per probe when no padding-only packet is used
/// and only a minimum in that case.
const C_BYTES_PER_PROBE: i64 = 1100;
/// `cSleepDuration`, the nominal bucket divisor.
const C_SLEEP_DURATION: PolicyDuration = PolicyDuration::from_millis(20);
/// `cSleepDurationMin`, the bucket floor and the wake-up interval every
/// non-complete cluster reports.
const C_SLEEP_DURATION_MIN: PolicyDuration = PolicyDuration::from_millis(10);

/// `time.Duration.Seconds()`: upstream converts to float seconds at these
/// points, and only there.
fn secs(duration: PolicyDuration) -> f64 {
    duration.as_nanos_i64() as f64 / 1_000_000_000.0
}

/// `ccutils.ProbeClusterMode`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum ProbeClusterMode {
    #[default]
    Uniform,
    LinearChirp,
}

impl core::fmt::Display for ProbeClusterMode {
    /// `ProbeClusterMode.String()`.
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Uniform => f.write_str("UNIFORM"),
            Self::LinearChirp => f.write_str("LINEAR_CHIRP"),
        }
    }
}

/// `ccutils.bucket`.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct Bucket {
    expected_elapsed_duration: PolicyDuration,
    expected_probe_bytes_sent: i64,
}

/// `ccutils.Cluster`. `start_time` is `mono.Now()` on the first `Process()`;
/// `None` is upstream's zero `time.Time`, which is what makes that first call
/// emit `cBytesPerProbe` immediately.
#[derive(Clone, Debug)]
struct Cluster {
    info: ProbeClusterInfo,
    mode: ProbeClusterMode,
    base_sleep_duration: PolicyDuration,
    buckets: Vec<Bucket>,
    bucket_idx: usize,
    probe_bytes_sent: i64,
    start_time: Option<Instant>,
    is_complete: bool,
}

impl Cluster {
    /// `newCluster`, including the `initProbes()` call it makes.
    fn new(id: ProbeClusterId, mode: ProbeClusterMode, mut goal: ProbeClusterGoal) -> Self {
        // `int(math.Round(float64(DesiredBps) * Duration.Seconds() / 8 + 0.5))`.
        goal.desired_bytes =
            go_round_i64(goal.desired_bps as f64 * secs(goal.duration) / 8.0 + 0.5);
        let mut cluster = Self {
            info: ProbeClusterInfo {
                id,
                // `newCluster` stamps `CreatedAt = mono.Now()`. The queue does
                // not own a clock, so the value is stamped when the front
                // cluster is first processed, which is when upstream's listener
                // sees it.
                created_at: None,
                goal,
                result: ProbeClusterResult::default(),
            },
            mode,
            base_sleep_duration: PolicyDuration::ZERO,
            buckets: Vec::new(),
            bucket_idx: 0,
            probe_bytes_sent: 0,
            start_time: None,
            is_complete: false,
        };
        cluster.init_probes();
        cluster
    }

    /// `Cluster.initProbes()`, in upstream's order and with its rounding.
    fn init_probes(&mut self) {
        let duration_secs = secs(self.info.goal.duration);
        let mut num_buckets = go_round_i64(duration_secs / secs(C_SLEEP_DURATION) + 0.5);
        if num_buckets < 1 {
            num_buckets = 1;
        }
        let mut num_intervals = num_buckets;

        // A linear chirp groups intervals with decreasing duration, i.e.
        // increasing bitrate: bucket `i` waits `(numBuckets - i)` base sleeps.
        if self.mode == ProbeClusterMode::LinearChirp {
            let mut sum = 0i64;
            let mut i = 1i64;
            loop {
                sum += i;
                if sum >= num_buckets {
                    break;
                }
                i += 1;
            }
            num_buckets = i;
            num_intervals = sum;
        }

        // `Duration / time.Duration(numIntervals)` is integer nanosecond
        // division, then floored at `cSleepDurationMin`.
        let mut base = PolicyDuration::from_nanos(
            self.info
                .goal
                .duration
                .as_nanos_i64()
                .div_euclid(num_intervals.max(1)),
        );
        if base < C_SLEEP_DURATION_MIN {
            base = C_SLEEP_DURATION_MIN;
        }
        self.base_sleep_duration = base;

        // `numIntervals` is recomputed against the floored base, which is why a
        // short goal sends fewer, larger probes than the raw division suggests.
        let num_intervals = go_round_i64(duration_secs / secs(base) + 0.5).max(1);
        let desired_probe_bytes_per_interval = go_round_i64(
            (duration_secs
                * (self.info.goal.desired_bps - self.info.goal.expected_usage_bps) as f64
                / 8.0
                + num_intervals as f64
                - 1.0)
                / num_intervals as f64
                + 0.5,
        );

        let mut buckets: Vec<Bucket> = Vec::with_capacity(num_buckets as usize);
        for i in 0..num_buckets {
            let expected_elapsed_duration = match self.mode {
                ProbeClusterMode::Uniform => base,
                ProbeClusterMode::LinearChirp => {
                    PolicyDuration::from_nanos((num_buckets - i) * base.as_nanos_i64())
                }
            };
            let mut bucket = Bucket {
                expected_elapsed_duration,
                expected_probe_bytes_sent: (i + 1) * desired_probe_bytes_per_interval,
            };
            if i > 0 {
                bucket.expected_elapsed_duration = PolicyDuration::from_nanos(
                    bucket.expected_elapsed_duration.as_nanos_i64()
                        + buckets[i as usize - 1]
                            .expected_elapsed_duration
                            .as_nanos_i64(),
                );
            }
            buckets.push(bucket);
        }
        self.buckets = buckets;
    }

    /// `Cluster.Process()`. The returned duration is upstream's ticker reset;
    /// `None` means the cluster is complete and the queue may drop it.
    fn process(&mut self, now: Instant) -> (i64, Option<PolicyDuration>) {
        if self.is_complete {
            return (0, None);
        }

        let mut bytes_to_send = 0;
        match self.start_time {
            None => {
                self.start_time = Some(now);
                bytes_to_send = C_BYTES_PER_PROBE;
            }
            Some(started_at) => {
                let since_start = PolicyDuration::from_nanos(
                    now.saturating_duration_since(started_at).as_nanos() as i64,
                );
                // `initProbes` always makes at least one bucket.
                let expected_elapsed = self.buckets[self.bucket_idx].expected_elapsed_duration;
                if since_start > expected_elapsed {
                    self.bucket_idx += 1;
                    let mut overflow = false;
                    if self.bucket_idx >= self.buckets.len() {
                        // When overflowing, repeat the last bucket.
                        self.bucket_idx = self.buckets.len() - 1;
                        overflow = true;
                    }
                    let expected = self.buckets[self.bucket_idx].expected_probe_bytes_sent;
                    if expected > self.probe_bytes_sent || overflow {
                        bytes_to_send =
                            C_BYTES_PER_PROBE.max(expected.saturating_sub(self.probe_bytes_sent));
                    }
                }
            }
        }

        (bytes_to_send, Some(C_SLEEP_DURATION_MIN))
    }

    /// `Cluster.MarkCompleted`.
    fn mark_completed(&mut self, result: ProbeClusterResult) {
        self.is_complete = true;
        self.info.result = result;
    }

    /// `Cluster.ProbesSent`.
    fn probes_sent(&mut self, bytes_sent: i64) {
        self.probe_bytes_sent += bytes_sent;
    }
}

/// `ProberListener`, surfaced as return values instead of a callback object.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum ProberEvent {
    /// `OnProbeClusterSwitch(info)`, the front cluster's first start.
    ClusterSwitch(ProbeClusterInfo),
    /// `OnSendProbe(bytesToSend)`.
    SendProbe(i64),
}

/// `ccutils.Prober`. Upstream keeps a deque plus an `activeCluster` pointer
/// that always aliases the front, so this models the queue plus a "front has
/// been started" flag: the two are the same cluster by construction.
#[derive(Clone, Debug, Default)]
pub(crate) struct Prober {
    cluster_id: ProbeClusterId,
    clusters: VecDeque<Cluster>,
    is_active: bool,
}

impl Prober {
    /// `Prober.IsRunning()`.
    pub(crate) fn is_running(&self) -> bool {
        !self.clusters.is_empty()
    }

    /// `Prober.GetActiveClusterId()`. Upstream only reads it from its own logs
    /// and tests, so the shipped build never calls it.
    #[cfg(test)]
    pub(crate) fn active_cluster_id(&self) -> ProbeClusterId {
        match (self.is_active, self.clusters.front()) {
            (true, Some(cluster)) => cluster.info.id,
            _ => PROBE_CLUSTER_ID_INVALID,
        }
    }

    /// `Prober.AddCluster`. The switch is *not* part of this call: upstream's
    /// listener sees the cluster when the queue runs it, which is
    /// [`Prober::process`].
    pub(crate) fn add_cluster(
        &mut self,
        mode: ProbeClusterMode,
        goal: ProbeClusterGoal,
    ) -> ProbeClusterInfo {
        if goal.desired_bps <= 0 {
            return ProbeClusterInfo::default();
        }
        self.cluster_id = self.cluster_id.wrapping_add(1);
        let cluster = Cluster::new(self.cluster_id, mode, goal);
        let info = cluster.info;
        self.clusters.push_back(cluster);
        info
    }

    /// `Prober.ProbesSent`, delivered to the front cluster only.
    ///
    /// Upstream reaches the cluster through `getFrontCluster()`, which *starts*
    /// it as a side effect and fires `OnProbeClusterSwitch`; likewise
    /// `ClusterDone` on a queue whose front was never started would promote it
    /// before marking it. The single driver here calls `process` before either
    /// of those, so a metered or completed cluster is always the already
    /// promoted front and that side effect is unreachable.
    pub(crate) fn probes_sent(&mut self, bytes_sent: i64) {
        if let Some(cluster) = self.clusters.front_mut() {
            cluster.probes_sent(bytes_sent);
        }
    }

    /// `Prober.ClusterDone`: only the front cluster can be finished this way.
    pub(crate) fn cluster_done(&mut self, info: &ProbeClusterInfo) {
        if self
            .clusters
            .front()
            .is_none_or(|cluster| cluster.info.id != info.id)
        {
            return;
        }
        if let Some(cluster) = self.clusters.front_mut() {
            cluster.mark_completed(info.result);
        }
        self.clusters.pop_front();
        self.is_active = false;
    }

    /// `Prober.Reset`: the active cluster, if it is the named one, keeps the
    /// result the caller already recorded, and the whole queue is dropped.
    pub(crate) fn reset(&mut self, info: &ProbeClusterInfo) {
        if self.is_active
            && let Some(cluster) = self.clusters.front_mut()
            && cluster.info.id == info.id
        {
            cluster.mark_completed(info.result);
        }
        self.clusters.clear();
        self.is_active = false;
    }

    /// One iteration of `Prober.run()`: start the front cluster when nothing is
    /// active, process it, and when it reports complete drop it and let the
    /// next cluster start in the same call, which is upstream's
    /// `sleepDuration == 0 -> continue` path.
    pub(crate) fn process(&mut self, now: Instant) -> (Vec<ProberEvent>, Option<PolicyDuration>) {
        let mut events = Vec::new();
        loop {
            if !self.is_active {
                let Some(cluster) = self.clusters.front_mut() else {
                    return (events, None);
                };
                cluster.info.created_at = Some(now);
                let info = cluster.info;
                self.is_active = true;
                events.push(ProberEvent::ClusterSwitch(info));
            }
            let Some(cluster) = self.clusters.front_mut() else {
                return (events, None);
            };
            let (bytes_to_send, sleep) = cluster.process(now);
            if bytes_to_send != 0 {
                events.push(ProberEvent::SendProbe(bytes_to_send));
            }
            match sleep {
                Some(sleep) => return (events, Some(sleep)),
                None => {
                    self.clusters.pop_front();
                    self.is_active = false;
                }
            }
        }
    }

    /// The pull-model deadline: `None` when nothing is queued, otherwise the
    /// wake-up interval upstream's ticker was reset to.
    pub(crate) fn next_deadline(
        &self,
        now: Instant,
        sleep: Option<PolicyDuration>,
    ) -> Option<Instant> {
        if !self.is_running() {
            return None;
        }
        let sleep = sleep.unwrap_or(C_SLEEP_DURATION_MIN).nonnegative_to_std()?;
        now.checked_add(sleep)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn goal(
        desired_bps: i64,
        expected_usage_bps: i64,
        duration: PolicyDuration,
    ) -> ProbeClusterGoal {
        ProbeClusterGoal {
            available_bandwidth_bps: 0,
            expected_usage_bps,
            desired_bps,
            duration,
            desired_bytes: 0,
        }
    }

    #[test]
    fn a_cluster_is_numbered_from_one_and_a_non_positive_goal_is_never_queued() {
        let mut prober = Prober::default();
        assert_eq!(prober.cluster_id, PROBE_CLUSTER_ID_INVALID);
        let rejected = prober.add_cluster(ProbeClusterMode::Uniform, goal(0, 0, C_SLEEP_DURATION));
        assert_eq!(rejected.id, PROBE_CLUSTER_ID_INVALID);
        assert!(!prober.is_running());
        assert_eq!(
            prober.cluster_id, PROBE_CLUSTER_ID_INVALID,
            "a rejection must not Inc()"
        );
        let negative = prober.add_cluster(ProbeClusterMode::Uniform, goal(-1, 0, C_SLEEP_DURATION));
        assert_eq!(negative.id, PROBE_CLUSTER_ID_INVALID);
        let first = prober.add_cluster(ProbeClusterMode::Uniform, goal(1, 0, C_SLEEP_DURATION));
        assert_eq!(first.id, 1);
        assert!(prober.is_running());
        assert_eq!(
            prober.active_cluster_id(),
            PROBE_CLUSTER_ID_INVALID,
            "an added cluster is not active until the queue runs it"
        );
    }

    /// One measured `newCluster()` shape from the pinned Go oracle
    /// (`e5655ac2`, `pkg/sfu/ccutils/prober.go`, captured by an in-package
    /// harness that printed `initProbes()` output and a `Process()` ladder).
    /// `buckets` is the full measured `(cumulative expected elapsed ns,
    /// expected probe bytes sent)` table.
    struct Schedule {
        name: &'static str,
        mode: ProbeClusterMode,
        desired_bps: i64,
        expected_usage_bps: i64,
        duration: PolicyDuration,
        desired_bytes: i64,
        base_sleep_ns: i64,
        buckets: &'static [(i64, i64)],
    }

    const SCHEDULES: &[Schedule] = &[
        Schedule {
            name: "uniform/5Mbps-over-4Mbps-100ms",
            mode: ProbeClusterMode::Uniform,
            desired_bps: 5_000_000,
            expected_usage_bps: 4_000_000,
            duration: PolicyDuration::from_millis(100),
            desired_bytes: 62_501,
            base_sleep_ns: 16_666_666,
            buckets: &[
                (16_666_666, 1_787),
                (33_333_332, 3_574),
                (49_999_998, 5_361),
                (66_666_664, 7_148),
                (83_333_330, 8_935),
                (99_999_996, 10_722),
            ],
        },
        Schedule {
            name: "uniform/5Mbps-over-4Mbps-40ms",
            mode: ProbeClusterMode::Uniform,
            desired_bps: 5_000_000,
            expected_usage_bps: 4_000_000,
            duration: PolicyDuration::from_millis(40),
            desired_bytes: 25_001,
            base_sleep_ns: 13_333_333,
            buckets: &[
                (13_333_333, 1_251),
                (26_666_666, 2_502),
                (39_999_999, 3_753),
            ],
        },
        Schedule {
            name: "uniform/5Mbps-over-4Mbps-5ms",
            mode: ProbeClusterMode::Uniform,
            desired_bps: 5_000_000,
            expected_usage_bps: 4_000_000,
            duration: PolicyDuration::from_millis(5),
            desired_bytes: 3_126,
            base_sleep_ns: 10_000_000,
            buckets: &[(10_000_000, 626)],
        },
        Schedule {
            name: "uniform/200kbps-150ms",
            mode: ProbeClusterMode::Uniform,
            desired_bps: 200_000,
            expected_usage_bps: 0,
            duration: PolicyDuration::from_millis(150),
            desired_bytes: 3_751,
            base_sleep_ns: 18_750_000,
            buckets: &[
                (18_750_000, 418),
                (37_500_000, 836),
                (56_250_000, 1_254),
                (75_000_000, 1_672),
                (93_750_000, 2_090),
                (112_500_000, 2_508),
                (131_250_000, 2_926),
                (150_000_000, 3_344),
            ],
        },
        Schedule {
            name: "uniform/5Mbps-over-4Mbps-zero-window",
            mode: ProbeClusterMode::Uniform,
            desired_bps: 5_000_000,
            expected_usage_bps: 4_000_000,
            duration: PolicyDuration::ZERO,
            desired_bytes: 1,
            base_sleep_ns: 10_000_000,
            buckets: &[(10_000_000, 1)],
        },
        Schedule {
            name: "uniform/zero-desired-20ms",
            mode: ProbeClusterMode::Uniform,
            desired_bps: 0,
            expected_usage_bps: 0,
            duration: PolicyDuration::from_millis(20),
            desired_bytes: 1,
            base_sleep_ns: 10_000_000,
            buckets: &[(10_000_000, 1), (20_000_000, 2)],
        },
        Schedule {
            name: "uniform/negative-desired-20ms",
            mode: ProbeClusterMode::Uniform,
            desired_bps: -1,
            expected_usage_bps: 0,
            duration: PolicyDuration::from_millis(20),
            desired_bytes: 0,
            base_sleep_ns: 10_000_000,
            buckets: &[(10_000_000, 1), (20_000_000, 2)],
        },
        Schedule {
            name: "uniform/3.2Mbps-over-3Mbps-75ms",
            mode: ProbeClusterMode::Uniform,
            desired_bps: 3_200_000,
            expected_usage_bps: 3_000_000,
            duration: PolicyDuration::from_millis(75),
            desired_bytes: 30_001,
            base_sleep_ns: 18_750_000,
            buckets: &[
                (18_750_000, 376),
                (37_500_000, 752),
                (56_250_000, 1_128),
                (75_000_000, 1_504),
            ],
        },
        Schedule {
            name: "chirp/5Mbps-over-4Mbps-500ms",
            mode: ProbeClusterMode::LinearChirp,
            desired_bps: 5_000_000,
            expected_usage_bps: 4_000_000,
            duration: PolicyDuration::from_millis(500),
            desired_bytes: 312_501,
            base_sleep_ns: 17_857_142,
            buckets: &[
                (124_999_994, 2_157),
                (232_142_846, 4_314),
                (321_428_556, 6_471),
                (392_857_124, 8_628),
                (446_428_550, 10_785),
                (482_142_834, 12_942),
                (499_999_976, 15_099),
            ],
        },
        Schedule {
            name: "chirp/5Mbps-over-4Mbps-100ms",
            mode: ProbeClusterMode::LinearChirp,
            desired_bps: 5_000_000,
            expected_usage_bps: 4_000_000,
            duration: PolicyDuration::from_millis(100),
            desired_bytes: 62_501,
            base_sleep_ns: 16_666_666,
            buckets: &[
                (49_999_998, 1_787),
                (83_333_330, 3_574),
                (99_999_996, 5_361),
            ],
        },
        Schedule {
            name: "chirp/5Mbps-over-4Mbps-40ms",
            mode: ProbeClusterMode::LinearChirp,
            desired_bps: 5_000_000,
            expected_usage_bps: 4_000_000,
            duration: PolicyDuration::from_millis(40),
            desired_bytes: 25_001,
            base_sleep_ns: 13_333_333,
            buckets: &[(26_666_666, 1_251), (39_999_999, 2_502)],
        },
        Schedule {
            name: "chirp/5Mbps-over-4Mbps-5ms",
            mode: ProbeClusterMode::LinearChirp,
            desired_bps: 5_000_000,
            expected_usage_bps: 4_000_000,
            duration: PolicyDuration::from_millis(5),
            desired_bytes: 3_126,
            base_sleep_ns: 10_000_000,
            buckets: &[(10_000_000, 626)],
        },
        Schedule {
            name: "chirp/200kbps-150ms",
            mode: ProbeClusterMode::LinearChirp,
            desired_bps: 200_000,
            expected_usage_bps: 0,
            duration: PolicyDuration::from_millis(150),
            desired_bytes: 3_751,
            base_sleep_ns: 15_000_000,
            buckets: &[
                (60_000_000, 342),
                (105_000_000, 684),
                (135_000_000, 1_026),
                (150_000_000, 1_368),
            ],
        },
        Schedule {
            name: "chirp/zero-desired-20ms",
            mode: ProbeClusterMode::LinearChirp,
            desired_bps: 0,
            expected_usage_bps: 0,
            duration: PolicyDuration::from_millis(20),
            desired_bytes: 1,
            base_sleep_ns: 10_000_000,
            // A chirp's first bucket spans `numBuckets` base sleeps, so the
            // table overshoots the requested window.
            buckets: &[(20_000_000, 1), (30_000_000, 2)],
        },
        Schedule {
            name: "chirp/negative-desired-20ms",
            mode: ProbeClusterMode::LinearChirp,
            desired_bps: -1,
            expected_usage_bps: 0,
            duration: PolicyDuration::from_millis(20),
            desired_bytes: 0,
            base_sleep_ns: 10_000_000,
            buckets: &[(20_000_000, 1), (30_000_000, 2)],
        },
        Schedule {
            name: "chirp/3.2Mbps-over-3Mbps-75ms",
            mode: ProbeClusterMode::LinearChirp,
            desired_bps: 3_200_000,
            expected_usage_bps: 3_000_000,
            duration: PolicyDuration::from_millis(75),
            desired_bytes: 30_001,
            base_sleep_ns: 12_500_000,
            buckets: &[(37_500_000, 314), (62_500_000, 628), (75_000_000, 942)],
        },
    ];

    fn schedule(s: &Schedule) -> Cluster {
        Cluster::new(
            1,
            s.mode,
            ProbeClusterGoal {
                available_bandwidth_bps: 0,
                expected_usage_bps: s.expected_usage_bps,
                desired_bps: s.desired_bps,
                duration: s.duration,
                desired_bytes: 0,
            },
        )
    }

    #[test]
    fn the_probe_cluster_schedule_replays_the_pinned_go_vectors() {
        for s in SCHEDULES {
            let cluster = schedule(s);
            assert_eq!(
                cluster.info.goal.desired_bytes, s.desired_bytes,
                "{}: `DesiredBytes` is `round(DesiredBps * secs / 8 + 0.5)`, which \
                 counts the *whole* desired rate, not the probe headroom",
                s.name
            );
            assert_eq!(
                cluster.base_sleep_duration.as_nanos_i64(),
                s.base_sleep_ns,
                "{}",
                s.name
            );
            let table: Vec<(i64, i64)> = cluster
                .buckets
                .iter()
                .map(|b| {
                    (
                        b.expected_elapsed_duration.as_nanos_i64(),
                        b.expected_probe_bytes_sent,
                    )
                })
                .collect();
            assert_eq!(table, s.buckets, "{}", s.name);
        }
    }

    #[test]
    fn a_long_uniform_window_keeps_the_measured_bucket_count_and_endpoints() {
        // 500 ms: 26 buckets (`round(0.5/0.02 + 0.5)`), a base of
        // 500 ms / 26, and 2 316 bytes per interval. The interval count is
        // recomputed against the *floored* base, which is why the total is not
        // the 62 500 bytes the headroom arithmetic suggests.
        let cluster = schedule(&Schedule {
            name: "",
            mode: ProbeClusterMode::Uniform,
            desired_bps: 5_000_000,
            expected_usage_bps: 4_000_000,
            duration: PolicyDuration::from_millis(500),
            desired_bytes: 0,
            base_sleep_ns: 0,
            buckets: &[],
        });
        assert_eq!(cluster.buckets.len(), 26);
        assert_eq!(cluster.base_sleep_duration.as_nanos_i64(), 19_230_769);
        assert_eq!(cluster.buckets[0].expected_probe_bytes_sent, 2_316);
        assert_eq!(
            cluster.buckets[25].expected_elapsed_duration.as_nanos_i64(),
            499_999_994
        );
        assert_eq!(cluster.buckets[25].expected_probe_bytes_sent, 60_216);
        for i in 1..cluster.buckets.len() {
            assert_eq!(
                cluster.buckets[i].expected_elapsed_duration.as_nanos_i64()
                    - cluster.buckets[i - 1]
                        .expected_elapsed_duration
                        .as_nanos_i64(),
                19_230_769,
                "a uniform bucket clock advances by exactly the base sleep"
            );
            assert_eq!(
                cluster.buckets[i].expected_probe_bytes_sent
                    - cluster.buckets[i - 1].expected_probe_bytes_sent,
                2_316
            );
        }
    }

    #[test]
    fn a_twelve_second_window_is_bounded_by_the_bucket_floor_not_the_goal() {
        // 1 bps for 12 s: `DesiredBytes` is 2, yet the window still gets 601
        // uniform buckets of 2 bytes each and 35 chirp buckets of the same.
        let uniform = Cluster::new(
            1,
            ProbeClusterMode::Uniform,
            goal(1, 0, PolicyDuration::from_secs(12)),
        );
        assert_eq!(uniform.info.goal.desired_bytes, 2);
        assert_eq!(uniform.buckets.len(), 601);
        assert_eq!(uniform.base_sleep_duration.as_nanos_i64(), 19_966_722);
        assert_eq!(uniform.buckets[0].expected_probe_bytes_sent, 2);
        assert_eq!(uniform.buckets[600].expected_probe_bytes_sent, 1_202);
        assert_eq!(
            uniform.buckets[600]
                .expected_elapsed_duration
                .as_nanos_i64(),
            11_999_999_922
        );

        let chirp = Cluster::new(
            1,
            ProbeClusterMode::LinearChirp,
            goal(1, 0, PolicyDuration::from_secs(12)),
        );
        assert_eq!(chirp.buckets.len(), 35);
        assert_eq!(chirp.base_sleep_duration.as_nanos_i64(), 19_047_619);
        assert_eq!(
            chirp.buckets[0].expected_elapsed_duration.as_nanos_i64(),
            666_666_665
        );
        assert_eq!(chirp.buckets[34].expected_probe_bytes_sent, 70);
        assert_eq!(
            chirp.buckets[34].expected_elapsed_duration.as_nanos_i64(),
            11_999_999_970
        );
    }

    #[test]
    fn the_first_process_emits_one_probe_and_later_ones_follow_the_bucket_clock() {
        let origin = Instant::now();
        let mut cluster = Cluster::new(
            1,
            ProbeClusterMode::Uniform,
            goal(5_000_000, 4_000_000, PolicyDuration::from_millis(100)),
        );
        // `startTime` is the zero time: the first call sends the minimum probe
        // and starts the clock.
        assert_eq!(cluster.process(origin), (1_100, Some(C_SLEEP_DURATION_MIN)));
        assert_eq!(cluster.bucket_idx, 0);

        // Measured Go ladder, un-metered: at most one bucket per call, and the
        // ask is the whole cumulative expectation because nothing is credited.
        let steps_ms = [15i64, 19, 40, 50, 75, 90, 100, 600];
        let mut sends = Vec::new();
        for ms in steps_ms {
            let (bytes, sleep) = cluster.process(origin + Duration::from_millis(ms as u64));
            assert_eq!(sleep, Some(C_SLEEP_DURATION_MIN), "upstream never idles");
            if bytes != 0 {
                sends.push(bytes);
            }
        }
        assert_eq!(
            sends,
            vec![3_574, 5_361, 7_148, 8_935, 10_722, 10_722, 10_722]
        );
        assert_eq!(cluster.bucket_idx, 5, "the index stops on the last bucket");
        assert_eq!(
            cluster.probe_bytes_sent, 0,
            "the prober is credited only on the wire"
        );
    }

    #[test]
    fn metering_a_cluster_asks_only_for_the_remaining_bucket_debt() {
        let origin = Instant::now();
        let mut cluster = Cluster::new(
            1,
            ProbeClusterMode::Uniform,
            goal(5_000_000, 4_000_000, PolicyDuration::from_millis(100)),
        );
        // Measured Go ladder, metered: every `OnSendProbe` is credited back
        // through `ProbesSent` before the next `Process`.
        let mut sends = vec![cluster.process(origin).0];
        cluster.probes_sent(1_100);
        for ms in [5i64, 10, 20, 30, 40, 60, 80, 100, 150, 200, 250] {
            let (bytes, _) = cluster.process(origin + Duration::from_millis(ms as u64));
            if bytes != 0 {
                cluster.probes_sent(bytes);
                sends.push(bytes);
            }
        }
        assert_eq!(
            sends,
            vec![
                1_100, 2_474, 1_787, 1_787, 1_787, 1_787, 1_100, 1_100, 1_100
            ]
        );
        assert_eq!(cluster.probe_bytes_sent, 14_022);

        // A single-bucket 5 ms window: the last bucket is repeated forever, so
        // the window has no end until the allocator stops it. `11 ms` rather
        // than the measured `10 ms` because upstream compares against a wall
        // clock that has already ticked past the boundary.
        let mut short = Cluster::new(
            1,
            ProbeClusterMode::Uniform,
            goal(5_000_000, 4_000_000, PolicyDuration::from_millis(5)),
        );
        let mut short_sends = vec![short.process(origin).0];
        short.probes_sent(1_100);
        for ms in [11i64, 20, 30] {
            let (bytes, _) = short.process(origin + Duration::from_millis(ms as u64));
            if bytes != 0 {
                short.probes_sent(bytes);
                short_sends.push(bytes);
            }
        }
        assert_eq!(
            short_sends,
            vec![1_100, 1_100, 1_100, 1_100],
            "overflow always asks for another probe even when the accounting is ahead"
        );
        assert_eq!(short.bucket_idx, 0);
    }

    #[test]
    fn a_chirp_front_loads_the_schedule_with_decreasing_intervals() {
        // Measured Go ladder for the 100 ms chirp: 3 buckets ending at
        // 49.999998 / 83.33333 / 99.999996 ms.
        let origin = Instant::now();
        let mut cluster = Cluster::new(
            1,
            ProbeClusterMode::LinearChirp,
            goal(5_000_000, 4_000_000, PolicyDuration::from_millis(100)),
        );
        let mut sends = vec![cluster.process(origin).0];
        cluster.probes_sent(1_100);
        for ms in [40i64, 60, 80, 100, 150, 200] {
            let (bytes, _) = cluster.process(origin + Duration::from_millis(ms as u64));
            if bytes != 0 {
                cluster.probes_sent(bytes);
                sends.push(bytes);
            }
        }
        assert_eq!(sends, vec![1_100, 2_474, 1_787, 1_100, 1_100]);
        assert_eq!(cluster.bucket_idx, 2);
    }

    #[test]
    fn a_complete_cluster_ends_the_schedule_and_the_next_one_starts_in_the_same_call() {
        let origin = Instant::now();
        let mut prober = Prober::default();
        let first = prober.add_cluster(
            ProbeClusterMode::Uniform,
            goal(5_000_000, 0, PolicyDuration::from_millis(20)),
        );
        let second = prober.add_cluster(
            ProbeClusterMode::Uniform,
            goal(6_000_000, 0, PolicyDuration::from_millis(20)),
        );
        assert_eq!(second.id, 2);

        let (events, sleep) = prober.process(origin);
        assert_eq!(events.len(), 2, "switch then the first probe");
        assert_eq!(
            events[0],
            ProberEvent::ClusterSwitch(ProbeClusterInfo {
                id: first.id,
                created_at: Some(origin),
                goal: first.goal,
                ..ProbeClusterInfo::default()
            })
        );
        assert_eq!(sleep, Some(C_SLEEP_DURATION_MIN));
        assert_eq!(prober.active_cluster_id(), first.id);

        // A result naming a cluster the queue does not run in front is ignored.
        prober.cluster_done(&ProbeClusterInfo {
            id: 99,
            ..ProbeClusterInfo::default()
        });
        assert_eq!(prober.clusters.len(), 2);
        assert_eq!(prober.active_cluster_id(), first.id);

        prober.cluster_done(&first);
        assert_eq!(prober.clusters.len(), 1);
        let (events, _) = prober.process(origin + Duration::from_millis(10));
        assert_eq!(events.len(), 2);
        assert_eq!(prober.active_cluster_id(), second.id);
        assert!(
            prober.clusters.front().is_some_and(|c| !c.is_complete),
            "the second cluster starts fresh"
        );
    }

    #[test]
    fn reset_marks_the_active_cluster_complete_and_clears_the_queue() {
        let origin = Instant::now();
        let mut prober = Prober::default();
        let pci = prober.add_cluster(
            ProbeClusterMode::Uniform,
            goal(5_000_000, 0, PolicyDuration::from_millis(20)),
        );
        prober.process(origin);
        let info = ProbeClusterInfo {
            result: ProbeClusterResult {
                bytes_probe: C_BYTES_PER_PROBE,
                ..ProbeClusterResult::default()
            },
            ..pci
        };
        prober.reset(&info);
        assert!(!prober.is_running());
        assert_eq!(prober.active_cluster_id(), PROBE_CLUSTER_ID_INVALID);
        let (events, sleep) = prober.process(origin + Duration::from_millis(10));
        assert!(events.is_empty());
        assert_eq!(sleep, None);
        assert_eq!(
            prober.next_deadline(origin, sleep),
            None,
            "an empty queue must not keep the owner ticking"
        );
        // The id counter is not reused after a reset, matching `Inc()`.
        let next = prober.add_cluster(
            ProbeClusterMode::Uniform,
            goal(5_000_000, 0, PolicyDuration::from_millis(20)),
        );
        assert_eq!(next.id, pci.id + 1);
    }

    #[test]
    fn the_deadline_only_exists_while_a_cluster_is_queued() {
        let origin = Instant::now();
        let mut prober = Prober::default();
        assert_eq!(
            prober.next_deadline(origin, Some(C_SLEEP_DURATION_MIN)),
            None
        );
        prober.add_cluster(
            ProbeClusterMode::Uniform,
            goal(5_000_000, 0, PolicyDuration::from_millis(20)),
        );
        assert_eq!(
            prober.next_deadline(origin, Some(C_SLEEP_DURATION_MIN)),
            Some(origin + Duration::from_millis(10))
        );
        // A negative policy duration is not silently turned into a zero timer.
        assert_eq!(
            prober.next_deadline(origin, Some(PolicyDuration::from_nanos(-1))),
            None
        );
    }
}
