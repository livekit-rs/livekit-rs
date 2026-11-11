//! Probe cluster data structures for bandwidth estimation.
//!
//! Probe clusters are short bursts of packets sent at specific bitrates to test
//! network capacity.

use std::cmp;
use std::time::{Duration, Instant};

use crate::rtp_::{Bitrate, DataSize, TwccClusterId};
use crate::util::{already_happened, not_happening};

// Google BitrateProberConfig::max_probe_delay, fixed reference commit
// 202acee882062f7ca632472e4810f25938828f6a. A delayed train is cancelled,
// never replayed as catch-up traffic after the pacing loop resumes.
const MAX_PROBE_DELAY: Duration = Duration::from_millis(10);

const MAX_PADDING_PACKET_SIZE: DataSize = DataSize::bytes(240);

/// Configuration for a probe cluster (the plan).
///
/// This represents the immutable blueprint for a bandwidth probe: what bitrate
/// to test, for how long, and with what constraints.
#[derive(Debug, Clone, Copy)]
pub struct ProbeClusterConfig {
    /// Unique identifier for this probe cluster
    cluster: TwccClusterId,

    /// Target bitrate to probe at (e.g., 3 Mbps)
    target_bitrate: Bitrate,

    /// How long to sustain the target bitrate (e.g., 15ms)
    target_duration: Duration,

    /// Minimum number of packets to send (e.g., 5)
    /// This ensures statistical validity even for short bursts.
    min_packet_count: usize,

    /// Delta time between sent bursts of packets during probe.
    ///
    /// Mirrors WebRTC's `ProbeClusterConfig.min_probe_delta`. The pacer/probe sender must not
    /// schedule probe packets more frequently than this.
    min_probe_delta: Duration,

    /// The kind of probe this is.
    kind: ProbeKind,

    // Native request time drives stale queued-cluster expiry, independently of
    // the later first actual packet send time used for measurement.
    requested_at: Option<Instant>,
    // Read-only observation metadata, excluded from scheduling arithmetic.
    started_at: Option<Instant>,
}

/// Kind of probe
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ProbeKind {
    /// Bounded sustained validation, independent of short-probe estimation.
    Validation,
    /// Startup discovery cluster.
    Initial,
    /// Further discovery after a successful preceding cluster.
    Exponential,
    /// Increased application demand while application-limited.
    IncreaseAlr,
    /// Periodic application-limited discovery.
    PeriodicAlr,
    /// Recovery discovery after a large estimate drop.
    LargeDrop,
    /// Discovery after unmet demand and a stagnant estimate.
    Stagnant,
}

impl ProbeKind {
    pub(crate) fn is_alr(&self) -> bool {
        matches!(self, ProbeKind::PeriodicAlr | ProbeKind::IncreaseAlr)
    }
}

impl ProbeClusterConfig {
    /// Create a new probe cluster configuration with standard defaults:
    /// - 15ms duration (enough to get meaningful feedback without excessive delay)
    /// - 5 minimum packets (statistical significance for BWE analysis)
    pub fn new(cluster: TwccClusterId, target_bitrate: Bitrate, kind: ProbeKind) -> Self {
        Self {
            cluster,
            target_bitrate,
            // WebRTC defaults
            target_duration: Duration::from_millis(15),
            min_packet_count: 5,
            // WebRTC default for general probing (not initial/network-state): 2ms.
            min_probe_delta: Duration::from_millis(2),
            kind,
            requested_at: None,
            started_at: None,
        }
    }

    /// Set a custom target duration for this probe.
    /// WebRTC uses 100ms for initial probes to allow time for media to start.
    pub fn with_duration(mut self, duration: Duration) -> Self {
        self.target_duration = duration;
        self
    }

    /// Set a custom minimum packet count for this probe.
    pub fn with_min_packet_count(mut self, min_packet_count: usize) -> Self {
        self.min_packet_count = min_packet_count;
        self
    }

    /// Set a custom minimum probe delta (spacing constraint) for this probe.
    pub fn with_min_probe_delta(mut self, min_probe_delta: Duration) -> Self {
        self.min_probe_delta = min_probe_delta;
        self
    }

    pub(crate) fn with_requested_at(mut self, now: Instant) -> Self {
        self.requested_at = Some(now);
        self
    }

    pub(crate) fn requested_at(&self) -> Option<Instant> {
        self.requested_at
    }

    pub(crate) fn with_started_at(mut self, now: Instant) -> Self {
        self.started_at = Some(now);
        self
    }

    pub(crate) fn started_at(&self) -> Option<Instant> {
        self.started_at
    }

    /// Native scheduling reason for this cluster.
    pub fn kind(&self) -> ProbeKind {
        self.kind
    }

    /// Check if this probe was created during ALR.
    pub fn is_alr_probe(&self) -> bool {
        self.kind.is_alr()
    }

    /// Get the probe cluster ID.
    pub fn cluster(&self) -> TwccClusterId {
        self.cluster
    }

    /// Get the target bitrate.
    pub fn target_bitrate(&self) -> Bitrate {
        self.target_bitrate
    }

    /// Get the minimum packet count required for a valid probe.
    pub fn min_packet_count(&self) -> usize {
        self.min_packet_count
    }

    /// Get the minimum probe delta (spacing constraint).
    pub fn min_probe_delta(&self) -> Duration {
        self.min_probe_delta
    }

    /// Calculate the target bytes for this probe.
    /// This is how much data we expect to send at target_bitrate for target_duration.
    pub fn target_bytes(&self) -> DataSize {
        self.target_bitrate * self.target_duration
    }
}

/// Runtime state of an active probe cluster (the execution).
///
/// This tracks what's actually happening as we send probe packets: how much
/// we've sent, how many packets, and when we started.
#[derive(Debug)]
pub struct ProbeClusterState {
    /// The immutable plan
    config: ProbeClusterConfig,

    /// Total bytes sent so far in this probe
    bytes_sent: DataSize,

    /// Total packets sent so far in this probe
    packets_sent: usize,

    /// When the first packet was sent
    /// None = probe hasn't started yet (still queued)
    started_at: Option<Instant>,

    /// When the last packet was sent
    /// Used to calculate actual send duration (first to last packet)
    last_packet_at: Option<Instant>,
    validation_next_at: Option<Instant>,
    pending_validation_credit_discard: Option<Duration>,

    /// When we last created a padding request
    /// Used to prevent creating multiple padding packets for the same instant
    last_padding_at: Instant,
}

impl ProbeClusterState {
    /// Create a new probe cluster state from a config.
    pub fn new(config: ProbeClusterConfig) -> Self {
        Self {
            config,
            bytes_sent: DataSize::ZERO,
            packets_sent: 0,
            started_at: None,
            last_packet_at: None,
            validation_next_at: None,
            pending_validation_credit_discard: None,
            last_padding_at: already_happened(),
        }
    }

    /// Get the probe configuration.
    pub fn config(&self) -> &ProbeClusterConfig {
        &self.config
    }

    /// Calculates the next probe time based on total bytes sent and target bitrate.
    /// This naturally handles variable packet sizes (media packets can be much larger
    /// than padding packets). Next packet can be sent when:
    /// `now >= start_time + (bytes_sent / target_bitrate)`
    pub fn next_probe_time(&self) -> Instant {
        let Some(started_at) = self.started_at else {
            return already_happened();
        };

        if self.config.target_bitrate == Bitrate::ZERO {
            return not_happening();
        }

        if self.config.kind == ProbeKind::Validation {
            // Packet deadlines and padding-request burst spacing are distinct.
            // Already queued packets may use at most the bounded scheduling credit.
            return self.validation_next_at.unwrap_or(started_at);
        }

        // Packet-level pacing: schedule based on bytes sent at the target bitrate.
        // next_time = start_time + (bytes_sent / target_bitrate)
        let send_duration = self.bytes_sent / self.config.target_bitrate;
        let probe_time = started_at + send_duration;

        let min_burst_time = self.last_padding_at + self.config.min_probe_delta();
        probe_time.max(min_burst_time)
    }

    /// Whether an already transmitting train missed its next deadline by more
    /// than Google's default maximum probe delay. Queued, unsent clusters do
    /// not have a finite send deadline and must not expire through this guard.
    pub(crate) fn is_late(&self, now: Instant) -> bool {
        self.config.kind != ProbeKind::Validation
            && self.last_packet_at.is_some()
            && now.saturating_duration_since(self.next_probe_time()) > MAX_PROBE_DELAY
    }

    /// Check if the probe cluster is complete.
    ///
    /// A probe is complete when BOTH conditions are met (matching WebRTC):
    /// 1. Sent bytes >= target_bytes (target_bitrate * target_duration)
    /// 2. Sent packets >= min_packet_count
    ///
    /// Note: WebRTC does NOT check duration for completion. Duration is only used
    /// to calculate the target bytes threshold. This ensures probes complete even
    /// when all packets are sent at the same instant (e.g., in tests or when the
    /// pacer generates bursts faster than wall-clock time advances).
    pub fn is_complete(&self, now: Instant) -> bool {
        if self.config.kind == ProbeKind::Validation
            && self.validation_end().is_some_and(|end| now >= end)
        {
            return true;
        }
        if self.started_at.is_none() {
            return false; // Not started yet
        }

        // Match WebRTC's BitrateProber::ProbeSent() logic:
        // if (sent_bytes >= probe_cluster_min_bytes && sent_probes >= probe_cluster_min_probes)
        let bytes_met = self.bytes_sent >= self.config.target_bytes();
        let packets_met = self.packets_sent >= self.config.min_packet_count;

        let validation_span_met = self.config.kind != ProbeKind::Validation
            || self
                .started_at
                .zip(self.last_packet_at)
                .is_some_and(|(first, last)| {
                    last.saturating_duration_since(first) >= Duration::from_secs(1)
                });
        bytes_met && packets_met && validation_span_met
    }

    pub(crate) fn validation_end(&self) -> Option<Instant> {
        (self.config.kind == ProbeKind::Validation)
            .then(|| {
                self.config
                    .requested_at
                    .or(self.started_at)
                    .map(|at| at + Duration::from_secs(2))
            })
            .flatten()
    }

    /// Packet pacing and padding generation deadlines are separate: empty
    /// queues must wait for the next request burst, not spin on an old packet time.
    pub(crate) fn next_padding_time(&self) -> Instant {
        self.next_probe_time()
            .max(self.last_padding_at + self.config.min_probe_delta())
    }

    /// Check if it's time to send the next probe packet.
    ///
    /// Returns `true` if `now >= next_probe_time()`, meaning we should send a packet now.
    pub fn should_send_now(&self, now: Instant) -> bool {
        now >= self.next_padding_time()
    }

    /// Calculate how much padding should be generated for the next probe packet.
    ///
    /// WebRTC probing uses **bursts** of packets, with a minimum delta between bursts
    /// (`min_probe_delta`). In str0m, a `PaddingRequest` is expressed as a number of bytes
    /// to queue (which may result in multiple RTP padding packets), so we request a burst
    /// sized approximately as:
    ///
    /// `burst_bytes = target_bitrate * min_probe_delta`
    ///
    /// This avoids unintentionally capping probe throughput at `MAX_PADDING_PACKET_SIZE /
    /// min_probe_delta` (e.g. 240 bytes / 2ms = 960 kbit/s), which would be far below the
    /// intended probe bitrate.
    ///
    /// Returns `None` if it's not time to send yet, or if we've already created
    /// a padding packet for this instant.
    pub fn next_packet(&mut self, now: Instant) -> Option<DataSize> {
        if self.started_at.is_none() {
            self.started_at = Some(now);
        }

        // Enforce min burst spacing (`min_probe_delta`) between successive padding requests.
        // (The very first burst is not gated because `last_padding_at` starts at already_happened()).
        if now < self.last_padding_at + self.config.min_probe_delta() {
            return None;
        }

        if now < self.next_probe_time() {
            return None;
        }

        // Check if we've already created a padding packet for this exact instant
        // This prevents creating multiple padding packets when handle_timeout() is
        // called multiple times with the same `now` value
        if self.last_padding_at >= now {
            return None;
        }

        // Mark that we've created padding for this instant
        self.last_padding_at = now;

        // It's time to send a new probe burst.
        // Calculate recommended probe size as target_bitrate * min_probe_delta.
        let min_delta = self.config.min_probe_delta();
        let scheduling_credit = if self.config.kind == ProbeKind::Validation {
            self.validation_next_at
                .map(|next| now.saturating_duration_since(next).min(MAX_PROBE_DELAY))
                .unwrap_or(Duration::ZERO)
        } else {
            Duration::ZERO
        };
        // Fill already earned, bounded scheduling credit as well as one future
        // generation interval. Packet deadlines, not queued bytes, authorize
        // actual sends. Media/RTX have already advanced the same packet ledger.
        let recommended_probe_size = self.config.target_bitrate * (min_delta + scheduling_credit);

        // Ensure we request at least one padding packet worth of bytes even if min_delta is 0.
        let recommended_probe_size = DataSize::bytes(cmp::max(
            recommended_probe_size.as_bytes_i64(),
            MAX_PADDING_PACKET_SIZE.as_bytes_i64(),
        ));

        // Calculate remaining bytes needed to complete the probe cluster.
        let bytes_remaining = self.config.target_bytes().saturating_sub(self.bytes_sent);

        // Like BitrateProber::ProbeSent, completion requires both the byte and
        // packet thresholds. At low rates the byte target can fit in one packet;
        // keep sending bounded padding until the minimum packet count is met.
        let request_bytes = if bytes_remaining == DataSize::ZERO
            && self.packets_sent < self.config.min_packet_count
        {
            MAX_PADDING_PACKET_SIZE
        } else {
            cmp::min(bytes_remaining, recommended_probe_size)
        };

        if request_bytes == DataSize::ZERO {
            None
        } else {
            Some(request_bytes)
        }
    }

    pub(crate) fn take_validation_credit_discard(&mut self) -> Option<Duration> {
        self.pending_validation_credit_discard.take()
    }

    /// Record a packet that was sent as part of this probe (media or padding).
    ///
    /// This updates the probe's tracking state so that `next_probe_time()` correctly
    /// calculates when the next packet should be sent based on the target bitrate.
    ///
    /// Should be called by the pacer whenever ANY packet is sent during an active probe,
    /// not just padding packets generated by `next_packet()`.
    pub fn record_packet(&mut self, now: Instant, size: DataSize) {
        // If a probe is active and we observe a media packet before any probe-generated padding,
        // treat that as the start of the probe. This keeps timing semantics consistent and ensures
        // `next_probe_time()` enforces `min_probe_delta` from the first packet onwards.
        if self.started_at.is_none() {
            self.started_at = Some(now);
        }

        self.bytes_sent += size;
        self.packets_sent += 1;
        self.last_packet_at = Some(now);
        if self.config.kind == ProbeKind::Validation {
            // Padding-generation spacing is not the scheduler's wake-up budget.
            // Preserve at most the existing 10ms native lateness tolerance;
            // this is bounded debt, not a higher requested bandwidth.
            let credit = MAX_PROBE_DELAY;
            let floor = now.checked_sub(credit).unwrap_or(now);
            if let Some(previous) = self.validation_next_at
                && previous < floor
            {
                self.pending_validation_credit_discard = Some(floor.duration_since(previous));
            }
            let base = self.validation_next_at.unwrap_or(floor).max(floor);
            self.validation_next_at = Some(base + size / self.config.target_bitrate);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn only_positive_actual_validation_credit_discard_emits_one_shot_boundary() {
        let start = Instant::now();
        let config = ProbeClusterConfig::new(0.into(), Bitrate::kbps(200), ProbeKind::Validation)
            .with_duration(Duration::from_secs(2));
        let mut state = ProbeClusterState::new(config);
        state.record_packet(start, DataSize::bytes(250));
        assert_eq!(state.take_validation_credit_discard(), None);
        // Initial virtual time is start-10ms+10ms = start. This real send
        // drops exactly40ms credit at a50ms wake; no poll-only event exists.
        let at = start + Duration::from_millis(50);
        state.record_packet(at, DataSize::bytes(250));
        assert_eq!(
            state.take_validation_credit_discard(),
            Some(Duration::from_millis(40))
        );
        assert_eq!(state.take_validation_credit_discard(), None);
        state.record_packet(at, DataSize::bytes(250));
        assert_eq!(state.take_validation_credit_discard(), None);
        let mut ordinary = ProbeClusterState::new(ProbeClusterConfig::new(
            1.into(),
            Bitrate::kbps(200),
            ProbeKind::Initial,
        ));
        ordinary.record_packet(start, DataSize::bytes(250));
        ordinary.record_packet(at, DataSize::bytes(250));
        assert_eq!(ordinary.take_validation_credit_discard(), None);
    }

    #[test]
    fn validation_padding_generation_and_two_second_wall_deadline_are_separate() {
        let start = Instant::now();
        let config = ProbeClusterConfig::new(0.into(), Bitrate::mbps(2), ProbeKind::Validation)
            .with_duration(Duration::from_secs(2))
            .with_requested_at(start);
        let mut state = ProbeClusterState::new(config);
        let size = state.next_packet(start).unwrap();
        state.record_packet(start, DataSize::bytes(280));
        // Queued packets may spend earned credit immediately, but an empty
        // padding queue waits for a fresh request burst instead of busy-spinning.
        assert!(state.next_probe_time() <= start);
        assert_eq!(state.next_padding_time(), start + Duration::from_millis(2));
        assert!(size > DataSize::ZERO);
        assert!(!state.is_complete(start + Duration::from_secs(2) - Duration::from_nanos(1)));
        assert!(state.is_complete(start + Duration::from_secs(2)));
    }

    #[test]
    fn long_validation_stall_credit_is_bounded_to_one_burst_plus_packet() {
        let start = Instant::now();
        for rate in [Bitrate::mbps(2), Bitrate::mbps(10), Bitrate::mbps(20)] {
            let config = ProbeClusterConfig::new(0.into(), rate, ProbeKind::Validation)
                .with_duration(Duration::from_secs(2));
            for delay in [Duration::from_millis(50), Duration::from_secs(1)] {
                let mut state = ProbeClusterState::new(config);
                state.record_packet(start, DataSize::bytes(240));
                let resumed = start + delay;
                let mut sent = DataSize::ZERO;
                while state.next_probe_time() <= resumed {
                    state.record_packet(resumed, DataSize::bytes(240));
                    sent += DataSize::bytes(240);
                    assert!(sent <= rate * MAX_PROBE_DELAY + DataSize::bytes(240));
                }
                assert!(sent > DataSize::ZERO);
                assert!(state.next_probe_time() > resumed);
            }
        }
    }

    #[test]
    fn validation_padding_supply_covers_earned_credit_without_authorizing_extra_sends() {
        let start = Instant::now();
        let rate = Bitrate::mbps(2);
        let mut state = ProbeClusterState::new(
            ProbeClusterConfig::new(0.into(), rate, ProbeKind::Validation)
                .with_duration(Duration::from_secs(2)),
        );
        state.record_packet(start, DataSize::bytes(240));
        let wake = start + Duration::from_millis(10);
        let requested = state.next_packet(wake).unwrap();
        assert!(requested > rate * Duration::from_millis(2));
        assert!(requested <= rate * (MAX_PROBE_DELAY + Duration::from_millis(2)));
        // Merely queueing padding consumes no actual packet credit.
        let before = state.next_probe_time();
        assert!(state.next_packet(wake).is_none());
        assert_eq!(state.next_probe_time(), before);
    }

    #[test]
    fn validation_irregular_wakes_and_stalls_obey_every_interval_sender_envelope() {
        for rate in [Bitrate::kbps(300), Bitrate::mbps(2), Bitrate::mbps(20)] {
            let start = Instant::now();
            let mut state = ProbeClusterState::new(
                ProbeClusterConfig::new(0.into(), rate, ProbeKind::Validation)
                    .with_duration(Duration::from_secs(2)),
            );
            let mut now = start;
            let mut sends = Vec::new();
            for delay_ms in [2, 7, 3, 10, 4, 9, 6, 5, 50, 2, 10, 1000, 3, 7] {
                now += Duration::from_millis(delay_ms);
                for _ in 0..16 {
                    if state.next_probe_time() > now {
                        break;
                    }
                    let size = DataSize::bytes([120, 240, 358, 1200][sends.len() % 4]);
                    state.record_packet(now, size);
                    sends.push((now, size));
                }
            }
            // Includes 50ms/1s stalls: only the last 10ms debt survives, never
            // a whole stale burst. Variable protected packet sizes remain real.
            for first in 0..sends.len() {
                let mut total = DataSize::ZERO;
                for last in first..sends.len() {
                    total += sends[last].1;
                    let span = sends[last].0.duration_since(sends[first].0);
                    assert!(total <= rate * (span + MAX_PROBE_DELAY) + DataSize::bytes(1200));
                }
            }
        }
    }

    #[test]
    fn explicit_dual_pacing_strength_has_its_own_bounded_packet_envelope() {
        let floor = Bitrate::mbps(2);
        let pacing = floor * 1.2;
        let start = Instant::now();
        let mut state = ProbeClusterState::new(
            ProbeClusterConfig::new(99.into(), pacing, ProbeKind::Validation)
                .with_duration(Duration::from_secs(2)),
        );
        let mut now = start;
        let mut sends = Vec::new();
        for delay in [1, 5, 10, 50, 2, 3, 1000, 2, 7, 8] {
            now += Duration::from_millis(delay);
            for _ in 0..16 {
                if state.next_probe_time() > now {
                    break;
                }
                let bytes = DataSize::bytes([240, 358, 1200][sends.len() % 3]);
                state.record_packet(now, bytes);
                sends.push((now, bytes));
            }
        }
        for first in 0..sends.len() {
            let mut bytes = DataSize::ZERO;
            for last in first..sends.len() {
                bytes += sends[last].1;
                let elapsed = sends[last].0.duration_since(sends[first].0);
                assert!(bytes <= pacing * (elapsed + MAX_PROBE_DELAY) + DataSize::bytes(1200));
            }
        }
        assert_eq!(state.config().target_bitrate(), pacing);
        assert!(state.is_complete(start + Duration::from_secs(2) + Duration::from_millis(1)));
    }

    #[test]
    fn late_probe_boundary_is_strict_and_uses_actual_send_progress() {
        let now = Instant::now();
        for bitrate in [Bitrate::kbps(200), Bitrate::mbps(2), Bitrate::mbps(20)] {
            let config = ProbeClusterConfig::new(0.into(), bitrate, ProbeKind::Initial);
            let mut state = ProbeClusterState::new(config);
            state.record_packet(now, DataSize::bytes(240));
            let deadline = state.next_probe_time();
            assert!(!state.is_late(deadline));
            assert!(!state.is_late(deadline + Duration::from_millis(10)));
            assert!(state.is_late(deadline + Duration::from_millis(10) + Duration::from_nanos(1)));
            assert!(!state.is_complete(deadline));
        }
    }

    #[test]
    fn queued_or_generated_but_unsent_probe_has_no_finite_late_deadline() {
        let now = Instant::now();
        let config = ProbeClusterConfig::new(0.into(), Bitrate::mbps(2), ProbeKind::Initial);
        let mut state = ProbeClusterState::new(config);
        assert!(!state.is_late(now + Duration::from_secs(60)));
        assert!(state.next_packet(now).is_some());
        // Generating padding is not actual transmission and cannot start expiry.
        assert!(!state.is_late(now + Duration::from_secs(60)));
        assert!(!state.is_complete(now + Duration::from_secs(60)));
    }

    #[test]
    fn pacing_stall_cannot_make_old_train_valid_for_catch_up() {
        let now = Instant::now();
        let config = ProbeClusterConfig::new(0.into(), Bitrate::mbps(4), ProbeKind::Initial);
        let mut state = ProbeClusterState::new(config);
        let bytes = state.next_packet(now).unwrap();
        state.record_packet(now, bytes);
        assert!(!state.is_late(now));
        assert!(state.is_late(state.next_probe_time() + Duration::from_millis(11)));
        // A fresh replacement has its own schedule instead of inheriting old debt.
        let mut replacement = ProbeClusterState::new(config);
        let resumed = now + Duration::from_secs(1);
        assert!(!replacement.is_late(resumed));
        assert!(replacement.next_packet(resumed).is_some());
        replacement.record_packet(resumed, bytes);
        assert!(!replacement.is_late(resumed));
        assert!(replacement.next_probe_time() > resumed);
    }

    #[test]
    fn low_rate_padding_reaches_minimum_packet_count() {
        let now = Instant::now();
        let config = ProbeClusterConfig::new(0.into(), Bitrate::kbps(10), ProbeKind::Initial);
        let mut state = ProbeClusterState::new(config);
        for i in 0..config.min_packet_count() {
            let at = if i == 0 { now } else { state.next_probe_time() };
            let size = state.next_packet(at).expect("minimum packets still needed");
            assert!(size <= MAX_PADDING_PACKET_SIZE);
            state.record_packet(at, size);
        }
        assert!(state.is_complete(state.next_probe_time()));
    }

    // Test helper to directly set state for testing
    impl ProbeClusterState {
        fn test_set_state(&mut self, bytes_sent: DataSize, packets_sent: usize, now: Instant) {
            self.bytes_sent = bytes_sent;
            self.packets_sent = packets_sent;
            if self.started_at.is_none() {
                self.started_at = Some(now);
            }
            self.last_packet_at = Some(now);
        }
    }

    #[test]
    fn probe_cluster_not_complete_before_start() {
        let now = Instant::now();
        let config = ProbeClusterConfig::new(1.into(), Bitrate::mbps(3), ProbeKind::Initial);
        let state = ProbeClusterState::new(config);

        assert!(!state.is_complete(now));
    }

    #[test]
    fn probe_cluster_not_complete_after_packets_but_not_bytes() {
        let now = Instant::now();
        let config = ProbeClusterConfig::new(1.into(), Bitrate::mbps(3), ProbeKind::Initial);
        // target_bytes = 3 Mbps * 15ms = 45,000 bits = 5,625 bytes
        let mut state = ProbeClusterState::new(config);

        // Send 5 packets (meets packet count) but only small packets (doesn't meet bytes)
        // 5 * 100 = 500 bytes, which is < 5,625 bytes
        state.test_set_state(DataSize::bytes(500), 5, now);

        // Not complete: packets met but bytes not met
        assert!(!state.is_complete(now));
    }

    #[test]
    fn probe_cluster_not_complete_after_bytes_but_not_packets() {
        let now = Instant::now();
        let config = ProbeClusterConfig::new(1.into(), Bitrate::mbps(3), ProbeKind::Initial);
        // target_bytes = 3 Mbps * 15ms = 45,000 bits = 5,625 bytes
        let mut state = ProbeClusterState::new(config);

        // Send 2 large packets (meets bytes) but not enough packets
        // 2 * 3000 = 6000 bytes > 5,625 bytes, but only 2 packets < 5 packets
        state.test_set_state(DataSize::bytes(6000), 2, now);

        // Not complete: bytes met but packets not met
        assert!(!state.is_complete(now));
    }

    #[test]
    fn probe_cluster_complete_when_both_criteria_met() {
        let now = Instant::now();
        let config = ProbeClusterConfig::new(1.into(), Bitrate::mbps(3), ProbeKind::Initial);
        // target_bytes = 3 Mbps * 15ms = 45,000 bits = 5,625 bytes
        let mut state = ProbeClusterState::new(config);

        // Send 5 packets of 1200 bytes each = 6000 bytes
        // Meets both: 6000 >= 5625 bytes AND 5 >= 5 packets
        state.test_set_state(DataSize::bytes(6000), 5, now);

        // Complete: both bytes and packets met, even at same instant
        assert!(state.is_complete(now));
    }

    #[test]
    fn probe_cluster_complete_even_with_zero_duration() {
        let now = Instant::now();
        let config = ProbeClusterConfig::new(1.into(), Bitrate::mbps(3), ProbeKind::Initial);
        let mut state = ProbeClusterState::new(config);

        // Send all packets at exactly the same instant (duration = 0)
        state.test_set_state(DataSize::bytes(6000), 5, now);

        // Should still complete (this is the bug fix - no duration requirement)
        assert!(state.is_complete(now));
    }

    #[test]
    fn probe_cluster_tracks_bytes_and_packets() {
        let now = Instant::now();
        let config = ProbeClusterConfig::new(1.into(), Bitrate::mbps(3), ProbeKind::Initial);
        let mut state = ProbeClusterState::new(config);

        state.test_set_state(DataSize::bytes(1200), 1, now);
        assert_eq!(state.bytes_sent, DataSize::bytes(1200));
        assert_eq!(state.packets_sent, 1);

        state.test_set_state(DataSize::bytes(2200), 2, now + Duration::from_millis(1));
        assert_eq!(state.bytes_sent, DataSize::bytes(2200));
        assert_eq!(state.packets_sent, 2);
    }

    #[test]
    fn min_probe_delta_is_enforced_in_next_probe_time() {
        let now = Instant::now();
        let config = ProbeClusterConfig::new(1.into(), Bitrate::mbps(3), ProbeKind::Initial)
            .with_min_probe_delta(Duration::from_millis(20));
        let mut state = ProbeClusterState::new(config);
        assert_eq!(state.config().min_probe_delta(), Duration::from_millis(20));

        // Start a burst at `now`, then verify that a second burst is blocked until >= now+20ms.
        assert!(state.next_packet(now).is_some());
        assert!(state.next_packet(now + Duration::from_millis(19)).is_none());
        assert!(state.next_packet(now + Duration::from_millis(20)).is_some());
        let next = state.last_padding_at;
        assert!(
            next >= now + Duration::from_millis(20),
            "next_probe_time {next:?} must be >= now + min_probe_delta"
        );
    }
}
