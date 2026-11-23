// Copyright 2023 LiveKit, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Application-owned send-side BWE: the `bwe.BWE` implementation LiveKit
//! selects when `use_send_side_bwe` is true.
//!
//! Upstream at commit `e5655ac20267fa5005dccaf3f0e3bfc3ea688c07`, the three
//! objects one peer connection owns for its sending direction are
//! `pkg/sfu/bwe/sendsidebwe/send_side_bwe.go` (the facade, which embeds
//! `bwe.NullBWE` and a `*congestionDetector`),
//! `pkg/sfu/pacer/probe_observer.go` (cluster byte accounting) and
//! `pkg/sfu/pacer/base.go:113` (the metering rule for both). This module owns
//! all three behind one serialized input stream, because the Rust transport has
//! a single owner of `Rtc`:
//!
//! ```text
//! pacer.patchRTPHeaderExtensions        ->  SendSideBwe::handle(Sent(..))
//! downtrack RTCP reader -> allocator    ->  SendSideBwe::handle(Report{..})
//! streamallocator periodic ping         ->  probe_cluster_* / reset / update_rtt
//! ```
//!
//! Deliberate port decisions, each of which a test below pins:
//!
//! * Upstream *assigns* the transport-wide sequence number inside the BWE
//!   (`RecordPacketSendAndGetSequenceNumber` returns it to the pacer, which
//!   stamps it). str0m owns the extension value, so this port feeds the actual
//!   stamped value in and only reconstructs the extended counter. The metered
//!   size is `packet_bytes`, the plain RTP datagram including RTP padding,
//!   because that is `p.HeaderSize + len(p.Payload) + p.Header.PaddingSize`
//!   (`pacer/base.go:113`), not the payload-only size the Pion interceptor uses.
//! * Upstream records the estimator packet only when the media negotiated the
//!   transport-wide extension (`p.TransportWideExtID != 0 && b.bwe != nil`) but
//!   always gives the packet to the probe observer
//!   (`b.ProbeObserver.RecordPacket(..)` is outside that `if`). Both gates are
//!   kept separately.
//! * `ProbeClusterId` is stamped per packet by the downtrack from the value the
//!   allocator published at cluster switch. The Rust owner publishes it itself,
//!   so the stamp it applies while observing a send is its own active cluster
//!   id, which is what the pacer would have carried for that same packet.
//! * The observer stamps `mono.UnixNano()`; this port stamps nanoseconds since
//!   the owner's own `Instant` origin. Only differences of these stamps are
//!   ever read (`Goal.Duration` comparison, `Result.Duration()`), so the unit
//!   choice is unobservable, and it keeps the clock monotonic.
//! * `SendSideFeedback::Discontinuity` has no upstream counterpart: the vendor
//!   boundary retired its queue because an application stopped draining it, so
//!   the sent history the estimator depends on is provably incomplete. Retiring
//!   the owner is the only honest reaction; the driver recreates it.
//! * Upstream's facade embeds `bwe.NullBWE`, so in this mode `HandleREMB`,
//!   `GetChannelCapacity` and the bandwidth-observer calls do nothing at all.
//!   The REMB-driven driver branch therefore has no send-side counterpart and
//!   must not be faked.
//! * The allocator-facing surface listed in the map above (`UpdateRTT`,
//!   `CanProbe`, `ProbeDuration`, `ProbeClusterStarting`/`ProbeClusterDone`,
//!   `ProbeSignal`) has no caller in the shipped build yet, because upstream
//!   reaches it only from `streamallocator.go:778` and `:1326` and this batch
//!   leaves the allocator out of scope. The strict CI gate
//!   (`cargo clippy --workspace --all-targets -- -D warnings`) fails on exactly
//!   that boundary, so each item the release-shape build reports carries its
//!   never-read state fields only those items read — naming the C74-e wiring
//!   that removes it. Nothing is module-suppressed, stubbed or blanket-allowed:
//!   the items stay compiled and lint-checked in both builds, and the parity
//!   harness (`oracle_input`, `#[cfg(test)]`) remains the consumer that
//!   executes every one of them.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use rtc_core::config::{CongestionControl, PolicyDuration, SendSidePacer};
use str0m::{SendSideFeedback, SendSidePacket};

use crate::pion_gcc::{Chunk, CompoundFeedback, FeedbackError, TwccReport, parse_compound};

mod bitmap;
mod ccutils;
mod detector;
mod feedback;
mod group;
mod measure;
#[cfg(test)]
mod oracle_input;
mod probe_group;
mod prober;
mod regulator;
mod time;
mod tracker;
mod traffic;
mod trend;

/// Re-exported because the driver builds a goal and reads the probe signal,
/// while upstream's allocator reaches `ccutils` directly.
pub(crate) use ccutils::{
    PROBE_CLUSTER_ID_INVALID, ProbeClusterGoal, ProbeClusterId, ProbeClusterInfo,
    ProbeClusterResult, ProbeSignal,
};
use detector::{CongestionDetector, TwccFeedbackReport};
use prober::{ProbeClusterMode, Prober, ProberEvent};
use rtc_core::config::StreamAllocatorConfig;

/// `bwe.CongestionState` as the driver sees it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum CongestionState {
    #[default]
    None,
    EarlyWarning,
    Congested,
}

impl CongestionState {
    fn from_detector(state: detector::CongestionState) -> Self {
        match state {
            detector::CongestionState::None => Self::None,
            detector::CongestionState::EarlyWarning => Self::EarlyWarning,
            detector::CongestionState::Congested => Self::Congested,
        }
    }
}

/// `bwe.BWEListener.OnBWEStateChange(from, to, estimatedAvailableChannelCapacity)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct StateChange {
    pub(crate) from: CongestionState,
    pub(crate) to: CongestionState,
    pub(crate) capacity_bps: i64,
}

impl StateChange {
    fn from_detector(change: detector::StateChange) -> Self {
        Self {
            from: CongestionState::from_detector(change.from),
            to: CongestionState::from_detector(change.to),
            capacity_bps: change.capacity_bps,
        }
    }
}

/// What one input produced, in the order upstream queued its events.
#[derive(Clone, Debug)]
pub(crate) struct Outcome {
    /// The input's own clock, which is what upstream timestamps the queued
    /// event with (`time.Now()` when the signal is dispatched).
    pub(crate) at: Instant,
    /// Detector congestion-state transitions, at most one per report.
    pub(crate) state_changes: Vec<StateChange>,
    /// The pacer probe observer completed this cluster itself, which upstream
    /// delivers as `OnPacerProbeObserverClusterComplete`.
    // no shipped caller yet: the `#[cfg(test)]` oracle replay executes it; C74-e wires the pacer/prober/allocator consumer
    pub(crate) completed: Option<ProbeClusterInfo>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum InputError {
    /// `packet.go` admitted nothing, so nothing was delivered either.
    Wire(&'static str),
    /// A non-wire `FeedbackError` reached this boundary.
    Admission(FeedbackError),
    /// A media negotiated the transport-wide extension but the emitted datagram
    /// carries no value for it, so the estimator has nothing to record.
    MissingTwcc,
    /// Retired by an earlier observation discontinuity.
    Retired,
}

impl From<FeedbackError> for InputError {
    fn from(error: FeedbackError) -> Self {
        // `parse_compound` only produces `Wire`, but the shared error type has
        // interceptor variants that must not be renamed into invented strings.
        match error {
            FeedbackError::Wire(message) => Self::Wire(message),
            other => Self::Admission(other),
        }
    }
}

impl std::fmt::Display for InputError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Wire(error) => f.write_str(error),
            Self::Admission(error) => write!(f, "feedback admission failed: {error:?}"),
            Self::MissingTwcc => f.write_str("missing transport wide sequence number extension"),
            Self::Retired => f.write_str("send side BWE retired after an input discontinuity"),
        }
    }
}

/// Read-only diagnostics; no quantity here grants capacity.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Stats {
    pub(crate) sent_packets: u64,
    /// Sends the estimator could meter, i.e. transport-wide extension negotiated.
    pub(crate) tracked_packets: u64,
    pub(crate) untracked_packets: u64,
    pub(crate) report_datagrams: u64,
    pub(crate) feedback_packets: u64,
    pub(crate) twcc_reports: u64,
    /// TWCC reports whose media SSRC this transport never emitted.
    pub(crate) twcc_unknown_ssrc: u64,
    pub(crate) rfc8888_reports: u64,
    pub(crate) state_changes: u64,
    pub(crate) cluster_starts: u64,
    pub(crate) ignored_cluster_starts: u64,
    pub(crate) cluster_ends: u64,
    pub(crate) ignored_cluster_ends: u64,
    pub(crate) mismatched_cluster_ends: u64,
    pub(crate) auto_completed_clusters: u64,
    /// Padding datagrams the vendor actually handed to the protection path, and
    /// their octets. `handleSignalSendProbe` is only a demand: this is the
    /// traffic that demand produced, counted where the datagram is observable.
    pub(crate) padding_packets: u64,
    pub(crate) padding_bytes: u64,
    /// Octets of that padding charged into the prober bucket because they
    /// carried a valid cluster stamp. This is upstream's `bytesSent` running
    /// total, i.e. the quantity `prober.ProbesSent(bytesSent)` sees.
    pub(crate) probe_bucket_bytes: u64,
    /// Padding the bucket meter refused to charge: a datagram stamped `Invalid`
    /// is traffic, but not evidence for any cluster.
    pub(crate) uncharged_padding_packets: u64,
    pub(crate) uncharged_padding_bytes: u64,
}

/// `pacer.go`'s constructor choice for the sending direction. Both available
/// behaviours are unpaced here, and `leaky_bucket` is the one documented
/// non-equivalence of this port.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PacerDecision {
    /// `pacerBehavior: ""` — upstream logs nothing and creates no pacer
    /// override; the media scheduler's own output timing is what leaves.
    Unspecified,
    /// `NewPassThrough`: every queued packet is written when it is queued.
    PassThrough,
    /// `NewNoQueue`: same observable output for this port, no extra queue.
    NoQueue,
    /// Upstream's `default` branch: `leaky_bucket` is not a constructible
    /// pacer in the pinned source (`NewLeakyBucket` is only built by tests and
    /// `SetBitrate` is never called in production), so the fixed server logs
    /// "send side BWE pacer unavailable, falling back" and uses `NewNoQueue`.
    /// This port records the same decision instead of inventing a rate limiter.
    LeakyBucketFallback,
}

impl PacerDecision {
    pub(crate) fn from_config(pacer: SendSidePacer) -> Self {
        match pacer {
            SendSidePacer::Unspecified => Self::Unspecified,
            SendSidePacer::PassThrough => Self::PassThrough,
            SendSidePacer::NoQueue => Self::NoQueue,
            SendSidePacer::LeakyBucket => Self::LeakyBucketFallback,
        }
    }

    /// Whether the decision needs the recorded product-decision warning.
    pub(crate) fn is_fallback(self) -> bool {
        self == Self::LeakyBucketFallback
    }
}

/// `pacer.ProbeObserver`. The `isInProbe` flag and the cluster info are one
/// atomic decision pair here, which is what upstream's separate atomic plus
/// mutex amount to under a single owner.
#[derive(Clone, Copy, Debug, Default)]
struct ProbeObserver {
    pci: ProbeClusterInfo,
    is_in_probe: bool,
    starts: u64,
    ignored_starts: u64,
    ends: u64,
    ignored_ends: u64,
    mismatched_ends: u64,
    auto_completed: u64,
}

impl ProbeObserver {
    /// `StartProbeCluster`: a second start while active is only logged.
    fn start(&mut self, pci: ProbeClusterInfo, at: Instant, origin: Instant) -> bool {
        if self.is_in_probe {
            self.ignored_starts += 1;
            return false;
        }
        let mut started = pci;
        started.result = ProbeClusterResult {
            start_time_ns: mono_ns(at, origin),
            ..ProbeClusterResult::default()
        };
        self.pci = started;
        self.is_in_probe = true;
        self.starts += 1;
        true
    }

    /// `EndProbeCluster`: inactive or non-active ids return
    /// `ProbeClusterInfoInvalid`, and the end stamp is taken exactly once.
    fn end(
        &mut self,
        id: ProbeClusterId,
        at: Instant,
        origin: Instant,
    ) -> Option<ProbeClusterInfo> {
        if !self.is_in_probe {
            if id != PROBE_CLUSTER_ID_INVALID {
                self.ignored_ends += 1;
            }
            return None;
        }
        if self.pci.id != id {
            self.mismatched_ends += 1;
            return None;
        }
        if self.pci.result.end_time_ns == 0 {
            self.pci.result.end_time_ns = mono_ns(at, origin);
        }
        self.is_in_probe = false;
        self.ends += 1;
        Some(self.pci)
    }

    /// `RecordPacket`, including the completion edge that fires
    /// `OnPacerProbeObserverClusterComplete`.
    fn record(
        &mut self,
        size: i64,
        is_rtx: bool,
        probe_cluster_id: ProbeClusterId,
        is_probe: bool,
        at: Instant,
        origin: Instant,
    ) -> bool {
        if !self.is_in_probe {
            return false;
        }
        if probe_cluster_id != self.pci.id || self.pci.result.end_time_ns != 0 {
            return false;
        }
        if is_probe {
            self.pci.result.packets_probe += 1;
            self.pci.result.bytes_probe += size;
        } else if is_rtx {
            self.pci.result.packets_non_probe_rtx += 1;
            self.pci.result.bytes_non_probe_rtx += size;
        } else {
            self.pci.result.packets_non_probe_primary += 1;
            self.pci.result.bytes_non_probe_primary += size;
        }
        // Go's `time.Duration(mono.UnixNano() - StartTime) >= Goal.Duration`,
        // with the same int64 wraparound as the subtraction upstream does.
        let elapsed = mono_ns(at, origin).wrapping_sub(self.pci.result.start_time_ns);
        if elapsed < self.pci.goal.duration.as_nanos_i64() {
            return false;
        }
        if self.pci.result.bytes() < self.pci.goal.desired_bytes {
            return false;
        }
        self.pci.result.end_time_ns = mono_ns(at, origin);
        self.pci.result.is_completed = true;
        self.auto_completed += 1;
        true
    }
}

/// Nanoseconds since the owner's origin, signed like `mono.UnixNano()` is
/// relative, so a clock read before the origin stays negative.
fn mono_ns(at: Instant, origin: Instant) -> i64 {
    match at.checked_duration_since(origin) {
        Some(delta) => delta.as_nanos().min(i64::MAX as u128) as i64,
        None => -(origin.duration_since(at).as_nanos().min(i64::MAX as u128) as i64),
    }
}

/// The wire boundary of `HandleTWCCFeedback`. Pion keeps the chunk stream and
/// the delta stream in their wire encoding; Go expands the chunks while walking
/// them and stops as soon as the sequence reaches `base + packet_status_count`.
/// Because a chunk can only contribute symbols, that walk is exactly the first
/// `packet_status_count` symbols of this expansion, and the detector performs
/// the break itself, so the overshoot a trailing status-vector chunk can produce
/// is carried through rather than trimmed here.
fn twcc_report(report: &TwccReport) -> TwccFeedbackReport {
    let mut symbols = Vec::new();
    for chunk in &report.chunks {
        match chunk {
            Chunk::Run { symbol, length } => {
                symbols.extend(std::iter::repeat_n(*symbol, usize::from(*length)))
            }
            Chunk::Vector(symbols_of_chunk) => symbols.extend(symbols_of_chunk.iter().copied()),
        }
    }
    TwccFeedbackReport {
        base_sequence_number: report.base,
        packet_status_count: report.status_count,
        reference_time: report.reference,
        fb_pkt_count: report.fb_pkt_count,
        symbols,
        deltas_us: report.deltas_us.clone(),
    }
}

/// `StreamAllocatorConfig` values the probe lifecycle reads. Upstream keeps
/// them in the allocator; here they are the constructor's snapshot of the same
/// normalized policy.
#[derive(Clone, Copy, Debug)]
struct AllocatorKnobs {
    /// `ProbeMode == "media"`: boost-and-reset instead of a padding cluster.
    probe_with_media: bool,
    /// `ProbeOveragePct`, applied as Go's integer `/100` after the multiply.
    probe_overage_pct: i64,
    /// `ProbeMinBps`, the floor of `max(overage, floor)`.
    probe_min_bps: i64,
    /// `PausedMinWait`, the paused-reset quiet period.
    paused_min_wait: Duration,
}

impl AllocatorKnobs {
    fn from_config(config: &StreamAllocatorConfig) -> Self {
        Self {
            probe_with_media: config.probe_mode == "media",
            probe_overage_pct: config.probe_overage_pct,
            probe_min_bps: config.probe_min_bps,
            paused_min_wait: config
                .paused_min_wait
                .nonnegative_to_std()
                .unwrap_or(Duration::ZERO),
        }
    }
}

/// What the allocation pass hands the probe lifecycle before each ping: the
/// two track-derived values upstream reads out of its own track registry.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ProbeInputs {
    /// `StreamAllocator.allowPause`.
    pub allow_pause: bool,
    /// `StreamAllocator.state == streamAllocatorStateDeficient`.
    pub deficient: bool,
    /// `StreamAllocator.overriddenChannelCapacity > 0`, which suppresses probing.
    pub override_active: bool,
    /// `StreamAllocator.committedChannelCapacity`, the raw value the goal uses
    /// and the commit-if-higher compares against.
    pub committed_bps: i64,
}

/// The allocator's side of one [`ProbeInputs`] result.
#[derive(Clone, Debug, Default)]
pub(crate) struct ProbePing {
    /// The capacity `handleSignalCongestionStateChange`/probe finalization
    /// committed: already compared against the caller's value, so the caller
    /// assigns it as-is.
    pub commit_bps: Option<i64>,
    /// A non-congesting finalization happened, so Go runs
    /// `maybeBoostDeficientTracks` and `adjustState`: reallocate.
    pub reallocate: bool,
    /// `maybeProbeWithMedia` boosted a track and reset the estimator.
    pub media_boost: bool,
    /// The estimator was reset by the paused reset.
    pub bwe_reset: bool,
    /// `maybeStopProbe` ran, so the pacer must close the probe window it opened
    /// for the cluster the allocator just stopped.
    pub cancel_probe: bool,
    /// The cluster this ping finalized, with the signal its result produced.
    /// Upstream logs the same pair as `stream allocator: probe result`; the
    /// caller books the outcome from it, because a cluster that never
    /// finalized — a refused or self-cancelled window, a reset — is not a
    /// probe outcome at all.
    pub probe_finalized: Option<(ProbeSignal, i64)>,
    /// What the prober's own clock asked the pacer to do.
    pub actions: Vec<ProbeAction>,
}

/// `ccutils.ProberListener`, resolved into what the transport must do.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum ProbeAction {
    /// `OnProbeClusterSwitch`: open a probe window that services the cluster at
    /// its goal rate for its goal duration.
    StartCluster {
        desired_bps: i64,
        duration: Duration,
    },
    /// `OnSendProbe(bytesToSend)`. The bytes themselves leave through the pacer
    /// and are metered back at the input boundary, so this is the bucket demand
    /// only.
    SendProbe(i64),
}

/// `sendsidebwe.SendSideBWE` plus the pacer's probe observer.
pub(crate) struct SendSideBwe {
    detector: CongestionDetector,
    observer: ProbeObserver,
    /// `StreamAllocator.activeProbeClusterId`: the cluster the periodic ping
    /// finalizes and the congestion-state handler defers a capacity commit
    /// against. Upstream clears it only at finalization and at the paused
    /// reset, so it survives the pacer's own `EndProbeCluster`.
    active_cluster_id: ProbeClusterId,
    /// `Pacer.activeProbeClusterId` merged with the per-downtrack stamp
    /// (`SetProbeClusterId`/`SwapProbeClusterId`): what the packets observe, and
    /// what `EndProbeCluster` clears while the field above stays put.
    stamped_cluster_id: ProbeClusterId,
    /// `StreamAllocator.activeProbeGoalReached`.
    active_probe_goal_reached: bool,
    /// `StreamAllocator.activeProbeCongesting`.
    active_probe_congesting: bool,
    /// `ccutils.Prober`: the allocator's own cluster scheduler, whose ids are
    /// the ones the estimator and the observer see.
    prober: Prober,
    /// The interval `Prober.run` last slept, so the driver can wake on the
    /// bucket clock instead of only on its own tick.
    prober_sleep: Option<PolicyDuration>,
    /// The `StreamAllocatorConfig` values `maybeProbe` and the paused reset read.
    allocator: AllocatorKnobs,
    /// `StreamAllocator.nextHigherTransition` input: the bandwidth delta of the
    /// deficient track farthest from its desired layer, refreshed by the
    /// allocation pass that owns the layer costs.
    next_higher_delta_bps: i64,
    /// `StreamAllocator.getExpectedBandwidthUsage`, same reason.
    expected_usage_bps: i64,
    /// `Prober.clusterId` in `AddCluster`: `Inc()` before use, so the first
    /// cluster is 1 and the invalid sentinel is never handed out. Only the
    /// `#[cfg(test)]` parity harness starts a cluster without the prober.
    // no shipped caller yet: the `#[cfg(test)]` oracle replay executes it; C74-e wires the pacer/prober/allocator consumer
    next_cluster_id: ProbeClusterId,
    /// The accepted `MediaSSRC` set. Upstream dispatches a report only to the
    /// downtrack whose `ssrc`/`ssrcRTx` it names, so a report for a stream this
    /// transport never emitted reaches no BWE at all. The set grows from the
    /// sends, which is the only SSRC knowledge this mode has: the remote
    /// feedback meter that would list SSRCs per m-line is deliberately off here.
    media_ssrcs: HashSet<u32>,
    /// `pacer.Base.lastPacketSentAt`, stored inside the transport-wide branch
    /// and read by `TimeSinceLastSentPacket()` for the paused reset.
    last_sent_at: Option<Instant>,
    retired: bool,
    origin: Instant,
    // no shipped caller yet: the `#[cfg(test)]` oracle replay executes it; C74-e wires the pacer/prober/allocator consumer
    pacer: PacerDecision,
    stats: Stats,
}

impl SendSideBwe {
    pub(crate) fn new(config: &CongestionControl, now: Instant) -> Self {
        let pacer = PacerDecision::from_config(config.send_side_bwe_pacer);
        if pacer.is_fallback() {
            // Same one-shot notice as the fixed `default` pacer branch.
            tracing::warn!(
                pacer_behavior = "leaky_bucket",
                fallback = "NoQueue",
                "send side BWE pacer unavailable, falling back"
            );
        }
        Self {
            detector: CongestionDetector::new(
                config.send_side_bwe.congestion_detector.clone(),
                now,
            ),
            observer: ProbeObserver::default(),
            active_cluster_id: PROBE_CLUSTER_ID_INVALID,
            stamped_cluster_id: PROBE_CLUSTER_ID_INVALID,
            active_probe_goal_reached: false,
            active_probe_congesting: false,
            prober: Prober::default(),
            prober_sleep: None,
            allocator: AllocatorKnobs::from_config(&config.stream_allocator),
            next_higher_delta_bps: 0,
            expected_usage_bps: 0,
            next_cluster_id: PROBE_CLUSTER_ID_INVALID,
            media_ssrcs: HashSet::new(),
            last_sent_at: None,
            retired: false,
            origin: now,
            pacer,
            stats: Stats::default(),
        }
    }

    #[cfg_attr(not(test), allow(dead_code))] // still no shipped caller after the C74-e2 probe wiring: test-only observation point
    pub(crate) fn pacer(&self) -> PacerDecision {
        self.pacer
    }

    #[cfg_attr(not(test), allow(dead_code))] // still no shipped caller after the C74-e2 probe wiring: test-only observation point
    pub(crate) fn is_retired(&self) -> bool {
        self.retired
    }

    /// The probe cluster counters come from the observer, because it is the
    /// component that decides whether a start or an end is accepted; counting
    /// them again in the owner would report an ignore the owner never saw.
    pub(crate) fn stats(&self) -> Stats {
        let mut stats = self.stats;
        stats.cluster_starts = self.observer.starts;
        stats.ignored_cluster_starts = self.observer.ignored_starts;
        stats.cluster_ends = self.observer.ends;
        stats.ignored_cluster_ends = self.observer.ignored_ends;
        stats.mismatched_cluster_ends = self.observer.mismatched_ends;
        stats.auto_completed_clusters = self.observer.auto_completed;
        stats
    }

    /// The one authenticated input boundary. Everything a malformed later
    /// packet could have touched stays untouched, because the compound is
    /// parsed before any packet is delivered.
    pub(crate) fn handle(&mut self, event: SendSideFeedback) -> Result<Outcome, InputError> {
        if self.retired {
            return Err(InputError::Retired);
        }
        match event {
            SendSideFeedback::Sent(packet) => self.record_sent(packet),
            SendSideFeedback::Report { at, packet } => self.handle_report(at, &packet),
            SendSideFeedback::Discontinuity => {
                self.retired = true;
                Err(InputError::Retired)
            }
        }
    }

    /// `patchRTPHeaderExtensions` for one actual datagram.
    fn record_sent(&mut self, packet: SendSidePacket) -> Result<Outcome, InputError> {
        self.stats.sent_packets = self.stats.sent_packets.wrapping_add(1);
        self.media_ssrcs.insert(packet.ssrc);

        // Upstream records the estimator first and the observer second, but the
        // observer call sits outside the transport-wide gate, so it happens for
        // every datagram either way. The two orders differ only for the input
        // this boundary cannot produce from str0m: a negotiated media with no
        // stamped value, which is metered here and then reported.
        let stamp = self.stamped_cluster_id;
        let completed = self.observer.record(
            i64::try_from(packet.packet_bytes).unwrap_or(i64::MAX),
            packet.is_rtx,
            stamp,
            packet.is_padding,
            packet.at,
            self.origin,
        );
        let completed = completed.then_some(self.observer.pci);
        if packet.is_padding {
            // Upstream meters probe traffic inside `handleSignalSendProbe`, from
            // the return value of `WriteProbePackets` — bytes the track really
            // wrote. This boundary is the equivalent place: the vendor has
            // already protected and queued the datagram, so the count is of
            // traffic that left, not of traffic that was asked for.
            self.stats.padding_packets = self.stats.padding_packets.wrapping_add(1);
            self.stats.padding_bytes = self
                .stats
                .padding_bytes
                .wrapping_add(u64::try_from(packet.packet_bytes).unwrap_or(u64::MAX));
            if stamp == PROBE_CLUSTER_ID_INVALID {
                self.stats.uncharged_padding_packets =
                    self.stats.uncharged_padding_packets.wrapping_add(1);
                self.stats.uncharged_padding_bytes = self
                    .stats
                    .uncharged_padding_bytes
                    .wrapping_add(u64::try_from(packet.packet_bytes).unwrap_or(u64::MAX));
            } else {
                let bytes = i64::try_from(packet.packet_bytes).unwrap_or(i64::MAX);
                self.prober.probes_sent(bytes);
                self.stats.probe_bucket_bytes = self
                    .stats
                    .probe_bucket_bytes
                    .wrapping_add(u64::try_from(bytes).unwrap_or(u64::MAX));
            }
        }

        // Upstream's estimator gate: only a media that negotiated the
        // transport-wide extension is metered into the packet tracker.
        if packet.twcc_negotiated {
            let sequence = packet.transport_sequence.ok_or(InputError::MissingTwcc)?;
            self.detector.packet_tracker_mut().record_packet_send(
                packet.at,
                packet.packet_bytes,
                packet.is_rtx,
                stamp,
                packet.is_padding,
                sequence,
            );
            self.stats.tracked_packets = self.stats.tracked_packets.wrapping_add(1);
            self.last_sent_at = Some(packet.at);
        } else {
            self.stats.untracked_packets = self.stats.untracked_packets.wrapping_add(1);
        }

        Ok(Outcome {
            at: packet.at,
            state_changes: Vec::new(),
            completed,
        })
    }

    /// `handleSignalFeedback` without the interceptor's `WriteRTCP`, plus the
    /// downtrack dispatch that decides which BWE a report reaches.
    fn handle_report(&mut self, at: Instant, raw: &[u8]) -> Result<Outcome, InputError> {
        let report = parse_compound(raw)?;
        self.stats.report_datagrams = self.stats.report_datagrams.wrapping_add(1);
        let mut outcome = Outcome {
            at,
            state_changes: Vec::new(),
            completed: None,
        };
        for feedback in report.feedback {
            self.stats.feedback_packets = self.stats.feedback_packets.wrapping_add(1);
            match feedback {
                CompoundFeedback::Twcc(twcc) => {
                    self.stats.twcc_reports = self.stats.twcc_reports.wrapping_add(1);
                    if !self.media_ssrcs.contains(&twcc.media_ssrc) {
                        self.stats.twcc_unknown_ssrc = self.stats.twcc_unknown_ssrc.wrapping_add(1);
                        continue;
                    }
                    if let Some(change) =
                        self.detector.handle_twcc_feedback(&twcc_report(&twcc), at)
                    {
                        self.stats.state_changes = self.stats.state_changes.wrapping_add(1);
                        outcome
                            .state_changes
                            .push(StateChange::from_detector(change));
                    }
                }
                // The detector consumes `*rtcp.TransportLayerCC` only. An
                // admitted RFC8888 report is counted and otherwise unused,
                // exactly as upstream's single-typed dispatch leaves it.
                CompoundFeedback::Rfc8888(_) => {
                    self.stats.rfc8888_reports = self.stats.rfc8888_reports.wrapping_add(1);
                }
            }
        }
        Ok(outcome)
    }

    // ------------------------------------------------
    // Estimator state the allocator reads
    // ------------------------------------------------

    /// `EstimatedAvailableChannelCapacity`, the value the allocator commits on a
    /// congestion transition and the ceiling `maybeProbe` compares against.
    pub(crate) fn capacity(&self) -> i64 {
        self.detector.estimated_available_channel_capacity()
    }

    pub(crate) fn congestion_state(&self) -> CongestionState {
        CongestionState::from_detector(self.detector.congestion_state())
    }

    #[cfg_attr(not(test), allow(dead_code))] // still no shipped caller after the C74-e2 probe wiring: test-only observation point
    pub(crate) fn rtt(&self) -> f64 {
        self.detector.rtt()
    }

    /// `UpdateRTT`, driven by the same 30-second poll the remote path uses.
    pub(crate) fn update_rtt(&mut self, rtt: f64) {
        self.detector.update_rtt(rtt);
    }

    /// `CongestionState() == None && !probe running && probeRegulator.CanProbe()`.
    pub(crate) fn can_probe(&self, at: Instant) -> bool {
        self.detector.can_probe(at)
    }

    /// `ProbeDuration`, the goal duration of the next cluster.
    pub(crate) fn probe_duration(&self) -> PolicyDuration {
        self.detector.probe_duration()
    }

    /// `Pacer.TimeSinceLastSentPacket()`. `None` means nothing transport-wide
    /// stamped has ever left this transport, which upstream reports as a
    /// zero-valued `time.Duration` for its unset monotonic time.
    pub(crate) fn time_since_last_sent(&self, now: Instant) -> Option<Duration> {
        now.checked_duration_since(self.last_sent_at?)
    }

    /// `Reset`, which keeps the packet tracker and the TWCC report bookkeeping.
    pub(crate) fn reset(&mut self, at: Instant) {
        self.detector.reset(at);
    }

    pub(crate) fn probe_cluster_id(&self) -> ProbeClusterId {
        self.active_cluster_id
    }

    #[cfg_attr(not(test), allow(dead_code))] // still no shipped caller after the C74-e2 probe wiring: test-only observation point
    pub(crate) fn probe_is_running(&self) -> bool {
        self.detector.probe_is_running()
    }

    /// `handleSignalProbeClusterSwitch`, in upstream's order:
    /// `BWE.ProbeClusterStarting(pci)` then `Pacer.StartProbeCluster(pci)`,
    /// then the per-downtrack stamp. The cluster info handed to the detector
    /// still carries Go's zero result, because only the observer stamps
    /// `StartTime`.
    #[cfg_attr(not(test), allow(dead_code))] // still no shipped caller after the C74-e2 probe wiring: test-only observation point
    pub(crate) fn start_probe_cluster(
        &mut self,
        goal: ProbeClusterGoal,
        at: Instant,
    ) -> ProbeClusterInfo {
        let id = self.next_cluster_id.wrapping_add(1);
        self.next_cluster_id = id;
        let pci = ProbeClusterInfo {
            id,
            created_at: Some(at),
            goal,
            result: ProbeClusterResult::default(),
        };
        self.handle_cluster_switch(pci, at);
        pci
    }

    /// The one place a cluster id becomes visible to the estimator, the
    /// observer and the packet stamp, which is where upstream clears its
    /// `activeProbeCongesting`/`goalReached` pair.
    fn handle_cluster_switch(&mut self, pci: ProbeClusterInfo, at: Instant) {
        self.active_cluster_id = pci.id;
        self.stamped_cluster_id = pci.id;
        self.active_probe_goal_reached = false;
        self.active_probe_congesting = false;
        self.detector.probe_cluster_starting(pci);
        self.observer.start(pci, at, self.origin);
    }

    /// `handleSignalPacerProbeObserverClusterComplete`: end the observer with
    /// the id it reported, clear the stamped id, then hand the recorded result
    /// to the detector so the settle wait can start, and drop the cluster from
    /// the prober's queue.
    pub(crate) fn end_probe_cluster(
        &mut self,
        id: ProbeClusterId,
        at: Instant,
    ) -> ProbeClusterInfo {
        let ended = self.observer.end(id, at, self.origin);
        self.stamped_cluster_id = PROBE_CLUSTER_ID_INVALID;
        let pci = ended.unwrap_or_default();
        // Upstream calls `ProbeClusterDone` with whatever `EndProbeCluster`
        // returned, including `ProbeClusterInfoInvalid` for an id it does not
        // own; the detector's own id checks decide what that does.
        self.detector.probe_cluster_done(&pci, at);
        self.prober.cluster_done(&pci);
        pci
    }

    /// `maybeStopProbe`, which is guarded on the allocator's own cluster id and
    /// resets the whole prober queue instead of dropping one cluster. It leaves
    /// the allocator id set: only finalization or the paused reset clears it.
    pub(crate) fn stop_probe_cluster(&mut self, at: Instant) -> Option<ProbeClusterInfo> {
        if self.active_cluster_id == PROBE_CLUSTER_ID_INVALID {
            return None;
        }
        let pci = self.end_probe_cluster(self.active_cluster_id, at);
        self.prober.reset(&pci);
        Some(pci)
    }

    /// `ProbeClusterIsGoalReached`.
    pub(crate) fn probe_cluster_is_goal_reached(&self) -> bool {
        self.detector.probe_cluster_is_goal_reached()
    }

    /// `ProbeClusterFinalize`, which also clears the allocator's active id.
    pub(crate) fn probe_cluster_finalize(&mut self, at: Instant) -> Option<(ProbeSignal, i64)> {
        let finalized = self.detector.probe_cluster_finalize(at);
        if finalized.is_some() {
            self.active_cluster_id = PROBE_CLUSTER_ID_INVALID;
        }
        finalized
    }

    // ------------------------------------------------
    // Allocator probe lifecycle
    // ------------------------------------------------

    /// The layer-derived values `maybeProbe` and the boost pass read straight
    /// from the track registry upstream: the expected bandwidth usage and the
    /// next-higher transition of the deficient track farthest from its desired
    /// layer. `None` is upstream's `available == false`, which is *not* a delta
    /// of zero: a zero delta still schedules a cluster.
    pub(crate) fn note_allocation(
        &mut self,
        expected_usage_bps: i64,
        next_higher_transition_bps: Option<i64>,
    ) {
        self.expected_usage_bps = expected_usage_bps;
        self.next_higher_delta_bps = next_higher_transition_bps.unwrap_or(i64::MIN);
    }

    /// `handleSignalCongestionStateChange`'s probe-side bookkeeping: a
    /// congested transition during an active cluster is remembered so the next
    /// ping does not treat a reached goal as a stop.
    pub(crate) fn note_probe_congesting(&mut self) {
        self.active_probe_congesting = true;
    }

    pub(crate) fn probe_congesting(&self) -> bool {
        self.active_probe_congesting
    }

    /// `handleSignalPeriodicPing` steps A–C plus the prober's bucket clock. The
    /// RTT pull (step D) belongs to the caller, because upstream reads it from
    /// an ICE getter this module has no handle on.
    pub(crate) fn periodic_ping(&mut self, inputs: &ProbeInputs, at: Instant) -> ProbePing {
        let mut ping = ProbePing::default();
        // A: a paused transport that has sent nothing for `PausedMinWait` while
        // the estimator still reports congestion has to be reset, or the probe
        // regulator never clears and the channel stays starved.
        if inputs.allow_pause
            && inputs.deficient
            && self.congestion_state() != CongestionState::None
            && self
                .time_since_last_sent(at)
                .is_some_and(|since| since > self.allocator.paused_min_wait)
        {
            self.stop_probe_cluster(at);
            self.reset(at);
            // The reset drops the in-flight probe group, so its finalization
            // can never clear the id; upstream clears it here for exactly that
            // reason.
            self.active_cluster_id = PROBE_CLUSTER_ID_INVALID;
            ping.bwe_reset = true;
            ping.cancel_probe = true;
        }
        if self.active_cluster_id != PROBE_CLUSTER_ID_INVALID {
            if !self.active_probe_congesting
                && !self.active_probe_goal_reached
                && self.probe_cluster_is_goal_reached()
            {
                self.active_probe_goal_reached = true;
                if self.stop_probe_cluster(at).is_some() {
                    ping.cancel_probe = true;
                }
            }
            let finalized = self.probe_cluster_finalize(at);
            ping.probe_finalized = finalized;
            if let Some((signal, channel_capacity)) = finalized
                && signal != ProbeSignal::Congesting
            {
                if channel_capacity > inputs.committed_bps {
                    ping.commit_bps = Some(channel_capacity);
                }
                ping.reallocate = true;
            }
        }
        if inputs.deficient {
            self.maybe_probe(inputs.override_active, inputs.committed_bps, &mut ping, at);
        }
        ping.actions = self.process_prober(at);
        ping
    }

    /// `maybeProbe`: no probing under a channel override, none while the
    /// estimator cannot probe, then the configured mode.
    fn maybe_probe(
        &mut self,
        override_active: bool,
        committed_bps: i64,
        ping: &mut ProbePing,
        at: Instant,
    ) {
        if override_active || !self.can_probe(at) {
            return;
        }
        if self.allocator.probe_with_media {
            // `maybeProbeWithMedia` boosts the farthest deficient track with an
            // infinite capacity and resets the estimator; the allocation pass
            // owns the track side, so it is asked to run again.
            self.detector.reset(at);
            ping.media_boost = true;
            return;
        }
        let delta = self.next_higher_delta_bps;
        if delta < 0 {
            return;
        }
        let desired_increase_bps =
            ((delta * self.allocator.probe_overage_pct) / 100).max(self.allocator.probe_min_bps);
        self.prober.add_cluster(
            ProbeClusterMode::Uniform,
            ProbeClusterGoal {
                available_bandwidth_bps: committed_bps,
                expected_usage_bps: self.expected_usage_bps,
                desired_bps: self.expected_usage_bps + desired_increase_bps,
                duration: self.probe_duration(),
                desired_bytes: 0,
            },
        );
    }

    /// One iteration of upstream's `Prober.run`: the front cluster is started
    /// (which is the switch signal this module resolves itself) and its bucket
    /// clock is advanced. Returns what the pacer must do.
    pub(crate) fn process_prober(&mut self, at: Instant) -> Vec<ProbeAction> {
        let (events, sleep) = self.prober.process(at);
        self.prober_sleep = sleep;
        events
            .into_iter()
            .map(|event| match event {
                ProberEvent::ClusterSwitch(pci) => {
                    self.handle_cluster_switch(pci, at);
                    ProbeAction::StartCluster {
                        desired_bps: pci.goal.desired_bps,
                        duration: pci
                            .goal
                            .duration
                            .nonnegative_to_std()
                            .unwrap_or(Duration::ZERO),
                    }
                }
                ProberEvent::SendProbe(bytes) => ProbeAction::SendProbe(bytes),
            })
            .collect()
    }

    /// When the prober wants to run again, which is upstream's ticker
    /// re-arm. `None` means the queue is empty.
    pub(crate) fn prober_deadline(&self, now: Instant) -> Option<Instant> {
        self.prober.next_deadline(now, self.prober_sleep)
    }

    /// Detector diagnostics for the owner's trace log.
    #[cfg_attr(not(test), allow(dead_code))] // still no shipped caller after the C74-e2 probe wiring: test-only observation point
    pub(crate) fn detector_stats(&self) -> (usize, u64, u64, Option<ProbeClusterId>) {
        (
            self.detector.packet_group_count(),
            self.detector.dropped().0,
            self.detector.dropped().1,
            self.detector.probe_packet_group_id(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pion_gcc::TwccReport;
    use rtc_core::config::CongestionControl;

    fn owner(now: Instant) -> SendSideBwe {
        SendSideBwe::new(&CongestionControl::default(), now)
    }

    fn goal(
        desired_bps: i64,
        desired_bytes: i64,
        duration: PolicyDuration,
    ) -> ccutils::ProbeClusterGoal {
        ccutils::ProbeClusterGoal {
            available_bandwidth_bps: 100_000_000,
            expected_usage_bps: 1_000_000,
            desired_bps,
            duration,
            desired_bytes,
        }
    }

    fn sent(at: Instant, ssrc: u32, sequence: u16, packet_bytes: usize) -> SendSideFeedback {
        SendSideFeedback::Sent(SendSidePacket {
            at,
            sequence,
            ssrc,
            header_bytes: 20,
            payload_bytes: packet_bytes.saturating_sub(20),
            packet_bytes,
            is_padding: false,
            is_rtx: false,
            twcc_negotiated: true,
            transport_sequence: Some(sequence),
        })
    }

    /// The pinned wire encoding of a `*rtcp.TransportLayerCC` that reports
    /// `received` packets as one run chunk of symbol 1, each with a small
    /// delta. Chunk bytes and delta bytes are adjacent because Pion keeps
    /// reading at the chunk cursor.
    fn twcc_datagram(media_ssrc: u32, base: u16, received: u16) -> Vec<u8> {
        let mut raw = Vec::new();
        raw.extend_from_slice(&[0x8f, 205, 0, 0]); // version/PT/FMT, length patched below
        raw.extend_from_slice(&7u32.to_be_bytes()); // feedback sender SSRC
        raw.extend_from_slice(&media_ssrc.to_be_bytes());
        raw.extend_from_slice(&base.to_be_bytes());
        raw.extend_from_slice(&received.to_be_bytes()); // packet status count
        raw.extend_from_slice(&[0, 0, 1]); // reference time
        raw.push(1); // feedback packet count
        raw.extend_from_slice(&(0x2000 | received).to_be_bytes());
        for index in 0..received {
            raw.push(index as u8 + 1); // one 250 microsecond unit each
        }
        while raw.len() % 4 != 0 {
            raw.push(0);
        }
        let words = (raw.len() / 4 - 1) as u16;
        raw[2..4].copy_from_slice(&words.to_be_bytes());
        raw
    }

    #[test]
    fn all_four_pacer_values_are_recognised_and_only_leaky_bucket_falls_back() {
        assert_eq!(
            PacerDecision::from_config(SendSidePacer::Unspecified),
            PacerDecision::Unspecified
        );
        assert_eq!(
            PacerDecision::from_config(SendSidePacer::PassThrough),
            PacerDecision::PassThrough
        );
        assert_eq!(
            PacerDecision::from_config(SendSidePacer::NoQueue),
            PacerDecision::NoQueue
        );
        let fallback = PacerDecision::from_config(SendSidePacer::LeakyBucket);
        assert_eq!(fallback, PacerDecision::LeakyBucketFallback);
        assert!(fallback.is_fallback());
        assert!(!PacerDecision::NoQueue.is_fallback());
        let now = Instant::now();
        let config = CongestionControl {
            send_side_bwe_pacer: SendSidePacer::LeakyBucket,
            ..Default::default()
        };
        assert_eq!(SendSideBwe::new(&config, now).pacer(), fallback);
        assert_eq!(owner(now).pacer(), PacerDecision::NoQueue);
    }

    #[test]
    fn only_transport_wide_negotiated_sends_reach_the_estimator_but_every_send_is_metered() {
        let now = Instant::now();
        let mut bwe = owner(now);
        let pci = bwe.start_probe_cluster(
            goal(2_000_000, 5_000, PolicyDuration::from_millis(100)),
            now,
        );
        assert_eq!(pci.id, 1, "the invalid sentinel is never handed out");
        // A media without the extension: observer bytes yes, tracker no.
        bwe.handle(SendSideFeedback::Sent(SendSidePacket {
            at: now,
            sequence: 7,
            ssrc: 500,
            header_bytes: 12,
            payload_bytes: 800,
            packet_bytes: 812,
            is_padding: true,
            is_rtx: false,
            twcc_negotiated: false,
            transport_sequence: None,
        }))
        .unwrap();
        let stats = bwe.stats();
        assert_eq!(
            (
                stats.sent_packets,
                stats.untracked_packets,
                stats.tracked_packets
            ),
            (1, 1, 0)
        );
        assert_eq!(
            bwe.observer.pci.result.bytes_probe, 812,
            "packet_bytes is what is metered"
        );
        assert_eq!(bwe.observer.pci.result.packets_probe, 1);
        assert_eq!(
            bwe.time_since_last_sent(now),
            None,
            "nothing stamped has left yet"
        );

        // A negotiated send is metered into the tracker as well.
        bwe.handle(sent(now + Duration::from_millis(1), 600, 9, 1_200))
            .unwrap();
        let stats = bwe.stats();
        assert_eq!((stats.tracked_packets, stats.untracked_packets), (1, 1));
        assert_eq!(
            bwe.time_since_last_sent(now + Duration::from_millis(5)),
            Some(Duration::from_millis(4))
        );
        assert_eq!(bwe.observer.pci.result.bytes_non_probe_primary, 1_200);
    }

    #[test]
    fn a_negotiated_send_without_an_extension_value_is_rejected_before_the_tracker() {
        let now = Instant::now();
        let mut bwe = owner(now);
        let error = bwe
            .handle(SendSideFeedback::Sent(SendSidePacket {
                at: now,
                sequence: 3,
                ssrc: 1,
                header_bytes: 20,
                payload_bytes: 100,
                packet_bytes: 120,
                is_padding: false,
                is_rtx: false,
                twcc_negotiated: true,
                transport_sequence: None,
            }))
            .unwrap_err();
        assert_eq!(error, InputError::MissingTwcc);
        // The observer already metered it, as upstream's ordering does.
        assert_eq!(bwe.stats().sent_packets, 1);
        assert_eq!(bwe.stats().tracked_packets, 0);
    }

    #[test]
    fn the_observer_completes_only_at_both_goal_bounds_and_stops_counting_afterwards() {
        let now = Instant::now();
        let mut bwe = owner(now);
        bwe.start_probe_cluster(
            goal(2_000_000, 1_000, PolicyDuration::from_millis(200)),
            now,
        );
        // Enough bytes, not enough time.
        let outcome = bwe.handle(sent(now, 1, 10, 1_500)).unwrap();
        assert!(outcome.completed.is_none());
        // Enough time, but the byte total was already reached, so this send
        // completes the cluster.
        let outcome = bwe
            .handle(sent(now + Duration::from_millis(250), 1, 11, 10))
            .unwrap();
        let completed = outcome.completed.expect("goal bounds both met");
        assert_eq!(completed.id, 1);
        assert!(completed.result.is_completed);
        assert_eq!(completed.result.bytes(), 1_510);
        assert_eq!(
            completed.result.duration(),
            PolicyDuration::from_nanos(250_000_000)
        );
        // A send after completion is a different cluster, so it is not counted.
        bwe.handle(sent(now + Duration::from_millis(300), 1, 12, 9_000))
            .unwrap();
        assert_eq!(bwe.observer.pci.result.bytes(), 1_510);
        assert_eq!(bwe.stats().auto_completed_clusters, 1);
    }

    #[test]
    fn a_second_start_is_ignored_and_an_end_of_another_id_is_ignored() {
        let now = Instant::now();
        let mut bwe = owner(now);
        bwe.start_probe_cluster(
            goal(2_000_000, 1_000, PolicyDuration::from_millis(100)),
            now,
        );
        let second = bwe.observer.start(
            ProbeClusterInfo {
                id: 99,
                ..ProbeClusterInfo::default()
            },
            now,
            now,
        );
        assert!(!second);
        assert_eq!(bwe.observer.pci.id, 1);
        assert_eq!(bwe.observer.end(42, now, now), None, "non-active id");
        assert_eq!(bwe.stats().ignored_cluster_starts, 1);
        assert_eq!(bwe.stats().mismatched_cluster_ends, 1);
        // The active id still ends, and only once keeps its end stamp.
        let first = bwe.observer.end(1, now, now).expect("active id ends");
        let again = bwe.observer.end(1, now + Duration::from_secs(1), now);
        assert_eq!(first.result.end_time_ns, 0);
        assert_eq!(again, None, "not active any more");
        assert_eq!(
            bwe.observer.pci.result.end_time_ns,
            first.result.end_time_ns
        );
    }

    #[test]
    fn stopping_an_idle_cluster_never_touches_the_detector_probe_group() {
        let now = Instant::now();
        let mut bwe = owner(now);
        let pci = bwe.start_probe_cluster(
            goal(2_000_000, 1_000, PolicyDuration::from_millis(100)),
            now,
        );
        assert!(bwe.probe_is_running());
        // maybeStopProbe returns before the BWE when no cluster is active.
        bwe.active_cluster_id = PROBE_CLUSTER_ID_INVALID;
        assert!(
            bwe.stop_probe_cluster(now + Duration::from_millis(1))
                .is_none()
        );
        assert!(
            bwe.probe_is_running(),
            "the detector still holds its cluster"
        );
        assert!(
            !bwe.probe_cluster_is_goal_reached(),
            "no result has been recorded"
        );
        // The completion path does reach it, with the observer's recorded result.
        let ended = bwe.end_probe_cluster(pci.id, now + Duration::from_millis(2));
        assert_eq!(ended.id, pci.id);
        assert_eq!(
            bwe.stamped_cluster_id, PROBE_CLUSTER_ID_INVALID,
            "the end clears the pacer/track stamp"
        );
        assert_eq!(
            bwe.probe_cluster_id(),
            PROBE_CLUSTER_ID_INVALID,
            "the guarded stop never ran, so the allocator id it would have used stays invalid"
        );
        assert_eq!(ended.result.bytes(), 0, "nothing was sent for this cluster");
    }

    /// The charge has to change what the production clock asks for, because a
    /// count that nothing reads back would not distinguish padding that left
    /// the pacer from padding that was only demanded.
    #[test]
    fn a_stamped_padding_charge_lowers_the_next_probe_demand() {
        let now = Instant::now();
        let cluster_goal = goal(2_000_000, 0, PolicyDuration::from_millis(100));
        let emitted_padding = |at: Instant, sequence: u16| {
            SendSideFeedback::Sent(SendSidePacket {
                at,
                sequence,
                ssrc: 7,
                header_bytes: 20,
                payload_bytes: 1_480,
                packet_bytes: 1_500,
                is_padding: true,
                is_rtx: false,
                twcc_negotiated: true,
                transport_sequence: Some(sequence),
            })
        };
        let demand = |actions: &[ProbeAction]| -> i64 {
            actions
                .iter()
                .map(|action| match action {
                    ProbeAction::SendProbe(bytes) => *bytes,
                    ProbeAction::StartCluster { .. } => 0,
                })
                .sum()
        };

        // The negative side of the gate first: with no cluster ever switched to
        // there is nothing to bill, so the padding is counted as traffic and not
        // as evidence.
        let mut idle = owner(now);
        for i in 0..10u64 {
            idle.handle(emitted_padding(
                now + Duration::from_millis(5 * i),
                (10 + i) as u16,
            ))
            .unwrap();
        }
        assert_eq!(idle.stats().padding_bytes, 15_000);
        assert_eq!(idle.stats().uncharged_padding_bytes, 15_000);
        assert_eq!(
            idle.stats().probe_bucket_bytes,
            0,
            "unsaved padding cannot advance a cluster that was never started"
        );

        // Two owners that differ only in whether the padding that left the
        // window was billed. Both cross the same bucket boundary, so the demand
        // is `expected - probe_bytes_sent` and the only variable is the charge.
        let mut baseline = owner(now);
        baseline
            .prober
            .add_cluster(ProbeClusterMode::Uniform, cluster_goal);
        let started = baseline.process_prober(now);
        assert!(
            matches!(started.first(), Some(ProbeAction::StartCluster { .. })),
            "the cluster has to be promoted by the prober clock, not by the test"
        );
        let asked = demand(&baseline.process_prober(now + Duration::from_millis(60)));

        let mut charged = owner(now);
        charged
            .prober
            .add_cluster(ProbeClusterMode::Uniform, cluster_goal);
        charged.process_prober(now);
        assert_ne!(
            charged.stamped_cluster_id, PROBE_CLUSTER_ID_INVALID,
            "the switch installs the stamp that makes padding billable"
        );
        for i in 0..100u64 {
            charged
                .handle(emitted_padding(
                    now + Duration::from_millis(5) * (i as u32 + 1),
                    (10 + i) as u16,
                ))
                .unwrap();
        }
        let charged_asked = demand(&charged.process_prober(now + Duration::from_millis(60)));

        assert_eq!(baseline.stats().probe_bucket_bytes, 0);
        assert_eq!(charged.stats().probe_bucket_bytes, 100 * 1_500);
        assert!(
            charged_asked < asked,
            "the same volume of padding that reached the bucket did not reduce the demand: \
             uncharged asked for {asked}, charged asked for {charged_asked}"
        );
    }

    #[test]
    fn finalize_settles_the_cluster_and_returns_the_detector_estimate_once() {
        let now = Instant::now();
        let mut bwe = owner(now);
        let pci = bwe.start_probe_cluster(
            goal(2_000_000, 1_000, PolicyDuration::from_millis(100)),
            now,
        );
        bwe.handle(sent(now, 7, 20, 1_500)).unwrap();
        let completed = bwe
            .handle(sent(now + Duration::from_millis(150), 7, 21, 10))
            .unwrap()
            .completed
            .expect("both goal bounds met");
        assert_eq!(completed.id, pci.id);
        bwe.end_probe_cluster(completed.id, now + Duration::from_millis(150));
        // `handleSignalPacerProbeObserverClusterComplete` ends the pacer
        // cluster and hands the result to the detector, but it never clears the
        // allocator's `activeProbeClusterId`: only finalization (or the paused
        // reset) does, because the settle wait still has to run.
        assert_eq!(
            bwe.probe_cluster_id(),
            pci.id,
            "the allocator keeps the id until the probe finalizes"
        );
        // The 70 ms default RTT gives a 350 ms settle wait, so 160 ms is too early.
        assert!(
            bwe.probe_cluster_finalize(now + Duration::from_millis(160))
                .is_none()
        );
        let capacity = bwe.capacity();
        let (signal, estimated) = bwe
            .probe_cluster_finalize(now + Duration::from_secs(2))
            .expect("settle wait elapsed");
        assert_eq!(
            bwe.probe_cluster_id(),
            PROBE_CLUSTER_ID_INVALID,
            "finalization is what clears the allocator id"
        );
        // No TWCC feedback reached the probe group, so the probe cannot claim
        // congestion and an empty group cannot raise the estimate.
        assert_eq!(signal, ProbeSignal::NotCongesting);
        assert_eq!(estimated, capacity);
        assert_eq!(bwe.capacity(), capacity);
        assert!(!bwe.probe_is_running(), "the finalized cluster is dropped");
        assert!(
            bwe.probe_cluster_finalize(now + Duration::from_secs(3))
                .is_none()
        );
        // A non-congesting signal is the branch that lengthens the next goal.
        assert_eq!(
            bwe.probe_duration(),
            PolicyDuration::from_millis(300),
            "200 ms scaled by the configured 1.5 duration factor"
        );
    }

    #[test]
    fn a_report_for_an_ssrc_this_transport_never_sent_is_not_delivered() {
        let now = Instant::now();
        let mut bwe = owner(now);
        // A delivered report always opens a group, so the group count is the
        // observable difference between dispatch and drop.
        let unknown = twcc_datagram(999, 1, 2);
        bwe.handle(SendSideFeedback::Report {
            at: now,
            packet: unknown.clone(),
        })
        .unwrap();
        let stats = bwe.stats();
        assert_eq!((stats.report_datagrams, stats.twcc_reports), (1, 1));
        assert_eq!(stats.twcc_unknown_ssrc, 1);
        assert_eq!(bwe.detector_stats().0, 0, "the detector saw nothing");
        // The same report for an SSRC this transport emitted is delivered.
        bwe.handle(sent(now, 999, 1, 1_200)).unwrap();
        bwe.handle(SendSideFeedback::Report {
            at: now,
            packet: unknown,
        })
        .unwrap();
        let stats = bwe.stats();
        assert_eq!(stats.twcc_unknown_ssrc, 1, "the second one was delivered");
        assert_eq!(stats.twcc_reports, 2);
        assert_eq!(bwe.detector_stats().0, 1, "the report opened a group");
    }

    #[test]
    fn a_malformed_later_packet_delivers_nothing_and_moves_no_estimator_state() {
        let now = Instant::now();
        let mut bwe = owner(now);
        bwe.handle(sent(now, 4, 1, 1_200)).unwrap();
        let good = twcc_datagram(4, 1, 2);
        let mut compound = good.clone();
        // A second packet that claims more bytes than the datagram holds.
        compound.extend_from_slice(&[0x8f, 205, 0, 30, 0, 0, 0, 4]);
        let error = bwe
            .handle(SendSideFeedback::Report {
                at: now,
                packet: compound,
            })
            .unwrap_err();
        assert!(matches!(error, InputError::Wire(_)), "{error}");
        let stats = bwe.stats();
        assert_eq!(
            stats.report_datagrams, 0,
            "the datagram never entered delivery"
        );
        assert_eq!(stats.twcc_reports, 0);
        assert_eq!(bwe.detector_stats().0, 0, "no group was opened or pruned");
        // The admitted datagram does deliver.
        bwe.handle(SendSideFeedback::Report {
            at: now,
            packet: good,
        })
        .unwrap();
        assert_eq!(bwe.stats().twcc_reports, 1);
        assert_eq!(bwe.detector_stats().0, 1);
    }

    #[test]
    fn discontinuity_retires_the_owner_instead_of_continuing_on_a_truncated_history() {
        let now = Instant::now();
        let mut bwe = owner(now);
        bwe.handle(sent(now, 1, 1, 1_200)).unwrap();
        assert!(matches!(
            bwe.handle(SendSideFeedback::Discontinuity),
            Err(InputError::Retired)
        ));
        assert!(bwe.is_retired());
        let groups = bwe.detector_stats().0;
        assert_eq!(
            bwe.handle(sent(now + Duration::from_millis(1), 1, 2, 1_200))
                .unwrap_err(),
            InputError::Retired
        );
        assert_eq!(bwe.stats().sent_packets, 1, "nothing more is metered");
        assert_eq!(bwe.detector_stats().0, groups);
    }

    #[test]
    fn chunk_expansion_keeps_the_pion_delta_stream_and_the_wire_order() {
        // 0x4001-ish vector chunk: two symbols, then a run of three received.
        let report = TwccReport {
            base: 100,
            status_count: 5,
            reference: 0x00_12_34_00,
            fb_pkt_count: 7,
            media_ssrc: 42,
            chunks: vec![
                Chunk::Vector(vec![1, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
                Chunk::Run {
                    symbol: 1,
                    length: 3,
                },
            ],
            deltas_us: vec![250, 500, 750, 1_000],
        };
        let expanded = twcc_report(&report);
        assert_eq!(
            expanded.symbols,
            vec![1, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1]
        );
        assert_eq!(expanded.base_sequence_number, 100);
        assert_eq!(expanded.packet_status_count, 5);
        assert_eq!(expanded.reference_time, 0x00_12_34_00);
        assert_eq!(expanded.fb_pkt_count, 7);
        assert_eq!(expanded.deltas_us, vec![250, 500, 750, 1_000]);
    }

    #[test]
    fn the_probe_gate_and_the_reset_boundary_stay_visible_through_the_facade() {
        let now = Instant::now();
        let mut bwe = owner(now);
        // `time.After` is strict and the regulator's earliest instant is
        // creation, so the very first poll cannot probe.
        assert!(!bwe.can_probe(now));
        let later = now
            + PolicyDuration::from_millis(1)
                .nonnegative_to_std()
                .expect("1 ms is representable");
        assert!(bwe.can_probe(later), "idle, clear and past the interval");
        assert_eq!(
            bwe.probe_duration(),
            PolicyDuration::from_millis(200),
            "the goal duration starts at the configured minimum"
        );
        bwe.start_probe_cluster(
            goal(2_000_000, 1_000, PolicyDuration::from_millis(100)),
            later,
        );
        assert!(
            !bwe.can_probe(later),
            "a running cluster blocks another probe"
        );
        let capacity = bwe.capacity();
        bwe.reset(later);
        assert_eq!(
            bwe.capacity(),
            capacity,
            "reset restores the initial capacity, not a cached estimate"
        );
        assert_eq!(capacity, detector::INITIAL_CHANNEL_CAPACITY_BPS);
        assert!(!bwe.probe_is_running(), "reset drops the probe group");
        assert_eq!(bwe.congestion_state(), CongestionState::None);
        // A zero RTT is upstream's "no measurement yet", and because the
        // detector is created at the default, a real measurement smooths
        // against it instead of replacing it.
        bwe.update_rtt(0.2);
        assert!((bwe.rtt() - 0.135).abs() < 1e-12, "{}", bwe.rtt());
        bwe.update_rtt(0.0);
        assert_eq!(bwe.rtt(), detector::DEFAULT_RTT);
        bwe.update_rtt(detector::DEFAULT_RTT);
        assert_eq!(bwe.rtt(), detector::DEFAULT_RTT);
    }

    /// The vendor probe window can refuse a request (no keys, no sending media,
    /// or a validation already open). The refusal consumer retires the cluster
    /// the way `maybeStopProbe` does; this is the allocator-side half of that
    /// contract: a retired cluster has to reach a finalization exactly once, and
    /// must not leave the allocator holding an active cluster forever or get
    /// booked as somebody else's outcome.
    #[test]
    fn a_retired_cluster_finalizes_exactly_once_and_asks_for_nothing_more() {
        let now = Instant::now();
        let mut bwe = owner(now);
        bwe.note_allocation(1_000_000, Some(500_000));
        let inputs = ProbeInputs {
            allow_pause: false,
            deficient: true,
            override_active: false,
            committed_bps: 0,
        };
        // The first poll cannot probe, so the cluster is scheduled one later.
        let started_at = now + Duration::from_millis(1);
        let ping = bwe.periodic_ping(&inputs, started_at);
        assert!(
            ping.actions
                .iter()
                .any(|action| matches!(action, ProbeAction::StartCluster { .. })),
            "a deficient allocator schedules a cluster, got {:?}",
            ping.actions
        );
        assert_eq!(
            ping.probe_finalized, None,
            "a cluster that has not settled is not an outcome"
        );
        let active = bwe.active_cluster_id;
        assert_ne!(active, PROBE_CLUSTER_ID_INVALID);

        bwe.stop_probe_cluster(started_at)
            .expect("the retired cluster was active");
        assert_eq!(
            bwe.stamped_cluster_id, PROBE_CLUSTER_ID_INVALID,
            "no further datagram may be stamped into a retired cluster"
        );

        // The settle wait has to pass first: 10 ms after a 70 ms default RTT is
        // still inside it, so nothing is bookable yet.
        let early = bwe.periodic_ping(&inputs, started_at + Duration::from_millis(10));
        assert_eq!(early.probe_finalized, None, "still settling");

        let settled = bwe.periodic_ping(&inputs, started_at + Duration::from_secs(1));
        let (signal, capacity) = settled
            .probe_finalized
            .expect("a retired cluster reaches a finalization instead of staying active");
        // Upstream computes `isSignalValid` in `ProbeClusterFinalize` only to put
        // it in the log; the booked signal always comes from
        // `ProbeSignalConfig.ProbeSignal(group)`. A group that carried nothing has
        // zero queuing delay and zero weighted loss, so it is NotCongesting, which
        // is also why the goal-reached check is the only place `IsValid` gates the
        // decision.
        assert_eq!(
            signal,
            ProbeSignal::NotCongesting,
            "a cluster that measured nothing is not a congestion failure"
        );
        assert_eq!(capacity, bwe.capacity());
        assert_eq!(
            bwe.active_cluster_id, PROBE_CLUSTER_ID_INVALID,
            "finalization is what clears the allocator id"
        );
        let after = bwe.periodic_ping(&inputs, started_at + Duration::from_secs(2));
        assert_eq!(
            after.probe_finalized, None,
            "the same cluster is never booked twice"
        );
    }

    /// `probe_cluster_finalize`'s capacity is what Go compares against
    /// `committedChannelCapacity` before the allocator adopts it, so the same
    /// finalization has to commit on an empty channel and refuse to lower a
    /// committed one. This is the negative control for the commit-if-higher gate
    /// the allocation pass reads.
    #[test]
    fn a_finalization_commits_only_when_it_raises_the_committed_capacity() {
        let now = Instant::now();
        let goal_duration = PolicyDuration::from_millis(100);
        let mut lower = owner(now);
        let lower_cluster = lower.start_probe_cluster(goal(2_000_000, 1_000, goal_duration), now);
        lower.handle(sent(now, 7, 20, 1_500)).unwrap();
        lower
            .handle(sent(now + Duration::from_millis(150), 7, 21, 10))
            .unwrap();
        // The two packets are judged on their own traffic, not on `IsValid`: the
        // group carries no queuing delay and no loss, so this is the NotCongesting
        // branch, the one that compares the returned estimate against what the
        // caller already committed.
        lower.end_probe_cluster(lower_cluster.id, now + Duration::from_millis(150));
        let inputs = ProbeInputs {
            allow_pause: false,
            deficient: false,
            override_active: false,
            committed_bps: 0,
        };
        let ping = lower.periodic_ping(&inputs, now + Duration::from_secs(1));
        let (signal, capacity) = ping
            .probe_finalized
            .expect("the settle wait elapsed with a recorded result");
        assert_eq!(signal, ProbeSignal::NotCongesting);
        assert_eq!(ping.commit_bps, Some(capacity));
        assert!(ping.reallocate);

        // The same finalization against a channel already committed above the
        // estimate must not lower it.
        let mut higher = owner(now);
        let higher_cluster = higher.start_probe_cluster(goal(2_000_000, 1_000, goal_duration), now);
        higher.handle(sent(now, 7, 20, 1_500)).unwrap();
        higher
            .handle(sent(now + Duration::from_millis(150), 7, 21, 10))
            .unwrap();
        higher.end_probe_cluster(higher_cluster.id, now + Duration::from_millis(150));
        let committed = ProbeInputs {
            committed_bps: capacity + 1,
            ..inputs
        };
        let held = higher.periodic_ping(&committed, now + Duration::from_secs(1));
        assert_eq!(
            held.probe_finalized.map(|(signal, _)| signal),
            Some(ProbeSignal::NotCongesting),
            "the cluster still finalizes"
        );
        assert_eq!(
            held.commit_bps, None,
            "a lower estimate never lowers a committed capacity"
        );
        assert!(held.reallocate);
    }
}
