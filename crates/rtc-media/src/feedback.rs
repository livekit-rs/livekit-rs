//! Runtime owner for the fixed RemoteBWE policy. Native validation remains separate.
//! Rates are payload allocation bits/s; execution counters are actual RTP bytes
//! (header+payload, excluding SRTP), as fixed pacer/base.go's ProbeObserver.
use crate::remote_bwe::{CongestionState, ProbeGoal, ProbeResult, ProbeSignal, RemoteController};
use rtc_core::config::{CongestionControl, FeedbackMode, StreamAllocatorConfig};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use str0m::media::Mid;
use str0m::{PaddingStatus, RemoteFeedbackStats, Rtc};

const POLL: Duration = Duration::from_millis(10);
#[path = "feedback/uniform.rs"]
mod uniform;
use uniform::Uniform;
#[path = "feedback/interceptor.rs"]
mod interceptor;
#[derive(Default, Clone, Copy)]
struct Counts {
    primary_packets: u64,
    primary_bytes: u64,
    rtx_packets: u64,
    rtx_bytes: u64,
    padding_packets: u64,
    padding_bytes: u64,
}
impl Counts {
    fn plus(&mut self, s: RemoteFeedbackStats) {
        self.primary_packets += u64::from(s.packets_primary);
        self.primary_bytes += s.bytes_primary;
        self.rtx_packets += s.packets_rtx;
        self.rtx_bytes += s.bytes_rtx;
        self.padding_packets += s.packets_padding;
        self.padding_bytes += s.bytes_padding;
    }
}
struct Active {
    id: u32,
    goal: ProbeGoal,
    started: Instant,
    base: Counts,
    done: bool,
    result: ProbeResult,
    padding_id: Option<u64>,
    schedule: Uniform,
}
/// Source allocator inputs for one currently negotiated managed video track.
#[derive(Clone, Copy)]
pub(crate) struct MediaDemand {
    pub mid: Mid,
    pub desired: i32,
    pub priority: u8,
    pub exempt: bool,
    pub rates: [u64; 3],
}
/// Distinct policy/feedback owner. Never routes default REMB through native validation.
pub(crate) struct Feedback {
    mode: FeedbackMode,
    interceptor_last_estimate: u64,
    interceptor: Option<interceptor::Provider>,
    policy: RemoteController,
    allocator: StreamAllocatorConfig,
    committed: i64,
    active: Option<Active>,
    next_id: u32,
    expected: i64,
    video_mids: Vec<Mid>,
    remb_ssrc: Option<u32>,
    media_targets: HashMap<Mid, i32>,
    previous: HashMap<(Mid, u32, u64), (u32, u32)>,
    output_previous: HashMap<(Mid, u32, u64), RemoteFeedbackStats>,
    /// Per-stream padding counters for probe accounting (all local streams).
    padding_previous: HashMap<(Mid, u32, u64), (u64, u64)>,
    output_totals: Counts,
    next_poll: Option<Instant>,
    origin: Instant,
    last_rtt: Instant,
}
impl Feedback {
    pub fn new(config: &CongestionControl, now: Instant) -> Self {
        Self {
            mode: config.effective_feedback_mode(),
            interceptor_last_estimate: 1_000_000,
            interceptor: (config.effective_feedback_mode() == FeedbackMode::InterceptorEstimate)
                .then(|| interceptor::Provider::new(now)),
            policy: RemoteController::new(config.remote_bwe.clone(), now),
            allocator: config.stream_allocator.clone(),
            committed: 100_000_000,
            active: None,
            next_id: 0,
            expected: 0,
            video_mids: vec![],
            remb_ssrc: None,
            media_targets: HashMap::new(),
            previous: HashMap::new(),
            output_previous: HashMap::new(),
            padding_previous: HashMap::new(),
            output_totals: Counts::default(),
            next_poll: None,
            origin: now,
            last_rtt: now,
        }
    }
    pub fn mode(&self) -> FeedbackMode {
        self.mode
    }
    /// Fixed Pion OnTargetBitrateChange reports changes, not initial state or
    /// periodic snapshots. Its NoOpPacer never drives normal-media pacing.
    pub fn interceptor_estimate_changed(&mut self, estimate: u64) -> bool {
        if self.mode != FeedbackMode::InterceptorEstimate
            || estimate == self.interceptor_last_estimate
        {
            return false;
        }
        self.interceptor_last_estimate = estimate;
        true
    }
    pub fn interceptor_target(&self) -> Option<u64> {
        self.interceptor.as_ref().map(|p| p.target())
    }
    pub fn interceptor_stats(&self) -> Option<interceptor::InputStats> {
        self.interceptor.as_ref().map(|p| p.stats())
    }
    pub fn interceptor_input(
        &mut self,
        event: str0m::SendSideFeedback,
    ) -> Result<(Instant, Vec<u64>), interceptor::InputError> {
        self.interceptor
            .as_mut()
            .expect("interceptor mode owns provider")
            .handle(event)
    }
    pub fn close_interceptor(&mut self) {
        if let Some(provider) = &mut self.interceptor {
            provider.close();
        }
    }
    pub fn capacity(&self) -> u64 {
        self.committed
            .max(self.allocator.min_channel_capacity)
            .max(0) as u64
    }
    pub fn deadline(&self) -> Option<Instant> {
        self.next_poll
    }
    fn counts(&mut self, stats: &[RemoteFeedbackStats]) -> Counts {
        self.output_previous.retain(|(mid, ssrc, generation), _| {
            stats.iter().any(|s| {
                s.mid == *mid
                    && s.ssrc == *ssrc
                    && s.generation == *generation
                    && self.video_mids.contains(mid)
            })
        });
        for s in stats.iter().filter(|s| self.video_mids.contains(&s.mid)) {
            let old = self
                .output_previous
                .insert((s.mid, s.ssrc, s.generation), *s);
            if let Some(old) = old {
                self.output_totals.primary_packets +=
                    u64::from(s.packets_primary.wrapping_sub(old.packets_primary));
                self.output_totals.primary_bytes +=
                    s.bytes_primary.saturating_sub(old.bytes_primary);
                self.output_totals.rtx_packets += s.packets_rtx.saturating_sub(old.packets_rtx);
                self.output_totals.rtx_bytes += s.bytes_rtx.saturating_sub(old.bytes_rtx);
            } else {
                let padding = (
                    self.output_totals.padding_packets,
                    self.output_totals.padding_bytes,
                );
                self.output_totals.plus(*s);
                (
                    self.output_totals.padding_packets,
                    self.output_totals.padding_bytes,
                ) = padding;
            }
        }
        // Probe padding is executed on whichever local stream the executor picks,
        // which need not be a tracked video m-line: count every stream's padding,
        // as fixed Go's probe cluster counts all bytes the pacer sent for it.
        // Counting only video m-lines left the probe "unsent" and re-requested.
        self.padding_previous
            .retain(|key, _| stats.iter().any(|s| (s.mid, s.ssrc, s.generation) == *key));
        for s in stats {
            let key = (s.mid, s.ssrc, s.generation);
            let old = self
                .padding_previous
                .insert(key, (s.packets_padding, s.bytes_padding));
            let (packets, bytes) = old.unwrap_or((0, 0));
            self.output_totals.padding_packets += s.packets_padding.saturating_sub(packets);
            self.output_totals.padding_bytes += s.bytes_padding.saturating_sub(bytes);
        }
        self.output_totals
    }
    pub fn update_tracks(&mut self, mids: Vec<Mid>, expected: u64) {
        self.video_mids = mids;
        self.expected = expected.min(i64::MAX as u64) as i64;
    }
    pub fn accept_remb(&mut self, ssrcs: &[u32], stats: &[RemoteFeedbackStats]) -> bool {
        if self.video_mids.is_empty() || ssrcs.is_empty() {
            return false;
        }
        let local = |ssrc: u32| {
            ssrc != 0
                && stats.iter().any(|s| {
                    self.video_mids.contains(&s.mid) && (s.ssrc == ssrc || s.rtx_ssrc == Some(ssrc))
                })
        };
        let selected = self
            .remb_ssrc
            .filter(|ssrc| ssrcs.contains(ssrc) && local(*ssrc))
            .or_else(|| ssrcs.iter().copied().find(|ssrc| local(*ssrc)));
        self.remb_ssrc = selected;
        selected.is_some()
    }
    pub fn maybe_reset_paused(&mut self, rtc: &mut Rtc, paused: bool, now: Instant) {
        if !paused || self.policy.state() != CongestionState::Congested {
            return;
        }
        let stats = rtc.direct_api().remote_feedback_stats();
        let last = stats
            .iter()
            .filter(|s| self.video_mids.contains(&s.mid))
            .filter_map(|s| s.last_output_at)
            .max()
            .unwrap_or(self.origin);
        if now.duration_since(last).as_nanos() as i128
            > i128::from(self.allocator.paused_min_wait.as_nanos_i64())
        {
            self.finish(rtc, &stats, false, now);
            self.active = None;
            self.next_poll = None;
            self.policy.reset(now); // committed allocator capacity deliberately unchanged.
        }
    }
    pub fn rtt(&mut self, rtt: Option<Duration>, now: Instant) {
        // Official stream allocator polls ICE RTT every30s, not cached TWCC RTT.
        if now.duration_since(self.last_rtt) > Duration::from_secs(30) {
            self.last_rtt = now;
            if let Some(rtt) = rtt {
                self.policy.update_rtt(rtt.as_secs_f64());
            }
        }
    }
    pub fn estimate(
        &mut self,
        estimate: u64,
        stats: &[RemoteFeedbackStats],
        rtc: &mut Rtc,
        now: Instant,
    ) -> bool {
        self.previous.retain(|(mid, ssrc, generation), _| {
            stats.iter().any(|s| {
                s.mid == *mid
                    && s.ssrc == *ssrc
                    && s.generation == *generation
                    && self.video_mids.contains(mid)
            })
        });
        let mut packets = 0u32;
        let mut repeated = 0u32;
        for s in stats.iter().filter(|s| self.video_mids.contains(&s.mid)) {
            let before = self
                .previous
                .insert(
                    (s.mid, s.ssrc, s.generation),
                    (s.packets_primary, s.repeated_nacks),
                )
                .unwrap_or((0, 0));
            packets = packets.wrapping_add(s.packets_primary.wrapping_sub(before.0));
            repeated = repeated.wrapping_add(s.repeated_nacks.wrapping_sub(before.1));
        }
        let change = self.policy.on_remb(
            estimate.min(i64::MAX as u64) as i64,
            self.expected,
            packets,
            repeated,
            now,
        );
        tracing::debug!(mode=?self.mode,receiver_estimate_bps=estimate,expected_usage_bps=self.expected,
            packets_delta=packets,repeated_packet_nacks_delta=repeated,policy_state=?self.policy.state(),
            committed_capacity_bps=self.committed,observation=%self.policy.observation(now),
            streams=?stats.iter().map(|s| (s.mid.to_string(), s.ssrc, s.rtx_ssrc, s.generation, s.packets_rtx, s.packets_padding)).collect::<Vec<_>>(),
            "remote feedback sample");
        if let Some(change) = change
            && change.to == CongestionState::Congested
        {
            self.media_targets.clear();
            if self.active.is_some() {
                self.finish(rtc, stats, false, now);
            } else {
                self.committed = change.capacity;
            }
        }
        if self.policy.goal_reached() {
            self.finish(rtc, stats, false, now);
        }
        change.is_some()
    }
    fn result(
        active: &Active,
        counts: Counts,
        origin: Instant,
        now: Instant,
        completed: bool,
    ) -> ProbeResult {
        ProbeResult {
            start_time_ns: active
                .started
                .duration_since(origin)
                .as_nanos()
                .min(i64::MAX as u128) as i64,
            end_time_ns: now.duration_since(origin).as_nanos().min(i64::MAX as u128) as i64,
            packets_probe: counts
                .padding_packets
                .saturating_sub(active.base.padding_packets) as i64,
            bytes_probe: counts
                .padding_bytes
                .saturating_sub(active.base.padding_bytes) as i64,
            packets_non_probe_primary: counts
                .primary_packets
                .saturating_sub(active.base.primary_packets)
                as i64,
            bytes_non_probe_primary: counts
                .primary_bytes
                .saturating_sub(active.base.primary_bytes)
                as i64,
            packets_non_probe_rtx: counts.rtx_packets.saturating_sub(active.base.rtx_packets)
                as i64,
            bytes_non_probe_rtx: counts.rtx_bytes.saturating_sub(active.base.rtx_bytes) as i64,
            is_completed: completed,
        }
    }
    pub fn finish(
        &mut self,
        rtc: &mut Rtc,
        stats: &[RemoteFeedbackStats],
        completed: bool,
        now: Instant,
    ) {
        let counts = self.counts(stats);
        let Some(active) = self.active.as_mut().filter(|p| !p.done) else {
            return;
        };
        if let Some(id) = active.padding_id.take() {
            rtc.direct_api().cancel_padding(id);
        }
        active.done = true;
        active.result = Self::result(active, counts, self.origin, now, completed);
        tracing::debug!(id=active.id,goal=?active.goal,actual=?active.result,"remote probe local execution ended");
        self.policy.finish_probe(active.id, active.result, now); // exactly once, including abort.
        self.next_poll = Some(now + POLL);
    }
    /// Source `maybeProbeWithMedia`: one real next-layer transition, policy
    /// reset, no fake probe result and no write to committed channel capacity.
    pub fn media_caps(
        &mut self,
        demands: &[MediaDemand],
        caps: &[i32],
        connected: bool,
        overridden: bool,
        now: Instant,
    ) -> Vec<i32> {
        let mut result = caps.to_vec();
        if self.allocator.probe_mode != "media" {
            return result;
        }
        self.media_targets
            .retain(|mid, _| demands.iter().any(|d| d.mid == *mid));
        if overridden {
            self.media_targets.clear();
            return result;
        }
        for ((d, cap), original) in demands.iter().zip(&mut result).zip(caps) {
            if d.desired < 0 {
                self.media_targets.remove(&d.mid);
                continue;
            }
            if let Some(target) = self.media_targets.get_mut(&d.mid) {
                *target = (*target).min(d.desired);
                if *target >= 0 && d.rates[*target as usize] > 0 {
                    *cap = (*original).max(*target);
                }
            }
        }
        if !connected || self.active.is_some() || !self.policy.can_probe(now) {
            return result;
        }
        let choice = demands
            .iter()
            .zip(&result)
            .enumerate()
            .filter(|(_, (d, cap))| !d.exempt && d.desired >= 0 && **cap < d.desired)
            .max_by_key(|(_, (d, cap))| (d.desired - **cap, d.priority))
            .and_then(|(index, (d, cap))| {
                (((*cap + 1).max(0))..=d.desired.min(2))
                    .find(|layer| d.rates[*layer as usize] > 0)
                    .map(|layer| (index, layer))
            });
        if let Some((index, layer)) = choice {
            self.media_targets.insert(demands[index].mid, layer);
            result[index] = layer;
            self.policy.reset(now);
        }
        result
    }
    /// Source `expected + max(nextDelta*overage/100, probeMin)`; no grant on request.
    pub fn maybe_start(
        &mut self,
        next_total: Option<u64>,
        connected: bool,
        overridden: bool,
        rtc: &mut Rtc,
        now: Instant,
    ) -> bool {
        if self.active.is_some() || !connected || overridden || !self.policy.can_probe(now) {
            return false;
        }
        // Media mode uses the explicit allocation transition below. Unknown
        // source probe-mode strings deliberately do not schedule probes.
        if self.allocator.probe_mode != "padding" {
            return false;
        }
        let Some(total) = next_total else {
            return false;
        };
        let delta = (total.min(i64::MAX as u64) as i64 - self.expected).max(0);
        let Some(overage) = delta
            .checked_mul(self.allocator.probe_overage_pct)
            .map(|v| v / 100)
        else {
            return false;
        };
        let Some(desired) = self
            .expected
            .checked_add(overage.max(self.allocator.probe_min_bps))
        else {
            return false;
        };
        let duration = self.policy.probe_duration();
        let Some(wall) = duration.nonnegative_to_std().filter(|d| !d.is_zero()) else {
            return false;
        };
        if desired <= 0 || self.next_id == u32::MAX {
            return false;
        }
        self.next_id += 1;
        let schedule = Uniform::new(desired, self.expected, wall);
        let goal = ProbeGoal {
            available_bandwidth_bps: self.committed,
            expected_usage_bps: self.expected,
            desired_bps: desired,
            duration,
            desired_bytes: schedule.desired_bytes,
        };
        let stats = rtc.direct_api().remote_feedback_stats();
        let baseline = self.counts(&stats);
        self.active = Some(Active {
            id: self.next_id,
            goal,
            started: now,
            base: baseline,
            done: false,
            result: ProbeResult::default(),
            padding_id: None,
            schedule,
        });
        tracing::debug!(id=self.next_id,goal=?goal,requested_at=?now,"remote probe starting");
        self.policy.start_probe(self.next_id, goal, now);
        self.next_poll = Some(now);
        true
    }
    /// Service on the transport owner, with explicit time and existing bounded drive.
    pub fn service(&mut self, rtc: &mut Rtc, connected: bool, now: Instant) -> bool {
        if self.active.is_none() {
            self.next_poll = None;
            return false;
        }
        if connected && self.next_poll.is_some_and(|at| at > now) {
            return false;
        }
        // Hangover still gets a future deadline; no stale now-deadline busy loop.
        self.next_poll = Some(now + POLL);
        let stats = rtc.direct_api().remote_feedback_stats();
        let counts = self.counts(&stats);
        if !connected {
            self.finish(rtc, &stats, false, now);
        }
        if let Some(active) = self.active.as_ref().filter(|p| !p.done) {
            let result = Self::result(active, counts, self.origin, now, false);
            let bytes =
                result.bytes_probe + result.bytes_non_probe_primary + result.bytes_non_probe_rtx;
            let duration = active
                .goal
                .duration
                .nonnegative_to_std()
                .unwrap_or(Duration::ZERO);
            if now.duration_since(active.started) >= duration && bytes >= active.goal.desired_bytes
            {
                self.finish(rtc, &stats, true, now);
            }
        }
        if let Some(outcome) = self.policy.finalize(now) {
            tracing::debug!(outcome=?outcome,"remote probe receiver policy settled");
            if outcome.signal != ProbeSignal::Congesting {
                self.committed = self.committed.max(outcome.capacity);
            }
            self.active = None;
            self.next_poll = None;
            return true;
        }
        let Some(active) = self.active.as_mut().filter(|p| !p.done) else {
            return false;
        };
        if let Some(id) = active.padding_id {
            match rtc.direct_api().padding_status() {
                Some(s) if s.id == id && s.status == PaddingStatus::Active => return false,
                _ => active.padding_id = None,
            }
        }
        let sent = counts
            .padding_bytes
            .saturating_sub(active.base.padding_bytes) as i64;
        let requested = active.schedule.poll(now, sent);
        if requested > 0 {
            // Each request is bounded; chunk completion is not cluster completion.
            // Fixed PassThrough writes each requested burst immediately. The
            // explicit executor also enforces a16-packet/1ms owner fairness yield.
            let bytes = (requested as u64).min(16 * 1024 * 1024);
            match rtc
                .direct_api()
                .request_padding_burst(bytes, Duration::from_secs(5), now)
            {
                Ok(id) => active.padding_id = Some(id),
                Err(error) => {
                    tracing::debug!(?error, "remote probe execution unavailable");
                    self.finish(rtc, &stats, false, now);
                }
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn rtc(now: Instant) -> Rtc {
        Rtc::builder()
            .set_crypto_provider(rtc_crypto::provider().into())
            .set_dtls_cert(rtc_crypto::dtls_cert().unwrap())
            .build(now)
    }
    #[test]
    fn execution_failure_does_not_grant_budget_or_leave_a_due_now_settle_loop() {
        let start = Instant::now();
        let mut r = rtc(start);
        let mut config = CongestionControl::default();
        config
            .remote_bwe
            .channel_observer_non_probe
            .estimate
            .required_samples = 3;
        config
            .remote_bwe
            .channel_observer_non_probe
            .estimate
            .required_samples_min = 3;
        config
            .remote_bwe
            .channel_observer_non_probe
            .estimate
            .collapse_threshold = rtc_core::config::PolicyDuration::ZERO;
        let mut f = Feedback::new(&config, start);
        f.update_tracks(vec![Mid::new()], 1_000_000);
        // Synthetic policy samples only: the separate public test supplies real SRTCP.
        for (index, value) in [800_000, 600_000, 400_000, 800_000, 900_000, 950_000]
            .into_iter()
            .enumerate()
        {
            f.estimate(
                value,
                &[],
                &mut r,
                start + Duration::from_millis((index as u64 + 1) * 10),
            );
        }
        assert_eq!(f.policy.state(), CongestionState::None);
        let committed = f.capacity();
        assert_eq!(committed, 400_000);
        let now = start + Duration::from_secs(4);
        assert!(f.maybe_start(Some(300_000), true, false, &mut r, now));
        let id = f.active.as_ref().unwrap().id;
        f.service(&mut r, true, now); // Real API rejects missing DTLS/source; no fake output.
        assert!(f.active.as_ref().unwrap().done);
        assert_eq!(f.capacity(), committed);
        assert!(f.deadline().unwrap() > now);
        for ms in [10, 20, 100] {
            let at = now + Duration::from_millis(ms);
            f.service(&mut r, true, at);
            assert!(f.deadline().unwrap() > at);
            assert_eq!(f.capacity(), committed);
        }
        assert!(f.service(&mut r, true, now + Duration::from_secs(1)));
        assert!(f.active.is_none());
        assert_eq!(f.capacity(), committed);
        assert!(f.deadline().is_none());
        assert!(f.maybe_start(
            Some(300_000),
            true,
            false,
            &mut r,
            now + Duration::from_secs(10)
        ));
        assert!(f.active.as_ref().unwrap().id > id);
    }
    #[test]
    fn source_media_probe_is_real_allocation_transition_not_measured_capacity() {
        let now = Instant::now();
        let mut cfg = CongestionControl::default();
        cfg.stream_allocator.probe_mode = "media".into();
        let mut f = Feedback::new(&cfg, now);
        f.committed = 100_000;
        let mid = Mid::new();
        let tracks = [MediaDemand {
            mid,
            desired: 2,
            priority: 1,
            exempt: false,
            rates: [100_000, 300_000, 900_000],
        }];
        let at = now + Duration::from_secs(1);
        assert_eq!(f.media_caps(&tracks, &[-1], true, false, at), vec![0]);
        assert_eq!(f.capacity(), 100_000);
        assert!(f.active.is_none());
        assert_eq!(
            f.media_caps(&tracks, &[-1], true, false, at + Duration::from_secs(1)),
            vec![1]
        );
        assert_eq!(
            f.media_caps(&tracks, &[-1], true, true, at + Duration::from_secs(2)),
            vec![-1]
        );
        assert_eq!(f.capacity(), 100_000); // override stops exploration, never changes proof.
    }
    #[test]
    fn native_flag_priority_does_not_reuse_receiver_policy_selection() {
        let now = Instant::now();
        let mut cfg = CongestionControl::default();
        assert_eq!(
            Feedback::new(&cfg, now).mode(),
            FeedbackMode::ReceiverEstimate
        );
        cfg.use_send_side_bwe_interceptor = true;
        assert_eq!(
            Feedback::new(&cfg, now).mode(),
            FeedbackMode::InterceptorEstimate
        );
        cfg.use_send_side_bwe = true;
        assert_eq!(
            Feedback::new(&cfg, now).mode(),
            FeedbackMode::NativeSendSide
        );
    }
}

#[cfg(test)]
mod counter_tests {
    use super::*;
    fn sample(mid: Mid, generation: u64, packets: u32, bytes: u64) -> RemoteFeedbackStats {
        RemoteFeedbackStats {
            mid,
            ssrc: 100,
            rtx_ssrc: Some(101),
            generation,
            packets_primary: packets,
            repeated_nacks: 0,
            bytes_primary: bytes,
            bytes_rtx: 0,
            packets_rtx: 0,
            bytes_padding: 0,
            packets_padding: 0,
            last_output_at: None,
        }
    }
    #[test]
    fn actual_counter_collection_survives_unsubscribe_wrap_and_ssrc_generation_without_fake_bytes()
    {
        let now = Instant::now();
        let mid = Mid::new();
        let mut f = Feedback::new(&CongestionControl::default(), now);
        f.update_tracks(vec![mid], 0);
        let before = f.counts(&[sample(mid, 0, u32::MAX, 1000)]);
        let wrapped = f.counts(&[sample(mid, 0, 2, 1100)]);
        assert_eq!(wrapped.primary_packets - before.primary_packets, 3);
        assert_eq!(wrapped.primary_bytes - before.primary_bytes, 100);
        let absent = f.counts(&[]);
        assert_eq!(absent.primary_bytes, 1100);
        assert!(f.output_previous.is_empty());
        let reset = f.counts(&[sample(mid, 1, 1, 120)]);
        assert_eq!(reset.primary_bytes, 1220);
        assert_eq!(reset.primary_packets, wrapped.primary_packets + 1);
    }
    #[test]
    fn probe_padding_on_an_untracked_stream_counts_toward_the_probe() {
        let now = Instant::now();
        let video = Mid::new();
        let padding_mid = Mid::new();
        let mut f = Feedback::new(&CongestionControl::default(), now);
        f.update_tracks(vec![video], 0);
        let mut pad = sample(padding_mid, 0, 0, 0);
        pad.ssrc = 200;
        let first = f.counts(&[sample(video, 0, 10, 1000), pad]);
        pad.packets_padding = 40;
        pad.bytes_padding = 9_600;
        let after = f.counts(&[sample(video, 0, 20, 2000), pad]);
        assert_eq!(after.padding_packets - first.padding_packets, 40);
        assert_eq!(after.padding_bytes - first.padding_bytes, 9_600);
        // Media of the untracked stream still does not count as video primary.
        assert_eq!(after.primary_packets - first.primary_packets, 10);
    }
    #[test]
    fn actual_report_identity_handles_rtx_subset_and_rejects_unknown_or_unsubscribed_sources() {
        let now = Instant::now();
        let mid = Mid::new();
        let mut f = Feedback::new(&CongestionControl::default(), now);
        f.update_tracks(vec![mid], 0);
        let samples = [sample(mid, 0, 1, 100)];
        assert!(f.accept_remb(&[100], &samples));
        assert!(f.accept_remb(&[101], &samples));
        assert!(!f.accept_remb(&[999], &samples));
        assert!(!f.accept_remb(&[], &samples));
        f.update_tracks(vec![], 0);
        assert!(!f.accept_remb(&[100, 101], &samples));
    }
}

#[cfg(test)]
mod interceptor_callback_tests {
    use super::*;
    #[test]
    fn source_callback_only_delivers_actual_target_changes_not_initial_or_cached_estimates() {
        let config = CongestionControl {
            use_send_side_bwe: false,
            use_send_side_bwe_interceptor: true,
            ..Default::default()
        };
        let mut feedback = Feedback::new(&config, Instant::now());
        assert_eq!(feedback.mode(), FeedbackMode::InterceptorEstimate);
        assert!(!feedback.interceptor_estimate_changed(1_000_000));
        assert!(feedback.interceptor_estimate_changed(700_000));
        assert!(!feedback.interceptor_estimate_changed(700_000));
        assert!(feedback.interceptor_estimate_changed(1_000_000));
        assert!(!feedback.interceptor_estimate_changed(1_000_000));
        assert_eq!(
            feedback.capacity(),
            100_000_000,
            "callback identity does not itself commit allocation"
        );
        let mut receiver = Feedback::new(&CongestionControl::default(), Instant::now());
        assert!(!receiver.interceptor_estimate_changed(700_000));
    }
}
