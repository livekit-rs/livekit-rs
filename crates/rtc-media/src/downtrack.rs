//! A subscription (down track): forwarding state of one published track towards one
//! subscriber transport (sfu-media.md §2, §4.4, §7).
//!
//! Layer logic is pure and unit-tested here; the engine feeds frames and timers and
//! performs the str0m writes.

use std::time::{Duration, Instant};

use rtc_core::ids::TransportId;
use rtc_core::media_api::DownTrackSpec;
use rtc_core::model::{TrackKind, TrackSettings, VideoQuality};
use str0m::media::{Frequency, MediaTime, Mid, Pt};

use crate::layers::{self, INVALID_SPATIAL, MAX_SPATIAL};
use crate::quality::Scorer;
use crate::uptrack::UpTrack;

/// `subscriptionDebounceInterval` (subscribedtrack.go:40).
pub(crate) const SETTINGS_DEBOUNCE: Duration = Duration::from_millis(100);
/// Keyframe request re-arm bounds (`keyFrameIntervalMin/Max`, downtrack.go:64-65).
const PLI_INTERVAL_MIN: Duration = Duration::from_millis(200);
const PLI_INTERVAL_MAX: Duration = Duration::from_millis(1000);
/// Initial acquisition grace (`initialLayerAcquisitionGrace`, forwarder.go:63).
const ACQUISITION_GRACE: Duration = Duration::from_secs(1);
/// `RTPBlankFramesMuteSeconds = 1.0` at 20 ms per opus frame.
pub(crate) const SILENCE_FRAMES: u32 = 50;
pub(crate) const SILENCE_INTERVAL: Duration = Duration::from_millis(20);

/// Re-arm interval for layer-lock PLIs: `clamp(2*RTT, 200 ms, 1000 ms)`.
pub(crate) fn pli_interval(rtt: Option<Duration>) -> Duration {
    let rtt = rtt.unwrap_or(Duration::from_millis(100));
    (rtt * 2).clamp(PLI_INTERVAL_MIN, PLI_INTERVAL_MAX)
}

/// Select an available layer, then fall back to a configured layer for
/// acquisition. An absent tracker must never be treated as temporarily idle.
pub(crate) fn select_target(max: i32, available: [bool; 3], configured: [bool; 3]) -> i32 {
    if max < 0 {
        return INVALID_SPATIAL;
    }
    let max = max.min(MAX_SPATIAL);
    for layers in [
        std::array::from_fn(|i| available[i] && configured[i]),
        configured,
    ] {
        if let Some(layer) = (0..=max).rev().find(|layer| layers[*layer as usize]) {
            return layer;
        }
        if let Some(layer) = (0..=MAX_SPATIAL)
            .rev()
            .find(|layer| layers[*layer as usize])
        {
            return layer;
        }
    }
    INVALID_SPATIAL
}

/// Keyframe-gated switch decision (videolayerselector/simulcast.go:63-161).
/// Returns the new current layer when a frame of `layer` may switch.
pub(crate) fn switch_on_keyframe(
    current: i32,
    target: i32,
    layer: i32,
    keyframe: bool,
    grace_over: bool,
) -> Option<i32> {
    if !keyframe || target < 0 || current == target {
        return None;
    }
    if current < 0 {
        // Start / resume: aim at the target during the grace period, then accept any
        // keyframe at or below it.
        return (layer == target || (grace_over && layer < target)).then_some(layer);
    }
    let upgrade = current < layer && layer <= target;
    let downgrade = target <= layer && layer < current;
    (upgrade || downgrade).then_some(layer)
}

/// What the forwarder decided for one frame.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Forward {
    Drop,
    /// Forward; `resumed` = first frame after (re)start (StreamState ACTIVE).
    Send {
        resumed: bool,
    },
}

#[derive(Clone, Copy, Debug)]
struct OutClock {
    numer: u64,
    freq: Frequency,
    wallclock: Instant,
}

pub(crate) struct DownTrack {
    pub spec: DownTrackSpec,
    pub kind: TrackKind,
    pub up: usize,
    pub transport: Option<TransportId>,
    pub mid: Option<Mid>,
    /// Mid negotiated (dual: answer accepted; single: assigned).
    pub negotiated: bool,
    pub bound_emitted: bool,
    pub rr_source_id: Option<u64>,
    pub rr_report_sequence: u64,
    pub encrypted: bool,
    pub av1: crate::av1::Forwarder,
    pub av1_packet_selector: crate::av1::PacketSelector,
    pub av1_packet_sequences: crate::av1::PacketSequences,
    pub(crate) one_dd: crate::av1::OneSelector,
    pub(crate) dd_fallback: crate::dd_fallback::Selector,
    pub red_disabled: bool,

    // settings
    pub settings: TrackSettings,
    pending_settings: Option<(TrackSettings, Instant)>,
    /// Max spatial from settings (-1 when disabled).
    pub max_spatial: i32,
    /// Allocator cap (-1 = BW paused).
    pub alloc_cap: i32,
    pub sub_muted: bool,
    pub pub_muted: bool,
    pub bw_paused: bool,

    // layer selection
    pub target: i32,
    pub current: i32,
    reference_spatial: Option<usize>,
    reference_reports: [bool; 3],
    svc_temporal: u8,
    svc_temporal_target: u8,
    pub(crate) subscriber_temporal_limit: u8,
    started_at: Instant,
    pub next_pli_at: Option<Instant>,

    // time continuity
    ts_offset: i64,
    last_out: Option<OutClock>,
    resync: bool,
    pub last_pt: Option<(Pt, Pt)>,
    pub codec_failed: bool,

    // sequence continuity (fixed Go RTPMunger): the forwarded source stream and
    // its last processed upstream sequence; upstream gaps become downstream gaps.
    pub(crate) seq_source: Option<((Option<str0m::media::Rid>, Pt), u64)>,
    pub(crate) pending_seq_gap: u64,

    // audio silence on publisher mute
    pub silence_left: u32,
    pub silence_next: Option<Instant>,

    pub scorer: Scorer,
    /// Last cumulative egress counters from str0m stats (deltas).
    pub egress_packets: u64,
    pub egress_nacks: u64,
    /// Actual subscriber PLI/FIR forwarding attempt time, Unix nanoseconds.
    pub debug_last_pli_unix_ns: i64,
}

impl DownTrack {
    pub(crate) fn debug_timestamp_offset(&self) -> u64 {
        0u64.wrapping_sub(self.ts_offset as u64)
    }
    pub(crate) fn new(
        spec: DownTrackSpec,
        kind: TrackKind,
        up: usize,
        encrypted: bool,
        mime: &str,
        now: Instant,
    ) -> Self {
        let mut settings = spec.settings.clone();
        if kind == TrackKind::Video && spec.adaptive_stream {
            if settings == TrackSettings::default() {
                // §2.1: no settings yet + adaptive stream → start LOW.
                settings.quality = VideoQuality::Low;
            } else {
                // Settings exist: adaptive stream re-sends visibility, start enabled.
                settings.disabled = false;
            }
        }
        Self {
            sub_muted: settings.disabled,
            settings,
            spec,
            kind,
            up,
            transport: None,
            mid: None,
            negotiated: false,
            bound_emitted: false,
            rr_source_id: None,
            rr_report_sequence: 0,
            encrypted,
            av1: Default::default(),
            av1_packet_selector: Default::default(),
            av1_packet_sequences: Default::default(),
            one_dd: Default::default(),
            dd_fallback: Default::default(),
            red_disabled: false,
            pending_settings: None,
            max_spatial: if kind == TrackKind::Video {
                MAX_SPATIAL
            } else {
                0
            },
            alloc_cap: MAX_SPATIAL,
            pub_muted: false,
            bw_paused: false,
            target: INVALID_SPATIAL,
            current: INVALID_SPATIAL,
            reference_spatial: None,
            reference_reports: [false; 3],
            svc_temporal: 3,
            svc_temporal_target: 3,
            subscriber_temporal_limit: 3,
            started_at: now,
            next_pli_at: None,
            ts_offset: 0,
            last_out: None,
            resync: true,
            last_pt: None,
            codec_failed: false,
            seq_source: None,
            pending_seq_gap: 0,
            silence_left: 0,
            silence_next: None,
            scorer: Scorer::new(mime),
            egress_packets: 0,
            egress_nacks: 0,
            debug_last_pli_unix_ns: 0,
        }
    }

    /// Keep subscription intent while retiring all state tied to the old RTP sender.
    pub(crate) fn replace_sender(&mut self, target: usize, up: &UpTrack, now: Instant) {
        let mut replacement = Self::new(
            self.spec.clone(),
            up.kind,
            target,
            up.spec.encrypted,
            &up.mime,
            now,
        );
        replacement.settings = self.settings.clone();
        replacement.pending_settings = self.pending_settings.take();
        replacement.pub_muted = up.muted;
        replacement.apply_settings_layers(&up.layers);
        replacement.subscriber_temporal_limit(up, replacement.max_spatial);
        replacement
            .scorer
            .set_muted(replacement.pub_muted || replacement.sub_muted);
        *self = replacement;
    }

    pub(crate) fn is_video(&self) -> bool {
        self.kind == TrackKind::Video
    }

    /// Recompute `max_spatial` from the applied settings and the publisher layers.
    pub(crate) fn apply_settings_layers(&mut self, up_layers: &[rtc_core::model::VideoLayer]) {
        self.sub_muted = self.settings.disabled;
        if !self.is_video() {
            self.max_spatial = 0;
            return;
        }
        let mut quality = self.settings.quality;
        if self.settings.width > 0 || self.settings.height > 0 {
            quality =
                layers::quality_for_dimension(up_layers, self.settings.width, self.settings.height);
        }
        self.max_spatial = layers::spatial_for_quality(up_layers, quality);
    }

    /// Go only applies a new maximum for enabled settings with a positive FPS.
    /// A zero value leaves the current DownTrack maximum in place.
    pub(crate) fn subscriber_temporal_limit(&mut self, up: &UpTrack, spatial: i32) -> u8 {
        if !self.sub_muted && self.settings.fps > 0 {
            self.subscriber_temporal_limit =
                up.svc_temporal_cap_for_spatial(spatial, self.settings.fps);
        }
        self.subscriber_temporal_limit
    }

    /// UpdateTrackSettings with 100 ms debounce, immediate on disabled→enabled.
    /// Returns true when settings were applied now.
    pub(crate) fn update_settings(&mut self, s: TrackSettings, now: Instant) -> bool {
        if self.pending_settings.is_none() && s == self.settings {
            return false;
        }
        if self.settings.disabled && !s.disabled {
            self.pending_settings = None;
            self.settings = s;
            return true;
        }
        self.pending_settings = Some((s, now + SETTINGS_DEBOUNCE));
        false
    }

    pub(crate) fn settings_deadline(&self) -> Option<Instant> {
        self.pending_settings.as_ref().map(|(_, t)| *t)
    }

    /// Apply debounced settings when due. Returns true when applied.
    pub(crate) fn poll_settings(&mut self, now: Instant) -> bool {
        match &self.pending_settings {
            Some((_, t)) if *t <= now => {
                let (s, _) = self.pending_settings.take().expect("checked");
                if s == self.settings {
                    return false;
                }
                self.settings = s;
                true
            }
            _ => false,
        }
    }

    /// Effective max layer: settings ∧ allocator, -1 when muted or paused.
    pub(crate) fn effective_max(&self) -> i32 {
        if self.sub_muted || self.pub_muted || self.bw_paused {
            return INVALID_SPATIAL;
        }
        if !self.is_video() {
            return 0;
        }
        self.max_spatial.min(self.alloc_cap)
    }

    pub(crate) fn demand_spatial(&self) -> i32 {
        if self.sub_muted {
            return INVALID_SPATIAL;
        }
        let layer = self.max_spatial.max(self.current);
        match self.reference_spatial {
            Some(reference) if !self.reference_reports[reference] => layer.max(reference as i32),
            _ => layer,
        }
    }

    pub(crate) fn observe_reference_layer(&mut self, spatial: i32) {
        if self.is_video() && (0..=MAX_SPATIAL).contains(&spatial) {
            self.reference_spatial.get_or_insert(spatial as usize);
        }
    }

    pub(crate) fn observe_reference_report(&mut self, spatial: i32) {
        if let Some(report) = usize::try_from(spatial)
            .ok()
            .and_then(|layer| self.reference_reports.get_mut(layer))
        {
            *report = true;
        }
    }

    pub(crate) fn restart_receiver(&mut self) {
        self.stop_forwarding();
        self.target = INVALID_SPATIAL;
        self.reference_spatial = None;
        self.reference_reports = [false; 3];
        self.last_pt = None;
        self.one_dd = Default::default();
    }

    #[cfg(test)]
    pub(crate) fn reference_state(&self) -> (Option<usize>, [bool; 3]) {
        (self.reference_spatial, self.reference_reports)
    }

    /// Recompute the target from availability. Returns true when it changed.
    pub(crate) fn refresh_target(&mut self, available: [bool; 3], configured: [bool; 3]) -> bool {
        let t = if self.is_video() {
            select_target(self.effective_max(), available, configured)
        } else if self.effective_max() < 0 {
            INVALID_SPATIAL
        } else {
            0
        };
        if t == self.target {
            return false;
        }
        self.target = t;
        if t < 0 {
            self.stop_forwarding();
        }
        true
    }

    pub(crate) fn one_dd_spatial(&self, layer: i32, key: bool, now: Instant) -> Option<i32> {
        if self.target < 0 {
            return None;
        }
        let grace_over = now.saturating_duration_since(self.started_at) >= ACQUISITION_GRACE;
        let current = switch_on_keyframe(self.current, self.target, layer, key, grace_over)
            .unwrap_or(self.current);
        (layer == current).then_some(layer)
    }

    pub(crate) fn rebase_one_dd_clock(&mut self, input: MediaTime, mapped: Option<u32>) -> bool {
        let Some(last) = self.last_out else {
            return true;
        };
        let delta = mapped.map_or(0, |mapped| mapped.wrapping_sub(last.numer as u32) as i32);
        if i64::from(delta) < -(i64::from(last.freq.get()) / 20) {
            return false;
        }
        self.ts_offset = last.numer as i64 + i64::from(delta.max(1)) - input.numer() as i64;
        self.resync = false;
        true
    }

    /// Commit only after a real original AV1 packet entered the native sender.
    pub(crate) fn commit_av1_packet_selection(
        &mut self,
        spatial: i32,
        temporal: u8,
        target_temporal: u8,
    ) {
        self.current = spatial;
        self.svc_temporal = temporal;
        self.svc_temporal_target = target_temporal;
    }

    /// Current temporal admission, after the actual keyframe-gated decision.
    pub(crate) fn forwarded_temporal_cap(&self) -> u8 {
        self.svc_temporal
    }

    /// Stop forwarding (mute/pause/disable); the next frame needs a resync.
    pub(crate) fn stop_forwarding(&mut self) {
        self.current = INVALID_SPATIAL;
        self.next_pli_at = None;
        self.resync = true;
        self.seq_source = None;
        self.pending_seq_gap = 0;
    }

    /// Fixed Go `RTPMunger.UpdateAndGetSnTs` maps an in-order packet to
    /// `SN - snOffset`: sequence numbers never received upstream stay a gap
    /// downstream, while packets the SFU drops itself (`PacketDropped`, layer
    /// filtering, padding) shift the offset instead. A (re)start or a switch to
    /// another source stream (`SetLastSnTs`/`UpdateSnTsOffsets`) continues
    /// without a gap. Frames of the current source update the position whether
    /// or not they are forwarded; only upstream sequence numbers between frames
    /// that never reached the depacketizer accumulate as a gap.
    ///
    /// A source is one upstream RTP stream: the rid and the payload type (a
    /// backup codec is a separate SSRC with its own sequence space). Returns
    /// whether the frame continued the tracked source.
    pub(crate) fn observe_source_seq(
        &mut self,
        rid: Option<str0m::media::Rid>,
        pt: Pt,
        start: u64,
        end: u64,
        forwarded: bool,
        resumed: bool,
    ) -> bool {
        let key = (rid, pt);
        match self.seq_source {
            Some((source, last)) if source == key && !resumed => {
                if end <= last {
                    return false; // duplicate or older than the current position
                }
                let gap = start.saturating_sub(last + 1);
                // A jump of half the sequence space is a source restart, not loss.
                if gap < 1 << 15 {
                    self.pending_seq_gap += gap;
                }
                self.seq_source = Some((key, end));
                true
            }
            _ if forwarded => {
                self.seq_source = Some((key, end));
                self.pending_seq_gap = 0;
                false
            }
            _ => false,
        }
    }

    /// Needs a keyframe from the publisher to lock onto the target.
    pub(crate) fn wants_keyframe(&self) -> bool {
        // Fixed Base.CheckSync uses requested spatial only. A temporal-only
        // upgrade waits for codec switching metadata rather than sending PLI.
        self.is_video() && self.target >= 0 && self.current != self.target
    }

    /// Decide for one incoming frame of `layer`.
    pub(crate) fn on_frame(&mut self, layer: i32, keyframe: bool, now: Instant) -> Forward {
        if self.target < 0 {
            return Forward::Drop;
        }
        if !self.is_video() {
            let resumed = self.current < 0;
            if resumed {
                self.current = 0;
                self.resync = true;
            }
            return Forward::Send { resumed: false };
        }
        let grace_over = now.saturating_duration_since(self.started_at) >= ACQUISITION_GRACE;
        let mut resumed = false;
        if let Some(new) =
            switch_on_keyframe(self.current, self.target, layer, keyframe, grace_over)
        {
            resumed = self.current < 0;
            self.current = new;
            self.resync = true;
            if self.current == self.target {
                self.next_pli_at = None;
            }
        }
        if layer != self.current {
            return Forward::Drop;
        }
        Forward::Send { resumed }
    }

    /// VP9 SVC carries all spatial dependencies in one frame. Keep the old
    /// target until a keyframe, then select the dependency-complete prefix.
    pub(crate) fn on_svc_frame(
        &mut self,
        highest: i32,
        temporal: u8,
        keyframe: bool,
        max_temporal: u8,
    ) -> Forward {
        self.svc_temporal_target = max_temporal.min(3);
        if self.target < 0 {
            return Forward::Drop;
        }
        let resumed = self.current < 0;
        if keyframe {
            self.current = self.target.min(highest);
            self.svc_temporal = self.svc_temporal_target;
            if resumed {
                self.resync = true;
            }
            if self.current == self.target {
                self.next_pli_at = None;
            }
        }
        if self.current < 0 || highest < self.current || temporal > self.svc_temporal {
            Forward::Drop
        } else {
            Forward::Send { resumed }
        }
    }

    /// Pinned Go VP9.Select compares VideoLayer spatial-first, then temporal.
    /// Lower spatial components remain admissible above the current temporal
    /// ceiling; only the current spatial layer is restricted by its TID.
    pub(crate) fn vp9_forwarded_spatial_cap(&self, temporal: u8) -> i32 {
        self.current - i32::from(temporal > self.svc_temporal)
    }
    pub(crate) fn on_vp9_frame(
        &mut self,
        highest: i32,
        temporal: u8,
        keyframe: bool,
        max_temporal: u8,
        switching_up: bool,
        beginning: bool,
        ending: bool,
    ) -> Forward {
        self.svc_temporal_target = max_temporal.min(3);
        if self.target < 0 {
            return Forward::Drop;
        }
        let resumed = self.current < 0;
        if resumed {
            if !keyframe {
                return Forward::Drop;
            }
            self.current = self.target.min(highest);
            self.svc_temporal = temporal.min(3);
            self.resync = true;
        } else {
            // Fixed VP9.Select: temporal down at E, up only on actual U+B;
            // neither waits for an unrelated codec keyframe.
            if self.svc_temporal > self.svc_temporal_target && ending {
                self.svc_temporal = self.svc_temporal_target;
            } else if self.svc_temporal < self.svc_temporal_target
                && temporal > self.svc_temporal
                && temporal <= self.svc_temporal_target
                && switching_up
                && beginning
            {
                self.svc_temporal = temporal;
            }
            if self.current > self.target && ending {
                self.current = self.target.min(highest);
            } else if keyframe && highest > self.current {
                self.current = self.target.min(highest);
            }
        }
        if self.current == self.target {
            self.next_pli_at = None;
        }
        if self.current < 0 || self.vp9_forwarded_spatial_cap(temporal) < 0 {
            Forward::Drop
        } else {
            Forward::Send { resumed }
        }
    }

    /// Restart the acquisition grace (new subscription target after (un)mute).
    pub(crate) fn restart_acquisition(&mut self, now: Instant) {
        self.started_at = now;
    }

    /// Late source RTP retains its original timestamp without moving the
    /// forward clock anchor used on a later pause/resume or source switch.
    pub(crate) fn out_original_packet_time(
        &mut self,
        input: MediaTime,
        wallclock: Instant,
        late: bool,
    ) -> MediaTime {
        if !late || self.resync {
            return self.out_time(input, wallclock);
        }
        MediaTime::new(
            (input.numer() as i64 + self.ts_offset).max(0) as u64,
            input.frequency(),
        )
    }

    /// Outgoing RTP time for an input frame. Continuous across layer switches and
    /// mutes: on resync, continue from the last output plus the wallclock gap.
    pub(crate) fn out_time(&mut self, input: MediaTime, wallclock: Instant) -> MediaTime {
        let freq = input.frequency();
        let in_numer = input.numer() as i64;
        if self.resync {
            self.resync = false;
            self.ts_offset = match self.last_out {
                Some(last) if last.freq == freq => {
                    let gap = wallclock.saturating_duration_since(last.wallclock);
                    let ticks = ((gap.as_micros() as u64).saturating_mul(freq.get() as u64)
                        / 1_000_000)
                        .max(1) as i64;
                    (last.numer as i64 + ticks) - in_numer
                }
                _ => self.ts_offset,
            };
        }
        let out = (in_numer + self.ts_offset).max(0) as u64;
        // Never go backwards (reordered frames keep their relative position).
        self.last_out = Some(OutClock {
            numer: out,
            freq,
            wallclock,
        });
        MediaTime::new(out, freq)
    }

    /// RTP time for a synthesized frame `step` after the last output (in the last
    /// output's clock rate).
    pub(crate) fn synth_time(&mut self, step: Duration, wallclock: Instant) -> Option<MediaTime> {
        let last = self.last_out?;
        let ticks = (step.as_micros() as u64).saturating_mul(last.freq.get() as u64) / 1_000_000;
        let numer = last.numer + ticks.max(1);
        self.last_out = Some(OutClock {
            numer,
            freq: last.freq,
            wallclock,
        });
        self.resync = true;
        Some(MediaTime::new(numer, last.freq))
    }

    /// Publisher mute changed. Audio starts ~1 s of silence when it was flowing.
    pub(crate) fn set_pub_muted(&mut self, muted: bool, now: Instant) {
        if self.pub_muted == muted {
            return;
        }
        self.pub_muted = muted;
        if muted {
            let was_flowing = self.current >= 0;
            self.stop_forwarding();
            if !self.is_video() {
                self.target = INVALID_SPATIAL;
                if was_flowing && self.last_out.is_some() {
                    self.silence_left = SILENCE_FRAMES;
                    self.silence_next = Some(now);
                }
            }
        } else {
            self.silence_left = 0;
            self.silence_next = None;
            self.restart_acquisition(now);
        }
        self.scorer.set_muted(muted || self.sub_muted);
    }

    /// Timer deadline (settings debounce, silence injection). `next_pli_at` is only a
    /// rate limit checked when frames arrive, not a wake-up.
    pub(crate) fn next_deadline(&self) -> Option<Instant> {
        match (self.settings_deadline(), self.silence_next) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtc_core::ids::{ParticipantSid, TrackSid};

    fn spec(adaptive: bool, settings: TrackSettings) -> DownTrackSpec {
        DownTrackSpec {
            subscriber: ParticipantSid::from("PA_sub"),
            publisher: ParticipantSid::from("PA_pub"),
            track_sid: TrackSid::from("TR_x"),
            stream_id: "PA_pub|TR_x".into(),
            settings,
            adaptive_stream: adaptive,
        }
    }

    fn video(now: Instant) -> DownTrack {
        DownTrack::new(
            spec(false, TrackSettings::default()),
            TrackKind::Video,
            0,
            false,
            "video/VP8",
            now,
        )
    }

    #[test]
    fn pli_interval_is_clamped() {
        assert_eq!(pli_interval(None), Duration::from_millis(200));
        assert_eq!(
            pli_interval(Some(Duration::from_millis(300))),
            Duration::from_millis(600)
        );
        assert_eq!(
            pli_interval(Some(Duration::from_secs(2))),
            Duration::from_millis(1000)
        );
        assert_eq!(
            pli_interval(Some(Duration::from_millis(10))),
            Duration::from_millis(200)
        );
    }

    #[test]
    fn reference_report_hold_survives_pause_but_not_receiver_restart() {
        let now = Instant::now();
        for reference in 0..=2 {
            let mut down = video(now);
            down.observe_reference_layer(reference);
            down.max_spatial = -1;
            down.stop_forwarding();
            assert_eq!(down.demand_spatial(), reference);
            down.observe_reference_layer((reference + 1) % 3);
            down.observe_reference_report((reference + 1) % 3);
            assert_eq!(down.demand_spatial(), reference);
            down.set_pub_muted(true, now);
            assert_eq!(down.demand_spatial(), reference);
            down.sub_muted = true;
            assert_eq!(down.demand_spatial(), -1);
            down.sub_muted = false;
            down.set_pub_muted(false, now);
            assert_eq!(down.demand_spatial(), reference);
            down.observe_reference_report(reference);
            assert_eq!(down.demand_spatial(), -1);
            down.restart_receiver();
            assert_eq!(down.reference_state(), (None, [false; 3]));
            assert_eq!(down.demand_spatial(), -1);
            down.observe_reference_layer(reference);
            assert_eq!(down.demand_spatial(), reference);
        }
    }

    #[test]
    fn reference_reports_are_subscriber_local_and_can_precede_forwarding() {
        let now = Instant::now();
        let mut earlier = video(now);
        earlier.observe_reference_report(2);
        earlier.observe_reference_layer(2);
        earlier.max_spatial = 0;
        assert_eq!(earlier.demand_spatial(), 0);
        let mut later = video(now);
        later.observe_reference_layer(2);
        later.max_spatial = 0;
        assert_eq!(later.demand_spatial(), 2);
        later.observe_reference_report(-1);
        later.observe_reference_report(3);
        assert_eq!(later.reference_state(), (Some(2), [false; 3]));
        earlier.current = 2;
        assert_eq!(earlier.demand_spatial(), 2);
        earlier.current = 0;
        assert_eq!(earlier.demand_spatial(), 0);
    }

    // Public Go Forwarder source-contract differential only, not packet/SRTP proof.
    mod c54_reference_source {
        use super::*;
        use serde::{Deserialize, Serialize};
        use serde_json::{Value, json};
        use std::io::Write;

        const STAGES: [&str; 20] = [
            "constructed",
            "before_first_packet",
            "first_packet",
            "set_max",
            "report",
            "resync",
            "resync_again",
            "muted",
            "unmuted",
            "pub_muted",
            "pub_unmuted",
            "matching_zero_sr",
            "matching_sr_resync",
            "restart",
            "restart_packet",
            "restart_resync",
            "before_fresh",
            "fresh",
            "fresh_packet",
            "fresh_resync",
        ];
        const OBSERVATION_FIELDS: [&str; 9] = [
            "stage",
            "max_subscribed_spatial",
            "max_spatial",
            "current_spatial",
            "target_spatial",
            "started",
            "reference_spatial",
            "muted",
            "pub_muted",
        ];

        #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Scenario {
            codec_mode: String,
            reference: i32,
            max_spatial: i32,
            report: String,
            report_layer: i32,
        }

        // Nullable does not mean optional: reject an absent property.
        fn required_reference<'de, D: serde::Deserializer<'de>>(
            deserializer: D,
        ) -> Result<Option<i32>, D::Error> {
            Option::<i32>::deserialize(deserializer)
        }

        #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Observation {
            stage: String,
            max_subscribed_spatial: i32,
            max_spatial: i32,
            current_spatial: i32,
            target_spatial: i32,
            started: bool,
            #[serde(deserialize_with = "required_reference")]
            reference_spatial: Option<i32>,
            muted: bool,
            pub_muted: bool,
        }

        #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Row {
            schema: u32,
            case_id: usize,
            scenario: Scenario,
            observations: Vec<Observation>,
        }

        fn scenarios() -> Vec<Scenario> {
            let mut result = Vec::new();
            for codec_mode in ["h264-simulcast", "vp9-svc"] {
                for reference in [0, 1, 2] {
                    for max_spatial in [-1, 0, 2] {
                        for report in [
                            "none",
                            "wrong-layer",
                            "matching-zero",
                            "matching-valid",
                            "premature-zero",
                        ] {
                            result.push(Scenario {
                                codec_mode: codec_mode.into(),
                                reference,
                                max_spatial,
                                report: report.into(),
                                report_layer: match report {
                                    "none" => -1,
                                    "wrong-layer" => (reference + 1) % 3,
                                    _ => reference,
                                },
                            });
                        }
                    }
                }
            }
            result
        }

        fn read_rows(input: &str) -> Result<Vec<Row>, String> {
            let inventory = scenarios();
            let mut rows = Vec::new();
            for (id, line) in input.lines().enumerate() {
                let row: Row =
                    serde_json::from_str(line).map_err(|error| format!("C54 row {id}: {error}"))?;
                if row.schema != 1 || row.case_id != id || inventory.get(id) != Some(&row.scenario)
                {
                    return Err(format!(
                        "C54 row {id}: schema/case order/scenario inventory"
                    ));
                }
                if row.observations.len() != STAGES.len() {
                    return Err(format!("C54 row {id}: expected 20 stages"));
                }
                for (observation, stage) in row.observations.iter().zip(STAGES) {
                    if observation.stage != stage {
                        return Err(format!("C54 row {id}: expected stage {stage}"));
                    }
                    for (name, layer) in [
                        (
                            "max_subscribed_spatial",
                            Some(observation.max_subscribed_spatial),
                        ),
                        ("max_spatial", Some(observation.max_spatial)),
                        ("current_spatial", Some(observation.current_spatial)),
                        ("target_spatial", Some(observation.target_spatial)),
                        ("reference_spatial", observation.reference_spatial),
                    ] {
                        if layer.is_some_and(|layer| !(-1..=2).contains(&layer)) {
                            return Err(format!("C54 row {id}/{stage}: invalid {name}"));
                        }
                    }
                }
                rows.push(row);
            }
            if rows.len() != inventory.len() {
                return Err(format!("C54 expected 90 rows, got {}", rows.len()));
            }
            Ok(rows)
        }

        fn new_down(scenario: &Scenario, now: Instant) -> DownTrack {
            let mime = match scenario.codec_mode.as_str() {
                "h264-simulcast" => "video/H264",
                "vp9-svc" => "video/VP9",
                _ => panic!("not in C54 inventory"),
            };
            let mut down = DownTrack::new(
                spec(false, TrackSettings::default()),
                TrackKind::Video,
                0,
                false,
                mime,
                now,
            );
            // Bare Forwarder state precedes applying subscription settings.
            down.max_spatial = INVALID_SPATIAL;
            down
        }

        fn packet(down: &mut DownTrack, scenario: &Scenario, sequence: u64, now: Instant) {
            // Equivalent test control to ForceLayerForTest, NOT an allocator.
            down.current = scenario.reference;
            down.target = scenario.reference;
            // Exercise real continuity state, not a harness-owned started flag.
            let rid = if scenario.codec_mode == "vp9-svc" {
                None
            } else {
                Some(["q", "h", "f"][scenario.reference as usize].into())
            };
            down.observe_source_seq(rid, Pt::new_with_value(96), sequence, sequence, true, false);
            down.out_time(
                MediaTime::new(90_000 + sequence * 3_000, Frequency::NINETY_KHZ),
                now,
            );
            // This controls successful forwarding, without asserting codec or transport admission.
            down.observe_reference_layer(scenario.reference);
        }

        fn observe(down: &DownTrack, stage: &str) -> Observation {
            // The output clock survives resync; sequence-source state does not.
            let started = down.last_out.is_some();
            Observation {
                stage: stage.into(),
                max_subscribed_spatial: down.demand_spatial(),
                max_spatial: down.max_spatial,
                current_spatial: down.current,
                target_spatial: down.target,
                started,
                reference_spatial: started.then(|| {
                    down.reference_state()
                        .0
                        .map_or(INVALID_SPATIAL, |layer| layer as i32)
                }),
                muted: down.sub_muted,
                pub_muted: down.pub_muted,
            }
        }

        fn run(id: usize, scenario: Scenario) -> Row {
            let now = Instant::now();
            let mut down = new_down(&scenario, now);
            let mut observations = Vec::new();
            for stage in STAGES {
                match stage {
                    "constructed" => {}
                    "before_first_packet" => {
                        down.max_spatial = 2;
                        if scenario.report == "premature-zero" {
                            down.observe_reference_report(scenario.report_layer);
                        }
                    }
                    "first_packet" | "fresh_packet" => packet(&mut down, &scenario, 1, now),
                    "set_max" => down.max_spatial = scenario.max_spatial,
                    "report" => {
                        if matches!(
                            scenario.report.as_str(),
                            "matching-zero" | "wrong-layer" | "matching-valid"
                        ) {
                            // Hold release tests presence, not NTP/RTP validity.
                            down.observe_reference_report(scenario.report_layer);
                        }
                    }
                    "resync" | "resync_again" | "matching_sr_resync" | "restart_resync"
                    | "fresh_resync" => down.stop_forwarding(),
                    "muted" => {
                        down.sub_muted = true;
                        down.stop_forwarding();
                    }
                    "unmuted" => {
                        down.sub_muted = false;
                        down.restart_acquisition(now);
                    }
                    "pub_muted" => down.set_pub_muted(true, now),
                    "pub_unmuted" => down.set_pub_muted(false, now),
                    "matching_zero_sr" | "before_fresh" => {
                        down.observe_reference_report(scenario.reference);
                    }
                    "restart" => down.restart_receiver(),
                    "restart_packet" => packet(&mut down, &scenario, 2, now),
                    "fresh" => {
                        down = new_down(&scenario, now);
                        down.max_spatial = scenario.max_spatial;
                    }
                    _ => unreachable!("fixed C54 stage inventory"),
                }
                observations.push(observe(&down, stage));
            }
            Row {
                schema: 1,
                case_id: id,
                scenario,
                observations,
            }
        }

        fn differences(expected: &Row, actual: &Row) -> Vec<String> {
            let mut failures = Vec::new();
            if (expected.schema, expected.case_id, &expected.scenario)
                != (actual.schema, actual.case_id, &actual.scenario)
            {
                failures.push(format!("C54 row {}: inventory differs", actual.case_id));
            }
            assert_eq!(expected.observations.len(), STAGES.len());
            assert_eq!(actual.observations.len(), STAGES.len());
            for (index, stage) in STAGES.iter().enumerate() {
                let expected = serde_json::to_value(&expected.observations[index]).unwrap();
                let actual_observation = serde_json::to_value(&actual.observations[index]).unwrap();
                for field in OBSERVATION_FIELDS {
                    if expected[field] != actual_observation[field] {
                        failures.push(format!(
                            "C54 row {}/{stage}/{field}: Go={} Rust={}",
                            actual.case_id, expected[field], actual_observation[field]
                        ));
                    }
                }
            }
            failures
        }

        #[test]
        #[ignore = "requires C54_GO_JSONL and fresh C54_RUST_JSONL; controlled Forwarder source contract only"]
        fn reference_layer_matrix() {
            let input = std::fs::read_to_string(
                std::env::var_os("C54_GO_JSONL").expect("original Go JSONL required"),
            )
            .unwrap();
            let expected = read_rows(&input).expect("strict C54 Go schema/inventory");
            let mut output = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(std::env::var_os("C54_RUST_JSONL").expect("fresh Rust JSONL path required"))
                .unwrap();
            // Go observations must not steer or initialize Rust state.
            let actual: Vec<Row> = scenarios()
                .into_iter()
                .enumerate()
                .map(|(id, scenario)| run(id, scenario))
                .collect();
            for row in &actual {
                writeln!(output, "{}", serde_json::to_string(row).unwrap()).unwrap();
            }
            output.sync_all().unwrap();
            // Persist ALL 90 x 20 actual observations before comparing any values.
            let failures: Vec<String> = expected
                .iter()
                .zip(&actual)
                .flat_map(|(expected, actual)| differences(expected, actual))
                .collect();
            println!(
                "C54_SOURCE rows={} stages={} field_mismatches={}",
                actual.len(),
                actual.len() * STAGES.len(),
                failures.len()
            );
            assert!(failures.is_empty(), "{}", failures.join("\n"));
        }

        // Synthetic schema fixtures ONLY, never accepted as Go differential evidence.
        fn schema_fixture() -> Vec<Value> {
            scenarios()
                .into_iter()
                .enumerate()
                .map(|(id, scenario)| {
                    json!({
                        "schema": 1, "case_id": id, "scenario": scenario,
                        "observations": STAGES.map(|stage| json!({
                            "stage": stage, "max_subscribed_spatial": 2, "max_spatial": 2,
                            "current_spatial": -1, "target_spatial": -1, "started": false,
                            "reference_spatial": null, "muted": false, "pub_muted": false
                        }))
                    })
                })
                .collect()
        }

        fn fixture_jsonl(rows: &[Value]) -> String {
            rows.iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n")
                + "\n"
        }

        #[test]
        fn reader_rejects_unknown_missing_and_duplicate_fields() {
            let fixture = schema_fixture();
            assert_eq!(read_rows(&fixture_jsonl(&fixture)).unwrap().len(), 90);
            for pointer in ["", "/scenario", "/observations/0"] {
                let object = fixture[0].pointer(pointer).unwrap().as_object().unwrap();
                for key in object.keys() {
                    let mut bad = fixture.clone();
                    bad[0]
                        .pointer_mut(pointer)
                        .unwrap()
                        .as_object_mut()
                        .unwrap()
                        .remove(key);
                    assert!(
                        read_rows(&fixture_jsonl(&bad)).is_err(),
                        "missing {pointer}/{key}"
                    );
                }
                let mut bad = fixture.clone();
                bad[0]
                    .pointer_mut(pointer)
                    .unwrap()
                    .as_object_mut()
                    .unwrap()
                    .insert("unexpected".into(), json!(0));
                assert!(
                    read_rows(&fixture_jsonl(&bad)).is_err(),
                    "unknown {pointer}"
                );
            }
            let input = fixture_jsonl(&fixture);
            for (key, value) in [
                ("schema", "1"),
                ("codec_mode", "\"h264-simulcast\""),
                ("started", "false"),
            ] {
                let duplicate = input.replacen(
                    &format!("\"{key}\":"),
                    &format!("\"{key}\":{value},\"{key}\":"),
                    1,
                );
                assert!(read_rows(&duplicate).is_err(), "duplicate {key}");
            }
        }

        #[test]
        fn reader_rejects_bad_inventory_order_types_and_ranges() {
            let fixture = schema_fixture();
            for (pointer, value) in [
                ("/schema", json!(2)),
                ("/schema", json!(1.0)),
                ("/case_id", json!(1)),
                ("/case_id", json!(-1)),
                ("/scenario/codec_mode", json!("vp9-svc")),
                ("/scenario/reference", json!(1)),
                ("/scenario/max_spatial", json!(0)),
                ("/scenario/report", json!("matching-zero")),
                ("/scenario/report_layer", json!(0)),
                ("/observations/0/stage", json!("first_packet")),
            ] {
                let mut bad = fixture.clone();
                *bad[0].pointer_mut(pointer).unwrap() = value;
                assert!(read_rows(&fixture_jsonl(&bad)).is_err(), "{pointer}");
            }
            for field in [
                "max_subscribed_spatial",
                "max_spatial",
                "current_spatial",
                "target_spatial",
                "reference_spatial",
            ] {
                for value in [
                    json!(-2),
                    json!(3),
                    json!(0.0),
                    json!("0"),
                    json!(true),
                    json!([]),
                    json!({}),
                ] {
                    let mut bad = fixture.clone();
                    bad[0]["observations"][0][field] = value;
                    assert!(read_rows(&fixture_jsonl(&bad)).is_err(), "{field}");
                }
                let mut nullable = fixture.clone();
                nullable[0]["observations"][0][field] = Value::Null;
                assert_eq!(
                    read_rows(&fixture_jsonl(&nullable)).is_ok(),
                    field == "reference_spatial"
                );
            }
            for field in ["started", "muted", "pub_muted"] {
                for value in [json!(0), json!("false"), Value::Null] {
                    let mut bad = fixture.clone();
                    bad[0]["observations"][0][field] = value;
                    assert!(read_rows(&fixture_jsonl(&bad)).is_err(), "{field}");
                }
            }
            for value in [Value::Null, json!(-1), json!(0), json!(2)] {
                let mut valid = fixture.clone();
                valid[0]["observations"][0]["reference_spatial"] = value;
                assert!(read_rows(&fixture_jsonl(&valid)).is_ok());
            }
            let mut bad = fixture.clone();
            bad.swap(0, 1);
            assert!(read_rows(&fixture_jsonl(&bad)).is_err());
            bad = fixture.clone();
            bad[1] = bad[0].clone();
            assert!(read_rows(&fixture_jsonl(&bad)).is_err());
            assert!(read_rows(&fixture_jsonl(&fixture[..89])).is_err());
            bad = fixture.clone();
            bad.push(fixture[0].clone());
            assert!(read_rows(&fixture_jsonl(&bad)).is_err());
            for count in [19, 21] {
                bad = fixture.clone();
                let observations = bad[0]["observations"].as_array_mut().unwrap();
                observations.resize(count, observations[0].clone());
                assert!(read_rows(&fixture_jsonl(&bad)).is_err());
            }
            bad = fixture.clone();
            bad[0]["observations"].as_array_mut().unwrap().swap(0, 1);
            assert!(read_rows(&fixture_jsonl(&bad)).is_err());
            for input in [
                String::new(),
                "not JSON\n".into(),
                format!("\n{}", fixture_jsonl(&fixture)),
            ] {
                assert!(read_rows(&input).is_err());
            }
        }

        #[test]
        fn observation_uses_persistent_clock_not_resync_sequence_state() {
            let scenario = scenarios().remove(0);
            let now = Instant::now();
            let mut down = new_down(&scenario, now);
            assert!(!observe(&down, "constructed").started);
            assert_eq!(observe(&down, "constructed").reference_spatial, None);
            packet(&mut down, &scenario, 1, now);
            assert!(down.seq_source.is_some());
            down.stop_forwarding();
            assert!(down.seq_source.is_none());
            assert_eq!(down.target, scenario.reference, "Resync preserves target");
            assert!(observe(&down, "resync").started);
            assert_eq!(
                observe(&down, "resync").reference_spatial,
                Some(scenario.reference)
            );
            down.set_pub_muted(true, now);
            assert_eq!(
                observe(&down, "pub_muted").target_spatial,
                scenario.reference
            );
            down.set_pub_muted(false, now);
            down.restart_receiver();
            let restarted = observe(&down, "restart");
            assert!(restarted.started);
            assert_eq!(restarted.reference_spatial, Some(-1));
            assert_eq!(restarted.target_spatial, -1);
            let fresh = new_down(&scenario, now);
            assert!(!observe(&fresh, "fresh").started);
            assert_eq!(observe(&fresh, "fresh").reference_spatial, None);
        }

        #[test]
        fn comparison_checks_every_observation_field_including_null_and_target() {
            let expected = read_rows(&fixture_jsonl(&schema_fixture()))
                .unwrap()
                .remove(0);
            assert!(differences(&expected, &expected).is_empty());
            for field in OBSERVATION_FIELDS {
                let mut changed = serde_json::to_value(&expected).unwrap();
                changed["observations"][0][field] = match field {
                    "stage" => json!("before_first_packet"),
                    "started" | "muted" | "pub_muted" => json!(true),
                    "reference_spatial" => json!(-1),
                    _ => json!(0),
                };
                let actual: Row = serde_json::from_value(changed).unwrap();
                let failures = differences(&expected, &actual);
                assert_eq!(failures.len(), 1, "{field}");
                assert!(failures[0].contains(&format!("/{field}:")));
            }
        }
    }

    #[test]
    fn target_selection_with_fallback_and_overshoot() {
        assert_eq!(select_target(2, [true, true, true], [true; 3]), 2);
        assert_eq!(select_target(2, [true, true, false], [true; 3]), 1);
        assert_eq!(select_target(0, [false, true, true], [true; 3]), 2);
        assert_eq!(select_target(1, [false, false, false], [true; 3]), 1);
        assert_eq!(select_target(-1, [true, true, true], [true; 3]), -1);
        assert_eq!(select_target(2, [false; 3], [false; 3]), -1);
        assert_eq!(select_target(2, [false; 3], [true, false, false]), 0);
        assert_eq!(select_target(2, [true; 3], [true, false, false]), 0);
    }

    #[test]
    fn switching_requires_keyframes() {
        assert_eq!(switch_on_keyframe(0, 2, 2, false, true), None);
        assert_eq!(switch_on_keyframe(0, 2, 1, true, true), Some(1));
        assert_eq!(switch_on_keyframe(2, 0, 1, true, true), Some(1));
        assert_eq!(switch_on_keyframe(2, 0, 0, true, true), Some(0));
        assert_eq!(switch_on_keyframe(1, 1, 0, true, true), None);
        assert_eq!(switch_on_keyframe(-1, 2, 0, true, false), None);
        assert_eq!(switch_on_keyframe(-1, 2, 0, true, true), Some(0));
        assert_eq!(switch_on_keyframe(-1, 2, 2, true, false), Some(2));
    }

    #[test]
    fn temporal_only_vp9_target_does_not_request_unrelated_keyframes() {
        let mut d = video(Instant::now());
        d.max_spatial = 2;
        d.refresh_target([true; 3], [true; 3]);
        assert!(d.wants_keyframe());
        assert_eq!(
            d.on_vp9_frame(2, 0, true, 3, false, true, true),
            Forward::Send { resumed: true }
        );
        assert!(
            !d.wants_keyframe(),
            "original Base.CheckSync spatial is already locked"
        );
        assert_eq!(
            d.forwarded_temporal_cap(),
            0,
            "no invented temporal upgrade"
        );
        assert_eq!(
            d.on_vp9_frame(2, 3, false, 3, true, true, true),
            Forward::Send { resumed: false }
        );
        assert_eq!(
            d.forwarded_temporal_cap(),
            3,
            "actual U+B permits the fourth temporal layer"
        );
        assert!(!d.wants_keyframe());
        d.max_spatial = 0;
        d.refresh_target([true; 3], [true; 3]);
        assert!(
            d.wants_keyframe(),
            "actual spatial mismatch still requests synchronization"
        );
    }

    #[test]
    fn late_original_packet_preserves_timestamp_without_rewinding_resume_anchor() {
        let t = Instant::now();
        let mut d = video(t);
        let time = |v| MediaTime::new(v, Frequency::NINETY_KHZ);
        assert_eq!(
            d.out_original_packet_time(time(30000), t, false).numer(),
            30000
        );
        assert_eq!(
            d.out_original_packet_time(time(48000), t + Duration::from_millis(200), false)
                .numer(),
            48000
        );
        assert_eq!(
            d.out_original_packet_time(time(45000), t + Duration::from_millis(300), true)
                .numer(),
            45000
        );
        d.stop_forwarding();
        assert_eq!(
            d.out_original_packet_time(time(500), t + Duration::from_millis(400), false)
                .numer(),
            66000,
            "resume anchors to latest forward output, not the late repair"
        );
    }

    #[test]
    fn forwarding_locks_on_target_and_drops_other_layers() {
        let t0 = Instant::now();
        let mut d = video(t0);
        d.max_spatial = 2;
        assert!(d.refresh_target([true, true, true], [true; 3]));
        assert!(d.wants_keyframe());
        assert_eq!(d.on_frame(2, false, t0), Forward::Drop);
        assert_eq!(d.on_frame(0, true, t0), Forward::Drop);
        assert_eq!(d.on_frame(2, true, t0), Forward::Send { resumed: true });
        assert!(!d.wants_keyframe());
        assert_eq!(d.on_frame(2, false, t0), Forward::Send { resumed: false });
        assert_eq!(d.on_frame(1, true, t0), Forward::Drop);
        // downgrade on the lower layer's keyframe; old layer flows until then
        d.max_spatial = 0;
        assert!(d.refresh_target([true, true, true], [true; 3]));
        assert_eq!(d.on_frame(2, false, t0), Forward::Send { resumed: false });
        assert_eq!(d.on_frame(0, true, t0), Forward::Send { resumed: false });
        assert_eq!(d.current, 0);
    }

    #[test]
    fn svc_keeps_dependency_complete_prefix_and_gates_switches() {
        let now = Instant::now();
        let mut d = video(now);
        d.max_spatial = 2;
        d.refresh_target([true; 3], [true; 3]);
        assert_eq!(d.on_svc_frame(2, 0, false, 2), Forward::Drop);
        assert_eq!(
            d.on_svc_frame(2, 0, true, 2),
            Forward::Send { resumed: true }
        );
        assert_eq!(d.current, 2);
        d.max_spatial = 0;
        d.refresh_target([true; 3], [true; 3]);
        assert_eq!(
            d.on_svc_frame(2, 2, false, 0),
            Forward::Send { resumed: false }
        );
        assert_eq!(
            d.current, 2,
            "wait for keyframe before dropping spatial dependencies"
        );
        assert_eq!(
            d.on_svc_frame(2, 0, true, 0),
            Forward::Send { resumed: false }
        );
        assert_eq!(d.current, 0);
        assert_eq!(d.on_svc_frame(2, 1, false, 0), Forward::Drop);
        assert_eq!(
            d.on_svc_frame(2, 0, false, 0),
            Forward::Send { resumed: false }
        );
        d.max_spatial = 2;
        d.refresh_target([true; 3], [true; 3]);
        assert_eq!(
            d.on_svc_frame(2, 2, false, 2),
            Forward::Drop,
            "temporal upgrade also waits for keyframe"
        );
        assert_eq!(
            d.on_svc_frame(2, 0, true, 2),
            Forward::Send { resumed: false }
        );
        assert_eq!(
            d.on_svc_frame(2, 2, false, 2),
            Forward::Send { resumed: false }
        );
        d.bw_paused = true;
        d.refresh_target([true; 3], [true; 3]);
        assert_eq!(d.on_svc_frame(2, 0, true, 2), Forward::Drop);
    }

    #[test]
    fn rtp_time_is_continuous_across_switch() {
        let t0 = Instant::now();
        let mut d = video(t0);
        let f = Frequency::NINETY_KHZ;
        // first frame passes through unchanged
        assert_eq!(d.out_time(MediaTime::new(1000, f), t0).numer(), 1000);
        assert_eq!(
            d.out_time(MediaTime::new(4000, f), t0 + Duration::from_millis(33))
                .numer(),
            4000
        );
        // switch to a layer with an unrelated time base
        d.resync = true;
        let t = d.out_time(MediaTime::new(9_000_000, f), t0 + Duration::from_millis(66));
        assert_eq!(t.numer(), 4000 + 2970);
        let t = d.out_time(MediaTime::new(9_003_000, f), t0 + Duration::from_millis(99));
        assert_eq!(t.numer(), 4000 + 2970 + 3000);
    }

    #[test]
    fn settings_debounce_and_immediate_enable() {
        let t0 = Instant::now();
        let mut d = video(t0);
        let mut s = TrackSettings {
            quality: VideoQuality::Medium,
            ..Default::default()
        };
        assert!(!d.update_settings(s.clone(), t0));
        assert!(!d.poll_settings(t0 + Duration::from_millis(50)));
        assert!(d.poll_settings(t0 + SETTINGS_DEBOUNCE));
        assert_eq!(d.settings.quality, VideoQuality::Medium);
        s.disabled = true;
        d.update_settings(s.clone(), t0);
        assert!(d.poll_settings(t0 + SETTINGS_DEBOUNCE));
        s.disabled = false;
        assert!(d.update_settings(s, t0));
    }

    #[test]
    fn adaptive_stream_starts_low() {
        let t0 = Instant::now();
        let d = DownTrack::new(
            spec(true, TrackSettings::default()),
            TrackKind::Video,
            0,
            false,
            "video/VP8",
            t0,
        );
        assert_eq!(d.settings.quality, VideoQuality::Low);
        let s = TrackSettings {
            disabled: true,
            width: 100,
            ..Default::default()
        };
        let d = DownTrack::new(spec(true, s), TrackKind::Video, 0, false, "video/VP8", t0);
        assert!(!d.settings.disabled);
    }

    #[test]
    fn audio_mute_schedules_silence() {
        let t0 = Instant::now();
        let mut d = DownTrack::new(
            spec(false, TrackSettings::default()),
            TrackKind::Audio,
            0,
            false,
            "audio/opus",
            t0,
        );
        d.refresh_target([false; 3], [true; 3]);
        assert_eq!(d.on_frame(0, false, t0), Forward::Send { resumed: false });
        d.out_time(MediaTime::new(48_000, Frequency::FORTY_EIGHT_KHZ), t0);
        d.set_pub_muted(true, t0);
        assert_eq!(d.silence_left, SILENCE_FRAMES);
        assert_eq!(d.on_frame(0, false, t0), Forward::Drop);
        let t = d
            .synth_time(SILENCE_INTERVAL, t0 + SILENCE_INTERVAL)
            .unwrap();
        assert_eq!(t.numer(), 48_960);
        d.set_pub_muted(false, t0);
        assert_eq!(d.silence_left, 0);
        d.refresh_target([false; 3], [true; 3]);
        assert_eq!(d.on_frame(0, false, t0), Forward::Send { resumed: false });
    }
}

#[cfg(test)]
mod vp9_temporal_source_tests {
    use super::*;
    #[test]
    fn source_end_flag_downshifts_without_keyframe_and_up_requires_u_begin() {
        let now = Instant::now();
        let mut d = DownTrack::new(
            DownTrackSpec {
                subscriber: "PA_s".into(),
                publisher: "PA_p".into(),
                track_sid: "TR_v".into(),
                stream_id: "s".into(),
                settings: TrackSettings::default(),
                adaptive_stream: false,
            },
            TrackKind::Video,
            2,
            false,
            "video/VP9",
            now,
        );
        d.target = 2;
        assert!(matches!(
            d.on_vp9_frame(2, 0, true, 2, false, true, true),
            Forward::Send { .. }
        ));
        d.on_vp9_frame(2, 2, false, 2, true, true, true);
        assert_eq!(d.forwarded_temporal_cap(), 2);
        d.on_vp9_frame(2, 0, false, 0, false, true, true);
        assert_eq!(
            d.forwarded_temporal_cap(),
            0,
            "fixed Go downshift uses E, not a future keyframe"
        );
        d.on_vp9_frame(2, 2, false, 2, false, true, true);
        assert_eq!(
            d.forwarded_temporal_cap(),
            0,
            "non-U packet cannot raise the decode target"
        );
        assert_eq!(
            d.vp9_forwarded_spatial_cap(2),
            1,
            "lower spatial T2 components still forwarded under the original lexicographic rule"
        );
        d.on_vp9_frame(2, 2, false, 2, true, true, true);
        assert_eq!(
            d.forwarded_temporal_cap(),
            2,
            "actual U+B packet resumes temporal layer"
        );
        d.target = 0;
        d.on_vp9_frame(2, 0, false, 0, false, true, true);
        assert_eq!(d.current, 0);
        assert_eq!(
            d.on_vp9_frame(0, 2, false, 0, false, true, true),
            Forward::Drop
        );
    }

    /// Fixed Go RTPMunger: upstream sequence numbers never received stay a gap
    /// downstream; SFU-dropped packets and source switches do not.
    #[test]
    fn upstream_gaps_are_preserved_but_sfu_drops_and_switches_are_not() {
        let now = Instant::now();
        let mut d = DownTrack::new(
            DownTrackSpec {
                subscriber: "PA_s".into(),
                publisher: "PA_p".into(),
                track_sid: "TR_v".into(),
                stream_id: "s".into(),
                settings: TrackSettings::default(),
                adaptive_stream: false,
            },
            TrackKind::Video,
            0,
            false,
            "video/VP8",
            now,
        );
        let q: Option<str0m::media::Rid> = Some("q".into());
        let f: Option<str0m::media::Rid> = Some("f".into());
        const PT: Pt = Pt::new_with_value(96);
        // Nothing forwarded yet: frames of no source do not start tracking.
        d.observe_source_seq(q, PT, 1, 3, false, false);
        assert_eq!((d.seq_source, d.pending_seq_gap), (None, 0));
        // First forwarded frame starts tracking without a gap.
        d.observe_source_seq(q, PT, 10, 12, true, true);
        assert_eq!((d.seq_source, d.pending_seq_gap), (Some(((q, PT), 12)), 0));
        // Contiguous frame: no gap.
        d.observe_source_seq(q, PT, 13, 14, true, false);
        assert_eq!(d.pending_seq_gap, 0);
        // 15..=19 lost upstream.
        d.observe_source_seq(q, PT, 20, 21, true, false);
        assert_eq!(d.pending_seq_gap, 5);
        d.pending_seq_gap = 0; // consumed by the write
        // A frame the SFU drops (temporal filtering) moves the position: no gap for it.
        d.observe_source_seq(q, PT, 22, 23, false, false);
        d.observe_source_seq(q, PT, 24, 24, true, false);
        assert_eq!(d.pending_seq_gap, 0);
        // A gap before a dropped frame is still upstream loss (Go munges, then PacketDropped).
        d.observe_source_seq(q, PT, 27, 27, false, false);
        d.observe_source_seq(q, PT, 28, 28, true, false);
        assert_eq!(d.pending_seq_gap, 2);
        d.pending_seq_gap = 0;
        // Duplicates and older frames change nothing.
        d.observe_source_seq(q, PT, 20, 21, true, false);
        assert_eq!((d.seq_source, d.pending_seq_gap), (Some(((q, PT), 28)), 0));
        // Frames of another simulcast layer are ignored until it becomes the source.
        d.observe_source_seq(f, PT, 500, 505, false, false);
        assert_eq!(d.seq_source, Some(((q, PT), 28)));
        // Switching source (SetLastSnTs / UpdateSnTsOffsets) continues without a gap.
        d.observe_source_seq(f, PT, 900, 903, true, true);
        assert_eq!((d.seq_source, d.pending_seq_gap), (Some(((f, PT), 903)), 0));
        // Resume after a pause on the same source: no gap either.
        d.observe_source_seq(f, PT, 2000, 2001, true, true);
        assert_eq!(
            (d.seq_source, d.pending_seq_gap),
            (Some(((f, PT), 2001)), 0)
        );
        // A half-sequence-space jump is a source restart, not loss.
        d.observe_source_seq(f, PT, 2001 + 40_000, 2001 + 40_001, true, false);
        assert_eq!(d.pending_seq_gap, 0);

        // A backup codec on the same rid is another upstream stream: no gap across it.
        d.observe_source_seq(f, PT, 2002, 2002, true, false);
        let backup = Pt::new_with_value(98);
        assert!(
            !d.observe_source_seq(f, backup, 7, 7, true, false),
            "new source"
        );
        assert_eq!(
            (d.seq_source, d.pending_seq_gap),
            (Some(((f, backup), 7)), 0)
        );
        // Continuing the same source reports it, so an undeliverable frame can be
        // counted as a gap (Go wrote it into a transport that could not deliver it).
        assert!(d.observe_source_seq(f, backup, 8, 9, true, false));
        // Stopping forwarding clears tracking.
        d.stop_forwarding();
        assert_eq!((d.seq_source, d.pending_seq_gap), (None, 0));
    }
}
