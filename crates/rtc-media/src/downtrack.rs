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

    /// Dynacast demand (spatial): `max(max_layer, current)`, -1 when the subscriber
    /// disabled the track. Publisher mute and BW pause do not change demand (§3.1).
    pub(crate) fn demand_spatial(&self) -> i32 {
        if self.sub_muted {
            return INVALID_SPATIAL;
        }
        self.max_spatial.max(self.current)
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
            self.target = INVALID_SPATIAL;
            if !self.is_video() && was_flowing && self.last_out.is_some() {
                self.silence_left = SILENCE_FRAMES;
                self.silence_next = Some(now);
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
