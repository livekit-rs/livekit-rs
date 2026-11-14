//! A published (upstream) track: bound remote media on a publisher transport.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use rtc_core::config::PliThrottle;
use rtc_core::ids::{ParticipantSid, TrackSid, TransportId};
use rtc_core::media_api::PublishedTrackSpec;
use rtc_core::model::{TrackKind, TrackSource, VideoLayer, VideoQuality};
use str0m::media::{MediaTime, Mid, Rid};

use crate::audio_level::{AudioLevel, AudioLevelParams};
use crate::dynacast::Dynacast;
use crate::layers;
use crate::quality::Scorer;

/// A layer counts as available while frames arrived within this window (a coarse
/// stand-in for upstream's stream tracker, §2.4).
const LAYER_AVAILABLE_WINDOW: Duration = Duration::from_millis(1500);
static COST_GENERATION: AtomicU64 = AtomicU64::new(1);

fn next_cost_generation() -> u64 {
    COST_GENERATION.fetch_add(1, Ordering::Relaxed)
}

#[derive(Default, Clone)]
pub(crate) struct LayerStat {
    pub last_seen: Option<Instant>,
    /// Actual rid seen for this spatial layer (used for PLI targeting).
    pub rid: Option<Rid>,
    window_bytes: u64,
    dd_window_bytes: [u64; 4],
    dd_bitrates: [u64; 4],
    /// Bitrate of the last complete measurement window (bps).
    pub bitrate_bps: u64,
    last_pli: Option<Instant>,
}

#[cfg(test)]
impl LayerStat {
    pub(crate) fn last_pli(&self) -> Option<Instant> {
        self.last_pli
    }

    pub(crate) fn clear_last_pli(&mut self) {
        self.last_pli = None;
    }
}

pub(crate) struct UpTrack {
    pub media_sizes: crate::video_dimensions::VideoDimensions,
    /// Fixed Go StreamTrackerManager packet trackers per simulcast layer
    /// (non-SVC video): layer availability for allocation and switching.
    trackers: [crate::stream_tracker::PacketTracker; 3],
    frame_trackers: [Option<crate::stream_tracker::FrameTracker>; 3],
    /// `StreamTrackerManager.maxExpectedLayer`: raising it resets the newly
    /// expected trackers so their next packet makes them available at once.
    max_expected: i32,
    /// Set once the packet trackers are fed (non-DD simulcast/single-stream video).
    /// Descriptor-driven sources keep their own availability path: fixed Go uses
    /// StreamTrackerDependencyDescriptor for them, not the packet tracker.
    packet_tracked: bool,
    /// Last received media packet count per layer stream and the last poll.
    tracker_counts: [Option<u64>; 3],
    /// Configured packet tracker per layer (`None`: no tracker for that layer).
    tracker_specs: Option<rtc_core::config::VideoStreamTrackers>,
    tracker_polled_at: Option<Instant>,
    pub sid: TrackSid,
    pub publisher: ParticipantSid,
    pub transport: TransportId,
    pub mid: Mid,
    pub kind: TrackKind,
    pub spec: PublishedTrackSpec,
    pub mime: String,
    /// `a=simulcast` send rids from the offer, `~` stripped, offer order.
    pub sdp_rids: Vec<String>,
    /// Layers with presence-aware spatial/rid (§1.2). Empty for audio.
    pub layers: Vec<VideoLayer>,
    pub started: bool,
    pub muted: bool,
    pub stats: [LayerStat; 3],
    window_start: Instant,
    cost_completed_at: [Option<Instant>; 3],
    bitrate_intervals: Option<[Option<Duration>; 3]>,
    bitrate_started_at: [Option<Instant>; 3],
    dd_bitrate_aggregated: bool,
    dd_active_spatial: Option<usize>,
    bitrate_report_pending: bool,
    cost_generation: u64,
    /// Down track slab indices subscribed to this track.
    pub subscribers: Vec<usize>,
    pub audio_level: Option<AudioLevel>,
    last_audio_time: Option<MediaTime>,
    pub dynacast: Option<Dynacast>,
    pub scorer: Scorer,
    /// Last cumulative ingress packet counter per rid (stats deltas).
    pub ingress_packets: Vec<(Option<Rid>, u64)>,
    pub stopped_reported: bool,
    pub svc: bool,
    pub av1: crate::av1::Receiver,
    svc_base_rate: crate::svc::TemporalRate,
    pub(crate) av1_packets: crate::av1::PacketReceiver,
    av1_packet_t0_time: Option<MediaTime>,
    pub(crate) dd_source: crate::dd_fallback::Source,
    pub(crate) one_dd: crate::one_dd_forward::Sources,
    pub(crate) source_rates: crate::temporal_packet::Receivers,
}

impl UpTrack {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        spec: PublishedTrackSpec,
        publisher: ParticipantSid,
        transport: TransportId,
        mid: Mid,
        kind: TrackKind,
        mime: String,
        sdp_rids: Vec<String>,
        audio: &AudioLevelParams,
        pause_delay: Duration,
        now: Instant,
    ) -> Self {
        let mut t = Self {
            media_sizes: Default::default(),
            trackers: crate::stream_tracker::VIDEO.map(crate::stream_tracker::PacketTracker::new),
            frame_trackers: [None; 3],
            max_expected: layers::MAX_SPATIAL,
            packet_tracked: false,
            tracker_counts: [None; 3],
            tracker_specs: None,
            tracker_polled_at: None,
            sid: spec.sid.clone(),
            publisher,
            transport,
            mid,
            kind,
            mime: mime.clone(),
            sdp_rids,
            layers: Vec::new(),
            started: false,
            muted: spec.muted,
            stats: Default::default(),
            window_start: now,
            cost_completed_at: [None; 3],
            bitrate_intervals: None,
            bitrate_started_at: [None; 3],
            dd_bitrate_aggregated: false,
            dd_active_spatial: None,
            bitrate_report_pending: false,
            cost_generation: next_cost_generation(),
            subscribers: Vec::new(),
            audio_level: None,
            last_audio_time: None,
            dynacast: None,
            scorer: Scorer::new(&mime),
            ingress_packets: Vec::new(),
            stopped_reported: false,
            svc: false,
            av1: Default::default(),
            svc_base_rate: Default::default(),
            av1_packets: Default::default(),
            av1_packet_t0_time: None,
            dd_source: Default::default(),
            one_dd: Default::default(),
            source_rates: Default::default(),
            spec,
        };
        if kind == TrackKind::Audio && matches!(t.spec.source, TrackSource::Microphone) {
            t.audio_level = Some(AudioLevel::new(audio));
        }
        if kind == TrackKind::Video {
            t.dynacast = Some(Dynacast::new(
                crate::codecs::dynacast_codec_name(&mime),
                pause_delay,
                now,
                t.muted,
            ));
        }
        t.rebuild_layers(now);
        t.scorer.set_muted(t.muted);
        t
    }

    /// Layers from the spec (primary codec layers, else track layers), rewritten
    /// with the SDP rids; synthesized for SDP-only publishes.
    pub(crate) fn rebuild_layers(&mut self, now: Instant) {
        if self.kind != TrackKind::Video {
            self.layers.clear();
            return;
        }
        let codec_layers = self
            .spec
            .codecs
            .iter()
            .find(|c| c.mime_type.is_empty() || c.mime_type.eq_ignore_ascii_case(&self.mime))
            .map(|c| c.layers.clone())
            .filter(|l| !l.is_empty());
        let mut l = codec_layers.unwrap_or_else(|| self.spec.layers.clone());
        if l.is_empty() {
            l = layers::synthesize_layers(&self.sdp_rids);
        }
        layers::rewrite_layers(&mut l, &self.sdp_rids);
        // The observed RTP SSRC binding is sticky metadata (first-SSRC-wins, like
        // upstream SetLayerSsrcsForRid); synthesized layers carry ssrc=0, so carry
        // the bound SSRC/RTX across a rebuild instead of dropping it.
        for new in l.iter_mut() {
            if let Some(old) = self.layers.iter().find(|o| o.quality == new.quality) {
                if new.ssrc == 0 {
                    new.ssrc = old.ssrc;
                }
                if new.repair_ssrc == 0 {
                    new.repair_ssrc = old.repair_ssrc;
                }
            }
        }
        // Bitrate/dimension metadata can change without changing the encoding
        // identity. Only actual RID/spatial/SSRC bindings invalidate history.
        let same_mapping = self.layers.len() == l.len()
            && self.layers.iter().zip(&l).all(|(old, new)| {
                old.quality == new.quality
                    && old.spatial_layer == new.spatial_layer
                    && old.rid == new.rid
                    && old.ssrc == new.ssrc
                    && old.repair_ssrc == new.repair_ssrc
            });
        if !same_mapping {
            self.reset_cost_measurements(now);
        }
        self.layers = l;
    }

    pub(crate) fn detected_source_restart(&mut self, audio: &AudioLevelParams, now: Instant) {
        self.stats = Default::default();
        self.ingress_packets.clear();
        self.reset_cost_measurements(now);
        self.av1 = Default::default();
        self.svc = false;
        self.svc_base_rate = Default::default();
        self.av1_packets.restart();
        self.dd_source = Default::default();
        self.av1_packet_t0_time = None;
        // The authenticated native source identity resets only its own encoding
        // lazily; a restart on RID q must not discard h/f measured rates.
        self.last_audio_time = None;
        if self.audio_level.is_some() {
            self.audio_level = Some(AudioLevel::new(audio));
        }
    }

    fn reset_cost_measurements(&mut self, now: Instant) {
        self.dd_bitrate_aggregated = false;
        self.reset_bitrate_epoch(now);
    }

    fn reset_bitrate_epoch(&mut self, now: Instant) {
        self.cost_generation = next_cost_generation();
        self.cost_completed_at = [None; 3];
        self.bitrate_started_at = [None; 3];
        self.dd_active_spatial = None;
        self.bitrate_report_pending = false;
        self.window_start = now;
        for layer in &mut self.stats {
            layer.window_bytes = 0;
            layer.dd_window_bytes = [0; 4];
            layer.dd_bitrates = [0; 4];
            layer.bitrate_bps = 0;
        }
    }

    pub(crate) fn update_spec(&mut self, spec: PublishedTrackSpec, now: Instant) {
        self.spec = spec;
        self.rebuild_layers(now);
    }

    pub(crate) fn rebind_source(
        &mut self,
        transport: TransportId,
        mid: Mid,
        rids: Vec<String>,
        mime: &str,
        spec: PublishedTrackSpec,
        now: Instant,
    ) {
        if self.transport != transport || self.mid != mid || self.sdp_rids != rids {
            self.one_dd = Default::default();
            self.media_sizes = Default::default();
            self.reset_cost_measurements(now);
        }
        self.transport = transport;
        self.mid = mid;
        self.sdp_rids = rids;
        self.stopped_reported = false;
        self.set_mime(mime, now);
        self.update_spec(spec, now);
    }

    pub(crate) fn set_mime(&mut self, mime: &str, now: Instant) {
        if self.mime != mime {
            self.reset_cost_measurements(now);
            self.mime = mime.to_string();
            self.svc = false;
            self.av1 = Default::default();
            self.svc_base_rate = Default::default();
            self.av1_packets = Default::default();
            self.dd_source = Default::default();
            self.one_dd = Default::default();
            self.av1_packet_t0_time = None;
            self.source_rates = Default::default();
            self.scorer.set_mime(mime);
            if let Some(d) = &mut self.dynacast {
                d.set_codec(crate::codecs::dynacast_codec_name(mime));
            }
            self.rebuild_layers(now);
        }
    }

    pub(crate) fn spatial_for_rid(&self, rid: Option<Rid>) -> i32 {
        match rid {
            None => 0,
            Some(r) => layers::spatial_for_rid(&self.layers, &r),
        }
    }

    /// Bind the observed RTP SSRC/RTX to its spatial layer, first-SSRC-wins like
    /// upstream `MediaTrackReceiver.SetLayerSsrcsForRid`: a layer already bound to
    /// a nonzero SSRC is never overridden. Returns true when a binding changed so
    /// the room can refresh `TrackInfo.layers`/`codecs[].layers`.
    pub(crate) fn observe_layer_ssrc(&mut self, spatial: i32, ssrc: u32, repair_ssrc: u32) -> bool {
        if self.kind != TrackKind::Video || ssrc == 0 {
            return false;
        }
        let Some(layer) = self.layers.iter_mut().find(|l| l.spatial_layer == spatial) else {
            return false;
        };
        if layer.ssrc != 0 {
            return false;
        }
        layer.ssrc = ssrc;
        if repair_ssrc != 0 {
            layer.repair_ssrc = repair_ssrc;
        }
        true
    }

    /// Record frame availability and its cost when no packet path owns the cost.
    pub(crate) fn on_frame(&mut self, spatial: i32, rid: Option<Rid>, len: usize, now: Instant) {
        self.observe_frame(spatial, rid, now);
        self.record_bitrate_bytes(spatial, len, now);
    }

    pub(crate) fn observe_frame(&mut self, spatial: i32, rid: Option<Rid>, now: Instant) {
        let s = &mut self.stats[spatial.clamp(0, 2) as usize];
        s.last_seen = Some(now);
        if rid.is_some() {
            s.rid = rid;
        }
    }

    pub(crate) fn record_bitrate_bytes(&mut self, spatial: i32, len: usize, now: Instant) {
        let i = spatial.clamp(0, 2) as usize;
        let record = match self.configured_bitrate_intervals() {
            // Once an aggregate DD worker exists, descriptor-free frames may
            // still be forwarded but do not feed that worker's counters.
            _ if self.dd_bitrate_aggregated => false,
            Some(intervals) => {
                if let Some(interval) = intervals[i] {
                    self.bitrate_started_at[i].get_or_insert(now);
                    self.close_configured_bitrate_window(i, interval, now);
                    true
                } else {
                    false
                }
            }
            None => true,
        };
        if record {
            let s = &mut self.stats[i];
            s.window_bytes = s.window_bytes.saturating_add(len as u64);
        }
    }

    pub(crate) fn ordinary_vp9_bitrate_layer(&self, rid: Option<Rid>) -> Option<i32> {
        if self.spec.encrypted || self.svc || !self.mime.eq_ignore_ascii_case("video/VP9") {
            return None;
        }
        self.one_dd_layer(&self.mime, rid).map(|layer| layer as i32)
    }

    /// SVC bandwidth is cumulative: target S1 needs S0 + S1. All layers
    /// share the same SSRC/RID; request PLI on that stream, never synthetic RIDs.
    pub(crate) fn on_svc_frame(
        &mut self,
        layers: crate::svc::Vp9Layers,
        keyframe: bool,
        time: MediaTime,
        now: Instant,
    ) {
        if !self.svc {
            self.reset_cost_measurements(now);
        }
        self.svc = true;
        for (spatial, end) in layers.ends.iter().enumerate() {
            if let Some(end) = end {
                self.on_frame(spatial as i32, None, *end, now);
            }
        }
        self.svc_base_rate.observe(layers.temporal, keyframe, time);
    }

    pub(crate) fn on_av1_frame(
        &mut self,
        picture: &crate::av1::Picture,
        time: MediaTime,
        now: Instant,
    ) {
        // Fixed Forwarder.DetermineCodec and ReceiverBase.forwardRTP use the
        // declared codec mode, not the presence of DD, for spatial selection.
        // ONE mode accounts/allocates the whole actual encoding as stream zero;
        // DD can still filter its temporal frames without cutting spatial OBUs.
        let stream_spatial =
            self.spec
                .codecs
                .iter()
                .find(|codec| codec.mime_type.eq_ignore_ascii_case(&self.mime))
                .is_some_and(|codec| {
                    matches!(codec.layer_mode,
                rtc_core::model::VideoLayerMode::OneSpatialLayerPerStream
                | rtc_core::model::VideoLayerMode::OneSpatialLayerPerStreamIncompleteRtcpSr)
                });
        let svc = !stream_spatial;
        if self.svc != svc {
            self.reset_cost_measurements(now);
        }
        self.svc = svc;
        if stream_spatial {
            self.on_frame(
                0,
                None,
                picture.cumulative_bytes(picture.highest as usize),
                now,
            );
        } else {
            for spatial in 0..=picture.highest {
                self.on_frame(
                    spatial,
                    None,
                    picture.cumulative_bytes(spatial as usize),
                    now,
                );
            }
        }
        self.svc_base_rate
            .observe(picture.temporal, picture.keyframe, time);
    }

    fn declared_layer_mode(&self, mime: &str) -> Option<rtc_core::model::VideoLayerMode> {
        self.spec
            .codecs
            .iter()
            .find(|codec| codec.mime_type.eq_ignore_ascii_case(mime))
            .map(|codec| codec.layer_mode)
    }

    pub(crate) fn unrecognized_vp9_layer_mode(&self) -> bool {
        self.mime.eq_ignore_ascii_case("video/VP9")
            && matches!(
                self.declared_layer_mode(&self.mime),
                Some(rtc_core::model::VideoLayerMode::Unrecognized(_))
            )
    }

    pub(crate) fn one_dd_layer(&self, mime: &str, rid: Option<Rid>) -> Option<usize> {
        use rtc_core::model::VideoLayerMode as Mode;
        if !matches!(
            self.declared_layer_mode(mime),
            Some(Mode::OneSpatialLayerPerStream | Mode::OneSpatialLayerPerStreamIncompleteRtcpSr)
        ) {
            return None;
        }
        match rid {
            None => Some(0),
            Some(rid) => self
                .layers
                .iter()
                .find(|layer| layer.rid == &*rid)
                .and_then(|layer| usize::try_from(layer.spatial_layer).ok())
                .filter(|layer| *layer < 3),
        }
    }

    pub(crate) fn one_dd_incomplete_sr(&self) -> bool {
        matches!(
            self.declared_layer_mode(&self.mime),
            Some(rtc_core::model::VideoLayerMode::OneSpatialLayerPerStreamIncompleteRtcpSr)
        )
    }

    /// SVC original packets share one DD domain. Declared ONE modes and
    /// independent RID encodings have different forwarding semantics.
    pub(crate) fn uses_av1_original_packets(&self, rid: Option<Rid>) -> bool {
        rid.is_none() && !self.spec.codecs.iter().find(|codec|codec.mime_type.eq_ignore_ascii_case(&self.mime)).is_some_and(|codec|matches!(codec.layer_mode,rtc_core::model::VideoLayerMode::OneSpatialLayerPerStream|rtc_core::model::VideoLayerMode::OneSpatialLayerPerStreamIncompleteRtcpSr))
    }

    pub(crate) fn on_av1_packet(
        &mut self,
        packet: &crate::av1::PacketParsed,
        time: MediaTime,
        payload_bytes: usize,
        logical_rtp_bytes: usize,
        now: Instant,
    ) {
        let stream_spatial = self
            .spec
            .codecs
            .iter()
            .find(|c| c.mime_type.eq_ignore_ascii_case(&self.mime))
            .is_some_and(|c| {
                matches!(
                    c.layer_mode,
                    rtc_core::model::VideoLayerMode::OneSpatialLayerPerStream
                        | rtc_core::model::VideoLayerMode::OneSpatialLayerPerStreamIncompleteRtcpSr
                )
            });
        let per_spatial_packet = self.unrecognized_vp9_layer_mode();
        let start_dd_bitrate = !stream_spatial
            && !per_spatial_packet
            && self.bitrate_intervals.is_some()
            && !self.dd_bitrate_aggregated;
        if self.svc == stream_spatial || start_dd_bitrate {
            self.reset_cost_measurements(now);
        }
        self.svc = !stream_spatial;
        if stream_spatial {
            self.on_frame(0, None, payload_bytes, now);
        } else if per_spatial_packet {
            // Go only uses the aggregate DD tracker for declared MULTIPLE.
            // Unknown VP9 observes the packet's own spatial layer.
            self.on_frame(i32::from(packet.spatial()), None, logical_rtp_bytes, now);
        } else {
            if start_dd_bitrate {
                self.dd_bitrate_aggregated = true;
            }
            let tracks_vp9_dd_activity = self.tracks_vp9_dd_activity();
            if self.dd_bitrate_aggregated {
                let active_spatial = packet
                    .active_spatial_ceiling()
                    .filter(|_| !self.muted && payload_bytes != 0);
                let targets_updated = if tracks_vp9_dd_activity {
                    active_spatial.is_some()
                } else {
                    packet.active_targets_updated
                };
                if tracks_vp9_dd_activity && let Some(spatial) = active_spatial {
                    self.dd_active_spatial = Some(spatial.min(self.stats.len() - 1));
                }
                // The source starts a paused worker to drain existing bytes.
                // After resume, only a fresh target update starts reporting.
                if (start_dd_bitrate && self.muted)
                    || (!self.muted && self.bitrate_started_at[0].is_none() && targets_updated)
                {
                    self.start_dd_bitrate_epoch(now);
                }
                // Rotate every layer before admitting a packet at the boundary.
                // Both timestamps and counters belong to this one DD epoch.
                self.close_due_bitrate_windows(now);
            }
            let intervals = self.configured_bitrate_intervals();
            // Go's DD tracker observes the whole logical RTP packet.
            let mut legacy_counted = [false; 3];
            for (spatial, temporal) in packet.cost_targets() {
                let Some(layer) = self.stats.get_mut(spatial) else {
                    continue;
                };
                layer.last_seen = Some(now);
                if intervals.is_some_and(|intervals| intervals[spatial].is_none()) {
                    continue;
                }
                if self.dd_bitrate_aggregated {
                    if !self.muted
                        && (!tracks_vp9_dd_activity || payload_bytes != 0)
                        && let Some(bytes) = layer.dd_window_bytes.get_mut(temporal)
                    {
                        *bytes = bytes.saturating_add(logical_rtp_bytes as u64);
                    }
                } else if !std::mem::replace(&mut legacy_counted[spatial], true) {
                    layer.window_bytes = layer.window_bytes.saturating_add(payload_bytes as u64);
                }
            }
        }
        if packet.temporal() == 0 && self.av1_packet_t0_time != Some(time) {
            self.av1_packet_t0_time = Some(time);
            self.svc_base_rate.observe(0, packet.attached(), time);
        }
    }

    pub(crate) fn rate_is_per_encoding(&self, rid: Option<Rid>) -> bool {
        if let Some(codec) = self
            .spec
            .codecs
            .iter()
            .find(|codec| codec.mime_type.eq_ignore_ascii_case(&self.mime))
        {
            return matches!(
                codec.layer_mode,
                rtc_core::model::VideoLayerMode::OneSpatialLayerPerStream
                    | rtc_core::model::VideoLayerMode::OneSpatialLayerPerStreamIncompleteRtcpSr
            );
        }
        rid.is_some() || !self.sdp_rids.is_empty() || self.source_rates.per_encoding()
    }
    pub(crate) fn temporal_rates(&self, spatial: usize) -> [f32; 4] {
        self.source_rates.rates(spatial)
    }
    pub(crate) fn svc_temporal_cap_for_spatial(&self, spatial: i32, fps: u32) -> u8 {
        if fps == 0 {
            return 3;
        }
        let rates = self.temporal_rates(spatial.max(0) as usize);
        let request = fps as f32 * 0.9_f32;
        rates
            .iter()
            .position(|rate| request <= *rate)
            .map_or(3, |layer| layer as u8)
    }
    pub(crate) fn svc_temporal_cap(&self, fps: u32) -> u8 {
        if fps == 0 {
            return 3;
        }
        let rates = self.temporal_rates(0);
        let requested = fps as f32 * 0.9_f32;
        (0..=3)
            .find(|layer| requested <= rates[*layer as usize])
            .unwrap_or(3)
    }

    /// Audio level observation from the RFC 6464 extension (`audio_level` is the
    /// str0m value: negative dBov).
    pub(crate) fn on_audio_level(
        &mut self,
        audio_level: Option<i8>,
        time: MediaTime,
        now: Instant,
    ) {
        let Some(al) = self.audio_level.as_mut() else {
            return;
        };
        // Go buffer_base.processAudioLevel calls the detector only when the
        // RFC6464 extension was present and parsed. Missing extensions must not
        // establish or advance its RTP timestamp baseline.
        let Some(level) = audio_level else { return };
        let duration_us = match self.last_audio_time {
            Some(prev) if time.frequency() == prev.frequency() && time.numer() > prev.numer() => {
                let ticks = time.numer() - prev.numer();
                (ticks.saturating_mul(1_000_000) / time.denom().max(1) as u64).min(1_000_000)
            }
            // Reordered/duplicate: ignore (audiolevel.go).
            Some(_) => return,
            // Fixed ObserveWithRTPTimestamp initializes its highest timestamp
            // to this first sample, so the first observed duration is zero.
            // Do not invent a 20ms packet before any RTP delta was received.
            None => 0,
        };
        self.last_audio_time = Some(time);
        let level = level.unsigned_abs().min(127);
        al.observe(level, duration_us, now);
    }

    /// A temporarily stopped layer can still be acquired. A layer the Go
    /// manager never creates (missing mode, layer entry or report interval)
    /// must not be selected by that acquisition fallback.
    pub(crate) fn layer_tracker_configured(&self, spatial: usize) -> bool {
        self.kind != TrackKind::Video
            || self.dd_bitrate_aggregated
            || self.tracker_specs.is_none()
            || self.tracker_spec(spatial).is_some()
    }

    pub(crate) fn configured_layers(&self) -> [bool; 3] {
        std::array::from_fn(|spatial| self.layer_tracker_configured(spatial))
    }

    fn tracks_vp9_dd_activity(&self) -> bool {
        self.dd_bitrate_aggregated
            && self.mime.eq_ignore_ascii_case("video/VP9")
            && matches!(
                self.declared_layer_mode(&self.mime),
                Some(rtc_core::model::VideoLayerMode::MultipleSpatialLayersPerStream)
            )
    }

    pub(crate) fn active_vp9_dd_layers(&self) -> Option<[bool; 3]> {
        (self.tracks_vp9_dd_activity() && self.dd_active_spatial.is_some())
            .then(|| std::array::from_fn(|spatial| self.vp9_dd_layer_active(spatial)))
    }

    fn vp9_dd_layer_active(&self, spatial: usize) -> bool {
        self.dd_active_spatial
            .is_some_and(|ceiling| spatial <= ceiling)
            && self
                .configured_bitrate_intervals()
                .is_some_and(|intervals| intervals[spatial].is_some())
    }

    pub(crate) fn layer_available(&self, spatial: i32, now: Instant) -> bool {
        if !(0..=2).contains(&spatial) {
            return false;
        }
        if !self.layer_tracker_configured(spatial as usize) {
            return false;
        }
        if self.tracks_vp9_dd_activity() {
            return self.vp9_dd_layer_active(spatial as usize);
        }
        if self.frame_tracking_eligible() {
            return self.frame_trackers[spatial as usize]
                .is_some_and(|tracker| tracker.is_active(now));
        }
        if self.packet_tracked && !self.svc {
            return self.trackers[spatial as usize].is_active(now);
        }
        self.stats[spatial as usize]
            .last_seen
            .is_some_and(|t| now.saturating_duration_since(t) <= LAYER_AVAILABLE_WINDOW)
    }

    /// Simulcast/single-stream video without descriptor-driven tracking, polled
    /// at most every 50 ms (well inside the 500 ms tracker cycle).
    pub(crate) fn wants_packet_tracking(&mut self, now: Instant) -> bool {
        if !self.packet_tracking_eligible() {
            return false;
        }
        if self
            .tracker_polled_at
            .is_some_and(|at| now.saturating_duration_since(at) < Duration::from_millis(50))
        {
            return false;
        }
        self.tracker_polled_at = Some(now);
        true
    }

    /// Fixed Go uses per-layer packet trackers for simulcast/single-stream video
    /// (only SVC with a dependency descriptor gets the DD tracker).
    pub(crate) fn packet_tracking_eligible(&self) -> bool {
        self.kind == TrackKind::Video
            && !self.svc
            && !self.dd_bitrate_aggregated
            && !self.frame_tracking_eligible()
    }

    pub(crate) fn frame_tracking_eligible(&self) -> bool {
        self.kind == TrackKind::Video
            && !self.dd_bitrate_aggregated
            && self.frame_trackers.iter().any(Option::is_some)
    }

    pub(crate) fn frame_tracker_spatial(&self, data: &str0m::media::MediaPacket) -> Option<usize> {
        if !self.frame_tracking_eligible() || data.packet.payload.is_empty() {
            return None;
        }
        if data.params.spec().codec == str0m::format::Codec::Vp9
            && !self.rate_is_per_encoding(data.rid)
        {
            return crate::temporal_packet::vp9_spatial(&data.packet.payload);
        }
        Some(self.tracker_spatial(data.rid) as usize)
    }

    pub(crate) fn observe_layer_frame(
        &mut self,
        spatial: usize,
        marker: bool,
        timestamp: u32,
        now: Instant,
    ) {
        if self.muted || !self.frame_tracking_eligible() {
            return;
        }
        if let Some(Some(tracker)) = self.frame_trackers.get_mut(spatial) {
            tracker.observe(now, marker, timestamp);
        }
    }

    /// Spatial index of a received frame's stream for the trackers.
    pub(crate) fn tracker_spatial(&self, rid: Option<Rid>) -> i32 {
        if rid.is_none() {
            0
        } else {
            self.spatial_for_rid(rid).clamp(0, 2)
        }
    }

    /// The receive stream rid of `spatial`: `None` (one stream) for a
    /// non-simulcast track's base layer, otherwise the layer's actual or
    /// declared rid. `None` from this function means no such layer stream.
    pub(crate) fn tracker_rid(&self, spatial: i32) -> Option<Option<Rid>> {
        let i = spatial.clamp(0, 2) as usize;
        if !self.is_simulcast() && self.sdp_rids.is_empty() {
            return (i == 0).then_some(None);
        }
        self.stats[i]
            .rid
            .or_else(|| layers::rid_for_spatial(&self.layers, spatial).map(Rid::from))
            .map(Some)
    }

    /// `StreamTracker.Observe` for the media packets a layer stream received
    /// since the previous poll (`count` is the stream's cumulative counter).
    pub(crate) fn observe_layer_count(&mut self, spatial: i32, count: u64, now: Instant) {
        let i = spatial.clamp(0, 2) as usize;
        let previous = self.tracker_counts[i].replace(count);
        let delta = match previous {
            Some(p) if count >= p => count - p,
            Some(_) => count, // stream restarted
            None => count,
        };
        if delta == 0 || self.muted {
            return;
        }
        self.packet_tracked = true;
        let Some(rtc_core::config::StreamTrackerSpec::Packet(config)) = self.tracker_spec(i) else {
            return; // no tracker configured for this layer: never available
        };
        let tracker = &mut self.trackers[i];
        tracker.set_config(config.into());
        tracker.observe(now, delta);
    }

    pub(crate) fn reset_stream_trackers(&mut self) {
        for tracker in &mut self.trackers {
            tracker.reset();
        }
        for tracker in self.frame_trackers.iter_mut().flatten() {
            tracker.reset();
        }
    }

    /// `StreamTrackerManager.SetMaxExpectedSpatialLayer` from the committed
    /// dynacast (subscribed quality) message.
    pub(crate) fn set_max_expected_spatial(&mut self, layer: i32) {
        let layer = layer.min(layers::MAX_SPATIAL);
        if layer > self.max_expected {
            for l in (self.max_expected + 1).max(0)..=layer {
                self.trackers[l as usize].reset();
                if let Some(tracker) = &mut self.frame_trackers[l as usize] {
                    tracker.reset();
                }
            }
        }
        self.max_expected = layer;
    }

    pub(crate) fn on_subscribed_codecs(&mut self, codecs: &[rtc_core::model::SubscribedCodec]) {
        let mime = crate::codecs::dynacast_codec_name(&self.mime);
        let Some(codec) = codecs.iter().find(|c| c.codec.eq_ignore_ascii_case(&mime)) else {
            return;
        };
        let enabled = |quality| {
            codec
                .qualities
                .iter()
                .any(|q| q.enabled && q.quality == quality)
        };
        let max = [VideoQuality::High, VideoQuality::Medium, VideoQuality::Low]
            .into_iter()
            .find(|q| enabled(*q))
            .unwrap_or(VideoQuality::Off);
        let layer = if max == VideoQuality::Off {
            layers::INVALID_SPATIAL
        } else {
            layers::spatial_for_quality(&self.layers, max)
        };
        self.set_max_expected_spatial(layer);
    }

    pub(crate) fn available(&self, now: Instant) -> [bool; 3] {
        [
            self.layer_available(0, now),
            self.layer_available(1, now),
            self.layer_available(2, now),
        ]
    }

    pub(crate) fn mark_paused(&mut self, rid: Option<Rid>) {
        if self.svc {
            for layer in &mut self.stats {
                layer.last_seen = None;
            }
            return;
        }
        let s = self.spatial_for_rid(rid);
        self.stats[s.clamp(0, 2) as usize].last_seen = None;
    }

    pub(crate) fn configure_stream_trackers(
        &mut self,
        config: rtc_core::config::VideoStreamTrackers,
    ) {
        self.tracker_specs = Some(config);
        self.frame_trackers = std::array::from_fn(|layer| match self.tracker_spec(layer) {
            Some(rtc_core::config::StreamTrackerSpec::Frame { min_fps }) => {
                Some(crate::stream_tracker::FrameTracker::new(min_fps))
            }
            _ => None,
        });
    }

    fn tracker_spec(&self, layer: usize) -> Option<rtc_core::config::StreamTrackerSpec> {
        let config = self.tracker_specs.unwrap_or_default();
        let layers = if self.is_screenshare() {
            config.screenshare
        } else {
            config.video
        };
        // Go's AddTracker also requires a bitrate-report entry for the layer.
        if let Some(intervals) = self.bitrate_intervals {
            intervals[layer]?;
        }
        layers[layer]
    }

    pub(crate) fn configure_bitrate_reports(
        &mut self,
        config: rtc_core::config::VideoBitrateReport,
    ) {
        if self.kind == TrackKind::Video {
            let intervals = if self.is_screenshare() {
                config.screenshare
            } else {
                config.video
            };
            self.bitrate_intervals = Some(intervals.map(|d| d.filter(|d| !d.is_zero())));
        }
    }

    fn configured_bitrate_intervals(&self) -> Option<[Option<Duration>; 3]> {
        if self.dd_bitrate_aggregated {
            let intervals = self.bitrate_intervals?;
            // AddDependencyDescriptorTrackers uses interval[0] for its shared
            // worker, then AddTracker still requires an entry for each layer.
            return Some(intervals.map(|layer| layer.and(intervals[0])));
        }
        // VP9 payload fallback uses the ordinary per-layer tracker. A legacy
        // frame-assembled SVC source keeps its existing shared cost window.
        if (self.svc && !self.dd_source.uses_vp9_payload())
            || (self.mime.eq_ignore_ascii_case("video/av1") && self.uses_av1_original_packets(None))
        {
            None
        } else {
            self.bitrate_intervals
        }
    }

    fn start_dd_bitrate_epoch(&mut self, now: Instant) {
        if let Some(intervals) = self.configured_bitrate_intervals() {
            self.bitrate_started_at = intervals.map(|interval| interval.map(|_| now));
        }
    }

    pub(crate) fn set_dd_bitrate_muted(&mut self, muted: bool, now: Instant) {
        if self.dd_bitrate_aggregated {
            if muted {
                self.start_dd_bitrate_epoch(now);
            } else {
                self.reset_bitrate_epoch(now);
            }
        }
    }

    fn close_configured_bitrate_window(&mut self, layer: usize, interval: Duration, now: Instant) {
        let Some(start) = self.bitrate_started_at[layer] else {
            return;
        };
        let elapsed = now.saturating_duration_since(start);
        if elapsed < interval {
            return;
        }
        let stat = &mut self.stats[layer];
        if self.dd_bitrate_aggregated {
            let rates = std::mem::take(&mut stat.dd_window_bytes)
                .map(|bytes| (bytes as f64 * 8.0 / elapsed.as_secs_f64()) as u64);
            self.bitrate_report_pending |= stat.dd_bitrates != rates;
            stat.dd_bitrates = rates;
        } else {
            let rate = (stat.window_bytes as f64 * 8.0 / elapsed.as_secs_f64()) as u64;
            self.bitrate_report_pending |= stat.bitrate_bps != rate;
            stat.bitrate_bps = rate;
        }
        stat.window_bytes = 0;
        self.bitrate_started_at[layer] = Some(now);
        self.cost_completed_at[layer] = Some(now);
    }

    pub(crate) fn next_bitrate_report(&self) -> Option<Instant> {
        let intervals = self.configured_bitrate_intervals()?;
        if self.bitrate_report_pending {
            return self.cost_completed_at.iter().copied().flatten().min();
        }
        self.bitrate_started_at
            .into_iter()
            .zip(intervals)
            .filter_map(|(start, interval)| start?.checked_add(interval?))
            .min()
    }

    fn close_due_bitrate_windows(&mut self, now: Instant) {
        if let Some(intervals) = self.configured_bitrate_intervals() {
            for (layer, interval) in intervals.into_iter().enumerate() {
                if let Some(interval) = interval {
                    self.close_configured_bitrate_window(layer, interval, now);
                }
            }
        }
    }

    pub(crate) fn advance_bitrate_reports(&mut self, now: Instant) -> bool {
        self.close_due_bitrate_windows(now);
        std::mem::take(&mut self.bitrate_report_pending)
    }

    /// Close the legacy shared bitrate window for audio and SVC without DD.
    pub(crate) fn roll_bitrate_window(&mut self, now: Instant) {
        if self.configured_bitrate_intervals().is_some() {
            return;
        }
        let elapsed = now
            .saturating_duration_since(self.window_start)
            .as_secs_f64();
        if elapsed < 0.5 {
            return;
        }
        for s in &mut self.stats {
            s.bitrate_bps = (s.window_bytes as f64 * 8.0 / elapsed) as u64;
            s.window_bytes = 0;
        }
        self.window_start = now;
        self.cost_completed_at = [Some(now); 3];
    }

    pub(crate) fn bitrate_observation_for(
        &self,
        temporal_limit: u8,
    ) -> crate::allocator::LayerBitrateObservation {
        let dd_temporal_limit = self.dd_bitrate_aggregated.then_some(temporal_limit.min(3));
        let dd_active_layers = self
            .tracks_vp9_dd_activity()
            .then(|| std::array::from_fn(|spatial| self.vp9_dd_layer_active(spatial)));
        crate::allocator::LayerBitrateObservation {
            generation: self.cost_generation,
            completed_at: self.cost_completed_at,
            dd_temporal_limit,
            dd_active_layers,
            bitrates: std::array::from_fn(|spatial| match dd_temporal_limit {
                Some(_) if dd_active_layers.is_some_and(|active| !active[spatial]) => 0,
                // Fixed Go Forwarder.getOptimalBandwidthNeeded searches the
                // highest measured temporal cell at or below the subscriber cap.
                Some(limit) => self.stats[spatial].dd_bitrates[..=usize::from(limit)]
                    .iter()
                    .rev()
                    .copied()
                    .find(|&rate| rate > 0)
                    .unwrap_or(0),
                None => self.stats[spatial].bitrate_bps,
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn bitrate_observation(&self) -> crate::allocator::LayerBitrateObservation {
        self.bitrate_observation_for(3)
    }

    #[cfg(test)]
    pub(crate) fn bitrates(&self) -> [u64; 3] {
        self.bitrate_observation().bitrates
    }

    /// Per-layer upstream PLI throttle (`rtc.pli_throttle`, §8.1). Returns the rid to
    /// request on when allowed.
    pub(crate) fn pli_allowed(
        &mut self,
        spatial: i32,
        throttle: &PliThrottle,
        now: Instant,
    ) -> Option<Option<Rid>> {
        // Go only remaps higher spatial requests to buffer 0 for the declared
        // MULTIPLE mode. An unknown VP9 mode without RIDs has no higher buffer.
        if spatial > 0 && self.sdp_rids.is_empty() && self.unrecognized_vp9_layer_mode() {
            return None;
        }
        let i = if self.svc {
            0
        } else {
            spatial.clamp(0, 2) as usize
        };
        let limit = match i {
            0 => throttle.low,
            1 => throttle.mid,
            _ => throttle.high,
        };
        let s = &mut self.stats[i];
        if s.last_pli
            .is_some_and(|t| now.saturating_duration_since(t) < limit)
        {
            return None;
        }
        s.last_pli = Some(now);
        if self.svc {
            return Some(None);
        }
        let rid = s
            .rid
            .or_else(|| layers::rid_for_spatial(&self.layers, spatial).map(Rid::from));
        Some(rid)
    }

    /// Quality of a spatial layer (dynacast).
    pub(crate) fn quality_for_spatial(&self, spatial: i32) -> VideoQuality {
        if self.layers.is_empty() {
            return if spatial >= 0 {
                VideoQuality::High
            } else {
                VideoQuality::Off
            };
        }
        layers::quality_for_spatial(&self.layers, spatial)
    }

    pub(crate) fn is_screenshare(&self) -> bool {
        matches!(self.spec.source, TrackSource::ScreenShare)
    }

    pub(crate) fn is_simulcast(&self) -> bool {
        !self.svc && self.layers.len() > 1
    }

    pub(crate) fn ingress_delta(&mut self, rid: Option<Rid>, total: u64) -> u64 {
        if let Some(e) = self.ingress_packets.iter_mut().find(|(r, _)| *r == rid) {
            let d = total.saturating_sub(e.1);
            e.1 = total;
            d
        } else {
            self.ingress_packets.push((rid, total));
            total
        }
    }
}

#[cfg(test)]
#[path = "uptrack_active_dd_test.rs"]
mod active_dd_tests;

#[cfg(test)]
mod svc_tests {
    use super::*;
    use str0m::media::Frequency;
    pub(super) fn track(now: Instant) -> UpTrack {
        UpTrack::new(
            PublishedTrackSpec {
                backup_codec_policy: Default::default(),
                sid: TrackSid::from("TR_svc"),
                kind: TrackKind::Video,
                source: TrackSource::Camera,
                cid: "camera".into(),
                layers: vec![],
                codecs: vec![],
                stereo: false,
                disable_dtx: false,
                disable_red: false,
                encrypted: false,
                muted: false,
            },
            ParticipantSid::from("PA_svc"),
            TransportId::next(),
            Mid::from("0"),
            TrackKind::Video,
            "video/VP9".into(),
            vec![],
            &AudioLevelParams {
                active_level: 35,
                min_percentile: 40,
                update_interval: Duration::from_millis(500),
                smooth_intervals: 2,
            },
            Duration::from_secs(5),
            now,
        )
    }

    #[test]
    fn observed_rtp_ssrc_binds_first_wins_and_survives_rebuild() {
        let now = Instant::now();
        let mut up = track(now);
        // Single-layer (non-simulcast) video: one spatial-0 layer, SSRC unbound.
        assert_eq!(up.layers.len(), 1);
        assert_eq!(up.layers[0].spatial_layer, 0);
        assert_eq!(up.layers[0].ssrc, 0);

        // First RTP observation binds the layer SSRC + RTX (SetLayerSsrcsForRid).
        assert!(up.observe_layer_ssrc(0, 3_369_745_197, 12_345));
        assert_eq!(up.layers[0].ssrc, 3_369_745_197);
        assert_eq!(up.layers[0].repair_ssrc, 12_345);

        // Sticky: a later different SSRC must NOT override the first binding.
        assert!(!up.observe_layer_ssrc(0, 999, 777));
        assert_eq!(up.layers[0].ssrc, 3_369_745_197);
        assert_eq!(up.layers[0].repair_ssrc, 12_345);

        // A zero SSRC is never bound.
        assert!(!up.observe_layer_ssrc(0, 0, 0));
        assert_eq!(up.layers[0].ssrc, 3_369_745_197);

        // A mime-triggered rebuild preserves the bound SSRC (carry-over).
        up.set_mime("video/VP8", now + Duration::from_secs(1));
        assert_eq!(up.layers[0].ssrc, 3_369_745_197);
        assert_eq!(up.layers[0].repair_ssrc, 12_345);
    }

    #[test]
    fn simulcast_layers_bind_independent_ssrcs_by_spatial() {
        let now = Instant::now();
        let mut up = track(now);
        up.sdp_rids = vec!["q".into(), "h".into(), "f".into()];
        up.rebuild_layers(now);
        assert_eq!(up.layers.len(), 3);
        assert!(up.observe_layer_ssrc(0, 100, 0));
        assert!(up.observe_layer_ssrc(1, 200, 21));
        assert!(up.observe_layer_ssrc(2, 300, 0));
        for (spatial, ssrc, repair) in [(0, 100, 0), (1, 200, 21), (2, 300, 0)] {
            let layer = up
                .layers
                .iter()
                .find(|l| l.spatial_layer == spatial)
                .unwrap();
            assert_eq!(layer.ssrc, ssrc);
            assert_eq!(layer.repair_ssrc, repair);
        }
        // An unknown spatial layer binds nothing.
        assert!(!up.observe_layer_ssrc(5, 400, 0));
    }

    #[test]
    fn unknown_vp9_mode_with_no_rid_only_addresses_the_base_pli_buffer() {
        use rtc_core::model::{TrackCodec, VideoLayerMode};

        let now = Instant::now();
        let mut up = track(now);
        up.svc = true;
        up.spec.codecs.push(TrackCodec {
            mime_type: "video/vp9".into(),
            mid: String::new(),
            cid: "camera".into(),
            sdp_cid: String::new(),
            layers: vec![],
            layer_mode: VideoLayerMode::from_i32(99),
        });
        let throttle = PliThrottle {
            low: Duration::from_secs(1),
            mid: Duration::from_secs(1),
            high: Duration::from_secs(1),
        };
        assert_eq!(up.pli_allowed(2, &throttle, now), None);
        assert_eq!(up.pli_allowed(1, &throttle, now), None);
        assert_eq!(up.pli_allowed(0, &throttle, now), Some(None));
        up.spec.codecs[0].layer_mode = VideoLayerMode::MultipleSpatialLayersPerStream;
        assert_eq!(
            up.pli_allowed(2, &throttle, now + Duration::from_secs(1)),
            Some(None)
        );
    }
    #[test]
    fn cost_epochs_only_publish_completed_windows_and_metadata_preserves_generation() {
        let now = Instant::now();
        let mut up = track(now);
        let generation = up.bitrate_observation().generation;
        up.on_frame(0, None, 1000, now);
        up.roll_bitrate_window(now + Duration::from_millis(499));
        assert!(
            up.bitrate_observation()
                .completed_at
                .iter()
                .all(Option::is_none)
        );
        up.roll_bitrate_window(now + Duration::from_secs(1));
        let completed = up.bitrate_observation();
        assert_eq!(
            completed.completed_at,
            [Some(now + Duration::from_secs(1)); 3]
        );
        assert_eq!(completed.bitrates, [8000, 0, 0]);
        assert_eq!(completed.generation, generation);
        let mut spec = up.spec.clone();
        spec.layers = up.layers.clone();
        for layer in &mut spec.layers {
            layer.bitrate = 9_000_000;
            layer.width += 16;
        }
        spec.muted = true;
        up.update_spec(spec, now + Duration::from_millis(1100));
        assert_eq!(up.bitrate_observation().generation, generation);
        assert_eq!(
            up.bitrate_observation().completed_at,
            completed.completed_at
        );
        assert_eq!(up.bitrates(), [8000, 0, 0]);
    }

    #[test]
    fn same_sid_codec_rid_and_source_rebinding_clear_completed_old_measurements() {
        let now = Instant::now();
        let mut up = track(now);
        let sid = up.sid.clone();
        let mut cache = crate::allocator::LayerBitrateCache::default();
        up.on_frame(0, None, 50_000, now);
        up.roll_bitrate_window(now + Duration::from_secs(1));
        let old = up.bitrate_observation();
        assert_eq!(
            cache.resolve(&sid, old, [0; 3], now + Duration::from_secs(1))[0],
            400_000
        );
        let changed = now + Duration::from_millis(1100);
        up.set_mime("video/VP8", changed);
        assert_ne!(up.bitrate_observation().generation, old.generation);
        assert!(
            up.bitrate_observation()
                .completed_at
                .iter()
                .all(Option::is_none)
        );
        assert_eq!(up.bitrates(), [0; 3]);
        assert_eq!(
            cache.resolve(&sid, up.bitrate_observation(), [80_000, 0, 0], changed)[0],
            80_000
        );
        up.on_frame(0, None, 10_000, changed);
        up.roll_bitrate_window(now + Duration::from_millis(2100));
        assert_eq!(up.bitrates()[0], 80_000);
        let before_rid = up.bitrate_observation().generation;
        up.rebind_source(
            up.transport,
            up.mid,
            vec!["q".into()],
            "video/VP8",
            up.spec.clone(),
            now + Duration::from_millis(2200),
        );
        assert_ne!(up.bitrate_observation().generation, before_rid);
        assert!(
            up.bitrate_observation()
                .completed_at
                .iter()
                .all(Option::is_none)
        );
        assert_eq!(up.sid, sid);
        let before_transport = up.bitrate_observation().generation;
        up.rebind_source(
            TransportId::next(),
            up.mid,
            up.sdp_rids.clone(),
            "video/VP8",
            up.spec.clone(),
            now + Duration::from_millis(2300),
        );
        assert_ne!(up.bitrate_observation().generation, before_transport);
    }

    #[test]
    fn svc_costs_include_all_spatial_dependencies_and_temporal_cap_uses_source_clock() {
        let now = Instant::now();
        let mut up = track(now);
        let layers = crate::svc::Vp9Layers {
            ends: [Some(100), Some(300), Some(600)],
            temporal: 0,
        };
        for frame in 0..10 {
            up.on_svc_frame(
                layers,
                false,
                MediaTime::new(frame * 12_000, Frequency::NINETY_KHZ),
                now + Duration::from_micros(frame * 133_333),
            );
        }
        up.roll_bitrate_window(now + Duration::from_secs(2));
        assert_eq!(up.bitrates(), [4_000, 12_000, 24_000]);
        assert_eq!(
            up.svc_temporal_cap(8),
            3,
            "timestamps without source picture identifiers cannot invent measured rates"
        );
        for frame in 0..64u16 {
            let temporal = if frame % 4 == 0 {
                0
            } else if frame % 2 == 0 {
                1
            } else {
                2
            };
            for spatial in 0..3 {
                up.source_rates.observe_vp9_test_source(
                    spatial,
                    frame,
                    temporal,
                    u32::from(frame) * 3000,
                );
            }
        }
        assert_eq!(up.svc_temporal_cap(30), 2);
        assert_eq!(up.svc_temporal_cap(15), 1);
        assert_eq!(up.svc_temporal_cap(8), 0);
        assert_eq!(
            up.svc_temporal_cap(9),
            1,
            "original consumer chooses first sufficient cumulative rate"
        );
        assert_eq!(up.svc_temporal_cap(0), 3);
        assert!(!up.is_simulcast());
        let throttle = PliThrottle {
            low: Duration::ZERO,
            mid: Duration::ZERO,
            high: Duration::ZERO,
        };
        assert_eq!(up.pli_allowed(2, &throttle, now), Some(None));
        assert!(
            up.available(now + Duration::from_secs(1))
                .iter()
                .all(|available| *available)
        );
        up.mark_paused(None);
        assert_eq!(up.available(now + Duration::from_secs(1)), [false; 3]);
        up.set_mime("video/VP8", now + Duration::from_secs(2));
        assert!(!up.svc);
        assert_eq!(up.svc_temporal_cap(8), 3);
    }
}
