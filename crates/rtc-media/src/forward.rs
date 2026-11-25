//! Media plane of the engine: subscriptions, frame forwarding with simulcast layer
//! switching, keyframe requests, mute/silence, dynacast, allocation, speakers and
//! connection quality (sfu-media.md §2-§7).

use std::collections::HashMap;
use std::time::Instant;

use rtc_core::config::PliThrottle;
use rtc_core::ids::{ParticipantSid, TrackSid, TransportId};
use rtc_core::media_api::{
    AudioLevel as CoreAudioLevel, DownTrackSpec, MediaEvent, PublishedTrackSpec, QualitySample,
};
use rtc_core::model::{StreamState, TrackKind};
use rtc_core::session::SubscriptionError;
use str0m::bwe::{Bitrate, BweKind};
use str0m::media::{KeyframeRequestKind, MediaData, MediaTime, Mid};
use str0m::stats::{MediaEgressStats, MediaIngressStats};

use crate::allocator::{self, AllocTrack, PRIORITY_DEFAULT, PRIORITY_SCREENSHARE};
use crate::codecs;
use crate::downtrack::{DownTrack, Forward, SILENCE_INTERVAL, pli_interval};
use crate::dynacast;
use crate::engine::MediaEngine;
use crate::layers::{INVALID_SPATIAL, MAX_SPATIAL};
use crate::quality;
use crate::transport::{IoCounters, Transport};
use crate::uptrack::UpTrack;

/// Request a PLI from the publisher for `spatial`, honouring the per-layer throttle.
pub(crate) fn request_pli(
    transports: &mut HashMap<TransportId, Transport>,
    up: &mut UpTrack,
    spatial: i32,
    throttle: &PliThrottle,
    io: &mut IoCounters,
    dirty: &mut Vec<TransportId>,
    now: Instant,
) {
    if up.kind != TrackKind::Video || up.muted {
        return;
    }
    let Some(rid) = up.pli_allowed(spatial, throttle, now) else {
        return;
    };
    let Some(t) = transports.get_mut(&up.transport) else {
        return;
    };
    if !t.usable() {
        return;
    }
    let Some(mut w) = t.rtc.writer(up.mid) else {
        return;
    };
    match w.request_keyframe(rid, KeyframeRequestKind::Pli) {
        Ok(()) => {
            tracing::debug!(track = %up.sid, spatial, "PLI sent upstream");
            io.plis += 1
        }
        Err(e) => tracing::trace!(track = %up.sid, error = %e, "keyframe request failed"),
    }
    if !dirty.contains(&up.transport) {
        dirty.push(up.transport);
    }
}

impl MediaEngine {
    // ------------------------------------------------------------------
    // subscriptions
    // ------------------------------------------------------------------

    pub(crate) fn subscribe(&mut self, spec: DownTrackSpec, now: Instant) {
        let key = (spec.subscriber.clone(), spec.track_sid.clone());
        if let Some(&di) = self.down_by_key.get(&key) {
            // Resubscribe: restore settings immediately (subscriptionmanager.go:1508).
            if let Some(d) = self.downs.get_mut(di) {
                d.spec.stream_id = spec.stream_id;
                d.settings = spec.settings;
            }
            self.on_settings_applied(di, now);
            return;
        }
        let Some(&ui) = self.up_by_sid.get(&spec.track_sid) else {
            self.events.push(MediaEvent::SubscriptionFailed {
                subscriber: spec.subscriber,
                track_sid: spec.track_sid,
                error: SubscriptionError::TrackNotFound,
            });
            return;
        };
        let Some(up) = self.ups.get(ui) else { return };
        let mut d = DownTrack::new(spec, up.kind, ui, up.spec.encrypted, &up.mime, now);
        d.pub_muted = up.muted;
        d.apply_settings_layers(&up.layers);
        d.subscriber_temporal_limit(up, d.max_spatial);
        d.scorer.set_muted(d.pub_muted || d.sub_muted);
        let di = self.downs.insert(d);
        self.down_by_key.insert(key, di);
        if let Some(d) = self.downs.get(di) {
            self.events.push(MediaEvent::SubscriptionPrepared {
                subscriber: d.spec.subscriber.clone(),
                track_sid: d.spec.track_sid.clone(),
            });
        }
        if let Some(up) = self.ups.get_mut(ui) {
            up.subscribers.push(di);
        }
        self.attach_down(di, now);
        self.update_dynacast(ui, now);
    }

    /// Go builds `dynacastManagerAudio` once, in `NewMediaTrack`, and only for
    /// an audio track whose `TrackInfo` declares more than one codec
    /// (mediatrack.go:161-167). A single-codec audio track has no manager, so
    /// nothing it notifies can reach the publisher; that is why the gate is
    /// codec count and not track kind.
    pub(crate) fn ensure_audio_dynacast(&mut self, ui: usize) {
        let Some(up) = self.ups.get(ui) else { return };
        if up.kind != TrackKind::Audio || up.spec.codecs.len() < 2 {
            return;
        }
        let sid = up.sid.clone();
        if self.audio_dynacast.contains_key(&sid) {
            return;
        }
        self.audio_dynacast.insert(sid, Default::default());
    }

    /// Go `SubscribedTrack.OnCodecNegotiated` (subscribedtrack.go:456-482): the
    /// negotiated mime of the down track is reported as enabled for that one
    /// subscriber, through the publisher's manager
    /// (mediatrack.go:189-195 → dynacastmanageraudio NotifySubscription). The
    /// mime is the codec receiver the down track follows, which is what pion
    /// negotiated for the publisher-side stream.
    pub(crate) fn notify_audio_codec_negotiated(&mut self, di: usize) {
        let Some(down) = self.downs.get(di) else {
            return;
        };
        if down.kind != TrackKind::Audio {
            return;
        }
        let Some(up) = self.ups.get(down.up) else {
            return;
        };
        if up.kind != TrackKind::Audio {
            return;
        }
        let sid = up.sid.clone();
        let publisher = up.publisher.clone();
        let subscriber = down.spec.subscriber.clone();
        // Upstream resolves the mime through the receiver that the negotiation
        // arrived on: subscribedtrack.go:456-459 returns unless
        // WrappedReceiver.DetermineReceiver(codec) reports the codec available in
        // that receiver's group, so a down track with no bound receiver in the
        // publisher's group announces nothing.
        let Some(mime) = self
            .codec_receivers
            .get(&sid)
            .and_then(|group| group.receivers.iter().find(|r| r.up == down.up))
            .map(|receiver| receiver.mime.clone())
        else {
            return;
        };
        let Some(manager) = self.audio_dynacast.get_mut(&sid) else {
            return;
        };
        let codecs = manager.notify_subscription(subscriber, &mime, true);
        if !codecs.is_empty() {
            self.events.push(MediaEvent::SubscribedAudioCodec {
                publisher,
                track_sid: sid,
                codecs,
            });
        }
    }

    pub(crate) fn remove_down(&mut self, di: usize, now: Instant) {
        self.retire_receiver_report_source(di);
        self.detach_down(di, now);
        let Some(d) = self.downs.remove(di) else {
            return;
        };
        self.down_by_key
            .remove(&(d.spec.subscriber.clone(), d.spec.track_sid.clone()));
        if let Some(u) = self.ups.get_mut(d.up) {
            u.subscribers.retain(|x| *x != di);
        }
        self.update_dynacast(d.up, now);
        self.events.push(MediaEvent::SubscriptionRemoved {
            subscriber: d.spec.subscriber,
            track_sid: d.spec.track_sid,
        });
    }

    pub(crate) fn unpublish(&mut self, sid: &TrackSid, now: Instant) {
        let retiring = self
            .ups
            .iter()
            .filter(|(_, up)| &up.sid == sid)
            .map(|(ui, _)| ui)
            .collect::<Vec<_>>();
        for ui in retiring {
            self.retire_receiver_traffic(ui, now);
        }
        self.published.remove(sid);
        for t in self.transports.values_mut() {
            t.expected.retain(|e| &e.sid != sid);
        }
        // Remove the logical publication first; no late per-codec dynacast
        // event may resurrect an unpublished track.
        self.up_by_sid.remove(sid);
        self.codec_receivers.remove(sid);
        let receivers = self
            .ups
            .iter()
            .filter(|(_, up)| &up.sid == sid)
            .map(|(ui, _)| ui)
            .collect::<Vec<_>>();
        for ui in receivers {
            let subscribers = self
                .ups
                .get(ui)
                .map(|up| up.subscribers.clone())
                .unwrap_or_default();
            for di in subscribers {
                self.remove_down(di, now);
            }
            if let Some(up) = self.ups.remove(ui) {
                self.up_by_mid.retain(|_, index| *index != ui);
                if let Some(transport) = self.transports.get_mut(&up.transport) {
                    transport.remote_mids.remove(&up.mid);
                    transport.remote_info.remove(&up.mid);
                    transport.publication_rtp.remove(&up.mid);
                }
            }
        }
    }

    pub(crate) fn update_published(&mut self, spec: PublishedTrackSpec, now: Instant) {
        for t in self.transports.values_mut() {
            for e in t.expected.iter_mut().filter(|e| e.sid == spec.sid) {
                *e = spec.clone();
            }
        }
        self.published.insert(spec.sid.clone(), spec.clone());
        let alternate = self
            .ups
            .iter()
            .filter(|(_, up)| up.sid == spec.sid)
            .map(|(ui, _)| ui)
            .collect::<Vec<_>>();
        for ui in alternate {
            if let Some(up) = self.ups.get_mut(ui) {
                up.update_spec(spec.clone(), now);
            }
        }
        let Some(&ui) = self.up_by_sid.get(&spec.sid) else {
            return;
        };
        let sid = spec.sid.clone();
        let muted = spec.muted;
        let was_muted = match self.ups.get_mut(ui) {
            Some(u) => {
                let was = u.muted;
                u.update_spec(spec, now);
                u.muted = was;
                u.spec.muted = was;
                was
            }
            None => return,
        };
        self.refresh_down_layers(ui, now);
        if muted != was_muted {
            self.set_publisher_muted(&sid, muted, now);
        }
    }

    pub(crate) fn set_publisher_muted(&mut self, sid: &TrackSid, muted: bool, now: Instant) {
        if let Some(spec) = self.published.get_mut(sid) {
            spec.muted = muted;
        }
        for transport in self.transports.values_mut() {
            for expected in transport
                .expected
                .iter_mut()
                .filter(|track| &track.sid == sid)
            {
                expected.muted = muted;
            }
        }
        let receivers = self
            .ups
            .iter()
            .filter(|(_, up)| &up.sid == sid)
            .map(|(ui, _)| ui)
            .collect::<Vec<_>>();
        for &ui in &receivers {
            self.set_receiver_muted(ui, muted, now);
        }
        if !muted
            && let Some(ui) = receivers
                .into_iter()
                .find(|ui| self.ups.get(*ui).is_some_and(|up| up.dynacast.is_some()))
        {
            self.commit_dynacast(ui, true, now);
        }
    }

    fn set_receiver_muted(&mut self, ui: usize, muted: bool, now: Instant) {
        let Some(up) = self.ups.get_mut(ui) else {
            return;
        };
        if up.muted == muted {
            return;
        }
        up.muted = muted;
        up.spec.muted = muted;
        up.reset_stream_trackers();
        up.set_dd_bitrate_muted(muted, now);
        up.scorer.set_muted(muted);
        if let Some(dynacast) = up.dynacast.as_mut() {
            dynacast.set_muted(muted);
        }
        let Self {
            ups,
            downs,
            transports,
            io,
            dirty,
            cfg,
            ..
        } = self;
        let up = ups.get_mut(ui).unwrap();
        let available = up.available(now);
        let configured_layers = up.configured_layers();
        let mut want_pli = [false; 3];
        for &di in &up.subscribers {
            let Some(d) = downs.get_mut(di) else { continue };
            d.set_pub_muted(muted, now);
            if !muted {
                d.refresh_target(available, configured_layers);
                if d.wants_keyframe() {
                    let rtt = d
                        .transport
                        .and_then(|t| transports.get(&t))
                        .and_then(|t| t.rtt);
                    d.next_pli_at = Some(now + pli_interval(rtt));
                    want_pli[d.target.clamp(0, MAX_SPATIAL) as usize] = true;
                }
            }
        }
        // Unmute: post keyframe requests so the subscribers re-lock (downtrack.go:1451).
        for (s, want) in want_pli.iter().enumerate() {
            if *want {
                request_pli(
                    transports,
                    up,
                    s as i32,
                    &cfg.rtc.pli_throttle,
                    io,
                    dirty,
                    now,
                );
            }
        }
    }

    /// Settings were applied (immediately or after the debounce).
    pub(crate) fn on_settings_applied(&mut self, di: usize, now: Instant) {
        let ui = {
            let Self {
                ups,
                downs,
                transports,
                io,
                dirty,
                cfg,
                ..
            } = self;
            let Some(d) = downs.get_mut(di) else { return };
            let Some(up) = ups.get_mut(d.up) else { return };
            let was_stopped = d.current < 0;
            d.apply_settings_layers(&up.layers);
            d.subscriber_temporal_limit(up, d.max_spatial);
            d.scorer.set_muted(d.sub_muted || d.pub_muted);
            let changed = d.refresh_target(up.available(now), up.configured_layers());
            if changed && was_stopped {
                d.restart_acquisition(now);
            }
            if d.negotiated && d.wants_keyframe() && d.next_pli_at.is_none_or(|t| t <= now) {
                let rtt = d
                    .transport
                    .and_then(|t| transports.get(&t))
                    .and_then(|t| t.rtt);
                d.next_pli_at = Some(now + pli_interval(rtt));
                let target = d.target;
                request_pli(
                    transports,
                    up,
                    target,
                    &cfg.rtc.pli_throttle,
                    io,
                    dirty,
                    now,
                );
            }
            d.up
        };
        self.update_dynacast(ui, now);
    }

    /// Publisher layers changed (bind / spec update): re-derive subscriber max layers.
    pub(crate) fn refresh_down_layers(&mut self, ui: usize, now: Instant) {
        let Self { ups, downs, .. } = self;
        let Some(up) = ups.get(ui) else { return };
        for &di in &up.subscribers {
            if let Some(d) = downs.get_mut(di) {
                d.apply_settings_layers(&up.layers);
            }
        }
        self.update_dynacast(ui, now);
    }

    // ------------------------------------------------------------------
    // forwarding
    // ------------------------------------------------------------------

    pub(crate) fn on_media_data(&mut self, tid: TransportId, data: MediaData, now: Instant) {
        let forwarding_dequeued_at = Instant::now();
        let Some(&ui) = self.up_by_mid.get(&(tid, data.mid)) else {
            // Media for unbound mids is dropped.
            return;
        };
        if matches!(
            data.params.spec().codec,
            str0m::format::Codec::Av1 | str0m::format::Codec::Vp9
        ) && self.ups.get(ui).is_some_and(|up| {
            up.mime
                .eq_ignore_ascii_case(codecs::mime_of(data.params.spec().codec))
                && ((up.uses_av1_original_packets(data.rid)
                    && (up.av1_packets.is_active() || up.dd_source.uses_vp9_payload()))
                    || up
                        .one_dd_layer(&up.mime, data.rid)
                        .is_some_and(|layer| up.one_dd.engaged(layer)))
        }) {
            // Original packets already ran the authenticated DD consumer. A
            // later depayloaded picture cannot duplicate media or telemetry.
            return;
        }
        let packet_cost_owned = data.logical_rtp_bytes().is_some()
            && !self.dependency_descriptor_negotiated(tid, data.mid)
            && (data.rid.is_some()
                || matches!(data.codec_extra, str0m::format::CodecExtra::Vp9(extra) if extra.tid.is_none()))
            && self.ups.get(ui).is_some_and(|up| {
                up.ordinary_vp9_bitrate_layer(data.rid).is_some()
                    && !up.dd_source.engaged()
                    && !up
                        .one_dd_layer(&up.mime, data.rid)
                        .is_some_and(|layer| up.one_dd.engaged(layer))
            });
        self.codec_receiver_started(ui, codecs::mime_of(data.params.spec().codec), now);
        if self.codec_receiver_unavailable(ui) {
            return;
        }
        self.select_codec_receivers(ui, now);
        let mut forwarding_admissions =
            crate::forward_timing::ForwardingAdmissions::new(data.forwarding_packets());
        let layer_changed = {
            let Self {
                ups,
                downs,
                transports,
                events,
                dirty,
                io,
                cfg,
                ..
            } = self;
            let Some(up) = ups.get_mut(ui) else { return };
            let is_video = up.kind == TrackKind::Video;
            // Go's receiver observes each packet's tracker before forwarding it,
            // so a layer's first packets make it available for this very frame.
            if up.packet_tracking_eligible()
                && let Some(count) = transports.get_mut(&tid).and_then(|t| {
                    t.rtc
                        .direct_api()
                        .stream_rx_by_mid(data.mid, data.rid)
                        .map(|stream| stream.media_packets_received())
                })
            {
                let spatial = up.tracker_spatial(data.rid);
                up.observe_layer_count(spatial, count, now);
            }
            let spatial = if is_video {
                up.spatial_for_rid(data.rid)
            } else {
                0
            };
            // Bind the observed RTP SSRC to its layer (first-SSRC-wins) so the
            // RoomService TrackInfo projection carries it like upstream Go.
            let observed_ssrc = if is_video {
                transports.get_mut(&tid).and_then(|t| {
                    t.rtc
                        .direct_api()
                        .stream_rx_by_mid(data.mid, data.rid)
                        .map(|s| (*s.ssrc(), s.rtx().map(|r| *r).unwrap_or(0)))
                })
            } else {
                None
            };
            let ssrc_changed = match observed_ssrc {
                Some((ssrc, repair)) => up.observe_layer_ssrc(spatial, ssrc, repair),
                None => false,
            };
            let first_start = !up.started;
            if first_start {
                up.started = true;
                if data.params.spec().codec == str0m::format::Codec::Vp9
                    && let Some(flags) = data
                        .ext_vals
                        .user_values
                        .get::<str0m::rtp::FrameUserExtensions>()
                        .and_then(|packets| {
                            packets
                                .0
                                .iter()
                                .find_map(|packet| packet.get::<str0m::media::Vp9PacketInfo>())
                        })
                {
                    tracing::debug!(track=%up.sid,source_flexible=flags.flexible,source_picture_bits=flags.picture_bits,source_picture_id=flags.picture_id,"VP9 original reference profile");
                }
                let mime = if up.mime.eq_ignore_ascii_case("audio/red")
                    && data.params.spec().codec == str0m::format::Codec::Opus
                {
                    "audio/red"
                } else {
                    codecs::mime_of(data.params.spec().codec)
                };
                if !mime.is_empty() {
                    up.set_mime(mime, now);
                }
            }
            if first_start || ssrc_changed {
                events.push(MediaEvent::TrackMediaStarted {
                    track_sid: up.sid.clone(),
                    mime_type: up.mime.clone(),
                    layers: up.layers.clone(),
                });
            }
            let svc = if is_video && data.rid.is_none() && !up.spec.encrypted {
                crate::svc::Vp9Layers::parse_with_dependencies(
                    data.codec_extra,
                    data.data.len(),
                    data.ext_vals
                        .user_values
                        .get::<str0m::media::Vp9FrameDependencies>()
                        .copied(),
                )
            } else {
                None
            };
            if is_video
                && matches!(data.codec_extra, str0m::format::CodecExtra::Vp9(extra) if extra.tid.is_some())
                && svc.is_none()
                && data.rid.is_none()
                && !up.spec.encrypted
            {
                request_pli(transports, up, 0, &cfg.rtc.pli_throttle, io, dirty, now);
                return;
            }
            let clear_av1 = data.params.spec().codec == str0m::format::Codec::Av1
                && !up.spec.encrypted
                && data.rid.is_none();
            let requires_av1_dd = clear_av1 && (up.svc || up.layers.len() > 1);
            let has_av1_dd = clear_av1
                && data
                    .ext_vals
                    .user_values
                    .get::<str0m::rtp::FrameUserExtensions>()
                    .is_some_and(|values| {
                        values
                            .0
                            .iter()
                            .any(|packet| packet.get::<crate::av1::RawDescriptor>().is_some())
                    });
            let av1 = if has_av1_dd {
                up.av1
                    .picture(&data.data, &data.ext_vals, data.is_keyframe())
            } else {
                None
            };
            if let Some(picture) = &av1 {
                forwarding_admissions.apply_av1_layers(picture.packet_layers());
            }
            if (has_av1_dd && av1.is_none()) || (requires_av1_dd && !has_av1_dd) {
                request_pli(transports, up, 0, &cfg.rtc.pli_throttle, io, dirty, now);
                return;
            }
            // The source updates one-shot TrackInfo from real codec/DD sizes.
            // Ordinary SDK tracks keep their client-provided metadata semantics.
            if is_video
                && !up.spec.encrypted
                && transports.get(&tid).is_some_and(|t| t.spec.one_shot)
            {
                let codec = data.params.spec().codec;
                let observation = if let Some(picture) = &av1 {
                    picture.attached_resolutions().map(|s| s.to_vec())
                } else if codec == str0m::format::Codec::Vp8 {
                    crate::video_dimensions::vp8(data.is_keyframe(), &data.data)
                        .map(|size| vec![size])
                } else if codec == str0m::format::Codec::Vp9 {
                    crate::video_dimensions::vp9(data.is_keyframe(), data.codec_extra)
                } else {
                    None
                };
                if let Some(sizes) = observation {
                    let multiple =
                        crate::video_dimensions::multiple_spatial_layers(&up.spec.codecs, &up.mime);
                    if let Some(sizes) = up.media_sizes.observe(&up.mime, spatial, multiple, &sizes)
                    {
                        events.push(MediaEvent::TrackVideoSizeChanged {
                            track_sid: up.sid.clone(),
                            mime_type: up.mime.clone(),
                            sizes,
                        });
                    }
                }
            }
            if let Some(picture) = &av1 {
                up.on_av1_frame(picture, data.time, now);
            } else if let Some(svc) = svc {
                up.on_svc_frame(svc, data.is_keyframe(), data.time, now);
            } else if packet_cost_owned {
                up.observe_frame(spatial, data.rid, now);
            } else {
                up.on_frame(
                    spatial,
                    data.rid,
                    data.logical_rtp_bytes().unwrap_or(data.data.len()),
                    now,
                );
            }
            if !is_video {
                up.on_audio_level(data.ext_vals.audio_level, data.time, now);
            }
            if up.muted || up.subscribers.is_empty() {
                return;
            }
            let available = up.available(now);
            let configured_layers = up.configured_layers();
            let keyframe = is_video
                && av1
                    .as_ref()
                    .map_or_else(|| data.is_keyframe(), |picture| picture.keyframe);
            // Go v1.13.10 does not request an upstream keyframe for a sequence gap:
            // loss is repaired by NACK and subscriber PLI/FIR forwarding, plus the
            // down track layer-lock requester (downtrack.go:978, :2079).
            let mut want_pli = [false; 3];
            let mut vp9_switch_point = false;
            let mut vp9_ending = false;
            let mut vp9_up_components = [false; 3];
            let mut vp9_picture = (0u16, 0u8);
            let mut vp9_flexible = [false; 3];
            let mut vp9_predicted = [false; 3];
            let mut vp9_references = [[0u8; 3]; 3];
            if let Some(original) = data
                .ext_vals
                .user_values
                .get::<str0m::rtp::FrameUserExtensions>()
            {
                for packet in &original.0 {
                    if let Some(flags) = packet.get::<str0m::media::Vp9PacketInfo>() {
                        vp9_picture = (flags.picture_id, flags.picture_bits);
                        let component = usize::from(flags.spatial);
                        vp9_flexible[component] = flags.flexible;
                        vp9_predicted[component] = flags.predicted;
                        vp9_references[component] = flags.reference_diffs;
                        let is_switch_point = flags.switching_up && flags.beginning;
                        vp9_switch_point |= is_switch_point;
                        vp9_up_components[component] |= is_switch_point;
                        vp9_ending |= flags.ending;
                    }
                }
            }
            let mut layer_changed = false;
            let mut svc_payloads: [Option<std::sync::Arc<[u8]>>; 3] = Default::default();

            for &di in &up.subscribers {
                let Some(d) = downs.get_mut(di) else { continue };
                if !d.negotiated || d.codec_failed {
                    continue;
                }
                let (Some(out_tid), Some(mid)) = (d.transport, d.mid) else {
                    continue;
                };
                d.refresh_target(available, configured_layers);
                let before = d.current;
                let verdict = if let Some(picture) = &av1 {
                    let temporal_limit = d.subscriber_temporal_limit(up, d.max_spatial);
                    d.on_svc_frame(
                        if up.svc { picture.highest } else { spatial },
                        picture.temporal,
                        keyframe,
                        temporal_limit,
                    )
                } else if let Some(svc) = svc {
                    let temporal_limit = d.subscriber_temporal_limit(up, d.max_spatial);
                    d.on_vp9_frame(
                        svc.highest(),
                        svc.temporal,
                        keyframe,
                        temporal_limit,
                        vp9_switch_point,
                        vp9_ending,
                    )
                } else if !is_video || configured_layers[spatial.clamp(0, 2) as usize] {
                    d.on_frame(spatial, keyframe, now)
                } else {
                    Forward::Drop
                };
                if d.current != before && d.current >= 0 {
                    layer_changed = true;
                }
                if d.wants_keyframe() && d.next_pli_at.is_none_or(|t| t <= now) {
                    let rtt = transports.get(&out_tid).and_then(|t| t.rtt);
                    d.next_pli_at = Some(now + pli_interval(rtt));
                    want_pli[d.target.clamp(0, MAX_SPATIAL) as usize] = true;
                }
                let seq = (**data.seq_range.start(), **data.seq_range.end());
                let Forward::Send { resumed } = verdict else {
                    // Go munges, then PacketDropped: the position moves, no gap.
                    d.observe_source_seq(data.rid, data.pt, seq.0, seq.1, false, false);
                    continue;
                };

                let av1_prepared = if let Some(picture) = &av1 {
                    // In declared ONE mode, current selects the encoding, not
                    // an internal DD spatial prefix. Retain the entire encoding.
                    let spatial = if up.svc { d.current } else { picture.highest };
                    let temporal = if up.svc {
                        d.forwarded_temporal_cap()
                    } else {
                        u8::MAX
                    };
                    if !d.av1.observe_picture(picture, spatial, temporal) {
                        // Original DD NotPresent/disabled target is an ordinary
                        // packet rejection, not decoder loss or a PLI trigger.
                        d.observe_source_seq(data.rid, data.pt, seq.0, seq.1, false, false);
                        continue;
                    }
                    match d.av1.prepare_target(picture, &data.data, spatial, temporal) {
                        Some(prepared) => Some(prepared),
                        None => {
                            d.stop_forwarding();
                            want_pli[d.target.clamp(0, MAX_SPATIAL) as usize] = true;
                            continue;
                        }
                    }
                } else {
                    None
                };

                let Some(t) = transports.get_mut(&out_tid) else {
                    continue;
                };
                // Go writes a forwarded packet even when its transport cannot deliver
                // it (or, unwritable, never munges it), so the subscriber sees these
                // sequence numbers as a gap either way.
                let undeliverable = |d: &mut DownTrack| {
                    if d.observe_source_seq(data.rid, data.pt, seq.0, seq.1, true, resumed) {
                        d.pending_seq_gap += seq.1 - seq.0 + 1;
                    }
                };
                if !t.usable() || !t.can_send(mid) {
                    undeliverable(d);
                    continue;
                }
                let Some(writer) = t.rtc.writer(mid) else {
                    undeliverable(d);
                    continue;
                };
                let pt = match d.last_pt {
                    Some((inp, outp)) if inp == data.pt => outp,
                    // The matched PT must also be negotiated on this m-line (str0m's
                    // match is session-wide).
                    _ => match writer
                        .match_params(data.params)
                        .filter(|p| writer.payload_params().any(|pp| pp.pt() == *p))
                    {
                        Some(p) => {
                            d.last_pt = Some((data.pt, p));
                            p
                        }
                        None => {
                            // Potential backup codecs are a source-level
                            // receiver promise. Await their actual publication
                            // rather than fail a negotiated compatible backup.
                            if up.spec.codecs.iter().any(|codec| {
                                writer.payload_params().any(|param| {
                                    codecs::mime_of(param.spec().codec)
                                        .eq_ignore_ascii_case(&codec.mime_type)
                                })
                            }) {
                                continue;
                            }
                            if !d.codec_failed {
                                d.codec_failed = true;
                                events.push(MediaEvent::SubscriptionFailed {
                                    subscriber: d.spec.subscriber.clone(),
                                    track_sid: d.spec.track_sid.clone(),
                                    error: SubscriptionError::CodecUnsupported,
                                });
                            }
                            continue;
                        }
                    },
                };
                // Only a frame actually written to this PT is part of the forwarded
                // source; an unmatched PT or an awaited backup codec never reached
                // this down track in Go (a separate receiver/stream).
                d.observe_source_seq(data.rid, data.pt, seq.0, seq.1, true, resumed);
                let gap = std::mem::take(&mut d.pending_seq_gap);
                let writer = if gap > 0 {
                    drop(writer);
                    if let Some(stream) = t.rtc.direct_api().stream_tx_by_mid(mid, None) {
                        for _ in 0..gap {
                            stream.next_seq_no();
                        }
                    }
                    let Some(writer) = t.rtc.writer(mid) else {
                        continue;
                    };
                    writer
                } else {
                    writer
                };
                let rtp_time = d.out_time(data.time, data.network_time);
                let mut w = writer;
                if is_video
                    && let Some((min, max)) = t.spec.effective_playout_delay(cfg.playout_delay)
                {
                    w = w.playout_delay(
                        MediaTime::from_millis(min as u64),
                        MediaTime::from_millis(max as u64),
                    );
                }
                if let Some(act) = data.ext_vals.abs_capture_time {
                    w = w.abs_capture_time(act);
                }
                if let Some(prepared) = &av1_prepared {
                    w = w.user_extension_value(prepared.descriptor.clone());
                }
                if let Some(svc) = svc {
                    let cap = d.vp9_forwarded_spatial_cap(svc.temporal);
                    let mut ends = svc.ends;
                    for (index, end) in ends.iter_mut().enumerate() {
                        if index as i32 > cap {
                            *end = None;
                        }
                    }
                    let dependencies = data
                        .ext_vals
                        .user_values
                        .get::<str0m::media::Vp9FrameDependencies>()
                        .map(|d| d.0)
                        .unwrap_or([Some(false), Some(true), Some(true)]);
                    w = w.user_extension_value(str0m::media::Vp9Forwarding {
                        source_picture_id: vp9_picture.0,
                        picture_bits: vp9_picture.1,
                        flexible: vp9_flexible,
                        predicted: vp9_predicted,
                        reference_diffs: vp9_references,
                        ends,
                        temporal: svc.temporal,
                        switching_up: vp9_up_components,
                        dependent: dependencies.map(|d| d.unwrap_or(false)),
                        target_spatial: d.current as u8,
                        complete: svc.temporal <= d.forwarded_temporal_cap()
                            && svc.ends[d.current as usize].is_some(),
                        keyframe,
                    });
                }
                let payload = if let Some(prepared) = &av1_prepared {
                    prepared.payload.clone()
                } else if let Some(svc) = svc {
                    let cap = d.vp9_forwarded_spatial_cap(svc.temporal);
                    let slot = &mut svc_payloads[cap as usize];
                    if slot.is_none() {
                        *slot = svc.payload(&data.data, cap);
                    }
                    let Some(payload) = slot.as_ref() else {
                        continue;
                    };
                    payload.clone()
                } else {
                    data.data.clone()
                };
                if let Err(e) = w.write(pt, data.network_time, rtp_time, payload) {
                    tracing::debug!(transport = %out_tid, error = %e, "media write failed");
                } else {
                    let reference = if let Some(svc) = svc {
                        svc.ends.iter().position(Option::is_some).map(|s| s as i32)
                    } else if up.svc {
                        av1.as_ref()
                            .zip(av1_prepared.as_ref())
                            .and_then(|(picture, prepared)| {
                                picture
                                    .packet_frames()
                                    .iter()
                                    .zip(picture.packet_layers())
                                    .find_map(|(frame, layer)| {
                                        frame
                                            .filter(|frame| prepared.input_frames.contains(frame))
                                            .and_then(|_| {
                                                layer.map(|(spatial, _)| i32::from(spatial))
                                            })
                                    })
                            })
                    } else {
                        Some(spatial)
                    };
                    if let Some(reference) = reference {
                        d.observe_reference_layer(reference);
                    }
                    if keyframe
                        && let Some(stream) = t.rtc.direct_api().stream_tx_by_mid(mid, None)
                        && stream.nack_throttled()
                    {
                        tracing::debug!(transport = %out_tid, %mid, "keyframe forwarded: NACK throttle cleared");
                        stream.set_nack_throttled(false);
                    }
                    let retained_spatial = if let Some(picture) = &av1 {
                        if up.svc { d.current } else { picture.highest }
                    } else if svc.is_some() {
                        d.current
                    } else {
                        spatial
                    };
                    let retained_temporal = if av1.is_some() || svc.is_some() {
                        d.forwarded_temporal_cap()
                    } else {
                        u8::MAX
                    };
                    if let (Some(picture), Some(prepared)) = (&av1, &av1_prepared) {
                        forwarding_admissions
                            .accepted_av1(picture.packet_frames(), &prepared.input_frames);
                    } else if svc.is_some() {
                        forwarding_admissions.accepted_vp9(d.current, retained_temporal, spatial);
                    } else {
                        forwarding_admissions.accepted(
                            retained_spatial,
                            retained_temporal,
                            spatial,
                        );
                    }
                    if let Some(prepared) = &av1_prepared {
                        d.av1.commit(prepared);
                    }
                }
                if !dirty.contains(&out_tid) {
                    dirty.push(out_tid);
                }
                if resumed && is_video {
                    events.push(MediaEvent::StreamState {
                        subscriber: d.spec.subscriber.clone(),
                        publisher: d.spec.publisher.clone(),
                        track_sid: d.spec.track_sid.clone(),
                        state: StreamState::Active,
                    });
                }
            }
            for (s, want) in want_pli.iter().enumerate() {
                if *want {
                    request_pli(
                        transports,
                        up,
                        s as i32,
                        &cfg.rtc.pli_throttle,
                        io,
                        dirty,
                        now,
                    );
                }
            }
            layer_changed
        };
        // Count each original packet once after the broadcast has at least
        // one actual packetizer admission, matching Go's successful Enqueue.
        // Each arrival remains distinct; frame assembly latency is included.
        self.record_forwarding_cohort(
            &forwarding_admissions,
            Instant::now(),
            forwarding_dequeued_at,
        );
        if layer_changed {
            self.update_dynacast(ui, now);
        }
    }

    /// Subscriber PLI/FIR → one throttled PLI upstream on the current (or target) layer.
    pub(crate) fn on_keyframe_request(&mut self, tid: TransportId, mid: Mid, now: Instant) {
        let Some(&di) = self.down_by_mid.get(&(tid, mid)) else {
            return;
        };
        let Self {
            ups,
            downs,
            transports,
            io,
            dirty,
            cfg,
            ..
        } = self;
        let Some(d) = downs.get_mut(di) else { return };
        if !d.is_video() {
            return;
        }
        let layer = if d.current >= 0 { d.current } else { d.target };
        if layer < 0 {
            return;
        }
        d.debug_last_pli_unix_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .min(i64::MAX as u128) as i64;
        let Some(up) = ups.get_mut(d.up) else { return };
        request_pli(transports, up, layer, &cfg.rtc.pli_throttle, io, dirty, now);
        // Fixed Go FlagStopRTXOnPLI: a forwarded subscriber PLI stops answering
        // NACKs on this down track until the next keyframe is forwarded.
        if let Some(t) = transports.get_mut(&tid)
            && let Some(stream) = t.rtc.direct_api().stream_tx_by_mid(mid, None)
        {
            tracing::debug!(transport = %tid, %mid, layer, "subscriber PLI: NACK throttled");
            stream.set_nack_throttled(true);
        }
    }

    // ------------------------------------------------------------------
    // timers
    // ------------------------------------------------------------------

    pub(crate) fn down_timers(&mut self, now: Instant) {
        let mut applied = Vec::new();
        let mut silence = Vec::new();
        for (i, d) in self.downs.iter_mut() {
            if d.poll_settings(now) {
                applied.push(i);
            }
            if d.silence_next.is_some_and(|t| t <= now) {
                silence.push(i);
            }
        }
        for di in applied {
            self.on_settings_applied(di, now);
        }
        for di in silence {
            self.inject_silence(di, now);
        }
    }

    /// One opus silence frame (20 ms cadence, ~1 s) after publisher mute (§7.1).
    fn inject_silence(&mut self, di: usize, now: Instant) {
        let Self {
            ups,
            downs,
            transports,
            dirty,
            cfg,
            ..
        } = self;
        let Some(d) = downs.get_mut(di) else { return };
        if d.silence_left == 0 {
            d.silence_next = None;
            return;
        }
        d.silence_left -= 1;
        d.silence_next = (d.silence_left > 0).then(|| now + SILENCE_INTERVAL);
        let is_opus = ups
            .get(d.up)
            .is_some_and(|u| u.mime.eq_ignore_ascii_case("audio/opus"));
        let (Some(tid), Some(mid), Some((_, pt)), true) = (d.transport, d.mid, d.last_pt, is_opus)
        else {
            return;
        };
        let Some(rtp_time) = d.synth_time(SILENCE_INTERVAL, now) else {
            return;
        };
        let mut payload =
            Vec::with_capacity(codecs::OPUS_SILENCE_FRAME.len() + cfg.sif_trailer.len());
        payload.extend_from_slice(&codecs::OPUS_SILENCE_FRAME);
        if d.encrypted {
            payload.extend_from_slice(&cfg.sif_trailer);
        }
        let Some(t) = transports.get_mut(&tid) else {
            return;
        };
        if !t.usable() || !t.can_send(mid) {
            return;
        }
        let Some(w) = t.rtc.writer(mid) else { return };
        if let Err(e) = w.write(pt, now, rtp_time, payload) {
            tracing::debug!(transport = %tid, error = %e, "silence write failed");
        }
        if !dirty.contains(&tid) {
            dirty.push(tid);
        }
    }

    /// Feed the fixed Go packet stream trackers from each layer's received media
    /// packet count (counted on arrival, like Go's buffer, not on frame completion).
    pub(crate) fn poll_stream_trackers(&mut self, now: Instant) {
        let Self {
            ups, transports, ..
        } = self;
        for (_, up) in ups.iter_mut() {
            if !up.wants_packet_tracking(now) {
                continue;
            }
            let Some(t) = transports.get_mut(&up.transport) else {
                continue;
            };
            let mid = up.mid;
            for spatial in 0..=crate::layers::MAX_SPATIAL {
                let Some(rid) = up.tracker_rid(spatial) else {
                    continue;
                };
                let Some(count) = t
                    .rtc
                    .direct_api()
                    .stream_rx_by_mid(mid, rid)
                    .map(|stream| stream.media_packets_received())
                else {
                    continue;
                };
                up.observe_layer_count(spatial, count, now);
            }
        }
    }

    /// Restart once per logical publication, retaining demand and shared debounce.
    pub(crate) fn restart_publisher_dynacast(
        &mut self,
        participant: &ParticipantSid,
        now: Instant,
    ) {
        let tracks = self.publisher_dynacast_tracks(participant);
        for sid in tracks {
            for (_, up) in self.ups.iter_mut() {
                if up.sid != sid || &up.publisher != participant {
                    continue;
                }
                let high = crate::layers::spatial_for_quality(
                    &up.layers,
                    rtc_core::model::VideoQuality::High,
                );
                up.set_max_expected_spatial(high);
                if let Some(dynacast) = up.dynacast.as_mut() {
                    dynacast.restart(now);
                }
            }
            if let Some(group) = self.codec_receivers.get_mut(&sid) {
                group.dynacast_commit.snapshot.clear();
                group.dynacast_commit.resend_eligible = false;
            }
        }
    }

    fn publisher_dynacast_tracks(&self, participant: &ParticipantSid) -> Vec<TrackSid> {
        self.up_by_sid
            .iter()
            .filter_map(|(sid, ui)| {
                self.ups
                    .get(*ui)
                    .filter(|up| &up.sid == sid && &up.publisher == participant)
                    .map(|_| sid.clone())
            })
            .collect()
    }

    pub(crate) fn resend_publisher_dynacast(&mut self, participant: &ParticipantSid) {
        for sid in self.publisher_dynacast_tracks(participant) {
            let Some(group) = self.codec_receivers.get(&sid) else {
                continue;
            };
            if !group.dynacast_commit.resend_eligible {
                continue;
            }
            let codecs = group.dynacast_commit.snapshot.clone();
            self.emit_dynacast_snapshot(sid, participant.clone(), codecs);
        }
    }

    pub(crate) fn dynacast_timers(&mut self, now: Instant) {
        let initial = self
            .ups
            .iter()
            .filter_map(|(ui, up)| {
                up.dynacast
                    .as_ref()?
                    .next_deadline()
                    .is_some_and(|at| at <= now)
                    .then_some(ui)
            })
            .collect::<Vec<_>>();
        for ui in initial {
            let recompute = self
                .ups
                .get(ui)
                .and_then(|up| up.dynacast.as_ref())
                .is_some_and(|d| d.needs_recompute());
            if self
                .ups
                .get_mut(ui)
                .unwrap()
                .dynacast
                .as_mut()
                .unwrap()
                .take_initial_update(now)
            {
                if recompute {
                    // A codec regression injected HIGH for a codec that had no demand of its
                    // own. Go's forced update re-reads that codec's subscribers, so the
                    // injection must give way to the real aggregate here.
                    self.update_dynacast(ui, now);
                } else {
                    self.commit_dynacast(ui, false, now);
                }
            }
        }
        let due = self
            .codec_receivers
            .iter_mut()
            .filter_map(|(sid, group)| {
                if !group.dynacast_commit.deadline.is_some_and(|at| at <= now) {
                    return None;
                }
                let active = group.receivers.iter().find_map(|r| {
                    self.ups
                        .get(r.up)
                        .filter(|up| &up.sid == sid && up.dynacast.is_some())
                        .map(|_| r.up)
                });
                if active.is_none() {
                    group.dynacast_commit.deadline = None;
                }
                active
            })
            .collect::<Vec<_>>();
        for ui in due {
            self.commit_dynacast(ui, true, now);
        }
    }

    pub(crate) fn commit_dynacast(&mut self, ui: usize, force: bool, now: Instant) {
        let Some(up) = self.ups.get(ui) else { return };
        let sid = up.sid.clone();
        let publisher = up.publisher.clone();
        if !self.up_by_sid.contains_key(&sid) {
            return;
        }
        let Some(group) = self.codec_receivers.get_mut(&sid) else {
            return;
        };
        let receivers = group
            .receivers
            .iter()
            .filter_map(|r| self.ups.get(r.up).filter(|up| up.sid == sid).map(|_| r.up))
            .collect::<Vec<_>>();
        if !group.dynacast_commit.update(
            receivers
                .iter()
                .filter_map(|ui| self.ups.get(*ui)?.dynacast.as_ref()),
            force,
            now,
        ) {
            return;
        }
        for &receiver in &receivers {
            if let Some(d) = self.ups.get_mut(receiver).unwrap().dynacast.as_mut() {
                d.commit();
            }
        }
        let Some(codecs) = self
            .codec_quality_snapshot(&sid)
            .or_else(|| self.ups.get(ui)?.dynacast.as_ref()?.committed_message())
        else {
            return;
        };
        self.codec_receivers
            .get_mut(&sid)
            .unwrap()
            .dynacast_commit
            .snapshot = codecs.clone();
        self.emit_dynacast_snapshot(sid, publisher, codecs);
    }

    fn emit_dynacast_snapshot(
        &mut self,
        sid: TrackSid,
        publisher: ParticipantSid,
        codecs: Vec<rtc_core::model::SubscribedCodec>,
    ) {
        let Some(up) = self.up_by_sid.get(&sid).and_then(|ui| self.ups.get(*ui)) else {
            return;
        };
        if up.sid != sid || up.publisher != publisher {
            return;
        }
        let muted = up.muted;
        let Some(group) = self.codec_receivers.get(&sid) else {
            return;
        };
        for receiver in &group.receivers {
            if let Some(up) = self.ups.get_mut(receiver.up)
                && up.sid == sid
                && up.publisher == publisher
            {
                up.on_subscribed_codecs(&codecs);
            }
        }
        if !muted {
            self.events.push(MediaEvent::SubscribedQuality {
                publisher,
                track_sid: sid,
                codecs,
            });
        }
    }

    /// Recompute the aggregated subscriber demand of a published video track.
    pub(crate) fn update_dynacast(&mut self, ui: usize, now: Instant) {
        let supported = self
            .ups
            .get(ui)
            .map(|up| {
                up.subscribers
                    .iter()
                    .copied()
                    .filter(|di| {
                        self.downs.get(*di).is_some_and(|down| !down.negotiated)
                            || self.receiver_codec_supported(*di, ui)
                    })
                    .collect::<std::collections::HashSet<_>>()
            })
            .unwrap_or_default();
        let Self {
            ups,
            downs,
            up_by_sid,
            ..
        } = self;
        let Some(up) = ups.get_mut(ui) else { return };
        if up.dynacast.is_none() || !up_by_sid.contains_key(&up.sid) {
            return;
        }
        let demand = dynacast::aggregate(
            up.subscribers
                .iter()
                .filter(|di| supported.contains(di))
                .filter_map(|di| downs.get(*di))
                .map(|d| up.quality_for_spatial(d.demand_spatial())),
        );
        if up.dynacast.as_mut().is_some_and(|d| d.set_demand(demand)) {
            self.commit_dynacast(ui, false, now);
        }
    }

    pub(crate) fn bitrate_report_timers(&mut self, now: Instant) {
        let mut changed_transports = std::collections::HashSet::new();
        for (_, track) in self.ups.iter_mut() {
            if track.advance_bitrate_reports(now) {
                changed_transports.extend(
                    track.subscribers.iter().filter_map(|index| {
                        self.downs.get(*index).and_then(|track| track.transport)
                    }),
                );
            }
        }
        for transport in changed_transports {
            if self
                .transports
                .get(&transport)
                .is_some_and(|t| t.sends_media() && t.usable())
            {
                self.allocate_transport(transport, now);
            }
        }
    }

    /// Once per second: bitrate windows and allocation on every sending transport.
    pub(crate) fn periodic_tick(&mut self, now: Instant) {
        // The engine invokes this on its existing one-second cadence. Include
        // publisher-only transports too: this observes server-generated feedback
        // about incoming RTP, separately from subscriber bandwidth estimates.
        if tracing::enabled!(tracing::Level::DEBUG) {
            for t in self.transports.values_mut().filter(|t| t.usable()) {
                if let Some(incoming_twcc) = t.rtc.bwe().incoming_twcc_snapshot() {
                    tracing::debug!(transport = %t.id(), observation = ?incoming_twcc,
                        "incoming RTP TWCC metadata (serialization poll, not kernel send)");
                }
            }
        }
        for (_, u) in self.ups.iter_mut() {
            u.roll_bitrate_window(now);
        }
        let tids: Vec<TransportId> = self
            .transports
            .values()
            .filter(|t| t.sends_media() && t.usable())
            .map(|t| t.id())
            .collect();
        for tid in tids {
            self.allocate_transport(tid, now);
        }
        self.probe_ping(now);
    }

    /// Go `StreamAllocator.handleSignalPeriodicPing` for the native transports,
    /// on its own pass rather than inside the allocation pass, because upstream
    /// runs it from its own ping timer. The allocation pass has already
    /// published the deficient state, the expected usage and the next-higher
    /// transition, so this is the estimator's lifecycle: the paused reset, the
    /// active cluster's goal check, finalization and capacity commit, the
    /// decision to probe, the prober's bucket clock and the RTT pull.
    pub(crate) fn probe_ping(&mut self, now: Instant) {
        let tids: Vec<TransportId> = self
            .transports
            .values()
            .filter(|t| t.has_bwe && t.send_side.is_some() && t.sends_media() && t.usable())
            .map(|t| t.id())
            .collect();
        let mut reallocate = Vec::new();
        for tid in tids {
            let Some(t) = self.transports.get_mut(&tid) else {
                continue;
            };
            let inputs = crate::send_side_bwe::ProbeInputs {
                allow_pause: t.spec.allow_pause,
                deficient: t.allocator_deficient,
                override_active: t.bw_override_bps > 0,
                committed_bps: t.committed_channel_capacity_bps,
            };
            let (actions, commit, cancel, repost, finalized) = {
                let Some(send_side) = &mut t.send_side else {
                    continue;
                };
                let ping = send_side.periodic_ping(&inputs, now);
                (
                    ping.actions,
                    ping.commit_bps,
                    ping.cancel_probe,
                    ping.reallocate || ping.media_boost,
                    ping.probe_finalized,
                )
            };
            Self::apply_probe_actions(t, &actions, now);
            if let Some((signal, channel_capacity)) = finalized {
                // Go logs this pair as `stream allocator: probe result`; the
                // tally is this port's telemetry of the same decision, so the
                // reported outcome of a probe is the one its own finalization
                // produced rather than a vendor validation verdict that may
                // never have been able to measure the cluster.
                let counters = &mut t.native_probes;
                match signal {
                    crate::send_side_bwe::ProbeSignal::NotCongesting => {
                        counters.confirmed += 1;
                    }
                    crate::send_side_bwe::ProbeSignal::Congesting => {
                        counters.congested += 1;
                    }
                    crate::send_side_bwe::ProbeSignal::Inconclusive => {
                        counters.inconclusive += 1;
                    }
                }
                tracing::debug!(transport = %tid, probe_signal = ?signal,
                    channel_capacity_bps = channel_capacity,
                    "probe cluster finalized");
            }
            if cancel {
                t.rtc.bwe().cancel_validation(now);
            }
            t.prober_deadline = t
                .send_side
                .as_ref()
                .and_then(|send_side| send_side.prober_deadline(now));
            if let Some(commit) = commit {
                t.committed_channel_capacity_bps = commit;
            }
            // Step D: the estimator's RTT is pulled from the ICE getter at most
            // every `cRTTPullInterval` (30 s). A TWCC report age is never a
            // substitute, and the pull is due even when no RTT is available yet.
            if now.saturating_duration_since(t.last_rtt_pull) >= std::time::Duration::from_secs(30)
            {
                t.last_rtt_pull = now;
                if let (Some(send_side), Some(rtt)) = (&mut t.send_side, t.rtt) {
                    send_side.update_rtt(rtt.as_secs_f64());
                }
            }
            if repost {
                reallocate.push(tid);
            }
        }
        // Go commits the probed capacity and immediately runs
        // `maybeBoostDeficientTracks`/`adjustState`, which is this port's
        // allocation pass over the tracks.
        for tid in reallocate {
            self.allocate_transport(tid, now);
        }
    }

    /// `Prober.run`'s ticker: the bucket clock advances between allocator pings,
    /// so a 20 ms schedule does not wait for the next engine tick.
    pub(crate) fn prober_clock(&mut self, now: Instant) {
        let due: Vec<TransportId> = self
            .transports
            .values()
            .filter(|t| t.send_side.is_some() && t.prober_deadline.is_some_and(|at| at <= now))
            .map(|t| t.id())
            .collect();
        for tid in due {
            let Some(t) = self.transports.get_mut(&tid) else {
                continue;
            };
            let actions = {
                let Some(send_side) = &mut t.send_side else {
                    continue;
                };
                send_side.process_prober(now)
            };
            Self::apply_probe_actions(t, &actions, now);
            t.prober_deadline = t
                .send_side
                .as_ref()
                .and_then(|send_side| send_side.prober_deadline(now));
        }
    }

    /// Resolve what the prober's clock asked the pacer to do. A cluster switch
    /// opens the vendor pacer's probe window at the cluster's goal rate: that
    /// window is this port's producer of the padding traffic Go asks for in
    /// `handleSignalSendProbe`, and the bytes it emits come back through the
    /// send-side boundary, where they are metered into the same bucket clock and
    /// counted by the probe observer.
    fn apply_probe_actions(
        t: &mut crate::transport::Transport,
        actions: &[crate::send_side_bwe::ProbeAction],
        now: Instant,
    ) {
        use crate::send_side_bwe::ProbeAction;
        let mut window = None;
        for action in actions {
            match action {
                ProbeAction::StartCluster {
                    desired_bps,
                    duration,
                } => window = Some((*desired_bps, *duration)),
                // Upstream's `handleSignalSendProbe` asks the pacer for bytes;
                // the pacer here is the vendor's, and the padding it emits
                // during the open window comes back through the send-side
                // boundary, where the prober meters it into the very bucket that
                // produced this demand. The ask is therefore not a quantity this
                // port has to carry: it is settled by the metering path, and only
                // the demand itself is traced so a shortfall stays visible.
                ProbeAction::SendProbe(bytes) => {
                    tracing::trace!(transport = %t.id(), probe_bytes_to_send = bytes,
                        "prober bucket demand is serviced by the open probe window");
                }
            }
        }
        if let Some((desired_bps, duration)) = window.filter(|(bps, _)| *bps > 0) {
            t.native_probes.started += 1;
            let rate = Bitrate::bps(desired_bps as u64);
            let id = t.rtc.bwe().start_validation_with_pacing(rate, rate, now);
            if id.is_none() {
                // Upstream cannot reach this state: its pacer owns the probe
                // traffic, so a cluster always has a producer. Here the padding
                // source is a vendor window that can refuse, and a cluster whose
                // window never opened would sit active with no traffic and no
                // way to reach its goal. Retire it like any other stopped probe,
                // which lets the next ping finalize it; the regulator's own
                // earliest-next-probe instant still gates the retry, so a
                // persistent refusal cannot turn into a per-tick probe loop.
                t.native_probes.refused_start += 1;
                if let Some(send_side) = &mut t.send_side {
                    send_side.stop_probe_cluster(now);
                }
                tracing::debug!(transport = %t.id(), goal_bps = desired_bps,
                    "probe window refused: cluster retired instead of left active");
            }
            // The legacy controller is not registered here. Its validation slot
            // is created only by its own `tick`, which does not run in native
            // mode, so a binding would either be dropped as a no-op or leave a
            // request that its ledger can never admit or fail. The native
            // accounting above and the outcome booked from the next ping's
            // finalization are the whole record of this cluster.
            tracing::debug!(transport = %t.id(), probe_cluster = ?id, goal_bps = desired_bps,
                goal_duration = ?duration, "allocator prober opened the probe window");
        }
    }

    /// Production router for the vendor's send-side observation stream. The
    /// mode decides which consumer handles it.
    pub(crate) fn on_send_side_input(&mut self, tid: TransportId, event: str0m::SendSideFeedback) {
        if self.cfg.rtc.congestion_control.effective_feedback_mode()
            == rtc_core::config::FeedbackMode::NativeSendSide
        {
            self.on_native_input(tid, event);
        } else {
            self.on_interceptor_input(tid, event);
        }
    }

    /// Feed one real vendor observation to the application-owned Go-parity
    /// send-side estimator and act on what it decided, in native mode.
    ///
    /// This is the transport's allocation authority: each detector congestion
    /// transition is handled exactly like Go
    /// `StreamAllocator.handleSignalCongestionStateChange`
    /// (streamallocator.go:856-897) — stop any probe cluster when the channel is
    /// not clear, then commit the estimated channel capacity and reallocate on a
    /// congested transition, unless a probe cluster is active, whose own
    /// finalization decides. `Transport::capacity()` reads that commit.
    pub(crate) fn on_native_input(&mut self, tid: TransportId, event: str0m::SendSideFeedback) {
        use crate::send_side_bwe::CongestionState;
        let Some(t) = self.transports.get_mut(&tid).filter(|t| !t.failed) else {
            return;
        };
        let Some(send_side) = &mut t.send_side else {
            return;
        };
        let outcome = match send_side.handle(event) {
            Ok(outcome) => outcome,
            Err(error) => {
                // Upstream drops a rejected report at the RTCP reader, before
                // any BWE signal reaches an estimator, so nothing is due.
                tracing::debug!(%error, "send side BWE input rejected");
                return;
            }
        };
        let at = outcome.at;
        t.send_side_observed = Some(crate::transport::SendSideObservation {
            estimate_bps: send_side.capacity(),
            congestion: send_side.congestion_state(),
            at,
        });
        let mut reallocate = false;
        for change in &outcome.state_changes {
            if change.to != CongestionState::None {
                // `maybeStopProbe`: an unclear channel must not keep a probe
                // running; the cluster stays active until it finalizes.
                send_side.stop_probe_cluster(at);
            }
            if change.from == CongestionState::EarlyWarning
                && change.to == CongestionState::None
                && !t.allocator_deficient
            {
                // Go releases an early-warning hold here with an *unconstrained*
                // `AllocateOptimal` pass per track. This allocator has no
                // early-warning hold to release (no sub-optimal hold state is
                // ever recorded), so the branch is deliberately inert: routing it
                // through the capacity-bounded pass would pause tracks Go leaves
                // at their optimal layer. Tracked as remaining in SPEC-0077.
                tracing::debug!(transport = %tid, "early warning cleared: no hold to release");
            }
            if change.to == CongestionState::Congested {
                if send_side.probe_cluster_id() != crate::send_side_bwe::PROBE_CLUSTER_ID_INVALID {
                    if !send_side.probe_congesting() {
                        send_side.note_probe_congesting();
                        tracing::debug!(transport = %tid,
                            committed_bps = t.committed_channel_capacity_bps,
                            estimated_bps = change.capacity_bps,
                            "congestion during an active probe cluster: capacity commit deferred");
                    }
                } else {
                    t.committed_channel_capacity_bps = change.capacity_bps;
                    reallocate = true;
                }
            }
        }
        if let Some(completed) = outcome.completed {
            // `handleSignalPacerProbeObserverClusterComplete`: the observer
            // reached the goal, so the pacer closes the window and the prober
            // drops the cluster, while the allocator keeps the id until the
            // estimator finalizes it on the next ping.
            let pci = send_side.end_probe_cluster(completed.id, at);
            if pci.id != crate::send_side_bwe::PROBE_CLUSTER_ID_INVALID {
                t.rtc.bwe().cancel_validation(at);
                tracing::debug!(transport = %tid, cluster = pci.id,
                    probe_bytes = completed.result.bytes(),
                    "probe cluster reached its goal: window closed");
            }
        }
        tracing::debug!(transport = %tid,
            go_parities_estimate_bps = send_side.capacity(),
            go_parities_congestion = ?send_side.congestion_state(),
            state_changes = outcome.state_changes.len(),
            committed_bps = t.committed_channel_capacity_bps,
            inputs = ?send_side.stats(),
            "application-owned Go-parity send-side estimator drove the allocation authority");
        if reallocate {
            self.allocate_transport(tid, at);
        }
    }

    pub(crate) fn on_interceptor_input(
        &mut self,
        tid: TransportId,
        event: str0m::SendSideFeedback,
    ) {
        let Some(t) = self.transports.get_mut(&tid).filter(|t| !t.failed) else {
            return;
        };
        let Some(remote) = &mut t.remote_feedback else {
            return;
        };
        if remote.mode() != rtc_core::config::FeedbackMode::InterceptorEstimate {
            return;
        }
        let (at, changes) = match remote.interceptor_input(event) {
            Ok(result) => result,
            Err(mut error) => {
                // Source typed admission is atomic; WriteRTCP processing is not.
                // Dispatch real earlier callbacks before reporting its later error.
                let prefix = error.take_prefix();
                tracing::debug!(%error,"Pion feedback input rejected");
                match prefix {
                    Some(prefix) => prefix,
                    None => return,
                }
            }
        };
        tracing::trace!(target_bps=?remote.interceptor_target(),inputs=?remote.interceptor_stats(),"actual Pion feedback owner inputs");
        let stats = t.rtc.direct_api().remote_feedback_stats();
        let mut changed = false;
        for estimate in changes {
            if remote.interceptor_estimate_changed(estimate) {
                t.estimate_bps = Some(estimate);
                changed |= remote.estimate(estimate, &stats, &mut t.rtc, at);
            }
        }
        if changed {
            self.allocate_transport(tid, at);
        }
    }

    pub(crate) fn on_bwe(&mut self, tid: TransportId, kind: BweKind, now: Instant) {
        let Some(t) = self.transports.get_mut(&tid) else {
            return;
        };
        if let Some(remote) = &mut t.remote_feedback {
            let stats = t.rtc.direct_api().remote_feedback_stats();
            let estimate = match kind {
                BweKind::ReceiverEstimate { estimate, ssrcs }
                    if remote.mode() == rtc_core::config::FeedbackMode::ReceiverEstimate
                        && remote.accept_remb(&ssrcs, &stats) =>
                {
                    estimate
                }
                _ => return,
            };
            t.estimate_bps = Some(estimate.as_u64());
            let changed = remote.estimate(estimate.as_u64(), &stats, &mut t.rtc, now);
            if changed {
                self.allocate_transport(tid, now);
            }
            return;
        }
        if !t.has_bwe {
            return;
        }
        let est = match kind {
            BweKind::Twcc {
                estimate,
                can_probe,
            } => {
                t.twcc_estimates |= can_probe;
                t.probes.on_estimate(estimate.as_u64(), can_probe, now);
                estimate.as_u64()
            }
            BweKind::Remb { estimate, .. } if !t.twcc_estimates => {
                t.probes.on_remb(estimate.as_u64(), now);
                estimate.as_u64()
            }
            _ => return,
        };
        let prev = t.estimate_bps.replace(est);
        let significant =
            prev.is_none_or(|p| (est as f64 - p as f64).abs() > 0.1 * (p.max(1) as f64));
        if significant || prev.is_some_and(|p| est < p) {
            self.allocate_transport(tid, now);
        }
    }

    /// Allocate measured subscriber capacity, then drive native TWCC probes.
    pub(crate) fn allocate_transport(&mut self, tid: TransportId, now: Instant) {
        let disable_unmanaged_estimation = self
            .cfg
            .rtc
            .congestion_control
            .stream_allocator
            .disable_estimation_unmanaged_tracks;
        let Self {
            transports,
            downs,
            ups,
            events,
            dirty,
            ..
        } = self;
        let Some(t) = transports.get_mut(&tid) else {
            return;
        };
        if !t.sends_media() || !t.usable() {
            return;
        }
        // Connected is latched after the first handshake; a recoverable ICE
        // disconnection is represented separately until the path reconnects.
        let connected = t.probe_path_connected();
        // Fixed StreamAllocator.IsBWEEnabled: only known, unmanaged video is
        // excluded; audio/unknown tracks retain true. Apply per outgoing stream,
        // never the whole transport containing other managed video.
        for (_, down) in downs.iter().filter(|(_, d)| d.transport == Some(tid)) {
            let Some(mid) = down.mid else { continue };
            let unmanaged = down.is_video()
                && ups
                    .get(down.up)
                    .is_some_and(|up| up.is_screenshare() && !up.svc && !up.is_simulcast());
            if let Some(stream) = t.rtc.direct_api().stream_tx_by_mid(mid, None) {
                stream.set_bwe_participating(!(disable_unmanaged_estimation && unmanaged));
            }
        }
        let video: Vec<usize> = downs
            .iter()
            .filter(|(_, d)| d.transport == Some(tid) && d.is_video() && d.negotiated)
            .map(|(i, _)| i)
            .collect();
        let audio_count = downs
            .iter()
            .filter(|(_, d)| d.transport == Some(tid) && !d.is_video())
            .count() as u64;
        let active = video
            .iter()
            .filter_map(|di| downs.get(*di).map(|d| d.spec.track_sid.clone()))
            .collect();
        t.layer_bitrates.retain(&active);
        let tracks: Vec<AllocTrack> = video
            .iter()
            .filter_map(|&di| {
                let d = downs.get(di)?;
                let u = ups.get(d.up)?;
                Some(AllocTrack {
                    priority: if u.is_screenshare() {
                        PRIORITY_SCREENSHARE
                    } else {
                        PRIORITY_DEFAULT
                    },
                    max_spatial: if d.sub_muted || d.pub_muted {
                        INVALID_SPATIAL
                    } else {
                        // A single encoding has only spatial zero, even when
                        // the client requests HIGH (=2 without layer metadata).
                        // Do not probe for fallback costs of nonexistent layers.
                        if u.svc || u.is_simulcast() {
                            d.max_spatial
                        } else {
                            d.max_spatial.min(0)
                        }
                    },
                    bitrates: {
                        let mut declared = [0u64; 3];
                        for layer in &u.layers {
                            if let Ok(spatial @ 0..=2) = usize::try_from(layer.spatial_layer) {
                                declared[spatial] = declared[spatial].max(u64::from(layer.bitrate));
                            }
                        }
                        t.layer_bitrates.resolve(
                            &u.sid,
                            u.bitrate_observation_for(d.subscriber_temporal_limit),
                            declared,
                            now,
                        )
                    },
                    exempt: u.is_screenshare() && !u.svc && !u.is_simulcast(),
                })
            })
            .collect();
        // Snapshot acceptance must use this cycle's subscription demand and
        // explicit override, rather than last cycle's now-obsolete controls.
        let initial_plan = allocator::plan_with_unmanaged_policy(
            t.capacity(),
            &tracks,
            audio_count,
            t.spec.allow_pause,
            disable_unmanaged_estimation,
        );
        let payload_demand = allocator::desired_usage(&tracks)
            .saturating_add(audio_count.saturating_mul(allocator::AUDIO_ALLOWANCE_BPS));
        let desired_wire = payload_demand.saturating_add(payload_demand.div_ceil(9));
        if t.remote_feedback.is_none() {
            // One channel-capacity authority for both surfaces. Go moves
            // `committedChannelCapacity` from exactly two observations — a
            // finalized non-congesting probe cluster, which may only raise it
            // (`streamallocator.go:758-761`), and the congested transition,
            // which replaces it (`:874-895`) — and the allocation pass reads
            // that one field back (`:1257-1277`). Rust measures the same two
            // events on two paths, the ported estimator's own cluster commit
            // (`:1487`) and the vendor-facing controller's id-scoped proof, so
            // this is where the controller's measurement merges into the single
            // field under Go's upward-only rule. Nothing else is published: the
            // congested transition is already owned by the estimator's state
            // change (`:1670-1676`), a receiver estimate or an expired
            // high-water mark has no writer in the pinned oracle, and an
            // explicit override lives in `overriddenChannelCapacity` instead
            // (`:846-854`).
            match t.probes.take_capacity_commit() {
                Some(crate::probe_controller::CapacityCommit::ProbeFinalized(bps)) => {
                    if i64::try_from(bps).unwrap_or(i64::MAX) > t.committed_channel_capacity_bps {
                        t.committed_channel_capacity_bps = i64::try_from(bps).unwrap_or(i64::MAX);
                    }
                }
                None => {}
            }
            // The vendor-facing controller mirrors that commit so its probe
            // gates read the same authority `Transport::capacity()` returns
            // (`streamallocator.go:1257-1277`).
            t.probes
                .sync_committed_capacity(t.capacity().unwrap_or_default());
            t.probes.prepare_admission(
                desired_wire,
                allocator::next_probe_budget(&tracks, &initial_plan),
                (t.bw_override_bps > 0).then_some(t.bw_override_bps),
                connected,
            );
            if let Some(snapshot) = t.rtc.bwe().snapshot() {
                t.probes.observe_native(snapshot, now);
            }
        }
        let capacity = t.capacity();
        let mut plan = allocator::plan_with_unmanaged_policy(
            capacity,
            &tracks,
            audio_count,
            t.spec.allow_pause,
            disable_unmanaged_estimation,
        );
        if t.has_bwe
            && let Some(remote) = &mut t.remote_feedback
        {
            let demands: Vec<_> = video
                .iter()
                .zip(&tracks)
                .filter_map(|(di, track)| {
                    downs
                        .get(*di)
                        .and_then(|d| d.mid)
                        .map(|mid| crate::feedback::MediaDemand {
                            mid,
                            desired: track.max_spatial,
                            priority: track.priority,
                            exempt: track.exempt,
                            rates: track.bitrates,
                        })
                })
                .collect();
            if demands.len() == plan.caps.len() {
                plan.caps =
                    remote.media_caps(&demands, &plan.caps, connected, t.bw_override_bps > 0, now);
                plan.allocated_bps = tracks.iter().zip(&plan.caps).fold(
                    audio_count.saturating_mul(allocator::AUDIO_ALLOWANCE_BPS),
                    |sum, (track, cap)| {
                        sum.saturating_add(if *cap >= 0 {
                            track.bitrates[*cap as usize]
                        } else {
                            0
                        })
                    },
                );
                plan.overshoot_bps = plan.video_budget_bps.map_or(0, |budget| {
                    plan.allocated_bps
                        .saturating_sub(audio_count.saturating_mul(allocator::AUDIO_ALLOWANCE_BPS))
                        .saturating_sub(budget)
                });
            }
        }
        t.allocated_bps = plan.allocated_bps;
        t.allocation_overshoot_bps = plan.overshoot_bps;
        t.paused_video_tracks = plan
            .caps
            .iter()
            .zip(&tracks)
            .filter(|(cap, track)| **cap < 0 && track.max_spatial >= 0)
            .count();
        // Go `adjustState` at the end of `allocateAllTracks`: any managed track
        // still below its desired layer leaves the channel Deficient, and only a
        // fully served set is Stable. An exempt (unmanaged) track always has its
        // optimal layer, so it can never make the channel deficient.
        t.allocator_deficient = tracks.iter().zip(&plan.caps).any(|(track, cap)| {
            !track.exempt && track.max_spatial >= 0 && *cap < track.max_spatial
        });
        if tracks.len() == video.len() && !tracks.is_empty() {
            let caps = &plan.caps;
            for (k, &di) in video.iter().enumerate() {
                let Some(d) = downs.get_mut(di) else { continue };
                if tracks[k].max_spatial < 0 {
                    continue;
                }
                let cap = caps[k];
                if cap < 0 {
                    if !d.bw_paused {
                        d.bw_paused = true;
                        d.stop_forwarding();
                        d.target = INVALID_SPATIAL;
                        d.scorer.set_paused(true);
                        events.push(MediaEvent::StreamState {
                            subscriber: d.spec.subscriber.clone(),
                            publisher: d.spec.publisher.clone(),
                            track_sid: d.spec.track_sid.clone(),
                            state: StreamState::Paused,
                        });
                    }
                    continue;
                }
                if d.bw_paused {
                    d.bw_paused = false;
                    d.scorer.set_paused(false);
                    d.restart_acquisition(now);
                }
                // A cap at or above the desired layer does not constrain later
                // settings upgrades.
                d.alloc_cap = if capacity.is_none() || cap >= tracks[k].max_spatial {
                    MAX_SPATIAL
                } else {
                    cap
                };
            }
        }
        if let Some(remote) = &mut t.remote_feedback {
            let mids = video
                .iter()
                .filter_map(|di| downs.get(*di).and_then(|d| d.mid))
                .collect();
            remote.update_tracks(
                mids,
                plan.allocated_bps
                    .saturating_sub(audio_count.saturating_mul(allocator::AUDIO_ALLOWANCE_BPS)),
            );
            remote.maybe_reset_paused(
                &mut t.rtc,
                t.spec.allow_pause && !video.is_empty() && t.paused_video_tracks == video.len(),
                now,
            );
            if !connected || t.bw_override_bps > 0 {
                let stats = t.rtc.direct_api().remote_feedback_stats();
                remote.finish(&mut t.rtc, &stats, false, now);
            }
            if t.has_bwe {
                // Native allocator helper returns reserved wire units; Remote source policy uses expected payload allocation.
                let next = allocator::next_probe_budget(&tracks, &plan).map(|wire| {
                    (wire.saturating_mul(9) / 10)
                        .saturating_sub(audio_count.saturating_mul(allocator::AUDIO_ALLOWANCE_BPS))
                });
                remote.maybe_start(next, connected, t.bw_override_bps > 0, &mut t.rtc, now);
                if !dirty.contains(&tid) {
                    dirty.push(tid);
                }
            }
        } else if t.has_bwe {
            // Inverse of the allocator's 10% wire reserve, rounded up: otherwise
            // a probe could stop 10% below the capacity needed for the next layer.
            let desired = desired_wire;
            let next_layer_wire_bps = allocator::next_probe_budget(&tracks, &plan);
            t.probes.set_upgrade_goal(next_layer_wire_bps);
            let action = t.probes.tick(
                now,
                desired,
                plan.allocated_bps,
                (t.bw_override_bps > 0).then_some(t.bw_override_bps),
                connected,
                !video.is_empty() && t.paused_video_tracks == video.len(),
            );
            if action.cancel_validation && t.send_side.is_none() {
                t.rtc.bwe().cancel_validation(now);
            }
            if let Some(initial) = action.reset_bwe_to {
                t.rtc.bwe().reset(Bitrate::bps(initial));
            }
            if let Some(target) = action.start_validation_bps
                && t.send_side.is_none()
            {
                // P is the proof/admission floor; Q is bounded transport service only.
                let floor = Bitrate::bps(target);
                let id = t
                    .rtc
                    .bwe()
                    .start_validation_with_pacing(floor, floor * 1.2, now);
                t.probes.validation_started(id, now);
            }
            t.rtc
                .bwe()
                .set_desired_bitrate(Bitrate::bps(action.desired_bps));
            // The probe lifecycle reads its two track-derived values from the
            // pass that owns the layer costs: the expected usage and the
            // next-higher transition of the farthest deficient track. Go reads
            // both straight out of its track registry on every ping.
            if let Some(send_side) = &mut t.send_side {
                send_side.note_allocation(
                    i64::try_from(plan.allocated_bps).unwrap_or(i64::MAX),
                    allocator::next_probe_transition(&tracks, &plan),
                );
            }
            tracing::debug!(transport = %tid, native_estimate_bps = ?t.estimate_bps, capacity_bps = ?capacity, video_budget_bps = ?plan.video_budget_bps,
                next_layer_wire_bps = ?next_layer_wire_bps, desired_wire_bps = desired, caps = ?plan.caps, layer_costs = ?tracks,
                allocated_bps = plan.allocated_bps, overshoot_bps = plan.overshoot_bps,
                paused_tracks = t.paused_video_tracks, discovery_needed = action.discovery_needed, probe_target_bps = action.desired_bps,
                native_bwe = ?t.rtc.bwe().snapshot(), "subscriber allocation");
            if !dirty.contains(&tid) {
                dirty.push(tid);
            }
        }
    }

    // ------------------------------------------------------------------
    // speakers & quality
    // ------------------------------------------------------------------

    pub(crate) fn emit_audio_levels(&mut self, now: Instant) {
        let mut out: Vec<CoreAudioLevel> = Vec::new();
        for (_, u) in self.ups.iter_mut() {
            let Some(al) = u.audio_level.as_mut() else {
                continue;
            };
            let (level, active) = al.level(now);
            let level = if active { level } else { 0.0 };
            match out.iter_mut().find(|e| e.participant == u.publisher) {
                Some(e) => {
                    if active && (!e.active || level > e.level) {
                        e.level = level;
                        e.active = true;
                    }
                }
                None => out.push(CoreAudioLevel {
                    participant: u.publisher.clone(),
                    level,
                    active,
                }),
            }
        }
        if !out.is_empty() {
            self.events.push(MediaEvent::AudioLevels(out));
        }
    }

    pub(crate) fn emit_quality(&mut self, now: Instant) {
        let mut per: Vec<(ParticipantSid, f64)> = Vec::new();
        let mut merge = |p: &ParticipantSid, s: f64| match per.iter_mut().find(|(q, _)| q == p) {
            Some(e) => e.1 = e.1.min(s),
            None => per.push((p.clone(), s)),
        };
        let Self {
            ups,
            downs,
            up_by_sid,
            ..
        } = self;
        for (ui, u) in ups.iter_mut() {
            if up_by_sid.get(&u.sid) != Some(&ui) {
                continue;
            }
            let s = u.scorer.update(now);
            merge(&u.publisher, s);
        }
        for (_, d) in downs.iter_mut() {
            if !d.negotiated {
                continue;
            }
            if d.is_video()
                && d.current >= 0
                && let Some(u) = ups.get(d.up)
            {
                let avail = u.available(now);
                let best = (0..=d.max_spatial.min(MAX_SPATIAL))
                    .rev()
                    .find(|s| avail[*s as usize])
                    .unwrap_or(d.current);
                d.scorer.add_layer_distance(best - d.current);
            }
            let s = d.scorer.update(now);
            merge(&d.spec.subscriber, s);
        }
        if per.is_empty() {
            return;
        }
        let samples = per
            .into_iter()
            .map(|(participant, score)| QualitySample {
                participant,
                quality: quality::quality_of(score),
                score: quality::mos(score),
            })
            .collect();
        self.events.push(MediaEvent::Quality(samples));
    }

    pub(crate) fn on_ingress_stats(
        &mut self,
        tid: TransportId,
        s: MediaIngressStats,
        now: Instant,
    ) {
        let Some(&ui) = self.up_by_mid.get(&(tid, s.mid)) else {
            return;
        };
        let Some(u) = self.ups.get_mut(ui) else {
            return;
        };
        let delta = u.ingress_delta(s.rid, s.packets);
        u.scorer.add_sample(delta, s.loss, now);
    }

    pub(crate) fn on_egress_stats(&mut self, tid: TransportId, s: MediaEgressStats, now: Instant) {
        let Some(&di) = self.down_by_mid.get(&(tid, s.mid)) else {
            return;
        };
        let Some(d) = self.downs.get_mut(di) else {
            return;
        };
        let delta = s.packets.saturating_sub(d.egress_packets);
        d.egress_packets = s.packets;
        self.io.nacks += s.nacks.saturating_sub(d.egress_nacks);
        d.egress_nacks = s.nacks;
        d.scorer.add_sample(delta, s.loss, now);
    }
}
