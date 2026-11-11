//! Publishing: AddTrack → TrackPublished, media matching, unpublish, mute
//! (signaling.md §4, sfu-media.md §1.2, §7).

use std::time::Instant;

use super::participant::PendingTrack;
use super::*;
use crate::events::EngineEvent;
use crate::media_api::{MediaCommand, PublishedTrackSpec, RemoteMedia};
use crate::session::{AddTrack, RequestOutcome};

impl Room {
    pub(super) fn add_track(&mut self, sid: &ParticipantSid, req: AddTrack, now: Instant) {
        self.add_track_counted(sid, req, now, true);
    }
    fn add_track_counted(
        &mut self,
        sid: &ParticipantSid,
        req: AddTrack,
        now: Instant,
        count_attempt: bool,
    ) {
        let (allowed, supports_rr) = match self.participants.get(sid) {
            Some(p) => (
                p.grants.video.can_publish_source(req.source),
                p.capabilities.request_response,
            ),
            None => return,
        };
        let reject = |room: &mut Room, outcome: RequestOutcome, message: String| {
            if supports_rr {
                room.signal(
                    sid,
                    SignalEvent::RequestResponse {
                        token: req.token,
                        request_id: 0,
                        outcome,
                        message,
                    },
                );
            }
        };
        if !allowed {
            return reject(self, RequestOutcome::NotAllowed, String::new());
        }
        if !matches!(req.kind, TrackKind::Audio | TrackKind::Video) {
            return reject(self, RequestOutcome::UnsupportedType, String::new());
        }

        // The SDK first declares available codecs, then assigns a CID when a
        // subscriber asks it to publish one (Go UpdateCodecInfo).
        if let Some(existing) = req.sid.clone() {
            let updated = {
                let Some(p) = self.participants.get_mut(sid) else {
                    return;
                };
                match p.published_mut(&existing) {
                    Some(track) => {
                        for c in &req.simulcast_codecs {
                            let mime = qualify_mime(req.kind, &c.codec);
                            if let Some(codec) = track
                                .codecs
                                .iter_mut()
                                .find(|codec| codec.mime_type.eq_ignore_ascii_case(&mime))
                            {
                                codec.cid.clone_from(&c.cid);
                                if !c.layers.is_empty() {
                                    codec.layers.clone_from(&c.layers);
                                }
                            }
                        }
                        track.version.advance();
                        let t = track.clone();
                        p.mark_dirty();
                        Some(t)
                    }
                    None => None,
                }
            };
            match updated {
                Some(track) => {
                    let spec = published_spec(&track, &req.cid);
                    let transport = self.participants.get(sid).map(|p| p.publish_transport());
                    if let Some(transport) = transport {
                        self.media(MediaCommand::ExpectTrack {
                            transport,
                            track: spec,
                        });
                    }
                    self.signal(
                        sid,
                        SignalEvent::TrackPublished {
                            cid: req.cid.clone(),
                            track,
                        },
                    );
                }
                None => reject(self, RequestOutcome::NotFound, "track not found".into()),
            }
            return;
        }

        let (pending_count, cid_busy, queued_for_cid) = {
            let p = self.participants.get(sid).expect("checked");
            let busy = p.pending.iter().any(|t| t.request.cid == req.cid)
                || p.info.tracks.iter().any(|t| track_has_cid(t, &req.cid));
            (
                p.pending.len(),
                busy,
                p.queued.get(&req.cid).map_or(0, |q| q.len()),
            )
        };
        if cid_busy {
            if queued_for_cid >= MAX_QUEUED_PER_CID {
                return reject(
                    self,
                    RequestOutcome::LimitExceeded,
                    "too many queued tracks with the same cid".into(),
                );
            }
            if count_attempt {
                self.lifecycle.track_publish_attempts += 1;
            }
            if let Some(p) = self.participants.get_mut(sid) {
                p.queued
                    .entry(req.cid.clone())
                    .or_default()
                    .push_back(req.clone());
            }
            return reject(self, RequestOutcome::Queued, String::new());
        }
        if pending_count >= MAX_PENDING_TRACKS {
            return reject(
                self,
                RequestOutcome::LimitExceeded,
                format!(
                    "too many pending tracks, rejecting new track, numPendingTracks: {pending_count}"
                ),
            );
        }

        // A newly authorized AddTrack starts a fresh publication lifecycle. Its
        // cid may have been remembered when an earlier request was cancelled.
        if let Some(p) = self.participants.get_mut(sid) {
            p.released_cids.retain(|group| {
                !group.contains(&req.cid)
                    && !req
                        .simulcast_codecs
                        .iter()
                        .any(|codec| group.contains(&codec.cid))
            });
        }
        if count_attempt {
            self.lifecycle.track_publish_attempts += 1;
        }
        let info = self.build_track_info(&req);
        let spec = published_spec(&info, &req.cid);
        let transport = {
            let p = self.participants.get_mut(sid).expect("checked");
            p.pending.push(PendingTrack {
                request: req.clone(),
                info: info.clone(),
            });
            p.publish_transport()
        };
        self.publication_pending(sid, &info.sid, info.muted, now);
        self.signal(
            sid,
            SignalEvent::TrackPublished {
                cid: req.cid.clone(),
                track: info,
            },
        );
        self.media(MediaCommand::ExpectTrack {
            transport,
            track: spec,
        });

        // Media that arrived before this AddTrack.
        let parked: Vec<RemoteMedia> = match self.participants.get_mut(sid) {
            Some(p) => std::mem::take(&mut p.parked_media),
            None => Vec::new(),
        };
        for media in parked {
            self.remote_media_added(media, now);
        }
    }

    fn build_track_info(&self, req: &AddTrack) -> TrackInfo {
        let mut layers = req.layers.clone();
        layers.sort_by_key(|l| l.quality);

        let mut codecs = Vec::new();
        if req.simulcast_codecs.is_empty() {
            codecs.push(TrackCodec {
                mime_type: String::new(),
                mid: String::new(),
                cid: req.cid.clone(),
                sdp_cid: String::new(),
                layers: layers.clone(),
                layer_mode: VideoLayerMode::OneSpatialLayerPerStream,
            });
        } else {
            for c in &req.simulcast_codecs {
                let mut mime = qualify_mime(req.kind, &c.codec);
                if !self.codec_enabled(&mime) {
                    mime = self.fallback_codec(req.kind);
                }
                if codecs
                    .iter()
                    .any(|tc: &TrackCodec| tc.mime_type.eq_ignore_ascii_case(&mime))
                {
                    continue;
                }
                let svc = is_svc(&mime);
                codecs.push(TrackCodec {
                    mime_type: mime,
                    mid: String::new(),
                    cid: c.cid.clone(),
                    sdp_cid: String::new(),
                    layers: if c.layers.is_empty() {
                        layers.clone()
                    } else {
                        c.layers.clone()
                    },
                    layer_mode: if c.layer_mode != VideoLayerMode::Unused {
                        c.layer_mode
                    } else if svc {
                        VideoLayerMode::MultipleSpatialLayersPerStream
                    } else {
                        VideoLayerMode::OneSpatialLayerPerStream
                    },
                });
            }
        }
        let simulcast = req.kind == TrackKind::Video
            && codecs.first().is_some_and(|codec| {
                codec.layer_mode != VideoLayerMode::MultipleSpatialLayersPerStream
                    && codec.layers.len() > 1
            });

        // Fixed Go utils.DedupeSlice sorts signed enum values before compacting.
        let mut audio_features = req.audio_features.clone();
        audio_features.sort_by_key(|feature| feature.as_i32());
        audio_features.dedup();
        let mut packet_trailer_features = req.packet_trailer_features.clone();
        packet_trailer_features.sort();
        packet_trailer_features.dedup();
        if req.stereo && !audio_features.contains(&AudioFeature::Stereo) {
            audio_features.push(AudioFeature::Stereo);
        }
        if req.disable_dtx && !audio_features.contains(&AudioFeature::NoDtx) {
            audio_features.push(AudioFeature::NoDtx);
        }
        let stream = if req.stream.is_empty() {
            match req.source {
                TrackSource::Camera | TrackSource::Microphone => "camera".to_string(),
                TrackSource::ScreenShare | TrackSource::ScreenShareAudio => "screen".to_string(),
                TrackSource::Unknown | TrackSource::Unrecognized(_) => "unknown".to_string(),
            }
        } else {
            req.stream.clone()
        };
        let backup_codec_policy = if req.kind == TrackKind::Audio
            && req.backup_codec_policy != BackupCodecPolicy::Regression
        {
            BackupCodecPolicy::Simulcast
        } else {
            req.backup_codec_policy
        };

        TrackInfo {
            sid: self.track_sid_for(req),
            kind: req.kind,
            name: req.name.clone(),
            muted: req.muted,
            width: req.width,
            height: req.height,
            simulcast,
            source: req.source,
            layers,
            mime_type: codecs
                .first()
                .map(|c| c.mime_type.clone())
                .unwrap_or_default(),
            mid: String::new(),
            codecs,
            disable_red: req.disable_red,
            disable_dtx: req.disable_dtx,
            stereo: req.stereo,
            encryption: req.encryption,
            stream,
            version: TimedVersion::now(),
            audio_features,
            packet_trailer_features,
            backup_codec_policy,
        }
    }

    /// `TR_` + kind (`V`/`A`) + source (`C`/`M`/`S`/`s`) + 12 random chars.
    fn track_sid_for(&self, req: &AddTrack) -> TrackSid {
        let kind = match req.kind {
            TrackKind::Video => "V",
            _ => "A",
        };
        let source = match req.source {
            TrackSource::Camera => "C",
            TrackSource::Microphone => "M",
            TrackSource::ScreenShare => "S",
            TrackSource::ScreenShareAudio => "s",
            TrackSource::Unknown | TrackSource::Unrecognized(_) => "",
        };
        TrackSid(crate::ids::new_sid(&format!(
            "{}{kind}{source}",
            crate::ids::TRACK_PREFIX
        )))
    }

    fn codec_enabled(&self, mime: &str) -> bool {
        self.info
            .enabled_codecs
            .iter()
            .any(|c| c.mime.eq_ignore_ascii_case(mime))
    }

    fn fallback_codec(&self, kind: TrackKind) -> String {
        let prefix = if kind == TrackKind::Audio {
            "audio/"
        } else {
            "video/"
        };
        self.info
            .enabled_codecs
            .iter()
            .find(|c| {
                c.mime.to_ascii_lowercase().starts_with(prefix)
                    && !c.mime.eq_ignore_ascii_case("video/rtx")
                    && !c.mime.eq_ignore_ascii_case("audio/red")
            })
            .map(|c| c.mime.clone())
            .unwrap_or_else(|| {
                if kind == TrackKind::Audio {
                    "audio/opus".into()
                } else {
                    "video/VP8".into()
                }
            })
    }

    /// Match remote media to a pending track (signaling.md §4.1).
    pub(super) fn remote_media_added(&mut self, media: RemoteMedia, now: Instant) {
        let sid = media.participant.clone();
        // A second codec belongs to the already published logical track. It is
        // not another pending publication and must never consume the next
        // same-kind AddTrack as a fallback.
        let additional = self.participants.get_mut(&sid).and_then(|p| {
            if p.is_disconnected()
                || p.publish_transport() != media.transport
                || p.is_released_cid(&media.msid_track_id)
            {
                return None;
            }
            let pos = p.info.tracks.iter().position(|track| {
                track.kind == media.kind && track_has_cid(track, &media.msid_track_id)
            })?;
            if !p.grants.video.can_publish_source(p.info.tracks[pos].source) {
                return None;
            }
            let track = &mut p.info.tracks[pos];
            let codec = track.codecs.iter_mut().find(|codec| {
                codec.cid == media.msid_track_id || codec.sdp_cid == media.msid_track_id
            })?;
            codec.mid = media.mid.clone();
            codec.sdp_cid = media.msid_track_id.clone();
            if codec.mime_type.is_empty() {
                codec.mime_type = media.mime_type.clone();
            }
            apply_sdp_rids(&mut codec.layers, &media.rids);
            track.version.advance();
            let info = track.clone();
            p.mark_dirty();
            Some(info)
        });
        if let Some(info) = additional {
            let primary_cid = info
                .codecs
                .first()
                .map(|codec| codec.cid.as_str())
                .unwrap_or("");
            let spec = published_spec(&info, primary_cid);
            self.media(MediaCommand::ExpectTrack {
                transport: media.transport,
                track: spec.clone(),
            });
            self.media(MediaCommand::BindTrack {
                transport: media.transport,
                mid: media.mid,
                track_sid: info.sid.clone(),
            });
            self.media(MediaCommand::UpdatePublishedTrack { track: spec });
            return;
        }
        // A known published CID rejected above (for example after permission
        // revocation) is never eligible to consume another pending track.
        if self.participants.get(&sid).is_some_and(|participant| {
            participant
                .info
                .tracks
                .iter()
                .any(|track| track.kind == media.kind && track_has_cid(track, &media.msid_track_id))
        }) {
            return;
        }
        let matched = {
            let Some(p) = self.participants.get_mut(&sid) else {
                return;
            };
            // A media event may outlive the transport/publication that produced
            // it. In particular a released cid must never fall back to a new
            // pending track merely because both are audio or both are video.
            if p.is_disconnected()
                || p.publish_transport() != media.transport
                || p.is_released_cid(&media.msid_track_id)
            {
                return;
            }
            let by_cid = p
                .pending
                .iter()
                .position(|t| t.request.cid == media.msid_track_id && t.info.kind == media.kind);
            let by_codec_cid = || {
                p.pending.iter().position(|t| {
                    t.info.kind == media.kind
                        && t.info.codecs.iter().any(|c| c.cid == media.msid_track_id)
                })
            };
            let idx = by_cid
                .or_else(by_codec_cid)
                .or_else(|| p.pending.iter().position(|t| t.info.kind == media.kind));
            match idx {
                Some(i) => Some(p.pending.remove(i)),
                None => {
                    // Bound unmatched SDP announcements as well as AddTrack
                    // requests; renegotiation must not grow this buffer forever.
                    if p.parked_media.len() < MAX_PENDING_TRACKS
                        && !p.parked_media.iter().any(|parked| {
                            parked.transport == media.transport && parked.mid == media.mid
                        })
                    {
                        p.parked_media.push(media.clone());
                    }
                    None
                }
            }
        };
        let Some(pending) = matched else { return };
        // Authorization is checked again when media becomes a publication, not
        // only when AddTrack was accepted before an admin permission update.
        if !self
            .participants
            .get(&sid)
            .is_some_and(|p| p.grants.video.can_publish_source(pending.info.source))
        {
            self.release_pending_track(&sid, pending, now);
            return;
        }

        let original = PendingTrack {
            request: pending.request.clone(),
            info: pending.info.clone(),
        };
        let mut info = pending.info;
        info.mid = media.mid.clone();
        if !media.mime_type.is_empty() {
            info.mime_type = media.mime_type.clone();
        }
        let codec_index = info
            .codecs
            .iter()
            .position(|codec| {
                codec.cid == media.msid_track_id || codec.sdp_cid == media.msid_track_id
            })
            .unwrap_or(0);
        if let Some(first) = info.codecs.get_mut(codec_index) {
            first.mid = media.mid.clone();
            if first.mime_type.is_empty() {
                first.mime_type = info.mime_type.clone();
            }
            if media.msid_track_id != pending.request.cid {
                first.sdp_cid = media.msid_track_id.clone();
            }
        }
        apply_sdp_rids(&mut info.layers, &media.rids);
        if let Some(first) = info.codecs.get_mut(codec_index) {
            apply_sdp_rids(&mut first.layers, &media.rids);
        }
        info.version.advance();

        self.media(MediaCommand::BindTrack {
            transport: media.transport,
            mid: media.mid.clone(),
            track_sid: info.sid.clone(),
        });
        self.media(MediaCommand::UpdatePublishedTrack {
            track: published_spec(&info, &pending.request.cid),
        });
        let track_sid = info.sid.clone();
        let sdp_registered = media.registration
            == crate::media_api::PublicationRegistration::SdpReceiver
            && !media.mime_type.is_empty()
            && (info.kind != TrackKind::Video
                || !self.participants[&sid]
                    .capabilities
                    .video_publication_requires_rtp);
        // Fixed Go OnTrack timing (see DeferredPublication): an SDP receiver publishes once the
        // publisher transport is connected (immediately on a renegotiation); RID or undeclared
        // sections publish on their first RTP.
        let needs_rtp =
            media.registration != crate::media_api::PublicationRegistration::SdpReceiver;
        if !needs_rtp && self.publisher_connected.contains(&sid) {
            self.publish(&sid, info, now);
            if sdp_registered {
                self.publication_observed(&sid, &track_sid, now);
            }
            return;
        }
        if let Some(p) = self.participants.get_mut(&sid) {
            p.deferred_publications
                .push(crate::room::participant::DeferredPublication {
                    pending: original,
                    info,
                    sdp_registered,
                    needs_rtp,
                });
        }
    }

    /// Publish deferred publications whose fixed Go OnTrack moment has come: all SDP receivers once
    /// the publisher transport is connected (`first_rtp: None`), or the one track whose first RTP
    /// arrived (`first_rtp: Some(track)`).
    pub(super) fn release_deferred_publications(
        &mut self,
        sid: &ParticipantSid,
        first_rtp: Option<&TrackSid>,
        now: Instant,
    ) {
        let connected = self.publisher_connected.contains(sid);
        let ready = {
            let Some(p) = self.participants.get_mut(sid) else {
                return;
            };
            if p.is_disconnected() {
                return;
            }
            let (ready, waiting): (Vec<_>, Vec<_>) = std::mem::take(&mut p.deferred_publications)
                .into_iter()
                .partition(|d| match first_rtp {
                    Some(track) => d.info.sid == *track,
                    None => connected && !d.needs_rtp,
                });
            p.deferred_publications = waiting;
            ready
        };
        for d in ready {
            let track_sid = d.info.sid.clone();
            self.publish(sid, d.info, now);
            if d.sdp_registered {
                self.publication_observed(sid, &track_sid, now);
            }
        }
    }

    /// The client stopped a section whose publication never became visible: release it like a
    /// pending track (no TrackUnpublished webhook, it was never published). Returns whether it
    /// was deferred.
    pub(super) fn cancel_deferred_publication(
        &mut self,
        sid: &ParticipantSid,
        track_sid: &TrackSid,
        now: Instant,
    ) -> bool {
        let pending = {
            let Some(p) = self.participants.get_mut(sid) else {
                return false;
            };
            let Some(pos) = p
                .deferred_publications
                .iter()
                .position(|d| d.info.sid == *track_sid)
            else {
                return false;
            };
            p.deferred_publications.remove(pos).pending
        };
        self.release_pending_track(sid, pending, now);
        true
    }

    /// Cancel an acknowledged publication whose media has not arrived yet.
    /// There is no TrackUnpublished webhook because it never became published.
    pub(super) fn release_pending_track(
        &mut self,
        sid: &ParticipantSid,
        pending: PendingTrack,
        now: Instant,
    ) {
        self.publication_cancelled(sid, &pending.info.sid);
        let supports_unpublish = match self.participants.get_mut(sid) {
            Some(p) => {
                let mut cids = vec![pending.request.cid.clone()];
                for codec in &pending.info.codecs {
                    cids.push(codec.cid.clone());
                    cids.push(codec.sdp_cid.clone());
                }
                p.remember_released_cids(cids);
                let released = &p.released_cids;
                p.parked_media.retain(|m| {
                    !released
                        .iter()
                        .any(|group| group.contains(&m.msid_track_id))
                });
                p.capabilities.unpublish
            }
            None => return,
        };
        self.media(MediaCommand::UnpublishTrack {
            track_sid: pending.info.sid.clone(),
        });
        if supports_unpublish {
            self.signal(
                sid,
                SignalEvent::TrackUnpublished {
                    track_sid: pending.info.sid,
                },
            );
        } else {
            self.signal(
                sid,
                SignalEvent::Mute {
                    track_sid: pending.info.sid,
                    muted: true,
                },
            );
        }
        let next = self.participants.get_mut(sid).and_then(|p| {
            p.queued
                .get_mut(&pending.request.cid)
                .and_then(|q| q.pop_front())
        });
        if let Some(next) = next {
            self.add_track_counted(sid, next, now, false);
        }
    }

    fn publish(&mut self, sid: &ParticipantSid, track: TrackInfo, now: Instant) {
        let muted = track.muted;
        let track_sid = track.sid.clone();
        {
            let Some(p) = self.participants.get_mut(sid) else {
                return;
            };
            p.info.tracks.push(track.clone());
            p.info.is_publisher = true;
            p.mark_dirty();
        }
        if muted {
            self.media(MediaCommand::SetPublisherMuted {
                track_sid: track_sid.clone(),
                muted: true,
            });
        }
        self.lifecycle.track_publish_success += 1;
        self.track_owner.insert(track_sid.clone(), sid.clone());
        // Publisher gets no update for its own publication; others do, now.
        self.broadcast_to_others(sid);
        self.schedule_room_update(now);

        let participant = match self.participants.get_mut(sid) {
            Some(p) => p.snapshot(),
            None => return,
        };
        let room = self.webhook_room();
        self.event(EngineEvent::TrackPublished {
            room,
            participant: webhook_participant(&participant),
            track,
        });

        // Active auto-subscribers pick the new track up.
        let subscribers: Vec<ParticipantSid> = self
            .participants
            .iter()
            .filter(|(s, p)| {
                *s != sid && p.is_active() && p.auto_subscribe && p.info.permission.can_subscribe
            })
            .map(|(s, _)| s.clone())
            .collect();
        for subscriber in subscribers {
            self.subscribe(&subscriber, &track_sid, now);
        }
        // Explicit subscriptions requested before the track existed.
        let waiting: Vec<ParticipantSid> = self
            .participants
            .iter()
            .filter(|(s, p)| {
                *s != sid
                    && p.subscriptions
                        .get(&track_sid)
                        .is_some_and(|sub| !sub.requested)
            })
            .map(|(s, _)| s.clone())
            .collect();
        for subscriber in waiting {
            self.resolve_subscription(&subscriber, &track_sid, now);
        }
    }

    /// Remove a published track. `server_initiated` tells the publisher with
    /// TrackUnpublished (v>6) or a server mute (older clients).
    pub(super) fn unpublish_track(
        &mut self,
        sid: &ParticipantSid,
        track_sid: &TrackSid,
        server_initiated: bool,
        now: Instant,
    ) {
        let (track, supports_unpublish, cid) = {
            let Some(p) = self.participants.get_mut(sid) else {
                return;
            };
            let Some(pos) = p.info.tracks.iter().position(|t| &t.sid == track_sid) else {
                return;
            };
            let track = p.info.tracks.remove(pos);
            // Source keeps publisher identity sticky after normal unpublish.
            // SetPermission is the explicit path that may recompute it false.
            p.track_subscribed_sent.remove(track_sid);
            p.mark_dirty();
            let cid = track
                .codecs
                .first()
                .map(|c| c.cid.clone())
                .unwrap_or_default();
            (track, p.capabilities.unpublish, cid)
        };
        self.publication_cleared(sid, track_sid, now);
        self.track_owner.remove(track_sid);
        self.media(MediaCommand::UnpublishTrack {
            track_sid: track_sid.clone(),
        });
        for p in self.participants.values_mut() {
            if let Some(sub) = p.subscriptions.get_mut(track_sid) {
                if sub.desired && !sub.event_reported {
                    self.lifecycle.track_subscribe_cancels += 1;
                    sub.event_reported = true;
                }
                sub.requested = false;
                sub.bound = false;
                sub.prepared = false;
            }
        }
        if server_initiated {
            if supports_unpublish {
                self.signal(
                    sid,
                    SignalEvent::TrackUnpublished {
                        track_sid: track_sid.clone(),
                    },
                );
            } else {
                self.signal(
                    sid,
                    SignalEvent::Mute {
                        track_sid: track_sid.clone(),
                        muted: true,
                    },
                );
            }
        }
        self.broadcast_to_others(sid);
        self.schedule_room_update(now);

        let participant = match self.participants.get_mut(sid) {
            Some(p) => p.snapshot(),
            None => return,
        };
        let room = self.webhook_room();
        self.event(EngineEvent::TrackUnpublished {
            room,
            participant: webhook_participant(&participant),
            track,
        });

        // A queued AddTrack with the same cid can go now.
        let next = self
            .participants
            .get_mut(sid)
            .and_then(|p| p.queued.get_mut(&cid).and_then(|q| q.pop_front()));
        if let Some(next) = next {
            self.add_track_counted(sid, next, now, false);
        }
    }

    /// Publisher or admin mute. Returns the updated track, `None` when unknown.
    pub(super) fn set_track_muted(
        &mut self,
        sid: &ParticipantSid,
        track_sid: &TrackSid,
        muted: bool,
        now: Instant,
    ) -> Option<TrackInfo> {
        let (published, changed, info) = {
            let p = self.participants.get_mut(sid)?;
            if let Some(track) = p.published_mut(track_sid) {
                let changed = track.muted != muted;
                track.muted = muted;
                if changed {
                    track.version.advance();
                }
                let info = track.clone();
                if changed {
                    p.mark_dirty();
                }
                (true, changed, info)
            } else {
                let pending = p.pending.iter_mut().find(|t| &t.info.sid == track_sid)?;
                pending.info.muted = muted;
                pending.request.muted = muted;
                (false, false, pending.info.clone())
            }
        };
        if let Some(watch) = self
            .publication_monitors
            .get_mut(&(sid.clone(), track_sid.clone()))
        {
            watch.muted(muted, now);
        }
        if published && changed {
            self.media(MediaCommand::SetPublisherMuted {
                track_sid: track_sid.clone(),
                muted,
            });
            self.broadcast_participant(sid, true, now);
        }
        Some(info)
    }

    pub(super) fn update_audio_track(
        &mut self,
        sid: &ParticipantSid,
        track_sid: &TrackSid,
        mut features: Vec<AudioFeature>,
        now: Instant,
    ) -> bool {
        // Go DedupeSlice sorts/compacts in place. Compact clears the old tail
        // to enum zero, and UpdateAudioTrack then ranges the ORIGINAL-length
        // update.Features slice. Any removed duplicate therefore also sets
        // TF_STEREO, even when zero is absent from the compacted metadata.
        let original_len = features.len();
        features.sort_by_key(|feature| feature.as_i32());
        features.dedup();
        let stereo = features.contains(&AudioFeature::Stereo) || features.len() < original_len;
        let disable_dtx = features.contains(&AudioFeature::NoDtx);
        {
            let Some(p) = self.participants.get_mut(sid) else {
                return false;
            };
            if let Some(track) = p.published_mut(track_sid) {
                // Receiver ignores published non-AUDIO and proto-equal updates.
                // UpdatePublishedAudioTrack still invokes its outer callback.
                if track.kind == TrackKind::Audio
                    && (track.audio_features != features
                        || track.stereo != stereo
                        || track.disable_dtx != disable_dtx)
                {
                    track.stereo = stereo;
                    track.disable_dtx = disable_dtx;
                    track.audio_features = features;
                    // Source UpdateAudioTrack does not rewrite TrackInfo.Version.
                }
                p.mark_dirty();
            } else if let Some(pending) = p.pending.iter_mut().find(|t| &t.info.sid == track_sid) {
                // The source pending branch intentionally has no kind guard.
                pending.info.audio_features = features;
                pending.info.stereo = stereo;
                pending.info.disable_dtx = disable_dtx;
                return true;
            } else {
                return false;
            }
        }
        // This callback is unconditional for an existing published track, even
        // when its receiver made no mutation (same fields or non-AUDIO kind).
        self.broadcast_participant(sid, false, now);
        true
    }

    pub(super) fn update_video_track(
        &mut self,
        sid: &ParticipantSid,
        track_sid: &TrackSid,
        width: u32,
        height: u32,
        now: Instant,
    ) -> bool {
        let published = {
            let Some(p) = self.participants.get_mut(sid) else {
                return false;
            };
            if let Some(track) = p.published_mut(track_sid) {
                // The fixed receiver ignores non-VIDEO and proto-equal updates;
                // the outer UpTrackManager callback still dirties/broadcasts.
                if track.kind == TrackKind::Video
                    && (track.width != width || track.height != height)
                {
                    track.width = width;
                    track.height = height;
                    // Source receiver retains TrackInfo.Version.
                }
                p.mark_dirty();
                true
            } else if let Some(pending) = p.pending.iter_mut().find(|t| &t.info.sid == track_sid) {
                pending.info.width = width;
                pending.info.height = height;
                return true;
            } else {
                return false;
            }
        };
        if published {
            self.broadcast_participant(sid, false, now);
        }
        true
    }

    /// Room as carried by track webhooks: minimal unless configured otherwise.
    fn webhook_room(&self) -> RoomInfo {
        let full = self.info();
        if self.policy.enable_full_room_in_webhooks {
            return full;
        }
        RoomInfo {
            sid: full.sid,
            name: full.name,
            empty_timeout_s: 0,
            departure_timeout_s: 0,
            max_participants: 0,
            creation_time_ms: 0,
            metadata: String::new(),
            num_participants: 0,
            num_publishers: 0,
            active_recording: false,
            enabled_codecs: Vec::new(),
            version: TimedVersion::default(),
            turn_password: String::new(),
        }
    }
}

/// Track webhooks carry only `{sid, identity}` of the participant.
fn webhook_participant(p: &ParticipantInfo) -> ParticipantInfo {
    ParticipantInfo {
        sid: p.sid.clone(),
        identity: p.identity.clone(),
        name: String::new(),
        state: ParticipantState::Joining,
        tracks: Vec::new(),
        data_tracks: Vec::new(),
        metadata: String::new(),
        attributes: Default::default(),
        joined_at_ms: 0,
        version: 0,
        permission: p.permission.clone(),
        region: String::new(),
        is_publisher: false,
        kind: ParticipantKind::Standard,
        disconnect_reason: DisconnectReason::Unknown,
        client_protocol: 0,
    }
}

pub(super) fn published_spec(track: &TrackInfo, cid: &str) -> PublishedTrackSpec {
    PublishedTrackSpec {
        backup_codec_policy: track.backup_codec_policy,
        sid: track.sid.clone(),
        kind: track.kind,
        source: track.source,
        cid: cid.to_string(),
        layers: track.layers.clone(),
        codecs: track.codecs.clone(),
        stereo: track.stereo || track.audio_features.contains(&AudioFeature::Stereo),
        disable_dtx: track.disable_dtx || track.audio_features.contains(&AudioFeature::NoDtx),
        disable_red: track.disable_red,
        encrypted: track.encryption != EncryptionType::None,
        muted: track.muted,
    }
}

fn track_has_cid(track: &TrackInfo, cid: &str) -> bool {
    track
        .codecs
        .iter()
        .any(|c| c.cid == cid || c.sdp_cid == cid)
}

fn qualify_mime(kind: TrackKind, codec: &str) -> String {
    if codec.contains('/') {
        return codec.to_string();
    }
    match kind {
        TrackKind::Audio => format!("audio/{codec}"),
        _ => format!("video/{codec}"),
    }
}

fn is_svc(mime: &str) -> bool {
    let m = mime.to_ascii_lowercase();
    m == "video/vp9" || m == "video/av1"
}

/// Assign rids from `a=simulcast` (low → high) to layers sorted by quality and set
/// the spatial index with presence-aware mapping (sfu-media.md §1.3).
fn apply_sdp_rids(layers: &mut [VideoLayer], rids: &[String]) {
    let n = layers.len();
    for (i, layer) in layers.iter_mut().enumerate() {
        layer.spatial_layer = if n <= 1 { 0 } else { i as i32 };
        if let Some(rid) = rids.get(i) {
            layer.rid = rid.clone();
        } else if layer.rid.is_empty() && n > 1 {
            layer.rid = ["q", "h", "f"]
                .get(i)
                .map(|s| s.to_string())
                .unwrap_or_default();
        }
    }
}
