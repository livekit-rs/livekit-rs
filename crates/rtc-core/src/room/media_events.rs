//! Media engine → room events.

use std::time::Instant;

use super::*;
use crate::events::EngineEvent;
use crate::media_api::MediaEvent;
use crate::session::SdpType;

impl Room {
    pub fn handle_media_event(&mut self, event: MediaEvent, now: Instant) {
        match event {
            MediaEvent::IceTcpConnectTimedOut {
                participant,
                transport,
                recovery_epoch,
            } => {
                if !self.participants.get(&participant).is_some_and(|p| {
                    p.media_recovery_epoch == recovery_epoch && p.has_transport(transport)
                }) {
                    return;
                }
                self.fallback_tcp_timed_out(&participant, transport, now);
                self.transport_failed(&participant, transport, now);
            }
            MediaEvent::IcePath {
                transport,
                connection,
                first_connected,
            } => self.ice_path(transport, connection, first_connected),
            MediaEvent::ReceiverLoss {
                recovery_epoch,
                transport,
                track,
                fraction_lost,
                received_at,
            } => {
                if self
                    .participants
                    .values()
                    .any(|p| p.has_transport(transport) && p.media_recovery_epoch == recovery_epoch)
                {
                    self.receiver_loss(transport, &track, fraction_lost, received_at);
                }
            }
            MediaEvent::SubscriberRtt {
                transport,
                track,
                rtt_ms,
                received_at,
            } => {
                self.subscriber_rtt(transport, &track, rtt_ms, received_at);
            }
            MediaEvent::DataFanoutCompleted { .. } => {}
            MediaEvent::DataWriteAdmission {
                request,
                to,
                kind,
                payload,
                expected_transport,
            } => self.admit_api_data_write(request, to, kind, payload, expected_transport),
            MediaEvent::JoiningReliableReplayed {
                participant,
                transport,
            } => self.joining_reliable_replayed(&participant, transport),
            MediaEvent::PrimaryTransportEstablished {
                participant,
                transport,
            } => {
                self.primary_transport_established(&participant, transport);
            }
            MediaEvent::SubscriptionRemoved {
                subscriber,
                track_sid,
            } => self.subscription_removed(&subscriber, &track_sid, now),
            MediaEvent::DataSendError {
                participant,
                transport,
            } => {
                if self.policy.reconnect_on_data_channel_error
                    && self
                        .participants
                        .get(&participant)
                        .is_some_and(|p| p.has_transport(transport) && !p.is_disconnected())
                {
                    self.full_reconnect(&participant, now);
                }
            }
            MediaEvent::TrackVideoSizeChanged {
                track_sid,
                mime_type,
                sizes,
            } => self.track_video_size_changed(&track_sid, &mime_type, &sizes, now),
            MediaEvent::RemoteDescriptionRejected {
                participant,
                id,
                message,
            } => {
                if self
                    .participants
                    .get(&participant)
                    .is_some_and(|p| p.capabilities.one_shot)
                {
                    self.signal(
                        &participant,
                        SignalEvent::RequestResponse {
                            token: id,
                            request_id: id,
                            outcome: crate::session::RequestOutcome::UnclassifiedError,
                            message,
                        },
                    );
                }
            }
            MediaEvent::SubscriptionPrepared {
                subscriber,
                track_sid,
            } => {
                if let Some(sub) = self
                    .participants
                    .get_mut(&subscriber)
                    .and_then(|p| p.subscriptions.get_mut(&track_sid))
                    && sub.requested
                {
                    sub.prepared = true;
                }
            }
            MediaEvent::LocalDescription {
                participant,
                kind,
                mut sdp,
                trickle_only,
                id,
                mid_to_track_id,
                ..
            } => {
                if trickle_only {
                    sdp = sdp
                        .split_inclusive('\n')
                        .filter(|line| {
                            !line.starts_with("a=candidate:")
                                && line.trim_end_matches(['\r', '\n']) != "a=end-of-candidates"
                        })
                        .collect();
                }
                let ev = match kind {
                    SdpType::Offer => SignalEvent::Offer {
                        sdp,
                        id,
                        mid_to_track_id,
                    },
                    SdpType::Answer => SignalEvent::Answer {
                        sdp,
                        id,
                        mid_to_track_id,
                    },
                };
                self.signal(&participant, ev);
            }
            MediaEvent::LocalCandidate {
                participant,
                target,
                candidate,
                last,
            } => {
                self.signal(
                    &participant,
                    SignalEvent::Trickle {
                        candidate,
                        target,
                        last,
                    },
                );
            }
            MediaEvent::MediaSectionsRequirement {
                participant,
                audios,
                videos,
            } => {
                self.signal(
                    &participant,
                    SignalEvent::MediaSectionsRequirement { audios, videos },
                );
            }
            MediaEvent::PublicationRtpRegistered {
                participant,
                transport,
                track_sid,
                mime_type,
                payload_type,
            } => {
                let track = self
                    .participants
                    .get(&participant)
                    .filter(|p| !p.is_disconnected() && p.publish_transport() == transport)
                    .and_then(|p| {
                        p.info
                            .tracks
                            .iter()
                            .find(|t| {
                                t.sid == track_sid
                                    && match t.kind {
                                        TrackKind::Audio => mime_type.starts_with("audio/"),
                                        TrackKind::Video => {
                                            mime_type.starts_with("video/")
                                                && (!p.capabilities.video_publication_requires_rtp
                                                    || payload_type != 0)
                                        }
                                        TrackKind::Data | TrackKind::Unrecognized(_) => false,
                                    }
                            })
                            .map(|t| t.sid.clone())
                    });
                if let Some(track) = track {
                    self.publication_observed(&participant, &track, now);
                }
            }
            MediaEvent::RemoteMediaAdded(media) => self.remote_media_added(media, now),
            MediaEvent::TrackMediaStarted {
                track_sid,
                mime_type,
                layers,
            } => {
                let deferred_owner = self
                    .participants
                    .iter()
                    .find(|(_, p)| {
                        p.deferred_publications
                            .iter()
                            .any(|d| d.info.sid == track_sid)
                    })
                    .map(|(sid, _)| sid.clone());
                if let Some(owner) = deferred_owner {
                    self.release_deferred_publications(&owner, Some(&track_sid), now);
                }
                self.track_media_started(&track_sid, mime_type, layers, now);
            }
            MediaEvent::RemoteMediaStopped {
                participant,
                track_sid,
            } => {
                if !self.cancel_deferred_publication(&participant, &track_sid, now) {
                    self.unpublish_track(&participant, &track_sid, false, now);
                }
            }
            MediaEvent::TransportEstablished {
                participant,
                transport,
            } => {
                if let Some(p) = self.participants.get_mut(&participant)
                    && p.has_transport(transport)
                {
                    p.established_transports.insert(transport);
                }
            }
            MediaEvent::TransportConnected {
                participant,
                transport,
            } => {
                self.transport_connected(&participant, transport, now);
            }
            MediaEvent::TransportFailed {
                participant,
                transport,
                recovery_epoch,
            } => {
                if !self.participants.get(&participant).is_some_and(|p| {
                    p.media_recovery_epoch == recovery_epoch && p.has_transport(transport)
                }) {
                    return;
                }
                self.transport_failed(&participant, transport, now);
            }
            MediaEvent::DataReceived {
                participant,
                kind,
                label,
                payload,
            } => {
                self.data_received(&participant, kind, &label, &payload, now);
            }
            MediaEvent::SubscriptionBound {
                subscriber,
                track_sid,
                ..
            } => {
                self.subscription_bound(&subscriber, &track_sid);
            }
            MediaEvent::SubscriptionFailed {
                subscriber,
                track_sid,
                error,
            } => {
                self.subscription_failed(&subscriber, &track_sid, error);
            }
            MediaEvent::StreamState {
                subscriber,
                publisher,
                track_sid,
                state,
            } => {
                self.signal(
                    &subscriber,
                    SignalEvent::StreamState(vec![StreamStateInfo {
                        participant_sid: publisher,
                        track_sid,
                        state,
                    }]),
                );
            }
            MediaEvent::SubscribedQuality {
                publisher,
                track_sid,
                codecs,
            } => {
                self.signal(
                    &publisher,
                    SignalEvent::SubscribedQuality { track_sid, codecs },
                );
            }
            MediaEvent::AudioLevels(levels) => self.handle_audio_levels(levels),
            MediaEvent::Quality(samples) => self.handle_quality(samples),
            MediaEvent::NegotiationFailed {
                participant,
                recovery_epoch,
            } => {
                if self
                    .participants
                    .get(&participant)
                    .is_some_and(|p| p.media_recovery_epoch == recovery_epoch)
                {
                    self.full_reconnect(&participant, now);
                }
            }
        }
    }

    fn track_media_started(
        &mut self,
        track_sid: &TrackSid,
        mime_type: String,
        layers: Vec<VideoLayer>,
        now: Instant,
    ) {
        let Some(owner) = self.track_owner.get(track_sid).cloned() else {
            return;
        };
        let changed = {
            let Some(p) = self.participants.get_mut(&owner) else {
                return;
            };
            let Some(track) = p.published_mut(track_sid) else {
                return;
            };
            let mut changed = false;
            let codec_index = track
                .codecs
                .iter()
                .position(|codec| codec.mime_type.eq_ignore_ascii_case(&mime_type))
                .unwrap_or(0);
            // SetupReceiver retains primary TrackInfo MIME/MID when a backup
            // receiver starts, including after codec regression.
            if codec_index == 0 && !mime_type.is_empty() && track.mime_type != mime_type {
                track.mime_type = mime_type.clone();
                changed = true;
            }
            if let Some(codec) = track.codecs.get_mut(codec_index) {
                if !mime_type.is_empty() && codec.mime_type != mime_type {
                    codec.mime_type = mime_type.clone();
                    changed = true;
                }
                if !layers.is_empty() && codec.layers != layers {
                    codec.layers = layers.clone();
                    changed = true;
                }
            }
            if codec_index == 0 && !layers.is_empty() && track.layers != layers {
                track.layers = layers;
                changed = true;
            }
            if changed {
                track.version.advance();
                p.mark_dirty();
            }
            changed
        };
        if changed {
            self.broadcast_participant(&owner, false, now);
        }
    }

    fn transport_connected(&mut self, sid: &ParticipantSid, transport: TransportId, now: Instant) {
        let became_active = {
            let Some(p) = self.participants.get_mut(sid) else {
                return;
            };
            if !p.has_transport(transport) {
                return;
            }
            // Connected is a stronger PC!=New fact, including for alternate
            // engine implementations; native Checking has its earlier producer.
            p.established_transports.insert(transport);
            if p.transports.iter().any(|(id, role)| {
                *id == transport
                    && matches!(
                        role,
                        crate::media_api::PcRole::Publisher | crate::media_api::PcRole::Single
                    )
            }) && self.publisher_connected.insert(sid.clone())
            {
                for ((owner, _), watch) in &mut self.publication_monitors {
                    if owner == sid {
                        watch.connected(now);
                    }
                }
            }
            p.ice_connected_once = true;
            p.transport_failed_at = None;
            if transport != p.primary || p.ever_active {
                false
            } else {
                p.ever_active = true;
                p.active_at = Some(now);
                p.set_state(ParticipantState::Active)
            }
        };
        if !became_active {
            self.release_deferred_publications(sid, None, now);
            return;
        }
        self.lifecycle.participant_rtc_active += 1;
        self.broadcast_participant(sid, false, now);
        let info = match self.participants.get_mut(sid) {
            Some(p) => p.snapshot(),
            None => return,
        };
        let room = self.info();
        self.event(EngineEvent::ParticipantJoined {
            room,
            participant: info,
        });
        self.subscribe_to_existing(sid, now);
        self.subscribe_existing_data_tracks(sid);
        self.release_deferred_publications(sid, None, now);
    }

    pub(super) fn transport_failed(
        &mut self,
        sid: &ParticipantSid,
        transport: TransportId,
        now: Instant,
    ) {
        self.fallback_transport_failed(sid, transport, now);
        if self
            .participants
            .get(sid)
            .is_some_and(|p| p.capabilities.one_shot && p.has_transport(transport))
        {
            self.remove_participant(sid, DisconnectReason::ConnectionTimeout, false, now);
            return;
        }
        let notify = {
            let Some(p) = self.participants.get_mut(sid) else {
                return;
            };
            if !p.has_transport(transport) || p.transport_failed_at.is_some() {
                return;
            }
            // Any failed transport starts the resume flow (participant.go:2792-2813).
            p.transport_failed_at = Some(now);
            p.signal_connected
        };
        if notify {
            self.send_leave(
                sid,
                DisconnectReason::ConnectionTimeout,
                LeaveAction::Resume,
            );
            self.effects.push(Effect::CloseSignal { to: sid.clone() });
        }
    }
}

impl Room {
    /// Fixed MediaTrackReceiver.UpdateVideoSize: strict maximum-width pairing,
    /// update only matching codec layers and preserve unknown/zero layer entries.
    fn track_video_size_changed(
        &mut self,
        sid: &TrackSid,
        mime: &str,
        sizes: &[(u32, u32)],
        _now: Instant,
    ) {
        if sizes.len() > 3 {
            return;
        }
        let Some(owner) = self.track_owner.get(sid).cloned() else {
            return;
        };
        let mut spec = None;
        if let Some(p) = self
            .participants
            .get_mut(&owner)
            .filter(|p| p.capabilities.one_shot)
            && let Some(track) = p.published_mut(sid).filter(|t| t.kind == TrackKind::Video)
            && track.apply_observed_video_sizes(mime, sizes)
        {
            spec = Some(super::publish::published_spec(
                track,
                track
                    .codecs
                    .first()
                    .map(|c| c.cid.as_str())
                    .unwrap_or_default(),
            ));
        }
        // Fixed source updates receiver TrackInfo/selection and telemetry only;
        // it does not generate a participant change or refresh the service store.
        if let Some(spec) = spec {
            self.media(MediaCommand::UpdatePublishedTrack { track: spec });
        }
    }
}
