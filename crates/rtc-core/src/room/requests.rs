//! Client request dispatch (signaling.md §12.1).

use std::time::{Duration, Instant};

use super::*;
use crate::media_api::{MediaCommand, PcRole};
use crate::session::{RequestOutcome, SdpType, SessionRequest, SignalTarget, Simulate, SyncState};

impl Room {
    pub fn handle_request(
        &mut self,
        sid: &ParticipantSid,
        generation: SessionGeneration,
        req: SessionRequest,
        now: Instant,
    ) {
        match self.participants.get(sid) {
            Some(p) if p.generation == generation && !p.is_disconnected() => {}
            _ => return, // stale or unknown session
        }
        if let Some(p) = self.participants.get_mut(sid) {
            p.fallback.signal(now);
        }
        match req {
            SessionRequest::PrepareOneShotOffer { sdp, id } => {
                if let Some(p) = self
                    .participants
                    .get(sid)
                    .filter(|p| p.capabilities.one_shot)
                {
                    self.media(MediaCommand::PrepareOneShotOffer {
                        transport: p.publish_transport(),
                        sdp,
                        id,
                    });
                }
            }
            SessionRequest::FinalizeOneShotAnswer { id } => {
                if let Some(p) = self
                    .participants
                    .get(sid)
                    .filter(|p| p.capabilities.one_shot)
                {
                    self.media(MediaCommand::FinalizeOneShotAnswer {
                        transport: p.publish_transport(),
                        id,
                    });
                }
            }
            SessionRequest::Offer { sdp, id } => {
                let transport = self.participants.get(sid).map(|p| p.publish_transport());
                if let Some(transport) = transport {
                    self.media(MediaCommand::RemoteDescription {
                        transport,
                        kind: SdpType::Offer,
                        sdp,
                        id,
                    });
                }
            }
            SessionRequest::Answer { sdp, id } => {
                let transport = self
                    .participants
                    .get(sid)
                    .and_then(|p| p.transport(PcRole::Subscriber));
                match transport {
                    Some(transport) => self.media(MediaCommand::RemoteDescription {
                        transport,
                        kind: SdpType::Answer,
                        sdp,
                        id,
                    }),
                    None => {
                        tracing::warn!(participant = %sid, "answer without subscriber transport ignored")
                    }
                }
            }
            SessionRequest::Trickle {
                token: _,
                candidate,
                target,
                last: _,
            } => {
                let transport = self.participants.get(sid).and_then(|p| match target {
                    SignalTarget::Publisher => Some(p.publish_transport()),
                    SignalTarget::Subscriber => p.transport(PcRole::Subscriber),
                });
                if let Some(transport) = transport {
                    self.media(MediaCommand::RemoteCandidate {
                        transport,
                        candidate,
                    });
                }
            }
            SessionRequest::AddTrack(add) => self.add_track(sid, add, now),
            SessionRequest::PublishDataTrack(request) => self.publish_data_track(sid, request, now),
            SessionRequest::StoreDataBlob { request_id, blob } => {
                self.store_data_blob(sid, request_id, blob)
            }
            SessionRequest::GetDataBlob {
                request_id,
                participant_identity,
                key,
            } => self.get_data_blob(sid, request_id, participant_identity, key),
            SessionRequest::UnpublishDataTrack { token, pub_handle } => {
                self.unpublish_data_track(sid, token, pub_handle, now)
            }
            SessionRequest::DataSubscription(updates) => {
                self.update_data_subscriptions(sid, updates)
            }
            SessionRequest::Mute {
                token,
                sid: track,
                muted,
            } => {
                if self.set_track_muted(sid, &track, muted, now).is_none() {
                    self.request_response(
                        sid,
                        token,
                        0,
                        RequestOutcome::NotFound,
                        "track not found",
                    );
                }
            }
            SessionRequest::Subscription {
                track_sids,
                subscribe,
            } => {
                for t in track_sids {
                    if subscribe {
                        self.subscribe(sid, &t, now);
                    } else {
                        self.unsubscribe(sid, &t, now);
                    }
                }
            }
            SessionRequest::TrackSettings {
                track_sids,
                settings,
            } => {
                self.update_track_settings(sid, track_sids, settings, now);
            }
            SessionRequest::Leave { reason } => {
                // Upstream echoes the leave back, then removes the participant.
                self.remove_participant(sid, reason, true, now);
            }
            SessionRequest::SubscriptionPermission(perm) => {
                self.set_subscription_permission(sid, perm, now)
            }
            SessionRequest::SyncState(state) => self.sync_state(sid, state, now),
            SessionRequest::Simulate(sim) => self.simulate(sid, sim, now),
            SessionRequest::UpdateMetadata {
                token,
                request_id,
                name,
                metadata,
                attributes,
            } => {
                self.update_own_metadata(sid, token, request_id, name, metadata, attributes, now);
            }
            SessionRequest::UpdateAudioTrack {
                token,
                sid: track,
                features,
            } => {
                if !self.update_audio_track(sid, &track, features, now) {
                    self.request_response(sid, token, 0, RequestOutcome::NotFound, "");
                }
            }
            SessionRequest::UpdateVideoTrack {
                token,
                sid: track,
                width,
                height,
            } => {
                if !self.update_video_track(sid, &track, width, height, now) {
                    self.request_response(sid, token, 0, RequestOutcome::NotFound, "");
                }
            }
            SessionRequest::SignalActivity => {} // owner freshness was updated after the generation fence
            SessionRequest::SignalRtt(rtt) => {
                if let Some(p) = self.participants.get_mut(sid) {
                    p.signal_rtt_ms = rtt;
                    p.fallback.signal_rtt(rtt);
                }
                self.media(MediaCommand::SetSignalingRtt {
                    participant: sid.clone(),
                    rtt_ms: rtt,
                });
            }
            SessionRequest::Unsupported {
                token,
                request_id,
                what,
            } => {
                // Explicit refusal so the client never waits for an answer (C15).
                self.request_response_always(
                    sid,
                    token,
                    request_id,
                    RequestOutcome::NotAllowed,
                    &format!("{what} not enabled"),
                );
            }
        }
    }

    /// Error responses: only for clients that support RequestResponse.
    pub(super) fn request_response(
        &mut self,
        sid: &ParticipantSid,
        token: u32,
        request_id: u32,
        outcome: RequestOutcome,
        message: &str,
    ) {
        let ok = self.participants.get(sid).is_some_and(|p| {
            p.capabilities.request_response
                && (outcome != RequestOutcome::Ok || p.capabilities.non_error_signal_response)
        });
        if ok {
            self.signal(
                sid,
                SignalEvent::RequestResponse {
                    token,
                    request_id,
                    outcome,
                    message: message.to_string(),
                },
            );
        }
    }

    fn request_response_always(
        &mut self,
        sid: &ParticipantSid,
        token: u32,
        request_id: u32,
        outcome: RequestOutcome,
        message: &str,
    ) {
        if self
            .participants
            .get(sid)
            .is_some_and(|p| p.capabilities.request_response)
        {
            self.signal(
                sid,
                SignalEvent::RequestResponse {
                    token,
                    request_id,
                    outcome,
                    message: message.to_string(),
                },
            );
        }
    }

    /// `update_metadata` (participant.go:709-768).
    #[allow(clippy::too_many_arguments)]
    fn update_own_metadata(
        &mut self,
        sid: &ParticipantSid,
        token: u32,
        request_id: u32,
        name: String,
        metadata: String,
        attributes: std::collections::BTreeMap<String, String>,
        now: Instant,
    ) {
        let allowed = self
            .participants
            .get(sid)
            .is_some_and(|p| p.info.permission.can_update_metadata);
        if !allowed {
            return self.request_response_always(
                sid,
                token,
                request_id,
                RequestOutcome::NotAllowed,
                "does not have permission to update own metadata",
            );
        }
        let policy = self.policy.clone();
        let limits = policy.as_ref();
        if !crate::config::signed_byte_length_allowed(
            limits.max_participant_name_length,
            name.len(),
        ) {
            return self.request_response_always(
                sid,
                token,
                request_id,
                RequestOutcome::LimitExceeded,
                "exceeds name length limit",
            );
        }
        if limits.max_metadata_size.is_some_and(|m| metadata.len() > m) {
            return self.request_response_always(
                sid,
                token,
                request_id,
                RequestOutcome::LimitExceeded,
                "exceeds metadata size limit",
            );
        }
        let attrs_size: usize = attributes.iter().map(|(k, v)| k.len() + v.len()).sum();
        if limits.max_attributes_size.is_some_and(|m| attrs_size > m) {
            return self.request_response_always(
                sid,
                token,
                request_id,
                RequestOutcome::LimitExceeded,
                "exceeds attributes size limit",
            );
        }
        let changed = self.apply_identity_update(sid, &name, &metadata, &attributes);
        if changed {
            self.broadcast_participant(sid, true, now);
            if let Some(p) = self.participants.get_mut(sid) {
                let claims =
                    crate::session::SignalClaimsSnapshot::new(p.snapshot(), p.grants.video.clone());
                self.signal(sid, SignalEvent::ClaimsChanged(Box::new(claims)));
            }
        }
        self.request_response(sid, token, request_id, RequestOutcome::Ok, "");
    }

    /// Apply name/metadata/attributes: empty name/metadata = unchanged, empty
    /// attribute value deletes the key. Returns whether anything changed.
    pub(super) fn apply_identity_update(
        &mut self,
        sid: &ParticipantSid,
        name: &str,
        metadata: &str,
        attributes: &std::collections::BTreeMap<String, String>,
    ) -> bool {
        let Some(p) = self.participants.get_mut(sid) else {
            return false;
        };
        // Fixed ParticipantImpl.SetAttributes treats every nonempty map as
        // an accepted update, including same-value writes and absent-key deletes.
        // It always dirties claims and emits the participant/claims callbacks.
        let mut changed = !attributes.is_empty();
        if !name.is_empty() && p.info.name != name {
            p.info.name = name.to_string();
            changed = true;
        }
        if !metadata.is_empty() && p.info.metadata != metadata {
            p.info.metadata = metadata.to_string();
            changed = true;
        }
        for (k, v) in attributes {
            if v.is_empty() {
                changed |= p.info.attributes.remove(k).is_some();
            } else if p.info.attributes.get(k) != Some(v) {
                p.info.attributes.insert(k.clone(), v.clone());
                changed = true;
            }
        }
        if changed {
            p.mark_dirty();
        }
        changed
    }

    /// SyncState after resume (signaling.md §11.1 step 8).
    fn sync_state(&mut self, sid: &ParticipantSid, state: SyncState, now: Instant) {
        let missing = match self.participants.get(sid) {
            Some(p) => state
                .publish_tracks
                .iter()
                .any(|t| p.published(t).is_none() && !p.pending.iter().any(|pt| &pt.info.sid == t)),
            None => return,
        };
        let data_missing = self.participants.get(sid).is_none_or(|p| {
            state
                .publish_data_tracks
                .iter()
                .any(|track| !p.info.data_tracks.iter().any(|t| &t.sid == track))
        });
        if missing || data_missing {
            return self.full_reconnect(sid, now);
        }
        if !state.track_sids_disabled.is_empty() {
            let settings = TrackSettings {
                disabled: true,
                ..Default::default()
            };
            self.update_track_settings(sid, state.track_sids_disabled, settings, now);
        }
        for t in state.subscription_track_sids {
            if state.subscribe {
                self.subscribe(sid, &t, now);
            } else {
                self.unsubscribe(sid, &t, now);
            }
        }
    }

    /// Leave RECONNECT/STATE_MISMATCH and close the session (signaling.md §8).
    pub(super) fn full_reconnect(&mut self, sid: &ParticipantSid, now: Instant) {
        self.send_leave(sid, DisconnectReason::StateMismatch, LeaveAction::Reconnect);
        self.remove_participant(sid, DisconnectReason::StateMismatch, false, now);
    }

    fn simulate(&mut self, sid: &ParticipantSid, sim: Simulate, now: Instant) {
        match sim {
            Simulate::ServerLeave => {
                self.remove_participant(sid, DisconnectReason::ServerShutdown, true, now);
            }
            // Fixed Go room.onSimulateScenario: participant.Close(sendLeave=false, …,
            // isExpectedToResume=true). The participant is closed without a leave, so the
            // client's resume finds no session and performs a full reconnect (new SID), and
            // other participants see it leave and rejoin. Reasons follow ToDisconnectReason:
            // SimulateNodeFailure → SERVER_SHUTDOWN, SimulateMigration → MIGRATION.
            Simulate::NodeFailure => {
                self.remove_participant(sid, DisconnectReason::ServerShutdown, false, now);
            }
            Simulate::Migration => {
                self.remove_participant(sid, DisconnectReason::Migration, false, now);
            }
            Simulate::DisconnectSignalOnResume | Simulate::DisconnectSignalOnResumeNoMessages => {
                if let Some(p) = self.participants.get_mut(sid) {
                    p.disconnect_on_resume = true;
                }
                self.effects.push(Effect::CloseSignal { to: sid.clone() });
            }
            Simulate::SubscriberBandwidth(bps) => {
                self.media(MediaCommand::SetBandwidthOverride {
                    participant: sid.clone(),
                    bps: bps.max(0) as u64,
                });
            }
            // Fixed Go: send an active speaker (level 0.9) now and the inactive update after
            // the requested seconds. The room's audio-level speaker state is not touched.
            Simulate::SpeakerUpdate { seconds } => {
                self.send_speaker_changes(vec![SpeakerInfo {
                    sid: sid.clone(),
                    level: 0.9,
                    active: true,
                }]);
                let delay = Duration::from_secs(seconds.max(0) as u64);
                self.simulated_speaker_clears
                    .push((now + delay, sid.clone()));
            }
            // Fixed Go participant.ICERestart: set the ICE candidate preference for both PCs
            // (configureICE with reset, remembered for the participant's next resume), then
            // restart ICE on the subscriber PC when one exists. ICT_TCP = 1, ICT_TLS = 2.
            Simulate::SwitchCandidateProtocol(kind) => {
                let preference = match kind {
                    1 => crate::transport_fallback::IcePreference::Tcp,
                    2 => crate::transport_fallback::IcePreference::Tls,
                    _ => crate::transport_fallback::IcePreference::Any,
                };
                let subscriber = match self.participants.get_mut(sid) {
                    Some(p) if !p.capabilities.one_shot => {
                        p.fallback.resume(preference, now);
                        p.transport(PcRole::Subscriber)
                    }
                    _ => return,
                };
                self.apply_ice_preference(sid, true, now);
                if let Some(t) = subscriber {
                    self.media(MediaCommand::IceRestart { transport: t });
                }
            }
            Simulate::LeaveRequestFullReconnect => {} // ignored upstream too
        }
    }
}
