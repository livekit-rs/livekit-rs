//! Server API operations on a room (api-auth-webhook.md §2.4).

use std::collections::BTreeMap;
use std::time::Instant;

use super::*;
use crate::media_api::DataKind;
use crate::permission::ParticipantPermission;
use crate::session::RequestOutcome;

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum OpError {
    /// Owner-side metadata validation rejected an existing participant update.
    /// Fixed signalling errors are ordinary errors, independent of HTTP preflight.
    #[error("{0}")]
    MetadataLimit(&'static str),
    #[error("{0}")]
    NotFound(&'static str),
    #[error("{0}")]
    FailedPrecondition(&'static str),
}

const PARTICIPANT_NOT_FOUND: &str = "participant does not exist";

impl Room {
    /// Owner-local OSS participant-store view. Reads do not refresh the snapshot.
    pub fn list_participants_stored(&self) -> Vec<ParticipantInfo> {
        self.stored_participants.values().cloned().collect()
    }

    /// Snapshot taken at the last participant-change callback-equivalent boundary.
    pub fn get_participant_stored(&self, identity: &Identity) -> Result<ParticipantInfo, OpError> {
        self.stored_participants
            .get(identity)
            .cloned()
            .ok_or(OpError::NotFound(PARTICIPANT_NOT_FOUND))
    }

    // OSS roommanager.OnParticipantChanged stores ToProto after real state/track/
    // metadata changes. Private video-size events deliberately never call this.
    pub(super) fn remember_participant_change(&mut self, sid: &ParticipantSid) {
        let Some(p) = self.participants.get_mut(sid) else {
            return;
        };
        if !p.is_disconnected() {
            let info = p.snapshot();
            self.stored_participants.insert(info.identity.clone(), info);
        }
    }

    pub fn list_participants(&mut self) -> Vec<ParticipantInfo> {
        self.participants
            .values_mut()
            .map(|p| p.snapshot())
            .collect()
    }

    /// Signal state of the exact participant generation, for owner-side
    /// termination cleanup that must not act on a resumed signal session.
    pub fn participant_signal_state(
        &self,
        sid: &ParticipantSid,
    ) -> Option<(SessionGeneration, bool)> {
        self.participants
            .get(sid)
            .map(|p| (p.generation, p.signal_connected))
    }

    pub fn get_participant(&mut self, identity: &Identity) -> Result<ParticipantInfo, OpError> {
        let sid = self
            .sid_by_identity(identity)
            .ok_or(OpError::NotFound(PARTICIPANT_NOT_FOUND))?;
        self.participants
            .get_mut(&sid)
            .map(|p| p.snapshot())
            .ok_or(OpError::NotFound(PARTICIPANT_NOT_FOUND))
    }

    pub fn api_remove_participant(
        &mut self,
        identity: &Identity,
        now: Instant,
    ) -> Result<(), OpError> {
        let sid = self
            .sid_by_identity(identity)
            .ok_or(OpError::NotFound(PARTICIPANT_NOT_FOUND))?;
        self.remove_participant(&sid, DisconnectReason::ParticipantRemoved, true, now);
        Ok(())
    }

    /// Admin mute: signals `mute` to the publisher then applies it. Unknown track
    /// returns `Ok(None)` like upstream.
    pub fn api_mute_track(
        &mut self,
        identity: &Identity,
        track: &TrackSid,
        muted: bool,
        now: Instant,
    ) -> Result<Option<TrackInfo>, OpError> {
        let sid = self
            .sid_by_identity(identity)
            .ok_or(OpError::NotFound(PARTICIPANT_NOT_FOUND))?;
        if !muted && !self.policy.enable_remote_unmute {
            return Err(OpError::FailedPrecondition("remote unmute not enabled"));
        }
        // The admin command is sent even if publication has not arrived (or the
        // track has already retired). Upstream SetTrackMuted signals first and
        // independently returns the optional published/pending TrackInfo.
        self.signal(
            &sid,
            SignalEvent::Mute {
                track_sid: track.clone(),
                muted,
            },
        );
        Ok(self.set_track_muted(&sid, track, muted, now))
    }

    pub fn api_update_participant(
        &mut self,
        identity: &Identity,
        name: &str,
        metadata: &str,
        attributes: &BTreeMap<String, String>,
        permission: Option<ParticipantPermission>,
        now: Instant,
    ) -> Result<ParticipantInfo, OpError> {
        let sid = self
            .sid_by_identity(identity)
            .ok_or(OpError::NotFound(PARTICIPANT_NOT_FOUND))?;
        // roommanager.UpdateParticipant resolves the participant first, then
        // calls UpdateMetadata(fromAdmin=true), before changing permission.
        let rejected = if !crate::config::signed_byte_length_allowed(
            self.policy.max_participant_name_length,
            name.len(),
        ) {
            Some(("name length exceeds limits", "exceeds name length limit"))
        } else if self
            .policy
            .max_metadata_size
            .is_some_and(|max| metadata.len() > max)
        {
            Some((
                "metadata size exceeds limits",
                "exceeds metadata size limit",
            ))
        } else if self.policy.max_attributes_size.is_some_and(|max| {
            attributes
                .iter()
                .map(|(k, v)| k.len() + v.len())
                .sum::<usize>()
                > max
        }) {
            Some((
                "attributes size exceeds limits",
                "exceeds attributes size limit",
            ))
        } else {
            None
        };
        if let Some((cause, message)) = rejected {
            // Source sends an error RequestResponse even for admin/request_id0.
            if self
                .participants
                .get(&sid)
                .is_some_and(|p| p.capabilities.request_response)
            {
                self.signal(
                    &sid,
                    SignalEvent::OwnedMetadataRequestResponse {
                        request: Box::new(crate::session::MetadataRequestEcho {
                            request_id: 0,
                            name: name.to_owned(),
                            metadata: metadata.to_owned(),
                            attributes: attributes.clone(),
                        }),
                        outcome: RequestOutcome::LimitExceeded,
                        message: message.to_owned(),
                    },
                );
            }
            return Err(OpError::MetadataLimit(cause));
        }
        let mut changed = self.apply_identity_update(&sid, name, metadata, attributes);

        if let Some(new_perm) = permission {
            let old_perm = self
                .participants
                .get(&sid)
                .map(|p| p.info.permission.clone())
                .ok_or(OpError::NotFound(PARTICIPANT_NOT_FOUND))?;
            if old_perm != new_perm {
                changed = true;
                if let Some(p) = self.participants.get_mut(&sid) {
                    // Grants are stored explicitly once the server rewrote them.
                    p.grants.video.can_publish = Some(new_perm.can_publish);
                    p.grants.video.can_subscribe = Some(new_perm.can_subscribe);
                    p.grants.video.can_publish_data = Some(new_perm.can_publish_data);
                    p.grants.video.can_publish_sources = new_perm
                        .can_publish_sources
                        .iter()
                        .map(|s| crate::permission::canonical_source(*s).into_owned())
                        .collect();
                    p.grants.video.can_update_own_metadata = Some(new_perm.can_update_metadata);
                    p.grants.video.hidden = new_perm.hidden;
                    p.grants.video.recorder = new_perm.recorder;
                    p.grants.video.agent = new_perm.agent;
                    p.grants.video.can_subscribe_metrics = Some(new_perm.can_subscribe_metrics);
                    p.grants.video.can_manage_agent_session =
                        Some(new_perm.can_manage_agent_session);
                    // Fixed Participant.ToProto reads the authoritative grant's
                    // projection. A newly materialized raw numeric source name
                    // is not an enum NAME and projects back to UNKNOWN here.
                    p.info.permission = p.grants.video.to_permission();
                    // Fixed SetPermission recomputes IsPublisher independently
                    // of current publications. PC.IsEstablished means !=New,
                    // including checking/failed/disconnected states.
                    p.info.is_publisher = new_perm.can_publish
                        && p.transports.iter().any(|(id, role)| {
                            matches!(
                                role,
                                crate::media_api::PcRole::Publisher
                                    | crate::media_api::PcRole::Single
                            ) && p.established_transports.contains(id)
                        });
                    p.mark_dirty();
                }
                // Cancel pending requests before removing active publications:
                // unpublish can release the next queued AddTrack for that cid.
                let revoked_pending = if let Some(p) = self.participants.get_mut(&sid) {
                    p.queued.retain(|_, queued| {
                        queued.retain(|request| new_perm.can_publish_source(request.source));
                        !queued.is_empty()
                    });
                    let (allowed, revoked): (Vec<_>, Vec<_>) = std::mem::take(&mut p.pending)
                        .into_iter()
                        .partition(|pending| new_perm.can_publish_source(pending.info.source));
                    p.pending = allowed;
                    revoked
                } else {
                    Vec::new()
                };
                for pending in revoked_pending {
                    self.release_pending_track(&sid, pending, now);
                }
                // Unpublish tracks whose source is no longer allowed.
                let revoked: Vec<TrackSid> = self
                    .participants
                    .get(&sid)
                    .map(|p| {
                        p.info
                            .tracks
                            .iter()
                            .filter(|t| !new_perm.can_publish_source(t.source))
                            .map(|t| t.sid.clone())
                            .collect()
                    })
                    .unwrap_or_default();
                for t in revoked {
                    self.unpublish_track(&sid, &t, true, now);
                }
                if !new_perm.can_subscribe {
                    self.revoke_all_subscriptions(&sid);
                } else if !old_perm.can_subscribe {
                    self.subscribe_to_existing(&sid, now);
                    let requested: Vec<_> = self
                        .participants
                        .get(&sid)
                        .map(|p| p.subscriptions.keys().cloned().collect())
                        .unwrap_or_default();
                    for track in requested {
                        self.resolve_subscription(&sid, &track, now);
                    }
                }
            }
        }

        if changed {
            if self
                .participants
                .get(&sid)
                .is_some_and(|p| !p.info.permission.can_publish_data)
            {
                self.remove_published_data_tracks(&sid, now);
            }
            self.reconcile_data_subscriptions();
            self.broadcast_participant(&sid, true, now);
            if let Some(p) = self.participants.get_mut(&sid) {
                let claims =
                    crate::session::SignalClaimsSnapshot::new(p.snapshot(), p.grants.video.clone());
                self.signal(&sid, SignalEvent::ClaimsChanged(Box::new(claims)));
            }
        }
        self.participants
            .get_mut(&sid)
            .map(|p| p.snapshot())
            .ok_or(OpError::NotFound(PARTICIPANT_NOT_FOUND))
    }

    pub fn api_update_subscriptions(
        &mut self,
        identity: &Identity,
        track_sids: &[TrackSid],
        subscribe: bool,
        now: Instant,
    ) -> Result<(), OpError> {
        let sid = self
            .sid_by_identity(identity)
            .ok_or(OpError::NotFound(PARTICIPANT_NOT_FOUND))?;
        for t in track_sids {
            if subscribe {
                self.subscribe(&sid, t, now);
            } else {
                self.unsubscribe(&sid, t, now);
            }
        }
        Ok(())
    }

    /// `SendData` from the server API: no sender, delivered to targeted or all.
    pub fn api_send_data(
        &mut self,
        kind: DataKind,
        payload: Vec<u8>,
        destination_identities: &[Identity],
        destination_sids: &[ParticipantSid],
    ) {
        self.route_data(
            None,
            kind,
            payload,
            destination_identities,
            destination_sids,
            false,
        );
    }

    /// Capture source destination inventory, then admit each operation when its
    /// actual fanout worker starts. Eligibility must not be frozen before an
    /// earlier serial/blocked recipient yields back to the live room owner.
    #[allow(clippy::too_many_arguments)]
    pub fn api_send_data_tracked(
        &mut self,
        id: u64,
        kind: DataKind,
        payload: Vec<u8>,
        identities: &[Identity],
        sids: &[ParticipantSid],
        nonce: &[u8],
        now: Instant,
    ) {
        // room.go2128–2131: a repeated non-empty nonce is dropped before any
        // recipient write; the API call itself still completes successfully.
        if !nonce.is_empty() && !self.admit_nonce(nonce, now) {
            self.media(crate::media_api::MediaCommand::SendDataFanout {
                id,
                writes: Vec::new(),
            });
            return;
        }
        let targeted = !identities.is_empty() || !sids.is_empty();
        let writes = self
            .participants
            .iter()
            .filter(|(_, p)| {
                !p.is_disconnected()
                    && (!targeted
                        || identities.contains(&p.info.identity)
                        || sids.contains(&p.info.sid))
            })
            .map(|(sid, p)| crate::media_api::DataWrite {
                to: sid.clone(),
                kind,
                payload: payload.clone(),
                expected_transport: Some(p.primary),
            })
            .collect();
        self.media(crate::media_api::MediaCommand::SendDataFanout { id, writes });
    }

    pub(super) fn admit_api_data_write(
        &mut self,
        request: u64,
        to: ParticipantSid,
        kind: DataKind,
        payload: Vec<u8>,
        expected_transport: Option<TransportId>,
    ) {
        let write = if let Some(p) = self.participants.get_mut(&to) {
            if p.is_disconnected() || expected_transport.is_some_and(|tid| p.primary != tid) {
                false
            } else if kind != DataKind::Reliable {
                p.is_active()
            } else if !p.can_write_reliable && !p.joining_replay_in_progress {
                if payload.len() <= 100_000_usize.saturating_sub(p.joining_reliable_bytes) {
                    p.joining_reliable_bytes += payload.len();
                    p.joining_reliable.push_back(payload);
                }
                false
            } else {
                p.is_active()
            }
        } else {
            false
        };
        self.media(crate::media_api::MediaCommand::DataWriteAdmitted { request, write });
    }

    /// Empty metadata is allowed and clears it.
    pub fn api_update_metadata(&mut self, metadata: String) -> RoomInfo {
        if self.info.metadata != metadata {
            self.info.metadata = metadata;
            self.info.version.advance();
        }
        self.broadcast_room_update();
        self.info()
    }

    pub fn participant_exists(&self, identity: &Identity) -> bool {
        self.by_identity.contains_key(identity)
    }
}
