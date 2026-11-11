//! Join, resume and participant removal (signaling.md §2, §8, §11).

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::time::Instant;

use super::*;
use crate::events::EngineEvent;
use crate::media_api::{MediaCommand, PcRole, TransportSpec};
use crate::session::{JoinError, JoinInfo, JoinParams, ReconnectInfo, SessionRequest};

/// Result of a successful join.
#[derive(Clone, Debug)]
pub struct Joined {
    pub sid: ParticipantSid,
    pub generation: SessionGeneration,
    pub resumed: bool,
    /// Generation of a session that was replaced (resume) and must be closed.
    pub replaced_generation: Option<SessionGeneration>,
}

impl Room {
    pub fn join(&mut self, params: JoinParams, now: Instant) -> Result<Joined, JoinError> {
        self.join_with_session_start(params, now, now)
    }
    /// Owner service admission time precedes potentially slow allocation; it is
    /// distinct from actor execution time used by media/state-machine deadlines.
    pub fn join_with_session_start(
        &mut self,
        params: JoinParams,
        now: Instant,
        session_started_at: Instant,
    ) -> Result<Joined, JoinError> {
        if self.closed {
            return Err(JoinError::RoomClosed);
        }

        if let Some(existing) = self.sid_by_identity(&params.identity) {
            if params.reconnect {
                return self.resume(&existing, params, now);
            }
            self.remove_participant(&existing, DisconnectReason::DuplicateIdentity, true, now);
        } else if params.reconnect {
            return Err(JoinError::ReconnectStateMismatch);
        }

        let permission = params.grants.video.to_permission();
        let dependent = params.capabilities.one_shot
            || matches!(
                params.grants.kind,
                ParticipantKind::Agent | ParticipantKind::Egress
            )
            || permission.agent
            || permission.recorder;
        if !dependent && self.info.max_participants > 0 {
            let count = self
                .participants
                .values()
                .filter(|p| !p.is_dependent())
                .count();
            if count >= self.info.max_participants as usize {
                return Err(JoinError::MaxParticipants);
            }
        }

        let sid = ParticipantSid::generate();
        let single_pc = params.single_pc;
        let subscriber_primary =
            !single_pc && params.capabilities.subscriber_primary && permission.can_subscribe;
        let allow_pause = params
            .subscriber_allow_pause
            .unwrap_or(self.policy.allow_pause);
        let ice_lite =
            self.policy.use_ice_lite && !params.disable_ice_lite && params.capabilities.ice_lite;
        let supports_red = params.capabilities.audio_red;

        let mut transports = Vec::new();

        let primary = if single_pc {
            let t = TransportId::next();
            transports.push((t, PcRole::Single));
            t
        } else {
            let publisher = TransportId::next();
            let subscriber = TransportId::next();
            transports.push((publisher, PcRole::Publisher));
            transports.push((subscriber, PcRole::Subscriber));
            if subscriber_primary {
                subscriber
            } else {
                publisher
            }
        };

        let info = ParticipantInfo {
            sid: sid.clone(),
            identity: params.identity.clone(),
            name: params.name.clone(),
            state: ParticipantState::Joining,
            tracks: Vec::new(),
            data_tracks: Vec::new(),
            metadata: params.metadata.clone(),
            attributes: params.attributes.clone(),
            joined_at_ms: crate::util::unix_ms(),
            version: 0,
            permission,
            region: self.policy.region.clone(),
            is_publisher: false,
            kind: params.grants.kind,
            disconnect_reason: DisconnectReason::Unknown,
            client_protocol: params.client.client_protocol,
        };

        let participant = Participant {
            info,
            dirty: false,
            grants: params.grants.clone(),
            client: params.client.clone(),
            capabilities: params.capabilities,
            auto_subscribe: params.auto_subscribe,
            auto_subscribe_data_track: params.auto_subscribe_data_track,
            data_subscriptions: BTreeMap::new(),
            next_data_handle: 0,
            data_blobs: Vec::new(),
            adaptive_stream: params.adaptive_stream,
            transports: transports.clone(),
            primary,
            established_transports: HashSet::new(),
            pending: Vec::new(),
            queued: HashMap::new(),
            released_cids: VecDeque::new(),
            parked_media: Vec::new(),
            deferred_publications: Vec::new(),
            subscription_permission: None,
            subscriptions: BTreeMap::new(),
            next_subscription_reconcile: now + Duration::from_secs(3),
            track_settings: HashMap::new(),
            sent_versions: HashMap::new(),
            ready: false,
            queued_updates: BTreeMap::new(),
            generation: SessionGeneration::default(),
            signal_connected: true,
            signal_closed_at: None,
            track_subscribed_sent: HashSet::new(),
            last_reliable_seq: 0,
            joining_reliable: VecDeque::new(),
            joining_reliable_bytes: 0,
            can_write_reliable: false,
            joining_replay_in_progress: false,
            ever_active: false,
            active_at: None,
            ice_connected_once: false,
            session_started_at,
            join_deadline: now + JOIN_VERIFY_TIMEOUT,
            transport_failed_at: None,
            media_recovery_epoch: 0,
            signal_rtt_ms: 0,
            fallback: crate::transport_fallback::TransportFallback::new(
                self.policy
                    .transport_fallback
                    .cached(&self.key, &params.identity, now),
                now,
            ),
            media_rtt: super::media_rtt::MediaRtt::new(now),
            disconnect_on_resume: false,
            last_quality: None,
        };

        if !participant.is_dependent() && self.joined_at_s == 0 {
            self.joined_at_s = crate::util::unix_s();
        }
        self.by_identity
            .insert(params.identity.clone(), sid.clone());
        self.participants.insert(sid.clone(), participant);

        for (id, role) in &transports {
            let spec = TransportSpec {
                ice_preference: self.participants[&sid].fallback.preference,
                one_shot: params.capabilities.one_shot,
                id: *id,
                participant: sid.clone(),
                role: *role,
                ice_lite,
                server_data_channels: *role == PcRole::Subscriber && subscriber_primary,
                enable_data_tracks: self.policy.enable_data_tracks,
                primary: *id == primary,
                client_supports_red: supports_red,
                codec_switch_requires_renegotiation: params
                    .capabilities
                    .codec_switch_requires_renegotiation,
                disable_playout_delay: self.sync_streams
                    && params.capabilities.disable_playout_delay_for_sync,
                allow_pause,
            };
            self.media(MediaCommand::CreateTransport(spec));
        }

        self.apply_ice_preference(&sid, false, now);

        // JoinResponse is the first message; it shows our own state as JOINING.
        let others = self.visible_others(&sid);
        let enabled_publish_codecs = self.enabled_publish_codecs();
        let room = self.info();
        let sif_trailer = self.sif_trailer.clone();
        let (me, can_publish) = {
            let p = self.participants.get_mut(&sid).expect("just inserted");
            (p.snapshot(), p.info.permission.can_publish)
        };
        let join = JoinInfo {
            ice_preference: self.participants[&sid].fallback.preference,
            room,
            participant: me,
            other_participants: others,
            subscriber_primary,
            sif_trailer,
            enabled_publish_codecs,
            fast_publish: can_publish,
            ping_interval_s: PING_INTERVAL_S,
            ping_timeout_s: PING_TIMEOUT_S,
        };
        self.signal(&sid, SignalEvent::Join(Box::new(join)));

        // Now ready: JOINING → JOINED, then the queued updates.
        let claims = {
            let p = self.participants.get_mut(&sid).expect("present");
            p.set_state(ParticipantState::Joined);
            p.ready = true;
            crate::session::SignalClaimsSnapshot::new(p.snapshot(), p.grants.video.clone())
        };
        self.flush_queued_updates(&sid);
        self.broadcast_participant(&sid, false, now);
        self.schedule_room_update(now);

        // Subscribe to existing tracks before the first subscriber offer (fast
        // start, v>7) or for single PC; otherwise this happens at ACTIVE.
        let fast_start = single_pc || (subscriber_primary && params.capabilities.fast_start);
        if fast_start {
            self.subscribe_to_existing(&sid, now);
        }

        // /rtc/v1: tracks from JoinRequest, then the publisher offer.
        for add in params.add_tracks {
            self.handle_request(
                &sid,
                SessionGeneration::default(),
                SessionRequest::AddTrack(add),
                now,
            );
        }
        if let Some((sdp, id)) = params.publisher_offer {
            self.handle_request(
                &sid,
                SessionGeneration::default(),
                SessionRequest::Offer { sdp, id },
                now,
            );
        }

        // Session start: adapter mints the first refresh token.
        self.signal(&sid, SignalEvent::ClaimsChanged(Box::new(claims)));

        self.lifecycle.participant_rtc_connected += 1;
        Ok(Joined {
            sid,
            generation: SessionGeneration::default(),
            resumed: false,
            replaced_generation: None,
        })
    }

    fn resume(
        &mut self,
        sid: &ParticipantSid,
        params: JoinParams,
        now: Instant,
    ) -> Result<Joined, JoinError> {
        // RoomManager captures these before HandleClientReconnect changes the
        // live ICE policy. Preserve both source response and restart snapshots.
        let cached_preference = self
            .participants
            .get(sid)
            .map(|p| {
                self.policy
                    .transport_fallback
                    .cached(&self.key, &p.info.identity, now)
            })
            .unwrap_or_default();
        let response_preference = self
            .participants
            .get(sid)
            .map(|p| p.fallback.preference)
            .unwrap_or_default();
        let (old_generation, signal_closed_at, capabilities, drop_signal) = {
            let Some(p) = self.participants.get_mut(sid) else {
                return Err(JoinError::ReconnectStateMismatch);
            };
            if p.is_disconnected() {
                return Err(JoinError::ReconnectStateMismatch);
            }
            let old = p.generation;
            p.generation = p.generation.bump();
            p.media_recovery_epoch = p
                .media_recovery_epoch
                .checked_add(1)
                .expect("media recovery epoch exhausted");
            p.ready = false;
            p.signal_connected = true;
            let closed_at = p.signal_closed_at.take();
            p.transport_failed_at = None;
            p.client = params.client.clone();
            p.capabilities = params.capabilities;
            let drop_signal = std::mem::take(&mut p.disconnect_on_resume);
            (old, closed_at, p.capabilities, drop_signal)
        };

        self.media(MediaCommand::BeginTransportRecovery {
            participant: sid.clone(),
            recovery_epoch: self.participants[sid].media_recovery_epoch,
        });

        let failing_role_exists = self.participants.get(sid).is_some_and(|p| {
            p.transports
                .iter()
                .any(|(_, role)| match params.reconnect_reason {
                    2 => matches!(role, PcRole::Publisher | PcRole::Single),
                    3 => *role == PcRole::Subscriber,
                    _ => false,
                })
        });
        if failing_role_exists {
            let changed = {
                let p = self.participants.get_mut(sid).expect("resume participant");
                let transport = if params.reconnect_reason == 2 {
                    p.publish_transport()
                } else {
                    p.transport(PcRole::Subscriber).expect("validated role")
                };
                p.fallback
                    .client_reconnect(
                        &self.policy.transport_fallback,
                        transport,
                        p.signal_connected,
                        p.capabilities.one_shot,
                        p.capabilities.ice_tcp,
                        now,
                    )
                    .is_some()
            };
            if changed {
                self.apply_ice_preference(sid, true, now);
            }
            self.media(MediaCommand::ArmShortConnectionReset {
                participant: sid.clone(),
            });
        }
        let last_message_seq = self
            .participants
            .get(sid)
            .map(|p| p.last_reliable_seq)
            .unwrap_or(0);
        self.signal(
            sid,
            SignalEvent::Reconnect(ReconnectInfo {
                last_message_seq,
                ice_preference: response_preference,
                ice_servers_preference: cached_preference,
            }),
        );
        if let Some(p) = self.participants.get_mut(sid) {
            p.ready = true;
        }
        self.flush_queued_updates(sid);
        if self
            .participants
            .get(sid)
            .is_some_and(|p| !p.data_subscriptions.is_empty())
        {
            self.send_data_handles(sid);
        }

        // v>8: participants that disconnected while the signal was down.
        if capabilities.disconnected_update {
            let since =
                signal_closed_at.map(|t| t.checked_sub(Duration::from_secs(3)).unwrap_or(t));
            let gone: Vec<ParticipantInfo> = self
                .recently_disconnected
                .iter()
                .filter(|(t, _)| since.is_none_or(|s| *t >= s))
                .map(|(_, info)| info.clone())
                .collect();
            if !gone.is_empty() {
                self.signal(sid, SignalEvent::ParticipantUpdate(gone));
            }
        }

        // Fixed RoomManager.StartSession resume explicitly increments active,
        // without re-counting a new RTC-connected participant.
        self.lifecycle.participant_rtc_active += 1;
        // Everyone, including self, then the room.
        let mut all = Vec::new();
        let sids: Vec<ParticipantSid> = self.participants.keys().cloned().collect();
        for other in sids {
            let hidden = self.participants.get(&other).is_some_and(|p| p.is_hidden());
            if hidden && &other != sid {
                continue;
            }
            if let Some(p) = self.participants.get_mut(&other) {
                all.push(p.snapshot());
            }
        }
        self.send_participant_updates(sid, all);
        let room = self.info();
        self.signal(sid, SignalEvent::RoomUpdate(room));

        let restore_changed = {
            let p = self.participants.get_mut(sid).expect("resume participant");
            let changed = p.fallback.preference != cached_preference;
            p.fallback.resume(cached_preference, now);
            changed
        };
        self.apply_ice_preference(sid, restore_changed, now);

        // Dual PC: the server restarts ICE on the subscriber transport. Single PC:
        // the client sends an ICE-restart offer.
        let subscriber_transport = self
            .participants
            .get(sid)
            .and_then(|p| p.transport(PcRole::Subscriber));
        if let Some(t) = subscriber_transport {
            self.media(MediaCommand::IceRestart { transport: t });
        }

        let claims = self.participants.get_mut(sid).map(|p| {
            crate::session::SignalClaimsSnapshot::new(p.snapshot(), p.grants.video.clone())
        });
        if let Some(claims) = claims {
            self.signal(sid, SignalEvent::ClaimsChanged(Box::new(claims)));
        }
        if drop_signal {
            self.effects.push(Effect::CloseSignal { to: sid.clone() });
        }

        let generation = self
            .participants
            .get(sid)
            .map(|p| p.generation)
            .unwrap_or_default();
        Ok(Joined {
            sid: sid.clone(),
            generation,
            resumed: true,
            replaced_generation: Some(old_generation),
        })
    }

    /// Signal connection closed without a leave (signaling.md §8).
    pub fn signal_closed(
        &mut self,
        sid: &ParticipantSid,
        generation: SessionGeneration,
        now: Instant,
    ) {
        let close_now = {
            let Some(p) = self.participants.get_mut(sid) else {
                return;
            };
            if p.generation != generation {
                return; // stale socket of a replaced session
            }
            p.signal_connected = false;
            p.ready = false;
            p.signal_closed_at = Some(now);
            !p.ice_connected_once
        };
        if close_now {
            self.remove_participant(sid, DisconnectReason::SignalClose, false, now);
        }
    }

    /// Others visible to `sid` (excludes hidden participants and self).
    pub(super) fn visible_others(&mut self, sid: &ParticipantSid) -> Vec<ParticipantInfo> {
        let mut out = Vec::new();
        for (other, p) in self.participants.iter_mut() {
            if other == sid || p.is_hidden() || p.is_disconnected() {
                continue;
            }
            out.push(p.snapshot());
        }
        if let Some(me) = self.participants.get_mut(sid) {
            for info in &out {
                me.sent_versions.insert(info.sid.clone(), info.version);
            }
        }
        out
    }

    /// Remove a participant from the room.
    pub(crate) fn remove_participant(
        &mut self,
        sid: &ParticipantSid,
        reason: DisconnectReason,
        send_leave: bool,
        now: Instant,
    ) {
        if !self.participants.contains_key(sid) {
            return;
        }
        if let Some(p) = self.participants.get(sid) {
            // Go IsConnectionCanceled's close-reason classification is separate
            // from an ICE/media failure; do not classify every aborted join as cancel.
            let canceled = matches!(
                reason,
                DisconnectReason::ClientInitiated
                    | DisconnectReason::DuplicateIdentity
                    | DisconnectReason::RoomClosed
                    | DisconnectReason::RoomDeleted
                    | DisconnectReason::ServerShutdown
                    | DisconnectReason::Migration
            ) || (reason == DisconnectReason::SignalClose
                && now.saturating_duration_since(p.session_started_at) < Duration::from_secs(3));
            if canceled {
                if !p.ice_connected_once {
                    self.lifecycle.participant_rtc_canceled += 1;
                }
                self.lifecycle.track_publish_cancels +=
                    p.pending.len() as u64 + p.queued.values().map(|q| q.len() as u64).sum::<u64>();
            }
            self.lifecycle.track_subscribe_cancels += p
                .subscriptions
                .values()
                .filter(|s| s.desired && !s.event_reported)
                .count() as u64;
        }
        self.publication_monitors
            .retain(|(participant, _), _| participant != sid);
        self.subscription_monitors
            .retain(|(participant, _), _| participant != sid);
        self.publisher_connected.remove(sid);
        if send_leave {
            self.send_leave(sid, reason, LeaveAction::Disconnect);
        }
        self.effects.push(Effect::CloseSignal { to: sid.clone() });
        // Unpublishing normally starts the next queued AddTrack for that cid.
        // A participant being removed must never start another publication.
        if let Some(p) = self.participants.get_mut(sid) {
            p.queued.clear();
            p.pending.clear();
            p.parked_media.clear();
        }

        // Unpublish everything it published (subscribers lose their down tracks).
        let published: Vec<TrackSid> = self
            .participants
            .get(sid)
            .map(|p| p.info.tracks.iter().map(|t| t.sid.clone()).collect())
            .unwrap_or_default();
        for track in &published {
            self.unpublish_track(sid, track, false, now);
        }
        self.remove_published_data_tracks(sid, now);
        // Drop its own subscriptions from the forwarding graph.
        self.media(MediaCommand::RemoveParticipant(sid.clone()));

        let Some(mut p) = self.participants.remove(sid) else {
            return;
        };
        if self.by_identity.get(&p.info.identity) == Some(sid) {
            self.by_identity.remove(&p.info.identity);
        }
        self.stored_participants.remove(&p.info.identity);
        self.batch_pending.retain(|s| s != sid);
        self.speakers.remove(sid);
        self.left_at_s = crate::util::unix_s();

        p.set_state(ParticipantState::Disconnected);
        p.info.disconnect_reason = reason;
        p.mark_dirty();
        let gone = p.snapshot();
        if self.recently_disconnected.len() >= RECENT_DISCONNECT_MEMORY {
            self.recently_disconnected.pop_front();
        }
        self.recently_disconnected.push_back((now, gone.clone()));

        if !p.is_hidden() {
            let recipients: Vec<ParticipantSid> = self.participants.keys().cloned().collect();
            for r in recipients {
                let skip = self.participants.get(&r).is_some_and(|rp| {
                    rp.capabilities.identity_based_reconnection
                        && reason == DisconnectReason::DuplicateIdentity
                });
                if !skip {
                    self.send_participant_updates(&r, vec![gone.clone()]);
                }
            }
        }

        let room = self.info();
        let event = if p.ever_active {
            EngineEvent::ParticipantLeft {
                room,
                participant: gone,
            }
        } else {
            EngineEvent::ParticipantConnectionAborted {
                room,
                participant: gone,
            }
        };
        self.event(event);
        self.schedule_room_update(now);
    }
}
