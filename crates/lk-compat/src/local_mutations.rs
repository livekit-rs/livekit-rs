//! Original standalone Room/Participant mutation topics on LocalDispatch's actual
//! LocalMessageBus. Clients remain the existing typed service constructors;
//! registrations capture only the real owner and lifecycle SID projections.
use crate::{
    convert,
    room_control::accepted::Accepted,
    service_bus::{
        local_transport::LocalTransport,
        rpc::{MethodSpec, RpcError, RpcServer, ServerLimits},
    },
};
use lk_protocol::livekit as pb;
use rtc_core::{
    ids::{Identity, ParticipantSid, RoomKey, RoomSid},
    model::ParticipantInfo,
};
use rtc_runtime::{ParticipantUpdate, RoomOwnerHandle};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{Mutex as AsyncMutex, Semaphore};
fn error(error: rtc_runtime::ApiError) -> RpcError {
    let e: crate::twirp::error::TwirpError = error.into();
    let mut r = RpcError::new(e.code.as_str(), e.msg);
    r.application_metadata = e.meta;
    r
}
pub(crate) fn room_spec(room: &str, method: &str) -> MethodSpec {
    MethodSpec {
        service: "Room".into(),
        method: method.into(),
        topics: vec![room.into()],
        require_claim: true,
        queue: true,
    }
}
pub(crate) fn participant_spec(room: &str, identity: &str, method: &str) -> MethodSpec {
    MethodSpec {
        service: "Participant".into(),
        method: method.into(),
        topics: vec![format!("{room}_{identity}")],
        require_claim: true,
        queue: true,
    }
}
fn check(
    owner: &RoomOwnerHandle,
    room: &str,
    expected: Option<(&str, &str)>,
) -> Result<(), RpcError> {
    if room != owner.key().name.0 {
        return Err(RpcError::new(
            "failed_precondition",
            "room does not match registered topic",
        ));
    }
    if expected.is_some_and(|(registered, request)| registered != request) {
        return Err(RpcError::new(
            "failed_precondition",
            "participant identity does not match registered topic",
        ));
    }
    Ok(())
}
struct Participant {
    sid: ParticipantSid,
    servers: Vec<RpcServer>,
}
struct Owner {
    sid: RoomSid,
    room: Vec<RpcServer>,
    participants: HashMap<String, Participant>,
}
#[derive(Default)]
struct Groups {
    stopped: bool,
    owners: HashMap<RoomKey, Owner>,
}
#[derive(Default)]
struct Presence {
    owners: HashMap<RoomKey, (RoomSid, HashMap<String, ParticipantInfo>)>,
}
pub(crate) struct LocalMutations {
    bus: Arc<LocalTransport>,
    groups: AsyncMutex<Groups>,
    presence: Mutex<Presence>,
    accepted: Arc<Accepted>,
    #[cfg(test)]
    observed: Arc<Mutex<Vec<(String, i64, i64)>>>,
}
impl LocalMutations {
    pub(crate) fn accepted_operations(&self) -> Arc<Accepted> {
        self.accepted.clone()
    }
    pub(crate) fn new(bus: Arc<LocalTransport>) -> Self {
        Self {
            bus,
            groups: AsyncMutex::new(Groups::default()),
            presence: Mutex::new(Presence::default()),
            accepted: Arc::new(Accepted::default()),
            #[cfg(test)]
            observed: Arc::new(Mutex::new(Vec::new())),
        }
    }
    fn limits() -> ServerLimits {
        ServerLimits {
            concurrent: Semaphore::MAX_PERMITS,
            max_request_duration: Duration::MAX,
            allow_skip_claim: false,
        }
    }
    pub(crate) fn project(&self, owner: &RoomOwnerHandle, participants: &[ParticipantInfo]) {
        let mut p = self.presence.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((sid, view)) = p.owners.get_mut(owner.key()) {
            if &*sid != owner.sid() {
                return;
            }
            *view = participants
                .iter()
                .map(|p| (p.identity.0.clone(), p.clone()))
                .collect();
        }
    }
    pub(crate) fn stored_participants(&self, key: &RoomKey) -> Vec<ParticipantInfo> {
        self.presence
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .owners
            .get(key)
            .map(|(_, view)| view.values().cloned().collect())
            .unwrap_or_default()
    }
    pub(crate) fn stored_participant(
        &self,
        key: &RoomKey,
        identity: &Identity,
    ) -> Result<ParticipantInfo, RpcError> {
        self.presence
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .owners
            .get(key)
            .and_then(|(_, view)| view.get(&identity.0))
            .cloned()
            .ok_or_else(|| RpcError::new("not_found", "participant does not exist"))
    }
    pub(crate) fn has_participant(&self, key: &RoomKey, identity: &Identity) -> bool {
        self.presence
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .owners
            .get(key)
            .is_some_and(|(_, p)| p.contains_key(&identity.0))
    }
    async fn retire(&self, group: Owner) -> Result<(), RpcError> {
        let mut first = None;
        for server in group
            .room
            .into_iter()
            .chain(group.participants.into_values().flat_map(|p| p.servers))
        {
            if let Err(e) = self.accepted.retire_server(&group.sid, server).await {
                first.get_or_insert(e);
            }
        }
        first.map_or(Ok(()), Err)
    }
    pub(crate) async fn opened(&self, owner: RoomOwnerHandle) -> Result<(), RpcError> {
        let mut groups = self.groups.lock().await;
        if groups.stopped {
            return Err(RpcError::new("canceled", "local mutation topics closed"));
        }
        if groups
            .owners
            .get(owner.key())
            .is_some_and(|g| &g.sid == owner.sid())
        {
            return Ok(());
        }
        if let Some(old) = groups.owners.remove(owner.key()) {
            self.retire(old).await?;
        }
        self.presence
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .owners
            .insert(owner.key().clone(), (owner.sid().clone(), HashMap::new()));
        groups.owners.insert(
            owner.key().clone(),
            Owner {
                sid: owner.sid().clone(),
                room: Vec::new(),
                participants: HashMap::new(),
            },
        );
        let group = groups.owners.get_mut(owner.key()).unwrap();
        macro_rules! register {
            ($name:literal,$request:ty,$response:ty,$o:ident,$r:ident,$body:block) => {{
                let captured = owner.clone();
                let accepted = self.accepted.clone();
                #[cfg(test)]
                let observed = self.observed.clone();
                let server = RpcServer::register::<$request, $response, _, _>(
                    self.bus.clone(),
                    room_spec(&owner.key().name.0, $name),
                    Self::limits(),
                    move |ctx, $r| {
                        #[cfg(test)]
                        observed.lock().unwrap().push((
                            $name.into(),
                            ctx.sent_at_ns,
                            ctx.expiry_ns,
                        ));
                        #[cfg(not(test))]
                        let _ = ctx;
                        let $o = captured.clone();
                        let accepted = accepted.clone();
                        async move {
                            let sid = $o.sid().clone();
                            accepted
                                .run(&sid, async move {
                                    check(&$o, &$r.room, None)?;
                                    $body
                                })
                                .await
                        }
                    },
                    None,
                )
                .await?;
                group.room.push(server);
            }};
        }
        register!(
            "DeleteRoom",
            pb::DeleteRoomRequest,
            pb::DeleteRoomResponse,
            actor,
            request,
            {
                actor.close_room().await.map_err(error)?;
                Ok(pb::DeleteRoomResponse {})
            }
        );
        register!(
            "UpdateRoomMetadata",
            pb::UpdateRoomMetadataRequest,
            pb::Room,
            actor,
            request,
            {
                let room = actor
                    .update_metadata(request.metadata)
                    .await
                    .map_err(error)?;
                Ok(convert::room_info_to_proto(&room))
            }
        );
        register!(
            "ListParticipants",
            pb::ListParticipantsRequest,
            pb::ListParticipantsResponse,
            actor,
            request,
            {
                Ok(pb::ListParticipantsResponse {
                    participants: actor
                        .list_participants()
                        .await
                        .map_err(error)?
                        .iter()
                        .map(convert::participant_info_to_proto)
                        .collect(),
                })
            }
        );
        register!(
            "GetParticipant",
            pb::RoomParticipantIdentity,
            pb::ParticipantInfo,
            actor,
            request,
            {
                let participant = actor
                    .get_participant(Identity(request.identity))
                    .await
                    .map_err(error)?;
                Ok(convert::participant_info_to_proto(&participant))
            }
        );
        Ok(())
    }
    pub(crate) async fn refresh(
        &self,
        owner: RoomOwnerHandle,
        participants: &[ParticipantInfo],
    ) -> Result<(), RpcError> {
        self.project(&owner, participants);
        let mut groups = self.groups.lock().await;
        if groups.stopped {
            return Err(RpcError::new("canceled", "local mutation topics closed"));
        }
        let group = groups
            .owners
            .get_mut(owner.key())
            .filter(|g| &g.sid == owner.sid())
            .ok_or_else(|| RpcError::new("aborted", "local mutation owner generation retired"))?;
        let desired: HashMap<_, _> = participants
            .iter()
            .map(|p| (p.identity.0.clone(), p.sid.clone()))
            .collect();
        let removed = group
            .participants
            .iter()
            .filter(|(id, p)| desired.get(*id) != Some(&p.sid))
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        for id in removed {
            let p = group.participants.remove(&id).unwrap();
            for server in p.servers {
                self.accepted.retire_server(owner.sid(), server).await?;
            }
        }
        for (identity, sid) in desired {
            if group.participants.contains_key(&identity) {
                continue;
            }
            group.participants.insert(
                identity.clone(),
                Participant {
                    sid,
                    servers: Vec::new(),
                },
            );
            let target = group.participants.get_mut(&identity).unwrap();
            macro_rules! register {
                ($name:literal,$request:ty,$response:ty,$o:ident,$r:ident,$body:block) => {{
                    let captured = owner.clone();
                    let registered = identity.clone();
                    let accepted = self.accepted.clone();
                    #[cfg(test)]
                    let observed = self.observed.clone();
                    let server = RpcServer::register::<$request, $response, _, _>(
                        self.bus.clone(),
                        participant_spec(&owner.key().name.0, &identity, $name),
                        Self::limits(),
                        move |ctx, $r| {
                            #[cfg(test)]
                            observed.lock().unwrap().push((
                                $name.into(),
                                ctx.sent_at_ns,
                                ctx.expiry_ns,
                            ));
                            #[cfg(not(test))]
                            let _ = ctx;
                            let $o = captured.clone();
                            let registered = registered.clone();
                            let accepted = accepted.clone();
                            async move {
                                let sid = $o.sid().clone();
                                accepted
                                    .run(&sid, async move {
                                        check(&$o, &$r.room, Some((&registered, &$r.identity)))?;
                                        $body
                                    })
                                    .await
                            }
                        },
                        None,
                    )
                    .await?;
                    target.servers.push(server);
                }};
            }
            register!(
                "RemoveParticipant",
                pb::RoomParticipantIdentity,
                pb::RemoveParticipantResponse,
                actor,
                request,
                {
                    actor
                        .remove_participant(Identity(request.identity))
                        .await
                        .map_err(error)?;
                    Ok(pb::RemoveParticipantResponse {})
                }
            );
            register!(
                "MutePublishedTrack",
                pb::MuteRoomTrackRequest,
                pb::MuteRoomTrackResponse,
                actor,
                request,
                {
                    let track = actor
                        .mute_published_track(
                            Identity(request.identity),
                            rtc_core::ids::TrackSid(request.track_sid),
                            request.muted,
                        )
                        .await
                        .map_err(error)?;
                    Ok(pb::MuteRoomTrackResponse {
                        track: track.as_ref().map(convert::track_info_to_proto),
                    })
                }
            );
            register!(
                "UpdateParticipant",
                pb::UpdateParticipantRequest,
                pb::ParticipantInfo,
                actor,
                request,
                {
                    let info = actor
                        .update_participant(
                            Identity(request.identity),
                            ParticipantUpdate {
                                name: request.name,
                                metadata: request.metadata,
                                attributes: request.attributes.into_iter().collect(),
                                permission: request
                                    .permission
                                    .as_ref()
                                    .map(convert::permission_from_proto),
                            },
                        )
                        .await
                        .map_err(error)?;
                    Ok(convert::participant_info_to_proto(&info))
                }
            );
            register!(
                "UpdateSubscriptions",
                pb::UpdateSubscriptionsRequest,
                pb::UpdateSubscriptionsResponse,
                actor,
                request,
                {
                    let mut tracks = request
                        .track_sids
                        .into_iter()
                        .map(rtc_core::ids::TrackSid)
                        .collect::<Vec<_>>();
                    for p in request.participant_tracks {
                        tracks.extend(p.track_sids.into_iter().map(rtc_core::ids::TrackSid));
                    }
                    actor
                        .update_subscriptions(Identity(request.identity), tracks, request.subscribe)
                        .await
                        .map_err(error)?;
                    Ok(pb::UpdateSubscriptionsResponse {})
                }
            );
        }
        Ok(())
    }
    pub(crate) async fn closed(&self, key: &RoomKey, sid: &RoomSid) {
        {
            let mut p = self.presence.lock().unwrap_or_else(|p| p.into_inner());
            if p.owners.get(key).is_some_and(|(current, _)| current == sid) {
                p.owners.remove(key);
            }
        }
        let mut groups = self.groups.lock().await;
        if groups.owners.get(key).is_some_and(|g| &g.sid == sid) {
            let group = groups.owners.remove(key).unwrap();
            if let Err(e) = self.retire(group).await {
                tracing::error!(%e,"local mutation admission retirement failed");
            }
        }
    }
    pub(crate) async fn shutdown(&self) {
        let groups = {
            let mut groups = self.groups.lock().await;
            groups.stopped = true;
            std::mem::take(&mut groups.owners)
        };
        self.presence
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .owners
            .clear();
        for (_, group) in groups {
            if let Err(e) = self.retire(group).await {
                tracing::error!(%e,"local mutation shutdown retirement failed");
            }
        }
        self.accepted.finish().await;
    }
}
impl crate::local_dispatch::LocalDispatch {
    pub(crate) async fn list_participants_typed(
        &self,
        r: pb::ListParticipantsRequest,
    ) -> Result<pb::ListParticipantsResponse, RpcError> {
        self.room
            .request(&room_spec(&r.room, "ListParticipants"), &r)
            .await
    }
    pub(crate) async fn get_participant_typed(
        &self,
        r: pb::RoomParticipantIdentity,
    ) -> Result<pb::ParticipantInfo, RpcError> {
        self.room
            .request(&room_spec(&r.room, "GetParticipant"), &r)
            .await
    }

    pub(crate) async fn delete_room(
        &self,
        r: pb::DeleteRoomRequest,
    ) -> Result<pb::DeleteRoomResponse, RpcError> {
        self.room
            .request(&room_spec(&r.room, "DeleteRoom"), &r)
            .await
    }
    pub(crate) async fn update_room_metadata(
        &self,
        r: pb::UpdateRoomMetadataRequest,
    ) -> Result<pb::Room, RpcError> {
        self.room
            .request(&room_spec(&r.room, "UpdateRoomMetadata"), &r)
            .await
    }
    pub(crate) async fn remove_participant(
        &self,
        r: pb::RoomParticipantIdentity,
    ) -> Result<pb::RemoveParticipantResponse, RpcError> {
        self.participant
            .request(
                &participant_spec(&r.room, &r.identity, "RemoveParticipant"),
                &r,
            )
            .await
    }
    pub(crate) async fn mute_published_track(
        &self,
        r: pb::MuteRoomTrackRequest,
    ) -> Result<pb::MuteRoomTrackResponse, RpcError> {
        self.participant
            .request(
                &participant_spec(&r.room, &r.identity, "MutePublishedTrack"),
                &r,
            )
            .await
    }
    pub(crate) async fn update_participant(
        &self,
        r: pb::UpdateParticipantRequest,
    ) -> Result<pb::ParticipantInfo, RpcError> {
        self.participant
            .request(
                &participant_spec(&r.room, &r.identity, "UpdateParticipant"),
                &r,
            )
            .await
    }
    pub(crate) async fn update_subscriptions(
        &self,
        r: pb::UpdateSubscriptionsRequest,
    ) -> Result<pb::UpdateSubscriptionsResponse, RpcError> {
        self.participant
            .request(
                &participant_spec(&r.room, &r.identity, "UpdateSubscriptions"),
                &r,
            )
            .await
    }
}
#[cfg(test)]
mod tests;
