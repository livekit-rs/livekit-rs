//! Fixed Participant PSRPC consumer for real owner metadata/permission updates.
//! protocol/rpc/typed_api.go209 formats the topic as room_identity; the service
//! is Participant, not a private Room.UpdateParticipant extension.
use super::*;
use rtc_runtime::ParticipantUpdate;
pub(super) fn participant_spec(room: &str, identity: &str, method: &str) -> MethodSpec {
    MethodSpec {
        service: "Participant".into(),
        method: method.into(),
        topics: vec![format!("{room}_{identity}")],
        require_claim: true,
        queue: true,
    }
}
impl RoomControl {
    pub async fn worker_room_sid(&self, name: &str) -> Result<String, RpcError> {
        self.allocator
            .catalog
            .load(name)
            .await
            .map(|room| room.room.message.sid)
            .map_err(RpcError::from)
    }
    pub async fn update_participant(
        &self,
        request: pb::UpdateParticipantRequest,
    ) -> Result<pb::ParticipantInfo, RpcError> {
        // Source OSS HasParticipant is HEXISTS, not a local owner lookup or
        // protobuf decode: malformed stored bytes do not alter this precheck.
        if !self
            .allocator
            .catalog
            .has_participant(&request.room, &request.identity)
            .await?
        {
            return Err(RpcError::new("not_found", "participant does not exist"));
        }
        self.configured(
            &participant_spec(&request.room, &request.identity, "UpdateParticipant"),
            &request,
        )
        .await
    }
    pub(super) async fn reconcile_participant_topics(
        &self,
        projection: &rtc_core::events::RoomProjection,
        is_current: &(impl Fn() -> bool + Send + Sync),
    ) -> Result<(), RpcError> {
        let current = || {
            if is_current() {
                Ok(())
            } else {
                Err(RpcError::new(
                    "aborted",
                    "participant projection changed during topic registration",
                ))
            }
        };
        let mut groups = self.generations.lock().await;
        current()?;
        let generation = groups
            .get_mut(&projection.room.name.0)
            .filter(|generation| generation.sid == projection.room.sid)
            .ok_or_else(|| RpcError::new("aborted", "participant room generation retired"))?;
        let owner = self
            .state
            .manager
            .capture_room_owner(
                &self.state.room_key(&projection.room.name.0),
                &projection.room.sid,
            )
            .map_err(api)?;
        let desired = projection
            .participants
            .iter()
            .map(|participant| participant.identity.0.clone())
            .collect();
        self.sync_participant_topics(&owner, &mut generation.participants, desired, &current)
            .await
    }
    pub(super) async fn sync_participant_topics(
        &self,
        owner: &RoomOwnerHandle,
        topics: &mut BTreeMap<String, Vec<RpcServer>>,
        desired: std::collections::BTreeSet<String>,
        current: &(impl Fn() -> Result<(), RpcError> + Send + Sync),
    ) -> Result<(), RpcError> {
        current()?;
        for identity in topics
            .keys()
            .filter(|identity| !desired.contains(*identity))
            .cloned()
            .collect::<Vec<_>>()
        {
            if let Some(servers) = topics.remove(&identity) {
                for server in servers {
                    self.accepted.retire_server(owner.sid(), server).await?;
                }
            }
            current()?;
        }
        for identity in desired {
            if let Some(servers) = topics.get_mut(&identity) {
                self.register_remaining_participant_topics(owner, &identity, servers)
                    .await?;
                continue;
            }
            current()?;
            let captured = owner.clone();
            let registered = identity.clone();
            let accepted = self.accepted.clone();
            let server = RpcServer::register_with_raw::<
                pb::UpdateParticipantRequest,
                pb::ParticipantInfo,
                _,
                _,
            >(
                self.bus.clone(),
                participant_spec(&owner.key().name.0, &identity, "UpdateParticipant"),
                self.limits,
                move |_, request| {
                    let owner = captured.clone();
                    let registered = registered.clone();
                    let accepted = accepted.clone();
                    async move {
                        let sid = owner.sid().clone();
                        accepted
                            .run(&sid, async move {
                                check(&owner, &request.message.room)?;
                                if request.message.identity != registered {
                                    return Err(RpcError::new(
                                        "failed_precondition",
                                        "participant identity does not match registered topic",
                                    ));
                                }
                                let request = request.message;
                                let updated = owner
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
                                    .map_err(api)?;
                                Ok(convert::participant_info_to_proto(&updated))
                            })
                            .await
                    }
                },
                None,
            )
            .await?;
            // Insert immediately: partial registration remains owned and is
            // retired with this exact room generation on error/cancellation.
            topics.insert(identity.clone(), vec![server]);
            self.register_remaining_participant_topics(
                owner,
                &identity,
                topics.get_mut(&identity).unwrap(),
            )
            .await?;
            current()?;
        }
        Ok(())
    }
}

impl RoomControl {
    pub async fn remove_participant(
        &self,
        request: pb::RoomParticipantIdentity,
    ) -> Result<pb::RemoveParticipantResponse, RpcError> {
        if !self
            .allocator
            .catalog
            .has_participant(&request.room, &request.identity)
            .await?
        {
            return Err(RpcError::new("not_found", "participant does not exist"));
        }
        self.configured(
            &participant_spec(&request.room, &request.identity, "RemoveParticipant"),
            &request,
        )
        .await
    }
    pub async fn mute_published_track(
        &self,
        request: pb::MuteRoomTrackRequest,
    ) -> Result<pb::MuteRoomTrackResponse, RpcError> {
        self.configured(
            &participant_spec(&request.room, &request.identity, "MutePublishedTrack"),
            &request,
        )
        .await
    }
    pub async fn update_subscriptions(
        &self,
        request: pb::UpdateSubscriptionsRequest,
    ) -> Result<pb::UpdateSubscriptionsResponse, RpcError> {
        self.configured(
            &participant_spec(&request.room, &request.identity, "UpdateSubscriptions"),
            &request,
        )
        .await
    }
    pub async fn send_data(
        &self,
        request: pb::SendDataRequest,
    ) -> Result<pb::SendDataResponse, RpcError> {
        self.configured(&super::room_spec(&request.room, "SendData"), &request)
            .await
    }
    pub async fn forward_participant(
        &self,
        request: pb::ForwardParticipantRequest,
    ) -> Result<pb::ForwardParticipantResponse, RpcError> {
        self.configured(
            &participant_spec(&request.room, &request.identity, "ForwardParticipant"),
            &request,
        )
        .await
    }
    pub async fn move_participant(
        &self,
        request: pb::MoveParticipantRequest,
    ) -> Result<pb::MoveParticipantResponse, RpcError> {
        self.configured(
            &participant_spec(&request.room, &request.identity, "MoveParticipant"),
            &request,
        )
        .await
    }
    async fn register_remaining_participant_topics(
        &self,
        owner: &RoomOwnerHandle,
        identity: &str,
        servers: &mut Vec<RpcServer>,
    ) -> Result<(), RpcError> {
        macro_rules! register {
            ($count:expr, $method:literal, $req:ty, $resp:ty, $owner:ident, $request:ident, $body:expr) => {
                if servers.len() < $count {
                    let captured = owner.clone();
                    let registered = identity.to_owned();
                    let accepted = self.accepted.clone();
                    servers.push(
                        RpcServer::register_with_raw::<$req, $resp, _, _>(
                            self.bus.clone(),
                            participant_spec(&owner.key().name.0, identity, $method),
                            self.limits,
                            move |_, raw| {
                                let $owner = captured.clone();
                                let registered = registered.clone();
                                let $request = raw.message;
                                let accepted = accepted.clone();
                                async move {
                                    let sid = $owner.sid().clone();
                                    accepted.run(&sid, async move {
                                    check(&$owner, &$request.room)?;
                                    if $request.identity != registered {
                                        return Err(RpcError::new(
                                            "failed_precondition",
                                            "participant identity does not match registered topic",
                                        ));
                                    }
                                    $body
                                    }).await
                                }
                            },
                            None,
                        )
                        .await?,
                    );
                }
            };
        }
        register!(
            2,
            "RemoveParticipant",
            pb::RoomParticipantIdentity,
            pb::RemoveParticipantResponse,
            actor,
            request,
            {
                actor
                    .remove_participant(Identity(request.identity))
                    .await
                    .map_err(api)?;
                Ok(pb::RemoveParticipantResponse {})
            }
        );
        register!(
            3,
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
                    .map_err(api)?;
                Ok(pb::MuteRoomTrackResponse {
                    track: track.as_ref().map(convert::track_info_to_proto),
                })
            }
        );
        register!(
            4,
            "UpdateSubscriptions",
            pb::UpdateSubscriptionsRequest,
            pb::UpdateSubscriptionsResponse,
            actor,
            request,
            {
                let mut tracks: Vec<_> = request
                    .track_sids
                    .into_iter()
                    .map(rtc_core::ids::TrackSid)
                    .collect();
                for participant in request.participant_tracks {
                    tracks.extend(
                        participant
                            .track_sids
                            .into_iter()
                            .map(rtc_core::ids::TrackSid),
                    );
                }
                actor
                    .update_subscriptions(Identity(request.identity), tracks, request.subscribe)
                    .await
                    .map_err(api)?;
                Ok(pb::UpdateSubscriptionsResponse {})
            }
        );
        // Fixed OSS RoomManager implements these registered consumers as errors,
        // not media migration. Missing topics still follow real PSRPC selection.
        register!(
            5,
            "ForwardParticipant",
            pb::ForwardParticipantRequest,
            pb::ForwardParticipantResponse,
            actor,
            request,
            {
                let _ = (&actor, &request);
                Err(RpcError::new("unknown", "not implemented"))
            }
        );
        register!(
            6,
            "MoveParticipant",
            pb::MoveParticipantRequest,
            pb::MoveParticipantResponse,
            actor,
            request,
            {
                let _ = (&actor, &request);
                Err(RpcError::new("unknown", "not implemented"))
            }
        );
        if servers.len() < 7 {
            servers.push(self.register_perform_rpc(owner, identity).await?);
        }
        Ok(())
    }
}

impl RoomControl {
    pub(super) async fn register_room_send_data(
        &self,
        owner: &RoomOwnerHandle,
    ) -> Result<RpcServer, RpcError> {
        super::accepted::register_send_data(
            self.bus.clone().into(),
            owner,
            self.limits,
            self.accepted.clone(),
        )
        .await
    }
}

impl RoomControl {
    pub async fn perform_rpc(
        &self,
        request: pb::PerformRpcRequest,
    ) -> Result<pb::PerformRpcResponse, RpcError> {
        self.configured(
            &participant_spec(&request.room, &request.destination_identity, "PerformRpc"),
            &request,
        )
        .await
    }
    async fn register_perform_rpc(
        &self,
        owner: &RoomOwnerHandle,
        identity: &str,
    ) -> Result<RpcServer, RpcError> {
        super::accepted::register_perform_rpc(
            self.bus.clone().into(),
            owner,
            identity,
            self.state.clone(),
            self.limits,
            self.accepted.clone(),
        )
        .await
    }
}
