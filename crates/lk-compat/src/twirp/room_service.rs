//! `livekit.RoomService` (impl-notes §2.4, `pkg/service/roomservice.go`).

#[cfg(test)]
use std::collections::BTreeMap;

use axum::response::Response;
use lk_protocol::livekit as pb;
use lk_protocol::twirp_json::livekit as tj;
use prost::Message;
use rtc_core::ids::*;
use rtc_core::permission::RoomConfig;
use rtc_runtime::{ApiError, CreateRoomOptions};

use super::codec::{Codec, decode, encode};
use super::error::{Code, TwirpError};
use crate::auth::VerifiedToken;
use crate::auth::permissions::*;
use crate::convert::*;
use crate::state::AppState;

pub const SERVICE: &str = "livekit.RoomService";

pub const METHODS: &[&str] = &[
    "CreateRoom",
    "ListRooms",
    "DeleteRoom",
    "ListParticipants",
    "GetParticipant",
    "RemoveParticipant",
    "MutePublishedTrack",
    "UpdateParticipant",
    "UpdateSubscriptions",
    "SendData",
    "UpdateRoomMetadata",
    "ForwardParticipant",
    "MoveParticipant",
    "PerformRpc",
];

type Auth<'a> = Option<&'a VerifiedToken>;
type TwirpResult<T> = Result<T, TwirpError>;

/// Decode, run, encode.
macro_rules! unary {
    ($codec:expr, $body:expr, $req:ty, $resp:ty, $twin:ty, $f:expr) => {{
        let req: $req = decode($codec, $body)?;
        let resp: $resp = $f(req).await?;
        Ok(encode::<$resp, $twin>($codec, &resp))
    }};
}

pub async fn call(
    state: &AppState,
    auth: Auth<'_>,
    method: &str,
    codec: Codec,
    body: &[u8],
) -> TwirpResult<Response> {
    let s = state;
    match method {
        "CreateRoom" => unary!(
            codec,
            body,
            pb::CreateRoomRequest,
            pb::Room,
            tj::Room,
            |r| create_room(s, auth, r)
        ),
        "ListRooms" => unary!(
            codec,
            body,
            pb::ListRoomsRequest,
            pb::ListRoomsResponse,
            tj::ListRoomsResponse,
            |r| list_rooms(s, auth, r)
        ),
        "DeleteRoom" => unary!(
            codec,
            body,
            pb::DeleteRoomRequest,
            pb::DeleteRoomResponse,
            tj::DeleteRoomResponse,
            |r| Box::pin(delete_room(s, auth, r))
        ),
        "ListParticipants" => unary!(
            codec,
            body,
            pb::ListParticipantsRequest,
            pb::ListParticipantsResponse,
            tj::ListParticipantsResponse,
            |r| Box::pin(list_participants(s, auth, r))
        ),
        "GetParticipant" => unary!(
            codec,
            body,
            pb::RoomParticipantIdentity,
            pb::ParticipantInfo,
            tj::ParticipantInfo,
            |r| Box::pin(get_participant(s, auth, r))
        ),
        "RemoveParticipant" => unary!(
            codec,
            body,
            pb::RoomParticipantIdentity,
            pb::RemoveParticipantResponse,
            tj::RemoveParticipantResponse,
            |r| Box::pin(remove_participant(s, auth, r))
        ),
        "MutePublishedTrack" => unary!(
            codec,
            body,
            pb::MuteRoomTrackRequest,
            pb::MuteRoomTrackResponse,
            tj::MuteRoomTrackResponse,
            |r| Box::pin(mute_published_track(s, auth, r))
        ),
        "UpdateParticipant" => unary!(
            codec,
            body,
            pb::UpdateParticipantRequest,
            pb::ParticipantInfo,
            tj::ParticipantInfo,
            |r| Box::pin(update_participant(s, auth, r))
        ),
        "UpdateSubscriptions" => unary!(
            codec,
            body,
            pb::UpdateSubscriptionsRequest,
            pb::UpdateSubscriptionsResponse,
            tj::UpdateSubscriptionsResponse,
            |r| Box::pin(update_subscriptions(s, auth, r))
        ),
        "SendData" => unary!(
            codec,
            body,
            pb::SendDataRequest,
            pb::SendDataResponse,
            tj::SendDataResponse,
            |r| Box::pin(send_data(s, auth, r))
        ),
        "UpdateRoomMetadata" => unary!(
            codec,
            body,
            pb::UpdateRoomMetadataRequest,
            pb::Room,
            tj::Room,
            |r| Box::pin(update_room_metadata(s, auth, r))
        ),
        "ForwardParticipant" => unary!(
            codec,
            body,
            pb::ForwardParticipantRequest,
            pb::ForwardParticipantResponse,
            tj::ForwardParticipantResponse,
            |r: pb::ForwardParticipantRequest| forward_or_move(
                s,
                auth,
                r.room,
                r.identity,
                r.destination_room,
                false
            )
        ),
        "MoveParticipant" => unary!(
            codec,
            body,
            pb::MoveParticipantRequest,
            pb::MoveParticipantResponse,
            tj::MoveParticipantResponse,
            |r: pb::MoveParticipantRequest| forward_or_move(
                s,
                auth,
                r.room,
                r.identity,
                r.destination_room,
                true
            )
        ),
        "PerformRpc" => unary!(
            codec,
            body,
            pb::PerformRpcRequest,
            pb::PerformRpcResponse,
            tj::PerformRpcResponse,
            // Keep the large typed PSRPC state machine out of every public
            // dispatcher future (including unrelated Forward/Create methods).
            |r| Box::pin(perform_rpc(s, auth, r))
        ),
        _ => Err(TwirpError::internal("unreachable: unknown method")),
    }
}

// ------------------------------------------------------------------ limits

fn within(limit: Option<usize>, len: usize) -> bool {
    match limit {
        None | Some(0) => true,
        Some(max) => len <= max,
    }
}

fn attributes_size<'a>(attrs: impl IntoIterator<Item = (&'a String, &'a String)>) -> usize {
    attrs.into_iter().map(|(k, v)| k.len() + v.len()).sum()
}

/// Public UpdateParticipant name preflight, including derived service calls.
/// The source checks even an absent/empty Name before metadata and lookup.
pub(crate) fn check_participant_name_limit(limit: i64, name: &str) -> TwirpResult<()> {
    if rtc_core::config::signed_byte_length_allowed(limit, name.len()) {
        Ok(())
    } else {
        Err(TwirpError::invalid_argument_arg(
            "name length exceeds limits",
            &limit.to_string(),
        ))
    }
}

fn check_metadata(state: &AppState, metadata: &str) -> TwirpResult<()> {
    let limit = state.config.limits.max_metadata_size;
    if within(limit, metadata.len()) {
        Ok(())
    } else {
        Err(TwirpError::invalid_argument_arg(
            "metadata size exceeds limits",
            &limit.unwrap_or(0).to_string(),
        ))
    }
}

fn check_attributes<'a>(
    state: &AppState,
    attrs: impl IntoIterator<Item = (&'a String, &'a String)>,
) -> TwirpResult<()> {
    let limit = state.config.limits.max_attributes_size;
    if within(limit, attributes_size(attrs)) {
        Ok(())
    } else {
        Err(TwirpError::invalid_argument_arg(
            "attribute size exceeds limits",
            &limit.unwrap_or(0).to_string(),
        ))
    }
}

/// `ErrParticipantNotFound` from the store pre-check.
async fn require_participant(
    state: &AppState,
    key: &RoomKey,
    identity: &Identity,
) -> TwirpResult<()> {
    if state
        .local_dispatch
        .mutations
        .has_participant(key, identity)
    {
        Ok(())
    } else {
        Err(TwirpError::new(
            Code::NotFound,
            "participant does not exist",
        ))
    }
}

// ------------------------------------------------------------------ methods

/// Request values that upstream treats as set (> 0 / non-empty / true).
pub(crate) fn room_config_from_request(r: &pb::CreateRoomRequest) -> RoomConfig {
    RoomConfig {
        tags: r.tags.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        automatic_recording: r
            .egress
            .as_ref()
            .map(crate::workers::auto_egress_policy::room_egress_in),
        empty_timeout_s: (r.empty_timeout > 0).then_some(r.empty_timeout),
        departure_timeout_s: (r.departure_timeout > 0).then_some(r.departure_timeout),
        max_participants: (r.max_participants > 0).then_some(r.max_participants),

        metadata: (!r.metadata.is_empty()).then(|| r.metadata.clone()),
        min_playout_delay: (r.min_playout_delay > 0).then_some(r.min_playout_delay),
        max_playout_delay: (r.max_playout_delay > 0).then_some(r.max_playout_delay),
        sync_streams: r.sync_streams.then_some(true),
        agent_dispatches: (!r.agents.is_empty()).then(|| {
            r.agents
                .iter()
                .map(crate::auth::claims::room_agent_from_proto)
                .collect()
        }),
    }
}

/// Test oracle for fixed allocator defaults; production resolves under owner lock.
#[cfg(test)]
fn apply_preset(mut c: RoomConfig, p: &RoomConfig) -> RoomConfig {
    c.empty_timeout_s = c.empty_timeout_s.or(p.empty_timeout_s);
    c.departure_timeout_s = c.departure_timeout_s.or(p.departure_timeout_s);
    c.max_participants = c.max_participants.or(p.max_participants);
    c.metadata = c.metadata.or_else(|| p.metadata.clone());
    c.min_playout_delay = c.min_playout_delay.or(p.min_playout_delay);
    c.max_playout_delay = c.max_playout_delay.or(p.max_playout_delay);
    c.sync_streams = c.sync_streams.or(p.sync_streams);
    c.agent_dispatches = c.agent_dispatches.or_else(|| p.agent_dispatches.clone());
    c.automatic_recording = c
        .automatic_recording
        .or_else(|| p.automatic_recording.clone());
    c
}

async fn create_room(
    state: &AppState,
    auth: Auth<'_>,
    req: pb::CreateRoomRequest,
) -> TwirpResult<pb::Room> {
    ensure_create(auth)?;
    if req.egress.is_some() && state.worker_services.is_none() {
        return Err(TwirpError::internal(
            "egress not connected (redis required)",
        ));
    }

    let name_limit = state.config.limits.max_room_name_length;
    if !rtc_core::config::signed_byte_length_allowed(name_limit, req.name.len()) {
        // The wrapped sentinel in service/errors.go is psrpc.InvalidArgument.
        // The installed error conversion unwraps it; wrapping preserves its code.
        return Err(TwirpError::new(
            Code::InvalidArgument,
            format!("room name length exceeds limits: max length {}", name_limit),
        ));
    }
    check_metadata(state, &req.metadata)?;
    for agent in &req.agents {
        check_metadata(state, &agent.metadata)?;
        check_attributes(state, &agent.attributes)?;
    }

    if let Some(router) = &state.room_control {
        return router
            .create_room(req)
            .await
            .map_err(crate::twirp::worker_error::failure);
    }
    create_local_owner_request(state, req).await
}

/// Internal routing CreateRoom boundary shared by public RoomService and
/// AgentDispatch auto-create. Each public adapter performs its own source auth
/// and preflight; this does not impose a roomCreate grant on room-admin agents.
pub(crate) async fn create_local_owner_request(
    state: &AppState,
    req: pb::CreateRoomRequest,
) -> TwirpResult<pb::Room> {
    state
        .check_local_room_allocation()
        .map_err(crate::twirp::worker_error::failure)?;
    let key = state.room_key(&req.name);
    let manager = state.manager.clone();
    let workers = state.worker_services.clone();
    state
        .room_creates
        .request(&state.config.room, move |deadline, expiry_ns| {
            let manager = manager.clone();
            let workers = workers.clone();
            let key = key.clone();
            let request = req.clone();
            async move {
                create_local_owner(manager, workers, key, request, deadline, expiry_ns)
                    .await
                    .map_err(|error| {
                        let mut result =
                            crate::service_bus::rpc::RpcError::new(error.code.as_str(), error.msg);
                        result.application_metadata = error.meta;
                        result
                    })
            }
        })
        .await
        .map_err(super::worker_error::failure)
}

// This owned operation never captures AppState/its task registry. Client request
// timeout/drop cannot delete the room, release a later SID, or abort Background
// automatic egress. Global worker shutdown cancels that work before registry join.
async fn create_local_owner(
    manager: rtc_runtime::RoomManager,
    workers: Option<std::sync::Arc<crate::worker_services::WorkerServices>>,
    key: rtc_core::ids::RoomKey,
    req: pb::CreateRoomRequest,
    deadline: std::time::Instant,
    expiry_ns: i64,
) -> TwirpResult<pb::Room> {
    let config = room_config_from_request(&req);
    let lease = manager
        .create_room_lease_with_preset(key, CreateRoomOptions { config }, &req.room_preset)
        .await?;
    let auto_room = lease
        .composite_policy()
        .map(crate::workers::auto_egress_policy::composite_out);
    if lease.created()
        && let Some(mut composite) = auto_room
    {
        let room = lease.snapshot();
        // Use the original room's identity, never a second room lookup after
        // awaiting a worker. Source overwrites a supplied conflicting room name.
        composite.room_name = req.name;
        let services = workers
            .as_ref()
            .ok_or_else(|| TwirpError::internal("egress not connected (redis required)"))?;
        let mut start = crate::workers::egress::wrap_room_wire(
            crate::workers::preserved::WireValue::from_message(composite),
            rtc_core::ids::new_sid("EG_"),
        );
        start.message.room_id = room.sid.0.clone();
        // Direct CreateRoom retains its accepted RPC attempt expiry. Global
        // automatic cancellation remains in force; background Track/Participant
        // launches elsewhere deliberately keep their independent lifetime.
        let mut options = services.automatic_options();
        options.deadline = Some(deadline);
        options.expiry_ns = Some(expiry_ns);
        services
            .egress
            .start_owned_wire(start, options)
            .await
            .map_err(super::worker_error::failure)?;
        // Upstream Release on failure is not DeleteRoom: the registered room
        // remains held by normal lifecycle/empty timeout. Never invent rollback.
    }
    // The actual held generation may have changed metadata/participants while
    // its real worker RPC was pending. Never return the pre-launch snapshot.
    Ok(room_info_to_proto(&lease.current_snapshot().await))
}

async fn list_rooms(
    state: &AppState,
    auth: Auth<'_>,
    req: pb::ListRoomsRequest,
) -> TwirpResult<pb::ListRoomsResponse> {
    ensure_list(auth)?;
    if let Some(router) = &state.room_control {
        return router
            .list_rooms(req)
            .await
            .map_err(crate::twirp::worker_error::failure);
    }
    let names: Vec<RoomName> = req.names.into_iter().map(RoomName).collect();
    let rooms = state.manager.list_rooms(&ProjectId::local(), &names);
    Ok(pb::ListRoomsResponse {
        rooms: rooms.iter().map(room_info_to_proto).collect(),
    })
}

async fn delete_room(
    state: &AppState,
    auth: Auth<'_>,
    req: pb::DeleteRoomRequest,
) -> TwirpResult<pb::DeleteRoomResponse> {
    ensure_create(auth)?;
    if let Some(router) = &state.room_control {
        return router
            .delete_room(req)
            .await
            .map_err(crate::twirp::worker_error::failure);
    }
    let key = state.room_key(&req.room);
    if !state.manager.room_exists(&key) {
        return Err(TwirpError::new(
            Code::NotFound,
            "requested room does not exist",
        ));
    }
    state
        .local_dispatch
        .delete_room(req)
        .await
        .map_err(crate::twirp::worker_error::failure)
}

async fn list_participants(
    state: &AppState,
    auth: Auth<'_>,
    req: pb::ListParticipantsRequest,
) -> TwirpResult<pb::ListParticipantsResponse> {
    ensure_admin(auth, &req.room)?;
    if let Some(router) = &state.room_control {
        return router
            .list_participants(req)
            .await
            .map_err(crate::twirp::worker_error::failure);
    }
    if state.config.enable_psrpc_for_get_list_participants {
        return state
            .local_dispatch
            .list_participants_typed(req)
            .await
            .map_err(crate::twirp::worker_error::failure);
    }
    let participants = state
        .local_dispatch
        .mutations
        .stored_participants(&state.room_key(&req.room));
    Ok(pb::ListParticipantsResponse {
        participants: participants.iter().map(participant_info_to_proto).collect(),
    })
}

async fn get_participant(
    state: &AppState,
    auth: Auth<'_>,
    req: pb::RoomParticipantIdentity,
) -> TwirpResult<pb::ParticipantInfo> {
    ensure_admin(auth, &req.room)?;
    if let Some(router) = &state.room_control {
        return router
            .get_participant(req)
            .await
            .map_err(crate::twirp::worker_error::failure);
    }
    if state.config.enable_psrpc_for_get_list_participants {
        return state
            .local_dispatch
            .get_participant_typed(req)
            .await
            .map_err(crate::twirp::worker_error::failure);
    }
    state
        .local_dispatch
        .mutations
        .stored_participant(&state.room_key(&req.room), &Identity(req.identity))
        .map(|p| participant_info_to_proto(&p))
        .map_err(crate::twirp::worker_error::failure)
}

async fn remove_participant(
    state: &AppState,
    auth: Auth<'_>,
    req: pb::RoomParticipantIdentity,
) -> TwirpResult<pb::RemoveParticipantResponse> {
    ensure_admin(auth, &req.room)?;
    if let Some(control) = &state.room_control {
        return control
            .remove_participant(req)
            .await
            .map_err(crate::twirp::worker_error::failure);
    }

    let key = state.room_key(&req.room);
    let identity = Identity(req.identity.clone());
    require_participant(state, &key, &identity).await?;
    state
        .local_dispatch
        .remove_participant(req)
        .await
        .map_err(crate::twirp::worker_error::failure)
}

async fn mute_published_track(
    state: &AppState,
    auth: Auth<'_>,
    req: pb::MuteRoomTrackRequest,
) -> TwirpResult<pb::MuteRoomTrackResponse> {
    ensure_admin(auth, &req.room)?;
    if let Some(control) = &state.room_control {
        return control
            .mute_published_track(req)
            .await
            .map_err(crate::twirp::worker_error::failure);
    }

    state
        .local_dispatch
        .mute_published_track(req)
        .await
        .map_err(crate::twirp::worker_error::failure)
}

async fn update_participant(
    state: &AppState,
    auth: Auth<'_>,
    req: pb::UpdateParticipantRequest,
) -> TwirpResult<pb::ParticipantInfo> {
    // Limits are checked before permissions (roomservice.go:263-275).
    check_participant_name_limit(state.config.limits.max_participant_name_length, &req.name)?;
    check_metadata(state, &req.metadata)?;
    check_attributes(state, &req.attributes)?;
    ensure_admin(auth, &req.room)?;

    if let Some(control) = &state.room_control {
        return control
            .update_participant(req)
            .await
            .map_err(crate::twirp::worker_error::failure);
    }
    let key = state.room_key(&req.room);
    let identity = Identity(req.identity.clone());
    require_participant(state, &key, &identity).await?;
    state
        .local_dispatch
        .update_participant(req)
        .await
        .map_err(crate::twirp::worker_error::failure)
}

async fn update_subscriptions(
    state: &AppState,
    auth: Auth<'_>,
    req: pb::UpdateSubscriptionsRequest,
) -> TwirpResult<pb::UpdateSubscriptionsResponse> {
    ensure_admin(auth, &req.room)?;
    if let Some(control) = &state.room_control {
        return control
            .update_subscriptions(req)
            .await
            .map_err(crate::twirp::worker_error::failure);
    }

    state
        .local_dispatch
        .update_subscriptions(req)
        .await
        .map_err(crate::twirp::worker_error::failure)
}

/// `DataPacket` the node builds for `SendData` (roommanager.go:977-998).
pub fn send_data_packet(req: &pb::SendDataRequest) -> pb::DataPacket {
    pb::DataPacket {
        kind: req.kind,
        destination_identities: req.destination_identities.clone(),
        value: Some(pb::data_packet::Value::User(pb::UserPacket {
            payload: req.data.clone(),
            destination_sids: req.destination_sids.clone(),
            destination_identities: req.destination_identities.clone(),
            topic: req.topic.clone(),
            nonce: req.nonce.clone(),
            ..Default::default()
        })),
        ..Default::default()
    }
}

async fn send_data(
    state: &AppState,
    auth: Auth<'_>,
    req: pb::SendDataRequest,
) -> TwirpResult<pb::SendDataResponse> {
    ensure_admin(auth, &req.room)?;
    if !req.nonce.is_empty() && req.nonce.len() != 16 {
        return Err(TwirpError::new(
            Code::InvalidArgument,
            format!(
                "nonce should be 16-bytes or not present, got: {} bytes",
                req.nonce.len()
            ),
        ));
    }
    if let Some(control) = &state.room_control {
        return control
            .send_data(req)
            .await
            .map_err(crate::twirp::worker_error::failure);
    }
    state
        .local_dispatch
        .send_data(req)
        .await
        .map_err(crate::twirp::worker_error::failure)
}

async fn update_room_metadata(
    state: &AppState,
    auth: Auth<'_>,
    req: pb::UpdateRoomMetadataRequest,
) -> TwirpResult<pb::Room> {
    check_metadata(state, &req.metadata)?;
    ensure_admin(auth, &req.room)?;
    if let Some(router) = &state.room_control {
        return router
            .update_metadata(req)
            .await
            .map_err(crate::twirp::worker_error::failure);
    }
    let key = state.room_key(&req.room);
    if !state.manager.room_exists(&key) {
        return Err(TwirpError::new(
            Code::NotFound,
            "requested room does not exist",
        ));
    }
    state
        .local_dispatch
        .update_room_metadata(req)
        .await
        .map_err(crate::twirp::worker_error::failure)
}

/// Forward/Move: permission and validation like upstream, then the OSS node
/// answers `errors.New("not implemented")` → `unknown` (roommanager.go:912-918).
async fn forward_or_move<R: Default>(
    state: &AppState,
    auth: Auth<'_>,
    room: String,
    identity: String,
    destination_room: String,
    moving: bool,
) -> TwirpResult<R> {
    ensure_dest_room(auth, &room, &destination_room)?;
    if room == destination_room {
        return Err(TwirpError::invalid_argument_arg(
            "destination room cannot be the same as source room",
            "",
        ));
    }
    if let Some(control) = &state.room_control {
        let result = if moving {
            control
                .move_participant(pb::MoveParticipantRequest {
                    room,
                    identity,
                    destination_room,
                    ..Default::default()
                })
                .await
                .map(|_| ())
        } else {
            control
                .forward_participant(pb::ForwardParticipantRequest {
                    room,
                    identity,
                    destination_room,
                    ..Default::default()
                })
                .await
                .map(|_| ())
        };
        result.map_err(crate::twirp::worker_error::failure)?;
        return Ok(R::default());
    }
    match state
        .manager
        .get_participant(&state.room_key(&room), &Identity(identity))
        .await
    {
        Ok(_) => Err(TwirpError::new(Code::Unknown, "not implemented")),
        // Nobody owns the participant topic: psrpc "no response from servers".
        Err(ApiError::NotFound(_)) | Err(ApiError::Unavailable(_)) => Err(TwirpError::new(
            Code::Unavailable,
            "no response from servers",
        )),
        Err(e) => Err(e.into()),
    }
}

async fn perform_rpc(
    state: &AppState,
    auth: Auth<'_>,
    req: pb::PerformRpcRequest,
) -> TwirpResult<pb::PerformRpcResponse> {
    ensure_admin(auth, &req.room)?;
    if req.destination_identity.is_empty() {
        return Err(TwirpError::new(
            Code::InvalidArgument,
            "destination identity is required",
        ));
    }
    if let Some(control) = &state.room_control {
        return control
            .perform_rpc(req)
            .await
            .map_err(crate::twirp::worker_error::failure);
    }
    state
        .local_dispatch
        .perform_rpc(req)
        .await
        .map_err(crate::twirp::worker_error::failure)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_auto_recording_keeps_empty_presence_and_zero_preset() {
        let none = room_config_from_request(&pb::CreateRoomRequest::default());
        assert!(none.automatic_recording.is_none());
        let value = room_config_from_request(&pb::CreateRoomRequest {
            egress: Some(pb::RoomEgress {
                participant: Some(pb::AutoParticipantEgress {
                    options: Some(pb::auto_participant_egress::Options::Preset(0)),
                    ..Default::default()
                }),
                tracks: Some(Default::default()),
                ..Default::default()
            }),
            ..Default::default()
        });
        let value = value.automatic_recording.unwrap();
        assert!(value.tracks.is_some());
        assert!(matches!(
            value.participant.unwrap().encoding,
            Some(rtc_core::auto_egress::RecordingEncoding::Preset(0))
        ));
    }

    #[test]
    fn limits_zero_disables() {
        assert!(within(None, 10));
        assert!(within(Some(0), 10));
        assert!(within(Some(10), 10));
        assert!(!within(Some(9), 10));
        let mut m = std::collections::HashMap::new();
        m.insert("ab".to_string(), "cde".to_string());
        assert_eq!(attributes_size(&m), 5);
    }

    #[test]
    fn preset_fills_only_unset() {
        let req = pb::CreateRoomRequest {
            empty_timeout: 10,
            ..Default::default()
        };
        let preset = RoomConfig {
            empty_timeout_s: Some(99),
            max_participants: Some(4),
            ..Default::default()
        };
        let c = apply_preset(room_config_from_request(&req), &preset);
        assert_eq!(c.empty_timeout_s, Some(10));
        assert_eq!(c.max_participants, Some(4));
        assert_eq!(c.metadata, None);
    }

    #[test]
    fn send_data_packet_layout() {
        let req = pb::SendDataRequest {
            room: "r".into(),
            data: b"x".to_vec(),
            kind: 1,
            destination_identities: vec!["bob".into()],
            topic: Some("t".into()),
            ..Default::default()
        };
        let dp = send_data_packet(&req);
        assert_eq!(dp.kind, 1);
        assert_eq!(dp.destination_identities, vec!["bob".to_string()]);
        assert!(dp.participant_identity.is_empty());
        match dp.value {
            Some(pb::data_packet::Value::User(u)) => {
                assert_eq!(u.payload, b"x");
                assert_eq!(u.topic.as_deref(), Some("t"));
                assert_eq!(u.destination_identities, vec!["bob".to_string()]);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn room_agents_in_preset_fill_only_unset_request_agents() {
        use rtc_core::permission::RoomAgentDispatchOptions;
        let preset = RoomConfig {
            agent_dispatches: Some(vec![RoomAgentDispatchOptions {
                agent_name: "preset".into(),
                ..Default::default()
            }]),
            ..Default::default()
        };
        let inherited = apply_preset(
            room_config_from_request(&pb::CreateRoomRequest::default()),
            &preset,
        );
        assert_eq!(inherited.agent_dispatches, preset.agent_dispatches);
        let explicit = room_config_from_request(&pb::CreateRoomRequest {
            agents: vec![pb::RoomAgentDispatch {
                agent_name: "explicit".into(),
                ..Default::default()
            }],
            ..Default::default()
        });
        assert_eq!(
            apply_preset(explicit, &preset).agent_dispatches.unwrap()[0].agent_name,
            "explicit"
        );
    }
}

#[cfg(test)]
mod playout_request_tests {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use rtc_core::events::NullSink;
    use rtc_runtime::net::{LocalInterface, NetConfig, NetRouter};
    use std::sync::Arc;
    use tower::ServiceExt;

    #[test]
    fn preset_merges_endpoint_intent_without_falling_back_to_global_pair() {
        let preset = RoomConfig {
            min_playout_delay: Some(40),
            max_playout_delay: Some(100),
            ..Default::default()
        };
        let explicit = room_config_from_request(&pb::CreateRoomRequest {
            min_playout_delay: 30,
            ..Default::default()
        });
        let merged = apply_preset(explicit, &preset);
        assert_eq!(
            (merged.min_playout_delay, merged.max_playout_delay),
            (Some(30), Some(100))
        );
        let token = crate::auth::claims::room_config_from_proto(&pb::RoomConfiguration {
            min_playout_delay: 30,
            ..Default::default()
        });
        assert_eq!(token.min_playout_delay, Some(30));
        assert_eq!(token.max_playout_delay, None);
    }

    #[tokio::test]
    async fn actual_create_room_route_preserves_active_room_and_recreates_generation() {
        let mut config = crate::test_support::config();
        config.room.playout_delay = Some((100, 200));
        config.room.presets.insert(
            "playout".into(),
            Some(RoomConfig {
                min_playout_delay: Some(40),
                max_playout_delay: Some(100),
                metadata: Some("preset-metadata".into()),
                ..Default::default()
            }),
        );
        let key = config.keys[0].clone();
        let net = NetRouter::start(NetConfig {
            udp_enabled: false,
            udp_port: None,
            port_range: (0, 0),
            tcp_port: None,
            interfaces: vec![LocalInterface {
                bind: std::net::Ipv4Addr::LOCALHOST.into(),
                advertise: std::net::Ipv4Addr::LOCALHOST.into(),
            }],
        })
        .await
        .unwrap();
        let state = crate::state::CompatServices::new(Arc::new(config))
            .assemble(net.clone(), Arc::new(NullSink));
        let token = crate::auth::jwt::tests::make_token(r#"{"alg":"HS256"}"#,
            &serde_json::json!({"iss":key.0,"sub":"admin","exp":99999999999_u64,"video":{"roomCreate":true}}).to_string(),&key.1);
        let invoke = |body: serde_json::Value| {
            crate::http::router(state.clone()).oneshot(
                Request::post("/twirp/livekit.RoomService/CreateRoom")
                    .header("authorization", format!("Bearer {token}"))
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
        };
        let first = invoke(serde_json::json!({"name":"playout-api","room_preset":"playout","min_playout_delay":30})).await.unwrap();
        assert_eq!(first.status(), StatusCode::OK);
        let first: serde_json::Value =
            serde_json::from_slice(&to_bytes(first.into_body(), 65536).await.unwrap()).unwrap();
        assert_eq!(first["metadata"], "preset-metadata");
        let active = invoke(serde_json::json!({"name":"playout-api","room_preset":"missing","max_playout_delay":900})).await.unwrap();
        assert_eq!(active.status(), StatusCode::OK);
        let active: serde_json::Value =
            serde_json::from_slice(&to_bytes(active.into_body(), 65536).await.unwrap()).unwrap();
        assert_eq!(active["sid"], first["sid"]);
        state
            .manager()
            .delete_room(&state.room_key("playout-api"))
            .await
            .unwrap();
        let next = invoke(serde_json::json!({"name":"playout-api","max_playout_delay":90}))
            .await
            .unwrap();
        assert_eq!(next.status(), StatusCode::OK);
        let next: serde_json::Value =
            serde_json::from_slice(&to_bytes(next.into_body(), 65536).await.unwrap()).unwrap();
        assert_ne!(next["sid"], first["sid"]);
        state.manager().close_all().await;
        net.shutdown();
    }
}

#[cfg(test)]
mod signed_name_consumer_tests {
    use super::*;
    #[tokio::test]
    async fn signed_frontend_checks_preserve_permission_order_and_do_not_create_owner() {
        let (mut state, net) = crate::test_support::state().await;
        std::sync::Arc::make_mut(&mut state.config)
            .limits
            .max_room_name_length = -1;
        std::sync::Arc::make_mut(&mut state.config)
            .limits
            .max_participant_name_length = -1;
        let mut auth = VerifiedToken::default();
        auth.grants.video.room_create = true;
        auth.extra.has_video = true;
        for name in ["", "a", "é"] {
            let request = pb::CreateRoomRequest {
                name: name.into(),
                ..Default::default()
            };
            assert_eq!(
                create_room(&state, None, request.clone())
                    .await
                    .unwrap_err()
                    .code,
                Code::Unauthenticated
            );
            let error = create_room(&state, Some(&auth), request).await.unwrap_err();
            assert_eq!(error.code, Code::InvalidArgument);
            assert_eq!(error.msg, "room name length exceeds limits: max length -1");
            assert!(!state.manager().room_exists(&state.room_key(name)));
            // Name is checked even when omitted, before admin authorization.
            let error = update_participant(
                &state,
                None,
                pb::UpdateParticipantRequest {
                    name: name.into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
            assert_eq!(error.code, Code::InvalidArgument);
            assert_eq!(error.msg, "name length exceeds limits -1");
        }
        std::sync::Arc::make_mut(&mut state.config)
            .limits
            .max_participant_name_length = 0;
        assert_eq!(
            update_participant(
                &state,
                None,
                pb::UpdateParticipantRequest {
                    name: "é".repeat(1000),
                    ..Default::default()
                }
            )
            .await
            .unwrap_err()
            .code,
            Code::Unauthenticated
        );
        state.manager().close_all().await;
        net.shutdown();
    }
}

#[cfg(test)]
mod room_tag_api_projection_tests {
    use super::*;
    #[test]
    fn explicit_create_request_tags_survive_neutral_projection_without_preset_inheritance() {
        let request = pb::CreateRoomRequest {
            tags: [("category".to_owned(), "voice".to_owned())]
                .into_iter()
                .collect(),
            ..Default::default()
        };
        let mut neutral = room_config_from_request(&request);
        assert_eq!(neutral.tags.get("category").unwrap(), "voice");
        let preset = RoomConfig {
            tags: [(
                "preset".to_owned(),
                "ignored-by-upstream-named-allocator".to_owned(),
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        };
        neutral.inherit(&preset);
        assert_eq!(neutral.tags.len(), 1);
    }
}

#[cfg(test)]
mod open_data_kind_owner_tests {
    use super::*;
    use rtc_core::data::{DataHandler, DataSender, DataVerdict};
    use rtc_core::media_api::{DataKind, MediaCommand, MediaEvent, PcRole};
    use rtc_core::room::{Effect, Room};
    use rtc_core::session::*;
    use std::sync::Arc;
    use std::time::Instant;
    struct DropData;
    impl DataHandler for DropData {
        fn inbound(&self, _: &DataSender<'_>, _: DataKind, _: &str, _: &[u8]) -> DataVerdict {
            DataVerdict::Drop
        }
    }
    fn params() -> JoinParams {
        JoinParams {
            room: RoomKey::new(ProjectId::local(), "open-kind"),
            grants: rtc_core::permission::Grants {
                identity: "target".into(),
                video: rtc_core::permission::VideoGrant {
                    room_join: true,
                    room: "open-kind".into(),
                    ..Default::default()
                },
                ..Default::default()
            },
            identity: "target".into(),
            name: String::new(),
            metadata: String::new(),
            attributes: BTreeMap::new(),
            client: ClientMeta {
                sdk: SdkKind::Js,
                protocol: 17,
                ..Default::default()
            },
            capabilities: ClientCapabilities {
                subscriber_primary: true,
                ..Default::default()
            },
            auto_subscribe: true,
            auto_subscribe_data_track: false,
            adaptive_stream: false,
            single_pc: true,
            disable_ice_lite: false,
            subscriber_allow_pause: None,
            reconnect: false,
            reconnect_reason: 0,
            participant_sid: None,
            add_tracks: vec![],
            publisher_offer: None,
        }
    }
    pub(super) fn owner(active: bool) -> (Room, ParticipantSid) {
        let now = Instant::now();
        let cfg = crate::test_support::config();
        let mut room = Room::new(
            RoomKey::new(ProjectId::local(), "open-kind"),
            Arc::new(rtc_core::config::RoomPolicy::from(&cfg)),
            Arc::new(DropData),
            &Default::default(),
            now,
        );
        room.take_effects();
        let joined = room.join(params(), now).unwrap();
        let transport = room
            .take_effects()
            .into_iter()
            .find_map(|e| match e {
                Effect::Media(MediaCommand::CreateTransport(spec))
                    if spec.role == PcRole::Single =>
                {
                    Some(spec.id)
                }
                _ => None,
            })
            .unwrap();
        if active {
            // Room model consumer only. Actual ICE/DTLS/SCTP is separately exercised.
            room.handle_media_event(
                MediaEvent::TransportConnected {
                    participant: joined.sid.clone(),
                    transport,
                },
                now,
            );
            room.take_effects();
            // Model helper represents source fully-established, not merely ACTIVE.
            room.handle_media_event(
                MediaEvent::PrimaryTransportEstablished {
                    participant: joined.sid.clone(),
                    transport,
                },
                now,
            );
            room.take_effects();
            // Model-only writer outcome, after the readiness-triggered replay.
            room.handle_media_event(
                MediaEvent::JoiningReliableReplayed {
                    participant: joined.sid.clone(),
                    transport,
                },
                now,
            );
            room.take_effects();
        }
        (room, joined.sid)
    }
    #[test]
    fn public_send_data_open_kind_reaches_active_owner_effect_and_keeps_raw_wire() {
        for raw in [0, 1, 99, -1, i32::MIN, i32::MAX] {
            let (mut room, sid) = owner(true);
            let req = pb::SendDataRequest {
                room: "open-kind".into(),
                kind: raw,
                data: b"actual-api-body".to_vec(),
                destination_identities: vec!["target".into()],
                ..Default::default()
            };
            let wire = send_data_packet(&req).encode_to_vec();
            let kind = data_kind_from_proto(req.kind);
            room.api_send_data(kind, wire.clone(), &[Identity::from("target")], &[]);
            let sends: Vec<_> = room
                .take_effects()
                .into_iter()
                .filter_map(|e| match e {
                    Effect::Media(MediaCommand::SendData { to, kind, payload }) => {
                        Some((to, kind, payload))
                    }
                    _ => None,
                })
                .collect();
            assert_eq!(sends.len(), 1);
            assert_eq!(sends[0].0, sid);
            assert_eq!(sends[0].2, wire);
            assert_eq!(
                pb::DataPacket::decode(sends[0].2.as_slice()).unwrap().kind,
                raw
            );
            assert_eq!(
                sends[0].1 == DataKind::Reliable,
                raw == 0,
                "raw kind {raw}: only0 may request reliable delivery"
            );
            assert_eq!(data_kind_to_proto(sends[0].1), raw);
        }
    }
    #[test]
    fn public_send_data_nonreliable_joining_owner_is_not_queued_or_signalled() {
        for raw in [1, 99, -1, i32::MIN, i32::MAX] {
            let (mut room, _) = owner(false);
            room.api_send_data(data_kind_from_proto(raw), vec![1, 2, 3], &[], &[]);
            assert!(room.take_effects().is_empty());
        }
        // This is an unchanged positive control, not an enum-only baseline RED.
        // Known reliable JOINING replay is a separately documented missing consumer.
    }
}

#[cfg(test)]
mod open_data_kind_go_tests {
    use super::*;
    #[test]
    #[ignore = "requires actual fixed Go component observation OPEN_DATA_KIND_GO_RESULTS"]
    fn actual_go_open_data_kind_eleven_source_observations() {
        let text = std::fs::read_to_string(
            std::env::var("OPEN_DATA_KIND_GO_RESULTS").expect("actual Go JSONL path"),
        )
        .unwrap();
        let mut seen = std::collections::BTreeSet::new();
        let ordered_cases: Vec<_> = [true, false]
            .into_iter()
            .flat_map(|active| {
                [0_i32, 1, 99, -1, i32::MIN, i32::MAX]
                    .into_iter()
                    .filter(move |raw| active || *raw != 0)
                    .map(move |raw| (active, raw))
            })
            .collect();
        let exact_fields: std::collections::BTreeSet<_> = [
            "id",
            "state",
            "raw_kind",
            "channel",
            "wire_kind",
            "written",
            "queued",
            "error",
        ]
        .into_iter()
        .collect();
        for (index, line) in text.lines().enumerate() {
            let row: serde_json::Value = serde_json::from_str(line).unwrap();
            let object = row.as_object().expect("source row must be an object");
            assert_eq!(
                object
                    .keys()
                    .map(String::as_str)
                    .collect::<std::collections::BTreeSet<_>>(),
                exact_fields,
                "all eight actual source fields, no missing/extra producer values"
            );
            let raw = i32::try_from(row["raw_kind"].as_i64().unwrap()).unwrap();
            assert!([0, 1, 99, -1, i32::MIN, i32::MAX].contains(&raw));
            let active = match row["state"].as_str().unwrap() {
                "active" => true,
                "joining" => false,
                value => panic!("unexpected state {value}"),
            };
            assert_eq!(
                ordered_cases.get(index),
                Some(&(active, raw)),
                "exact original ordered11 state/raw inputs"
            );
            let state = if active { "active" } else { "joining" };
            let suffix = match raw {
                0 => "RELIABLE".to_owned(),
                1 => "LOSSY".to_owned(),
                raw => raw.to_string(),
            };
            assert_eq!(
                row["id"].as_str(),
                Some(format!("{state}-{suffix}").as_str()),
                "actual original EnumStringOf case identity"
            );
            assert!(row["channel"].is_string() && row["error"].is_string());
            assert!(row["written"].is_boolean());
            assert!(row["queued"].as_u64().is_some());
            assert!(
                row["wire_kind"]
                    .as_i64()
                    .and_then(|raw| i32::try_from(raw).ok())
                    .is_some()
            );
            assert!(
                active || raw != 0,
                "known reliable joining queue is explicitly a separate corpus/slice"
            );
            assert!(seen.insert((active, raw)), "duplicate source case");
            assert_eq!(row["queued"].as_u64(), Some(0));
            let kind = crate::convert::data_kind_from_proto(raw);
            // Actual old public owner consumer per observed source row. This is
            // state-machine evidence, not a fabricated native channel success.
            let (mut owner, target) = super::open_data_kind_owner_tests::owner(active);
            let request = pb::SendDataRequest {
                room: "open-kind".into(),
                kind: raw,
                data: b"actual-api-body".to_vec(),
                destination_identities: vec!["target".into()],
                ..Default::default()
            };
            let wire = send_data_packet(&request).encode_to_vec();
            owner.api_send_data(kind, wire.clone(), &[Identity::from("target")], &[]);
            let effects = owner.take_effects();
            let sends: Vec<_> = effects
                .iter()
                .filter_map(|e| match e {
                    rtc_core::room::Effect::Media(
                        rtc_core::media_api::MediaCommand::SendData { to, kind, payload },
                    ) => Some((to, kind, payload)),
                    _ => None,
                })
                .collect();
            assert_eq!(
                sends.len(),
                usize::from(row["written"].as_bool().unwrap()),
                "actual source written success equals actual owner emission"
            );
            if let Some((to, actual_kind, payload)) = sends.first() {
                assert_eq!(**to, target);
                assert_eq!(payload.as_slice(), wire.as_slice());
                assert_eq!(
                    pb::DataPacket::decode(payload.as_slice()).unwrap().kind,
                    row["wire_kind"].as_i64().unwrap() as i32
                );
                let actual_channel = if **actual_kind == rtc_core::media_api::DataKind::Reliable {
                    "_reliable"
                } else {
                    "_lossy"
                };
                assert_eq!(row["channel"].as_str(), Some(actual_channel));
                assert_eq!(crate::convert::data_kind_to_proto(**actual_kind), raw);
            } else {
                assert!(
                    effects.is_empty(),
                    "source unavailable joining target produces no owner effects"
                );
                assert_eq!(
                    row["wire_kind"].as_i64(),
                    Some(0),
                    "original source records zero sentinel only when no actual writer body exists"
                );
                assert_eq!(
                    row["error"].as_str(),
                    Some("data channel is not available"),
                    "original rtc/errors.go sentinel text, not arbitrary error success"
                );
            }
            if active {
                let selected = if kind == rtc_core::media_api::DataKind::Reliable {
                    "_reliable"
                } else {
                    "_lossy"
                };
                assert_eq!(row["channel"].as_str(), Some(selected));
                assert_eq!(row["written"].as_bool(), Some(true));
                assert_eq!(
                    crate::convert::data_kind_to_proto(kind) as i64,
                    row["wire_kind"].as_i64().unwrap()
                );
                assert_eq!(row["error"].as_str(), Some(""));
            } else {
                assert_eq!(row["channel"].as_str(), Some("none"));
                assert_eq!(row["written"].as_bool(), Some(false));
                assert!(!row["error"].as_str().unwrap().is_empty());
            }
        }
        assert_eq!(seen.len(), 11);
        for raw in [0, 1, 99, -1, i32::MIN, i32::MAX] {
            assert!(seen.contains(&(true, raw)));
            if raw != 0 {
                assert!(seen.contains(&(false, raw)));
            }
        }
    }
}

/// Integration tests for all RoomService API methods via the Twirp HTTP layer.
/// Covers auth checks, error paths, and success paths for OPEN_AUDIT items.
#[cfg(test)]
mod room_service_api_method_tests {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    async fn invoke(
        state: &AppState,
        token: &str,
        method: &str,
        body: serde_json::Value,
    ) -> (StatusCode, serde_json::Value) {
        let resp = crate::http::router(state.clone())
            .oneshot(
                Request::post(format!("/twirp/livekit.RoomService/{method}"))
                    .header("authorization", format!("Bearer {token}"))
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
        (status, json)
    }

    fn make_jwt(key: &(String, String), grants: serde_json::Value) -> String {
        crate::auth::jwt::tests::make_token(
            r#"{"alg":"HS256"}"#,
            &serde_json::json!({
                "iss": key.0,
                "sub": "admin",
                "exp": 99999999999_u64,
                "video": grants
            })
            .to_string(),
            &key.1,
        )
    }

    fn admin_grants(room: &str) -> serde_json::Value {
        serde_json::json!({
            "roomAdmin": true,
            "room": room,
            "roomCreate": true,
            "roomList": true
        })
    }

    #[tokio::test]
    async fn list_rooms_unauthenticated_returns_401() {
        let (state, net) = crate::test_support::state().await;
        let resp = crate::http::router(state.clone())
            .oneshot(
                Request::post("/twirp/livekit.RoomService/ListRooms")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn list_rooms_without_room_list_grant_returns_permission_denied() {
        let (state, net) = crate::test_support::state().await;
        let key = state.config.keys[0].clone();
        let token = make_jwt(&key, serde_json::json!({"roomAdmin": true, "room": "x"}));
        let (status, body) = invoke(&state, &token, "ListRooms", serde_json::json!({})).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert!(body["msg"].as_str().unwrap().contains("permission"));
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn list_rooms_with_room_list_grant_returns_empty_initially() {
        let (state, net) = crate::test_support::state().await;
        let key = state.config.keys[0].clone();
        let token = make_jwt(&key, serde_json::json!({"roomList": true}));
        let (status, body) = invoke(&state, &token, "ListRooms", serde_json::json!({})).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["rooms"].as_array().unwrap().is_empty());
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn list_rooms_after_create_shows_room() {
        let (state, net) = crate::test_support::state().await;
        let key = state.config.keys[0].clone();
        let admin_token = make_jwt(&key, admin_grants("api-list-test"));
        let (status, created) = invoke(
            &state,
            &admin_token,
            "CreateRoom",
            serde_json::json!({"name": "api-list-test"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(created["name"], "api-list-test");
        let list_token = make_jwt(&key, serde_json::json!({"roomList": true}));
        let (status, body) = invoke(&state, &list_token, "ListRooms", serde_json::json!({})).await;
        assert_eq!(status, StatusCode::OK);
        let rooms = body["rooms"].as_array().unwrap();
        assert_eq!(rooms.len(), 1);
        assert_eq!(rooms[0]["name"], "api-list-test");
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn delete_room_unauthenticated_returns_401() {
        let (state, net) = crate::test_support::state().await;
        let resp = crate::http::router(state.clone())
            .oneshot(
                Request::post("/twirp/livekit.RoomService/DeleteRoom")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn delete_room_nonexistent_returns_not_found() {
        let (state, net) = crate::test_support::state().await;
        let key = state.config.keys[0].clone();
        let token = make_jwt(&key, admin_grants("ghost-room"));
        let (status, body) = invoke(
            &state,
            &token,
            "DeleteRoom",
            serde_json::json!({"room": "ghost-room"}),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(body["msg"].as_str().unwrap().contains("does not exist"));
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn delete_room_after_create_succeeds() {
        let (state, net) = crate::test_support::state().await;
        let key = state.config.keys[0].clone();
        let token = make_jwt(&key, admin_grants("api-delete-test"));
        let (status, _) = invoke(
            &state,
            &token,
            "CreateRoom",
            serde_json::json!({"name": "api-delete-test"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, _body) = invoke(
            &state,
            &token,
            "DeleteRoom",
            serde_json::json!({"room": "api-delete-test"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let list_token = make_jwt(&key, serde_json::json!({"roomList": true}));
        let (_, list_body) = invoke(&state, &list_token, "ListRooms", serde_json::json!({})).await;
        assert!(list_body["rooms"].as_array().unwrap().is_empty());
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn list_participants_unauthenticated_returns_401() {
        let (state, net) = crate::test_support::state().await;
        let resp = crate::http::router(state.clone())
            .oneshot(
                Request::post("/twirp/livekit.RoomService/ListParticipants")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"room":"x"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn list_participants_wrong_room_admin_returns_permission_denied() {
        let (state, net) = crate::test_support::state().await;
        let key = state.config.keys[0].clone();
        let token = make_jwt(&key, admin_grants("other-room"));
        let (status, _) = invoke(
            &state,
            &token,
            "ListParticipants",
            serde_json::json!({"room": "mismatched-room"}),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn list_participants_empty_room_returns_empty_list() {
        let (state, net) = crate::test_support::state().await;
        let key = state.config.keys[0].clone();
        let token = make_jwt(&key, admin_grants("api-lp-test"));
        let (status, _) = invoke(
            &state,
            &token,
            "CreateRoom",
            serde_json::json!({"name": "api-lp-test"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, body) = invoke(
            &state,
            &token,
            "ListParticipants",
            serde_json::json!({"room": "api-lp-test"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["participants"].as_array().unwrap().is_empty());
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn get_participant_nonexistent_returns_not_found() {
        let (state, net) = crate::test_support::state().await;
        let key = state.config.keys[0].clone();
        let token = make_jwt(&key, admin_grants("api-gp-test"));
        let (status, _) = invoke(
            &state,
            &token,
            "CreateRoom",
            serde_json::json!({"name": "api-gp-test"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, body) = invoke(
            &state,
            &token,
            "GetParticipant",
            serde_json::json!({"room": "api-gp-test", "identity": "ghost"}),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(body["msg"].as_str().unwrap().contains("does not exist"));
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn remove_participant_nonexistent_returns_not_found() {
        let (state, net) = crate::test_support::state().await;
        let key = state.config.keys[0].clone();
        let token = make_jwt(&key, admin_grants("api-rp-test"));
        invoke(
            &state,
            &token,
            "CreateRoom",
            serde_json::json!({"name": "api-rp-test"}),
        )
        .await;
        let (status, body) = invoke(
            &state,
            &token,
            "RemoveParticipant",
            serde_json::json!({"room": "api-rp-test", "identity": "nobody"}),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(body["msg"].as_str().unwrap().contains("does not exist"));
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn mute_published_track_unauthenticated_returns_401() {
        let (state, net) = crate::test_support::state().await;
        let resp = crate::http::router(state.clone())
            .oneshot(
                Request::post("/twirp/livekit.RoomService/MutePublishedTrack")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn update_subscriptions_unauthenticated_returns_401() {
        let (state, net) = crate::test_support::state().await;
        let resp = crate::http::router(state.clone())
            .oneshot(
                Request::post("/twirp/livekit.RoomService/UpdateSubscriptions")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn send_data_unauthenticated_returns_401() {
        let (state, net) = crate::test_support::state().await;
        let resp = crate::http::router(state.clone())
            .oneshot(
                Request::post("/twirp/livekit.RoomService/SendData")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn send_data_invalid_nonce_length_returns_bad_request() {
        let (state, net) = crate::test_support::state().await;
        let key = state.config.keys[0].clone();
        let token = make_jwt(&key, admin_grants("api-sd-test"));
        invoke(
            &state,
            &token,
            "CreateRoom",
            serde_json::json!({"name": "api-sd-test"}),
        )
        .await;
        let (status, body) = invoke(
            &state,
            &token,
            "SendData",
            serde_json::json!({
                "room": "api-sd-test",
                "data": "aGVsbG8=",
                "nonce": "dG9vc2hvcnQ="
            }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body["msg"].as_str().unwrap().contains("nonce"));
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn update_room_metadata_nonexistent_returns_not_found() {
        let (state, net) = crate::test_support::state().await;
        let key = state.config.keys[0].clone();
        let token = make_jwt(&key, admin_grants("api-urm-test"));
        let (status, body) = invoke(
            &state,
            &token,
            "UpdateRoomMetadata",
            serde_json::json!({"room": "api-urm-test", "metadata": "test-meta"}),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(body["msg"].as_str().unwrap().contains("does not exist"));
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn update_room_metadata_success_returns_updated_room() {
        let (state, net) = crate::test_support::state().await;
        let key = state.config.keys[0].clone();
        let token = make_jwt(&key, admin_grants("api-urm-ok"));
        invoke(
            &state,
            &token,
            "CreateRoom",
            serde_json::json!({"name": "api-urm-ok"}),
        )
        .await;
        let (status, body) = invoke(
            &state,
            &token,
            "UpdateRoomMetadata",
            serde_json::json!({"room": "api-urm-ok", "metadata": "updated-value"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["metadata"], "updated-value");
        assert_eq!(body["name"], "api-urm-ok");
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn forward_participant_same_room_returns_invalid_argument() {
        let (state, net) = crate::test_support::state().await;
        let mut token = VerifiedToken::default();
        token.grants.video.room_admin = true;
        token.grants.video.room = "same-room".into();
        token.grants.video.destination_room = "same-room".into();
        let result = forward_or_move::<pb::ForwardParticipantResponse>(
            &state,
            Some(&token),
            "same-room".into(),
            "alice".into(),
            "same-room".into(),
            false,
        )
        .await;
        assert_eq!(result.unwrap_err().code, Code::InvalidArgument);
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn forward_participant_nonexistent_returns_unavailable() {
        let (state, net) = crate::test_support::state().await;
        let key = state.config.keys[0].clone();
        let mut token = VerifiedToken::default();
        token.grants.video.room_admin = true;
        token.grants.video.room = "src-room".into();
        token.grants.video.destination_room = "dst-room".into();
        let create_token = make_jwt(&key, admin_grants("src-room"));
        invoke(
            &state,
            &create_token,
            "CreateRoom",
            serde_json::json!({"name": "src-room"}),
        )
        .await;
        let result = forward_or_move::<pb::ForwardParticipantResponse>(
            &state,
            Some(&token),
            "src-room".into(),
            "ghost".into(),
            "dst-room".into(),
            false,
        )
        .await;
        assert_eq!(result.unwrap_err().code, Code::Unavailable);
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn move_participant_same_room_returns_invalid_argument() {
        let (state, net) = crate::test_support::state().await;
        let mut token = VerifiedToken::default();
        token.grants.video.room_admin = true;
        token.grants.video.room = "move-room".into();
        token.grants.video.destination_room = "move-room".into();
        let result = forward_or_move::<pb::MoveParticipantResponse>(
            &state,
            Some(&token),
            "move-room".into(),
            "bob".into(),
            "move-room".into(),
            true,
        )
        .await;
        assert_eq!(result.unwrap_err().code, Code::InvalidArgument);
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn perform_rpc_empty_destination_returns_bad_request() {
        let (state, net) = crate::test_support::state().await;
        let key = state.config.keys[0].clone();
        let token = make_jwt(&key, admin_grants("api-rpc-test"));
        let (status, body) = invoke(
            &state,
            &token,
            "PerformRpc",
            serde_json::json!({
                "room": "api-rpc-test",
                "destination_identity": "",
                "method": "test",
                "payload": ""
            }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(
            body["msg"]
                .as_str()
                .unwrap()
                .contains("destination identity")
        );
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn perform_rpc_unauthenticated_returns_401() {
        let (state, net) = crate::test_support::state().await;
        let resp = crate::http::router(state.clone())
            .oneshot(
                Request::post("/twirp/livekit.RoomService/PerformRpc")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn update_room_metadata_unauthenticated_returns_401() {
        let (state, net) = crate::test_support::state().await;
        let resp = crate::http::router(state.clone())
            .oneshot(
                Request::post("/twirp/livekit.RoomService/UpdateRoomMetadata")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn update_participant_unauthenticated_returns_401() {
        let (state, net) = crate::test_support::state().await;
        let resp = crate::http::router(state.clone())
            .oneshot(
                Request::post("/twirp/livekit.RoomService/UpdateParticipant")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn forward_participant_unauthenticated_returns_401() {
        let (state, net) = crate::test_support::state().await;
        let resp = crate::http::router(state.clone())
            .oneshot(
                Request::post("/twirp/livekit.RoomService/ForwardParticipant")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn move_participant_unauthenticated_returns_401() {
        let (state, net) = crate::test_support::state().await;
        let resp = crate::http::router(state.clone())
            .oneshot(
                Request::post("/twirp/livekit.RoomService/MoveParticipant")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        state.manager().close_all().await;
        net.shutdown();
    }
}

#[cfg(test)]
#[path = "room_service_future_size_tests.rs"]
mod future_size_tests;
