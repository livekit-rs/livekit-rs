//! Pinned routing/interfaces.go ParticipantInit <-> StartSession projection.
use super::*;
use rtc_core::{
    ids::{Identity, ParticipantSid, ProjectId, RoomKey},
    permission::Grants,
    session::JoinParams,
};
use serde_json::json;

pub(crate) fn start_session(
    params: &JoinParams,
    tracks: Vec<pb::AddTrackRequest>,
) -> Result<pb::StartSession, RpcError> {
    let grants = &params.grants;
    let mut claims = json!({"identity":params.identity.0,"name":params.name,"metadata":params.metadata,"attributes":params.attributes,
        "kind":crate::convert::participant_kind_to_claim(grants.kind),"video":crate::auth::minter::video_grant_json(&grants.video,grants.video.can_manage_agent_session),
        "roomPreset":grants.room_preset,"sha256":grants.sha256});
    if let Some(config) = &grants.room_config {
        claims["roomConfig"] =
            serde_json::to_value(crate::auth::claims::room_config_to_proto(config))
                .map_err(failure)?;
    }
    Ok(pb::StartSession {
        room_name: params.room.name.0.clone(),
        identity: params.identity.0.clone(),
        connection_id: rtc_core::ids::new_sid("CO_"),
        reconnect: params.reconnect,
        auto_subscribe: params.auto_subscribe,
        auto_subscribe_data_track: Some(params.auto_subscribe_data_track),
        client: Some(crate::convert::meta_to_client_info(&params.client)),
        name: params.name.clone(),
        grants_json: claims.to_string(),
        adaptive_stream: params.adaptive_stream,
        participant_id: params
            .participant_sid
            .as_ref()
            .map(|s| s.0.clone())
            .unwrap_or_default(),
        reconnect_reason: params.reconnect_reason,
        subscriber_allow_pause: params.subscriber_allow_pause,
        disable_ice_lite: params.disable_ice_lite,
        create_room: Some(crate::room_control::signal_create_request(params)),
        add_track_requests: tracks,
        publisher_offer: params
            .publisher_offer
            .as_ref()
            .map(|(sdp, id)| pb::SessionDescription {
                r#type: "offer".into(),
                sdp: sdp.clone(),
                id: *id,
                ..Default::default()
            }),
        use_single_peer_connection: params.single_pc,
        token_expires_at: grants.expires_at,
        ..Default::default()
    })
}
pub(crate) fn join_params(api_key: &str, start: &pb::StartSession) -> Result<JoinParams, RpcError> {
    let value: serde_json::Value = serde_json::from_str(&start.grants_json).map_err(failure)?;
    let object = value
        .as_object()
        .ok_or_else(|| RpcError::new("invalid_argument", "invalid grants JSON"))?;
    let decoded = crate::auth::claims::decode_grants(object).map_err(failure)?;
    let client = crate::convert::client_info_to_meta(&start.client.clone().unwrap_or_default());
    let mut capabilities = crate::profile::capabilities(&client);
    // Fixed ClientInfo.SupportsICETCP distinguishes absent StartSession.client
    // from an explicit Unknown SDK object. Interactive query metadata is present.
    capabilities.ice_tcp &= start.client.is_some();
    let grants = Grants {
        api_key: api_key.to_owned(),
        identity: start.identity.clone(),
        name: decoded.name.clone(),
        metadata: decoded.metadata.clone(),
        attributes: decoded.attributes.clone(),
        video: decoded.video,
        kind: decoded.kind,
        sha256: decoded.sha256,
        room_preset: decoded.room_preset,
        room_config: decoded.room_config,
        expires_at: start.token_expires_at,
    };
    let room = start
        .create_room
        .as_ref()
        .map(|r| r.name.as_str())
        .filter(|r| !r.is_empty())
        .unwrap_or(&start.room_name);
    Ok(JoinParams {
        room: RoomKey::new(ProjectId::local(), room),
        identity: Identity(start.identity.clone()),
        name: start.name.clone(),
        metadata: grants.metadata.clone(),
        attributes: grants.attributes.clone(),
        capabilities,
        client,
        auto_subscribe: start.auto_subscribe,
        auto_subscribe_data_track: start.auto_subscribe_data_track.unwrap_or(false),
        adaptive_stream: start.adaptive_stream,
        single_pc: start.use_single_peer_connection,
        disable_ice_lite: start.disable_ice_lite,
        subscriber_allow_pause: start.subscriber_allow_pause,
        reconnect: start.reconnect,
        reconnect_reason: start.reconnect_reason,
        participant_sid: (!start.participant_id.is_empty())
            .then(|| ParticipantSid(start.participant_id.clone())),
        add_tracks: start
            .add_track_requests
            .iter()
            .enumerate()
            .map(|(i, req)| crate::signal::params::add_track_from_proto(req, i as u32 + 1))
            .collect(),
        publisher_offer: start
            .publisher_offer
            .as_ref()
            .map(|s| (s.sdp.clone(), s.id)),
        grants,
    })
}
/// Root/repeated children retain their exact source cohorts, including Blob
/// key/schema unknown fields. No protobuf->typed->protobuf relay projection.
pub(super) fn request_batch(seq: u64, close: bool, raw: &[Vec<u8>]) -> Result<Vec<u8>, RpcError> {
    let typed = rpc::RelaySignalRequest {
        seq,
        close,
        ..Default::default()
    };
    let mut value = PreservedMessage::from_message(&typed);
    value.replace_repeated(
        "requests",
        raw.iter()
            .map(|raw| PreservedMessage::decode("livekit.SignalRequest", raw))
            .collect::<Result<Vec<_>, _>>()
            .map_err(failure)?,
    );
    Ok(value.encode_to_vec())
}
pub(super) fn response_batch(seq: u64, close: bool, raw: &[Vec<u8>]) -> Result<Vec<u8>, RpcError> {
    let typed = rpc::RelaySignalResponse {
        seq,
        close,
        ..Default::default()
    };
    let mut value = PreservedMessage::from_message(&typed);
    value.replace_repeated(
        "responses",
        raw.iter()
            .map(|raw| PreservedMessage::decode("livekit.SignalResponse", raw))
            .collect::<Result<Vec<_>, _>>()
            .map_err(failure)?,
    );
    Ok(value.encode_to_vec())
}
pub(super) fn read_batch(
    name: &str,
    field: &str,
    raw: &[u8],
    seq: u64,
    expected: &mut u64,
) -> Result<Vec<Vec<u8>>, RpcError> {
    let value = PreservedMessage::decode(name, raw).map_err(failure)?;
    let mut messages = value
        .repeated(field)
        .into_iter()
        .map(|value| value.encode_to_vec())
        .collect::<Vec<_>>();
    if *expected < seq {
        return Err(RpcError::new("data_loss", "signal message dropped"));
    }
    let skipped = expected.saturating_sub(seq).min(messages.len() as u64) as usize;
    messages.drain(..skipped);
    *expected = expected.saturating_add(messages.len() as u64);
    Ok(messages)
}
