//! Agent dispatch request contract from the pinned upstream service.
//!
//! Dispatch persistence belongs to the room owner. A dispatch with no assigned
//! worker has no jobs; accepting it never fabricates an agent job or participant.

use lk_protocol::livekit as pb;

use super::super::codec::{Codec, decode, encode};
use super::super::error::{Code, TwirpError};
use crate::auth::VerifiedToken;
use crate::auth::permissions::ensure_admin;
use crate::state::AppState;
use axum::response::Response;
use lk_protocol::twirp_json::livekit as tj;

/// Typed requests after the generated transport decoder and service preflight.
#[derive(Debug)]
pub enum DispatchRequest {
    Create(pb::CreateAgentDispatchRequest),
    Delete(pb::DeleteAgentDispatchRequest),
    List(pb::ListAgentDispatchRequest),
}

/// Decode before authentication, matching generated Twirp handlers. Creation
/// then checks deployment, metadata and attributes in upstream service order.
pub fn prepare(
    auth: Option<&VerifiedToken>,
    method: &str,
    codec: Codec,
    body: &[u8],
    max_metadata: Option<usize>,
    max_attributes: Option<usize>,
) -> Result<DispatchRequest, TwirpError> {
    match method {
        "CreateDispatch" => {
            let req: pb::CreateAgentDispatchRequest = decode(codec, body)?;
            ensure_admin(auth, &req.room)?;
            validate_deployment(&req.deployment)?;
            if exceeds(max_metadata, req.metadata.len()) {
                return Err(TwirpError::new(
                    Code::InvalidArgument,
                    "metadata size exceeds limits",
                ));
            }
            let attributes_size = req.attributes.iter().fold(0usize, |size, (key, value)| {
                size.saturating_add(key.len()).saturating_add(value.len())
            });
            if exceeds(max_attributes, attributes_size) {
                return Err(TwirpError::new(
                    Code::InvalidArgument,
                    "attribute size exceeds limits",
                ));
            }
            Ok(DispatchRequest::Create(req))
        }
        "DeleteDispatch" => {
            let req: pb::DeleteAgentDispatchRequest = decode(codec, body)?;
            ensure_admin(auth, &req.room)?;
            Ok(DispatchRequest::Delete(req))
        }
        "ListDispatch" => {
            let req: pb::ListAgentDispatchRequest = decode(codec, body)?;
            ensure_admin(auth, &req.room)?;
            Ok(DispatchRequest::List(req))
        }
        _ => Err(TwirpError::new(
            Code::Unimplemented,
            "unknown agent dispatch method",
        )),
    }
}

fn exceeds(limit: Option<usize>, size: usize) -> bool {
    limit.is_some_and(|limit| limit != 0 && size > limit)
}

/// Upstream validates UTF-8 bytes, not Unicode character categories. Non-ASCII
/// whitespace is therefore accepted, while ASCII controls, DEL and `_` fail.
pub fn validate_deployment(deployment: &str) -> Result<(), TwirpError> {
    if deployment.len() > 64 {
        return Err(TwirpError::new(
            Code::InvalidArgument,
            "deployment exceeds 64 bytes",
        ));
    }
    for (position, byte) in deployment.bytes().enumerate() {
        if byte == b'_' {
            return Err(TwirpError::new(
                Code::InvalidArgument,
                "deployment contains reserved character '_'",
            ));
        }
        if byte <= b' ' || byte == 0x7f {
            return Err(TwirpError::new(
                Code::InvalidArgument,
                format!("deployment contains whitespace or control byte at position {position}"),
            ));
        }
    }
    Ok(())
}

/// Execute against the room task's shared dispatch registry. No agent worker or
/// successful job is inferred from accepting a dispatch record.
pub async fn call(
    state: &AppState,
    auth: Option<&VerifiedToken>,
    method: &str,
    codec: Codec,
    body: &[u8],
) -> Result<Response, TwirpError> {
    let prepared = prepare(
        auth,
        method,
        codec,
        body,
        state.config.limits.max_metadata_size,
        state.config.limits.max_attributes_size,
    )?;
    match prepared {
        DispatchRequest::Create(req) => {
            if let Some(router) = &state.room_control {
                if state.config.room.auto_create {
                    router
                        .create_room(pb::CreateRoomRequest {
                            name: req.room.clone(),
                            ..Default::default()
                        })
                        .await
                        .map_err(crate::twirp::worker_error::failure)?;
                }
                let dispatch = router
                    .create_dispatch(pb::AgentDispatch {
                        id: rtc_core::ids::new_sid("AD_"),
                        agent_name: req.agent_name,
                        room: req.room,
                        metadata: req.metadata,
                        restart_policy: req.restart_policy,
                        deployment: req.deployment,
                        attributes: req.attributes,
                        ..Default::default()
                    })
                    .await
                    .map_err(crate::twirp::worker_error::failure)?;
                return Ok(encode::<pb::AgentDispatch, tj::AgentDispatch>(
                    codec, &dispatch,
                ));
            }
            if state.config.room.auto_create {
                // Fixed AgentDispatchService calls router.CreateRoom, including
                // its method-scoped retry/accepted-operation boundary. Reuse
                // that local boundary without RoomService's roomCreate grant.
                crate::twirp::room_service::create_local_owner_request(
                    state,
                    pb::CreateRoomRequest {
                        name: req.room.clone(),
                        ..Default::default()
                    },
                )
                .await?;
            }
            let dispatch = state
                .local_dispatch
                .create(pb::AgentDispatch {
                    id: rtc_core::ids::new_sid("AD_"),
                    agent_name: req.agent_name,
                    room: req.room,
                    metadata: req.metadata,
                    restart_policy: req.restart_policy,
                    deployment: req.deployment,
                    attributes: req.attributes,
                    ..Default::default()
                })
                .await
                .map_err(crate::twirp::worker_error::failure)?;
            Ok(encode::<pb::AgentDispatch, tj::AgentDispatch>(
                codec, &dispatch,
            ))
        }

        DispatchRequest::Delete(req) => {
            if let Some(router) = &state.room_control {
                let dispatch = router
                    .delete_dispatch(req)
                    .await
                    .map_err(crate::twirp::worker_error::failure)?;
                return Ok(encode::<pb::AgentDispatch, tj::AgentDispatch>(
                    codec, &dispatch,
                ));
            }
            let dispatch = state
                .local_dispatch
                .delete(req)
                .await
                .map_err(crate::twirp::worker_error::failure)?;
            Ok(encode::<pb::AgentDispatch, tj::AgentDispatch>(
                codec, &dispatch,
            ))
        }
        DispatchRequest::List(req) => {
            if let Some(router) = &state.room_control {
                let dispatches = router
                    .list_dispatches(req)
                    .await
                    .map_err(crate::twirp::worker_error::failure)?;
                return Ok(encode::<
                    pb::ListAgentDispatchResponse,
                    tj::ListAgentDispatchResponse,
                >(codec, &dispatches));
            }
            let result = state
                .local_dispatch
                .list(req)
                .await
                .map_err(crate::twirp::worker_error::failure)?;
            Ok(encode::<
                pb::ListAgentDispatchResponse,
                tj::ListAgentDispatchResponse,
            >(codec, &result))
        }
    }
}

#[cfg(test)]
mod tests;
