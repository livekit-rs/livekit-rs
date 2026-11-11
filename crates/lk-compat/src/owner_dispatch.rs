//! Fixed AgentDispatchInternal topics bound to an instantiated room generation.
//!
//! Public authentication/limits/auto-create stay in the public adapter. This is
//! trusted PSRPC control, not an HTTP loopback or a second dispatch store.
use crate::service_bus::rpc_transport::RpcBus;
use crate::service_bus::{
    redis_transport::RedisTransport,
    rpc::{MethodSpec, RpcError, RpcRetirement, RpcServer, ServerLimits},
};
use lk_protocol::livekit as pb;
use rtc_core::ids::{RoomKey, RoomSid};
use rtc_runtime::api::{AgentDispatchOptions, AgentDispatchRecord};
use rtc_runtime::{RoomManager, RoomOwnerHandle};
use std::sync::Arc;

pub fn method(room: &str, method: &str) -> MethodSpec {
    MethodSpec {
        service: "AgentDispatchInternal".into(),
        method: method.into(),
        topics: vec![room.into()],
        require_claim: true,
        queue: true,
    }
}

/// Owns exactly one room generation's three actual subscription groups.
/// This must be stored alongside that generation's RoomTopics, never by name
/// alone; the caller fences retirement against the registered RoomSID.
pub struct AgentDispatchTopics {
    sid: RoomSid,
    servers: Vec<RpcServer>,
    accepted: Arc<crate::room_control::accepted::Accepted>,
    owns_accepted: bool,
}
impl AgentDispatchTopics {
    /// Returns only after all six request/claim subscriptions acknowledge Redis
    /// readiness. Partial failures retire every earlier acceptance group.
    pub async fn register(
        manager: &RoomManager,
        key: &RoomKey,
        sid: &RoomSid,
        bus: Arc<RedisTransport>,
        limits: ServerLimits,
    ) -> Result<Self, RpcError> {
        let owner = manager.capture_room_owner(key, sid).map_err(api_error)?;
        Self::register_captured(owner, bus.into(), limits).await
    }
    pub(crate) async fn register_captured(
        owner: RoomOwnerHandle,
        bus: RpcBus,
        limits: ServerLimits,
    ) -> Result<Self, RpcError> {
        Self::register_inner(
            owner,
            bus,
            limits,
            Arc::new(crate::room_control::accepted::Accepted::default()),
            true,
        )
        .await
    }
    pub(crate) async fn register_with_accepted(
        owner: RoomOwnerHandle,
        bus: RpcBus,
        limits: ServerLimits,
        accepted: Arc<crate::room_control::accepted::Accepted>,
    ) -> Result<Self, RpcError> {
        Self::register_inner(owner, bus, limits, accepted, false).await
    }
    async fn register_inner(
        owner: RoomOwnerHandle,
        bus: RpcBus,
        limits: ServerLimits,
        accepted: Arc<crate::room_control::accepted::Accepted>,
        owns_accepted: bool,
    ) -> Result<Self, RpcError> {
        let key = owner.key().clone();
        let sid = owner.sid().clone();
        let mut servers = Vec::new();
        let result: Result<(), RpcError> = async {
            let captured = owner.clone();
            let operations = accepted.clone();
            servers.push(
                RpcServer::register_with_raw::<pb::AgentDispatch, pb::AgentDispatch, _, _>(
                    bus.clone(),
                    method(&key.name.0, "CreateDispatch"),
                    limits,
                    move |_, request| {
                        let owner = captured.clone();
                        {
                            let operations = operations.clone();
                            async move {
                                let sid = owner.sid().clone();
                                operations
                                    .run(&sid, async move { create(&owner, request.message).await })
                                    .await
                            }
                        }
                    },
                    None,
                )
                .await?,
            );
            let captured = owner.clone();
            let operations = accepted.clone();
            servers.push(
                RpcServer::register_with_raw::<
                    pb::DeleteAgentDispatchRequest,
                    pb::AgentDispatch,
                    _,
                    _,
                >(
                    bus.clone(),
                    method(&key.name.0, "DeleteDispatch"),
                    limits,
                    move |_, request| {
                        let owner = captured.clone();
                        {
                            let operations = operations.clone();
                            async move {
                                let sid = owner.sid().clone();
                                operations
                                    .run(&sid, async move { delete(&owner, request.message).await })
                                    .await
                            }
                        }
                    },
                    None,
                )
                .await?,
            );
            let captured = owner.clone();
            let operations = accepted.clone();
            servers.push(
                RpcServer::register_with_raw::<
                    pb::ListAgentDispatchRequest,
                    pb::ListAgentDispatchResponse,
                    _,
                    _,
                >(
                    bus,
                    method(&key.name.0, "ListDispatch"),
                    limits,
                    move |_, request| {
                        let owner = captured.clone();
                        {
                            let operations = operations.clone();
                            async move {
                                let sid = owner.sid().clone();
                                operations
                                    .run(&sid, async move { list(&owner, request.message).await })
                                    .await
                            }
                        }
                    },
                    None,
                )
                .await?,
            );
            Ok(())
        }
        .await;
        if let Err(error) = result {
            // Stop acceptance and retain its acknowledged retirement before any
            // independently accepted body can outlive the registration attempt.
            for server in servers {
                let _ = accepted.retire_server(&sid, server).await;
            }
            if owns_accepted {
                accepted.finish().await;
            }
            return Err(error);
        }
        Ok(Self {
            sid: sid.clone(),
            servers,
            accepted,
            owns_accepted,
        })
    }
    pub fn sid(&self) -> &RoomSid {
        &self.sid
    }
    /// Stop new acceptance. Shared production owners retain receipts and join
    /// application tasks in their external shutdown tracker; the convenience
    /// constructor owns its tracker and completes that join here.
    pub async fn retire(self) -> Result<Vec<RpcRetirement>, RpcError> {
        let mut failure = None;
        for server in self.servers {
            if let Err(error) = self.accepted.retire_server(&self.sid, server).await {
                if failure.is_none() {
                    failure = Some(error);
                }
            }
        }
        // Product owners share their externally joined generation tracker. The
        // convenience constructor owns its tracker and must join it here.
        if self.owns_accepted {
            self.accepted.finish().await;
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(Vec::new()),
        }
    }
}

fn check_room(owner: &RoomOwnerHandle, room: &str) -> Result<(), RpcError> {
    if owner.key().name.0 != room {
        // A trusted caller cannot use one generation capability to mutate some
        // other room. This internal invariant is not a public validation policy.
        return Err(RpcError::new(
            "failed_precondition",
            "owner request room does not match registered topic",
        ));
    }
    Ok(())
}
async fn create(
    owner: &RoomOwnerHandle,
    request: pb::AgentDispatch,
) -> Result<pb::AgentDispatch, RpcError> {
    check_room(owner, &request.room)?;
    owner
        .create_agent_dispatch(AgentDispatchOptions {
            id: request.id,
            agent_name: request.agent_name,
            metadata: request.metadata,
            restart_policy: request.restart_policy,
            deployment: request.deployment,
            attributes: request.attributes.into_iter().collect(),
        })
        .await
        .map(to_proto)
        .map_err(api_error)
}
async fn delete(
    owner: &RoomOwnerHandle,
    request: pb::DeleteAgentDispatchRequest,
) -> Result<pb::AgentDispatch, RpcError> {
    check_room(owner, &request.room)?;
    owner
        .delete_agent_dispatch(&request.dispatch_id)
        .await
        .map(to_proto)
        .map_err(api_error)
}
async fn list(
    owner: &RoomOwnerHandle,
    request: pb::ListAgentDispatchRequest,
) -> Result<pb::ListAgentDispatchResponse, RpcError> {
    check_room(owner, &request.room)?;
    owner
        .list_agent_dispatches(
            (!request.dispatch_id.is_empty()).then_some(request.dispatch_id.as_str()),
        )
        .await
        .map(|dispatches| pb::ListAgentDispatchResponse {
            agent_dispatches: dispatches.into_iter().map(to_proto).collect(),
        })
        .map_err(api_error)
}
fn api_error(error: rtc_runtime::ApiError) -> RpcError {
    // Retain the current actual public/runtime mapping, including MetadataLimit;
    // no string matching can turn a remote application error into NoResponse.
    let error: crate::twirp::error::TwirpError = error.into();
    let mut result = RpcError::new(error.code.as_str(), error.msg);
    result.application_metadata = error.meta;
    result
}
fn to_proto(dispatch: AgentDispatchRecord) -> pb::AgentDispatch {
    pb::AgentDispatch {
        id: dispatch.id,
        agent_name: dispatch.agent_name,
        room: dispatch.room,
        metadata: dispatch.metadata,
        restart_policy: dispatch.restart_policy,
        deployment: dispatch.deployment,
        attributes: dispatch.attributes.into_iter().collect(),
        state: Some(pb::AgentDispatchState {
            jobs: dispatch
                .jobs
                .iter()
                .map(crate::agent::bridge::to_proto)
                .collect(),
            created_at: dispatch.created_at_ns,
            deleted_at: 0,
        }),
    }
}

#[cfg(test)]
mod tests;
