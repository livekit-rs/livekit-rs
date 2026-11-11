//! Real standalone Participant PSRPC memberships, sharing LocalDispatch's bus.
//! Accepted data RPCs have their own bounded lifetime like Go Participant.PerformRpc;
//! a caller retry/expiry cannot erase already accepted work.
use crate::rpc::{RpcFailure, RpcRegistry};
use crate::service_bus::{
    local_transport::LocalTransport,
    rpc::{CallOptions, MethodSpec, RpcClient, RpcError, RpcRetirement, RpcServer, ServerLimits},
};
use crate::workers::retry::{GenericRetry, RpcPolicy};
use lk_protocol::livekit as pb;
use rtc_core::{
    ids::{Identity, ParticipantSid, RoomKey, RoomSid},
    model::ParticipantInfo,
};
use rtc_runtime::RoomOwnerHandle;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{Mutex as AsyncMutex, Semaphore, oneshot};
fn spec(room: &str, identity: &str) -> MethodSpec {
    MethodSpec {
        service: "Participant".into(),
        method: "PerformRpc".into(),
        topics: vec![format!("{room}_{identity}")],
        require_claim: true,
        queue: true,
    }
}
#[derive(Default)]
struct Accepted {
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}
impl Accepted {
    async fn call(
        &self,
        registry: Arc<RpcRegistry>,
        owner: RoomOwnerHandle,
        sid: ParticipantSid,
        request: pb::PerformRpcRequest,
    ) -> Result<String, RpcFailure> {
        let response = self.start(async move {
            crate::rpc::perform_rpc_owned(
                &registry,
                &owner,
                &Identity(request.destination_identity),
                &request.method,
                &request.payload,
                request.response_timeout_ms,
                &sid,
            )
            .await
        });
        response.await.map_err(|_| {
            RpcFailure::Api(rtc_runtime::ApiError::Unavailable(
                "server is closed".into(),
            ))
        })?
    }
    fn start<F>(&self, future: F) -> oneshot::Receiver<Result<String, RpcFailure>>
    where
        F: std::future::Future<Output = Result<String, RpcFailure>> + Send + 'static,
    {
        let (reply, response) = oneshot::channel();
        let task = tokio::spawn(async move {
            let _ = reply.send(future.await);
        });
        {
            let mut tasks = self.tasks.lock().unwrap_or_else(|p| p.into_inner());
            tasks.retain(|t| !t.is_finished());
            tasks.push(task);
        }
        response
    }
    fn is_finished(&self) -> bool {
        self.tasks
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .all(|task| task.is_finished())
    }
    async fn finish(&self) {
        let tasks = std::mem::take(&mut *self.tasks.lock().unwrap_or_else(|p| p.into_inner()));
        for task in tasks {
            let _ = task.await;
        }
    }
}
impl Drop for Accepted {
    fn drop(&mut self) {
        for task in self
            .tasks
            .get_mut()
            .unwrap_or_else(|p| p.into_inner())
            .drain(..)
        {
            task.abort();
        }
    }
}
struct ParticipantTopic {
    sid: ParticipantSid,
    server: RpcServer,
    accepted: Arc<Accepted>,
}
struct Owner {
    sid: RoomSid,
    topics: HashMap<String, ParticipantTopic>,
}
#[derive(Default)]
struct Groups {
    stopped: bool,
    owners: HashMap<RoomKey, Owner>,
    retired: Vec<RpcRetirement>,
    accepted: Vec<Arc<Accepted>>,
}
impl Groups {
    fn retain_accepted_work(&mut self) {
        // A retired server/handler may hold this owner before its admitted
        // operation gets its first poll. Empty tasks alone cannot prove drain.
        // If this collection is the sole Arc owner, no future handler can enter.
        self.accepted
            .retain(|work| Arc::strong_count(work) > 1 || !work.is_finished());
    }
}
pub(crate) struct LocalParticipantRpc {
    bus: Arc<LocalTransport>,
    client: RpcClient,
    policy: RpcPolicy,
    registry: Arc<RpcRegistry>,
    groups: AsyncMutex<Groups>,
}
impl LocalParticipantRpc {
    pub(crate) fn new(
        bus: Arc<LocalTransport>,
        policy: RpcPolicy,
        registry: Arc<RpcRegistry>,
    ) -> Result<Self, RpcError> {
        Ok(Self {
            client: RpcClient::new_for_service(bus.clone(), Semaphore::MAX_PERMITS, "Participant")?
                .with_tracing(
                    crate::service_bus::telemetry::RpcTracing::production_client_params(),
                ),
            bus,
            policy,
            registry,
            groups: AsyncMutex::new(Groups::default()),
        })
    }
    pub(crate) async fn perform(
        &self,
        request: pb::PerformRpcRequest,
    ) -> Result<pb::PerformRpcResponse, RpcError> {
        self.request(
            &spec(&request.room, &request.destination_identity),
            &request,
        )
        .await
    }
    pub(crate) async fn request<Request, Response>(
        &self,
        method: &MethodSpec,
        request: &Request,
    ) -> Result<Response, RpcError>
    where
        Request: prost::Message,
        Response: prost::Message + Default,
    {
        let mut retry = GenericRetry::new(self.policy);
        loop {
            let mut options = CallOptions::new(
                retry
                    .timeout()
                    .unwrap_or_else(|| self.policy.request_timeout()),
            );
            options.selection.accept_first = true;
            options.selection.affinity_timeout = Some(Duration::from_secs(1));
            // Heap-own the nested PSRPC/telemetry attempt so a caller's
            // dispatcher does not inline all of its poll state on the stack.
            match Box::pin(self.client.unary(method, request, options)).await {
                Ok(response) => return Ok(response),
                Err(error) if retry.next(&error) => {}
                Err(error) => return Err(error),
            }
        }
    }

    async fn retire(&self, topic: ParticipantTopic, groups: &mut Groups) -> Result<(), RpcError> {
        self.registry.disconnect_participant(&topic.sid);
        groups.retired.push(topic.server.retire().await?);
        groups.accepted.push(topic.accepted);
        Ok(())
    }
    pub(crate) async fn refresh(
        &self,
        owner: RoomOwnerHandle,
        participants: Vec<ParticipantInfo>,
    ) -> Result<(), RpcError> {
        let mut groups = self.groups.lock().await;
        if groups.stopped {
            return Err(RpcError::new(
                "canceled",
                "local Participant client is closed",
            ));
        }
        let key = owner.key().clone();
        let mut current = groups.owners.remove(&key).unwrap_or_else(|| Owner {
            sid: owner.sid().clone(),
            topics: HashMap::new(),
        });
        if &current.sid != owner.sid() {
            for (_, topic) in current.topics.drain() {
                self.retire(topic, &mut groups).await?;
            }
            current.sid = owner.sid().clone();
        }
        let desired: HashMap<_, _> = participants
            .into_iter()
            .map(|p| (p.identity.0, p.sid))
            .collect();
        let removed: Vec<_> = current
            .topics
            .iter()
            .filter(|(identity, topic)| desired.get(*identity) != Some(&topic.sid))
            .map(|(identity, _)| identity.clone())
            .collect();
        for identity in removed {
            self.retire(
                current.topics.remove(&identity).expect("present"),
                &mut groups,
            )
            .await?;
        }
        // Local bus membership registration cannot wait on the actor: this is
        // called by that actor before delivering a successful Join handle.
        for (identity, sid) in desired {
            if current.topics.contains_key(&identity) {
                continue;
            }
            let registered = identity.clone();
            let captured = owner.clone();
            let registry = self.registry.clone();
            let accepted = Arc::new(Accepted::default());
            let work = accepted.clone();
            let expected = sid.clone();
            let server =
                RpcServer::register::<pb::PerformRpcRequest, pb::PerformRpcResponse, _, _>(
                    self.bus.clone(),
                    spec(&key.name.0, &identity),
                    ServerLimits {
                        concurrent: Semaphore::MAX_PERMITS,
                        max_request_duration: Duration::MAX,
                        allow_skip_claim: false,
                    },
                    move |_, request| {
                        let owner = captured.clone();
                        let identity = registered.clone();
                        let registry = registry.clone();
                        let work = work.clone();
                        let expected = expected.clone();
                        async move {
                            if request.room != owner.key().name.0
                                || request.destination_identity != identity
                            {
                                return Err(RpcError::new(
                                    "failed_precondition",
                                    "participant identity does not match registered topic",
                                ));
                            }
                            let payload = work
                                .call(registry, owner, expected, request)
                                .await
                                .map_err(|failure| {
                                    let error: crate::twirp::error::TwirpError = failure.into();
                                    let mut result = RpcError::new(error.code.as_str(), error.msg);
                                    result.application_metadata = error.meta;
                                    result
                                })?;
                            Ok(pb::PerformRpcResponse { payload })
                        }
                    },
                    None,
                )
                .await;
            let server = match server {
                Ok(server) => server,
                Err(error) => {
                    groups.owners.insert(key, current);
                    return Err(error);
                }
            };
            current.topics.insert(
                identity,
                ParticipantTopic {
                    sid,
                    server,
                    accepted,
                },
            );
        }
        groups.retired.retain(|receipt| !receipt.is_finished());
        groups.retain_accepted_work();
        groups.owners.insert(key, current);
        Ok(())
    }
    pub(crate) async fn closed(&self, key: &RoomKey, sid: &RoomSid) {
        let mut groups = self.groups.lock().await;
        if groups
            .owners
            .get(key)
            .is_some_and(|owner| &owner.sid == sid)
        {
            let owner = groups.owners.remove(key).unwrap();
            for (_, topic) in owner.topics {
                if let Err(error) = self.retire(topic, &mut groups).await {
                    tracing::error!(%error,"local Participant retirement failed");
                }
            }
        }
    }
    pub(crate) async fn shutdown(&self) {
        let (retired, accepted) = {
            let mut groups = self.groups.lock().await;
            groups.stopped = true;
            self.client.close();
            for (_, owner) in std::mem::take(&mut groups.owners) {
                for (_, topic) in owner.topics {
                    if let Err(error) = self.retire(topic, &mut groups).await {
                        tracing::error!(%error,"local Participant shutdown failed");
                    }
                }
            }
            (
                std::mem::take(&mut groups.retired),
                std::mem::take(&mut groups.accepted),
            )
        };
        for receipt in retired {
            receipt.finish().await;
        }
        for work in accepted {
            work.finish().await;
        }
        self.client.close_and_wait().await;
    }
}

#[cfg(test)]
#[path = "local_participant/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "local_participant/retirement_lifetime.rs"]
mod retirement_lifetime;
