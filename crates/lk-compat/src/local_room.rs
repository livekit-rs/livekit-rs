//! Original Room.SendData typed PSRPC consumer for standalone LocalMessageBus.
//! No parallel room authority: each membership captures the real actor SID.
use crate::{
    service_bus::{
        local_transport::LocalTransport,
        rpc::{
            CallOptions, MethodSpec, RpcClient, RpcError, RpcRetirement, RpcServer, ServerLimits,
        },
    },
    workers::retry::{GenericRetry, RpcPolicy},
};
use lk_protocol::livekit as pb;
use prost::Message;
use rtc_core::ids::{Identity, ParticipantSid, RoomKey, RoomSid};
use rtc_runtime::RoomOwnerHandle;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{Mutex as AsyncMutex, Semaphore, oneshot};
fn method(room: &str) -> MethodSpec {
    MethodSpec {
        service: "Room".into(),
        method: "SendData".into(),
        topics: vec![room.into()],
        require_claim: true,
        queue: true,
    }
}
fn api(error: rtc_runtime::ApiError) -> RpcError {
    let error: crate::twirp::error::TwirpError = error.into();
    let mut result = RpcError::new(error.code.as_str(), error.msg);
    result.application_metadata = error.meta;
    result
}
#[derive(Default)]
struct Accepted(Mutex<Vec<tokio::task::JoinHandle<()>>>);
impl Accepted {
    async fn send(
        &self,
        owner: RoomOwnerHandle,
        request: pb::SendDataRequest,
    ) -> Result<pb::SendDataResponse, RpcError> {
        let response = self.start(async move {
            let packet = crate::twirp::room_service::send_data_packet(&request).encode_to_vec();
            let result = owner
                .send_data_with_nonce(
                    crate::convert::data_kind_from_proto(request.kind),
                    packet,
                    request
                        .destination_identities
                        .into_iter()
                        .map(Identity)
                        .collect(),
                    request
                        .destination_sids
                        .into_iter()
                        .map(ParticipantSid)
                        .collect(),
                    request.nonce,
                )
                .await
                .map_err(api)
                .map(|_| pb::SendDataResponse {});
            result
        });
        response
            .await
            .map_err(|_| RpcError::new("unavailable", "accepted room data operation closed"))?
    }
    fn start<F>(&self, future: F) -> oneshot::Receiver<Result<pb::SendDataResponse, RpcError>>
    where
        F: std::future::Future<Output = Result<pb::SendDataResponse, RpcError>> + Send + 'static,
    {
        let (reply, response) = oneshot::channel();
        let task = tokio::spawn(async move {
            let _ = reply.send(future.await);
        });
        {
            let mut tasks = self.0.lock().unwrap_or_else(|p| p.into_inner());
            tasks.retain(|t| !t.is_finished());
            tasks.push(task);
        }
        response
    }
    fn finished(&self) -> bool {
        self.0
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .all(|t| t.is_finished())
    }
    async fn join(&self) {
        let tasks = std::mem::take(&mut *self.0.lock().unwrap_or_else(|p| p.into_inner()));
        for task in tasks {
            let _ = task.await;
        }
    }
}
impl Drop for Accepted {
    fn drop(&mut self) {
        for task in self
            .0
            .get_mut()
            .unwrap_or_else(|p| p.into_inner())
            .drain(..)
        {
            task.abort();
        }
    }
}
struct Owner {
    sid: RoomSid,
    server: RpcServer,
    accepted: Arc<Accepted>,
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
            .retain(|work| Arc::strong_count(work) > 1 || !work.finished());
    }
}
pub(crate) struct LocalRoom {
    bus: Arc<LocalTransport>,
    client: RpcClient,
    policy: RpcPolicy,
    groups: AsyncMutex<Groups>,
}
impl LocalRoom {
    pub(crate) fn new(bus: Arc<LocalTransport>, policy: RpcPolicy) -> Result<Self, RpcError> {
        Ok(Self {
            client: RpcClient::new_for_service(bus.clone(), Semaphore::MAX_PERMITS, "Room")?
                .with_tracing(
                    crate::service_bus::telemetry::RpcTracing::production_client_params(),
                ),
            bus,
            policy,
            groups: AsyncMutex::new(Groups::default()),
        })
    }
    pub(crate) async fn send(
        &self,
        request: pb::SendDataRequest,
    ) -> Result<pb::SendDataResponse, RpcError> {
        self.request(&method(&request.room), &request).await
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
            match Box::pin(self.client.unary(method, request, options)).await {
                Ok(response) => return Ok(response),
                Err(error) if retry.next(&error) => {}
                Err(error) => return Err(error),
            }
        }
    }

    async fn retire(&self, owner: Owner, groups: &mut Groups) -> Result<(), RpcError> {
        groups.retired.push(owner.server.retire().await?);
        groups.accepted.push(owner.accepted);
        Ok(())
    }
    pub(crate) async fn opened(&self, owner: RoomOwnerHandle) -> Result<(), RpcError> {
        let mut groups = self.groups.lock().await;
        if groups.stopped {
            return Err(RpcError::new("canceled", "local Room client is closed"));
        }
        let key = owner.key().clone();
        if groups
            .owners
            .get(&key)
            .is_some_and(|entry| &entry.sid == owner.sid())
        {
            return Ok(());
        }
        if let Some(old) = groups.owners.remove(&key) {
            self.retire(old, &mut groups).await?;
        }
        let accepted = Arc::new(Accepted::default());
        let work = accepted.clone();
        let captured = owner.clone();
        let server = RpcServer::register::<pb::SendDataRequest, pb::SendDataResponse, _, _>(
            self.bus.clone(),
            method(&key.name.0),
            ServerLimits {
                concurrent: Semaphore::MAX_PERMITS,
                max_request_duration: Duration::MAX,
                allow_skip_claim: false,
            },
            move |_, request| {
                let owner = captured.clone();
                let work = work.clone();
                async move {
                    if request.room != owner.key().name.0 {
                        return Err(RpcError::new(
                            "failed_precondition",
                            "room does not match registered topic",
                        ));
                    }
                    work.send(owner, request).await
                }
            },
            None,
        )
        .await?;
        groups.retired.retain(|t| !t.is_finished());
        groups.retain_accepted_work();
        groups.owners.insert(
            key,
            Owner {
                sid: owner.sid().clone(),
                server,
                accepted,
            },
        );
        Ok(())
    }
    /// Test-only observation of the actual admission lifecycle gate; no fake
    /// retirement/ACK can be injected through this read-only probe.
    #[cfg(test)]
    pub(crate) fn admission_is_busy(&self) -> bool {
        self.groups.try_lock().is_err()
    }
    pub(crate) async fn closed(&self, key: &RoomKey, sid: &RoomSid) {
        let mut groups = self.groups.lock().await;
        if groups
            .owners
            .get(key)
            .is_some_and(|owner| &owner.sid == sid)
        {
            let owner = groups.owners.remove(key).unwrap();
            if let Err(error) = self.retire(owner, &mut groups).await {
                tracing::error!(%error,"local Room admission retirement failed");
            }
        }
    }
    pub(crate) async fn shutdown(&self) {
        let (retired, accepted) = {
            let mut groups = self.groups.lock().await;
            groups.stopped = true;
            self.client.close();
            for (_, owner) in std::mem::take(&mut groups.owners) {
                if let Err(error) = self.retire(owner, &mut groups).await {
                    tracing::error!(%error,"local Room shutdown failed");
                }
            }
            (
                std::mem::take(&mut groups.retired),
                std::mem::take(&mut groups.accepted),
            )
        };
        for task in retired {
            task.finish().await;
        }
        for work in accepted {
            work.join().await;
        }
        self.client.close_and_wait().await;
    }
}
#[cfg(test)]
#[path = "local_room/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "local_room/retirement_lifetime.rs"]
mod retirement_lifetime;
