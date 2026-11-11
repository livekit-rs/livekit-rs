//! Generation-owned accepted application operations. PSRPC's wire deadline still
//! bounds its response handler; it cannot withdraw a source synchronous room
//! operation that has already entered (Go RoomManager SendData/PerformRpc).
use super::*;
use std::{collections::BTreeMap, sync::Mutex as StdMutex};
use tokio::sync::{oneshot, watch};
#[derive(Default)]
struct Generation {
    operations: Vec<Arc<Operation>>,
    servers: Vec<crate::service_bus::rpc::RpcRetirement>,
}
struct Operation {
    done: watch::Receiver<bool>,
    task: StdMutex<Option<tokio::task::JoinHandle<()>>>,
}
impl Operation {
    fn finished(&self) -> bool {
        *self.done.borrow()
    }
    async fn finish(&self) {
        let mut done = self.done.clone();
        let _ = done.wait_for(|v| *v).await;
        let task = self.task.lock().unwrap_or_else(|p| p.into_inner()).take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }
}
impl Drop for Operation {
    fn drop(&mut self) {
        if let Some(task) = self
            .task
            .get_mut()
            .unwrap_or_else(|p| p.into_inner())
            .take()
        {
            task.abort();
        }
    }
}
struct Completion(watch::Sender<bool>);
impl Drop for Completion {
    fn drop(&mut self) {
        self.0.send_replace(true);
    }
}
#[derive(Default)]
pub(crate) struct Accepted {
    #[cfg(test)]
    outcomes: Arc<StdMutex<Vec<Option<String>>>>,
    generations: StdMutex<BTreeMap<RoomSid, Generation>>,
    closed: std::sync::atomic::AtomicBool,
}
impl Accepted {
    pub(crate) async fn run<T, F>(&self, sid: &RoomSid, future: F) -> Result<T, RpcError>
    where
        T: Send + 'static,
        F: std::future::Future<Output = Result<T, RpcError>> + Send + 'static,
    {
        let (reply, response) = oneshot::channel();
        {
            let mut generations = self.generations.lock().unwrap_or_else(|p| p.into_inner());
            if self.closed.load(std::sync::atomic::Ordering::Acquire) {
                return Err(RpcError::new(
                    "unavailable",
                    "accepted room operations closed",
                ));
            }
            let (done_tx, done) = watch::channel(false);
            // Capture the guard before spawn: even abort-before-first-poll notifies
            // completion, rather than leaving shutdown waiting for an impossible poll.
            let completion = Completion(done_tx);
            #[cfg(test)]
            let outcomes = self.outcomes.clone();
            // Entering a different owned Tokio task must not erase the actual
            // server RPC trace parent carried by the admitted request.
            use opentelemetry::trace::FutureExt as _;
            let future = future.with_current_context();
            let task = tokio::spawn(async move {
                let _completion = completion;
                let result = future.await;
                #[cfg(test)]
                outcomes
                    .lock()
                    .unwrap()
                    .push(result.as_ref().err().map(|e| e.code.clone()));
                let _ = reply.send(result);
            });
            let operation = Arc::new(Operation {
                done,
                task: StdMutex::new(Some(task)),
            });
            generations.retain(|_, g| {
                g.operations.retain(|op| !op.finished());
                g.servers.retain(|s| !s.is_finished());
                !g.operations.is_empty() || !g.servers.is_empty()
            });
            generations
                .entry(sid.clone())
                .or_default()
                .operations
                .push(operation);
        }
        response
            .await
            .map_err(|_| RpcError::new("unavailable", "accepted room operation closed"))?
    }
    pub(crate) async fn retire_server(
        &self,
        sid: &RoomSid,
        server: RpcServer,
    ) -> Result<(), RpcError> {
        let receipt = server.retire().await?;
        self.generations
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entry(sid.clone())
            .or_default()
            .servers
            .push(receipt);
        Ok(())
    }
    /// Called only from RoomControl's independently owned ShutdownRun, after
    /// routing projection detach and every live generation's subscription ACK.
    pub(crate) async fn finish(&self) {
        let receipts = {
            let mut groups = self.generations.lock().unwrap_or_else(|p| p.into_inner());
            groups
                .values_mut()
                .flat_map(|g| std::mem::take(&mut g.servers))
                .collect::<Vec<_>>()
        };
        // All admitted/pre-grant handlers finish first, so no late handler can
        // enter after we take this generation's actual operation collection.
        for receipt in receipts {
            receipt.finish().await;
        }
        let operations = {
            let groups = self.generations.lock().unwrap_or_else(|p| p.into_inner());
            self.closed
                .store(true, std::sync::atomic::Ordering::Release);
            groups
                .values()
                .flat_map(|g| g.operations.iter().cloned())
                .collect::<Vec<_>>()
        };
        for operation in operations {
            operation.finish().await;
        }
        self.generations
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clear();
    }
    #[cfg(test)]
    pub(crate) fn outcomes(&self) -> Vec<Option<String>> {
        self.outcomes.lock().unwrap().clone()
    }
    #[cfg(test)]
    pub(crate) fn pending(&self) -> usize {
        self.generations
            .lock()
            .unwrap()
            .values()
            .flat_map(|g| &g.operations)
            .filter(|o| !o.finished())
            .count()
    }
}

use crate::service_bus::rpc_transport::RpcBus;
pub(super) async fn register_send_data(
    bus: RpcBus,
    owner: &RoomOwnerHandle,
    limits: ServerLimits,
    accepted: Arc<Accepted>,
) -> Result<RpcServer, RpcError> {
    let captured = owner.clone();
    RpcServer::register_with_raw::<pb::SendDataRequest, pb::SendDataResponse, _, _>(
        bus,
        super::room_spec(&owner.key().name.0, "SendData"),
        limits,
        move |_, raw| {
            let owner = captured.clone();
            let accepted = accepted.clone();
            async move {
                let sid = owner.sid().clone();
                accepted
                    .run(&sid, async move {
                        let request = raw.message;
                        check(&owner, &request.room)?;
                        let packet = crate::twirp::room_service::send_data_packet(&request);
                        use prost::Message;
                        owner
                            .send_data_with_nonce(
                                convert::data_kind_from_proto(request.kind),
                                packet.encode_to_vec(),
                                request
                                    .destination_identities
                                    .into_iter()
                                    .map(Identity)
                                    .collect(),
                                request
                                    .destination_sids
                                    .into_iter()
                                    .map(rtc_core::ids::ParticipantSid)
                                    .collect(),
                                request.nonce,
                            )
                            .await
                            .map_err(api)?;
                        Ok(pb::SendDataResponse {})
                    })
                    .await
            }
        },
        None,
    )
    .await
}
pub(super) async fn register_perform_rpc(
    bus: RpcBus,
    owner: &RoomOwnerHandle,
    identity: &str,
    state: AppState,
    limits: ServerLimits,
    accepted: Arc<Accepted>,
) -> Result<RpcServer, RpcError> {
    let captured = owner.clone();
    let registered = identity.to_owned();
    RpcServer::register_with_raw::<pb::PerformRpcRequest, pb::PerformRpcResponse, _, _>(
        bus,
        super::participants::participant_spec(&owner.key().name.0, identity, "PerformRpc"),
        limits,
        move |_, raw| {
            let owner = captured.clone();
            let registered = registered.clone();
            let state = state.clone();
            let accepted = accepted.clone();
            async move {
                let sid = owner.sid().clone();
                accepted
                    .run(&sid, async move {
                        let request = raw.message;
                        check(&owner, &request.room)?;
                        if request.destination_identity != registered {
                            return Err(RpcError::new(
                                "failed_precondition",
                                "participant identity does not match registered topic",
                            ));
                        }
                        let payload = crate::rpc::perform_rpc_bound(
                            &state,
                            owner.key(),
                            &Identity(request.destination_identity),
                            &request.method,
                            &request.payload,
                            request.response_timeout_ms,
                            Some(&owner),
                        )
                        .await
                        .map_err(|failure| {
                            let error: crate::twirp::error::TwirpError = failure.into();
                            let mut result = RpcError::new(error.code.as_str(), error.msg);
                            result.application_metadata = error.meta;
                            result
                        })?;
                        Ok(pb::PerformRpcResponse { payload })
                    })
                    .await
            }
        },
        None,
    )
    .await
}
