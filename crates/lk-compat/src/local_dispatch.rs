//! Real standalone AgentDispatchInternal client/topics over the common RPC core.
//! No second business registry; every handler owns the actual RoomOwnerHandle.
use crate::owner_dispatch::AgentDispatchTopics;
use crate::service_bus::{
    local_transport::LocalTransport,
    redis_transport::TransportLimits,
    rpc::{CallOptions, RpcClient, RpcError, RpcRetirement, ServerLimits},
    wire::{CompressionPolicy, WireLimits},
};
use crate::workers::retry::{GenericRetry, RpcPolicy};
use lk_protocol::livekit as pb;
use rtc_core::ids::{RoomKey, RoomSid};
use rtc_runtime::{RoomOwnerHandle, RoomOwnerLifecycle};
use std::collections::HashMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;
use tokio::sync::{Mutex as AsyncMutex, Semaphore};

pub(crate) struct LocalDispatch {
    bus: Arc<LocalTransport>,
    pub(crate) participant: crate::local_participant::LocalParticipantRpc,
    pub(crate) room: crate::local_room::LocalRoom,
    pub(crate) mutations: crate::local_mutations::LocalMutations,
    client: RpcClient,
    policy: RpcPolicy,
    owners: AsyncMutex<HashMap<RoomKey, AgentDispatchTopics>>,
    retired: Mutex<Vec<RpcRetirement>>,
    stopped: AtomicBool,
    shutdown_gate: AsyncMutex<()>,
}
impl LocalDispatch {
    #[cfg(test)]
    pub(crate) fn owned_admission_bus_for_test(&self) -> Arc<LocalTransport> {
        self.bus.clone()
    }

    pub(crate) fn new(
        policy: RpcPolicy,
        compression: CompressionPolicy,
        max_decompressed_size: usize,
        registry: Arc<crate::rpc::RpcRegistry>,
    ) -> Result<Arc<Self>, RpcError> {
        let size = policy
            .handler_channel_size()
            .map_err(|e| RpcError::new("invalid_argument", e))?;
        // Source localbus has no global subscription/command or handler-duration
        // ceiling. These fields remain explicit source-unbounded values.
        let bus = LocalTransport::new_with_gzip_limit(
            TransportLimits {
                wire: WireLimits {
                    encoded_bytes: usize::MAX,
                    decompressed_bytes: isize::MAX as usize - 1,
                },
                compression,
                inbox_messages: size,
                subscriptions: Semaphore::MAX_PERMITS,
                commands: Semaphore::MAX_PERMITS,
                io_timeout: Duration::MAX,
            },
            max_decompressed_size,
        )?;
        let client = RpcClient::new_for_service(
            bus.clone(),
            Semaphore::MAX_PERMITS,
            "AgentDispatchInternal",
        )?
        .with_tracing(crate::service_bus::telemetry::RpcTracing::production_client_params());
        let room = crate::local_room::LocalRoom::new(bus.clone(), policy)?;
        let mutations = crate::local_mutations::LocalMutations::new(bus.clone());
        let participant =
            crate::local_participant::LocalParticipantRpc::new(bus.clone(), policy, registry)?;
        Ok(Arc::new(Self {
            bus,
            participant,
            room,
            mutations,
            client,
            policy,
            owners: AsyncMutex::new(HashMap::new()),
            retired: Mutex::new(Vec::new()),
            stopped: AtomicBool::new(false),
            shutdown_gate: AsyncMutex::new(()),
        }))
    }
    async fn configured<Req, Resp>(
        &self,
        room: &str,
        method: &str,
        request: &Req,
    ) -> Result<Resp, RpcError>
    where
        Req: prost::Message,
        Resp: prost::Message + prost::Name + Default,
    {
        let spec = crate::owner_dispatch::method(room, method);
        let mut retry = GenericRetry::new(self.policy);
        loop {
            let mut options = CallOptions::new(
                retry
                    .timeout()
                    .unwrap_or_else(|| self.policy.request_timeout()),
            );
            options.selection.accept_first = true;
            options.selection.affinity_timeout = Some(Duration::from_secs(1));
            match self.client.unary(&spec, request, options).await {
                Ok(value) => return Ok(value),
                Err(error) if retry.next(&error) => {}
                Err(error) => return Err(error),
            }
        }
    }
    pub(crate) async fn create(
        &self,
        request: pb::AgentDispatch,
    ) -> Result<pb::AgentDispatch, RpcError> {
        self.configured(&request.room, "CreateDispatch", &request)
            .await
    }
    pub(crate) async fn delete(
        &self,
        request: pb::DeleteAgentDispatchRequest,
    ) -> Result<pb::AgentDispatch, RpcError> {
        self.configured(&request.room, "DeleteDispatch", &request)
            .await
    }
    pub(crate) async fn list(
        &self,
        request: pb::ListAgentDispatchRequest,
    ) -> Result<pb::ListAgentDispatchResponse, RpcError> {
        self.configured(&request.room, "ListDispatch", &request)
            .await
    }
    async fn retain_retirement(&self, group: AgentDispatchTopics) {
        // Every group here was registered on this local transport; a Redis
        // group is structurally excluded by the constructor above.
        let receipts = group
            .retire()
            .await
            .expect("local owner acceptance close is acknowledged");
        let mut retired = self.retired.lock().unwrap_or_else(|p| p.into_inner());
        retired.retain(|receipt| !receipt.is_finished());
        retired.extend(receipts);
    }
    pub(crate) async fn perform_rpc(
        &self,
        request: pb::PerformRpcRequest,
    ) -> Result<pb::PerformRpcResponse, RpcError> {
        self.participant.perform(request).await
    }
    pub(crate) async fn send_data(
        &self,
        request: pb::SendDataRequest,
    ) -> Result<pb::SendDataResponse, RpcError> {
        self.room.send(request).await
    }
    pub(crate) async fn shutdown(&self) {
        self.room.shutdown().await;
        self.participant.shutdown().await;
        let _joining = self.shutdown_gate.lock().await;
        let retired = {
            // This lock is also held by opened/closed. Closing admission,
            // retiring all memberships and taking all receipts is one boundary;
            // no close callback can append a receipt after this snapshot.
            let mut owners = self.owners.lock().await;
            self.stopped.store(true, Ordering::Release);
            self.client.close();
            for (_, group) in std::mem::take(&mut *owners) {
                self.retain_retirement(group).await;
            }
            std::mem::take(&mut *self.retired.lock().unwrap_or_else(|p| p.into_inner()))
        };
        for task in retired {
            task.finish().await;
        }
        self.mutations.shutdown().await;
        self.client.close_and_wait().await;
    }
}
impl RoomOwnerLifecycle for LocalDispatch {
    fn opened(
        &self,
        owner: RoomOwnerHandle,
    ) -> rtc_runtime::RoomOwnerOperation<'_, Result<(), String>> {
        Box::pin(async move {
            self.room
                .opened(owner.clone())
                .await
                .map_err(|error| error.to_string())?;
            self.mutations
                .opened(owner.clone())
                .await
                .map_err(|e| e.to_string())?;
            self.client
                .ready_for_service("AgentDispatchInternal")
                .await
                .map_err(|e| e.to_string())?;
            let mut owners = self.owners.lock().await;
            if self.stopped.load(Ordering::Acquire) {
                return Err("local dispatch client is closed".into());
            }
            let key = owner.key().clone();
            if owners
                .get(&key)
                .is_some_and(|group| group.sid() == owner.sid())
            {
                return Ok(());
            }
            if let Some(old) = owners.remove(&key) {
                self.retain_retirement(old).await;
            }
            let group = AgentDispatchTopics::register_with_accepted(
                owner,
                self.bus.clone().into(),
                ServerLimits {
                    concurrent: Semaphore::MAX_PERMITS,
                    max_request_duration: Duration::MAX,
                    allow_skip_claim: false,
                },
                self.mutations.accepted_operations(),
            )
            .await
            .map_err(|e| e.to_string())?;
            owners.insert(key, group);
            Ok(())
        })
    }
    fn participants(
        &self,
        owner: RoomOwnerHandle,
        participants: Vec<rtc_core::model::ParticipantInfo>,
    ) -> rtc_runtime::RoomOwnerOperation<'_, Result<(), String>> {
        Box::pin(async move {
            self.mutations.project(&owner, &participants);
            self.participant
                .refresh(owner.clone(), participants.clone())
                .await
                .map_err(|error| error.to_string())?;
            self.mutations
                .refresh(owner, &participants)
                .await
                .map_err(|e| e.to_string())
        })
    }
    fn closed(&self, key: RoomKey, sid: RoomSid) -> rtc_runtime::RoomOwnerOperation<'_, ()> {
        Box::pin(async move {
            self.room.closed(&key, &sid).await;
            self.participant.closed(&key, &sid).await;
            self.mutations.closed(&key, &sid).await;
            let mut owners = self.owners.lock().await;
            if owners.get(&key).is_some_and(|group| group.sid() == &sid)
                && let Some(group) = owners.remove(&key)
            {
                self.retain_retirement(group).await;
            }
        })
    }
}

#[cfg(test)]
mod owner_ack_tests {
    use super::*;
    use crate::service_bus::channels::Channel;
    use crate::state::CompatServices;
    use futures_util::{pin_mut, poll};
    use rtc_core::events::NullSink;
    use rtc_runtime::net::{LocalInterface, NetConfig, NetRouter};
    use std::task::Poll;
    #[tokio::test]
    async fn real_manager_create_waits_for_topics_and_delete_waits_for_subscription_close_ack() {
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
        let state = CompatServices::new(Arc::new(crate::test_support::config()))
            .assemble(net.clone(), Arc::new(NullSink));
        let bus = state.local_dispatch.bus.clone();
        let held = Channel {
            legacy: "source-owner-readiness-held".into(),
            ..Default::default()
        };
        let sub = bus.subscribe_sized(&held, true, 1).await;
        bus.publish_encoded(&held, b"one").await.unwrap();
        let publishing = bus.publish_encoded(&held, b"two");
        pin_mut!(publishing);
        assert!(matches!(poll!(publishing.as_mut()), Poll::Pending));
        let registering = bus.subscribe_sized(&held, true, 1);
        pin_mut!(registering);
        assert!(matches!(poll!(registering.as_mut()), Poll::Pending));
        let key = state.room_key("real-ready-ack");
        let creating = {
            let manager = state.manager.clone();
            let key = key.clone();
            tokio::spawn(async move { manager.create_room(key, Default::default()).await })
        };
        tokio::time::timeout(Duration::from_secs(3), async {
            while !state.manager.room_exists(&key) {
                assert!(
                    !creating.is_finished(),
                    "owner construction must await readiness, not fail due to now_or_never"
                );
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::task::yield_now().await;
        assert!(
            !creating.is_finished(),
            "manager must not return success before actual owner topic subscriptions become ready"
        );
        let (closed, sent, sub) = tokio::join!(sub.unsubscribe(&held), publishing, registering);
        closed.unwrap();
        sent.unwrap();
        sub.unsubscribe(&held).await.unwrap();
        let room = creating.await.unwrap().unwrap();
        assert!(!room.sid.0.is_empty());
        let dispatch = state
            .local_dispatch
            .create(pb::AgentDispatch {
                id: "AD_actual_ready".into(),
                room: key.name.0.clone(),
                agent_name: "actual-owner".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(dispatch.id, "AD_actual_ready");
        assert!(dispatch.state.unwrap().jobs.is_empty());
        let held = Channel {
            legacy: "source-owner-close-held".into(),
            ..Default::default()
        };
        let sub = bus.subscribe_sized(&held, true, 1).await;
        bus.publish_encoded(&held, b"one").await.unwrap();
        let publishing = bus.publish_encoded(&held, b"two");
        pin_mut!(publishing);
        assert!(matches!(poll!(publishing.as_mut()), Poll::Pending));
        let registering = bus.subscribe_sized(&held, true, 1);
        pin_mut!(registering);
        assert!(matches!(poll!(registering.as_mut()), Poll::Pending));
        let deleting = {
            let manager = state.manager.clone();
            let key = key.clone();
            tokio::spawn(async move { manager.delete_room(&key).await })
        };
        // Observe the actual close consumer owning its lifecycle mutex while
        // waiting for real subscription retirement; no fake Close effect.
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                // Room.SendData is the first real retirement group now. Both
                // groups wait for the same held transport unsubscribe ACK;
                // observing only the later Agent group would never see it.
                if state.local_dispatch.room.admission_is_busy()
                    || state.local_dispatch.owners.try_lock().is_err()
                {
                    break;
                }
                assert!(
                    !deleting.is_finished(),
                    "DeleteRoom returned before topic Close ACK"
                );
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!deleting.is_finished());
        let (closed, sent, sub) = tokio::join!(sub.unsubscribe(&held), publishing, registering);
        closed.unwrap();
        sent.unwrap();
        sub.unsubscribe(&held).await.unwrap();
        deleting.await.unwrap().unwrap();
        assert!(!state.manager.room_exists(&key));
        state.manager.close_all().await;
        state.shutdown_workers().await;
        net.shutdown_and_wait().await;
    }
}

#[cfg(test)]
#[path = "local_dispatch_configured_tests.rs"]
mod configured_constructor_tests;
