//! Shared adapter state handed to every HTTP/WebSocket handler.

use std::sync::Arc;

use rtc_core::config::ServerConfig;
use rtc_core::events::EventSink;
use rtc_core::ids::{ProjectId, RoomKey, RoomName};
use rtc_runtime::net::NetRouter;
use rtc_runtime::{RoomManager, RuntimeDeps};

use crate::auth::{KeyStore, TokenMinter};
use crate::data::LiveKitDataHandler;
use crate::rpc::RpcRegistry;
use crate::turn_auth::TurnAuth;

#[derive(Clone)]
pub struct AppState {
    pub(crate) local_dispatch: Arc<crate::local_dispatch::LocalDispatch>,
    pub(crate) room_creates: Arc<crate::room_create::LocalRoomCreate>,
    pub(crate) routing_events: Arc<crate::room_control::events::RoutingEvents>,
    pub(crate) room_control: Option<Arc<crate::room_control::RoomControl>>,
    pub(crate) cluster_signal: Option<Arc<crate::cluster_signal::ClusterSignal>>,
    pub(crate) whip: Arc<crate::whip::WhipService>,
    pub(crate) whip_rpc: Option<Arc<crate::whip::cluster::WhipRpc>>,
    /// Same explicitly projected PSRPC policy as the worker/WHIP adapters.
    pub(crate) rpc_policy: crate::workers::retry::RpcPolicy,
    pub(crate) signal_connect_attempts: usize,
    pub(crate) worker_services: Option<Arc<crate::worker_services::WorkerServices>>,
    pub(crate) agents: Arc<crate::agent::service::AgentService>,
    pub(crate) manager: RoomManager,
    pub(crate) config: Arc<ServerConfig>,
    pub(crate) keys: Arc<KeyStore>,
    pub(crate) rpc: Arc<RpcRegistry>,
    pub(crate) tokens: Arc<TokenMinter>,
    pub(crate) turn_auth: Arc<TurnAuth>,
    pub(crate) sip_catalog: Option<Arc<crate::sip::catalog::SipCatalog>>,
    /// This process's `livekit.Node` record (`routing.LocalNode`), installed by
    /// the binary the moment `NewLocalNode` succeeds. `healthCheck` reads its
    /// `stats.updated_at`, so a server whose stats never advance reports 406 in
    /// both single-node and Redis routing modes.
    pub(crate) node: Option<Arc<crate::routing::LocalNode>>,
    pub(crate) local_allocation: Option<crate::local_allocation::LocalAllocation>,
}

/// Constructs adapter and runtime collaborators as one consistent service graph.
///
/// Pending RPCs and inbound data share a registry by construction. Accepting an
/// independently constructed manager here would allow its data handler to use a
/// different registry, leaving HTTP `PerformRpc` callers waiting forever.
pub struct CompatServices {
    #[cfg(test)]
    test_lifecycle_decorator: Option<
        Box<
            dyn FnOnce(
                    Arc<crate::local_dispatch::LocalDispatch>,
                ) -> Arc<dyn rtc_runtime::RoomOwnerLifecycle>
                + Send
                + Sync,
        >,
    >,

    whip_policy: crate::workers::retry::RpcPolicy,
    signal_connect_attempts: usize,
    local_rpc_compression: crate::service_bus::wire::CompressionPolicy,
    local_rpc_max_decompressed: usize,
    agent_config: crate::agent::service::ServiceConfig,
    config: Arc<ServerConfig>,
    keys: Arc<KeyStore>,
    tokens: Arc<TokenMinter>,
    turn_auth: Arc<TurnAuth>,
    rpc: Arc<RpcRegistry>,
    data: Arc<LiveKitDataHandler>,
    sip_catalog: Option<Arc<crate::sip::catalog::SipCatalog>>,
}

impl CompatServices {
    pub fn new(config: Arc<ServerConfig>) -> Self {
        let keys = Arc::new(KeyStore::from_config(&config));
        let rpc = Arc::new(RpcRegistry::new());
        Self {
            #[cfg(test)]
            test_lifecycle_decorator: None,
            agent_config: crate::agent::service::ServiceConfig::default(),
            whip_policy: crate::workers::retry::RpcPolicy::default(),
            signal_connect_attempts: 3,
            local_rpc_compression: crate::service_bus::wire::CompressionPolicy {
                quality: 0,
                threshold: 1024,
            },
            local_rpc_max_decompressed: isize::MAX as usize - 1,
            tokens: Arc::new(TokenMinter::new(keys.clone())),
            turn_auth: Arc::new(TurnAuth::new(keys.clone())),
            data: Arc::new(LiveKitDataHandler::new(rpc.clone())),
            config,
            keys,
            rpc,
            sip_catalog: None,
        }
    }

    /// Fixed RTCService retries the entire start+first-response operation. This
    /// applies equally to the local router and Redis relay deployments.
    pub fn with_signal_connect_attempts(mut self, attempts: usize) -> Self {
        self.signal_connect_attempts = attempts;
        self
    }

    /// Adapter-owned worker policy, projected from native configuration before
    /// runtime construction; the protocol-neutral media engine does not own it.
    pub fn with_whip_policy(mut self, policy: crate::workers::retry::RpcPolicy) -> Self {
        self.whip_policy = policy;
        self
    }
    /// The same source PSRPC bus options apply when Redis is absent.
    pub fn with_local_rpc_compression(
        mut self,
        compression: crate::service_bus::wire::CompressionPolicy,
        max_decompressed_size: usize,
    ) -> Self {
        self.local_rpc_compression = compression;
        self.local_rpc_max_decompressed = max_decompressed_size;
        self
    }
    pub fn with_agent_config(mut self, config: crate::agent::service::ServiceConfig) -> Self {
        self.agent_config = config;
        self
    }

    /// Attach the shared persistent catalog before runtime service assembly.
    pub fn with_sip_catalog(mut self, catalog: crate::sip::catalog::SipCatalog) -> Self {
        self.sip_catalog = Some(Arc::new(catalog));
        self
    }

    /// The same configured keys used by tokens and TURN, available for Webhook
    /// validation before sockets or room tasks are started.
    pub fn keys(&self) -> &Arc<KeyStore> {
        &self.keys
    }

    #[cfg(test)]
    pub(crate) fn with_test_lifecycle_decorator(
        mut self,
        decorate: impl FnOnce(
            Arc<crate::local_dispatch::LocalDispatch>,
        ) -> Arc<dyn rtc_runtime::RoomOwnerLifecycle>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.test_lifecycle_decorator = Some(Box::new(decorate));
        self
    }
    pub fn assemble(self, net: NetRouter, events: Arc<dyn EventSink>) -> AppState {
        let routing_events = Arc::new(crate::room_control::events::RoutingEvents::new(events));
        let manager = RoomManager::new(RuntimeDeps {
            config: self.config.clone(),
            net,
            events: routing_events.clone(),
            data: self.data,
        });
        manager
            .attach_participant_update_sizer(Arc::new(
                crate::participant_updates::LiveKitParticipantSizer,
            ))
            .expect("fresh manager accepts the actual protocol size provider");
        let local_dispatch = crate::local_dispatch::LocalDispatch::new(
            self.whip_policy,
            self.local_rpc_compression,
            self.local_rpc_max_decompressed,
            self.rpc.clone(),
        )
        .expect("validated local PSRPC configuration");
        #[cfg(test)]
        let lifecycle: Arc<dyn rtc_runtime::RoomOwnerLifecycle> =
            match self.test_lifecycle_decorator {
                Some(decorate) => decorate(local_dispatch.clone()),
                None => local_dispatch.clone(),
            };
        #[cfg(not(test))]
        let lifecycle = local_dispatch.clone();
        manager
            .attach_owner_lifecycle(lifecycle)
            .expect("fresh manager has no owner lifecycle");
        let agents = crate::agent::service::AgentService::new(
            manager.clone(),
            self.keys.clone(),
            self.tokens.clone(),
            crate::signal::server_info(&self.config),
            self.agent_config,
        )
        .expect("fresh factory manager has exactly one agent consumer");
        let whip = crate::whip::WhipService::new(manager.clone(), self.whip_policy);
        AppState {
            local_dispatch,
            room_creates: Arc::new(crate::room_create::LocalRoomCreate::default()),
            routing_events,
            room_control: None,
            cluster_signal: None,
            rpc_policy: self.whip_policy,
            signal_connect_attempts: self.signal_connect_attempts,
            whip,
            whip_rpc: None,
            worker_services: None,
            agents,
            manager,
            config: self.config,
            keys: self.keys,
            rpc: self.rpc,
            tokens: self.tokens,
            turn_auth: self.turn_auth,
            sip_catalog: self.sip_catalog,
            node: None,
            local_allocation: None,
        }
    }
}

impl AppState {
    /// Install the node record. Separate from `CompatServices` construction
    /// because upstream builds the node in `main` *before* the service graph and
    /// hands it in; the field exists on the shared state either way.
    pub fn enable_node(&mut self, node: Arc<crate::routing::LocalNode>) {
        self.node = Some(node);
    }

    /// Install the fixed single-node allocation consumer after the node exists.
    /// Redis routing uses its selected owner's policy, not this ingress node.
    pub fn enable_local_allocation_selector(
        &mut self,
        selector: crate::room_control::selector::Config,
    ) -> Result<(), crate::service_bus::rpc::RpcError> {
        let node = self.node.clone().ok_or_else(|| {
            crate::service_bus::rpc::RpcError::new(
                "failed_precondition",
                "local node must be installed before its allocation policy",
            )
        })?;
        self.local_allocation = Some(crate::local_allocation::LocalAllocation::new(
            node, selector,
        )?);
        Ok(())
    }

    /// Real LocalRouter allocation preflight shared by public API, RTC and WHIP.
    /// Standalone unbound component graphs have no node to select; product
    /// assembly explicitly binds this policy in single-node mode.
    pub(crate) async fn rtc_node_at_capacity(&self, room: &str) -> bool {
        match &self.room_control {
            Some(control) => control.signal_node_at_capacity(room).await,
            None => self
                .local_allocation
                .as_ref()
                .is_some_and(|policy| policy.at_capacity()),
        }
    }

    pub fn check_local_room_allocation(&self) -> Result<(), crate::service_bus::rpc::RpcError> {
        if self.room_control.is_some() {
            return Ok(());
        }
        match &self.local_allocation {
            Some(policy) => policy.check(),
            None => Ok(()),
        }
    }

    /// Explicit control-plane component assembly. This is not automatic product
    /// Redis/cluster activation: remote signal and WHIP owners are separate gates.
    pub async fn enable_room_control_component(
        &mut self,
        bus: Arc<crate::service_bus::redis_transport::RedisTransport>,
        config: crate::room_control::Config,
    ) -> Result<Arc<crate::room_control::RoomControl>, crate::service_bus::rpc::RpcError> {
        let control = crate::room_control::RoomControl::start(self.clone(), bus, config).await?;
        if let Some(workers) = &self.worker_services {
            workers.bind_room_control(&control)?;
        }
        self.room_control = Some(control.clone());
        Ok(control)
    }

    pub async fn enable_managed_room_control_component(
        &mut self,
        bus: Arc<crate::service_bus::redis_transport::RedisTransport>,
        config: crate::room_control::ManagedConfig,
        endpoint: crate::redis_endpoint::RedisEndpoint,
        node: Arc<crate::routing::LocalNode>,
    ) -> Result<Arc<crate::room_control::RoomControl>, crate::service_bus::rpc::RpcError> {
        let control = crate::room_control::RoomControl::start_managed(
            self.clone(),
            bus,
            config,
            endpoint,
            node,
        )
        .await?;
        if let Some(workers) = &self.worker_services {
            workers.bind_room_control(&control)?;
        }
        self.room_control = Some(control.clone());
        Ok(control)
    }
    pub async fn enable_cluster_signal(
        &mut self,
        bus: Arc<crate::service_bus::redis_transport::RedisTransport>,
        config: crate::cluster_signal::RelayConfig,
    ) -> Result<Arc<crate::cluster_signal::ClusterSignal>, crate::service_bus::rpc::RpcError> {
        if self.cluster_signal.is_some() {
            return Err(crate::service_bus::rpc::RpcError::new(
                "already_exists",
                "cluster signal already started",
            ));
        }
        let signal = crate::cluster_signal::ClusterSignal::start(self.clone(), bus, config).await?;
        self.signal_connect_attempts = config.connect_attempts;
        self.cluster_signal = Some(signal.clone());
        Ok(signal)
    }
    pub async fn shutdown_cluster_signal(&self) {
        if let Some(signal) = &self.cluster_signal {
            signal.shutdown().await;
        }
    }

    /// Activate worker listeners before exposing HTTP endpoints. Every broker
    /// subscription must acknowledge readiness before this returns.
    pub async fn enable_workers(
        &mut self,
        config: crate::worker_services::WorkerConfig,
        notifier: crate::webhook::WebhookNotifier,
    ) -> Result<(), crate::service_bus::rpc::RpcError> {
        if self.worker_services.is_some() {
            return Err(crate::service_bus::rpc::RpcError::new(
                "already_exists",
                "worker services already started",
            ));
        }
        let events = Arc::new(crate::worker_notifications::WorkerNotifications::new(
            notifier,
        ));
        let workers = crate::worker_services::WorkerServices::connect(
            config,
            self.manager.clone(),
            self.sip_catalog.as_deref().cloned(),
            events,
        )
        .await?;
        if let Some(control) = &self.room_control {
            workers.bind_room_control(control)?;
        }
        self.worker_services = Some(workers);
        Ok(())
    }

    /// Single-node activation: the same worker graph without a broker. Upstream
    /// still builds its message bus (`psrpc.NewLocalMessageBus`) and its egress
    /// launcher in this mode; only the stores are nil.
    pub async fn enable_local_workers(
        &mut self,
        policy: crate::worker_services::WorkerPolicy,
        notifier: Option<crate::webhook::WebhookNotifier>,
    ) -> Result<(), crate::service_bus::rpc::RpcError> {
        if self.worker_services.is_some() {
            return Err(crate::service_bus::rpc::RpcError::new(
                "already_exists",
                "worker services already started",
            ));
        }
        let events: Arc<dyn crate::workers::io_service::IoEvents> = match notifier {
            Some(notifier) => Arc::new(crate::worker_notifications::WorkerNotifications::new(
                notifier,
            )),
            None => Arc::new(crate::worker_services::NoRecordingNotifications),
        };
        let workers = crate::worker_services::WorkerServices::connect_local(
            policy,
            self.manager.clone(),
            self.sip_catalog.as_deref().cloned(),
            events,
        )
        .await?;
        if let Some(control) = &self.room_control {
            workers.bind_room_control(control)?;
        }
        self.worker_services = Some(workers);
        Ok(())
    }

    pub async fn enable_whip_rpc(
        &mut self,
        bus: crate::service_bus::rpc_transport::RpcBus,
        policy: crate::workers::retry::RpcPolicy,
    ) -> Result<Arc<crate::whip::cluster::WhipRpc>, crate::service_bus::rpc::RpcError> {
        if self.whip_rpc.is_some() {
            return Err(crate::service_bus::rpc::RpcError::new(
                "already_exists",
                "WHIP RPC already enabled",
            ));
        }
        let rpc = crate::whip::cluster::WhipRpc::start(self.clone(), bus, policy).await?;
        self.whip_rpc = Some(rpc.clone());
        Ok(rpc)
    }
    pub async fn shutdown_whip(&self) {
        if let Some(rpc) = &self.whip_rpc {
            rpc.stop_admission().await;
        }
        self.whip.shutdown().await;
        if let Some(rpc) = &self.whip_rpc {
            rpc.shutdown().await;
        }
    }

    pub async fn shutdown_workers(&self) {
        // Stop new/retrying CreateRoom clients, then cancel global automatic
        // workers before joining accepted owner work which may await them.
        self.room_creates.close_admission();
        self.local_dispatch.shutdown().await;
        if let Some(workers) = &self.worker_services {
            workers.shutdown().await;
        }
        self.room_creates.drain().await;
    }

    pub fn worker_message_bus(&self) -> Option<crate::service_bus::rpc_transport::RpcBus> {
        self.worker_services
            .as_ref()
            .map(|workers| workers.message_bus())
    }

    pub fn agents(&self) -> &Arc<crate::agent::service::AgentService> {
        &self.agents
    }

    pub fn manager(&self) -> &RoomManager {
        &self.manager
    }

    pub fn turn_auth(&self) -> &Arc<TurnAuth> {
        &self.turn_auth
    }

    pub fn node(&self) -> Option<&Arc<crate::routing::LocalNode>> {
        self.node.as_ref()
    }

    /// Standalone mode: every API key shares the local project (upstream OSS).
    pub fn room_key(&self, name: &str) -> RoomKey {
        RoomKey {
            project: ProjectId::local(),
            name: RoomName(name.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lk_protocol::livekit as pb;
    use prost::Message;
    use rtc_core::data::{DataHandler, DataSender, DataVerdict};
    use rtc_core::ids::{Identity, ParticipantSid};
    use rtc_core::media_api::DataKind;
    use rtc_core::model::ParticipantKind;
    use std::time::Duration;

    use crate::auth::{TokenClaims, authenticate};
    use crate::data::RELIABLE_LABEL;
    use crate::rpc::{DataChannelRpcError, RpcOutcome};

    fn services() -> CompatServices {
        CompatServices::new(Arc::new(crate::test_support::config()))
    }

    fn inbound(
        services: &CompatServices,
        sid: &ParticipantSid,
        value: pb::data_packet::Value,
    ) -> DataVerdict {
        let identity = Identity("client".into());
        let sender = DataSender {
            sid,
            identity: &identity,
            hidden: false,
            kind: ParticipantKind::Standard,
            is_agent: false,
        };
        services.data.inbound(
            &sender,
            DataKind::Reliable,
            RELIABLE_LABEL,
            &pb::DataPacket {
                value: Some(value),
                ..Default::default()
            }
            .encode_to_vec(),
        )
    }

    #[tokio::test]
    async fn factory_routes_encoded_ack_and_response_to_the_pending_call() {
        let services = services();
        let sid = ParticipantSid("PA_recipient".into());
        let (mut ack, response) = services.rpc.register("request", sid.clone()).unwrap();
        assert_eq!(
            inbound(
                &services,
                &sid,
                pb::data_packet::Value::RpcAck(pb::RpcAck {
                    request_id: "request".into(),
                })
            ),
            DataVerdict::Consumed
        );
        assert_eq!(ack.try_recv(), Ok(()));
        assert_eq!(services.rpc.pending_count(), 1);
        assert_eq!(
            inbound(
                &services,
                &sid,
                pb::data_packet::Value::RpcResponse(pb::RpcResponse {
                    request_id: "request".into(),
                    value: Some(pb::rpc_response::Value::Payload("echo:server".into())),
                })
            ),
            DataVerdict::Consumed
        );
        assert_eq!(
            response.await.unwrap(),
            RpcOutcome::Payload("echo:server".into())
        );
        assert_eq!(services.rpc.pending_count(), 0);
        assert!(matches!(
            inbound(
                &services,
                &sid,
                pb::data_packet::Value::RpcAck(pb::RpcAck {
                    request_id: "request".into(),
                })
            ),
            DataVerdict::Forward { .. }
        ));
    }

    #[tokio::test]
    async fn factory_rejects_wrong_sender_and_preserves_error_response() {
        let services = services();
        let sid = ParticipantSid("PA_recipient".into());
        let wrong = ParticipantSid("PA_other".into());
        let (mut ack, mut response) = services.rpc.register("request", sid.clone()).unwrap();
        let value = pb::data_packet::Value::RpcResponse(pb::RpcResponse {
            request_id: "request".into(),
            value: Some(pb::rpc_response::Value::Error(pb::RpcError {
                code: 1500,
                message: "handler failed".into(),
                data: "detail".into(),
            })),
        });
        assert!(matches!(
            inbound(&services, &wrong, value.clone()),
            DataVerdict::Forward { .. }
        ));
        assert!(matches!(
            inbound(
                &services,
                &wrong,
                pb::data_packet::Value::RpcAck(pb::RpcAck {
                    request_id: "request".into(),
                })
            ),
            DataVerdict::Forward { .. }
        ));
        assert!(matches!(
            ack.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        assert!(matches!(
            response.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        assert_eq!(services.rpc.pending_count(), 1);
        assert_eq!(inbound(&services, &sid, value), DataVerdict::Consumed);
        // A response can arrive before its ACK; it completes both waiters.
        ack.await.unwrap();
        assert_eq!(
            response.await.unwrap(),
            RpcOutcome::Error(DataChannelRpcError {
                code: 1500,
                message: "handler failed".into(),
                data: "detail".into(),
            })
        );
        assert_eq!(services.rpc.pending_count(), 0);
    }

    #[test]
    fn factory_keys_sign_tokens_and_authenticate_turn_consistently() {
        let services = services();
        let token = services
            .tokens
            .mint(
                "devkey",
                &TokenClaims {
                    identity: "client".into(),
                    ..Default::default()
                },
                Duration::from_secs(60),
                rtc_core::util::unix_s(),
            )
            .unwrap();
        let grants = authenticate(services.keys(), Some(&token))
            .unwrap()
            .unwrap();
        assert_eq!(grants.identity, "client");
        assert_eq!(grants.api_key, "devkey");
        let sid = ParticipantSid("PA_recipient".into());
        let (username, password) = services
            .turn_auth
            .create_credentials_at("devkey", &sid, Duration::from_secs(300), 100)
            .unwrap();
        let (_, key) = services
            .turn_auth
            .authenticate_at(
                &rtc_turn::AuthRequest {
                    username: &username,
                    realm: "livekit",
                    method: rtc_turn::Method::Allocate,
                    src: "127.0.0.1:9999".parse().unwrap(),
                },
                101,
            )
            .unwrap();
        assert_eq!(
            key,
            rtc_turn::long_term_key(&username, "livekit", &password)
        );
        assert!(
            services
                .turn_auth
                .create_credentials_at("unknown", &sid, Duration::from_secs(300), 100)
                .is_none()
        );
    }

    #[tokio::test]
    async fn assembled_state_and_its_clone_complete_the_factory_data_handlers_request() {
        use rtc_core::events::NullSink;
        use rtc_runtime::net::{LocalInterface, NetConfig};

        let services = services();
        let data = services.data.clone();
        let local = std::net::Ipv4Addr::LOCALHOST.into();
        let net = NetRouter::start(NetConfig {
            udp_enabled: false,
            udp_port: None,
            port_range: (0, 0),
            tcp_port: None,
            interfaces: vec![LocalInterface {
                bind: local,
                advertise: local,
            }],
        })
        .await
        .unwrap();
        let state = services.assemble(net.clone(), Arc::new(NullSink));
        let copy = state.clone();
        let sid = ParticipantSid("PA_recipient".into());
        let (ack, response) = state.rpc.register("assembled", sid.clone()).unwrap();
        let identity = Identity("client".into());
        let sender = DataSender {
            sid: &sid,
            identity: &identity,
            hidden: false,
            kind: ParticipantKind::Standard,
            is_agent: false,
        };
        let packet = pb::DataPacket {
            value: Some(pb::data_packet::Value::RpcResponse(pb::RpcResponse {
                request_id: "assembled".into(),
                value: Some(pb::rpc_response::Value::Payload("assembled reply".into())),
            })),
            ..Default::default()
        }
        .encode_to_vec();
        assert_eq!(
            data.inbound(&sender, DataKind::Reliable, RELIABLE_LABEL, &packet),
            DataVerdict::Consumed
        );
        ack.await.unwrap();
        assert_eq!(
            response.await.unwrap(),
            RpcOutcome::Payload("assembled reply".into())
        );
        assert_eq!(copy.rpc.pending_count(), 0);
        copy.manager().close_all().await;
        net.shutdown();
    }
}
