//! Owned external-worker service graph. Construction waits for real subscription
//! acknowledgements; only actual workers can supply successful media results.
use crate::redis_endpoint::RedisEndpoint;
use crate::{
    service_bus::{
        local_transport::LocalTransport,
        redis_transport::{RedisTransport, TransportLimits},
        rpc::{CallOptions, RpcClient, RpcError, ServerLimits},
        rpc_transport::RpcBus,
        wire::{CompressionPolicy, WireLimits},
    },
    sip::catalog::SipCatalog,
    workers::{
        egress::{EgressGateway, EgressRooms},
        ingress::{
            FixedGoUrl, FixedGuid, IngressAdapters, IngressConfig, IngressDeletionEvents,
            IngressGateway,
        },
        io_service::{IoEvents, IoInfo, IoRunning},
        retry::RpcPolicy,
        sip_callbacks::CatalogSipCallbacks,
        sip_gateway::{SipGateway, SipRooms},
        store::IoStore,
    },
};
use futures_util::future::BoxFuture;
use rtc_core::ids::{Identity, ProjectId, RoomKey, RoomName};
use rtc_runtime::{ApiError, ParticipantUpdate, RoomManager};
use std::{sync::Arc, time::Duration};
use tokio::sync::Mutex;

/// The PSRPC/source projection a single-node server also needs.
///
/// Upstream builds the message bus from the same client params either way
/// (`getMessageBus`: `psrpc.NewLocalMessageBus(opts...)` when the Redis client
/// is nil, `redisbus.New` otherwise), so the policy half is Redis-independent.
pub struct WorkerPolicy {
    pub rpc: RpcPolicy,
    pub compression: CompressionPolicy,
    pub max_decompressed_size: usize,
    pub ingress: IngressConfig,
    pub ingress_legacy_psrpc: bool,
}

/// Source policy projected by the executable, without a dependency on CLI types.
pub struct WorkerConfig {
    pub redis: RedisEndpoint,
    pub rpc: RpcPolicy,
    pub compression: CompressionPolicy,
    pub max_decompressed_size: usize,
    pub ingress: IngressConfig,
    pub ingress_legacy_psrpc: bool,
}

impl WorkerConfig {
    /// Split off the broker-independent half so the same policy can drive the
    /// process-local bus.
    pub fn policy(&self) -> WorkerPolicy {
        WorkerPolicy {
            rpc: self.rpc,
            compression: self.compression,
            max_decompressed_size: self.max_decompressed_size,
            ingress: self.ingress.clone(),
            ingress_legacy_psrpc: self.ingress_legacy_psrpc,
        }
    }
}

mod room_authority;

pub struct WorkerServices {
    rooms: Arc<LocalWorkerRooms>,
    pub egress: EgressGateway,
    pub sip: SipGateway,
    pub ingress: IngressGateway,
    pub io: Arc<IoInfo>,
    client: Arc<RpcClient>,
    start_client: Arc<RpcClient>,
    sip_client: Arc<RpcClient>,
    recording_cancel: crate::service_bus::rpc::Cancellation,
    running: Mutex<Option<IoRunning>>,
}
impl WorkerServices {
    pub(crate) fn bind_room_control(
        &self,
        control: &Arc<crate::room_control::RoomControl>,
    ) -> Result<(), RpcError> {
        self.rooms.authority.bind(control)
    }
    pub fn message_bus(&self) -> RpcBus {
        self.client.message_bus()
    }
    /// Real PSRPC transport shared with actual IngressHandler callbacks.
    pub(crate) fn ingress_notify_client(&self) -> Arc<RpcClient> {
        self.client.clone()
    }
    /// Single-node shape: no Redis, so the same graph runs on the process-local
    /// PSRPC bus and every store handle is nil. Upstream keeps the egress
    /// launcher and IOInfo handlers in this mode too (`getMessageBus` returns
    /// `psrpc.NewLocalMessageBus`), which is why a start request without a
    /// worker waits out the retry schedule instead of failing up front.
    pub async fn connect_local(
        policy: WorkerPolicy,
        manager: RoomManager,
        catalog: Option<SipCatalog>,
        events: Arc<dyn IoEvents>,
    ) -> Result<Arc<WorkerServices>, RpcError> {
        Self::build(None, policy, manager, catalog, events).await
    }
    /// Broker-backed shape; same graph, real subscriptions.
    pub async fn connect(
        config: WorkerConfig,
        manager: RoomManager,
        catalog: Option<SipCatalog>,
        events: Arc<dyn IoEvents>,
    ) -> Result<Arc<WorkerServices>, RpcError> {
        let policy = config.policy();
        Self::build(Some(config.redis), policy, manager, catalog, events).await
    }
    async fn build(
        redis: Option<RedisEndpoint>,
        policy: WorkerPolicy,
        manager: RoomManager,
        catalog: Option<SipCatalog>,
        events: Arc<dyn IoEvents>,
    ) -> Result<Arc<Self>, RpcError> {
        let config = policy;
        // An explicit old-worker profile rejects an incompatible global wire
        // setting; it never silently disables compression for another service.
        if config.ingress_legacy_psrpc && config.compression.quality > 0 {
            return Err(RpcError::new(
                "invalid_argument",
                "livecloud.ingress_worker_protocol=psrpc-v0.7.1 requires psrpc.compression.quality=0; positive gzip with this legacy worker remains unsupported",
            ));
        }
        // The source generated clients have distinct buffer policies.
        // EgressInternal overrides to 1000 and the generic handlers use config.
        let handler_buffer = config
            .rpc
            .handler_channel_size()
            .map_err(|e| RpcError::new("invalid_argument", e))?;
        let timeout = Duration::from_secs(5);
        // `getEgressStore`/`getIngressStore`/`getSIPStore` only accept the Redis
        // store; with a local bus the handle is nil, which every gateway turns
        // into its own "not connected" error.
        let store = match redis.as_ref() {
            Some(endpoint) => {
                let store = Arc::new(IoStore::connect(endpoint.clone(), timeout).await?);
                store.start("1.13.10").await?;
                Some(store)
            }
            None => None,
        };
        let limits = |inbox_messages| TransportLimits {
            wire: WireLimits {
                encoded_bytes: isize::MAX as usize - 1,
                decompressed_bytes: config.max_decompressed_size,
            },
            compression: config.compression,
            inbox_messages,
            subscriptions: 4096,
            commands: 1000,
            io_timeout: timeout,
        };
        // Source localbus has no global subscription/command or handler-duration
        // ceiling; the broker half keeps the configured ones.
        let local_limits = |inbox_messages| TransportLimits {
            wire: WireLimits {
                encoded_bytes: usize::MAX,
                decompressed_bytes: isize::MAX as usize - 1,
            },
            compression: config.compression,
            inbox_messages,
            subscriptions: tokio::sync::Semaphore::MAX_PERMITS,
            commands: tokio::sync::Semaphore::MAX_PERMITS,
            io_timeout: Duration::MAX,
        };
        // 三个客户端视图共享一个本地成员表，压缩解码上限仍消费配置。
        let local = if redis.is_none() {
            Some(LocalTransport::new_with_gzip_limit(
                local_limits(100),
                config.max_decompressed_size,
            )?)
        } else {
            None
        };
        macro_rules! bus {
            ($inbox:expr) => {{
                match redis.as_ref() {
                    Some(endpoint) => RpcBus::from(
                        RedisTransport::connect_endpoint(endpoint.clone(), limits($inbox)).await?,
                    ),
                    None => RpcBus::from(
                        local
                            .as_ref()
                            .expect("本地总线已构造")
                            .with_inbox_size($inbox)?,
                    ),
                }
            }};
        }
        let server_bus = bus!(100);
        let handler_bus = bus!(handler_buffer);
        let start_bus = bus!(1000);
        // Fixed service/wire.go newSIPClient omits the generic PSRPC
        // policy. Keep the native 100-message client channel and independent
        // client identity without opening a second Redis command pool.
        let sip_bus = handler_bus.with_inbox_size(100)?;
        let client = Arc::new(
            RpcClient::new(handler_bus, tokio::sync::Semaphore::MAX_PERMITS)?.with_tracing(
                crate::service_bus::telemetry::RpcTracing::production_client_params(),
            ),
        );
        let start_client = Arc::new(
            RpcClient::new(start_bus, tokio::sync::Semaphore::MAX_PERMITS)?.with_tracing(
                crate::service_bus::telemetry::RpcTracing::production_client_params(),
            ),
        );
        let sip_client = Arc::new(
            RpcClient::new(sip_bus, tokio::sync::Semaphore::MAX_PERMITS)?.with_tracing(
                crate::service_bus::telemetry::RpcTracing::production_client_params(),
            ),
        );
        let sip_catalog = catalog.clone();
        let io = IoInfo::new(
            store.clone(),
            events.clone(),
            Arc::new(CatalogSipCallbacks::new(catalog)),
        );
        let running = io
            .register(
                server_bus,
                ServerLimits {
                    concurrent: tokio::sync::Semaphore::MAX_PERMITS,
                    max_request_duration: Duration::MAX,
                    allow_skip_claim: false,
                },
            )
            .await?;
        let ingress = IngressGateway::new(
            config.ingress,
            IngressAdapters {
                store,
                client: Some(client.clone()),
                io: io.clone(),
                events: Arc::new(IngressDeletionEvents(events.clone())),
                urls: Arc::new(FixedGoUrl),
                ids: Arc::new(FixedGuid),
            },
            config.rpc,
        );
        let rooms = Arc::new(LocalWorkerRooms::new(manager.clone()));
        let sip = SipGateway::new(Some(sip_client.clone()), sip_catalog, rooms.clone());
        let egress = EgressGateway::new(
            Some(client.clone()),
            io.clone(),
            rooms.clone(),
            Duration::from_secs(3),
        )
        .with_start_client(Some(start_client.clone()))
        .with_rpc_policy(config.rpc);
        let services = Arc::new(Self {
            rooms,
            egress,
            sip,
            ingress,
            io,
            client,
            start_client,
            sip_client,
            recording_cancel: Default::default(),
            running: Mutex::new(Some(running)),
        });
        if let Err(error) = manager.attach_recording_launcher(Arc::new(AutomaticRecordingAdapter {
            services: Arc::downgrade(&services),
            events: events.clone(),
        })) {
            services.shutdown().await;
            return Err(RpcError::new("failed_precondition", error.to_string()));
        }

        Ok(services)
    }
    pub fn call_options(&self) -> CallOptions {
        CallOptions::new(Duration::from_secs(3))
    }
    pub(crate) fn automatic_options(&self) -> CallOptions {
        let mut options = self.call_options();
        options.cancel = self.recording_cancel.clone();
        options
    }
    pub async fn shutdown(&self) {
        // Wake real RPC waits AND source retry backoff sleeps before joining.
        self.recording_cancel.cancel();
        self.client.close();
        self.start_client.close();
        self.sip_client.close();
        if let Some(running) = self.running.lock().await.take() {
            running.shutdown().await;
        }
        self.client.close_and_wait().await;
        self.start_client.close_and_wait().await;
        self.sip_client.close_and_wait().await;
    }
}
struct LocalWorkerRooms {
    authority: room_authority::RoomAuthority,
    manager: RoomManager,
    max_participant_name_length: i64,
    max_metadata_size: Option<usize>,
}
impl LocalWorkerRooms {
    fn new(manager: RoomManager) -> Self {
        let max_metadata_size = manager.config().limits.max_metadata_size;
        let max_participant_name_length = manager.config().limits.max_participant_name_length;
        Self {
            authority: Default::default(),
            manager,
            max_participant_name_length,
            max_metadata_size,
        }
    }
}
fn check_layout_metadata(limit: Option<usize>, metadata: &str) -> Result<(), RpcError> {
    if let Some(limit) = limit
        && limit != 0
        && metadata.len() > limit
    {
        // Match RoomService.UpdateParticipant's InvalidArgumentError argument
        // and validation message, after Go-compatible JSON was produced.
        return Err(RpcError::new(
            "invalid_argument",
            format!("metadata size exceeds limits {limit}"),
        )
        .with_application_metadata("argument", "metadata size exceeds limits"));
    }
    Ok(())
}
fn key(name: &str) -> RoomKey {
    RoomKey {
        project: ProjectId::local(),
        name: RoomName(name.into()),
    }
}
fn room_error(error: ApiError) -> RpcError {
    let code = match &error {
        ApiError::MetadataLimit(_) => "unknown",
        ApiError::NotFound(_) => "not_found",
        ApiError::Unavailable(_) => "unavailable",
        ApiError::FailedPrecondition(_) => "failed_precondition",
        ApiError::InvalidArgument(_) => "invalid_argument",
        ApiError::Internal(_) => "internal",
    };
    RpcError::new(code, error.to_string())
}
impl EgressRooms for LocalWorkerRooms {
    fn room_sid(&self, name: &str) -> BoxFuture<'_, Result<String, RpcError>> {
        let name = name.to_owned();
        Box::pin(async move {
            if let Some(control) = self.authority.get()? {
                return control.worker_room_sid(&name).await;
            }
            self.manager
                .list_rooms(&ProjectId::local(), &[RoomName(name)])
                .first()
                .map(|r| r.sid.0.clone())
                .ok_or_else(|| RpcError::new("not_found", "requested room does not exist"))
        })
    }
    fn layout_metadata(
        &self,
        room: &str,
        identity: &str,
        metadata: String,
    ) -> BoxFuture<'_, Result<(), RpcError>> {
        let room = key(room);
        let identity = Identity(identity.into());
        Box::pin(async move {
            // Source Egress.UpdateLayout invokes the full public room-service
            // UpdateParticipant with empty Name after GetEgress succeeds.
            crate::twirp::room_service::check_participant_name_limit(
                self.max_participant_name_length,
                "",
            )
            .map_err(|error| {
                let mut result = RpcError::new(error.code.as_str(), error.msg);
                result.application_metadata = error.meta;
                result
            })?;
            check_layout_metadata(self.max_metadata_size, &metadata)?;
            if let Some(control) = self.authority.get()? {
                return control
                    .update_participant(lk_protocol::livekit::UpdateParticipantRequest {
                        room: room.name.0,
                        identity: identity.0,
                        metadata,
                        ..Default::default()
                    })
                    .await
                    .map(|_| ());
            }
            self.manager
                .update_participant(
                    &room,
                    &identity,
                    ParticipantUpdate {
                        metadata,
                        ..Default::default()
                    },
                )
                .await
                .map(|_| ())
                .map_err(room_error)
        })
    }
}

impl SipRooms for LocalWorkerRooms {
    fn participant(
        &self,
        room: &str,
        identity: &str,
    ) -> BoxFuture<'_, Result<lk_protocol::livekit::ParticipantInfo, RpcError>> {
        let manager = self.manager.clone();
        let room = key(room);
        let identity = Identity(identity.into());
        Box::pin(async move {
            if let Some(control) = self.authority.get()? {
                return control
                    .get_participant(lk_protocol::livekit::RoomParticipantIdentity {
                        room: room.name.0,
                        identity: identity.0,
                        ..Default::default()
                    })
                    .await;
            }
            manager
                .get_participant(&room, &identity)
                .await
                .map(|value| crate::convert::participant_info_to_proto(&value))
                .map_err(room_error)
        })
    }
}

/// Source-local room lookup still runs in its original order without a broker.
pub(crate) fn local_sip_gateway(manager: RoomManager, catalog: Option<SipCatalog>) -> SipGateway {
    SipGateway::new(None, catalog, Arc::new(LocalWorkerRooms::new(manager)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn actual_layout_adapter_empty_name_preflight_precedes_metadata_and_missing_identity() {
        let mut config = crate::test_support::config();
        config.limits.max_participant_name_length = -1;
        config.limits.max_metadata_size = Some(3);
        let net = rtc_runtime::net::NetRouter::start(rtc_runtime::net::NetConfig {
            udp_enabled: false,
            udp_port: None,
            port_range: (0, 0),
            tcp_port: None,
            interfaces: vec![rtc_runtime::net::LocalInterface {
                bind: std::net::Ipv4Addr::LOCALHOST.into(),
                advertise: std::net::Ipv4Addr::LOCALHOST.into(),
            }],
        })
        .await
        .unwrap();
        let state = crate::CompatServices::new(Arc::new(config))
            .assemble(net.clone(), Arc::new(rtc_core::events::NullSink));
        let rooms = LocalWorkerRooms::new(state.manager().clone());
        let error = rooms
            .layout_metadata("missing", "EG_missing", "éé".into())
            .await
            .unwrap_err();
        state.manager.close_all().await;
        state.agents().shutdown().await;
        net.shutdown_and_wait().await;
        assert_eq!(error.code, "invalid_argument");
        assert_eq!(error.message, "name length exceeds limits -1");
        assert_eq!(
            error.application_metadata["argument"],
            "name length exceeds limits"
        );
    }
    #[test]
    fn layout_metadata_limit_matches_room_service_byte_boundary_and_argument() {
        assert!(check_layout_metadata(None, &"x".repeat(100)).is_ok());
        assert!(check_layout_metadata(Some(0), &"x".repeat(100)).is_ok());
        assert!(check_layout_metadata(Some(4), "éé").is_ok());
        let error = check_layout_metadata(Some(3), "éé").unwrap_err();
        assert_eq!(error.code, "invalid_argument");
        assert_eq!(error.message, "metadata size exceeds limits 3");
        assert_eq!(
            error
                .application_metadata
                .get("argument")
                .map(String::as_str),
            Some("metadata size exceeds limits")
        );
    }
    #[tokio::test]
    async fn actual_local_egress_room_adapter_checks_limit_before_participant_lookup() {
        let mut config = crate::test_support::config();
        config.limits.max_metadata_size = Some(3);
        let net = rtc_runtime::net::NetRouter::start(rtc_runtime::net::NetConfig {
            udp_enabled: false,
            udp_port: None,
            port_range: (0, 0),
            tcp_port: None,
            interfaces: vec![rtc_runtime::net::LocalInterface {
                bind: std::net::Ipv4Addr::LOCALHOST.into(),
                advertise: std::net::Ipv4Addr::LOCALHOST.into(),
            }],
        })
        .await
        .unwrap();
        let state = crate::CompatServices::new(Arc::new(config))
            .assemble(net.clone(), Arc::new(rtc_core::events::NullSink));
        let rooms = LocalWorkerRooms::new(state.manager().clone());
        assert_eq!(rooms.max_metadata_size, Some(3));
        let error = rooms
            .layout_metadata("missing", "EG_missing", "éé".into())
            .await
            .unwrap_err();
        assert_eq!(error.code, "invalid_argument");
        assert_eq!(
            error.application_metadata["argument"],
            "metadata size exceeds limits"
        );
        let missing = rooms.room_sid("missing").await.unwrap_err();
        assert_eq!(missing.code, "not_found");
        assert_eq!(missing.message, "requested room does not exist");
        state.agents().shutdown().await;
        net.shutdown();
    }
}

/// SPEC-0074 A10: the single-node shape has to stay exactly as observable as the
/// original — a real local bus (start requests wait out their schedule) and nil
/// store handles (list/update keep their "not connected" errors).
#[cfg(test)]
mod local_bus_tests {
    use super::*;
    use crate::auth::VerifiedToken;
    use crate::auth::claims::SipGrant;
    use crate::service_bus::rpc::{MethodSpec, RawRequest, RpcServer};
    use crate::workers::preserved::WireValue;
    use lk_protocol::{livekit as pb, rpc as internal};
    use rtc_runtime::net::{LocalInterface, NetConfig, NetRouter};

    fn policy() -> WorkerPolicy {
        WorkerPolicy {
            rpc: RpcPolicy::default(),
            compression: CompressionPolicy {
                quality: 0,
                threshold: 1024,
            },
            max_decompressed_size: isize::MAX as usize - 1,
            ingress: IngressConfig::default(),
            ingress_legacy_psrpc: false,
        }
    }

    fn token() -> VerifiedToken {
        let mut token = VerifiedToken::default();
        token.grants.api_key = "devkey".into();
        token.grants.video.room_record = true;
        token.grants.video.ingress_admin = true;
        token.extra.sip = SipGrant {
            admin: true,
            call: true,
        };
        token
    }

    async fn local_state() -> (crate::state::AppState, NetRouter) {
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
        let state = crate::CompatServices::new(Arc::new(crate::test_support::config()))
            .assemble(net.clone(), Arc::new(rtc_core::events::NullSink));
        (state, net)
    }

    async fn shutdown(state: crate::state::AppState, net: NetRouter) {
        state.shutdown_workers().await;
        state.agents().shutdown().await;
        net.shutdown_and_wait().await;
    }

    /// A real process-local bus, exactly the transport `build` wires in
    /// single-node mode (no broker ceiling on subscriptions or duration).
    fn local_bus() -> Arc<LocalTransport> {
        LocalTransport::new(TransportLimits {
            wire: WireLimits {
                encoded_bytes: 1 << 20,
                decompressed_bytes: 1 << 20,
            },
            compression: CompressionPolicy {
                quality: 0,
                threshold: 1024,
            },
            inbox_messages: 8,
            subscriptions: 100,
            commands: 100,
            io_timeout: Duration::from_secs(5),
        })
        .unwrap()
    }

    struct NoRooms;
    impl EgressRooms for NoRooms {
        fn room_sid(&self, room: &str) -> BoxFuture<'_, Result<String, RpcError>> {
            let room = room.to_string();
            Box::pin(async move { Err(RpcError::new("not_found", room)) })
        }
        fn layout_metadata(
            &self,
            _: &str,
            _: &str,
            _: String,
        ) -> BoxFuture<'_, Result<(), RpcError>> {
            Box::pin(async { Ok(()) })
        }
    }

    #[tokio::test]
    async fn single_node_graph_keeps_nil_store_handles_and_their_not_connected_errors() {
        let (mut state, net) = local_state().await;
        state.enable_local_workers(policy(), None).await.unwrap();
        let services = state.worker_services.clone().expect("local worker graph");
        let token = token();
        let egress = services
            .egress
            .list_wire(Some(&token), pb::ListEgressRequest::default())
            .await
            .unwrap_err();
        assert_eq!(egress.code, "internal");
        assert_eq!(egress.message, "egress not connected (redis required)");
        let ingress = services
            .ingress
            .list(Some(&token), pb::ListIngressRequest::default())
            .await
            .unwrap_err();
        assert_eq!(ingress.code, "internal");
        assert_eq!(ingress.message, "ingress not connected (redis required)");
        let sip = services
            .sip
            .create_wire(
                Some(&token),
                WireValue::from_message(pb::CreateSipParticipantRequest {
                    sip_trunk_id: "ST_x".into(),
                    sip_call_to: "+1555".into(),
                    room_name: "r1".into(),
                    ..Default::default()
                }),
                CallOptions::new(Duration::from_secs(3)),
            )
            .await
            .unwrap_err();
        assert_eq!(sip.code, "internal");
        assert_eq!(sip.message, "sip not connected (redis required)");
        // Upstream starts the graph once; a second activation is rejected.
        let duplicate = state
            .enable_local_workers(policy(), None)
            .await
            .unwrap_err();
        assert_eq!(duplicate.code, "already_exists");
        shutdown(state, net).await;
    }

    #[tokio::test]
    async fn single_node_clients_reach_the_registered_io_graph() {
        let (mut state, net) = local_state().await;
        let mut configured = policy();
        configured.rpc = RpcPolicy::from_raw(1, 3_000_000_000, 0, 7);
        state.enable_local_workers(configured, None).await.unwrap();
        let services = state.worker_services.clone().unwrap();
        let spec = MethodSpec {
            service: "IOInfo".into(),
            method: "GetEgress".into(),
            topics: vec![],
            require_claim: true,
            queue: true,
        };
        for client in [&services.client, &services.start_client] {
            let mut options = CallOptions::new(Duration::from_millis(300));
            options.selection.accept_first = true;
            let error = client
                .unary::<_, pb::EgressInfo>(
                    &spec,
                    &internal::GetEgressRequest {
                        egress_id: "EG_absent".into(),
                    },
                    options,
                )
                .await
                .unwrap_err();
            assert!(
                error.is_remote(),
                "客户端必须收到实际 IOInfo handler 的响应：{error:?}"
            );
            assert_eq!(error.message, "egress not connected (redis required)");
        }
        shutdown(state, net).await;
    }

    #[tokio::test]
    async fn single_node_gzip_limit_is_consumed_by_real_io_subscriptions() {
        // 上限只约束 gzip；同样的大消息未压缩时仍能到达真实 handler。
        for quality in [0, 1] {
            let (mut state, net) = local_state().await;
            let mut configured = policy();
            configured.compression = CompressionPolicy {
                quality,
                threshold: 1,
            };
            configured.max_decompressed_size = 128;
            state.enable_local_workers(configured, None).await.unwrap();
            let services = state.worker_services.clone().unwrap();
            let spec = MethodSpec {
                service: "IOInfo".into(),
                method: "GetEgress".into(),
                topics: vec![],
                require_claim: true,
                queue: true,
            };
            let mut options = CallOptions::new(Duration::from_millis(300));
            options.selection.accept_first = true;
            let error = services
                .client
                .unary::<_, pb::EgressInfo>(
                    &spec,
                    &internal::GetEgressRequest {
                        egress_id: "x".repeat(1024),
                    },
                    options,
                )
                .await
                .unwrap_err();
            if quality == 0 {
                assert!(error.is_remote(), "未压缩消息应到达 handler：{error:?}");
            } else {
                assert!(
                    error.is_local_no_response(),
                    "超限 gzip 不应到达 handler：{error:?}"
                );
            }
            shutdown(state, net).await;
        }
    }

    /// The decisive A10 difference against the old implementation: a start
    /// request must reach the bus and wait, never fail up front with the
    /// store-derived "not connected" error. The caller deadline (below the 10s
    /// source base timeout) is what bounds this call.
    #[tokio::test]
    async fn single_node_start_waits_on_the_local_bus_instead_of_reporting_not_connected() {
        let (mut state, net) = local_state().await;
        state.enable_local_workers(policy(), None).await.unwrap();
        let services = state.worker_services.clone().expect("local worker graph");
        let mut options = CallOptions::new(Duration::from_secs(5));
        options.deadline = Some(std::time::Instant::now() + Duration::from_millis(1_500));
        let started = std::time::Instant::now();
        let start = WireValue::from_message(internal::StartEgressRequest {
            room_id: "RM_local_bus".into(),
            ..Default::default()
        });
        let error = services
            .egress
            .start_owned_wire(start, options)
            .await
            .unwrap_err();
        let elapsed = started.elapsed();
        assert_ne!(error.message, "egress not connected (redis required)");
        assert_eq!(error.code, "deadline_exceeded");
        assert_eq!(error.message, "request timed out");
        assert!(
            elapsed >= Duration::from_millis(1_400),
            "the request must wait on the bus, not fail immediately: {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_secs(9),
            "the caller deadline must cap the 10s source base timeout: {elapsed:?}"
        );
        shutdown(state, net).await;
    }

    /// End-to-end over the same transport the single-node graph uses: the
    /// request is published as a real `EgressInternal/StartEgress` call and the
    /// reply is the registered handler's, not an invented one.
    #[tokio::test]
    async fn local_bus_start_round_trips_a_registered_egress_handler() {
        let local = local_bus();
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = seen.clone();
        let server = RpcServer::register_with_raw(
            local.clone(),
            MethodSpec {
                service: "EgressInternal".into(),
                method: "StartEgress".into(),
                topics: vec![String::new()],
                require_claim: true,
                queue: false,
            },
            ServerLimits {
                concurrent: 4,
                // Single-node handlers have no duration ceiling (`build` uses
                // `Duration::MAX`); the 10s source base timeout must be legal.
                max_request_duration: Duration::MAX,
                allow_skip_claim: false,
            },
            move |_, request: RawRequest<internal::StartEgressRequest>| {
                let sink = sink.clone();
                async move {
                    sink.lock().unwrap().push(request.message.clone());
                    Ok(pb::EgressInfo {
                        egress_id: request.message.egress_id.clone(),
                        status: pb::EgressStatus::EgressStarting as i32,
                        ..Default::default()
                    })
                }
            },
            None,
        )
        .await
        .unwrap();
        let io = IoInfo::new(
            None,
            Arc::new(NoRecordingNotifications),
            Arc::new(CatalogSipCallbacks::new(None)),
        );
        let client = Arc::new(RpcClient::new(local.clone(), 8).unwrap());
        let gateway = EgressGateway::new(
            Some(client.clone()),
            io,
            Arc::new(NoRooms),
            Duration::from_secs(3),
        )
        .with_start_client(Some(client.clone()))
        .with_rpc_policy(RpcPolicy::default());
        let info = gateway
            .start_owned_wire(
                WireValue::from_message(internal::StartEgressRequest {
                    room_id: "RM_local_bus".into(),
                    ..Default::default()
                }),
                CallOptions::new(Duration::from_secs(10)),
            )
            .await
            .unwrap();
        let sent = {
            let seen = seen.lock().unwrap();
            seen.first().cloned().expect("request reached the handler")
        };
        assert_eq!(sent.room_id, "RM_local_bus");
        assert!(sent.egress_id.starts_with("EG_"));
        assert_eq!(info.message.egress_id, sent.egress_id);
        assert_eq!(info.message.status, pb::EgressStatus::EgressStarting as i32);
        server.shutdown().await;
        client.close();
    }
}

/// Weak edge avoids manager -> launcher -> services -> manager retention cycle.
struct AutomaticRecordingAdapter {
    services: std::sync::Weak<WorkerServices>,
    events: Arc<dyn IoEvents>,
}
impl rtc_runtime::automatic_recording::RecordingLauncher for AutomaticRecordingAdapter {
    fn launch_room(
        &self,
        room: rtc_core::model::RoomInfo,
        policy: rtc_core::auto_egress::RoomCompositeRecordingPolicy,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'static>>
    {
        let services = self.services.clone();
        Box::pin(async move {
            let services = services
                .upgrade()
                .ok_or_else(|| "egress not connected (redis required)".to_string())?;
            let mut request = crate::workers::auto_egress_policy::composite_out(&policy);
            request.room_name = room.name.0;
            let mut start = crate::workers::egress::wrap_room_wire(
                crate::workers::preserved::WireValue::from_message(request),
                String::new(),
            );
            start.message.room_id = room.sid.0;
            services
                .egress
                .start_owned_wire(start, services.automatic_options())
                .await
                .map(|_| ())
                .map_err(|error| error.message)
        })
    }
    fn launch(
        &self,
        room: rtc_core::model::RoomInfo,
        action: rtc_core::auto_egress::RecordingLaunch,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'static>>
    {
        let services = self.services.clone();
        let events = self.events.clone();
        Box::pin(async move {
            let (mut start, mut failure) = automatic_recording_request(&room, action)?;
            start.message.room_id = room.sid.0.clone();
            let result = if let Some(services) = services.upgrade() {
                services
                    .egress
                    .start_owned_wire(start, services.automatic_options())
                    .await
                    .map(|_| ())
                    .map_err(|error| error.message)
            } else {
                Err("egress launcher not found".to_string())
            };
            if let Err(error) = &result {
                failure.error = error.clone();
                events.notify(crate::workers::io_service::IoEvent::EgressEnded(Box::new(
                    failure,
                )));
            }
            result
        })
    }
}

fn automatic_recording_request(
    room: &rtc_core::model::RoomInfo,
    action: rtc_core::auto_egress::RecordingLaunch,
) -> Result<
    (
        crate::workers::preserved::WireValue<lk_protocol::rpc::StartEgressRequest>,
        lk_protocol::livekit::EgressInfo,
    ),
    String,
> {
    use crate::workers::{auto_egress_policy as policy, egress, preserved::WireValue};
    use lk_protocol::livekit as pb;
    use rtc_core::auto_egress::RecordingLaunch;
    let (start, request) = match action {
        RecordingLaunch::Participant {
            room_sid,
            identity,
            policy: options,
            ..
        } => {
            if room_sid != room.sid {
                return Err("automatic recording room generation mismatch".into());
            }
            let request = policy::participant_request(&options, &room.name.0, &identity.0);
            (
                egress::wrap_participant_wire(
                    WireValue::from_message(request.clone()),
                    String::new(),
                ),
                pb::egress_info::Request::Participant(request),
            )
        }
        RecordingLaunch::Track {
            room_sid,
            track_sid,
            policy: options,
            ..
        } => {
            if room_sid != room.sid {
                return Err("automatic recording room generation mismatch".into());
            }
            let request = policy::track_request(&options, &room.name.0, &track_sid.0);
            (
                egress::wrap_track_wire(WireValue::from_message(request.clone()), String::new()),
                pb::egress_info::Request::Track(request),
            )
        }
    };
    let failure = pb::EgressInfo {
        room_id: room.sid.0.clone(),
        room_name: room.name.0.clone(),
        status: pb::EgressStatus::EgressFailed as i32,
        request: Some(request),
        ..Default::default()
    };
    Ok((start, failure))
}

#[cfg(test)]
mod automatic_recording_adapter_tests {
    use super::*;
    use rtc_core::{
        auto_egress::*,
        model::{RoomInfo, TimedVersion},
    };
    fn room() -> RoomInfo {
        RoomInfo {
            turn_password: String::new(),
            sid: "RM_real_owner".into(),
            name: "actual-room".into(),
            empty_timeout_s: 0,
            departure_timeout_s: 0,
            max_participants: 0,
            creation_time_ms: 0,
            metadata: String::new(),
            num_participants: 0,
            num_publishers: 0,
            active_recording: false,
            enabled_codecs: vec![],
            version: TimedVersion {
                unix_micro: 0,
                ticks: 0,
            },
        }
    }
    #[test]
    fn failed_launch_shape_has_actual_request_and_no_invented_worker_identity_or_completion() {
        let room = room();
        let action = RecordingLaunch::Track {
            room_sid: room.sid.clone(),
            participant_sid: "PA_publisher".into(),
            identity: "human".into(),
            track_sid: "TR_source".into(),
            policy: TrackRecordingPolicy {
                filepath: "dir.with.dot/file.ogg".into(),
                disable_manifest: true,
                ..Default::default()
            },
        };
        let (start, failure) = automatic_recording_request(&room, action).unwrap();
        assert!(start.message.egress_id.is_empty());
        assert_eq!(failure.room_id, room.sid.0);
        assert_eq!(failure.room_name, room.name.0);
        assert_eq!(failure.status, 4);
        assert!(failure.egress_id.is_empty());
        assert_eq!(failure.error_code, 0);
        assert_eq!(failure.started_at, 0);
        assert_eq!(failure.ended_at, 0);
        let Some(lk_protocol::livekit::egress_info::Request::Track(request)) = failure.request
        else {
            panic!("real source track request missing")
        };
        assert_eq!(request.track_id, "TR_source");
        let Some(lk_protocol::livekit::track_egress_request::Output::File(file)) = request.output
        else {
            panic!("source DirectFile request missing")
        };
        assert_eq!(file.filepath, "dir-{track_id}.with.dot/file.ogg");
        assert!(!file.disable_manifest);
    }
    #[test]
    fn stale_room_action_cannot_be_rebound_by_same_room_name() {
        let room = room();
        let action = RecordingLaunch::Participant {
            room_sid: "RM_old".into(),
            participant_sid: "PA_old".into(),
            identity: "human".into(),
            policy: Default::default(),
        };
        assert_eq!(
            automatic_recording_request(&room, action).unwrap_err(),
            "automatic recording room generation mismatch"
        );
    }
}

pub struct NoRecordingNotifications;
impl IoEvents for NoRecordingNotifications {
    fn notify(&self, _: crate::workers::io_service::IoEvent) {}
}
/// Install the source nil-launcher failure path before exposing standalone HTTP.
/// Notification is omitted only when no webhook notifier exists in assembly.
///
/// SPEC-0074 A10: no longer used by the executable. Single-node runs keep the
/// real launcher on the process-local bus (`connect_local`), exactly like
/// upstream, so an unattended start waits the retry schedule instead of
/// reporting "not connected". Kept for callers that assemble a state without
/// worker services on purpose (test fixtures).
pub fn install_disconnected_recording(
    manager: &RoomManager,
    notifier: Option<crate::webhook::WebhookNotifier>,
) -> Result<(), RpcError> {
    let events: Arc<dyn IoEvents> = match notifier {
        Some(notifier) => Arc::new(crate::worker_notifications::WorkerNotifications::new(
            notifier,
        )),
        None => Arc::new(NoRecordingNotifications),
    };
    manager
        .attach_recording_launcher(Arc::new(AutomaticRecordingAdapter {
            services: std::sync::Weak::new(),
            events,
        }))
        .map_err(|error| RpcError::new("failed_precondition", error.to_string()))
}

#[cfg(test)]
mod automatic_recording_shutdown_redis_tests {
    use super::*;
    use rtc_runtime::automatic_recording::RecordingLauncher;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Events(AtomicUsize);
    impl IoEvents for Events {
        fn notify(&self, event: crate::workers::io_service::IoEvent) {
            assert!(matches!(
                event,
                crate::workers::io_service::IoEvent::EgressEnded(_)
            ));
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    /// Actual RESP3 PubSub ACKs and original EgressGateway retries. Observer
    /// deliberately supplies NO worker response; this is shutdown/resource
    /// correctness, never a recording success/media capability test.
    #[tokio::test]
    #[ignore = "requires an isolated owned Redis in AUTO_EGRESS_TEST_REDIS_URL; root executes explicitly"]
    async fn actual_pending_automatic_rpc_unlimited_retries_cancel_before_outer_join() {
        use crate::service_bus::{channels, redis_transport::TransportLimits, wire::WireLimits};
        let url = std::env::var("AUTO_EGRESS_TEST_REDIS_URL").expect("dedicated Redis required");
        assert!(
            url.starts_with("redis://127.0.0.1:") && !url.contains('@'),
            "owned local credential-free test broker only"
        );
        for (max_attempts, composite) in [(0, false), (-1, false), (0, true), (-1, true)] {
            let net = rtc_runtime::net::NetRouter::start(rtc_runtime::net::NetConfig {
                udp_enabled: false,
                udp_port: None,
                port_range: (0, 0),
                tcp_port: None,
                interfaces: vec![rtc_runtime::net::LocalInterface {
                    bind: std::net::Ipv4Addr::LOCALHOST.into(),
                    advertise: std::net::Ipv4Addr::LOCALHOST.into(),
                }],
            })
            .await
            .unwrap();
            let state = crate::CompatServices::new(Arc::new(crate::test_support::config()))
                .assemble(net.clone(), Arc::new(rtc_core::events::NullSink));
            let manager = state.manager().clone();
            let events = Arc::new(Events(AtomicUsize::new(0)));
            let observer = RedisTransport::connect(
                &url,
                TransportLimits {
                    wire: WireLimits {
                        encoded_bytes: 1024 * 1024,
                        decompressed_bytes: 1024 * 1024,
                    },
                    compression: CompressionPolicy::default(),
                    inbox_messages: 32,
                    subscriptions: 16,
                    commands: 32,
                    io_timeout: Duration::from_secs(5),
                },
            )
            .await
            .unwrap();
            let channel = channels::request("EgressInternal", "StartEgress", &[""], false);
            // subscribe awaits real Redis ACK; no sleep is treated as readiness.
            let mut requests = observer.subscribe(&channel, false).await.unwrap();
            let info = redis::IntoConnectionInfo::into_connection_info(url.clone()).unwrap();
            let services = WorkerServices::connect(
                WorkerConfig {
                    redis: RedisEndpoint::from(info),
                    rpc: RpcPolicy::from_raw(max_attempts, 3_000_000_000, 2_000_000_000, 1000),
                    compression: CompressionPolicy::default(),
                    max_decompressed_size: 1024 * 1024,
                    ingress: IngressConfig::default(),
                    ingress_legacy_psrpc: false,
                },
                manager.clone(),
                None,
                events.clone(),
            )
            .await
            .unwrap();
            let key = rtc_core::ids::RoomKey::new(
                rtc_core::ids::ProjectId::local(),
                format!("unlimited-{}", rtc_core::ids::new_sid("test_")),
            );
            let room = manager
                .create_room(key.clone(), Default::default())
                .await
                .unwrap();
            let launcher = AutomaticRecordingAdapter {
                services: Arc::downgrade(&services),
                events: events.clone(),
            };
            let task = if composite {
                tokio::spawn(launcher.launch_room(
                    room.clone(),
                    rtc_core::auto_egress::RoomCompositeRecordingPolicy {
                        audio_only: true,
                        ..Default::default()
                    },
                ))
            } else {
                tokio::spawn(launcher.launch(
                    room.clone(),
                    rtc_core::auto_egress::RecordingLaunch::Track {
                        room_sid: room.sid.clone(),
                        participant_sid: "PA_test_source".into(),
                        identity: "actual-publisher".into(),
                        track_sid: "TR_test_source".into(),
                        policy: Default::default(),
                    },
                ))
            };
            // Four actual wire requests cross the inner three-attempt boundary:
            // the source-configured outer unlimited retry is really consumed.
            for _ in 0..4 {
                let bytes = tokio::time::timeout(Duration::from_secs(30), requests.recv())
                    .await
                    .unwrap()
                    .unwrap();
                assert!(!bytes.is_empty());
            }
            assert!(!task.is_finished());
            // After the fourth real request's no-response result, pending clears
            // while the actual inner retry backoff sleeps. Global cancel must
            // wake that sleep, not merely a later client-closed RPC receive.
            tokio::time::timeout(Duration::from_secs(5), async {
                while services.start_client.pending_count() > 0 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .unwrap();
            assert!(
                !task.is_finished(),
                "source unlimited retry must still own this real attempt"
            );
            manager.close_all().await; // Must not join unlimited worker retries.
            services.shutdown().await; // Cancels owned RPC and retry sleepers.
            let result = tokio::time::timeout(Duration::from_secs(5), task)
                .await
                .expect("global RPC cancellation must complete actual retry consumer")
                .unwrap();
            assert_eq!(result.unwrap_err(), "request canceled");
            assert_eq!(services.start_client.pending_count(), 0);
            assert_eq!(
                events.0.load(Ordering::SeqCst),
                if composite { 0 } else { 1 }
            );
            assert!(
                services
                    .io
                    .store
                    .as_ref()
                    .unwrap()
                    .list_egress(&room.name.0, false)
                    .await
                    .unwrap()
                    .is_empty(),
                "no worker reply means no fabricated stored job"
            );
            manager.drain_recording_work().await;
            state.agents().shutdown().await;
            net.shutdown();
            drop(requests);
            drop(observer);
        }
    }
}

#[cfg(test)]
mod direct_create_composite_shutdown_redis_tests {
    use super::*;
    use crate::service_bus::{
        channels,
        wire::{self, WireLimits},
    };
    use lk_protocol::{livekit as pb, psrpc::internal as bus, rpc as internal};
    use prost::Message;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Events(AtomicUsize);
    impl IoEvents for Events {
        fn notify(&self, _: crate::workers::io_service::IoEvent) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// This enters the actual public RoomService.call/CreateRoom dispatch and
    /// owner lease; it is deliberately NOT launcher.launch_room. The observer
    /// supplies no fake worker response, and no recording success is asserted.
    #[tokio::test]
    #[ignore = "requires isolated owned Redis AUTO_EGRESS_TEST_REDIS_URL; coordinator executes explicitly"]
    async fn direct_create_composite_real_retry_sleep_cancels_for_zero_and_negative_attempt_limits()
    {
        let url = std::env::var("AUTO_EGRESS_TEST_REDIS_URL").expect("dedicated Redis required");
        assert!(
            url.starts_with("redis://127.0.0.1:") && !url.contains('@'),
            "owned credential-free local broker only"
        );
        for attempts in [0, -1] {
            let (initial, net) = crate::test_support::state().await;
            // This existing test isolates global automatic cancellation, not
            // CreateRoom expiry: source accepted handler has its own deadline.
            // Keep every original request/response assertion and allow the
            // measured four worker requests before testing global shutdown.
            let mut config = (*initial.config).clone();
            config.room.create_room_timeout = rtc_core::config::PolicyDuration::from_secs(120);
            config.room.create_room_attempts = 1;
            let mut state = crate::state::CompatServices::new(Arc::new(config))
                .assemble(net.clone(), Arc::new(rtc_core::events::NullSink));
            let manager = state.manager().clone();
            let events = Arc::new(Events(AtomicUsize::new(0)));
            let limits = WireLimits {
                encoded_bytes: 1024 * 1024,
                decompressed_bytes: 1024 * 1024,
            };
            let observer = RedisTransport::connect(
                &url,
                TransportLimits {
                    wire: limits,
                    compression: CompressionPolicy::default(),
                    inbox_messages: 32,
                    subscriptions: 16,
                    commands: 32,
                    io_timeout: Duration::from_secs(5),
                },
            )
            .await
            .unwrap();
            let channel = channels::request("EgressInternal", "StartEgress", &[""], false);
            let mut requests = observer.subscribe(&channel, false).await.unwrap();
            let services = WorkerServices::connect(
                WorkerConfig {
                    redis: RedisEndpoint::from(
                        redis::IntoConnectionInfo::into_connection_info(url.clone()).unwrap(),
                    ),
                    rpc: RpcPolicy::from_raw(attempts, 3_000_000_000, 2_000_000_000, 1000),
                    compression: CompressionPolicy::default(),
                    max_decompressed_size: 1024 * 1024,
                    ingress: IngressConfig::default(),
                    ingress_legacy_psrpc: false,
                },
                manager.clone(),
                None,
                events.clone(),
            )
            .await
            .unwrap();
            state.worker_services = Some(services.clone());
            let agents = state.agents().clone();
            let room_name = format!("direct-cancel-{}", rtc_core::ids::new_sid("test_"));
            let mut auth = crate::auth::VerifiedToken::default();
            auth.grants.video.room_create = true;
            assert!(
                !auth.grants.video.room_record,
                "CreateRoom authorization must not invent roomRecord"
            );
            let body = pb::CreateRoomRequest {
                name: room_name.clone(),
                empty_timeout: 1,
                egress: Some(pb::RoomEgress {
                    room: Some(pb::RoomCompositeEgressRequest {
                        room_name: "wrong-input-room-must-be-overwritten".into(),
                        audio_only: true,
                        file_outputs: vec![pb::EncodedFileOutput {
                            filepath: "pending-cancel.ogg".into(),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }
            .encode_to_vec();
            let task = tokio::spawn(async move {
                crate::twirp::room_service::call(
                    &state,
                    Some(&auth),
                    "CreateRoom",
                    crate::twirp::codec::Codec::Protobuf,
                    &body,
                )
                .await
            });
            let mut first_owner = None;
            let mut first_egress = None;
            for _ in 0..4 {
                let bytes = tokio::time::timeout(Duration::from_secs(30), requests.recv())
                    .await
                    .unwrap()
                    .unwrap();
                let request = wire::decode::<bus::Request>(&bytes, limits)
                    .unwrap()
                    .message;
                assert!(!request.raw_request.is_empty());
                let start =
                    lk_protocol::wire::decode::<internal::StartEgressRequest>(&request.raw_request)
                        .unwrap();
                let owners = manager.list_rooms(
                    &rtc_core::ids::ProjectId::local(),
                    &[rtc_core::ids::RoomName(room_name.clone())],
                );
                assert_eq!(
                    owners.len(),
                    1,
                    "real CreateRoom must register exactly one owner before invoking worker"
                );
                assert_eq!(start.room_id, owners[0].sid.0);
                assert!(start.egress_id.starts_with("EG_"));
                if let Some(sid) = &first_owner {
                    assert_eq!(sid, &start.room_id);
                } else {
                    first_owner = Some(start.room_id.clone());
                }
                if let Some(id) = &first_egress {
                    assert_eq!(
                        id, &start.egress_id,
                        "same worker request identity across genuine retries"
                    );
                } else {
                    first_egress = Some(start.egress_id.clone());
                }
                let Some(internal::start_egress_request::Request::RoomComposite(composite)) =
                    start.request
                else {
                    panic!("actual direct API must publish RoomComposite request")
                };
                assert_eq!(composite.room_name, room_name);
                assert!(composite.audio_only);
            }
            assert!(!task.is_finished());
            // Request #4 crosses the three inner attempts. With no worker reply,
            // pending then clears into the genuine next retry backoff sleep.
            tokio::time::timeout(Duration::from_secs(5), async {
                while services.start_client.pending_count() > 0 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .unwrap();
            assert!(
                !task.is_finished(),
                "the direct API still owns an unlimited actual retry, not a fabricated completed response"
            );
            manager.close_all().await;
            services.shutdown().await;
            let result = tokio::time::timeout(Duration::from_secs(5), task)
                .await
                .expect("owned automatic cancellation must wake the direct API retry sleep")
                .unwrap();
            let error = result.unwrap_err();
            assert_eq!(error.code, crate::twirp::error::Code::Canceled);
            assert_eq!(error.msg, "request canceled");
            assert_eq!(services.start_client.pending_count(), 0);
            assert_eq!(
                events.0.load(Ordering::SeqCst),
                0,
                "direct Composite failure is an API error, not a synthetic ended webhook"
            );
            assert!(
                services
                    .io
                    .store
                    .as_ref()
                    .unwrap()
                    .list_egress(&room_name, false)
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert!(
                manager
                    .list_rooms(
                        &rtc_core::ids::ProjectId::local(),
                        &[rtc_core::ids::RoomName(room_name)]
                    )
                    .is_empty()
            );
            manager.drain_recording_work().await;
            agents.shutdown().await;
            net.shutdown();
            drop(requests);
            drop(observer);
        }
    }
}

#[cfg(test)]
#[path = "worker_services/create_room_timeout.rs"]
mod create_room_timeout;

// Test-only access to the exact StartEgress request producer, not a new bus.
#[cfg(test)]
impl WorkerServices {
    pub(crate) async fn test_automatic_start_barrier(&self) -> Result<usize, RpcError> {
        self.start_client.test_command_barrier().await?;
        Ok(self.start_client.pending_count())
    }
}

#[cfg(test)]
pub(crate) mod cluster_rooms_tests;
