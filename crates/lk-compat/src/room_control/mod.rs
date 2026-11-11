//! Explicit sole-owner control-plane component over source PSRPC/Redis storage.
//!
//! This component is not product cluster activation: signal/WHIP and default
//! cached-participant reconciliation remain prerequisites before that claim.
pub(crate) mod accepted;
pub mod catalog;
pub(crate) mod create_policy;
pub mod events;
mod participants;
pub mod registry;
pub mod selector;

use crate::service_bus::{
    redis_transport::RedisTransport,
    rpc::{CallOptions, MethodSpec, RpcClient, RpcError, RpcServer, ServerLimits},
};
use crate::workers::{
    preserved::WireValue,
    retry::{GenericRetry, RpcPolicy},
};
use crate::{convert, owner_dispatch::AgentDispatchTopics, state::AppState};
use catalog::{RoomAllocator, RoomCatalog};
use lk_protocol::livekit as pb;
use registry::RedisRegistry;
use rtc_core::ids::{Identity, RoomSid};
use rtc_runtime::RoomOwnerHandle;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::sync::Mutex;

/// Method-scoped request options. Absence of a selection override preserves
/// source PSRPC no-affinity defaults; an explicit value overrides them whole.
#[derive(Clone, Debug)]
pub struct CreateRoomCallOptions {
    pub timeout: Duration,
    pub deadline: Option<std::time::Instant>,
    pub cancel: crate::service_bus::rpc::Cancellation,
    pub metadata: BTreeMap<String, String>,
    pub selection: Option<crate::service_bus::rpc::SelectionOptions>,
}
impl CreateRoomCallOptions {
    pub fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            deadline: None,
            cancel: Default::default(),
            metadata: BTreeMap::new(),
            selection: None,
        }
    }
    fn resolve(&self, configured: Option<Duration>) -> CallOptions {
        // Nonpositive method settings install no WithRequestTimeout. The source
        // getRequestOpts then inherits caller remaining time before default3s.
        // An already-expired deadline remains zero/past, never default3s.
        let timeout = if !self.timeout.is_zero() {
            self.timeout
        } else if let Some(configured) = configured {
            configured
        } else if let Some(deadline) = self.deadline {
            deadline.saturating_duration_since(std::time::Instant::now())
        } else {
            Duration::from_secs(3)
        };
        let mut options = CallOptions::new(timeout);
        options.deadline = self.deadline;
        options.cancel = self.cancel.clone();
        options.metadata = self.metadata.clone();
        options.selection.accept_first = true;
        options.selection.affinity_timeout = Some(Duration::from_secs(1));
        if let Some(selection) = self.selection.as_ref() {
            options.selection = *selection;
        }
        options
    }
}

pub struct Config {
    #[cfg(test)]
    pub(crate) startup_gate: Option<Arc<ProjectionGate>>,
    pub node: pb::Node,
    pub redis: redis::ConnectionInfo,
    pub timeout: Duration,
    pub max_protobuf_bytes: usize,
    pub max_pending: usize,
    pub selector: selector::Config,
    pub limits: ServerLimits,
}
/// Production-only controls. Node identity and Redis topology come from their
/// actual shared owners, so callers cannot pass unused credentials or a second
/// node snapshot through the legacy isolated-component Config.
pub struct ManagedConfig {
    pub timeout: Duration,
    pub max_protobuf_bytes: usize,
    pub max_pending: usize,
    pub selector: selector::Config,
    pub limits: ServerLimits,
}
struct OwnerGeneration {
    sid: RoomSid,
    room: Vec<RpcServer>,
    participants: BTreeMap<String, Vec<RpcServer>>,
    agents: AgentDispatchTopics,
}
#[cfg(test)]
pub(crate) struct ProjectionGate {
    pub entered: tokio::sync::Notify,
    pub release: tokio::sync::Semaphore,
}
struct ShutdownRun {
    completion: tokio::sync::watch::Sender<Option<Result<(), RpcError>>>,
    _task: tokio::task::JoinHandle<()>,
}
pub struct RoomControl {
    startup_attached: std::sync::atomic::AtomicBool,
    registration_attempted: std::sync::atomic::AtomicBool,
    startup_task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    #[cfg(test)]
    startup_gate: Option<Arc<ProjectionGate>>,
    shutdown_run: std::sync::Mutex<Option<ShutdownRun>>,
    #[cfg(test)]
    before_shutdown: std::sync::Mutex<Option<Arc<ProjectionGate>>>,
    #[cfg(test)]
    after_projection_cas: std::sync::Mutex<Option<Arc<ProjectionGate>>>,
    #[cfg(test)]
    before_participant_store: std::sync::Mutex<Option<Arc<ProjectionGate>>>,
    ready: std::sync::atomic::AtomicBool,
    state: AppState,
    node: Mutex<WireValue<pb::Node>>,
    registry: Arc<RedisRegistry>,
    managed_node: Option<Arc<crate::routing::LocalNode>>,
    allocator: Arc<RoomAllocator>,
    client: RpcClient,
    bus: Arc<RedisTransport>,
    limits: ServerLimits,
    policy: RpcPolicy,
    node_server: Mutex<Option<RpcServer>>,
    generations: Mutex<BTreeMap<String, OwnerGeneration>>,
    accepted: Arc<accepted::Accepted>,
    user_draining: std::sync::atomic::AtomicBool,
}
pub(crate) fn signal_create_request(
    params: &rtc_core::session::JoinParams,
) -> pb::CreateRoomRequest {
    let mut request = pb::CreateRoomRequest {
        name: params.room.name.0.clone(),
        room_preset: params.grants.room_preset.clone(),
        ..Default::default()
    };
    if let Some(config) = &params.grants.room_config {
        let config = crate::auth::claims::room_config_to_proto(config);
        request.empty_timeout = config.empty_timeout;
        request.departure_timeout = config.departure_timeout;
        request.max_participants = config.max_participants;
        request.metadata = config.metadata;
        request.egress = config.egress;
        request.min_playout_delay = config.min_playout_delay;
        request.max_playout_delay = config.max_playout_delay;
        request.sync_streams = config.sync_streams;
        request.agents = config.agents;
        request.tags = config.tags;
    }
    request
}
fn api(error: rtc_runtime::ApiError) -> RpcError {
    let error: crate::twirp::error::TwirpError = error.into();
    let mut result = RpcError::new(error.code.as_str(), error.msg);
    result.application_metadata = error.meta;
    result
}
fn error(error: impl std::fmt::Display) -> RpcError {
    RpcError::new("unknown", error.to_string())
}
fn create_spec(node: &str) -> MethodSpec {
    MethodSpec {
        service: "RoomManager".into(),
        method: "CreateRoom".into(),
        topics: vec![node.into()],
        require_claim: true,
        queue: true,
    }
}
fn room_spec(room: &str, method: &str) -> MethodSpec {
    MethodSpec {
        service: "Room".into(),
        method: method.into(),
        topics: vec![room.into()],
        require_claim: true,
        queue: true,
    }
}
fn check(owner: &RoomOwnerHandle, room: &str) -> Result<(), RpcError> {
    if owner.key().name.0 == room {
        Ok(())
    } else {
        Err(RpcError::new(
            "failed_precondition",
            "owner request room does not match registered topic",
        ))
    }
}
impl RoomControl {
    /// Explicit component assembly only. Existing AppState must have no room
    /// router yet, preventing a recursively retained collaborator graph.
    pub async fn start(
        state: AppState,
        bus: Arc<RedisTransport>,
        config: Config,
    ) -> Result<Arc<Self>, RpcError> {
        let endpoint = crate::redis_endpoint::RedisEndpoint::simple(config.redis.clone());
        Self::start_inner(state, bus, config, endpoint, None).await
    }
    /// Production assembly uses the NodeRouter-owned record. RoomControl owns
    /// room routing/topics only; it cannot independently register/delete nodes.
    pub async fn start_managed(
        state: AppState,
        bus: Arc<RedisTransport>,
        config: ManagedConfig,
        endpoint: crate::redis_endpoint::RedisEndpoint,
        node: Arc<crate::routing::LocalNode>,
    ) -> Result<Arc<Self>, RpcError> {
        // A credential-free inert compatibility field for start_inner's shared
        // isolated constructor shape. Managed startup exclusively uses endpoint.
        let legacy = Config {
            #[cfg(test)]
            startup_gate: None,
            node: node.snapshot(),
            redis: redis::IntoConnectionInfo::into_connection_info("redis://127.0.0.1/")
                .expect("constant credential-free legacy placeholder"),
            timeout: config.timeout,
            max_protobuf_bytes: config.max_protobuf_bytes,
            max_pending: config.max_pending,
            selector: config.selector,
            limits: config.limits,
        };
        Self::start_inner(state, bus, legacy, endpoint, Some(node)).await
    }
    async fn start_inner(
        state: AppState,
        bus: Arc<RedisTransport>,
        config: Config,
        endpoint: crate::redis_endpoint::RedisEndpoint,
        managed_node: Option<Arc<crate::routing::LocalNode>>,
    ) -> Result<Arc<Self>, RpcError> {
        if !state
            .manager
            .list_rooms(&rtc_core::ids::ProjectId::local(), &[])
            .is_empty()
        {
            return Err(RpcError::new(
                "failed_precondition",
                "room authority must attach before owner admission",
            ));
        }
        if state.room_control.is_some() {
            return Err(RpcError::new(
                "already_exists",
                "room control already installed",
            ));
        }
        if config.node.id.is_empty() {
            return Err(RpcError::new("invalid_argument", "node ID cannot be empty"));
        }
        // Reject a closed/quarantined shared authority before any new Redis command.
        state.routing_events.ensure_attachable()?;
        let registry = Arc::new(
            RedisRegistry::connect_endpoint(
                endpoint.clone(),
                config.timeout,
                config.max_protobuf_bytes,
            )
            .await
            .map_err(error)?,
        );
        let catalog = Arc::new(
            RoomCatalog::connect_endpoint(endpoint, config.timeout, config.max_protobuf_bytes)
                .await
                .map_err(error)?,
        );
        let selector = Arc::new(selector::ConfiguredSelector::new(config.selector).map_err(error)?);
        let allocator = Arc::new(RoomAllocator {
            catalog,
            registry: registry.clone(),
            config: state.config.clone(),
            selector,
        });
        #[cfg(test)]
        let startup_gate = config.startup_gate.clone();
        let control = Arc::new(Self {
            startup_attached: std::sync::atomic::AtomicBool::new(false),
            registration_attempted: std::sync::atomic::AtomicBool::new(false),
            startup_task: std::sync::Mutex::new(None),
            #[cfg(test)]
            startup_gate,
            ready: std::sync::atomic::AtomicBool::new(false),
            shutdown_run: std::sync::Mutex::new(None),
            #[cfg(test)]
            before_shutdown: std::sync::Mutex::new(None),
            #[cfg(test)]
            after_projection_cas: std::sync::Mutex::new(None),
            #[cfg(test)]
            before_participant_store: std::sync::Mutex::new(None),
            policy: state.rpc_policy,
            state,
            node: Mutex::new(WireValue::from_message(config.node)),
            registry,
            managed_node,
            allocator,
            client: RpcClient::new(bus.clone(), config.max_pending)?.with_tracing(
                crate::service_bus::telemetry::RpcTracing::production_client_params()
                    .excluding_service("RoomManager"),
            ),
            bus,
            limits: config.limits,
            node_server: Mutex::new(None),
            generations: Mutex::new(BTreeMap::new()),
            accepted: Arc::new(accepted::Accepted::default()),
            user_draining: std::sync::atomic::AtomicBool::new(false),
        });
        // Cancellation of the caller cannot drop half of registration while
        // leaving shared authority attached. The tracked factory task owns the
        // outcome until it is handed to the caller or fully rolled back.
        let (result, receive) = tokio::sync::oneshot::channel();
        let (accepted, handoff) = tokio::sync::oneshot::channel();
        let initializing = control.clone();
        let task = tokio::spawn(async move {
            match initializing.initialize().await {
                Ok(()) => {
                    if (result.send(Ok(initializing.clone())).is_err() || handoff.await.is_err())
                        && let Err(error) = initializing.rollback_start().await
                    {
                        tracing::error!(code=%error.code,"abandoned room control startup rollback failed");
                    }
                }
                Err(error) => {
                    if let Err(rollback) = initializing.rollback_start().await {
                        tracing::error!(code=%rollback.code,"failed room control startup rollback failed");
                    }
                    let _ = result.send(Err(error));
                }
            }
        });
        *control
            .startup_task
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = Some(task);
        let control = receive.await.map_err(|_| RpcError::closed())??;
        // There is no await between handoff, admission and returning the actual
        // owner. Before handoff, accepted node requests cannot create a room.
        control
            .ready
            .store(true, std::sync::atomic::Ordering::Release);
        let _ = accepted.send(());
        Ok(control)
    }
    async fn initialize(self: &Arc<Self>) -> Result<(), RpcError> {
        self.state.routing_events.attach(Arc::downgrade(self), 0)?;
        self.startup_attached
            .store(true, std::sync::atomic::Ordering::Release);
        let node = self.node.lock().await.message.id.clone();
        let weak = Arc::downgrade(self);
        let server = RpcServer::register_with_raw::<pb::CreateRoomRequest, pb::Room, _, _>(
            self.bus.clone(),
            create_spec(&node),
            self.limits,
            move |context, request| {
                let weak = weak.clone();
                async move {
                    let control = weak
                        .upgrade()
                        .ok_or_else(|| RpcError::new("unavailable", "room control stopped"))?;
                    if !control.ready.load(std::sync::atomic::Ordering::Acquire) {
                        return Err(RpcError::new("unavailable", "room control is not ready"));
                    }
                    control
                        .create_owner_with_expiry(
                            request.message,
                            Some(context.deadline),
                            Some(context.expiry_ns),
                        )
                        .await
                }
            },
            None,
        )
        .await?;
        *self.node_server.lock().await = Some(server);
        #[cfg(test)]
        if let Some(gate) = self.startup_gate.as_ref() {
            gate.entered.notify_one();
            let permit = gate
                .release
                .acquire()
                .await
                .map_err(|_| RpcError::closed())?;
            permit.forget();
        }
        if self.managed_node.is_some() {
            return Ok(());
        }
        self.registration_attempted
            .store(true, std::sync::atomic::Ordering::Release);
        // Take a consistent owned snapshot; no node mutex is held across Redis I/O.
        let node = self.node.lock().await.clone();
        self.registry.register_node(&node).await.map_err(error)
    }
    async fn rollback_start(&self) -> Result<(), RpcError> {
        let mut failure = None;
        self.ready
            .store(false, std::sync::atomic::Ordering::Release);
        self.client.close();
        if let Some(server) = self.node_server.lock().await.take() {
            server.close().await;
        }
        // A duplicate bootstrap which never owned attach must not detach/reset
        // the already-live authority, its budget, topics or generation ledger.
        // A successful reply proves the old removal finished; an error does
        // not. Never turn an uncertain same-ID HDEL into a reusable authority.
        let mut unregister_uncertain = false;
        if self
            .registration_attempted
            .swap(false, std::sync::atomic::Ordering::AcqRel)
            && let Err(error) = self
                .registry
                .unregister_node(&self.node.lock().await.message.id)
                .await
        {
            unregister_uncertain = true;
            failure = Some(self::error(error));
        }
        if self
            .startup_attached
            .swap(false, std::sync::atomic::Ordering::AcqRel)
        {
            let cleanup = if unregister_uncertain {
                self.state
                    .routing_events
                    .quarantine_empty_attachment()
                    .await
            } else {
                self.state.routing_events.rollback_empty_attachment().await
            };
            if let Err(error) = cleanup
                && failure.is_none()
            {
                failure = Some(error);
            }
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
    async fn configured<Req, Resp>(
        &self,
        spec: &MethodSpec,
        request: &Req,
    ) -> Result<Resp, RpcError>
    where
        Req: prost::Message,
        Resp: prost::Message + prost::Name + Default,
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
            match self.client.unary(spec, request, options).await {
                Ok(value) => return Ok(value),
                Err(failure) if retry.next(&failure) => {}
                Err(failure) => return Err(failure),
            }
        }
    }
    /// Fixed service/utils.go ValidateConnectRequest: failed owner lookup is
    /// ignored here; only an existing owner's configured capacity rejects RTC.
    pub(crate) async fn signal_node_at_capacity(&self, name: &str) -> bool {
        match self.registry.get_node_for_room(name).await {
            Ok(node) => self.allocator.selector.at_capacity(&node.message),
            Err(_) => false,
        }
    }

    pub async fn signal_room_exists(&self, name: &str) -> Result<bool, RpcError> {
        self.allocator
            .catalog
            .public_room_exists(name)
            .await
            .map_err(Into::into)
    }
    /// Select once before creating the actual owner, then resolve the durable
    /// mapping again. Both HTTP/Twirp and websocket admission use this authority.
    pub async fn signal_owner(
        &self,
        params: &rtc_core::session::JoinParams,
    ) -> Result<String, RpcError> {
        let name = &params.room.name.0;
        if !self.state.config.room.auto_create
            && !params.grants.video.room_create
            && !self.signal_room_exists(name).await?
        {
            return Err(RpcError::new("not_found", "requested room does not exist"));
        }
        self.select_signal_owner(name).await
    }

    pub(crate) async fn select_signal_owner(&self, name: &str) -> Result<String, RpcError> {
        // rtcservice.startConnection selects the mapping before RelaySignal;
        // getOrCreateRoom and actual owner side effects run on the chosen node.
        self.allocator
            .select_owner(name, "")
            .await
            .map_err(Into::into)
    }
    /// WHIPService.SelectRoomNode has no room.auto_create admission check and
    /// does not pre-create an owner before the selected node's StartSession.
    pub async fn select_whip_owner(&self, room: &str) -> Result<String, RpcError> {
        self.allocator
            .select_owner(room, "")
            .await
            .map_err(Into::into)
    }
    pub async fn prepare_signal_room(
        self: &Arc<Self>,
        request: pb::CreateRoomRequest,
    ) -> Result<(), RpcError> {
        self.verify_signal_owner(&request.name).await?;
        self.create_owner_with_expiry(request, None, None)
            .await
            .map(|_| ())
    }
    pub async fn verify_signal_owner(&self, name: &str) -> Result<(), RpcError> {
        let assigned = self.registry.get_node_for_room(name).await.map_err(error)?;
        let local = self
            .managed_node
            .as_ref()
            .map(|node| node.id())
            .unwrap_or_else(|| self.state.config.node_id.clone());
        if assigned.message.id != local {
            return Err(RpcError::new(
                "failed_precondition",
                "called participant on incorrect node",
            ));
        }
        Ok(())
    }
    pub async fn create_room(&self, request: pb::CreateRoomRequest) -> Result<pb::Room, RpcError> {
        // Source public RoomService SelectRoomNode precedes node CreateRoom;
        // allocation/room-store locking belongs to the actual selected owner.
        let node = self
            .allocator
            .select_owner(&request.name, &request.node_id)
            .await
            .map_err(RpcError::from)?;
        self.create_on_selected_node(&create_spec(&node), &request, None)
            .await
    }
    /// Source caller request timeout/cancellation/deadline applies to every
    /// attempt. Node selection stays outside the retry loop.
    pub async fn create_room_with_options(
        &self,
        request: pb::CreateRoomRequest,
        options: CreateRoomCallOptions,
    ) -> Result<pb::Room, RpcError> {
        let node = self
            .allocator
            .select_owner(&request.name, &request.node_id)
            .await
            .map_err(RpcError::from)?;
        self.create_on_selected_node(&create_spec(&node), &request, Some(options))
            .await
    }
    async fn create_on_selected_node(
        &self,
        spec: &MethodSpec,
        request: &pb::CreateRoomRequest,
        caller: Option<CreateRoomCallOptions>,
    ) -> Result<pb::Room, RpcError> {
        let mut retry = create_policy::CreateRoomRetry::new(&self.state.config.room);
        loop {
            let options = caller.as_ref().map_or_else(
                || CreateRoomCallOptions::new(Duration::ZERO).resolve(retry.configured_timeout()),
                |caller| caller.resolve(retry.configured_timeout()),
            );
            let result = self.client.unary(spec, request, options.clone()).await;
            match result {
                Ok(room) => return Ok(room),
                Err(failure) if retry.next(&failure) => {
                    // Source checks caller Done only after deciding to retry,
                    // not by sleeping or replacing the original terminal error.
                    if options.cancel.is_cancelled()
                        || options
                            .deadline
                            .is_some_and(|d| d <= std::time::Instant::now())
                    {
                        let mut canceled = RpcError::new("canceled", "request canceled");
                        canceled.origin = crate::service_bus::rpc::RpcErrorOrigin::Canceled;
                        return Err(canceled);
                    }
                }
                Err(failure) => return Err(failure),
            }
        }
    }
    #[cfg(test)]
    async fn create_owner(
        self: &Arc<Self>,
        request: pb::CreateRoomRequest,
    ) -> Result<pb::Room, RpcError> {
        self.create_owner_with_deadline(request, None).await
    }
    #[cfg(test)]
    async fn create_owner_with_deadline(
        self: &Arc<Self>,
        request: pb::CreateRoomRequest,
        deadline: Option<std::time::Instant>,
    ) -> Result<pb::Room, RpcError> {
        self.create_owner_with_expiry(
            request,
            deadline,
            deadline.map(crate::service_bus::rpc::capture_expiry_ns),
        )
        .await
    }
    async fn create_owner_with_expiry(
        self: &Arc<Self>,
        request: pb::CreateRoomRequest,
        deadline: Option<std::time::Instant>,
        expiry_ns: Option<i64>,
    ) -> Result<pb::Room, RpcError> {
        let node_id = self.node.lock().await.message.id.clone();
        let (shared, effective, catalog_created) =
            self.allocator.create_for_owner(request, &node_id).await?;
        let mut policy = catalog::runtime_config(&shared);
        policy.tags = effective
            .tags
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        // RoomComposite is create-request-only. Retain stored participant/track
        // policy; do not invent a RoomInternal composite field or lose presence.
        if let Some(egress) = effective.egress.as_ref() {
            let resolved = crate::workers::auto_egress_policy::room_egress_in(egress);
            if let Some(current) = policy.automatic_recording.as_mut() {
                current.room = resolved.room;
            } else {
                policy.automatic_recording = Some(resolved);
            }
        }
        let lease = self
            .state
            .manager
            .create_room_lease_from_catalog(
                self.state.room_key(&shared.room.message.name),
                rtc_runtime::CreateRoomOptions { config: policy },
                convert::room_info_from_proto(&shared.room.message),
            )
            .await
            .map_err(api)?;
        self.ensure_generation(&shared.room.message.name, &lease.snapshot().sid)
            .await?;
        // Go RoomManager launches a composite only when this allocation created
        // the catalog generation. Creating an actor for an existing RoomSID
        // after ownership handoff must not launch the create-only policy again.
        if catalog_created
            && lease.created()
            && let Some(policy) = lease.composite_policy()
        {
            let services = self.state.worker_services.as_ref().ok_or_else(|| {
                RpcError::new("internal", "egress not connected (redis required)")
            })?;
            let mut composite = crate::workers::auto_egress_policy::composite_out(policy);
            composite.room_name = shared.room.message.name.clone();
            let mut start = crate::workers::egress::wrap_room_wire(
                WireValue::from_message(composite),
                rtc_core::ids::new_sid("EG_"),
            );
            start.message.room_id = lease.snapshot().sid.0.clone();
            let mut options = services.automatic_options();
            options.deadline = deadline;
            options.expiry_ns = expiry_ns;
            services.egress.start_owned_wire(start, options).await?;
        }
        let mut result = convert::room_info_to_proto(&lease.current_snapshot().await);
        result.turn_password = shared.room.message.turn_password;
        self.allocator
            .catalog
            .publish_owner_room(&result, &self.node.lock().await.message.id)
            .await?;
        Ok(result)
    }
    async fn ensure_generation(
        self: &Arc<Self>,
        name: &str,
        sid: &RoomSid,
    ) -> Result<(), RpcError> {
        self.ensure_generation_current(name, sid, || true).await
    }
    async fn ensure_generation_current(
        self: &Arc<Self>,
        name: &str,
        sid: &RoomSid,
        is_current: impl Fn() -> bool + Send + Sync,
    ) -> Result<(), RpcError> {
        let current = || {
            if is_current() {
                Ok(())
            } else {
                Err(RpcError::new(
                    "aborted",
                    "room projection changed during topic registration",
                ))
            }
        };
        let mut groups = self.generations.lock().await;
        current()?;
        if let Some(previous) = groups.get(name)
            && previous.sid == *sid
        {
            return Ok(());
        }
        if let Some(old) = groups.remove(name) {
            current()?;
            retire(old, &self.accepted).await?;
            current()?;
        }
        let key = self.state.room_key(name);
        let owner = self
            .state
            .manager
            .capture_room_owner(&key, sid)
            .map_err(api)?;
        let mut servers = Vec::new();
        let result: Result<AgentDispatchTopics, RpcError> = async {
            let captured = owner.clone();
            let weak = Arc::downgrade(self);
            let accepted = self.accepted.clone();
            let room = name.to_owned();
            let generation = sid.clone();
            servers.push(
                    RpcServer::register_with_raw::<
                        pb::DeleteRoomRequest,
                        pb::DeleteRoomResponse,
                        _,
                        _,
                    >(
                        self.bus.clone(),
                        room_spec(name, "DeleteRoom"),
                        self.limits,
                        move |_, request| {
                            let owner = captured.clone();
                            let weak = weak.clone();
                            let room = room.clone();
                            let generation = generation.clone();
                            let accepted=accepted.clone();
                            async move {
                                let sid=owner.sid().clone();
                                accepted.run(&sid,async move {
                                check(&owner, &request.message.room)?;
                                let control = weak.upgrade().ok_or_else(|| {
                                    RpcError::new("unavailable", "room control stopped")
                                })?;
                                control.close_generation(&owner).await?;
                                control.retire_generation(&room, &generation).await?;
                                Ok(pb::DeleteRoomResponse {})
                            }).await
                            }
                        },
                        None,
                    )
                    .await?,
                );
            current()?;
            let captured = owner.clone();
            let weak = Arc::downgrade(self);
            let accepted = self.accepted.clone();
            servers.push(
                RpcServer::register_with_raw::<pb::UpdateRoomMetadataRequest, pb::Room, _, _>(
                    self.bus.clone(),
                    room_spec(name, "UpdateRoomMetadata"),
                    self.limits,
                    move |_, request| {
                        let owner = captured.clone();
                        let weak = weak.clone();
                        let accepted = accepted.clone();
                        async move {
                            let sid = owner.sid().clone();
                            accepted
                                .run(&sid, async move {
                                    check(&owner, &request.message.room)?;
                                    let updated = owner
                                        .update_metadata(request.message.metadata)
                                        .await
                                        .map_err(api)?;
                                    let control = weak.upgrade().ok_or_else(|| {
                                        RpcError::new("unavailable", "room control stopped")
                                    })?;
                                    let stored = control
                                        .allocator
                                        .catalog
                                        .load(&request.message.room)
                                        .await
                                        .map_err(RpcError::from)?;
                                    let mut result = convert::room_info_to_proto(&updated);
                                    result.version =
                                        Some(convert::timed_version_to_proto(&updated.version));
                                    result.turn_password = stored.room.message.turn_password;
                                    control
                                        .allocator
                                        .catalog
                                        .publish_owner_metadata(
                                            &result,
                                            &control.node.lock().await.message.id,
                                        )
                                        .await?;
                                    result.version = None;
                                    Ok(result)
                                })
                                .await
                        }
                    },
                    None,
                )
                .await?,
            );
            current()?;
            let captured = owner.clone();
            servers.push(
                RpcServer::register_with_raw::<
                    pb::ListParticipantsRequest,
                    pb::ListParticipantsResponse,
                    _,
                    _,
                >(
                    self.bus.clone(),
                    room_spec(name, "ListParticipants"),
                    self.limits,
                    move |_, request| {
                        let owner = captured.clone();
                        async move {
                            check(&owner, &request.message.room)?;
                            Ok(pb::ListParticipantsResponse {
                                participants: owner
                                    .list_participants()
                                    .await
                                    .map_err(api)?
                                    .iter()
                                    .map(convert::participant_info_to_proto)
                                    .collect(),
                            })
                        }
                    },
                    None,
                )
                .await?,
            );
            current()?;
            let captured = owner.clone();
            servers.push(
                RpcServer::register_with_raw::<
                    pb::RoomParticipantIdentity,
                    pb::ParticipantInfo,
                    _,
                    _,
                >(
                    self.bus.clone(),
                    room_spec(name, "GetParticipant"),
                    self.limits,
                    move |_, request| {
                        let owner = captured.clone();
                        async move {
                            check(&owner, &request.message.room)?;
                            owner
                                .get_participant(Identity(request.message.identity))
                                .await
                                .map(|p| convert::participant_info_to_proto(&p))
                                .map_err(api)
                        }
                    },
                    None,
                )
                .await?,
            );
            current()?;
            current()?;
            servers.push(self.register_room_send_data(&owner).await?);
            let agents = AgentDispatchTopics::register_with_accepted(
                owner.clone(),
                self.bus.clone().into(),
                self.limits,
                self.accepted.clone(),
            )
            .await?;
            if let Err(stale) = current() {
                let _ = agents.retire().await;
                return Err(stale);
            }
            Ok(agents)
        }
        .await;
        match result {
            Ok(agents) => {
                groups.insert(
                    name.to_owned(),
                    OwnerGeneration {
                        sid: sid.clone(),
                        room: servers,
                        participants: BTreeMap::new(),
                        agents,
                    },
                );
                Ok(())
            }
            Err(failure) => {
                for server in servers {
                    let _ = self.accepted.retire_server(sid, server).await;
                }
                Err(failure)
            }
        }
    }
    async fn close_generation(&self, owner: &RoomOwnerHandle) -> Result<(), RpcError> {
        // Use the same real catalog creation lock across owner closure and its
        // SID/node-CAS storage retirement. A same-name create cannot bootstrap
        // the just-closed stored SID in the normal non-expired lock interval.
        // The source five-second lock is not claimed to be a perpetual HA lease.
        let lock = self
            .registry
            .lock_room(&owner.key().name.0)
            .await
            .map_err(error)?;
        let result = async {
            let stored = self
                .allocator
                .catalog
                .load(&owner.key().name.0)
                .await
                .map_err(RpcError::from)?;
            if stored.room.message.sid != owner.sid().0 {
                return Err(RpcError::new(
                    "failed_precondition",
                    "shared room generation does not match local owner",
                ));
            }
            owner.close_room().await.map_err(api)?;
            self.allocator
                .catalog
                .finish_generation(
                    &owner.key().name.0,
                    &owner.sid().0,
                    &self.node.lock().await.message.id,
                )
                .await
                .map_err(RpcError::from)?;
            Ok(())
        }
        .await;
        if self.registry.unlock_room(&lock).await.is_err() {
            tracing::warn!("room deletion unlock failed");
        }
        result
    }
    async fn retire_generation(&self, name: &str, sid: &RoomSid) -> Result<(), RpcError> {
        let mut groups = self.generations.lock().await;
        if groups.get(name).is_some_and(|current| current.sid == *sid)
            && let Some(group) = groups.remove(name)
        {
            retire(group, &self.accepted).await?;
        }
        Ok(())
    }
    pub async fn list_rooms(
        &self,
        request: pb::ListRoomsRequest,
    ) -> Result<pb::ListRoomsResponse, RpcError> {
        let names = (!request.names.is_empty()).then_some(request.names.as_slice());
        Ok(pb::ListRoomsResponse {
            rooms: self
                .allocator
                .catalog
                .list(names)
                .await
                .map_err(RpcError::from)?
                .into_iter()
                .map(|r| r.message)
                .collect(),
        })
    }
    pub async fn delete_room(
        &self,
        request: pb::DeleteRoomRequest,
    ) -> Result<pb::DeleteRoomResponse, RpcError> {
        if !self
            .allocator
            .catalog
            .public_room_exists(&request.room)
            .await
            .map_err(RpcError::from)?
        {
            return Err(catalog::CatalogError::NotFound.into());
        }
        let generation = self
            .create_room(pb::CreateRoomRequest {
                name: request.room.clone(),
                ..Default::default()
            })
            .await?;
        let owner = self
            .registry
            .get_node_for_room(&request.room)
            .await
            .map_err(error)?;
        let result = self
            .configured(&room_spec(&request.room, "DeleteRoom"), &request)
            .await?;
        self.allocator
            .catalog
            .finish_generation(&request.room, &generation.sid, &owner.message.id)
            .await
            .map_err(RpcError::from)?;
        Ok(result)
    }
    pub async fn update_metadata(
        &self,
        request: pb::UpdateRoomMetadataRequest,
    ) -> Result<pb::Room, RpcError> {
        self.allocator
            .catalog
            .load(&request.room)
            .await
            .map_err(RpcError::from)?;
        self.configured(&room_spec(&request.room, "UpdateRoomMetadata"), &request)
            .await
    }
    pub async fn list_participants(
        &self,
        request: pb::ListParticipantsRequest,
    ) -> Result<pb::ListParticipantsResponse, RpcError> {
        if !self.state.config.enable_psrpc_for_get_list_participants {
            return Ok(pb::ListParticipantsResponse {
                participants: self
                    .allocator
                    .catalog
                    .list_participants(&request.room)
                    .await
                    .map_err(RpcError::from)?
                    .into_iter()
                    .map(|p| p.message)
                    .collect(),
            });
        }
        self.configured(&room_spec(&request.room, "ListParticipants"), &request)
            .await
    }
    pub async fn get_participant(
        &self,
        request: pb::RoomParticipantIdentity,
    ) -> Result<pb::ParticipantInfo, RpcError> {
        if !self.state.config.enable_psrpc_for_get_list_participants {
            return Ok(self
                .allocator
                .catalog
                .load_participant(&request.room, &request.identity)
                .await
                .map_err(RpcError::from)?
                .message);
        }
        self.configured(&room_spec(&request.room, "GetParticipant"), &request)
            .await
    }
    pub async fn create_dispatch(
        &self,
        request: pb::AgentDispatch,
    ) -> Result<pb::AgentDispatch, RpcError> {
        self.configured(
            &crate::owner_dispatch::method(&request.room, "CreateDispatch"),
            &request,
        )
        .await
    }
    pub async fn delete_dispatch(
        &self,
        request: pb::DeleteAgentDispatchRequest,
    ) -> Result<pb::AgentDispatch, RpcError> {
        self.configured(
            &crate::owner_dispatch::method(&request.room, "DeleteDispatch"),
            &request,
        )
        .await
    }
    pub async fn list_dispatches(
        &self,
        request: pb::ListAgentDispatchRequest,
    ) -> Result<pb::ListAgentDispatchResponse, RpcError> {
        self.configured(
            &crate::owner_dispatch::method(&request.room, "ListDispatch"),
            &request,
        )
        .await
    }
    pub(super) fn reconcile_capacity(&self) -> usize {
        self.bus.limits().subscriptions
    }
    fn reconcile_config(&self) -> events::ReconcileConfig {
        events::ReconcileConfig {
            min_retry: Duration::from_millis(100),
            max_retry: Duration::from_secs(1),
            retry_timeout: self.policy.request_timeout(),
        }
    }
    pub(super) async fn reconciliation_blocked(&self, blocked: bool) -> Result<(), RpcError> {
        if let Some(node) = &self.managed_node {
            node.set_routing_blocked(blocked);
            return Ok(());
        }
        let mut node = self.node.lock().await;
        node.message.state = if blocked
            || self
                .user_draining
                .load(std::sync::atomic::Ordering::Acquire)
        {
            pb::NodeState::ShuttingDown
        } else {
            pb::NodeState::Serving
        } as i32;
        self.registry.register_node(&node).await.map_err(error)
    }
    pub(super) async fn reconcile_projection(
        self: &Arc<Self>,
        projection: &rtc_core::events::RoomProjection,
        is_current: impl Fn() -> bool + Send + Sync,
        prepare_cache: impl FnOnce(&catalog::CachedParticipantIds) -> Option<Vec<String>> + Send,
    ) -> Result<(), RpcError> {
        let name = &projection.room.name.0;
        let sid = &projection.room.sid;
        if projection.terminal {
            self.state
                .manager
                .wait_room_generation_closed(&self.state.room_key(name), sid)
                .await;
            // Local retired topics belong to this exact generation even when
            // public DeleteRoom already removed catalog state.
            self.retire_generation(name, sid).await?;
            self.allocator
                .catalog
                .finish_generation(name, &sid.0, &self.node.lock().await.message.id)
                .await
                .map_err(RpcError::from)?;
        } else {
            let own = self.node.lock().await.message.id.clone();
            // Fixed StartSession registers every Participant method before
            // StoreParticipant makes HasParticipant/GetParticipant succeed.
            // Do not expose cached presence while its actual queue/claim
            // subscriptions are still being installed in the background.
            // Retain SID/owner/revision fences around the ordinary Redis writes.
            if !self
                .allocator
                .catalog
                .owns_generation(name, &sid.0, &own)
                .await
                .map_err(RpcError::from)?
            {
                self.retire_generation(name, sid).await?;
                return Ok(());
            }
            if !is_current() {
                return Err(RpcError::new(
                    "aborted",
                    "room projection changed before participant registration",
                ));
            }
            self.ensure_generation_current(name, sid, &is_current)
                .await?;
            if let Err(error) = self
                .reconcile_participant_topics(projection, &is_current)
                .await
            {
                // StartSession closes the participant when actual topic
                // registration fails. A superseded projection is retried, not
                // an actual registration failure for the current participant.
                if error.code != "aborted" {
                    self.state
                        .routing_events
                        .participant_topics_failed(projection, &error);
                }
                return Err(error);
            }
            if !is_current() {
                return Err(RpcError::new(
                    "aborted",
                    "room projection changed during participant registration",
                ));
            }
            #[cfg(test)]
            if !projection.participants.is_empty() {
                let gate = self
                    .before_participant_store
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .take();
                if let Some(gate) = gate {
                    gate.entered.notify_one();
                    let permit = gate.release.acquire().await.map_err(|_| {
                        RpcError::new("canceled", "participant store test barrier closed")
                    })?;
                    permit.forget();
                }
            }
            let store_attempt = self
                .allocator
                .catalog
                .reconcile_generation(projection, &own, &is_current, prepare_cache)
                .await;
            // Go StartSession logs StoreParticipant errors and still starts
            // rtcSessionWorker. Signal input waits for this actual attempt,
            // not successful Redis recovery. An explicit lost-owner result
            // cannot admit this retired generation. A superseded projection or
            // failed optimistic CAS is not a StoreParticipant I/O failure: it
            // may have published no participant at all. Retry the current
            // projection before allowing this SID's RTC request pump.
            if !matches!(
                &store_attempt,
                Ok(false) | Err(catalog::CatalogError::ReconcileConflict)
            ) {
                self.state
                    .routing_events
                    .participant_store_attempted(projection);
            }
            let committed = store_attempt.map_err(|failure| match failure {
                catalog::CatalogError::ReconcileConflict => {
                    RpcError::new("aborted", "room projection changed during reconciliation")
                }
                other => RpcError::from(other),
            })?;
            if committed {
                #[cfg(test)]
                {
                    let gate = self
                        .after_projection_cas
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .take();
                    if let Some(gate) = gate {
                        gate.entered.notify_one();
                        let permit =
                            gate.release.acquire().await.map_err(|_| {
                                RpcError::new("canceled", "projection barrier closed")
                            })?;
                        permit.forget();
                    }
                }
                if !is_current() {
                    return Err(RpcError::new(
                        "aborted",
                        "room projection changed after Redis CAS",
                    ));
                }
            } else {
                self.retire_generation(name, sid).await?;
            }
        }
        Ok(())
    }
    pub async fn heartbeat(&self, stats: pb::NodeStats) -> Result<(), RpcError> {
        if self.managed_node.is_some() {
            return Err(RpcError::new(
                "failed_precondition",
                "managed node heartbeat belongs to NodeRouter",
            ));
        }
        let node = {
            let mut node = self.node.lock().await;
            node.message.stats = Some(stats);
            node.clone()
        };
        self.registry.register_node(&node).await.map_err(error)
    }
    /// Every caller observes the same tracked local-stop operation, even if an
    /// earlier caller is canceled. Redis errors cannot skip local retirement.
    pub async fn shutdown(self: &Arc<Self>) -> Result<(), RpcError> {
        let mut completion = {
            let mut run = self.shutdown_run.lock().unwrap_or_else(|p| p.into_inner());
            if run.is_none() {
                self.user_draining
                    .store(true, std::sync::atomic::Ordering::Release);
                let completion = tokio::sync::watch::channel(None).0;
                let published = completion.clone();
                let control = self.clone();
                let task = tokio::spawn(async move {
                    let result = control.shutdown_inner().await;
                    published.send_replace(Some(result));
                });
                *run = Some(ShutdownRun {
                    completion,
                    _task: task,
                });
            }
            run.as_ref()
                .expect("shutdown run initialized")
                .completion
                .subscribe()
        };
        loop {
            if let Some(result) = completion.borrow().clone() {
                return result;
            }
            completion.changed().await.map_err(|_| RpcError::closed())?;
        }
    }
    async fn shutdown_inner(&self) -> Result<(), RpcError> {
        #[cfg(test)]
        {
            let gate = self
                .before_shutdown
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .take();
            if let Some(gate) = gate {
                gate.entered.notify_one();
                if let Ok(permit) = gate.release.acquire().await {
                    permit.forget();
                }
            }
        }
        self.ready
            .store(false, std::sync::atomic::Ordering::Release);
        let mut failure = None;
        {
            let mut node = self.node.lock().await;
            node.message.state = pb::NodeState::ShuttingDown as i32;
            if self.managed_node.is_none() {
                if let Err(error) = self.registry.register_node(&node).await {
                    failure = Some(self::error(error));
                }
            }
        }
        // Source RedisRouter.Stop proceeds with cancellation after registry
        // errors. Preserve the first error while completing every local stage.
        self.client.close();
        if let Some(server) = self.node_server.lock().await.take()
            && let Err(error) = server.retire().await
            && failure.is_none()
        {
            failure = Some(error);
        }
        // Outer server closes owners/cancels automatic worker calls first.
        // The authority cannot ensure new groups after this complete detach.
        if let Err(error) = self.state.routing_events.detach().await
            && failure.is_none()
        {
            failure = Some(error);
        }
        let groups = std::mem::take(&mut *self.generations.lock().await);
        for (_, group) in groups {
            if let Err(error) = retire(group, &self.accepted).await
                && failure.is_none()
            {
                failure = Some(error);
            }
        }
        self.accepted.finish().await;
        if self.managed_node.is_none()
            && let Err(error) = self
                .registry
                .unregister_node(&self.node.lock().await.message.id)
                .await
            && failure.is_none()
        {
            failure = Some(self::error(error));
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}
async fn retire(group: OwnerGeneration, accepted: &accepted::Accepted) -> Result<(), RpcError> {
    let mut failure = None;
    for server in group
        .room
        .into_iter()
        .chain(group.participants.into_values().flatten())
    {
        if let Err(error) = accepted.retire_server(&group.sid, server).await
            && failure.is_none()
        {
            failure = Some(error);
        }
    }
    if let Err(error) = group.agents.retire().await
        && failure.is_none()
    {
        failure = Some(error);
    }
    match failure {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod accepted_native;

#[cfg(test)]
impl RoomControl {
    pub(crate) fn accepted_pending_for_test(&self) -> usize {
        self.accepted.pending()
    }
    pub(crate) fn accepted_outcomes_for_test(&self) -> Vec<Option<String>> {
        self.accepted.outcomes()
    }
}

#[cfg(test)]
#[path = "accepted_native_strict.rs"]
mod accepted_native_strict;
