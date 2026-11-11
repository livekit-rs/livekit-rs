//! Agent service assembly: one registry and one actual job-effect consumer.
use super::{
    bridge::Bridge,
    cluster::Cluster,
    registry::{Limits, Registry},
    websocket::WorkerHttpState,
};
use crate::auth::{KeyStore, TokenMinter};
use lk_protocol::livekit as pb;
use rtc_runtime::{RoomManager, api::ApiError};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};
#[derive(Clone, Copy, Debug)]
pub struct ServiceConfig {
    pub signal_message_size_limit: i64,
    pub enable_recording: bool,
    pub enable_redaction: bool,
    pub target_load: f32,
    pub endpoints_disabled: bool,
    pub endpoints_max_streams: u32,
    pub endpoints_max_tunnels: u32,
    pub endpoints_tunnel_drain_timeout_ns: i64,
}
impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            signal_message_size_limit: 2 << 20,
            enable_recording: false,
            enable_redaction: false,
            target_load: 0.7,
            endpoints_disabled: false,
            endpoints_max_streams: 0,
            endpoints_max_tunnels: 0,
            endpoints_tunnel_drain_timeout_ns: 0,
        }
    }
}
pub struct AgentService {
    pub registry: Registry,
    keys: Arc<KeyStore>,
    tokens: Arc<TokenMinter>,
    server_info: pb::ServerInfo,
    config: ServiceConfig,
    events: mpsc::Sender<super::websocket::WorkerEvent>,
    shutdown: watch::Sender<bool>,
    task: Mutex<Option<JoinHandle<()>>>,
    endpoint_drain_started: AtomicBool,
    endpoint_drain_task: Mutex<Option<JoinHandle<()>>>,
    cluster: Mutex<Option<Arc<Cluster>>>,
    cluster_started: AtomicBool,
}
impl AgentService {
    pub fn new(
        manager: RoomManager,
        keys: Arc<KeyStore>,
        tokens: Arc<TokenMinter>,
        server_info: pb::ServerInfo,
        config: ServiceConfig,
    ) -> Result<Arc<Self>, ApiError> {
        Self::new_with_capacity(manager, keys, tokens, server_info, config, 4096)
    }
    fn new_with_capacity(
        manager: RoomManager,
        keys: Arc<KeyStore>,
        tokens: Arc<TokenMinter>,
        server_info: pb::ServerInfo,
        config: ServiceConfig,
        launch_queue_capacity: usize,
    ) -> Result<Arc<Self>, ApiError> {
        let registry = Registry::new(Limits::default());
        registry
            .endpoints
            .configure(config.endpoints_disabled, config.endpoints_max_streams);
        registry.endpoints.configure_tunnels(
            config.endpoints_max_tunnels,
            config.endpoints_tunnel_drain_timeout_ns,
        );
        let (events, worker_receiver) = mpsc::channel(1024);
        let (runtime_sender, runtime_receiver) =
            mpsc::channel(launch_queue_capacity.clamp(1, 1024));
        let (shutdown, shutdown_receiver) = watch::channel(false);
        manager.attach_agent_events(runtime_sender)?;
        let mut bridge = Bridge::new(
            manager,
            registry.clone(),
            tokens.clone(),
            runtime_receiver,
            worker_receiver,
            shutdown_receiver,
            config,
        );
        bridge.launch_queue_limit = launch_queue_capacity.max(1);
        let task = tokio::spawn(bridge.run());
        Ok(Arc::new(Self {
            registry,
            keys,
            tokens,
            server_info,
            config,
            events,
            shutdown,
            task: Mutex::new(Some(task)),
            endpoint_drain_started: AtomicBool::new(false),
            endpoint_drain_task: Mutex::new(None),
            cluster: Mutex::new(None),
            cluster_started: AtomicBool::new(false),
        }))
    }
    /// Activate the actual AgentInternal graph over the node's existing bus.
    /// All initial subscriptions acknowledge readiness before HTTP serving.
    pub async fn enable_cluster_rpc(
        &self,
        bus: impl Into<crate::service_bus::rpc_transport::RpcBus>,
    ) -> Result<(), crate::service_bus::rpc::RpcError> {
        if self.cluster_started.swap(true, Ordering::AcqRel) {
            return Err(crate::service_bus::rpc::RpcError::new(
                "already_exists",
                "AgentInternal already started",
            ));
        }
        match Cluster::start(
            self.registry.clone(),
            self.tokens.clone(),
            self.config.target_load,
            bus.into(),
        )
        .await
        {
            Ok(cluster) => {
                *self.cluster.lock().unwrap_or_else(|p| p.into_inner()) = Some(cluster);
                Ok(())
            }
            Err(error) => {
                self.cluster_started.store(false, Ordering::Release);
                Err(error)
            }
        }
    }
    pub fn routes(self: &Arc<Self>) -> axum::Router {
        super::websocket::routes(self.worker_state())
            .merge(super::endpoint::routes(self.worker_state()))
    }
    /// The full worker control-plane state — the same bag the WebSocket `/agent`
    /// adapter runs on — so another transport (the SPEC-0074 C15 WebTransport
    /// listener) drives an identical registry/dispatch loop, sharing the job-
    /// effect consumer, token minter and `ServerInfo` built by the assembler.
    pub fn worker_state(self: &Arc<Self>) -> WorkerHttpState {
        WorkerHttpState {
            registry: self.registry.clone(),
            keys: self.keys.clone(),
            tokens: self.tokens.clone(),
            server_info: self.server_info.clone(),
            signal_message_size_limit: self.config.signal_message_size_limit,
            events: self.events.clone(),
            shutdown: self.shutdown.subscribe(),
            _keepalive: Some(self.clone()),
        }
    }
    /// Refuse new tunnels synchronously, then own the drain deadline in a task
    /// retained until shutdown. Starting room drain must not await this task.
    pub fn start_endpoint_drain(&self) {
        let mut slot = self
            .endpoint_drain_task
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if self.endpoint_drain_started.swap(true, Ordering::AcqRel) {
            return;
        }
        self.registry.endpoints.begin_tunnel_drain();
        let endpoints = self.registry.endpoints.clone();
        let task = tokio::spawn(async move {
            endpoints.drain_tunnels().await;
        });
        *slot = Some(task);
    }
    /// The second signal skips the remaining grace period without dropping the
    /// tracked task or pretending its resources have already been released.
    pub fn force_endpoint_drain(&self) {
        self.registry.endpoints.force_tunnel_drain();
    }
    pub async fn shutdown(&self) {
        self.start_endpoint_drain();
        let drain = self
            .endpoint_drain_task
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        if let Some(task) = drain {
            if let Err(error) = task.await {
                tracing::error!(%error, "agent endpoint drain task failed");
            }
        }
        let _ = self.shutdown.send(true);
        self.registry.close_all();
        let cluster = self
            .cluster
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        if let Some(cluster) = cluster {
            cluster.shutdown().await;
        }
        let task = self.task.lock().unwrap_or_else(|p| p.into_inner()).take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }
}
impl Drop for AgentService {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
        self.registry.endpoints.force_tunnel_drain();
        self.registry.close_all();
        if let Some(cluster) = self
            .cluster
            .get_mut()
            .unwrap_or_else(|p| p.into_inner())
            .take()
        {
            cluster.cancel();
        }
        if let Some(task) = self
            .endpoint_drain_task
            .get_mut()
            .unwrap_or_else(|p| p.into_inner())
            .take()
        {
            task.abort();
        }
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

#[cfg(test)]
pub(super) mod tests;
