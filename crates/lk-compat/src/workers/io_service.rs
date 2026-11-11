//! Exact IOInfo callbacks over real subscribed PSRPC; requires real SIP
//! evaluator and telemetry adapters, no success-producing fallback implementations.
use super::{error::IoError, preserved::WireValue, store::IoStore};
use crate::service_bus::{
    rpc::{MethodSpec, RawRequest, RawResponse, RpcError, RpcServer, ServerLimits},
    rpc_transport::RpcBus,
};
use futures_util::future::BoxFuture;
use lk_protocol::{livekit as pb, rpc as internal}; // integration must generate rpc/*
use opentelemetry::trace::FutureExt as _;
use std::{sync::Arc, time::Duration};
use tokio::{sync::watch, task::JoinHandle};
#[derive(Clone, Debug)]
pub enum IoEvent {
    EgressStarted(Box<pb::EgressInfo>),
    EgressUpdated(Box<pb::EgressInfo>),
    EgressEnded(Box<pb::EgressInfo>),
    IngressCreated(Box<pb::IngressInfo>),
    IngressDeleted(Box<pb::IngressInfo>),
    IngressStarted(Box<pb::IngressInfo>),
    IngressUpdated(Box<pb::IngressInfo>),
    IngressEnded(Box<pb::IngressInfo>),
}
pub trait IoEvents: Send + Sync {
    fn notify(&self, event: IoEvent);
}
pub trait SipCallbacks: Send + Sync {
    fn authenticate(
        &self,
        request: internal::GetSipTrunkAuthenticationRequest,
    ) -> BoxFuture<'_, Result<WireValue<internal::GetSipTrunkAuthenticationResponse>, RpcError>>;
    fn evaluate(
        &self,
        request: internal::EvaluateSipDispatchRulesRequest,
    ) -> BoxFuture<'_, Result<WireValue<internal::EvaluateSipDispatchRulesResponse>, RpcError>>;
}
pub struct IoInfo {
    pub store: Option<Arc<IoStore>>,
    events: Arc<dyn IoEvents>,
    sip: Arc<dyn SipCallbacks>,
}
impl IoInfo {
    pub fn new(
        store: Option<Arc<IoStore>>,
        events: Arc<dyn IoEvents>,
        sip: Arc<dyn SipCallbacks>,
    ) -> Arc<Self> {
        Arc::new(Self { store, events, sip })
    }
    fn egress_store(&self) -> Result<&IoStore, RpcError> {
        self.store
            .as_deref()
            .ok_or_else(|| RpcError::new("internal", "egress not connected (redis required)"))
    }
    fn ingress_store(&self) -> Result<&IoStore, RpcError> {
        self.store
            .as_deref()
            .ok_or_else(|| RpcError::new("internal", "ingress not connected (redis required)"))
    }
    pub async fn create_egress(&self, info: pb::EgressInfo) -> Result<(), RpcError> {
        self.create_egress_wire(WireValue::from_message(info)).await
    }
    pub async fn create_egress_wire(
        &self,
        value: WireValue<pb::EgressInfo>,
    ) -> Result<(), RpcError> {
        let info = &value.message;
        let store = self.egress_store()?;
        if store.load_egress(&info.egress_id).await.is_ok() {
            return Ok(());
        }
        store.store_egress_wire(&value).await?;
        self.events
            .notify(IoEvent::EgressStarted(Box::new(info.clone())));
        Ok(())
    }
    pub async fn update_egress(&self, info: pb::EgressInfo) -> Result<(), RpcError> {
        self.update_egress_wire(WireValue::from_message(info)).await
    }
    pub async fn update_egress_wire(
        &self,
        value: WireValue<pb::EgressInfo>,
    ) -> Result<(), RpcError> {
        let info = value.message.clone();
        let result = self.egress_store()?.update_egress_wire(&value).await;
        match info.status {
            1 | 2 => self.events.notify(IoEvent::EgressUpdated(Box::new(info))),
            3..=6 => self.events.notify(IoEvent::EgressEnded(Box::new(info))),
            _ => {}
        };
        result.map_err(Into::into)
    }
    pub async fn get_egress(&self, id: &str) -> Result<pb::EgressInfo, RpcError> {
        self.egress_store()?
            .load_egress(id)
            .await
            .map_err(Into::into)
    }
    pub async fn list_egress(
        &self,
        req: pb::ListEgressRequest,
    ) -> Result<pb::ListEgressResponse, RpcError> {
        let store = self.egress_store()?;
        let items = if !req.egress_id.is_empty() {
            vec![store.load_egress(&req.egress_id).await?]
        } else {
            store.list_egress(&req.room_name, req.active).await?
        };
        Ok(pb::ListEgressResponse {
            items,
            ..Default::default()
        })
    }
    pub async fn get_egress_wire(&self, id: &str) -> Result<WireValue<pb::EgressInfo>, RpcError> {
        self.egress_store()?
            .load_egress_wire(id)
            .await
            .map_err(Into::into)
    }
    pub async fn list_egress_wire(
        &self,
        req: pb::ListEgressRequest,
    ) -> Result<WireValue<pb::ListEgressResponse>, RpcError> {
        let store = self.egress_store()?;
        let items = if !req.egress_id.is_empty() {
            vec![store.load_egress_wire(&req.egress_id).await?]
        } else {
            store.list_egress_wire(&req.room_name, req.active).await?
        };
        let mut value = WireValue::from_message(pb::ListEgressResponse {
            items: items.iter().map(|v| v.message.clone()).collect(),
            ..Default::default()
        });
        value
            .wire
            .replace_repeated("items", items.into_iter().map(|v| v.wire));
        Ok(value)
    }
    pub async fn create_ingress_wire(
        &self,
        value: WireValue<pb::IngressInfo>,
    ) -> Result<WireValue<internal::CreateIngressResponse>, RpcError> {
        self.create_ingress_wire_typed(value)
            .await
            .map_err(Into::into)
    }
    /// Local typed domain result. Remote RPC errors never recover local identity.
    pub async fn create_ingress_wire_typed(
        &self,
        value: WireValue<pb::IngressInfo>,
    ) -> Result<WireValue<internal::CreateIngressResponse>, IoError> {
        self.store
            .as_deref()
            .ok_or_else(|| IoError::typed("internal", "ingress not connected (redis required)"))?
            .store_ingress_wire(&value)
            .await?;
        self.events
            .notify(IoEvent::IngressCreated(Box::new(value.message.clone())));
        let mut response = WireValue::from_message(internal::CreateIngressResponse {
            info: Some(value.message),
        });
        response.wire.replace_nested("info", Some(&value.wire));
        Ok(response)
    }
    pub async fn get_ingress_wire(
        &self,
        req: internal::GetIngressInfoRequest,
    ) -> Result<WireValue<internal::GetIngressInfoResponse>, RpcError> {
        let store = self.ingress_store()?;
        let value = if !req.ingress_id.is_empty() {
            store.load_ingress_wire(&req.ingress_id).await?
        } else if !req.stream_key.is_empty() {
            store
                .load_ingress_from_stream_key_wire(&req.stream_key)
                .await?
        } else {
            return Err(IoError::ordinary(
                "request needs to specify either IngressId or StreamKey",
            )
            .into());
        };
        let mut response = WireValue::from_message(internal::GetIngressInfoResponse {
            info: Some(value.message),
            ..Default::default()
        });
        response.wire.replace_nested("info", Some(&value.wire));
        Ok(response)
    }
    pub async fn create_ingress(
        &self,
        info: pb::IngressInfo,
    ) -> Result<internal::CreateIngressResponse, RpcError> {
        self.ingress_store()?.store_ingress(&info).await?;
        self.events
            .notify(IoEvent::IngressCreated(Box::new(info.clone())));
        Ok(internal::CreateIngressResponse { info: Some(info) })
    }
    pub async fn get_ingress(
        &self,
        req: internal::GetIngressInfoRequest,
    ) -> Result<internal::GetIngressInfoResponse, RpcError> {
        let store = self.ingress_store()?;
        let info = if !req.ingress_id.is_empty() {
            store.load_ingress(&req.ingress_id).await?
        } else if !req.stream_key.is_empty() {
            store.load_ingress_from_stream_key(&req.stream_key).await?
        } else {
            return Err(IoError::ordinary(
                "request needs to specify either IngressId or StreamKey",
            )
            .into());
        };
        Ok(internal::GetIngressInfoResponse {
            info: Some(info),
            ..Default::default()
        })
    }
    pub async fn update_ingress_state(
        &self,
        req: internal::UpdateIngressStateRequest,
    ) -> Result<(), RpcError> {
        self.update_ingress_state_wire(req, None).await
    }
    pub async fn update_ingress_state_wire(
        &self,
        req: internal::UpdateIngressStateRequest,
        request_wire: Option<crate::sip::wire::PreservedMessage>,
    ) -> Result<(), RpcError> {
        let store = self.ingress_store()?;
        let mut info = store.load_ingress(&req.ingress_id).await?;
        let before = info.state.as_ref().map_or(0, |state| state.status);
        // Actual generated request nil state is malformed to source callers: reject
        // rather than fabricate successful metadata for a nonexistent update.
        let state = req
            .state
            .ok_or_else(|| RpcError::new("invalid_argument", "ingress state is required"))?;
        let mut value = WireValue::from_message(state.clone());
        if let Some(wire) = request_wire.and_then(|wire| wire.nested("state")) {
            value.wire = wire;
        }
        store
            .update_ingress_state_wire(&req.ingress_id, &value)
            .await?;
        let status = state.status;
        info.state = Some(state);
        if before == status {
            self.events.notify(IoEvent::IngressUpdated(Box::new(info)));
        } else {
            match status {
                0 | 3 | 4 => self.events.notify(IoEvent::IngressEnded(Box::new(info))),
                2 => self.events.notify(IoEvent::IngressStarted(Box::new(info))),
                1 => self.events.notify(IoEvent::IngressUpdated(Box::new(info))),
                _ => {}
            }
        }
        Ok(())
    }
    /// Register all fixed IOInfo methods, roll back every successful subscription
    /// if any following registration fails. Constructor readiness is actual SUB ACK.
    /// The bus is either a broker connection or the process-local bus
    /// (`psrpc.NewLocalMessageBus` in single-node mode); both carry the same
    /// request handling.
    pub async fn register(
        self: &Arc<Self>,
        bus: impl Into<RpcBus>,
        limits: ServerLimits,
    ) -> Result<IoRunning, RpcError> {
        let bus: RpcBus = bus.into();
        let mut servers = Vec::new();
        macro_rules! register {
            ($kind:ident,$method:literal,$request:ty,$response:ty,$call:expr) => {{
                let owner = self.clone();
                let spec = MethodSpec {
                    service: "IOInfo".into(),
                    method: $method.into(),
                    topics: vec![],
                    require_claim: true,
                    queue: true,
                };
                match RpcServer::$kind::<$request, $response, _, _>(
                    bus.clone(),
                    spec,
                    limits,
                    move |_, request| {
                        let owner = owner.clone();
                        async move { ($call)(owner, request).await }
                    },
                    None,
                )
                .with_context(
                    crate::service_bus::telemetry::RpcTracing::factory_options().scoped_context(),
                )
                .await
                {
                    Ok(server) => servers.push(server),
                    Err(error) => {
                        for server in servers {
                            server.close().await;
                        }
                        return Err(error);
                    }
                }
            }};
        }
        register!(
            register_with_raw,
            "CreateEgress",
            pb::EgressInfo,
            (),
            |owner: Arc<Self>, request: RawRequest<pb::EgressInfo>| async move {
                owner
                    .create_egress_wire(WireValue::decode(&request.raw)?)
                    .await
            }
        );
        register!(
            register_with_raw,
            "UpdateEgress",
            pb::EgressInfo,
            (),
            |owner: Arc<Self>, request: RawRequest<pb::EgressInfo>| async move {
                owner
                    .update_egress_wire(WireValue::decode(&request.raw)?)
                    .await
            }
        );
        register!(
            register_with_wire,
            "GetEgress",
            internal::GetEgressRequest,
            pb::EgressInfo,
            |owner: Arc<Self>, request: RawRequest<internal::GetEgressRequest>| async move {
                Ok(raw_response(
                    owner.get_egress_wire(&request.message.egress_id).await?,
                ))
            }
        );
        register!(
            register_with_wire,
            "ListEgress",
            pb::ListEgressRequest,
            pb::ListEgressResponse,
            |owner: Arc<Self>, request: RawRequest<pb::ListEgressRequest>| async move {
                Ok(raw_response(owner.list_egress_wire(request.message).await?))
            }
        );
        register!(
            register_with_raw,
            "UpdateMetrics",
            internal::UpdateMetricsRequest,
            (),
            |_owner: Arc<Self>, request: RawRequest<internal::UpdateMetricsRequest>| async move {
                let request = request.message;
                tracing::info!(
                    egress_id = request
                        .info
                        .as_ref()
                        .map(|i| i.egress_id.as_str())
                        .unwrap_or_default(),
                    avg_cpu = request.avg_cpu_usage,
                    max_cpu = request.max_cpu_usage,
                    "received egress metrics"
                );
                Ok(())
            }
        );
        register!(
            register_with_wire,
            "CreateIngress",
            pb::IngressInfo,
            internal::CreateIngressResponse,
            |owner: Arc<Self>, request: RawRequest<pb::IngressInfo>| async move {
                Ok(raw_response(
                    owner
                        .create_ingress_wire(WireValue::decode(&request.raw)?)
                        .await?,
                ))
            }
        );
        register!(
            register_with_wire,
            "GetIngressInfo",
            internal::GetIngressInfoRequest,
            internal::GetIngressInfoResponse,
            |owner: Arc<Self>, request: RawRequest<internal::GetIngressInfoRequest>| async move {
                Ok(raw_response(owner.get_ingress_wire(request.message).await?))
            }
        );
        register!(
            register_with_raw,
            "UpdateIngressState",
            internal::UpdateIngressStateRequest,
            (),
            |owner: Arc<Self>, request: RawRequest<internal::UpdateIngressStateRequest>| async move {
                let value = WireValue::<internal::UpdateIngressStateRequest>::decode(&request.raw)?;
                owner
                    .update_ingress_state_wire(value.message, Some(value.wire))
                    .await
            }
        );
        register!(
            register_with_wire,
            "GetSIPTrunkAuthentication",
            internal::GetSipTrunkAuthenticationRequest,
            internal::GetSipTrunkAuthenticationResponse,
            |owner: Arc<Self>, request: RawRequest<internal::GetSipTrunkAuthenticationRequest>| async move {
                owner
                    .sip
                    .authenticate(request.message)
                    .await
                    .map(raw_response)
            }
        );
        register!(
            register_with_wire,
            "EvaluateSIPDispatchRules",
            internal::EvaluateSipDispatchRulesRequest,
            internal::EvaluateSipDispatchRulesResponse,
            |owner: Arc<Self>, request: RawRequest<internal::EvaluateSipDispatchRulesRequest>| async move {
                owner.sip.evaluate(request.message).await.map(raw_response)
            }
        );
        // These two are explicit TODO no-ops in the fixed source, not a claim
        // of implemented call persistence or audit storage.
        register!(
            register_with_raw,
            "UpdateSIPCallState",
            internal::UpdateSipCallStateRequest,
            (),
            |_owner: Arc<Self>, _request: RawRequest<internal::UpdateSipCallStateRequest>| async move {
                Ok(())
            }
        );
        register!(
            register_with_raw,
            "RecordCallContext",
            internal::RecordCallContextRequest,
            (),
            |_owner: Arc<Self>, _request: RawRequest<internal::RecordCallContextRequest>| async move {
                Ok(())
            }
        );
        let (stop, mut shutdown) = watch::channel(false);
        let store = self.store.clone();
        let sweeper = tokio::spawn(async move {
            if let Some(store) = store {
                let mut tick = tokio::time::interval_at(
                    tokio::time::Instant::now() + Duration::from_secs(1800),
                    Duration::from_secs(1800),
                );
                loop {
                    tokio::select! {_=shutdown.changed()=>break,_=tick.tick()=>{let now=rtc_core::util::unix_ms().saturating_mul(1_000_000);if let Err(error)=store.clean_ended_egress(now).await{tracing::warn!(code=error.code,"egress cleanup failed");}}}
                }
            }
        });
        Ok(IoRunning {
            servers,
            stop,
            sweeper: Some(sweeper),
        })
    }
}
fn raw_response<T: prost::Message + prost::Name + Default>(value: WireValue<T>) -> RawResponse<T> {
    let raw = value.encode();
    RawResponse {
        message: value.message,
        raw,
    }
}
pub struct IoRunning {
    servers: Vec<RpcServer>,
    stop: watch::Sender<bool>,
    sweeper: Option<JoinHandle<()>>,
}
impl IoRunning {
    pub async fn shutdown(mut self) {
        let _ = self.stop.send(true);
        if let Some(task) = self.sweeper.take() {
            let _ = task.await;
        }
        // IOInfo is one logical generated service, split into method servers
        // locally. Stop every method's request subscription before waiting for
        // accepted handlers, retaining grants until their original expiry.
        futures_util::future::join_all(self.servers.drain(..).map(RpcServer::shutdown)).await;
    }
}
impl Drop for IoRunning {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        if let Some(task) = self.sweeper.take() {
            task.abort();
        }
    }
}
