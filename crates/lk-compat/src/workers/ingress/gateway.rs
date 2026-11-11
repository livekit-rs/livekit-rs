//! Source-bound external Ingress gateway draft, with concrete Go URL/GUID
//! implementations and actual store/RPC/events adapters. NOT_RUN.
use super::validation::{patch, update_transcoding, validate, validate_for_serialization};
use crate::{
    auth::{VerifiedToken, permissions::ensure_ingress_admin},
    service_bus::rpc::{
        CallOptions, MethodSpec, RawRequest, RpcClient, RpcError, SelectionOptions,
    },
    workers::{
        io_service::IoInfo,
        preserved::WireValue,
        retry::{GenericRetry, RpcPolicy},
        store::IoStore,
    },
};
use lk_protocol::{livekit as pb, rpc as internal};
use std::sync::Arc;
#[derive(Clone, Debug, Default)]
pub struct IngressConfig {
    pub rtmp_base_url: String,
    pub whip_base_url: String,
    pub enable_udp_url_pull: bool,
}
pub trait IngressDeleted: Send + Sync {
    fn deleted(&self, info: pb::IngressInfo);
}
pub struct ParsedUrl {
    pub scheme: String,
    pub normalized: String,
}
pub trait GoUrlParser: Send + Sync {
    fn parse(&self, raw: &str) -> Result<ParsedUrl, RpcError>;
}
pub trait IngressIds: Send + Sync {
    fn deterministic(&self, prefix: &str, request_id: &str) -> Result<String, RpcError>;
    fn random(&self, prefix: &str) -> Result<String, RpcError>;
}
pub struct IngressAdapters {
    pub store: Option<Arc<IoStore>>,
    pub client: Option<Arc<RpcClient>>,
    pub io: Arc<IoInfo>,
    pub events: Arc<dyn IngressDeleted>,
    pub urls: Arc<dyn GoUrlParser>,
    pub ids: Arc<dyn IngressIds>,
}
pub struct IngressGateway {
    config: IngressConfig,
    store: Option<Arc<IoStore>>,
    client: Option<Arc<RpcClient>>,
    io: Arc<IoInfo>,
    events: Arc<dyn IngressDeleted>,
    urls: Arc<dyn GoUrlParser>,
    ids: Arc<dyn IngressIds>,
    policy: RpcPolicy,
}
impl IngressGateway {
    pub fn new(config: IngressConfig, adapters: IngressAdapters, policy: RpcPolicy) -> Self {
        Self {
            config,
            store: adapters.store,
            client: adapters.client,
            io: adapters.io,
            events: adapters.events,
            urls: adapters.urls,
            ids: adapters.ids,
            policy,
        }
    }
    fn grant(auth: Option<&VerifiedToken>) -> Result<(), RpcError> {
        ensure_ingress_admin(auth).map_err(|e| RpcError::new("unauthenticated", e.to_string()))
    }
    fn store(&self) -> Result<&IoStore, RpcError> {
        self.store
            .as_deref()
            .ok_or_else(|| RpcError::new("internal", "ingress not connected (redis required)"))
    }
    fn client(&self) -> Result<&RpcClient, RpcError> {
        self.client
            .as_deref()
            .ok_or_else(|| RpcError::new("internal", "ingress not connected (redis required)"))
    }
    async fn call<Req, Resp>(
        &self,
        service: &str,
        method: &str,
        topics: Vec<String>,
        queue: bool,
        request: WireValue<Req>,
        mut options: CallOptions,
    ) -> Result<WireValue<Resp>, RpcError>
    where
        Req: prost::Message + prost::Name + Default + PartialEq + Clone,
        Resp: prost::Message + prost::Name + Default,
    {
        let client = self.client()?;
        // This helper currently serves only affinity StartIngress or queued
        // no-affinity handler methods, with fixed psrpc client defaults.
        options.selection = if queue {
            SelectionOptions {
                accept_first: true,
                affinity_timeout: Some(std::time::Duration::from_secs(1)),
                ..Default::default()
            }
        } else {
            SelectionOptions {
                affinity_timeout: Some(std::time::Duration::from_secs(1)),
                short_circuit_timeout: Some(std::time::Duration::from_millis(200)),
                ..Default::default()
            }
        };
        let spec = MethodSpec {
            service: service.into(),
            method: method.into(),
            topics,
            queue,
            require_claim: true,
        };
        let raw = RawRequest {
            message: request.message.clone(),
            raw: Arc::from(request.encode()),
        };
        let mut retry = GenericRetry::new(self.policy);
        loop {
            let mut call = options.clone();
            if let Some(timeout) = retry.timeout() {
                call.timeout = timeout;
            }
            match client.unary_raw_request::<_, Resp>(&spec, &raw, call).await {
                Ok(response) => return WireValue::decode(&response.raw).map_err(Into::into),
                Err(error) => {
                    if !retry.next(&error) {
                        return Err(error);
                    }
                    // Fixed PSRPC checks its caller context after deciding
                    // to retry, including a configured unbounded attempt count.
                    if options.cancel.is_cancelled()
                        || options
                            .deadline
                            .is_some_and(|deadline| deadline <= std::time::Instant::now())
                    {
                        return Err(RpcError::new("canceled", "request canceled"));
                    }
                }
            }
        }
    }
    pub async fn create(
        &self,
        auth: Option<&VerifiedToken>,
        request: WireValue<pb::CreateIngressRequest>,
        request_id: &str,
        options: CallOptions,
    ) -> Result<WireValue<pb::IngressInfo>, RpcError> {
        // Source's input dispatch precedes the common auth helper.
        let mut url = match request.message.input_type {
            0 => self.config.rtmp_base_url.clone(),
            1 => self.config.whip_base_url.clone(),
            2 => String::new(),
            _ => return Err(RpcError::new("invalid_argument", "invalid ingress type")),
        };
        Self::grant(auth)?;
        self.store()?;
        let req = &request.message;
        if req.input_type == 2 {
            if req.url.is_empty() {
                return Err(RpcError::new(
                    "invalid_argument",
                    "invalid ingress: missing URL parameter",
                ));
            }
            let parsed = self.urls.parse(&req.url)?;
            match parsed.scheme.as_str() {
                "http" | "https" | "srt" => {}
                "udp" if self.config.enable_udp_url_pull => {}
                "udp" => {
                    return Err(RpcError::new(
                        "invalid_argument",
                        "invalid ingress: udp url pull is not enabled",
                    ));
                }
                _ => {
                    return Err(RpcError::new(
                        "invalid_argument",
                        format!("invalid ingress: invalid url scheme {}", parsed.scheme),
                    ));
                }
            }
            url = parsed.normalized;
        }
        let info = pb::IngressInfo {
            ingress_id: self.ids.deterministic("IN_", request_id)?,
            name: req.name.clone(),
            stream_key: if req.input_type == 2 {
                String::new()
            } else {
                self.ids.random("")?
            },
            url,
            input_type: req.input_type,
            audio: req.audio.clone(),
            video: req.video.clone(),
            enable_transcoding: req.enable_transcoding,
            room_name: req.room_name.clone(),
            participant_identity: req.participant_identity.clone(),
            participant_name: req.participant_name.clone(),
            participant_metadata: req.participant_metadata.clone(),
            state: Some(pb::IngressState::default()),
            enabled: req.enabled,
            reusable: req.input_type != 2,
            ..Default::default()
        };
        if req.input_type == 2 {
            validate(&info)?;
        } else {
            validate_for_serialization(&info)?;
        }
        let mut info = WireValue::from_message(info);
        // Go constructs a new info but directly copies the actual audio/video children.
        for field in ["audio", "video"] {
            info.wire
                .replace_nested(field, request.wire.nested(field).as_ref());
        }
        update_transcoding(&mut info.message);
        if req.input_type == 2 {
            let mut start = WireValue::from_message(internal::StartIngressRequest {
                info: Some(info.message.clone()),
                ..Default::default()
            });
            start
                .wire
                .replace_nested("info", Some(&info.wire.overlay(&info.message)));
            self.call(
                "IngressInternal",
                "StartIngress",
                vec![],
                false,
                start,
                options,
            )
            .await
        } else {
            match self.io.create_ingress_wire_typed(info.clone()).await {
                Ok(response) => {
                    // The sidecar retains the same-source unknowns, while the typed
                    // child may have changed (for example default transcoding).
                    // Materialize current known values before extracting it.
                    if let Some(child) = response.wire.overlay(&response.message).nested("info") {
                        WireValue::decode(&child.encode_to_vec()).map_err(Into::into)
                    } else {
                        Ok(info)
                    }
                }
                Err(error) if error.is_ingress_out_of_date() => Ok(info),
                // Only the local typed sentinel is tolerated; an ordinary or
                // remote equal-text failed_precondition is not that identity.
                Err(error) => Err(error.into()),
            }
        }
    }
    pub async fn list(
        &self,
        auth: Option<&VerifiedToken>,
        request: pb::ListIngressRequest,
    ) -> Result<WireValue<pb::ListIngressResponse>, RpcError> {
        Self::grant(auth)?;
        let store = self.store()?;
        let items = if !request.ingress_id.is_empty() {
            vec![store.load_ingress_wire(&request.ingress_id).await?]
        } else {
            store.list_ingress_wire(&request.room_name).await?
        };
        let mut value = WireValue::from_message(pb::ListIngressResponse {
            items: items.iter().map(|v| v.message.clone()).collect(),
            ..Default::default()
        });
        value
            .wire
            .replace_repeated("items", items.into_iter().map(|v| v.wire));
        Ok(value)
    }
    pub async fn update(
        &self,
        auth: Option<&VerifiedToken>,
        request: WireValue<pb::UpdateIngressRequest>,
        options: CallOptions,
    ) -> Result<WireValue<pb::IngressInfo>, super::failure::IngressFailure> {
        Self::grant(auth)?;
        self.client()?;
        let store = self
            .store
            .as_deref()
            .ok_or(super::failure::IngressFailure::PanicRecovery)?;
        let mut info = store.load_ingress_wire(&request.message.ingress_id).await?;
        if !info.message.reusable {
            return Err(RpcError::new(
                "invalid_argument",
                "ingress is not reusable and cannot be modified",
            )
            .into());
        }
        // Source assumes stored state exists. A vanished state is an explicit boundary
        // requiring the documented integration decision, never fabricated active state.
        let status = info
            .message
            .state
            .as_ref()
            .ok_or(super::failure::IngressFailure::PanicRecovery)?
            .status;
        match status {
            0 | 3 => {
                if status == 3 {
                    info.message.state.as_mut().unwrap().status = 0;
                    let update = internal::UpdateIngressStateRequest {
                        ingress_id: info.message.ingress_id.clone(),
                        state: info.message.state.clone(),
                    };
                    let mut update = WireValue::from_message(update);
                    update
                        .wire
                        .replace_nested("state", info.wire.nested("state").as_ref());
                    update.wire = update.wire.overlay(&update.message);
                    if let Err(error) = self
                        .io
                        .update_ingress_state_wire(update.message, Some(update.wire))
                        .await
                    {
                        tracing::warn!(code = error.code, "could not store ingress state");
                    }
                }
                patch(&request.message, &mut info.message)?;
            }
            1 | 2 => {
                patch(&request.message, &mut info.message)?;
                if let Err(error) = self
                    .call::<_, pb::IngressState>(
                        "IngressHandler",
                        "UpdateIngress",
                        vec![request.message.ingress_id.clone()],
                        true,
                        request.clone(),
                        options,
                    )
                    .await
                {
                    tracing::warn!(code = error.code, "could not update active ingress");
                }
            }
            _ => {}
        }
        if matches!(status, 0..=3) {
            for field in ["audio", "video"] {
                if let Some(child) = request.wire.nested(field) {
                    info.wire.replace_nested(field, Some(&child));
                }
            }
        }
        store.update_ingress_wire(&info).await?;
        Ok(info)
    }
    pub async fn delete(
        &self,
        auth: Option<&VerifiedToken>,
        request: WireValue<pb::DeleteIngressRequest>,
        options: CallOptions,
    ) -> Result<WireValue<pb::IngressInfo>, super::failure::IngressFailure> {
        Self::grant(auth)?;
        self.client()?;
        let store = self
            .store
            .as_deref()
            .ok_or(super::failure::IngressFailure::PanicRecovery)?;
        let mut info = store.load_ingress_wire(&request.message.ingress_id).await?;
        let status = info
            .message
            .state
            .as_ref()
            .ok_or(super::failure::IngressFailure::PanicRecovery)?
            .status;
        if matches!(status, 1 | 2)
            && let Err(error) = self
                .call::<_, pb::IngressState>(
                    "IngressHandler",
                    "DeleteIngress",
                    vec![request.message.ingress_id.clone()],
                    true,
                    request,
                    options,
                )
                .await
        {
            tracing::warn!(code = error.code, "could not stop active ingress");
        }
        store.delete_ingress(&info.message).await?;
        info.message.state.as_mut().unwrap().status = 0;
        self.events.deleted(info.message.clone());
        Ok(info)
    }
}
