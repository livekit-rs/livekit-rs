//! Typed Egress gateway/launcher, real PSRPC worker replies only.
//! Public Twirp wrapper must decode its exact request before invoking these
//! grant-checked methods and preserve original unknown-field sidecars.
use super::{
    io_service::IoInfo,
    preserved::WireValue,
    retry::{GenericRetry, RpcPolicy, egress_recoverable},
};
use crate::{
    auth::{VerifiedToken, permissions::ensure_record},
    service_bus::rpc::{
        CallOptions, MethodSpec, RawRequest, RpcClient, RpcError, SelectionOptions,
    },
};
use futures_util::future::BoxFuture;
use lk_protocol::{livekit as pb, rpc as internal};
use std::{sync::Arc, time::Duration};

pub trait EgressRooms: Send + Sync {
    fn room_sid(&self, room: &str) -> BoxFuture<'_, Result<String, RpcError>>;
    /// Must call the actual room owner with room-admin scoped grants, not mutate
    /// an API-only record. Identity is the genuine egress ID returned by worker.
    fn layout_metadata(
        &self,
        room: &str,
        identity: &str,
        metadata: String,
    ) -> BoxFuture<'_, Result<(), RpcError>>;
}
pub struct EgressGateway {
    client: Option<Arc<RpcClient>>,
    start_client: Option<Arc<RpcClient>>,
    io: Arc<IoInfo>,
    rooms: Arc<dyn EgressRooms>,
    base_timeout: Duration,
    policy: RpcPolicy,
}
impl EgressGateway {
    pub fn new(
        client: Option<Arc<RpcClient>>,
        io: Arc<IoInfo>,
        rooms: Arc<dyn EgressRooms>,
        configured_timeout: Duration,
    ) -> Self {
        Self {
            start_client: client.clone(),
            client,
            io,
            rooms,
            base_timeout: configured_timeout.max(Duration::from_secs(10)),
            policy: RpcPolicy::default(),
        }
    }
    /// EgressInternal forces its actual response subscription channel to1000;
    /// the handler client's channel consumes configured PSRPC buffer_size.
    pub fn with_start_client(mut self, client: Option<Arc<RpcClient>>) -> Self {
        self.start_client = client;
        self
    }
    pub fn with_rpc_policy(mut self, policy: RpcPolicy) -> Self {
        self.base_timeout =
            Duration::from_nanos(policy.timeout.as_nanos_i64().max(10_000_000_000) as u64);
        self.policy = policy;
        self
    }
    fn record(auth: Option<&VerifiedToken>) -> Result<(), RpcError> {
        ensure_record(auth).map_err(|e| RpcError::new("unauthenticated", e.to_string()))
    }
    fn client(&self) -> Result<&RpcClient, RpcError> {
        self.client
            .as_deref()
            .ok_or_else(|| RpcError::new("internal", "egress not connected (redis required)"))
    }
    pub async fn start(
        &self,
        auth: Option<&VerifiedToken>,
        request: internal::StartEgressRequest,
        options: CallOptions,
    ) -> Result<pb::EgressInfo, RpcError> {
        self.start_wire(auth, WireValue::from_message(request), options)
            .await
            .map(|v| v.message)
    }
    pub async fn start_wire(
        &self,
        auth: Option<&VerifiedToken>,
        value: WireValue<internal::StartEgressRequest>,
        options: CallOptions,
    ) -> Result<WireValue<pb::EgressInfo>, RpcError> {
        Self::record(auth)?;
        self.start_wire_inner(value, options, true).await
    }

    /// Internal room-owner launcher; public entry points check their own create
    /// or record grant first. This does not invent a roomRecord grant for a
    /// source CreateRoom request, whose owner launcher does not require it.
    pub(crate) async fn start_owned_wire(
        &self,
        value: WireValue<internal::StartEgressRequest>,
        options: CallOptions,
    ) -> Result<WireValue<pb::EgressInfo>, RpcError> {
        self.start_wire_inner(value, options, false).await
    }

    async fn start_wire_inner(
        &self,
        mut value: WireValue<internal::StartEgressRequest>,
        mut options: CallOptions,
        public_timeout: bool,
    ) -> Result<WireValue<pb::EgressInfo>, RpcError> {
        let request = &mut value.message;
        let client = self
            .start_client
            .as_deref()
            .ok_or_else(|| RpcError::new("internal", "egress not connected (redis required)"))?;
        if request.egress_id.is_empty() {
            request.egress_id = rtc_core::ids::new_sid("EG_");
        }
        if request.room_id.is_empty() {
            let name = request_room(request);
            if !name.is_empty() {
                request.room_id = self.rooms.room_sid(name).await?;
            }
        }
        options.selection = SelectionOptions {
            maximum_affinity: 1.0,
            affinity_timeout: Some(Duration::from_secs(1)),
            short_circuit_timeout: Some(Duration::from_millis(500)),
            ..Default::default()
        };
        let spec = MethodSpec {
            service: "EgressInternal".into(),
            method: "StartEgress".into(),
            topics: vec![String::new()],
            require_claim: true,
            queue: false,
        };
        // Source retry attempt starts at1. Same request/EgressID across retries.
        let mut outer = GenericRetry::new(self.policy);
        let info = 'outer: loop {
            let mut attempt = 1;
            let mut timeout = self.base_timeout;
            loop {
                let mut call = options.clone();
                call.timeout = timeout;
                let request = RawRequest {
                    message: value.message.clone(),
                    raw: Arc::from(value.encode()),
                };
                match client
                    .unary_raw_request::<_, pb::EgressInfo>(&spec, &request, call)
                    .await
                {
                    Ok(info) => {
                        break 'outer WireValue::decode(&info.raw).map_err(RpcError::from)?;
                    }
                    Err(error) => {
                        if attempt >= 3 || !egress_recoverable(&error) {
                            if outer.next(&error) {
                                if public_timeout
                                    && options.deadline.is_some_and(|deadline| {
                                        deadline <= std::time::Instant::now()
                                    })
                                {
                                    return Err(RpcError::new("canceled", "request canceled"));
                                }
                                continue 'outer;
                            }
                            return Err(error);
                        }
                        let mut random = [0u8; 8];
                        getrandom::fill(&mut random)
                            .map_err(|e| RpcError::new("unknown", e.to_string()))?;
                        let fraction =
                            (u64::from_le_bytes(random) >> 11) as f64 / (1u64 << 53) as f64;
                        let wait = Duration::from_secs_f64(
                            (1u64 << (attempt - 1)) as f64 * (1.0 + fraction),
                        );
                        timeout = self.base_timeout.saturating_mul(1u32 << attempt);
                        attempt += 1;
                        let sleep = tokio::time::sleep(wait);
                        if let Some(deadline) = options.deadline {
                            tokio::select! {
                                _=options.cancel.cancelled()=>return Err(RpcError::new("canceled","request canceled")),
                                _=tokio::time::sleep_until(deadline.into())=>{
                                    return Err(if public_timeout {
                                        // Fixed middleware/retry.go reports canceled when
                                        // its context closes during the retry wait.
                                        RpcError::new("canceled", "request canceled")
                                    } else {
                                        RpcError::new("deadline_exceeded", "request timed out")
                                    });
                                },
                                _=sleep=>{}
                            }
                        } else {
                            tokio::select! {_=options.cancel.cancelled()=>return Err(RpcError::new("canceled","request canceled")),_=sleep=>{}}
                        }
                    }
                }
            }
        };
        // IO failure does not erase an actual successful external recording launch.
        if let Err(error) = self.io.create_egress_wire(info.clone()).await {
            tracing::error!(code = error.code, "failed to create egress");
        }
        Ok(info)
    }
    pub async fn list(
        &self,
        auth: Option<&VerifiedToken>,
        request: pb::ListEgressRequest,
    ) -> Result<pb::ListEgressResponse, RpcError> {
        Self::record(auth)?;
        self.io.list_egress(request).await
    }
    pub async fn list_wire(
        &self,
        auth: Option<&VerifiedToken>,
        request: pb::ListEgressRequest,
    ) -> Result<WireValue<pb::ListEgressResponse>, RpcError> {
        Self::record(auth)?;
        self.io.list_egress_wire(request).await
    }
    pub async fn update_layout(
        &self,
        auth: Option<&VerifiedToken>,
        request: pb::UpdateLayoutRequest,
    ) -> Result<pb::EgressInfo, RpcError> {
        self.update_layout_wire(auth, request)
            .await
            .map(|v| v.message)
    }
    pub async fn update_layout_wire(
        &self,
        auth: Option<&VerifiedToken>,
        request: pb::UpdateLayoutRequest,
    ) -> Result<WireValue<pb::EgressInfo>, RpcError> {
        Self::record(auth)?;
        let value = self.io.get_egress_wire(&request.egress_id).await?;
        let metadata = layout_metadata(&request.layout)?;
        self.rooms
            .layout_metadata(&value.message.room_name, &value.message.egress_id, metadata)
            .await?;
        Ok(value)
    }
    async fn handler_call<Request>(
        &self,
        spec: &MethodSpec,
        request: WireValue<Request>,
        mut options: CallOptions,
    ) -> Result<WireValue<pb::EgressInfo>, RpcError>
    where
        Request: prost::Message + prost::Name + Default + PartialEq + Clone,
    {
        let client = self.client()?;
        // Fixed psrpc/pkg/client/options.go: no-affinity methods accept the
        // first eligible server; timeout controls selection, not handler budget.
        options.selection = SelectionOptions {
            accept_first: true,
            affinity_timeout: Some(Duration::from_secs(1)),
            ..Default::default()
        };
        let mut retry = GenericRetry::new(self.policy);
        let raw = RawRequest {
            message: request.message.clone(),
            raw: Arc::from(request.encode()),
        };
        loop {
            let mut call = options.clone();
            if let Some(timeout) = retry.timeout() {
                call.timeout = timeout;
            }
            match client
                .unary_raw_request::<_, pb::EgressInfo>(spec, &raw, call)
                .await
            {
                Ok(response) => return WireValue::decode(&response.raw).map_err(Into::into),
                Err(error) => {
                    if !retry.next(&error) {
                        return Err(error);
                    }
                }
            }
        }
    }
    pub async fn update_stream(
        &self,
        auth: Option<&VerifiedToken>,
        request: pb::UpdateStreamRequest,
        options: CallOptions,
    ) -> Result<pb::EgressInfo, RpcError> {
        self.update_stream_wire(auth, WireValue::from_message(request), options)
            .await
            .map(|v| v.message)
    }
    pub async fn update_stream_wire(
        &self,
        auth: Option<&VerifiedToken>,
        request: WireValue<pb::UpdateStreamRequest>,
        options: CallOptions,
    ) -> Result<WireValue<pb::EgressInfo>, RpcError> {
        Self::record(auth)?;
        self.client()?;
        let spec = MethodSpec {
            service: "EgressHandler".into(),
            method: "UpdateStream".into(),
            topics: vec![request.message.egress_id.clone()],
            require_claim: true,
            queue: true,
        };
        match self.handler_call(&spec, request.clone(), options).await {
            Ok(info) => Ok(info),
            Err(error) => {
                let current = self.io.get_egress(&request.message.egress_id).await?;
                if matches!(current.status, 0 | 1) {
                    Err(error)
                } else {
                    Err(RpcError::new(
                        "failed_precondition",
                        format!(
                            "egress with status {} cannot be updated",
                            status_name(current.status)
                        ),
                    ))
                }
            }
        }
    }
    pub async fn stop(
        &self,
        auth: Option<&VerifiedToken>,
        request: pb::StopEgressRequest,
        options: CallOptions,
    ) -> Result<pb::EgressInfo, RpcError> {
        self.stop_wire(auth, WireValue::from_message(request), options)
            .await
            .map(|v| v.message)
    }
    /// Only actual local ErrNoResponse maps to timeout; a remote worker's
    /// identical-looking unavailable error retains its origin and code.
    pub async fn stop_wire(
        &self,
        auth: Option<&VerifiedToken>,
        request: WireValue<pb::StopEgressRequest>,
        options: CallOptions,
    ) -> Result<WireValue<pb::EgressInfo>, RpcError> {
        self.stop_inner(auth, request, options)
            .await
            .map_err(|error| {
                if error.is_local_no_response() {
                    RpcError::new("deadline_exceeded", "request timed out")
                } else {
                    error
                }
            })
    }
    async fn stop_inner(
        &self,
        auth: Option<&VerifiedToken>,
        request: WireValue<pb::StopEgressRequest>,
        options: CallOptions,
    ) -> Result<WireValue<pb::EgressInfo>, RpcError> {
        Self::record(auth)?;
        self.client()?;
        let spec = MethodSpec {
            service: "EgressHandler".into(),
            method: "StopEgress".into(),
            topics: vec![request.message.egress_id.clone()],
            require_claim: true,
            queue: true,
        };
        match self.handler_call(&spec, request.clone(), options).await {
            Ok(info) => Ok(info),
            Err(error) => {
                let current = self.io.get_egress(&request.message.egress_id).await?;
                if matches!(current.status, 0 | 1) {
                    Err(error)
                } else {
                    Err(RpcError::new(
                        "failed_precondition",
                        format!(
                            "egress with status {} cannot be stopped",
                            status_name(current.status)
                        ),
                    ))
                }
            }
        }
    }
}
fn request_room(request: &internal::StartEgressRequest) -> &str {
    use internal::start_egress_request::Request;
    match request.request.as_ref() {
        Some(Request::Egress(r)) => &r.room_name,
        Some(Request::RoomComposite(r)) => &r.room_name,
        Some(Request::Participant(r)) => &r.room_name,
        Some(Request::TrackComposite(r)) => &r.room_name,
        Some(Request::Track(r)) => &r.room_name,
        Some(Request::Web(_)) | Some(Request::Replay(_)) | None => "",
    }
}
fn status_name(status: i32) -> String {
    pb::EgressStatus::try_from(status)
        .map(|value| value.as_str_name().to_owned())
        .unwrap_or_else(|_| status.to_string())
}
/// Each public Start method constructs its real internal oneof, preserving the
/// caller-supplied middleware EgressID. No internal worker flags are invented.
pub fn wrap_start(request: pb::StartEgressRequest, id: String) -> internal::StartEgressRequest {
    internal::StartEgressRequest {
        egress_id: id,
        request: Some(internal::start_egress_request::Request::Egress(request)),
        ..Default::default()
    }
}
pub fn wrap_room(
    request: pb::RoomCompositeEgressRequest,
    id: String,
) -> internal::StartEgressRequest {
    internal::StartEgressRequest {
        egress_id: id,
        request: Some(internal::start_egress_request::Request::RoomComposite(
            request,
        )),
        ..Default::default()
    }
}
pub fn wrap_web(request: pb::WebEgressRequest, id: String) -> internal::StartEgressRequest {
    internal::StartEgressRequest {
        egress_id: id,
        request: Some(internal::start_egress_request::Request::Web(request)),
        ..Default::default()
    }
}
pub fn wrap_participant(
    request: pb::ParticipantEgressRequest,
    id: String,
) -> internal::StartEgressRequest {
    internal::StartEgressRequest {
        egress_id: id,
        request: Some(internal::start_egress_request::Request::Participant(
            request,
        )),
        ..Default::default()
    }
}
pub fn wrap_tracks(
    request: pb::TrackCompositeEgressRequest,
    id: String,
) -> internal::StartEgressRequest {
    internal::StartEgressRequest {
        egress_id: id,
        request: Some(internal::start_egress_request::Request::TrackComposite(
            request,
        )),
        ..Default::default()
    }
}
pub fn wrap_track(request: pb::TrackEgressRequest, id: String) -> internal::StartEgressRequest {
    internal::StartEgressRequest {
        egress_id: id,
        request: Some(internal::start_egress_request::Request::Track(request)),
        ..Default::default()
    }
}
macro_rules! preserved_start_wrapper {
    ($name:ident,$typed:ident,$request:ty,$field:literal) => {
        pub fn $name(
            request: WireValue<$request>,
            id: String,
        ) -> WireValue<internal::StartEgressRequest> {
            let mut value = WireValue::from_message($typed(request.message, id));
            value.wire.replace_nested($field, Some(&request.wire));
            value
        }
    };
}
preserved_start_wrapper!(
    wrap_start_wire,
    wrap_start,
    pb::StartEgressRequest,
    "egress"
);
preserved_start_wrapper!(
    wrap_room_wire,
    wrap_room,
    pb::RoomCompositeEgressRequest,
    "room_composite"
);
preserved_start_wrapper!(wrap_web_wire, wrap_web, pb::WebEgressRequest, "web");
preserved_start_wrapper!(
    wrap_participant_wire,
    wrap_participant,
    pb::ParticipantEgressRequest,
    "participant"
);
preserved_start_wrapper!(
    wrap_tracks_wire,
    wrap_tracks,
    pb::TrackCompositeEgressRequest,
    "track_composite"
);
preserved_start_wrapper!(wrap_track_wire, wrap_track, pb::TrackEgressRequest, "track");

/// Go encoding/json.Marshal's default EscapeHTML behavior is observable in
/// participant metadata, which is a JSON string rather than a parsed object.
fn layout_metadata(layout: &str) -> Result<String, RpcError> {
    let json = serde_json::to_string(&serde_json::json!({"layout": layout}))
        .map_err(|e| RpcError::new("unknown", e.to_string()))?;
    let mut escaped = String::with_capacity(json.len());
    for ch in json.chars() {
        match ch {
            '<' => escaped.push_str("\\u003c"),
            '>' => escaped.push_str("\\u003e"),
            '&' => escaped.push_str("\\u0026"),
            '\u{2028}' => escaped.push_str("\\u2028"),
            '\u{2029}' => escaped.push_str("\\u2029"),
            _ => escaped.push(ch),
        }
    }
    Ok(escaped)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn layout_metadata_escapes_go_html_and_javascript_separators() {
        let value = layout_metadata("<>&\u{2028}\u{2029}\"\\\n").unwrap();
        assert_eq!(
            value,
            r#"{"layout":"\u003c\u003e\u0026\u2028\u2029\"\\\n"}"#
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&value).unwrap()["layout"],
            "<>&\u{2028}\u{2029}\"\\\n"
        );
    }

    #[test]
    #[ignore = "requires actual Go encoding/json oracle output"]
    fn layout_metadata_matches_actual_go_marshal_rows() {
        let path = std::env::var("WORKERS_LAYOUT_GO_RESULTS").expect("actual Go results path");
        let rows: Vec<serde_json::Value> =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(rows.len(), 7);
        for row in rows {
            let input = row["layout"].as_str().unwrap();
            assert_eq!(
                layout_metadata(input).unwrap(),
                row["metadata"].as_str().unwrap(),
                "{}",
                row["name"]
            );
        }
    }

    #[test]
    fn only_actual_request_room_requires_lookup_and_public_oneof_is_retained() {
        let request = wrap_room(
            pb::RoomCompositeEgressRequest {
                room_name: "fixture-room".into(),
                ..Default::default()
            },
            "EG_fixture".into(),
        );
        assert_eq!(request_room(&request), "fixture-room");
        assert_eq!(request.egress_id, "EG_fixture");
        assert_eq!(
            request_room(&wrap_web(pb::WebEgressRequest::default(), String::new())),
            ""
        );
        assert!(!egress_recoverable(&RpcError::new(
            "permission_denied",
            "x"
        )));
        assert!(egress_recoverable(&RpcError::new(
            "resource_exhausted",
            "x"
        )));
        assert_eq!(status_name(3), "EGRESS_COMPLETE");
    }
}
