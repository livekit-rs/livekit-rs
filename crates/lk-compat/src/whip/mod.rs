//! Native WHIP transport adapter. Fixed source: whipservice.go and
//! roommanager_service.go at v1.13.10, Apache-2.0. Real RTC owns every response.
mod client_info;
pub mod cluster;
mod fragment;
mod http_policy;
mod notify;
mod offer;
#[cfg(test)]
mod oracle_tests;
mod runtime;
mod sdp_syntax;
#[cfg(test)]
mod track_oracle_tests;
use crate::{auth::RequestAuth, state::AppState, workers::retry::RpcPolicy};
use axum::{
    Router,
    body::Body,
    extract::{Path, Request, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, header},
    response::Response,
    routing::any,
};
use futures_util::StreamExt;
use rtc_core::{
    ids::{Identity, ParticipantSid, RoomKey},
    permission::{Grants, RoomConfig},
    session::{ClientMeta, JoinParams, NamedSubscription},
};
use runtime::{Failure, WhipSessions};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
/// One service owns all HTTP session lifetimes, with no AppState/RoomManager cycle.
pub struct WhipService {
    sessions: WhipSessions,
    policy: RpcPolicy,
    notifiers: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}
impl WhipService {
    pub fn new(manager: rtc_runtime::RoomManager, policy: RpcPolicy) -> Arc<Self> {
        Arc::new(Self {
            sessions: WhipSessions::new(manager),
            policy,
            notifiers: Mutex::new(Vec::new()),
        })
    }
    fn deadline(&self) -> Instant {
        let duration = self
            .policy
            .timeout
            .nonnegative_to_std()
            .filter(|v| !v.is_zero())
            .unwrap_or(Duration::from_secs(3));
        Instant::now()
            .checked_add(duration)
            .unwrap_or_else(Instant::now)
    }
    /// Root closes real rooms first; completion watches then finish true Closed RPCs.
    pub async fn shutdown(&self) {
        self.sessions.shutdown().await;
        for task in self.notifiers.lock().await.drain(..) {
            let _ = task.await;
        }
    }
    async fn notify(
        &self,
        state: &AppState,
        room: RoomKey,
        identity: Identity,
        sid: ParticipantSid,
    ) {
        let done = self.sessions.done(&sid).await;
        let client = state
            .worker_services
            .as_ref()
            .map(|w| w.ingress_notify_client());
        let task = tokio::spawn(notify::run(
            state.manager.clone(),
            room,
            identity,
            sid,
            client,
            done,
        ));
        let mut tasks = self.notifiers.lock().await;
        tasks.retain(|task| !task.is_finished());
        tasks.push(task);
    }
}
pub fn routes(state: AppState) -> Router {
    Router::new()
        .route("/whip/v1", any(root))
        .route("/whip/v1/{participant_id}", any(participant))
        .with_state(state)
}
fn response(status: StatusCode, content_type: Option<&str>, body: impl Into<Body>) -> Response {
    let mut response = Response::new(body.into());
    *response.status_mut() = status;
    if let Some(value) = content_type {
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_str(value).expect("static content-type"),
        );
    }
    response
}
fn error(status: StatusCode, content_type: Option<&str>, message: impl AsRef<str>) -> Response {
    // Go json.Encoder newline + HTML escaping. This is the WHIP error envelope,
    // independent of the Twirp public API JSON codec.
    let quoted = serde_json::to_string(message.as_ref())
        .expect("string JSON")
        .replace('&', "\\u0026")
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029");
    response(
        status,
        content_type.or(Some("text/plain; charset=utf-8")),
        format!("{{\"error\":{quoted}}}\n"),
    )
}
fn policy_error(value: http_policy::Failure, content: Option<&str>) -> Response {
    error(value.status, content, value.message)
}
fn principal<'a>(
    state: &AppState,
    request: &'a Request,
    create: bool,
) -> Result<&'a Grants, http_policy::Failure> {
    let auth = request.extensions().get::<RequestAuth>();
    let verified = auth.and_then(|a| a.0.as_deref());
    http_policy::principal(
        verified.map(|v| &v.grants),
        verified.is_some_and(|v| v.extra.has_video),
        create,
        &state.config.limits,
    )?;
    Ok(&verified.expect("principal validated").grants)
}
async fn read_body(request: Request, label: &str) -> Result<Vec<u8>, http_policy::Failure> {
    let mut stream = request.into_body().into_data_stream();
    let mut bytes = Vec::new();
    while let Some(next) = stream.next().await {
        let chunk = next.map_err(|e| {
            if let Some(limit) = crate::http::api_body_limit(&e) {
                http_policy::Failure {
                    status: StatusCode::PAYLOAD_TOO_LARGE,
                    message: format!("request body exceeds {limit} bytes"),
                }
            } else {
                http_policy::Failure {
                    status: StatusCode::BAD_REQUEST,
                    message: format!("body does not have SDP {label}: {e}"),
                }
            }
        })?;
        if chunk.len() > http_policy::BODY_LIMIT - bytes.len() {
            return Err(http_policy::Failure {
                status: StatusCode::PAYLOAD_TOO_LARGE,
                message: format!("request body exceeds {} bytes", http_policy::BODY_LIMIT),
            });
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}
async fn root(State(state): State<AppState>, request: Request) -> Response {
    match *request.method() {
        Method::GET | Method::HEAD => response(StatusCode::NO_CONTENT, None, Body::empty()),
        Method::OPTIONS => {
            let mut r = response(StatusCode::OK, None, Body::empty());
            for (name, value) in [
                ("access-control-allow-origin", "*"),
                ("access-control-allow-headers", "*"),
                (
                    "access-control-allow-methods",
                    "PATCH, OPTIONS, GET, POST, DELETE",
                ),
                ("access-control-expose-headers", "*"),
            ] {
                r.headers_mut()
                    .insert(name, HeaderValue::from_static(value));
            }
            r
        }
        Method::POST => create(state, request).await,
        _ => crate::http::not_found().await,
    }
}
async fn participant(
    State(state): State<AppState>,
    Path(id): Path<String>,
    request: Request,
) -> Response {
    let sid = ParticipantSid(id);
    match *request.method() {
        Method::GET | Method::HEAD => response(StatusCode::NO_CONTENT, None, Body::empty()),
        Method::PATCH => patch(state, request, sid).await,
        Method::DELETE => delete(state, request, sid).await,
        _ => crate::http::not_found().await,
    }
}
fn client(query: Option<&str>, headers: &HeaderMap) -> ClientMeta {
    let param =
        |name: &str| crate::auth::query_param(query.unwrap_or_default(), name).unwrap_or_default();
    let mut client = ClientMeta {
        sdk: crate::convert::sdk_from_query(&param("sdk")),
        version: param("version"),
        client_protocol: param("client_protocol").parse().unwrap_or(0),
        protocol: param("protocol").parse().unwrap_or(0),
        os: param("os"),
        os_version: param("os_version"),
        device_model: param("device_model"),
        browser: param("browser"),
        browser_version: param("browser_version"),
        network: param("network"),
        ..Default::default()
    };
    if client.protocol == 0 {
        client.protocol = 17;
    }
    crate::signal::params::augment_client_info(&mut client, headers, None);
    client
}
fn merge_preset(mut value: RoomConfig, preset: &RoomConfig) -> RoomConfig {
    value.inherit(preset);
    value
}

fn prepare_owner(state: &AppState, grants: &mut Grants) -> Result<(), Failure> {
    let room = state.room_key(&grants.video.room);
    if state
        .config
        .limits
        .max_metadata_size
        .is_some_and(|max| grants.metadata.len() > max)
    {
        state.manager.record_rtc_init();
        return Err(Failure::Internal("metadata size exceeds limits".into()));
    }
    if state.config.limits.max_attributes_size.is_some_and(|max| {
        grants
            .attributes
            .iter()
            .map(|(k, v)| k.len() + v.len())
            .sum::<usize>()
            > max
    }) {
        state.manager.record_rtc_init();
        return Err(Failure::Internal("attribute size exceeds limits".into()));
    }
    // Real StartSession checks metadata/attributes before getOrCreateRoom applies the preset.
    if !state.manager.room_exists(&room) && !grants.room_preset.is_empty() {
        let Some(preset) = state.config.room.presets.get(&grants.room_preset) else {
            state.manager.record_rtc_init();
            state.manager.record_rtc_canceled(1);
            return Err(Failure::Internal(
                "unknown room configuration in create room request".into(),
            ));
        };
        // A nil preset (`name: ~`) is dereferenced upstream; negroni
        // Recovery turns that handler panic into a bare 500.
        let Some(preset) = preset else {
            state.manager.record_rtc_init();
            return Err(Failure::Internal("nil room configuration".into()));
        };
        grants.room_config = Some(merge_preset(
            grants.room_config.clone().unwrap_or_default(),
            preset,
        ));
    }
    Ok(())
}

async fn create(state: AppState, request: Request) -> Response {
    const CONTENT: &str = "application/sdp";
    if let Err(e) = http_policy::content_type(request.headers(), CONTENT) {
        return policy_error(e, None);
    }
    let mut grants = match principal(&state, &request, true) {
        Ok(g) => g.clone(),
        Err(r) => return policy_error(r, Some(CONTENT)),
    };
    let header = request
        .headers()
        .get("x-livekit-clientinfo")
        .map(|v| v.as_bytes())
        .unwrap_or_default();
    let info = if header.is_empty() {
        client_info::ClientInfo::default()
    } else {
        match client_info::decode(header) {
            Ok(v) => v,
            Err(e) => {
                return error(
                    StatusCode::BAD_REQUEST,
                    Some(CONTENT),
                    format!("malformed json in client info header: {e}"),
                );
            }
        }
    };
    // Fixed source decodes ClientIP but does not forward it in WHIPCreateRequest.
    let _client_ip = info.client_ip;
    let from_ingress = request
        .headers()
        .get("x-livekit-ingress")
        .is_some_and(|v| !v.is_empty());
    let meta = client(request.uri().query(), request.headers());
    let body = match read_body(request, "offer").await {
        Ok(v) => v,
        Err(r) => return policy_error(r, Some(CONTENT)),
    };
    if let Err(e) = http_policy::create_body(&body) {
        return policy_error(e, Some(CONTENT));
    }
    // Source Pion grammar only at HTTP; real RTC semantics are checked later.
    if let Err(e) = sdp_syntax::validate(&body) {
        return error(
            StatusCode::BAD_REQUEST,
            Some(CONTENT),
            format!("malformed SDP offer: {e}"),
        );
    }
    // WHIPService.Create selects a node after validateCreate and before
    // StartSession; local routing still applies the configured node limits.
    if let Err(allocation_error) = state.check_local_room_allocation() {
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            Some(CONTENT),
            allocation_error.message,
        );
    }
    let offer = match String::from_utf8(body) {
        Ok(v) => v,
        Err(e) => {
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                Some(CONTENT),
                format!("SDP text cannot be represented by native RTC: {e}"),
            );
        }
    };
    let room = state.room_key(&grants.video.room);
    if state.whip_rpc.is_none() {
        if let Err(e) = prepare_owner(&state, &mut grants) {
            if matches!(&e,Failure::Internal(message) if message=="nil room configuration") {
                return axum::response::IntoResponse::into_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                );
            }
            return runtime_error(e, CONTENT, true, false);
        }
    }
    let identity = Identity(grants.identity.clone());
    let mut capabilities = crate::profile::capabilities(&meta);
    capabilities.one_shot = true;
    let params = JoinParams {
        room: room.clone(),
        identity: identity.clone(),
        name: grants.name.clone(),
        metadata: grants.metadata.clone(),
        attributes: grants.attributes.clone(),
        capabilities,
        client: meta,
        auto_subscribe: true,
        auto_subscribe_data_track: false,
        adaptive_stream: false,
        single_pc: true,
        disable_ice_lite: true,
        subscriber_allow_pause: None,
        reconnect: false,
        reconnect_reason: 0,
        participant_sid: None,
        add_tracks: offer::synthesize(&offer),
        publisher_offer: None,
        grants,
    };
    let targets = info
        .subscribed
        .into_iter()
        .flat_map(|(publisher, names)| {
            names.into_iter().map(move |track_name| NamedSubscription {
                publisher: Identity(publisher.clone()),
                track_name,
            })
        })
        .collect();
    let (answer, ice_servers) = if let Some(rpc) = &state.whip_rpc {
        match rpc.create(&params, offer, targets, from_ingress).await {
            Ok(result) => (
                runtime::Answer {
                    sid: ParticipantSid(result.participant_id),
                    sdp: result.answer_sdp,
                    ice_session_id: result.ice_session_id,
                },
                result.ice_servers,
            ),
            Err(error) => {
                let status = if error.application_metadata.contains_key("whip_allocation") {
                    StatusCode::INTERNAL_SERVER_ERROR
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                };
                return self::error(status, Some(CONTENT), error.message);
            }
        }
    } else {
        let answer = match state
            .whip
            .sessions
            .create(params, offer, targets, state.whip.deadline())
            .await
        {
            Ok(v) => v,
            Err(e) => return runtime_error(e, CONTENT, true, false),
        };
        if from_ingress {
            state
                .whip
                .notify(&state, room, identity, answer.sid.clone())
                .await;
        }
        let servers = state
            .keys
            .first()
            .map(|(key, _)| crate::signal::ice::ice_servers(&state, key, &answer.sid))
            .unwrap_or_default();
        (answer, servers)
    };
    let mut r = response(StatusCode::CREATED, Some(CONTENT), answer.sdp);
    insert(
        &mut r,
        header::LOCATION,
        format!("/whip/v1/{}", answer.sid.0),
    );
    insert(&mut r, header::ETAG, answer.ice_session_id);
    {
        for server in ice_servers {
            for url in server.urls {
                let mut value = format!("<{url}>; rel=\"ice-server\"");
                for (name, text) in [
                    ("username", &server.username),
                    ("credential", &server.credential),
                ] {
                    if !text.is_empty() {
                        value.push_str(&format!(
                            "; {name}=\"{}\"",
                            text.replace('\\', "\\\\").replace('"', "\\\"")
                        ));
                    }
                }
                if let Ok(value) = HeaderValue::from_str(&value) {
                    r.headers_mut().append(header::LINK, value);
                }
            }
        }
    }
    r
}
fn insert(response: &mut Response, name: header::HeaderName, value: String) {
    if let Ok(value) = HeaderValue::from_str(&value) {
        response.headers_mut().insert(name, value);
    }
}
async fn patch(state: AppState, request: Request, sid: ParticipantSid) -> Response {
    const CONTENT: &str = "application/trickle-ice-sdpfrag";
    if let Err(e) = http_policy::content_type(request.headers(), CONTENT) {
        return policy_error(e, None);
    }
    let tag = match http_policy::if_match(request.headers()) {
        Ok(v) => v,
        Err(e) => return policy_error(e, Some(CONTENT)),
    };
    let grants = match principal(&state, &request, false) {
        Ok(v) => v.clone(),
        Err(r) => return policy_error(r, Some(CONTENT)),
    };
    if sid.0.is_empty() {
        return error(
            StatusCode::BAD_REQUEST,
            Some(CONTENT),
            "participant ID cannot be empty",
        );
    }
    let body = match read_body(request, "fragment").await {
        Ok(v) => v,
        Err(r) => return policy_error(r, Some(CONTENT)),
    };
    let fragment = String::from_utf8_lossy(&body).into_owned();
    let room = state.room_key(&grants.video.room);
    let identity = Identity(grants.identity);
    if let Some(rpc) = &state.whip_rpc {
        return if tag == "*" {
            match rpc.restart(&room, &identity, &sid, fragment).await {
                Ok(answer) => {
                    let mut r = response(StatusCode::OK, Some(CONTENT), answer.sdp_fragment);
                    insert(&mut r, header::ETAG, answer.ice_session_id);
                    r
                }
                Err(e) => runtime_error(e, CONTENT, false, true),
            }
        } else {
            match rpc.trickle(&room, &identity, &sid, tag, fragment).await {
                Ok(()) => response(StatusCode::NO_CONTENT, Some(CONTENT), Body::empty()),
                Err(e) => runtime_error(e, CONTENT, false, false),
            }
        };
    }
    if tag == "*" {
        match state
            .whip
            .sessions
            .restart(&room, &identity, &sid, fragment, state.whip.deadline())
            .await
        {
            Ok(answer) => {
                match fragment::Document::from_validated(&answer.sdp).extract_fragment() {
                    Ok(fragment) => {
                        let mut r = response(StatusCode::OK, Some(CONTENT), fragment.marshal());
                        insert(&mut r, header::ETAG, answer.ice_session_id);
                        r
                    }
                    Err(e) => error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Some(CONTENT),
                        format!("local SDP fragment: {e:?}"),
                    ),
                }
            }
            Err(e) => runtime_error(e, CONTENT, false, true),
        }
    } else {
        match state
            .whip
            .sessions
            .trickle(&room, &identity, &sid, tag, fragment, state.whip.deadline())
            .await
        {
            Ok(()) => response(StatusCode::NO_CONTENT, Some(CONTENT), Body::empty()),
            Err(e) => runtime_error(e, CONTENT, false, false),
        }
    }
}
async fn delete(state: AppState, request: Request, sid: ParticipantSid) -> Response {
    // DELETE does not set SDP content-type, unlike PATCH/Create.
    let auth = request
        .extensions()
        .get::<RequestAuth>()
        .and_then(|a| a.0.as_deref());
    if let Err(e) = http_policy::principal(
        auth.map(|a| &a.grants),
        auth.is_some_and(|a| a.extra.has_video),
        false,
        &state.config.limits,
    ) {
        return policy_error(e, None);
    }
    let grants = &auth.expect("principal validated").grants;
    let room = state.room_key(&grants.video.room);
    let identity = Identity(grants.identity.clone());
    let result = if let Some(rpc) = &state.whip_rpc {
        rpc.delete(&room, &identity, &sid).await
    } else {
        state
            .whip
            .sessions
            .delete(&room, &identity, &sid, state.whip.deadline())
            .await
    };
    match result {
        Ok(()) => response(StatusCode::OK, None, Body::empty()),
        Err(Failure::RoomNotFound) => {
            error(StatusCode::NOT_FOUND, None, "requested room does not exist")
        }
        Err(e) => error(StatusCode::NOT_FOUND, None, failure_message(e)),
    }
}
fn failure_message(error: Failure) -> String {
    match error {
        Failure::RoomNotFound => "requested room does not exist".into(),
        Failure::NotFound => "participant does not exist".into(),
        Failure::RoomGone => "participant is closed".into(),
        Failure::InvalidFragment => "invalid sdp fragment".into(),
        Failure::MidMismatch => "media mid does not match bundle mid".into(),
        Failure::CredentialMismatch => "ice credential mismatch".into(),
        Failure::IceSessionMismatch { expected, got } => {
            format!("ice session mismatch, expected: {expected}, got: {got}")
        }
        Failure::NoResponse => "no response from servers".into(),
        Failure::Deadline => "request timed out".into(),
        Failure::Internal(message) => message,
        Failure::Remote(error) => error.message,
    }
}
fn runtime_error(cause: Failure, content: &str, create: bool, restart: bool) -> Response {
    let status = if create {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        match cause {
            Failure::Remote(ref error) if error.code == "not_found" => StatusCode::NOT_FOUND,
            Failure::Remote(ref error)
                if error.code == "invalid_argument"
                    && (error.message == "invalid sdp fragment"
                        || (!restart
                            && matches!(
                                error.message.as_str(),
                                "media mid does not match bundle mid" | "ice credential mismatch"
                            ))) =>
            {
                StatusCode::BAD_REQUEST
            }
            Failure::RoomNotFound | Failure::NotFound => StatusCode::NOT_FOUND,
            Failure::InvalidFragment => StatusCode::BAD_REQUEST,
            Failure::MidMismatch | Failure::CredentialMismatch if !restart => {
                StatusCode::BAD_REQUEST
            }
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    };
    error(status, Some(content), failure_message(cause))
}

impl Drop for WhipService {
    fn drop(&mut self) {
        for task in self.notifiers.get_mut().drain(..) {
            task.abort();
        }
    }
}
#[cfg(test)]
mod http_tests {
    use super::*;
    fn request(
        method: &str,
        uri: &str,
        content: Option<&str>,
        tag: Option<&str>,
        grants: Option<Grants>,
    ) -> Request {
        let mut builder = Request::builder().method(method).uri(uri);
        if let Some(content) = content {
            builder = builder.header("content-type", content);
        }
        if let Some(tag) = tag {
            builder = builder.header("if-match", tag);
        }
        let mut request = builder.body(Body::empty()).unwrap();
        if let Some(grants) = grants {
            request.extensions_mut().insert(RequestAuth(Some(Arc::new(
                crate::auth::VerifiedToken {
                    grants,
                    extra: crate::auth::ExtraGrants {
                        has_video: true,
                        ..Default::default()
                    },
                },
            ))));
        }
        request
    }
    async fn text(response: Response) -> String {
        String::from_utf8(
            axum::body::to_bytes(response.into_body(), 1 << 20)
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap()
    }
    async fn finish(state: &AppState, net: rtc_runtime::net::NetRouter) {
        state.manager.close_all().await;
        state.agents.shutdown().await;
        state.shutdown_whip().await;
        net.shutdown();
    }
    #[tokio::test]
    async fn go_mux_unregistered_methods_are_not_found_without_allow() {
        let (state, net) = crate::test_support::state().await;
        for (method, resource) in [("PUT", false), ("PUT", true), ("OPTIONS", true)] {
            let r = if resource {
                participant(
                    State(state.clone()),
                    Path("PA_absent".into()),
                    request(method, "/whip/v1/PA_absent", None, None, None),
                )
                .await
            } else {
                root(
                    State(state.clone()),
                    request(method, "/whip/v1", None, None, None),
                )
                .await
            };
            assert_eq!(r.status(), StatusCode::NOT_FOUND);
            assert!(!r.headers().contains_key(header::ALLOW));
            assert_eq!(text(r).await, "404 page not found\n");
        }
        finish(&state, net).await;
    }

    #[tokio::test]
    async fn source_validation_order_and_source_error_content_types() {
        let (state, net) = crate::test_support::state().await;
        let r = root(
            State(state.clone()),
            request(
                "POST",
                "/whip/v1",
                Some("application/sdp; charset=utf-8"),
                None,
                None,
            ),
        )
        .await;
        assert_eq!(r.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            r.headers()[header::CONTENT_TYPE],
            "text/plain; charset=utf-8"
        );
        assert_eq!(
            text(r).await,
            "{\"error\":\"unsupported content-type: application/sdp; charset=utf-8\"}\n"
        );
        let r = root(
            State(state.clone()),
            request("POST", "/whip/v1", Some("application/sdp"), None, None),
        )
        .await;
        assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(r.headers()[header::CONTENT_TYPE], "application/sdp");
        assert_eq!(
            text(r).await,
            "{\"error\":\"no permissions to access the room\"}\n"
        );
        let r = participant(
            State(state.clone()),
            Path("PA_missing".into()),
            request(
                "PATCH",
                "/whip/v1/PA_missing",
                Some("application/trickle-ice-sdpfrag"),
                None,
                None,
            ),
        )
        .await;
        assert_eq!(r.status(), StatusCode::PRECONDITION_REQUIRED);
        assert_eq!(text(r).await, "{\"error\":\"missing entity tag\"}\n");
        let r = participant(
            State(state.clone()),
            Path("PA_missing".into()),
            request("GET", "/whip/v1/PA_missing", None, None, None),
        )
        .await;
        assert_eq!(r.status(), StatusCode::NO_CONTENT);
        let r = root(
            State(state.clone()),
            request("OPTIONS", "/whip/v1", None, None, None),
        )
        .await;
        assert_eq!(r.status(), StatusCode::OK);
        assert_eq!(
            r.headers()["access-control-allow-methods"],
            "PATCH, OPTIONS, GET, POST, DELETE"
        );
        assert!(!r.headers().contains_key(header::LINK));
        finish(&state, net).await;
    }
    #[tokio::test]
    async fn delete_requires_an_actual_registered_whip_identity_topic() {
        let (state, net) = crate::test_support::state().await;
        let mut grants = Grants {
            identity: "owner".into(),
            ..Default::default()
        };
        grants.video.room_join = true;
        grants.video.room = "whip-room".into();
        let r = delete(
            state.clone(),
            request(
                "DELETE",
                "/whip/v1/PA_missing",
                None,
                None,
                Some(grants.clone()),
            ),
            ParticipantSid("PA_missing".into()),
        )
        .await;
        assert_eq!(r.status(), StatusCode::NOT_FOUND);
        assert_eq!(text(r).await, "{\"error\":\"no response from servers\"}\n");
        state
            .manager
            .create_room(state.room_key("whip-room"), Default::default())
            .await
            .unwrap();
        let r = delete(
            state.clone(),
            request("DELETE", "/whip/v1/PA_missing", None, None, Some(grants)),
            ParticipantSid("PA_missing".into()),
        )
        .await;
        assert_eq!(r.status(), StatusCode::NOT_FOUND);
        assert_eq!(text(r).await, "{\"error\":\"no response from servers\"}\n");
        assert!(state.manager.room_exists(&state.room_key("whip-room")));
        finish(&state, net).await;
    }
}

#[cfg(test)]
mod sdp_syntax_oracle_tests;
#[cfg(test)]
mod video_oracle_tests;

#[cfg(test)]
mod whip_http_integration_tests {
    //! HTTP-level integration tests for `/whip/v1` and `/whip/v1/{participant_id}`
    //! exercising the full middleware stack (auth, CORS, body limit, path normalization,
    //! recovery) before WHIP handling.

    use axum::body::{Body, to_bytes};
    use axum::extract::ConnectInfo;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    const TEST_REMOTE: &str = "127.0.0.1:50000";

    fn remote_addr() -> ConnectInfo<std::net::SocketAddr> {
        ConnectInfo(TEST_REMOTE.parse().unwrap())
    }

    async fn send_request(
        state: &crate::state::AppState,
        method: &str,
        uri: &str,
        headers: Vec<(&str, &str)>,
    ) -> (StatusCode, String, bool) {
        let mut builder = Request::builder()
            .method(method)
            .uri(uri)
            .extension(remote_addr());
        for (k, v) in headers {
            builder = builder.header(k, v);
        }
        let resp = crate::http::router(state.clone())
            .oneshot(builder.body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = resp.status();
        let cors = resp.headers().get("access-control-allow-origin").is_some();
        let bytes = to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        (status, String::from_utf8_lossy(&bytes).into_owned(), cors)
    }

    #[tokio::test]
    async fn whip_get_root_returns_204() {
        let (state, _net) = crate::test_support::state().await;
        let (status, _body, _cors) = send_request(&state, "GET", "/whip/v1", vec![]).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        state.manager.close_all().await;
        state.agents.shutdown().await;
        state.shutdown_whip().await;
    }

    #[tokio::test]
    async fn whip_options_root_returns_cors() {
        let (state, _net) = crate::test_support::state().await;
        let (status, _body, cors) = send_request(&state, "OPTIONS", "/whip/v1", vec![]).await;
        assert_eq!(status, StatusCode::OK);
        assert!(cors, "WHIP OPTIONS must include CORS headers");
        state.manager.close_all().await;
        state.agents.shutdown().await;
        state.shutdown_whip().await;
    }

    #[tokio::test]
    async fn whip_post_without_auth_returns_401() {
        let (state, _net) = crate::test_support::state().await;
        let (status, _body, _cors) = send_request(
            &state,
            "POST",
            "/whip/v1",
            vec![("content-type", "application/sdp")],
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        state.manager.close_all().await;
        state.agents.shutdown().await;
        state.shutdown_whip().await;
    }

    #[tokio::test]
    async fn whip_patch_without_auth_returns_428_or_401() {
        let (state, _net) = crate::test_support::state().await;
        // PATCH requires If-Match header; without it, returns 428 before auth check.
        // With If-Match but no auth, returns 401.
        let (status_no_tag, _body, _cors) = send_request(
            &state,
            "PATCH",
            "/whip/v1/PA_test123",
            vec![("content-type", "application/trickle-ice-sdpfrag")],
        )
        .await;
        assert!(
            status_no_tag == StatusCode::PRECONDITION_REQUIRED
                || status_no_tag == StatusCode::UNAUTHORIZED,
            "PATCH without If-Match or auth: expected 428 or 401, got {status_no_tag}"
        );
        let (status_with_tag, _body, _cors) = send_request(
            &state,
            "PATCH",
            "/whip/v1/PA_test123",
            vec![
                ("content-type", "application/trickle-ice-sdpfrag"),
                ("if-match", "*"),
            ],
        )
        .await;
        assert_eq!(status_with_tag, StatusCode::UNAUTHORIZED);
        state.manager.close_all().await;
        state.agents.shutdown().await;
        state.shutdown_whip().await;
    }

    #[tokio::test]
    async fn whip_delete_without_auth_returns_401() {
        let (state, _net) = crate::test_support::state().await;
        let (status, _body, _cors) =
            send_request(&state, "DELETE", "/whip/v1/PA_test123", vec![]).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        state.manager.close_all().await;
        state.agents.shutdown().await;
        state.shutdown_whip().await;
    }

    #[tokio::test]
    async fn whip_get_participant_returns_204() {
        let (state, _net) = crate::test_support::state().await;
        let (status, _body, _cors) =
            send_request(&state, "GET", "/whip/v1/PA_test123", vec![]).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        state.manager.close_all().await;
        state.agents.shutdown().await;
        state.shutdown_whip().await;
    }

    #[tokio::test]
    async fn whip_unsupported_method_returns_404() {
        let (state, _net) = crate::test_support::state().await;
        let (status, _body, _cors) = send_request(&state, "PUT", "/whip/v1", vec![]).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        state.manager.close_all().await;
        state.agents.shutdown().await;
        state.shutdown_whip().await;
    }

    #[tokio::test]
    async fn whip_post_wrong_content_type_returns_400() {
        let (state, _net) = crate::test_support::state().await;
        // POST with wrong content-type returns 400 Bad Request (source uses BAD_REQUEST, not 415)
        let (status, _body, _cors) = send_request(
            &state,
            "POST",
            "/whip/v1",
            vec![("content-type", "application/json")],
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        state.manager.close_all().await;
        state.agents.shutdown().await;
        state.shutdown_whip().await;
    }

    #[tokio::test]
    async fn whip_patch_wrong_content_type_returns_400() {
        let (state, _net) = crate::test_support::state().await;
        // PATCH with wrong content-type returns 400 before If-Match/auth checks
        let (status, _body, _cors) = send_request(
            &state,
            "PATCH",
            "/whip/v1/PA_test123",
            vec![("content-type", "application/json"), ("if-match", "*")],
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        state.manager.close_all().await;
        state.agents.shutdown().await;
        state.shutdown_whip().await;
    }
}
