//! `/rtc`, `/rtc/validate`, `/rtc/v1`, `/rtc/v1/validate` (signaling.md §1).

pub(crate) mod blob;
pub(crate) mod encode;
pub(crate) mod ice;
pub(crate) use ice::server_info;
pub(crate) mod params;
mod remote;
pub(crate) mod session;
mod start;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use axum::Router;
use axum::extract::ws::rejection::WebSocketUpgradeRejection;
use axum::extract::ws::{CloseFrame, Message, Utf8Bytes, WebSocketUpgrade};
use axum::extract::{ConnectInfo, RawQuery, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use futures_util::SinkExt;
use lk_protocol::livekit as pb;
use prost::Message as _;
use rtc_core::model::DisconnectReason;
use rtc_core::session::{ClientMeta, JoinError, JoinParams, LeaveAction, SignalEvent};

use crate::state::AppState;
use encode::{EncodeCtx, RequestCache, encode_signal};
pub use ice::{COMPAT_SERVER_VERSION, PROTOCOL_VERSION};
use params::{HttpFailure, parse_join};

pub fn routes(state: AppState) -> Router {
    Router::new()
        .route("/rtc", get(rtc_v0))
        .route("/rtc/validate", get(validate_v0))
        .route("/rtc/v1", get(rtc_v1))
        .route("/rtc/v1/validate", get(validate_v1))
        .with_state(state)
}

type Ws = Result<WebSocketUpgrade, WebSocketUpgradeRejection>;

async fn rtc_v0(
    State(state): State<AppState>,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
    ws: Ws,
) -> Response {
    connect(state, remote, headers, query, ws, false).await
}

async fn rtc_v1(
    State(state): State<AppState>,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
    ws: Ws,
) -> Response {
    connect(state, remote, headers, query, ws, true).await
}

async fn validate_v0(
    State(state): State<AppState>,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
) -> Response {
    with_cors(validate(state, remote, headers, query, false).await)
}

async fn validate_v1(
    State(state): State<AppState>,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
) -> Response {
    with_cors(validate(state, remote, headers, query, true).await)
}

/// gorilla/websocket `IsWebSocketUpgrade`: both header token lists contain
/// the value (comma separated, trimmed, ASCII case-insensitive).
fn is_websocket_upgrade(headers: &HeaderMap) -> bool {
    let contains = |name: header::HeaderName, value: &str| {
        headers.get_all(name).iter().any(|v| {
            v.to_str().is_ok_and(|v| {
                v.split(',')
                    .any(|t| t.trim_matches([' ', '\t']).eq_ignore_ascii_case(value))
            })
        })
    };
    contains(header::CONNECTION, "upgrade") && contains(header::UPGRADE, "websocket")
}

fn with_cors(mut resp: Response) -> Response {
    resp.headers_mut().insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    resp
}

fn plain(status: StatusCode, body: impl Into<String>) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        body.into(),
    )
        .into_response()
}

fn failure(f: HttpFailure) -> Response {
    plain(f.status, f.body)
}

/// Authenticate and parse; shared by connect and validate.
// This HTTP boundary returns Axum's response directly; boxing only the error
// would add an allocation to every rejected handshake without shrinking state.
#[allow(clippy::result_large_err)]
async fn prepare(
    state: &AppState,
    remote: SocketAddr,
    headers: &HeaderMap,
    query: &HashMap<String, String>,
    v1: bool,
    strict: bool,
) -> Result<JoinParams, Response> {
    let token = crate::auth::extract_token(headers, query).map_err(IntoResponse::into_response)?;
    let grants = crate::auth::authenticate(&state.keys, token.as_deref())
        .map_err(IntoResponse::into_response)?;
    let parsed = parse_join(
        &state.config,
        grants,
        query,
        headers,
        Some(remote),
        v1,
        strict,
    )
    .map_err(failure)?;
    validate_admission(state, &parsed.params).await?;
    parsed.finish().map_err(failure)
}

async fn validate(
    state: AppState,
    remote: SocketAddr,
    headers: HeaderMap,
    query: Option<String>,
    v1: bool,
) -> Response {
    let query = crate::auth::first_query_values(query.as_deref().unwrap_or_default());
    match prepare(&state, remote, &headers, &query, v1, true).await {
        Ok(_) => plain(StatusCode::OK, "success"),
        Err(response) => response,
    }
}

// Fixed ValidateConnectRequest checks the public catalog before owner capacity.
// A rejection here precedes the connection retry loop on both RTC endpoints.
async fn validate_admission(state: &AppState, params: &JoinParams) -> Result<(), Response> {
    let may_create = state.config.room.auto_create || params.grants.video.room_create;
    if !may_create {
        let exists = match &state.room_control {
            Some(control) => control
                .signal_room_exists(&params.room.name.0)
                .await
                .map_err(|error| plain(StatusCode::INTERNAL_SERVER_ERROR, error.message))?,
            None => state.manager.room_exists(&params.room),
        };
        if !exists {
            return Err(plain(
                StatusCode::NOT_FOUND,
                "requested room does not exist",
            ));
        }
    }
    if state.rtc_node_at_capacity(&params.room.name.0).await {
        return Err(plain(
            StatusCode::SERVICE_UNAVAILABLE,
            "node has exceeded its configured limit",
        ));
    }
    Ok(())
}

/// The selected RTC owner checks profile sizes before it creates or opens
/// the room. A successful HTTP preflight does not reserve an RTC session.
pub(crate) fn check_owner_profile_sizes(
    state: &AppState,
    params: &JoinParams,
) -> Result<(), &'static str> {
    let limits = &state.config.limits;
    let violation = if limits
        .max_metadata_size
        .is_some_and(|max| params.metadata.len() > max)
    {
        Some("metadata size exceeds limits")
    } else if limits.max_attributes_size.is_some_and(|max| {
        let size: usize = params
            .attributes
            .iter()
            .map(|(key, value)| key.len() + value.len())
            .sum();
        size > max
    }) {
        Some("attribute size exceeds limits")
    } else {
        None
    };
    if let Some(message) = violation {
        state.manager.record_rtc_init();
        return Err(message);
    }
    Ok(())
}

async fn connect(
    state: AppState,
    remote: SocketAddr,
    headers: HeaderMap,
    query: Option<String>,
    ws: Ws,
    v1: bool,
) -> Response {
    // rtcservice.go serve: non-WebSocket requests get a bare 404 before any
    // token or join parameter is inspected.
    if !is_websocket_upgrade(&headers) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let query = crate::auth::first_query_values(query.as_deref().unwrap_or_default());
    let params = match prepare(&state, remote, &headers, &query, v1, false).await {
        Ok(p) => p,
        Err(r) => return r,
    };
    // Validation passed; a plain HTTP request is answered with an empty 404.
    let ws = match ws {
        Ok(ws) => ws,
        Err(_) => return StatusCode::NOT_FOUND.into_response(),
    };
    // Fixed rtcservice.go installs a transport read cap only when positive.
    // Override Axum's hidden 64MiB message / 16MiB frame defaults explicitly.
    // usize::MAX is checked lazily; this does not reserve that allocation.
    let limit = state
        .config
        .limits
        .signal_message_size_limit
        .unwrap_or(usize::MAX);
    // RFC6455 control frames retain their independent legal 125-byte bound,
    // even when a configured data-message limit is smaller than that.
    let ws = ws.max_message_size(limit).max_frame_size(limit.max(125));
    // rtcservice.go enables Gorilla no-context-takeover compression after auth.
    let ws = ws
        .permessage_deflate(true)
        .max_decompressed_message_size(state.config.limits.signal_message_size_limit)
        .livekit_rtc_reserved_bits();

    let client = params.client.clone();
    let grants = params.grants.clone();
    let add_track_requests = initial_add_tracks(&query);

    let tracks = add_track_requests
        .iter()
        .map(|(_, request)| request.clone())
        .collect::<Vec<_>>();
    let started = start::retry_start(state.signal_connect_attempts, |_, budget| {
        start::attempt(&state, params.clone(), tracks.clone(), budget)
    })
    .await;
    let (handle, first) = match started {
        Ok(None) => {
            return ws.on_upgrade(move |socket| send_empty_initial(socket, state.manager.clone()));
        }
        Ok(Some(start::Started::Remote(remote, first))) => {
            return ws.on_upgrade(move |socket| {
                remote::run(socket, remote, first, state.manager.clone())
            });
        }
        Ok(Some(start::Started::Local(handle, first))) => (handle, first),
        Ok(Some(start::Started::StateMismatch)) => {
            return ws
                .on_upgrade(move |socket| send_only_leave(socket, client, state.manager.clone()));
        }
        Err(start::StartError::Closed) => {
            return plain(
                StatusCode::INTERNAL_SERVER_ERROR,
                "connection closed by media",
            );
        }
        Err(start::StartError::Timeout) => {
            return plain(
                StatusCode::INTERNAL_SERVER_ERROR,
                "timed out while waiting for signal response",
            );
        }
        Err(start::StartError::Join(JoinError::RoomNotFound)) => {
            return plain(StatusCode::NOT_FOUND, "requested room does not exist");
        }
        Err(start::StartError::Join(JoinError::Internal(msg))) => {
            return plain(StatusCode::SERVICE_UNAVAILABLE, msg);
        }
        Err(start::StartError::Join(error)) => {
            return plain(StatusCode::INTERNAL_SERVER_ERROR, error.to_string());
        }
    };

    let mut cache = RequestCache::default();
    for (token, req) in add_track_requests {
        cache.store_with(token, pb::request_response::Request::AddTrack(req));
    }
    let ctx = session::SessionCtx {
        state,
        grants,
        client,
        cache,
    };
    ws.on_upgrade(move |socket| session::run(socket, handle, first, ctx))
}

/// `JoinRequest.add_track_requests` keyed by the tokens `parse_join` assigned
/// (1-based index), so RequestResponses can echo them.
fn initial_add_tracks(query: &HashMap<String, String>) -> Vec<(u32, pb::AddTrackRequest)> {
    use base64::Engine as _;
    let Some(raw) = query.get("join_request") else {
        return Vec::new();
    };
    let Ok(bytes) = base64::engine::general_purpose::URL_SAFE.decode(raw) else {
        return Vec::new();
    };
    let Ok(wrapped) = lk_protocol::wire::decode::<pb::WrappedJoinRequest>(bytes.as_slice()) else {
        return Vec::new();
    };
    let inner = if wrapped.compression == pb::wrapped_join_request::Compression::Gzip as i32 {
        use std::io::Read;
        let mut out = Vec::new();
        let mut r = flate2::read::GzDecoder::new(wrapped.join_request.as_slice()).take(1 << 20);
        if r.read_to_end(&mut out).is_err() {
            return Vec::new();
        }
        out
    } else {
        wrapped.join_request
    };
    lk_protocol::wire::decode::<pb::JoinRequest>(inner.as_slice())
        .map(|jr| {
            jr.add_track_requests
                .into_iter()
                .enumerate()
                .map(|(i, r)| (i as u32 + 1, r))
                .collect()
        })
        .unwrap_or_default()
}

/// Fixed nonpositive StartSession loop has no initial response or session.
/// Marshal(nil) is one binary empty frame, then observed normal closure. Safely
/// produce those bytes without reproducing Go's nil-source panic or task leak.
async fn send_empty_initial(
    mut socket: axum::extract::ws::WebSocket,
    manager: rtc_runtime::RoomManager,
) {
    if session::send(&manager, &mut socket, Message::Binary(Vec::new().into()))
        .await
        .is_ok()
    {
        manager.record_signal_connected();
    }
    if session::send(
        &manager,
        &mut socket,
        Message::Close(Some(CloseFrame {
            code: 1000,
            reason: Utf8Bytes::from_static(""),
        })),
    )
    .await
    .is_ok()
    {
        // A client can acknowledge immediately. Consume that close before
        // dropping the socket, so unread bytes do not reset the connection.
        let _ = tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(Ok(message)) = socket.recv().await {
                if matches!(message, Message::Close(_)) {
                    break;
                }
            }
            socket.close().await
        })
        .await;
    }
}

async fn send_only_leave(
    mut socket: axum::extract::ws::WebSocket,
    client: ClientMeta,
    manager: rtc_runtime::RoomManager,
) {
    let mut cache = RequestCache::default();
    let ev = SignalEvent::Leave {
        reason: DisconnectReason::StateMismatch,
        action: LeaveAction::Reconnect,
    };
    if let Some(resp) = encode_signal(ev, &EncodeCtx { client: &client }, &mut cache) {
        if session::send(
            &manager,
            &mut socket,
            Message::Binary(resp.encode_to_vec().into()),
        )
        .await
        .is_ok()
        {
            manager.record_signal_connected();
        }
    }
    let _ = socket
        .send(Message::Close(Some(CloseFrame {
            code: 1000,
            reason: Utf8Bytes::from_static(""),
        })))
        .await;
    let _ = socket.close().await;
}

#[cfg(test)]
mod signal_http_integration_tests {
    //! HTTP-level integration tests for `/rtc`, `/rtc/validate`, `/rtc/v1`,
    //! `/rtc/v1/validate` exercising the full middleware stack (auth, CORS,
    //! body limit, path normalization, recovery) before signal handling.

    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::extract::ConnectInfo;
    use axum::http::Request;
    use tower::ServiceExt;

    const TEST_REMOTE: &str = "127.0.0.1:50000";

    fn remote_addr() -> ConnectInfo<std::net::SocketAddr> {
        ConnectInfo(TEST_REMOTE.parse().unwrap())
    }

    fn make_join_jwt(room: &str, identity: &str) -> String {
        crate::auth::jwt::tests::make_token(
            r#"{"alg":"HS256"}"#,
            &serde_json::json!({
                "iss": "devkey",
                "sub": identity,
                "exp": 99999999999_u64,
                "video": {
                    "roomJoin": true,
                    "room": room,
                    "canPublish": true,
                    "canSubscribe": true
                }
            })
            .to_string(),
            "secret",
        )
    }

    async fn get_validate(
        state: &AppState,
        uri: &str,
        token: Option<&str>,
    ) -> (StatusCode, String, bool) {
        let mut builder = Request::get(uri).extension(remote_addr());
        if let Some(t) = token {
            builder = builder.header("authorization", format!("Bearer {t}"));
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

    async fn get_rtc_no_ws(state: &AppState, uri: &str) -> StatusCode {
        let resp = crate::http::router(state.clone())
            .oneshot(
                Request::get(uri)
                    .extension(remote_addr())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        resp.status()
    }

    #[tokio::test]
    async fn validate_v0_no_auth_returns_401_with_cors() {
        let (state, net) = crate::test_support::state().await;
        let (status, _body, cors) = get_validate(&state, "/rtc/validate?room=r", None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert!(cors, "validate must include CORS headers");
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn validate_v0_bad_token_returns_401_with_cors() {
        let (state, net) = crate::test_support::state().await;
        let (status, _body, cors) =
            get_validate(&state, "/rtc/validate?access_token=garbage", None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert!(cors, "validate must include CORS headers");
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn validate_v0_no_room_returns_400_with_cors() {
        let (state, net) = crate::test_support::state().await;
        let jwt = make_join_jwt("", "alice");
        let (status, _body, cors) = get_validate(&state, "/rtc/validate", Some(&jwt)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(cors, "validate must include CORS headers");
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn validate_v0_success_returns_200_with_cors() {
        let (state, net) = crate::test_support::state().await;
        let jwt = make_join_jwt("testroom", "alice");
        let (status, body, cors) =
            get_validate(&state, "/rtc/validate?room=testroom", Some(&jwt)).await;
        assert_eq!(status, StatusCode::OK, "body: {body}");
        assert_eq!(body, "success");
        assert!(cors, "validate must include CORS headers");
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn validate_v1_no_auth_returns_401_with_cors() {
        let (state, net) = crate::test_support::state().await;
        let (status, _body, cors) = get_validate(&state, "/rtc/v1/validate", None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert!(cors, "v1 validate must include CORS headers");
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn validate_v1_missing_join_request_returns_400_with_cors() {
        let (state, net) = crate::test_support::state().await;
        let jwt = make_join_jwt("testroom", "alice");
        let (status, _body, cors) = get_validate(&state, "/rtc/v1/validate", Some(&jwt)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(cors, "v1 validate must include CORS headers");
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn rtc_v0_without_websocket_upgrade_returns_404() {
        let (state, net) = crate::test_support::state().await;
        let status = get_rtc_no_ws(&state, "/rtc?room=r").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        state.manager().close_all().await;
        net.shutdown();
    }

    #[tokio::test]
    async fn rtc_v1_without_websocket_upgrade_returns_404() {
        let (state, net) = crate::test_support::state().await;
        let status = get_rtc_no_ws(&state, "/rtc/v1").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        state.manager().close_all().await;
        net.shutdown();
    }
}

#[cfg(test)]
mod binary_join_cache_regressions {
    use super::*;
    use base64::Engine as _;
    #[test]
    fn initial_add_track_echo_cache_uses_same_compatible_decode_as_join() {
        let mut inner = pb::JoinRequest {
            add_track_requests: vec![pb::AddTrackRequest {
                cid: "cid-kept".into(),
                name: "camera".into(),
                ..Default::default()
            }],
            ..Default::default()
        }
        .encode_to_vec();
        inner.extend([0x18, 7]);
        let mut outer = pb::WrappedJoinRequest {
            join_request: inner,
            ..Default::default()
        }
        .encode_to_vec();
        outer.extend([0x10, 7]);
        let query = HashMap::from([(
            "join_request".into(),
            base64::engine::general_purpose::URL_SAFE.encode(outer),
        )]);
        let tracks = initial_add_tracks(&query);
        assert_eq!(tracks.len(), 1);
        assert_eq!(tracks[0].0, 1);
        assert_eq!(tracks[0].1.cid, "cid-kept");
        assert_eq!(tracks[0].1.name, "camera");
        let invalid = HashMap::from([(
            "join_request".into(),
            base64::engine::general_purpose::URL_SAFE.encode([0x12, 2, 1]),
        )]);
        assert!(initial_add_tracks(&invalid).is_empty());
    }
}

#[cfg(test)]
mod rtc_capacity_tests;

#[cfg(test)]
#[path = "nonpositive_attempt_tests.rs"]
mod nonpositive_attempt_tests;
