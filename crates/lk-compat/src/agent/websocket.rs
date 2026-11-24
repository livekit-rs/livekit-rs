//! Official /agent WebSocket control adapter backed by real worker lifecycle.
use super::{
    codec::WireCodec,
    registry::{Outbound, REGISTER_TIMEOUT, Registration, Registry, WorkerError, send_message},
};
use crate::auth::{KeyStore, TokenMinter};
use axum::{
    Router,
    extract::ws::{Message, WebSocket, WebSocketUpgrade, rejection::WebSocketUpgradeRejection},
    extract::{RawQuery, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::any,
};
use futures_util::{SinkExt, StreamExt};
use lk_protocol::livekit as pb;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinSet,
};

#[derive(Debug)]
pub enum WorkerEvent {
    Registered {
        worker_id: String,
    },
    JobState {
        worker_id: String,
        job_id: String,
        state: pb::JobState,
    },
    /// The worker's application-level `WorkerPing`, observed on the way to its
    /// pong so a real consumer can measure the round trip. Go stamps the pong in
    /// `WorkerPingHandler.HandlePing` (pkg/agent/worker.go:135) and keeps no such
    /// event; this is the Rust-side observation seam, not a protocol change.
    Ping {
        worker_id: String,
        last_timestamp: i64,
        timestamp: i64,
    },
    Disconnected {
        worker_id: String,
        jobs: Vec<String>,
    },
}
#[derive(Clone)]
pub struct WorkerHttpState {
    pub registry: Registry,
    pub keys: Arc<KeyStore>,
    pub tokens: Arc<TokenMinter>,
    pub server_info: pb::ServerInfo,
    /// Positive bounds a complete decompressed message. <=0 disables the source limit.
    pub signal_message_size_limit: i64,
    /// Real room bridge must consume this bounded queue, not a no-op success sink.
    pub events: mpsc::Sender<WorkerEvent>,
    pub shutdown: tokio::sync::watch::Receiver<bool>,
    /// Keeps the [`super::service::AgentService`] (and its job-effect consumer
    /// task) alive for as long as a session references this state. Public so a
    /// non-WebSocket transport (the SPEC-0074 C15 WebTransport listener) can
    /// build the same bag; callers that already hold the service elsewhere may
    /// leave it `None`.
    pub _keepalive: Option<Arc<super::service::AgentService>>,
}
pub fn routes(state: WorkerHttpState) -> Router {
    Router::new()
        .route("/agent", any(upgrade))
        .with_state(state)
}

/// Go `websocket.IsWebSocketUpgrade` (gorilla/websocket v1.5.3 server.go:325-328), the
/// gate `AgentSocketUpgrader.Upgrade` runs first (`pkg/service/agentws.go:49-52`). It is
/// a `1#token` match on both headers, so it accepts a plain GET whose `Connection`
/// already carries `upgrade` — unlike axum's extractor, which also requires
/// `Sec-WebSocket-Key`/Version and would reject the request before the grant check
/// upstream performs at `agentws.go:54-58`.
fn is_websocket_upgrade(headers: &HeaderMap) -> bool {
    token_list_contains_value(headers, header::CONNECTION, "upgrade")
        && token_list_contains_value(headers, header::UPGRADE, "websocket")
}

/// gorilla `tokenListContainsValue` (util.go:202-225): for every header value, skip
/// optional whitespace, read one RFC 7230 token, require the next octet to be `,` or the
/// end of the value, then compare ASCII-case-insensitively. A value that ends without a
/// separator still yields its token (a lone `Connection: Upgrade` matches), and a quoted
/// string produces an empty token, so `Connection: "upgrade"` does not match.
fn token_list_contains_value(headers: &HeaderMap, name: header::HeaderName, token: &str) -> bool {
    for value in headers.get_all(name) {
        let Ok(mut remaining) = value.to_str() else {
            continue;
        };
        loop {
            remaining = skip_space(remaining);
            let (candidate, rest) = next_token(remaining);
            if candidate.is_empty() {
                break;
            }
            remaining = skip_space(rest);
            if !remaining.is_empty() && !remaining.starts_with(',') {
                break;
            }
            if candidate.eq_ignore_ascii_case(token) {
                return true;
            }
            if remaining.is_empty() {
                break;
            }
            remaining = &remaining[1..];
        }
    }
    false
}

/// gorilla `skipSpace` (util.go:120-127): leading spaces and horizontal tabs.
fn skip_space(value: &str) -> &str {
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() && (bytes[index] == b' ' || bytes[index] == b'\t') {
        index += 1;
    }
    &value[index..]
}

/// gorilla `nextToken` (util.go:130-135): the leading token octets and the remainder.
fn next_token(value: &str) -> (&str, &str) {
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() && is_token_octet(bytes[index]) {
        index += 1;
    }
    (&value[..index], &value[index..])
}

/// gorilla `isTokenOctet` (util.go:35-...): the RFC 7230 `tchar` set.
fn is_token_octet(byte: u8) -> bool {
    matches!(
        byte,
        b'!' | b'#'
            | b'$'
            | b'%'
            | b'&'
            | b'\''
            | b'*'
            | b'+'
            | b'-'
            | b'.'
            | b'0'..=b'9'
            | b'A'..=b'Z'
            | b'^'
            | b'_'
            | b'`'
            | b'a'..=b'z'
            | b'|'
            | b'~'
    )
}
fn plain(code: StatusCode, body: &str) -> Response {
    (
        code,
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        body.to_owned(),
    )
        .into_response()
}
async fn upgrade(
    State(state): State<WorkerHttpState>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
    ws: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
) -> Response {
    // Upstream order: the upgrade gate first (404 for a non-upgrade request), then the
    // grant check (401 `no permissions to access the room`), and only then the actual
    // upgrade (`pkg/service/agentws.go:49-67`). The extractor is therefore consulted
    // after both, so a GET with an agent grant that cannot complete the handshake still
    // answers 401 like Go, not 404.
    if !is_websocket_upgrade(&headers) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let raw = query.as_deref().unwrap_or("");
    let token = match crate::auth::extract_token(&headers, raw) {
        Ok(token) => token,
        Err(e) => return e.into_response(),
    };
    let grants = match crate::auth::authenticate(&state.keys, token.as_deref()) {
        Ok(Some(grants)) if grants.video.agent => grants,
        Ok(_) => {
            return plain(
                StatusCode::UNAUTHORIZED,
                "no permissions to access the room",
            );
        }
        Err(e) => return e.into_response(),
    };
    let Ok(mut ws) = ws else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let protocol = crate::auth::query_param(raw, "protocol")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(1);
    if state.signal_message_size_limit > 0 {
        let limit = usize::try_from(state.signal_message_size_limit).unwrap_or(usize::MAX);
        ws = ws.max_message_size(limit).max_frame_size(limit);
    } else {
        ws = ws.max_message_size(usize::MAX).max_frame_size(usize::MAX);
    }
    ws.on_upgrade(move |socket| session(state, socket, protocol, grants.api_key))
}
fn now_ms() -> i64 {
    rtc_core::util::unix_ms()
}
fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(i64::MAX as u128) as i64
}
// Go WorkerPingHandler.HandlePing (pkg/agent/worker.go:135): echo the request
// timestamp, stamp the server clock. Shared with the WebTransport control plane.
pub(super) fn pong_frame(last_timestamp: i64) -> pb::ServerMessage {
    pb::ServerMessage {
        message: Some(pb::server_message::Message::Pong(pb::WorkerPong {
            last_timestamp,
            timestamp: now_ms(),
        })),
    }
}
// The pong is only observable once its actual writer has acknowledged the frame,
// matching Go's WriteServerMessage ordering; the event consumer never resurrects
// a write failure into Ok (Go DispatchAgentWorkerSignal returns false on it).
async fn send_pong(
    outgoing: &mpsc::Sender<Outbound>,
    events: &mpsc::Sender<WorkerEvent>,
    worker_id: &str,
    ping: &pb::WorkerPing,
) -> Result<(), WorkerError> {
    let timestamp = now_ms();
    send_message(outgoing, pong_frame(ping.timestamp)).await?;
    events
        .send(WorkerEvent::Ping {
            worker_id: worker_id.to_owned(),
            last_timestamp: ping.timestamp,
            timestamp,
        })
        .await
        .map_err(|_| WorkerError::Closed)?;
    Ok(())
}
// Fixed wsprotocol.go closeWriteTimeout. Never synthesize a Close on decode errors.
const PEER_CONTROL_TIMEOUT: Duration = Duration::from_secs(5);
struct WriterTask(tokio::task::JoinHandle<()>);
impl Drop for WriterTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}
struct SessionGuard {
    registry: Registry,
    id: String,
    registered: bool,
}
impl SessionGuard {
    fn disconnect(&mut self) -> Vec<String> {
        if !self.registered {
            return Vec::new();
        }
        self.registered = false;
        self.registry.disconnect(&self.id)
    }
}
impl Drop for SessionGuard {
    fn drop(&mut self) {
        self.disconnect();
    }
}
async fn session(mut state: WorkerHttpState, socket: WebSocket, protocol: i64, api_key: String) {
    if *state.shutdown.borrow() {
        return;
    }
    let (mut sink, mut incoming) = socket.split();
    let (outgoing, mut queue) = mpsc::channel::<Outbound>(64);
    // Tungstenite owns the queued peer Close ACK. Only this actual close path
    // wakes the writer to flush it; no Close frame is manually synthesized.
    let (close_ack, mut close_requested) = oneshot::channel::<()>();
    let json = Arc::new(AtomicBool::new(false));
    let writer_json = json.clone();
    let mut writer = WriterTask(tokio::spawn(async move {
        let mut heartbeat = tokio::time::interval_at(
            tokio::time::Instant::now() + Duration::from_secs(10),
            Duration::from_secs(10),
        );
        loop {
            tokio::select! {
             biased;
             requested=&mut close_requested=>{
                if requested.is_ok(){let _=sink.flush().await;}
                break;
             },
             value=queue.recv()=>{let Some(value)=value else {break};let codec=if writer_json.load(Ordering::Acquire){WireCodec::Json}else{WireCodec::Protobuf};let result=match codec.encode(&value.message){Ok(frame)=>sink.send(frame).await.map_err(|_|WorkerError::Closed),Err(e)=>Err(e)};let failed=result.is_err();let _=value.written.send(result);if failed {break;}},
             _=heartbeat.tick()=>{if !matches!(tokio::time::timeout(Duration::from_secs(2),sink.send(Message::Ping(Vec::new().into()))).await,Ok(Ok(()))){break;}}
            }
        }
    }));
    let id = rtc_core::ids::new_sid("AW_");
    let mut guard = SessionGuard {
        registry: state.registry.clone(),
        id: id.clone(),
        registered: false,
    };
    let result = async {
        let until = tokio::time::Instant::now() + REGISTER_TIMEOUT;
        let mut codec = WireCodec::Protobuf;
        loop {
            let frame = tokio::time::timeout_at(until, incoming.next())
                .await
                .map_err(|_| WorkerError::Closed)?
                .ok_or(WorkerError::Closed)?
                .map_err(|_| WorkerError::Closed)?;
            if matches!(&frame, Message::Close(_)) {
                return Ok(());
            }
            let message = codec.decode(frame)?;
            json.store(codec == WireCodec::Json, Ordering::Release);
            let Some(message) = message else { continue };
            match message.message {
                Some(pb::worker_message::Message::Ping(p)) => {
                    send_pong(&outgoing, &state.events, &id, &p).await?
                }
                Some(pb::worker_message::Message::Register(request)) => {
                    let mut registration =
                        Registration::from_request(id.clone(), protocol, &request)?;
                    registration.api_key = api_key.clone();
                    send_message(
                        &outgoing,
                        pb::ServerMessage {
                            message: Some(pb::server_message::Message::Register(
                                pb::RegisterWorkerResponse {
                                    worker_id: id.clone(),
                                    server_info: Some(state.server_info.clone()),
                                    endpoint_settings: None,
                                },
                            )),
                        },
                    )
                    .await?;
                    state
                        .registry
                        .register_ready(registration, outgoing.clone())
                        .await?;
                    guard.registered = true;
                    state
                        .events
                        .send(WorkerEvent::Registered {
                            worker_id: id.clone(),
                        })
                        .await
                        .map_err(|_| WorkerError::Closed)?;
                    break;
                }
                _ => {
                    return Err::<(), WorkerError>(WorkerError::InvalidRegistration(
                        "unknown or unimplemented worker signal before registration".into(),
                    ));
                }
            }
        }
        let mut simulated = JoinSet::new();
        loop {
            let frame = incoming
                .next()
                .await
                .ok_or(WorkerError::Closed)?
                .map_err(|_| WorkerError::Closed)?;
            if matches!(&frame, Message::Close(_)) {
                return Ok(());
            }
            let message = codec.decode(frame)?;
            json.store(codec == WireCodec::Json, Ordering::Release);
            let Some(message) = message else { continue };
            match message.message {
                Some(pb::worker_message::Message::Ping(p)) => {
                    send_pong(&outgoing, &state.events, &id, &p).await?
                }
                Some(pb::worker_message::Message::UpdateWorker(update)) => {
                    state.registry.update_worker(&id, &update)?
                }
                Some(pb::worker_message::Message::Availability(response)) => {
                    let _ = state.registry.availability(&id, response);
                }
                Some(pb::worker_message::Message::UpdateJob(update)) => {
                    if let Ok(job_state) = state.registry.update_job(&id, &update, now_ns()) {
                        state
                            .events
                            .send(WorkerEvent::JobState {
                                worker_id: id.clone(),
                                job_id: update.job_id,
                                state: job_state,
                            })
                            .await
                            .map_err(|_| WorkerError::Closed)?;
                    }
                }
                Some(pb::worker_message::Message::MigrateJob(_)) => {} // fixed OSS intentionally no-op
                Some(pb::worker_message::Message::SimulateJob(request)) => {
                    while simulated.try_join_next().is_some() {}
                    if simulated.len() >= 256 {
                        return Err(WorkerError::Capacity);
                    }
                    let registry = state.registry.clone();
                    let tokens = state.tokens.clone();
                    let worker = id.clone();
                    let registration = registry.registration(&id).ok_or(WorkerError::Closed)?;
                    let job = pb::Job {
                        id: rtc_core::ids::new_sid("AJ_"),
                        r#type: if request.participant.is_some() { 1 } else { 0 },
                        room: request.room,
                        participant: request.participant,
                        namespace: registration.key.namespace,
                        agent_name: registration.key.agent_name,
                        ..Default::default()
                    };
                    simulated.spawn(async move {
                        let _ = registry
                            .assign(&worker, job, now_ns(), |r, j, a| {
                                super::token::mint_assignment(
                                    &tokens,
                                    &r.api_key,
                                    r,
                                    j,
                                    a,
                                    now_ms() / 1000,
                                )
                            })
                            .await;
                    });
                }
                _ => {
                    return Err::<(), WorkerError>(WorkerError::InvalidRegistration(
                        "unknown or unimplemented worker signal".into(),
                    ));
                }
            }
        }
    };
    let (writer_done, peer_closed) = tokio::select! {
        _=&mut writer.0=>(true,false),
        outcome=result=>(false,outcome.is_ok()),
        _=state.shutdown.changed()=>(false,false),
    };
    if !writer_done {
        if peer_closed {
            let _ = close_ack.send(());
        }
        // Only an actual peer Close permits its already queued ACK to flush.
        // Malformed JSON/protobuf, registration failures and shutdown still
        // abort immediately, preserving the official abrupt-close behavior.
        if !peer_closed
            || tokio::time::timeout(PEER_CONTROL_TIMEOUT, &mut writer.0)
                .await
                .is_err()
        {
            writer.0.abort();
            let _ = (&mut writer.0).await;
        }
    }
    if guard.registered {
        let jobs = guard.disconnect();
        let _ = state
            .events
            .send(WorkerEvent::Disconnected {
                worker_id: id,
                jobs,
            })
            .await;
    }
}

#[cfg(test)]
mod peer_control_tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::watch;

    struct Fixture {
        task: tokio::task::JoinHandle<()>,
        shutdown: watch::Sender<bool>,
        _events: mpsc::Receiver<WorkerEvent>,
        address: std::net::SocketAddr,
        token: String,
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            self.shutdown.send_replace(true);
            self.task.abort();
        }
    }
    impl Fixture {
        async fn new() -> Self {
            let keys = Arc::new(KeyStore::new(vec![(
                "peer-control".into(),
                "owned-control-fixture-secret".into(),
            )]));
            let tokens = Arc::new(TokenMinter::new(keys.clone()));
            let token = tokens
                .mint(
                    "peer-control",
                    &crate::auth::TokenClaims {
                        video: Some(rtc_core::permission::VideoGrant {
                            agent: true,
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                    Duration::from_secs(60),
                    now_ms() / 1000,
                )
                .unwrap();
            let (events, receiver) = mpsc::channel(4);
            let (shutdown, shutdown_rx) = watch::channel(false);
            let router = routes(WorkerHttpState {
                registry: Registry::new(super::super::registry::Limits::default()),
                keys,
                tokens,
                server_info: Default::default(),
                signal_message_size_limit: 65536,
                events,
                shutdown: shutdown_rx,
                _keepalive: None,
            });
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let task = tokio::spawn(async move {
                axum::serve(listener, router).await.unwrap();
            });
            Self {
                task,
                shutdown,
                _events: receiver,
                address,
                token,
            }
        }
        async fn connect(&self) -> tokio::net::TcpStream {
            let mut peer = tokio::net::TcpStream::connect(self.address).await.unwrap();
            peer.write_all(format!("GET /agent?protocol=1&access_token={} HTTP/1.1\r\nHost: {}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n",self.token,self.address).as_bytes()).await.unwrap();
            let mut response = Vec::new();
            while !response.ends_with(b"\r\n\r\n") {
                assert!(response.len() < 8192, "bounded real handshake");
                response.push(
                    tokio::time::timeout(Duration::from_secs(2), peer.read_u8())
                        .await
                        .unwrap()
                        .unwrap(),
                );
            }
            assert!(response.starts_with(b"HTTP/1.1 101 "));
            peer
        }
    }
    async fn send(peer: &mut tokio::net::TcpStream, opcode: u8, payload: &[u8]) {
        assert!(payload.len() < 126);
        let mask = [1, 2, 3, 4];
        let mut frame = vec![0x80 | opcode, 0x80 | payload.len() as u8];
        frame.extend(mask);
        frame.extend(payload.iter().enumerate().map(|(n, v)| v ^ mask[n % 4]));
        peer.write_all(&frame).await.unwrap();
    }
    async fn receive(peer: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
        tokio::time::timeout(Duration::from_secs(2), async {
            let mut header = [0; 2];
            peer.read_exact(&mut header).await.unwrap();
            assert_eq!(header[1] & 128, 0);
            let len = match header[1] & 127 {
                126 => peer.read_u16().await.unwrap() as usize,
                127 => panic!("unbounded fixture response"),
                n => n as usize,
            };
            assert!(len <= 65535);
            let mut bytes = vec![0; len];
            peer.read_exact(&mut bytes).await.unwrap();
            (header[0] & 15, bytes)
        })
        .await
        .expect("real peer Close ACK must flush without the10s heartbeat")
    }
    async fn register_json(peer: &mut tokio::net::TcpStream) {
        send(peer, 1, br#"{"register":{}}"#).await;
        let (kind, body) = receive(peer).await;
        assert_eq!(kind, 1);
        let response: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(
            response.get("register").is_some(),
            "actual registration reply"
        );
    }
    #[tokio::test]
    async fn real_axum_tungstenite_close_ack_flushes_before_and_after_registration() {
        let fixture = Fixture::new().await;
        for registered in [false, true] {
            let mut peer = fixture.connect().await;
            if registered {
                register_json(&mut peer).await;
            }
            send(&mut peer, 8, &1000u16.to_be_bytes()).await;
            assert_eq!(
                receive(&mut peer).await,
                (8, 1000u16.to_be_bytes().to_vec())
            );
        }
    }
    #[tokio::test]
    async fn malformed_json_remains_abrupt_without_invented_normal_close() {
        let fixture = Fixture::new().await;
        for registered in [false, true] {
            let mut peer = fixture.connect().await;
            if registered {
                register_json(&mut peer).await;
            }
            send(&mut peer, 1, b"{").await;
            let result = tokio::time::timeout(Duration::from_secs(2), peer.read_u8())
                .await
                .expect("decode failure must terminate");
            assert!(
                matches!(result,Err(error) if matches!(error.kind(),std::io::ErrorKind::UnexpectedEof|std::io::ErrorKind::ConnectionReset)),
                "no normal Close frame may be synthesized"
            );
        }
    }
}

/// HTTP-level integration tests for the `/agent` route through the full middleware stack.
#[cfg(test)]
mod agent_http_integration_tests {
    use axum::body::{Body, to_bytes};
    use axum::extract::ConnectInfo;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    fn remote_addr() -> ConnectInfo<std::net::SocketAddr> {
        ConnectInfo("127.0.0.1:50000".parse().unwrap())
    }

    async fn send_agent_request(
        state: &crate::state::AppState,
        method: &str,
        uri: &str,
        headers: Vec<(&str, &str)>,
    ) -> (StatusCode, String) {
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
        let bytes = to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        (status, String::from_utf8_lossy(&bytes).into_owned())
    }

    // Go's chain for mux.Handle("/agent") is recovery, CORS, body limiter, then the API
    // key middleware, which only records grants when a token is present and otherwise
    // calls next (`pkg/service/auth.go:86-119`). agentWSService then answers 404 for
    // every request that is not a WebSocket upgrade, before it looks at credentials
    // (`pkg/service/agentws.go:49-52`). The API-chain CORS middleware keeps a bare
    // OPTIONS as an actual request because rs/cors only treats it as preflight with an
    // `Access-Control-Request-Method` header (`cors@v1.11.1/cors.go:309`).
    #[tokio::test]
    async fn agent_non_upgrade_requests_return_404() {
        let (state, _net) = crate::test_support::state().await;
        let cases: &[(&str, Vec<&str>)] = &[
            ("GET", vec![]),
            ("POST", vec![]),
            ("DELETE", vec![]),
            ("OPTIONS", vec![]),
            // either half of the gorilla conjunction alone is still not an upgrade
            ("GET", vec!["connection: Upgrade"]),
            ("GET", vec!["upgrade: websocket"]),
            ("GET", vec!["connection: keep-alive", "upgrade: websocket2"]),
        ];
        for (method, raw_headers) in cases {
            let headers = raw_headers
                .iter()
                .map(|line| line.split_once(": ").expect("header fixture"))
                .collect();
            let (status, body) = send_agent_request(&state, method, "/agent", headers).await;
            assert_eq!(
                status,
                StatusCode::NOT_FOUND,
                "{method} {raw_headers:?} -> {body}"
            );
        }
    }

    #[tokio::test]
    async fn agent_upgrade_without_credentials_returns_401_permission_denied() {
        let (state, _net) = crate::test_support::state().await;
        // A real upgrade request but no token: auth.go leaves the context without grants,
        // so agentws.go:56-60 reports ErrPermissionDenied as 401 before attempting Upgrade.
        let (status, body) = send_agent_request(
            &state,
            "GET",
            "/agent",
            vec![
                ("upgrade", "websocket"),
                ("connection", "Upgrade"),
                ("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ=="),
                ("sec-websocket-version", "13"),
            ],
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body, "no permissions to access the room");
    }

    #[tokio::test]
    async fn agent_options_preflight_is_answered_before_the_upgrade_gate() {
        let (state, _net) = crate::test_support::state().await;
        let (status, body) = send_agent_request(
            &state,
            "OPTIONS",
            "/agent",
            vec![
                ("origin", "https://neton.live"),
                ("access-control-request-method", "GET"),
            ],
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT, "preflight body {body}");
        assert_eq!(body, "");
    }
}

/// Direct coverage of the gorilla `IsWebSocketUpgrade` port, whose token rules decide
/// whether `/agent` answers 404 or proceeds to the grant check.
#[cfg(test)]
mod upgrade_gate_tests {
    use super::is_websocket_upgrade;
    use axum::http::{HeaderMap, HeaderValue, header};

    fn headers(pairs: &[(&'static str, &'static str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.append(
                header::HeaderName::from_lowercase(name.as_bytes()).unwrap(),
                HeaderValue::from_static(value),
            );
        }
        map
    }

    #[test]
    fn upgrade_gate_follows_gorilla_token_list_rules() {
        let websocket = ("upgrade", "websocket");
        for (desc, pairs, want) in [
            (
                "both tokens",
                &[websocket, ("connection", "Upgrade")][..],
                true,
            ),
            // tokenListContainsValue keeps scanning after a separator, so a lone token
            // with no comma still yields its candidate.
            (
                "lone token, comma first",
                &[websocket, ("connection", "keep-alive, Upgrade")][..],
                true,
            ),
            (
                "lone token trailing",
                &[websocket, ("connection", "Upgrade, keep-alive")][..],
                true,
            ),
            (
                "ows around token",
                &[websocket, ("connection", "  Upgrade  ")][..],
                true,
            ),
            (
                "ascii case folded",
                &[("upgrade", "WebSocket"), ("connection", "upgrade")][..],
                true,
            ),
            (
                "repeated header values",
                &[
                    websocket,
                    ("connection", "keep-alive"),
                    ("connection", "Upgrade"),
                ][..],
                true,
            ),
            ("missing connection", &[websocket][..], false),
            ("missing upgrade", &[("connection", "Upgrade")][..], false),
            (
                "wrong upgrade token",
                &[("upgrade", "websocket2"), ("connection", "Upgrade")][..],
                false,
            ),
            // nextToken stops at a non-tchar and the octet after it must be `,`, so a
            // `;` terminator ends the scan without matching.
            (
                "semicolon terminator",
                &[websocket, ("connection", "Upgrade; x")][..],
                false,
            ),
            // a quoted-string value produces an empty token, which is never a match.
            (
                "quoted value",
                &[websocket, ("connection", "\"Upgrade\"")][..],
                false,
            ),
            ("empty value", &[websocket, ("connection", "")][..], false),
            (
                "token prefix only",
                &[websocket, ("connection", "xUpgrade")][..],
                false,
            ),
        ] {
            assert_eq!(is_websocket_upgrade(&headers(pairs)), want, "{desc}");
        }
    }
}
