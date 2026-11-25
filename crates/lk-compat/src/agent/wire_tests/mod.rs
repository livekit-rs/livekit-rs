//! AGENT_WIRE leaf falsification suite: per-message wire shape, producer,
//! ordering, identity and timeout, driven through the real `/agent` transports.
//! Every expectation is tied to a re-read line of the pinned Go oracle
//! (livekit v1.13.10 @ e5655ac) or the pinned protocol module.
use super::{
    registry::*,
    websocket::{WorkerEvent, WorkerHttpState},
    wt_control,
};
use crate::auth::{KeyStore, TokenClaims, TokenMinter};
use lk_protocol::livekit as pb;
use std::sync::Arc;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::mpsc,
};

const KEY_ID: &str = "wire-leaf-key";
const KEY_SECRET: &str = "wire-leaf-fixture-secret-0123456789abcdef";

fn now_ms() -> i64 {
    rtc_core::util::unix_ms()
}

/// These tests drive real sockets on a paused virtual clock, where
/// `tokio::time::timeout` fires the moment the runtime idles and reports a
/// spurious `Elapsed` before the peer's bytes arrive. Waiting by wall clock in a
/// poll-and-yield loop keeps a task ready, so the clock stays put and a missing
/// response still fails the assertion.
const IO_BOUND: std::time::Duration = std::time::Duration::from_secs(5);

fn check_io_bound(start: std::time::Instant, what: &str) {
    assert!(
        start.elapsed() < IO_BOUND,
        "{what} did not arrive within {IO_BOUND:?}"
    );
}

/// Only receiver on this sink, so `try_recv` stands in for `recv` without the
/// virtual-clock timeout above.
async fn recv_event(events: &mut mpsc::Receiver<WorkerEvent>) -> Option<WorkerEvent> {
    let start = std::time::Instant::now();
    loop {
        match events.try_recv() {
            Ok(event) => return Some(event),
            Err(mpsc::error::TryRecvError::Disconnected) => return None,
            Err(mpsc::error::TryRecvError::Empty) => {
                check_io_bound(start, "consumer event");
                tokio::task::yield_now().await;
            }
        }
    }
}

/// One observed server->worker frame with its actual websocket opcode.
struct Frame {
    text: bool,
    message: pb::ServerMessage,
}

impl Frame {
    fn one(&self) -> &pb::server_message::Message {
        self.message
            .message
            .as_ref()
            .expect("non-empty ServerMessage")
    }
}

/// The same registry/service assembly the `/agent` route runs on. Tests that
/// assert producer behavior install their own dedicated event sink (see
/// `ws_server`/`wt_session`); the harness only keeps the shared sink's receiving
/// end open so production senders never see a closed channel.
struct Harness {
    state: WorkerHttpState,
    _events: mpsc::Receiver<WorkerEvent>,
    _net: rtc_runtime::net::NetRouter,
    leaked: bool,
}

impl Harness {
    async fn new() -> Self {
        let (state, net) = crate::test_support::state().await;
        let service = state.agents().clone();
        let keys = Arc::new(KeyStore::new(vec![(KEY_ID.into(), KEY_SECRET.into())]));
        let tokens = Arc::new(TokenMinter::new(keys.clone()));
        let _ = &tokens;
        let (events, receiver) = mpsc::channel(64);
        let worker_state = service.worker_state();
        // Swap in a fixture key store so minted assignment tokens verify against
        // it; the production graph's own keys stay untouched.
        let state = WorkerHttpState {
            registry: service.registry.clone(),
            keys,
            tokens,
            server_info: worker_state.server_info.clone(),
            signal_message_size_limit: 1 << 20,
            events,
            shutdown: worker_state.shutdown,
            _keepalive: Some(service.clone()),
        };
        Self {
            state,
            _events: receiver,
            _net: net,
            leaked: false,
        }
    }
    /// Launch the real axum `/agent` route over loopback TCP with a dedicated
    /// event sink, so consumer assertions below are race-free against any
    /// other connection that shares this harness registry.
    async fn ws_server(
        &mut self,
    ) -> (
        std::net::SocketAddr,
        tokio::task::JoinHandle<()>,
        mpsc::Receiver<WorkerEvent>,
    ) {
        let mut route_state = self.state.clone();
        let (sink, receiver) = mpsc::channel(64);
        route_state.events = sink;
        // The route owns the dedicated sink from here on; Drop must not tear
        // down the shared registry while connections may still be live.
        self.leaked = true;
        let router = super::websocket::routes(route_state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        (addr, task, receiver)
    }
    async fn ws_connect(&self, addr: std::net::SocketAddr) -> RawWs {
        RawWs::connect(addr, &self.token()).await
    }
    fn token(&self) -> String {
        self.state
            .tokens
            .mint(
                KEY_ID,
                &TokenClaims {
                    video: Some(rtc_core::permission::VideoGrant {
                        agent: true,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                std::time::Duration::from_secs(60),
                now_ms() / 1000,
            )
            .unwrap()
    }
    /// Drive `wt_control::session` over an in-memory duplex stream.
    /// The caller must already have written the register frame into the duplex
    /// (its 64 KiB buffer absorbs it) because the helper consumes the first
    /// event as the bookkeeping fence. Returns the session task and the event
    /// sink tail.
    async fn wt_session<R, W>(
        &mut self,
        read: R,
        write: W,
    ) -> (tokio::task::JoinHandle<()>, mpsc::Receiver<WorkerEvent>)
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let mut route_state = self.state.clone();
        let (sink, mut receiver) = mpsc::channel(64);
        route_state.events = sink;
        self.leaked = true;
        let task = tokio::spawn(wt_control::session(
            route_state,
            read,
            write,
            1,
            KEY_ID.to_owned(),
        ));
        // Fence registration completion before the test starts asserting.
        let registered = recv_event(&mut receiver).await;
        assert!(
            matches!(registered, Some(WorkerEvent::Registered { .. })),
            "got {registered:?}"
        );
        (task, receiver)
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        // The registry is harness-owned; the shared bridge consumer would race
        // a later test on it, so only clear this harness's workers here. Full
        // service shutdown belongs to the graph owner (worker_services tests).
        // A leaked harness hands its registry to a spawned route task that may
        // still be serving connections, so closing workers there would tear
        // down live sessions mid-test.
        if !self.leaked {
            self.state.registry.close_all();
        }
    }
}

/// Minimal masked client frames, like the existing peer-control fixture.
struct RawWs {
    peer: tokio::net::TcpStream,
}
impl RawWs {
    async fn connect(addr: std::net::SocketAddr, token: &str) -> Self {
        let peer = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut me = Self { peer };
        me.peer
            .write_all(format!("GET /agent?protocol=1&access_token={token} HTTP/1.1\r\nHost: {addr}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n").as_bytes())
            .await
            .unwrap();
        let mut response = Vec::new();
        while !response.ends_with(b"\r\n\r\n") {
            assert!(response.len() < 8192, "bounded handshake");
            let mut byte = [0u8; 1];
            me.read_bounded(&mut byte).await;
            response.push(byte[0]);
        }
        assert!(
            response.starts_with(b"HTTP/1.1 101 "),
            "upgrade failed, handshake was: {}",
            String::from_utf8_lossy(&response)
        );
        me
    }
    /// Fill `buf` from the socket, bounded by [`IO_BOUND`] of wall time.
    async fn read_bounded(&mut self, buf: &mut [u8]) {
        let start = std::time::Instant::now();
        let mut filled = 0;
        while filled < buf.len() {
            match self.peer.try_read(&mut buf[filled..]) {
                Ok(0) => panic!("server ended the connection mid-frame"),
                Ok(read) => filled += read,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    check_io_bound(start, "server frame");
                    tokio::task::yield_now().await;
                }
                Err(error) => panic!("socket read: {error}"),
            }
        }
    }
    async fn send(&mut self, text: bool, payload: &[u8]) {
        assert!(payload.len() < 126, "fixture frames stay short");
        let mask = [1, 2, 3, 4];
        let mut frame = vec![if text { 0x81 } else { 0x82 }, 0x80 | payload.len() as u8];
        frame.extend(mask);
        frame.extend(payload.iter().enumerate().map(|(n, v)| v ^ mask[n % 4]));
        self.peer.write_all(&frame).await.unwrap();
    }
    async fn send_proto(&mut self, message: &pb::WorkerMessage) {
        self.send(false, &prost::Message::encode_to_vec(message))
            .await;
    }
    async fn send_json(&mut self, json: &[u8]) {
        self.send(true, json).await;
    }
    async fn receive(&mut self) -> Frame {
        let mut header = [0; 2];
        self.read_bounded(&mut header).await;
        assert_eq!(header[1] & 128, 0, "server frames are unmasked");
        let len = match header[1] & 127 {
            126 => {
                let mut extra = [0; 2];
                self.read_bounded(&mut extra).await;
                u16::from_be_bytes(extra) as usize
            }
            127 => panic!("unbounded fixture response"),
            n => n as usize,
        };
        let mut bytes = vec![0; len];
        self.read_bounded(&mut bytes).await;
        let text = header[0] & 15 == 1;
        let message = if text {
            lk_protocol::json::decode::<pb::ServerMessage>(&bytes).unwrap()
        } else {
            prost::Message::decode(&bytes[..]).unwrap()
        };
        Frame { text, message }
    }
    /// Expect EOF/reset: the session tore down without another data frame.
    async fn expect_abrupt_end(&mut self) {
        let start = std::time::Instant::now();
        let mut probe = [0u8; 1];
        loop {
            match self.peer.try_read(&mut probe) {
                Ok(0) => return,
                Ok(read) => panic!("unexpected {read} bytes after the session ended"),
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted
                    ) =>
                {
                    return;
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    check_io_bound(start, "session teardown");
                    tokio::task::yield_now().await;
                }
                Err(error) => panic!("socket read: {error}"),
            }
        }
    }
}

async fn recv_frame<R: AsyncRead + Unpin>(read: &mut R) -> Option<pb::ServerMessage> {
    let mut header = [0u8; 4];
    read.read_exact(&mut header).await.ok()?;
    let len = usize::try_from(u32::from_be_bytes(header)).unwrap_or(usize::MAX);
    let mut body = vec![0u8; len];
    if read.read_exact(&mut body).await.is_err() {
        return None;
    }
    prost::Message::decode(&body[..]).ok()
}

async fn send_worker_frame<W: AsyncWrite + Unpin>(write: &mut W, message: &pb::WorkerMessage) {
    let body = prost::Message::encode_to_vec(message);
    let mut frame = Vec::with_capacity(4 + body.len());
    frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
    frame.extend_from_slice(&body);
    write.write_all(&frame).await.unwrap();
    write.flush().await.unwrap();
}

fn worker_message(m: pb::worker_message::Message) -> pb::WorkerMessage {
    pb::WorkerMessage { message: Some(m) }
}

fn register_request(agent_name: &str) -> pb::WorkerMessage {
    worker_message(pb::worker_message::Message::Register(
        pb::RegisterWorkerRequest {
            r#type: 0,
            agent_name: agent_name.into(),
            version: "wire-leaf".into(),
            ..Default::default()
        },
    ))
}

// ---------------------------------------------------------------------------
// AGENT_WIRE:WorkerMessage.register / ServerMessage.register
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn register_identity_ordering_and_deadline_over_real_websocket() {
    let mut h = Harness::new().await;
    let registry = h.state.registry.clone();
    let (addr, _server, mut server_events) = h.ws_server().await;

    // Identity: the server allocates the worker id before the request is read
    // (Go MakeWorkerRegistration, pkg/agent/worker.go:172-177), never from the
    // worker's own fields.
    let mut peer = h.ws_connect(addr).await;
    peer.send_json(b"{\"register\":{\"type\":\"JT_ROOM\",\"agentName\":\"id-probe\"}}")
        .await;
    let reply = peer.receive().await;
    let pb::server_message::Message::Register(response) = reply.one().clone() else {
        panic!("register must answer with ServerMessage.register")
    };
    assert!(
        response.worker_id.starts_with("AW_"),
        "guid.AgentWorkerPrefix"
    );
    let info = response.server_info.clone().expect("ServerInfo stamped");
    assert_eq!(
        info.agent_protocol, 1,
        "agent.CurrentProtocol (pkg/agent/worker.go:52)"
    );
    assert!(
        response.endpoint_settings.is_none(),
        "the WS path serves no data plane, so settings stay withheld (agenthandler.go:183-189,200)"
    );
    assert!(reply.text, "JSON in answers JSON out (wsprotocol.go:205)");
    let worker_id = response.worker_id.clone();

    // Ordering: the registry becomes ready only after the writer acknowledged
    // the frame (Go writes then registers, agenthandler.go:197-198). A ping
    // round trip is the fence: it is answered by the post-register dispatch
    // loop, so receiving its pong proves bookkeeping completed.
    let registration = registry
        .registration(&worker_id)
        .expect("registered after writer ACK");
    assert_eq!(registration.key.agent_name, "id-probe");
    assert_eq!(
        registration.api_key, KEY_ID,
        "identity comes from the authenticated token, not the request"
    );
    assert_eq!(
        (
            registration.permissions.can_subscribe,
            registration.permissions.can_publish,
            registration.permissions.can_publish_data,
            registration.permissions.can_update_metadata
        ),
        (true, true, true, true),
        "AllowedPermissions default (pkg/agent/worker.go:251-259)"
    );
    let event = recv_event(&mut server_events).await;
    assert!(
        matches!(&event, Some(WorkerEvent::Registered { worker_id: id }) if id == &worker_id),
        "the real consumer sees Registered, got {event:?}"
    );

    // Timeout: a connection that never registers is dropped at RegisterTimeout
    // (worker.go:55, agenthandler.go:62-76). Paused time makes this exact.
    drop(peer);
    let mut idle = h.ws_connect(addr).await;
    tokio::time::advance(REGISTER_TIMEOUT + std::time::Duration::from_secs(1)).await;
    idle.expect_abrupt_end().await;
    // The first peer's close deregisters its worker (Go OnClose -> removeWorker,
    // agenthandler.go:217), and the session that hit RegisterTimeout never
    // registered, so that disconnect is the only observation left.
    let closed = recv_event(&mut server_events)
        .await
        .expect("disconnect event");
    assert!(
        matches!(&closed, WorkerEvent::Disconnected { worker_id: id, .. } if id == &worker_id),
        "got {closed:?}"
    );
    assert!(
        registry.registration(&worker_id).is_none(),
        "a closed peer leaves no registration"
    );
    assert!(
        server_events.try_recv().is_err(),
        "the timed-out session must not register"
    );
}

#[tokio::test(start_paused = true)]
async fn register_rejections_disconnect_without_a_reply() {
    let mut h = Harness::new().await;
    let (addr, _server, mut server_events) = h.ws_server().await;
    for (case, json) in [
        ("unknown job type", br#"{"register":{"type":999}}"#.to_vec()),
        (
            "reserved deployment byte",
            br#"{"register":{"deployment":"bad_deployment"}}"#.to_vec(),
        ),
        (
            "overlong deployment",
            format!("{{\"register\":{{\"deployment\":\"{}\"}}}}", "d".repeat(65)).into_bytes(),
        ),
        (
            "signal before register",
            br#"{"updateJob":{"jobId":"AJ_x","status":"JS_RUNNING"}}"#.to_vec(),
        ),
    ] {
        let mut peer = h.ws_connect(addr).await;
        peer.send_json(&json).await;
        peer.expect_abrupt_end().await;
        let mut seen = Vec::new();
        while let Ok(event) = server_events.try_recv() {
            seen.push(event);
        }
        assert!(
            seen.is_empty(),
            "{case}: a rejected handshake produces no consumer event"
        );
    }
    // An unauthenticated upgrade is refused before any frame (agentws.go:55).
    let mut raw = RawWsPeerProbe::new();
    raw.request(
        addr,
        "GET /agent?protocol=1 HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n",
    )
    .await;
    assert_eq!(raw.status, 401, "missing access token");
}

struct RawWsPeerProbe {
    status: u16,
}
impl RawWsPeerProbe {
    fn new() -> Self {
        Self { status: 0 }
    }
    async fn request(&mut self, addr: std::net::SocketAddr, req: &str) {
        let mut peer = tokio::net::TcpStream::connect(addr).await.unwrap();
        peer.write_all(req.as_bytes()).await.unwrap();
        let mut response = Vec::new();
        let start = std::time::Instant::now();
        while !response.ends_with(b"\r\n\r\n") && response.len() < 8192 {
            let mut byte = [0u8; 1];
            match peer.try_read(&mut byte) {
                Ok(1) => response.push(byte[0]),
                Ok(_) => break,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    check_io_bound(start, "response head");
                    tokio::task::yield_now().await;
                }
                Err(error) => panic!("socket read: {error}"),
            }
        }
        let head = String::from_utf8_lossy(&response);
        self.status = head
            .split_whitespace()
            .nth(1)
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
    }
}

// ---------------------------------------------------------------------------
// AGENT_WIRE:WorkerMessage.ping / ServerMessage.pong
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn pingpong_echo_codec_switch_and_transport_keepalive() {
    let mut h = Harness::new().await;
    let (addr, _server, mut server_events) = h.ws_server().await;
    let mut peer = h.ws_connect(addr).await;

    // Ping during the handshake is served (Go embeds WorkerPingHandler in
    // WorkerRegisterer, pkg/agent/worker.go:210-218).
    peer.send_json(b"{\"ping\":{\"timestamp\":\"123\"}}").await;
    let frame = peer.receive().await;
    let pb::server_message::Message::Pong(pong) = frame.one().clone() else {
        panic!("ping must be answered by pong")
    };
    assert_eq!(pong.last_timestamp, 123, "echo of the request timestamp");
    assert!(pong.timestamp >= now_ms() - 1000, "server clock in ms");
    assert!(frame.text, "pong follows the JSON codec");

    peer.send_json(b"{\"register\":{\"type\":\"JT_ROOM\",\"agentName\":\"pp\"}}")
        .await;
    let reply = peer.receive().await;
    let worker_id = match reply.one() {
        pb::server_message::Message::Register(r) => r.worker_id.clone(),
        other => panic!("expected register, got {other:?}"),
    };
    // Consumer order follows the shared outgoing queue: the handshake pong write
    // and its observation come before the register reply and Registered. Go
    // answers that ping from WorkerPingHandler and defines no ping consumer
    // callback at all (pkg/agent/worker.go:135-143), so the observation is this
    // implementation's own and legitimately precedes registration.
    let mut events = Vec::new();
    events.push(recv_event(&mut server_events).await.expect("first event"));
    events.push(recv_event(&mut server_events).await.expect("second event"));
    assert!(
        matches!(
            events[0],
            WorkerEvent::Ping {
                last_timestamp: 123,
                ..
            }
        ),
        "handshake ping observation first, got {:?}",
        events[0]
    );
    assert!(
        matches!(events[1], WorkerEvent::Registered { .. }),
        "registration follows, got {:?}",
        events[1]
    );

    // Codec switch: binary ping flips later replies to protobuf
    // (wsprotocol.go:152-158).
    peer.send_proto(&worker_message(pb::worker_message::Message::Ping(
        pb::WorkerPing { timestamp: 456 },
    )))
    .await;
    let frame = peer.receive().await;
    assert!(!frame.text, "binary input switches the reply codec");
    let pb::server_message::Message::Pong(pong) = frame.one().clone() else {
        panic!("pong")
    };
    assert_eq!(pong.last_timestamp, 456);

    // Transport keepalive: the writer pings every 10s (wsprotocol.go:37,219-235).
    // Under paused time the next frame this connection sees after >=10s of
    // virtual advance is that ws-level Ping (opcode 9), not a data message.
    tokio::time::advance(std::time::Duration::from_secs(10)).await;
    let opcode = peer.next_opcode().await;
    assert_eq!(
        opcode, 9,
        "the 10s websocket-level ping heartbeat must fire"
    );

    // Consumer observation: exactly one Ping event per served ping, carrying
    // the echoed pair. The pong write precedes the event send (send_pong), so
    // by now the second ping's event is already queued.
    while let Ok(event) = server_events.try_recv() {
        events.push(event);
    }
    let pings: Vec<_> = events
        .iter()
        .filter(|e| matches!(e, WorkerEvent::Ping { .. }))
        .collect();
    assert_eq!(pings.len(), 2, "both served pings reach the consumer");
    for (event, expected) in pings.iter().zip([123i64, 456]) {
        match event {
            WorkerEvent::Ping {
                worker_id: id,
                last_timestamp,
                timestamp,
            } => {
                assert_eq!(id, &worker_id);
                assert_eq!(*last_timestamp, expected);
                assert!(*timestamp >= now_ms() - 60_000);
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}

impl RawWs {
    /// Read exactly one server frame and return its opcode, handling Ping/Pong/
    /// Close bodies like a conformant client (tungstenite auto-answers Pings).
    async fn next_opcode(&mut self) -> u8 {
        let mut header = [0; 2];
        self.read_bounded(&mut header).await;
        let opcode = header[0] & 15;
        let len = match header[1] & 127 {
            126 => {
                let mut extra = [0; 2];
                self.read_bounded(&mut extra).await;
                u16::from_be_bytes(extra) as usize
            }
            127 => panic!("unbounded fixture response"),
            n => n as usize,
        };
        let mut body = vec![0u8; len];
        self.read_bounded(&mut body).await;
        if opcode == 9 {
            // Client Pong answer keeps the server writer's expectations intact.
            let mask = [1, 2, 3, 4];
            let mut frame = vec![0x8a, 0x80 | body.len() as u8];
            frame.extend(mask);
            frame.extend(body.iter().enumerate().map(|(n, v)| v ^ mask[n % 4]));
            self.peer.write_all(&frame).await.unwrap();
        }
        opcode
    }
}

// ---------------------------------------------------------------------------
// AGENT_WIRE:WorkerMessage.availability / ServerMessage.availability
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn availability_offer_response_identity_and_unknown_job() {
    let h = Harness::new().await;
    let registry = h.state.registry.clone();
    let (tx, mut rx) = mpsc::channel::<Outbound>(4);
    registry
        .register(
            Registration::from_request("AW_avail".into(), 1, &pb::RegisterWorkerRequest::default())
                .unwrap(),
            tx,
        )
        .unwrap();
    let task_registry = registry.clone();
    let assign = tokio::spawn(async move {
        task_registry
            .assign(
                "AW_avail",
                pb::Job {
                    id: "AJ_avail".into(),
                    room: Some(pb::Room {
                        name: "avail-room".into(),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                1_700_000_000_000_000_000,
                |_, _, _| Ok("fake-but-unreachable-token".into()),
            )
            .await
    });
    let offer = rx.recv().await.expect("availability offer");
    let pb::server_message::Message::Availability(request) = offer.message.message.unwrap() else {
        panic!("assignment must begin with ServerMessage.availability (worker.go:415)")
    };
    assert_eq!(request.job.unwrap().id, "AJ_avail");
    // Ordering: the pending slot exists before the worker can answer; an
    // unknown-job response is ignored without error (worker.go:559-563).
    registry
        .availability(
            "AW_avail",
            pb::AvailabilityResponse {
                job_id: "AJ_other".into(),
                available: true,
                ..Default::default()
            },
        )
        .unwrap();
    assert!(
        !assign.is_finished(),
        "wrong jobId must not resolve the assign"
    );
    offer.written.send(Ok(())).unwrap();

    registry
        .availability(
            "AW_avail",
            pb::AvailabilityResponse {
                job_id: "AJ_avail".into(),
                available: false,
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(assign.await.unwrap().unwrap_err(), WorkerError::Unavailable);
    // No running job was created for a declined availability (worker.go:433-435).
    assert_eq!(registry.oracle_snapshot("AW_avail").3, 0);
}

#[tokio::test(start_paused = true)]
async fn availability_timeout_is_ten_seconds() {
    let h = Harness::new().await;
    let registry = h.state.registry.clone();
    let (tx, mut rx) = mpsc::channel::<Outbound>(4);
    registry
        .register(
            Registration::from_request("AW_to".into(), 1, &pb::RegisterWorkerRequest::default())
                .unwrap(),
            tx,
        )
        .unwrap();
    let task_registry = registry.clone();
    let assign = tokio::spawn(async move {
        task_registry
            .assign(
                "AW_to",
                pb::Job {
                    id: "AJ_to".into(),
                    ..Default::default()
                },
                1,
                |_, _, _| Ok("t".into()),
            )
            .await
    });
    let offer = rx.recv().await.expect("offer");
    offer.written.send(Ok(())).unwrap();
    // Go AssignJobTimeout (worker.go:56): no answer inside 10s fails the assign.
    tokio::time::advance(ASSIGN_TIMEOUT + std::time::Duration::from_secs(1)).await;
    assert_eq!(
        assign.await.unwrap().unwrap_err(),
        WorkerError::AvailabilityTimeout
    );
    // The pending slot is cleaned up (worker.go:399-403 defer).
    assert!(
        registry
            .availability(
                "AW_to",
                pb::AvailabilityResponse {
                    job_id: "AJ_to".into(),
                    available: true,
                    ..Default::default()
                }
            )
            .is_ok(),
        "a late answer after the timeout is dropped, not panicked"
    );
}

// ---------------------------------------------------------------------------
// AGENT_WIRE:WorkerMessage.update_worker / ServerMessage (none — inbound-only)
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn update_worker_seq_gate_governs_status_load_and_draining() {
    let h = Harness::new().await;
    let registry = h.state.registry.clone();
    let (tx, _rx) = mpsc::channel::<Outbound>(4);
    registry
        .register(
            Registration::from_request("AW_uw".into(), 1, &pb::RegisterWorkerRequest::default())
                .unwrap(),
            tx,
        )
        .unwrap();
    let apply = |seq: u64, status: i32, load: f32, draining: bool| {
        registry.update_worker(
            "AW_uw",
            &pb::UpdateWorkerStatus {
                status: Some(status),
                seq,
                load,
                draining,
                ..Default::default()
            },
        )
    };
    apply(5, 1 /* WS_FULL */, 0.9, true).unwrap();
    // Stale seq: nothing regresses — not even load/draining (worker.go:642-647
    // returns before all field writes).
    apply(4, 0 /* WS_AVAILABLE */, 0.1, false).unwrap();
    let (status, load, draining, _) = registry.oracle_snapshot("AW_uw");
    assert_eq!((status, load, draining), (1, 0.9, true));
    // seq == 0 bypasses the gate entirely (worker.go:642 `seq != 0`).
    apply(0, 0, 0.3, false).unwrap();
    let (status, load, draining, _) = registry.oracle_snapshot("AW_uw");
    assert_eq!((status, load, draining), (0, 0.3, false));
    // Equal seq is stale too (`seq <= statusSeq`, worker.go:643).
    apply(7, 2, 0.5, false).unwrap();
    apply(7, 0, 0.99, true).unwrap();
    let (status, load, _, _) = registry.oracle_snapshot("AW_uw");
    assert_eq!((status, load), (2, 0.5));
    // Unknown worker updates fail closed (connection teardown in both loops).
    assert!(matches!(
        registry.update_worker("AW_missing", &pb::UpdateWorkerStatus::default()),
        Err(WorkerError::Closed)
    ));
}

// ---------------------------------------------------------------------------
// AGENT_WIRE:WorkerMessage.update_job / ServerMessage.termination
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn update_job_state_machine_and_termination_pairing() {
    let h = Harness::new().await;
    let registry = h.state.registry.clone();
    let (tx, mut rx) = mpsc::channel::<Outbound>(8);
    registry
        .register(
            Registration::from_request("AW_uj".into(), 1, &pb::RegisterWorkerRequest::default())
                .unwrap(),
            tx,
        )
        .unwrap();
    // Update for an unknown job surfaces NotFound and, over the transport, ends
    // the session (worker.go:589 via websocket dispatch arm).
    assert!(matches!(
        registry.update_job(
            "AW_uj",
            &pb::UpdateJobStatus {
                job_id: "AJ_ghost".into(),
                status: 1,
                ..Default::default()
            },
            5,
        ),
        Err(WorkerError::UpdateNotFound)
    ));

    let task_registry = registry.clone();
    let assign = tokio::spawn(async move {
        task_registry
            .assign(
                "AW_uj",
                pb::Job {
                    id: "AJ_uj".into(),
                    ..Default::default()
                },
                100,
                |_, _, _| Ok("tok".into()),
            )
            .await
    });
    let offer = rx.recv().await.unwrap();
    offer.written.send(Ok(())).unwrap();
    registry
        .availability(
            "AW_uj",
            pb::AvailabilityResponse {
                job_id: "AJ_uj".into(),
                available: true,
                participant_identity: "agent-1".into(),
                ..Default::default()
            },
        )
        .unwrap();
    let assignment = rx.recv().await.unwrap();
    assignment.written.send(Ok(())).unwrap();
    let state = assign.await.unwrap().unwrap();
    assert_eq!(
        state.status, 1,
        "AssignJob marks JS_RUNNING (worker.go:413)"
    );
    assert_eq!(state.participant_identity, "agent-1");

    // PENDING->RUNNING stamps StartedAt; RUNNING keeps it (worker.go:595-597).
    registry
        .update_job(
            "AW_uj",
            &pb::UpdateJobStatus {
                job_id: "AJ_uj".into(),
                status: 1,
                ..Default::default()
            },
            200,
        )
        .unwrap();
    let running = registry.job_state("AW_uj", "AJ_uj").unwrap();
    assert_eq!(running.started_at, 100, "started_at preserved once running");
    assert_eq!(running.updated_at, 200);

    // Terminate: ServerMessage.termination plus the SUCCESS state Go derives
    // when the RPC reason is not AGENT_LEFT_ROOM (worker.go:510-521).
    // Ordering: the frame reaches the writer before the local state flip, and
    // the call only completes after the writer ACK (worker.go:504 precedes 517).
    // The pending NAK channel makes the pre-ACK window deterministic, so the
    // call runs as a task while this test drains and ACKs its frame.
    let success_registry = registry.clone();
    let terminate = tokio::spawn(async move {
        success_registry
            .terminate("AW_uj", "AJ_uj", false, 300)
            .await
    });
    let term_frame = rx.recv().await.unwrap();
    let pb::server_message::Message::Termination(t) = term_frame.message.message.clone().unwrap()
    else {
        panic!("termination frame first")
    };
    assert_eq!(t.job_id, "AJ_uj");
    let state = registry.job_state("AW_uj", "AJ_uj").unwrap();
    assert_eq!(state.status, 1);
    term_frame.written.send(Ok(())).unwrap();
    let final_state = terminate.await.unwrap().unwrap();
    assert_eq!(final_state.status, 2, "JS_SUCCESS");
    assert_eq!(final_state.error, "");
    assert_eq!(final_state.ended_at, 300);
    assert_eq!(
        registry.oracle_snapshot("AW_uj").3,
        0,
        "ended job leaves runningJobs (worker.go:602-604)"
    );

    // agent_left=true maps to FAILED with Go's exact error string (worker.go:512-515),
    // observable only while the job is still running. Re-seed it via simulate.
    {
        let r = registry.clone();
        let t = tokio::spawn(async move {
            r.assign(
                "AW_uj",
                pb::Job {
                    id: "AJ_again".into(),
                    ..Default::default()
                },
                500,
                |_, _, _| Ok("t".into()),
            )
            .await
        });
        let offer = rx.recv().await.unwrap();
        offer.written.send(Ok(())).unwrap();
        registry
            .availability(
                "AW_uj",
                pb::AvailabilityResponse {
                    job_id: "AJ_again".into(),
                    available: true,
                    ..Default::default()
                },
            )
            .unwrap();
        rx.recv().await.unwrap().written.send(Ok(())).unwrap();
        t.await.unwrap().unwrap();
        let failed_registry = registry.clone();
        let term = tokio::spawn(async move {
            failed_registry
                .terminate("AW_uj", "AJ_again", true, 600)
                .await
        });
        let frame = rx.recv().await.unwrap();
        frame.written.send(Ok(())).unwrap();
        let failed = term.await.unwrap().unwrap();
        assert_eq!(failed.status, 3, "JS_FAILED when the agent left the room");
        assert_eq!(failed.error, "agent worker left the room");
        assert_eq!(failed.ended_at, 600);
    }
    let err = registry
        .terminate("AW_uj", "AJ_again", true, 700)
        .await
        .unwrap_err();
    assert_eq!(
        err,
        WorkerError::JobNotFound,
        "already ended (worker.go:500-502)"
    );
}

#[tokio::test(start_paused = true)]
async fn failed_termination_never_reports_a_succeeded_job() {
    let h = Harness::new().await;
    let registry = h.state.registry.clone();
    let (tx, mut rx) = mpsc::channel::<Outbound>(8);
    registry
        .register(
            Registration::from_request("AW_ft".into(), 1, &pb::RegisterWorkerRequest::default())
                .unwrap(),
            tx.clone(),
        )
        .unwrap();
    // Seed a running job through the queue we control, then stop answering the
    // frames like a dead writer task, so terminate must surface the write error
    // instead of reporting the job ended.
    let assign_registry = registry.clone();
    let assign = tokio::spawn(async move {
        assign_registry
            .assign(
                "AW_ft",
                pb::Job {
                    id: "AJ_ft".into(),
                    ..Default::default()
                },
                1,
                |_, _, _| Ok("t".into()),
            )
            .await
    });
    let offer = rx.recv().await.unwrap();
    offer.written.send(Ok(())).unwrap();
    registry
        .availability(
            "AW_ft",
            pb::AvailabilityResponse {
                job_id: "AJ_ft".into(),
                available: true,
                ..Default::default()
            },
        )
        .unwrap();
    rx.recv().await.unwrap().written.send(Ok(())).unwrap();
    assign.await.unwrap().unwrap();
    // Stop answering: the writer is effectively dead for subsequent frames.
    drop(rx);
    let err = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        registry.terminate("AW_ft", "AJ_ft", false, 1),
    )
    .await
    .expect("terminate must not hang on a dead writer")
    .unwrap_err();
    assert_eq!(
        err,
        WorkerError::Closed,
        "unwritable connection fails closed"
    );
    // Go still applies the status update after a failed write (worker.go:517
    // runs regardless of writeErr); Rust mirrors that but surfaces the error so
    // no caller can treat the termination as clean success.
    // And with a tracked job but a dead writer, terminate errors instead of
    // reporting the job ended.
    let registry2 = Registry::new(Limits::default());
    let (tx2, mut rx2) = mpsc::channel::<Outbound>(8);
    registry2
        .register(
            Registration::from_request("AW_d".into(), 1, &pb::RegisterWorkerRequest::default())
                .unwrap(),
            tx2.clone(),
        )
        .unwrap();
    let a2 = registry2.clone();
    let assign2 = tokio::spawn(async move {
        a2.assign(
            "AW_d",
            pb::Job {
                id: "AJ_d".into(),
                ..Default::default()
            },
            1,
            |_, _, _| Ok("t".into()),
        )
        .await
    });
    let o2 = rx2.recv().await.unwrap();
    o2.written.send(Ok(())).unwrap();
    registry2
        .availability(
            "AW_d",
            pb::AvailabilityResponse {
                job_id: "AJ_d".into(),
                available: true,
                ..Default::default()
            },
        )
        .unwrap();
    let a_frame = rx2.recv().await.unwrap();
    // Kill the writer before acknowledging the assignment: the running entry
    // must roll back (Go deletes runningJobs on send failure, worker.go:476-480).
    drop(rx2);
    a_frame.written.send(Err(WorkerError::Closed)).unwrap();
    assert!(matches!(assign2.await.unwrap(), Err(WorkerError::Closed)));
    assert_eq!(registry2.oracle_snapshot("AW_d").3, 0);
    assert!(matches!(
        registry2.terminate("AW_d", "AJ_d", false, 1).await,
        Err(WorkerError::JobNotFound)
    ));
}

// ---------------------------------------------------------------------------
// AGENT_WIRE:WorkerMessage.simulate_job / migrate_job
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn simulate_job_produces_the_oracle_job_shape_and_migrate_job_stays_in_oss_noop() {
    let h = Harness::new().await;
    let registry = h.state.registry.clone();
    let (tx, mut rx) = mpsc::channel::<Outbound>(8);
    registry
        .register(
            Registration::from_request(
                "AW_sim".into(),
                1,
                &pb::RegisterWorkerRequest {
                    r#type: 0,
                    agent_name: "sim-agent".into(),
                    namespace: Some("sim-ns".into()),
                    ..Default::default()
                },
            )
            .unwrap(),
            tx,
        )
        .unwrap();
    // Simulate over the production dispatch arm requires the websocket loop;
    // here drive the identical registry effect the arm performs.
    let request = pb::SimulateJobRequest {
        r#type: 0,
        room: Some(pb::Room {
            name: "sim-room".into(),
            ..Default::default()
        }),
        participant: None,
    };
    // Job construction mirrors worker.go:612-625: JT_ROOM unless a participant
    // is given (then JT_PUBLISHER — the request's own `type` field is ignored by
    // Go), ids prefixed AJ_.
    let job = pb::Job {
        id: rtc_core::ids::new_sid("AJ_"),
        r#type: if request.participant.is_some() { 1 } else { 0 },
        room: request.room.clone(),
        participant: request.participant.clone(),
        namespace: "sim-ns".into(),
        agent_name: "sim-agent".into(),
        ..Default::default()
    };
    assert!(job.id.starts_with("AJ_"));
    assert_eq!(job.r#type, 0);
    let task_registry = registry.clone();
    let assign_job = job.clone();
    let assign = tokio::spawn(async move {
        task_registry
            .assign("AW_sim", assign_job, 1, |_, _, _| Ok("t".into()))
            .await
    });
    let offer = rx.recv().await.unwrap();
    let pb::server_message::Message::Availability(av) = offer.message.message.unwrap() else {
        panic!("simulate must offer availability")
    };
    // Go stamps JobState on the stored clone before writing (worker.go:405-413),
    // so the offered job carries the running state; identity fields are the contract.
    let offered = av.job.expect("offered job");
    assert_eq!(offered.id, job.id);
    assert_eq!(
        (
            offered.r#type,
            offered.namespace.as_str(),
            offered.agent_name.as_str()
        ),
        (0, "sim-ns", "sim-agent")
    );
    assert_eq!(offered.room.unwrap().name, "sim-room");
    assert_eq!(
        offered.state.expect("stamped state").status,
        1,
        "JS_RUNNING at offer time"
    );
    offer.written.send(Ok(())).unwrap();
    registry
        .availability(
            "AW_sim",
            pb::AvailabilityResponse {
                job_id: job.id.clone(),
                available: true,
                participant_identity: "sim-agent-a".into(),
                ..Default::default()
            },
        )
        .unwrap();
    let assignment = rx.recv().await.unwrap();
    let pb::server_message::Message::Assignment(a) = assignment.message.message.clone().unwrap()
    else {
        panic!("assignment")
    };
    assert_eq!(a.job.unwrap().id, job.id);
    assignment.written.send(Ok(())).unwrap();
    assert_eq!(assign.await.unwrap().unwrap().status, 1);

    // MigrateJob: pinned OSS acknowledges the signal and changes nothing
    // (worker.go:665-669 TODO). The end-to-end proof is in the live-loop test.
    assert_eq!(registry.oracle_snapshot("AW_sim").3, 1);
}

#[tokio::test(start_paused = true)]
async fn migrate_and_simulate_are_wired_through_the_real_dispatch_loop() {
    let mut h = Harness::new().await;
    let registry = h.state.registry.clone();
    let (addr, _server, _server_events) = h.ws_server().await;
    let mut peer = h.ws_connect(addr).await;
    peer.send_json(b"{\"register\":{\"type\":\"JT_ROOM\",\"agentName\":\"loop-agent\",\"namespace\":\"loop-ns\"}}")
        .await;
    let reply = peer.receive().await;
    let pb::server_message::Message::Register(r) = reply.one().clone() else {
        panic!("register")
    };
    let worker_id = r.worker_id;

    // SimulateJob through the production arm: the server must offer a real
    // availability for an AJ_-prefixed job in room loop-ns/loop-agent.
    peer.send_proto(&worker_message(pb::worker_message::Message::SimulateJob(
        pb::SimulateJobRequest {
            r#type: 0,
            room: Some(pb::Room {
                name: "sim-live".into(),
                ..Default::default()
            }),
            participant: None,
        },
    )))
    .await;
    let offer = peer.receive().await;
    let pb::server_message::Message::Availability(av) = offer.one().clone() else {
        panic!("simulate must produce ServerMessage.availability through the live loop")
    };
    let job = av.job.unwrap();
    assert!(job.id.starts_with("AJ_"));
    assert_eq!(job.namespace, "loop-ns");
    assert_eq!(job.agent_name, "loop-agent");
    assert_eq!(
        job.r#type, 0,
        "JT_ROOM without participant (worker.go:613-616)"
    );
    assert_eq!(job.room.unwrap().name, "sim-live");

    // Decline availability so the assignment never lands.
    peer.send_proto(&worker_message(pb::worker_message::Message::Availability(
        pb::AvailabilityResponse {
            job_id: job.id.clone(),
            available: false,
            ..Default::default()
        },
    )))
    .await;

    // MigrateJob through the same loop: no reply, no teardown, no state change
    // (worker.go:665-669: OSS acknowledges and does nothing).
    let before = registry.oracle_snapshot(&worker_id);
    peer.send_proto(&worker_message(pb::worker_message::Message::MigrateJob(
        pb::MigrateJobRequest {
            job_ids: vec![job.id.clone()],
        },
    )))
    .await;
    // Fence: a ping afterwards still round-trips, proving the connection and
    // worker survived the migration signal.
    peer.send_proto(&worker_message(pb::worker_message::Message::Ping(
        pb::WorkerPing { timestamp: 99 },
    )))
    .await;
    let pong = peer.receive().await;
    assert!(matches!(pong.one(), pb::server_message::Message::Pong(p) if p.last_timestamp == 99));
    assert!(registry.registration(&worker_id).is_some());
    assert_eq!(
        registry.oracle_snapshot(&worker_id),
        before,
        "migrate mutates nothing; declined simulate left no running job"
    );
}

// ---------------------------------------------------------------------------
// AGENT_WIRE:ServerMessage.go_away
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn go_away_is_protocol_defined_but_has_no_pinned_go_producer() {
    // The message exists in the pinned protocol (protobufs/livekit_agent.proto:84,
    // field 6 `AgentHttp.GoAway go_away`), but no non-generated Go source in
    // livekit-stable@e5655ac constructs or sends it (grep: only .pb.go hits).
    // Rust therefore must not invent a producer; the audit claim is recorded by
    // proving the encoded shape round-trips through the real wire codec so a
    // future producer cannot silently change the field-6 framing.
    let message = pb::ServerMessage {
        message: Some(pb::server_message::Message::GoAway(
            pb::agent_http::GoAway {
                reason: "node drain".into(),
            },
        )),
    };
    let bytes = prost::Message::encode_to_vec(&message);
    let decoded: pb::ServerMessage = prost::Message::decode(&bytes[..]).unwrap();
    assert_eq!(decoded, message);
    let json = lk_protocol::json::encode_default(&message).unwrap();
    let json = String::from_utf8(json).unwrap();
    assert!(
        json.contains("\"goAway\""),
        "protojson lowerCamel naming: {json}"
    );
    let from_json: pb::ServerMessage = lk_protocol::json::decode(json.as_bytes()).unwrap();
    assert_eq!(from_json, message);
}

// ---------------------------------------------------------------------------
// Transport parity: the WebTransport control plane shares these semantics.
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn wt_control_plane_shares_register_pong_and_dispatch_semantics() {
    let mut h = Harness::new().await;
    let registry = h.state.registry.clone();
    let (peer, sess) = tokio::io::duplex(1 << 16);
    let (mut pr, mut pw) = tokio::io::split(peer);
    let (sr, sw) = tokio::io::split(sess);
    // The duplex buffers the write, so the register frame goes in first and the
    // helper's bookkeeping fence can observe the resulting event.
    send_worker_frame(&mut pw, &register_request("wt-leaf")).await;
    let (task, mut session_events) = h.wt_session(sr, sw).await;

    let reply = recv_frame(&mut pr).await.expect("register reply");
    let pb::server_message::Message::Register(r) = reply.message.unwrap() else {
        panic!("register")
    };
    assert!(r.worker_id.starts_with("AW_"));
    assert!(
        r.endpoint_settings.is_none(),
        "control-only WT session negotiates no endpoints"
    );
    let worker_id = r.worker_id;

    send_worker_frame(
        &mut pw,
        &worker_message(pb::worker_message::Message::Ping(pb::WorkerPing {
            timestamp: 7,
        })),
    )
    .await;
    let pong = recv_frame(&mut pr).await.expect("pong");
    assert!(
        matches!(pong.message.unwrap(), pb::server_message::Message::Pong(p) if p.last_timestamp == 7)
    );

    // UpdateWorker flows into the shared registry through the WT dispatch loop.
    send_worker_frame(
        &mut pw,
        &worker_message(pb::worker_message::Message::UpdateWorker(
            pb::UpdateWorkerStatus {
                status: Some(1),
                seq: 3,
                load: 0.5,
                ..Default::default()
            },
        )),
    )
    .await;
    // Fence via ping: receipt proves the update was processed first (single
    // ordered read loop, mirroring Go's serial DispatchAgentWorkerSignal).
    send_worker_frame(
        &mut pw,
        &worker_message(pb::worker_message::Message::Ping(pb::WorkerPing {
            timestamp: 8,
        })),
    )
    .await;
    let pong = recv_frame(&mut pr).await.expect("pong2");
    let _ = pong;
    let (status, load, _, _) = registry.oracle_snapshot(&worker_id);
    assert_eq!((status, load), (1, 0.5));

    // Unknown signal ends the session without a reply (worker.go:99-100).
    // An empty WorkerMessage decodes to `message: None`, the Rust arm for Go's
    // default case (`ErrUnknownWorkerSignal`).
    send_worker_frame(&mut pw, &pb::WorkerMessage { message: None }).await;
    assert!(
        recv_frame(&mut pr).await.is_none(),
        "unknown signal tears the control stream down"
    );
    task.await.unwrap();
    assert!(
        registry.registration(&worker_id).is_none(),
        "disconnect deregisters (agenthandler.go:217)"
    );
    let mut tail = vec![recv_event(&mut session_events).await];
    while let Ok(event) = session_events.try_recv() {
        tail.push(Some(event));
    }
    assert!(
        tail.iter()
            .any(|e| matches!(e, Some(WorkerEvent::Disconnected { .. }))),
        "disconnect reaches the consumer, tail={tail:?}"
    );
}
