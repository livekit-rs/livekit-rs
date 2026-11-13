use super::super::{
    registry::{Limits, Registration as WorkerRegistration, Registry as WorkerRegistry},
    websocket::WorkerHttpState,
};
use super::*;
use crate::auth::{KeyStore, TokenMinter};
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use lk_protocol::livekit as pb;
use prost::Message;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, DuplexStream},
    sync::{mpsc, watch},
};
use tower::ServiceExt;
fn fixture() -> WorkerHttpState {
    let keys = Arc::new(KeyStore::new(vec![(
        "key".into(),
        "secret-of-at-least-32-bytes-abcdefgh".into(),
    )]));
    let tokens = Arc::new(TokenMinter::new(keys.clone()));
    let (events, _) = mpsc::channel(8);
    let (_, shutdown) = watch::channel(false);
    WorkerHttpState {
        registry: WorkerRegistry::new(Limits::default()),
        keys,
        tokens,
        server_info: Default::default(),
        signal_message_size_limit: 1 << 20,
        events,
        shutdown,
        _keepalive: None,
    }
}
fn request(public: bool, path: &str) -> pb::RegisterWorkerRequest {
    pb::RegisterWorkerRequest {
        agent_name: "worker".into(),
        instance_id: "instance".into(),
        endpoint_protocol: 7,
        endpoints: vec![pb::agent_http::AgentEndpoint {
            path: path.into(),
            methods: vec!["POST".into(), "GET".into()],
            kind: 0,
            public,
        }],
        ..Default::default()
    }
}
fn install(
    state: &WorkerHttpState,
    id: &str,
    req: &pb::RegisterWorkerRequest,
) -> mpsc::UnboundedReceiver<DuplexStream> {
    let registration = WorkerRegistration::from_request(id.into(), 1, req).unwrap();
    let registration = WorkerRegistration {
        api_key: "key".into(),
        ..registration
    };
    let (out, _) = mpsc::channel(8);
    state.registry.register(registration.clone(), out).unwrap();
    let (tx, rx) = mpsc::unbounded_channel();
    let opener: StreamOpener = Arc::new(move || {
        let tx = tx.clone();
        Box::pin(async move {
            let (node, worker) = tokio::io::duplex(8192);
            tx.send(worker)
                .map_err(|_| std::io::Error::other("worker gone"))?;
            Ok(Box::new(node) as StreamIo)
        })
    });
    let manifest = state.registry.endpoints.negotiate(req).unwrap();
    state
        .registry
        .endpoints
        .register(&registration, manifest, opener);
    rx
}
fn token(agent: &str, deployment: &str, call: bool) -> String {
    let claims = serde_json::json!({"iss":"key","sub":"caller","exp":4102444800u64,
        "agentEndpoint":{"call":call,"agentName":agent,"deployment":deployment}});
    // Sign independently of jsonwebtoken's process-global CryptoProvider. The
    // production verifier still validates the real HS256 signature and grants.
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    use hmac::{Hmac, Mac};
    let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256","typ":"JWT"}"#);
    let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap());
    let signing = format!("{header}.{payload}");
    let mut mac =
        Hmac::<sha2::Sha256>::new_from_slice(b"secret-of-at-least-32-bytes-abcdefgh").unwrap();
    mac.update(signing.as_bytes());
    format!(
        "{signing}.{}",
        URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
    )
}
async fn preamble(stream: &mut DuplexStream) -> pb::agent_http::StreamPreamble {
    let len = stream.read_u32().await.unwrap();
    assert!(len <= 64 << 10);
    let mut bytes = vec![0; len as usize];
    stream.read_exact(&mut bytes).await.unwrap();
    pb::agent_http::StreamPreamble::decode(bytes.as_slice()).unwrap()
}
// Read the actual request head before responding; otherwise a fake worker can
// race Hyper's request dispatch and make an unrelated early-response error pass.
async fn request_head(stream: &mut DuplexStream) {
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        assert!(head.len() < 64 << 10);
        head.push(stream.read_u8().await.unwrap());
    }
    assert!(head.starts_with(b"GET ") || head.starts_with(b"POST "));
}
async fn settled(reg: &Arc<Registration>) {
    for _ in 0..20 {
        if reg.inflight() == 0 {
            return;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(reg.inflight(), 0, "stream capacity was not released");
}
#[test]
fn manifest_negotiation_keeps_fixed_converter_and_method_contract() {
    for (tpl, yes, no) in [
        ("/x/{id:int}", "/x/12", "/x/-1"),
        ("/x/{id:float}", "/x/12.3", "/x/.3"),
        ("/x/{id}", "/x/a", "/x/a/b"),
        ("/x/{id:path}", "/x/a/b", "/x/a\nb"),
        (
            "/x/{id:uuid}",
            "/x/123456781234123412341234567890ab",
            "/x/nope",
        ),
    ] {
        let manifest = manifest::negotiate(&request(true, tpl)).unwrap();
        assert!(manifest[0].matches(yes), "{tpl}");
        assert!(!manifest[0].matches(no), "{tpl}");
    }
    for tpl in ["/x/{id:custom}", "/x/{id}/{id}", "relative"] {
        assert!(manifest::negotiate(&request(true, tpl)).is_err());
    }
    let a = manifest::negotiate(&request(true, "/x/{a}")).unwrap();
    let b = manifest::negotiate(&request(true, "/x/{b:str}")).unwrap();
    assert_eq!(a[0].canonical, b[0].canonical);
    let mut r = request(true, "/");
    r.endpoints[0].methods = vec!["get".into()];
    assert!(manifest::negotiate(&r).is_err());
    r = request(true, "/");
    r.instance_id.clear();
    assert!(manifest::negotiate(&r).is_err());
    r = request(true, "/");
    r.endpoint_protocol = 0;
    assert!(manifest::negotiate(&r).is_err());
    r = request(true, "/");
    r.agent_name = "_".into();
    assert!(manifest::negotiate(&r).is_err());
}
#[tokio::test]
async fn max_streams_is_soft_and_disconnect_closes_epoch_releasing_exactly_once() {
    let s = fixture();
    s.registry.endpoints.configure(false, 1);
    let mut rx = install(&s, "AW_a", &request(true, "/"));
    let reg = s.registry.endpoints.scope("key", "worker", "")[0].clone();
    assert_eq!(reg.spare_streams(), 1);
    let (a, lease_a) = reg.open().await.unwrap();
    let _peer_a = rx.recv().await.unwrap();
    assert_eq!(reg.inflight(), 1);
    assert_eq!(reg.spare_streams(), 0);
    let (b, lease_b) = reg.open().await.unwrap();
    let _peer_b = rx.recv().await.unwrap();
    assert_eq!(
        reg.inflight(),
        2,
        "soft cap must not reject a live QUIC stream"
    );
    s.registry.disconnect("AW_a");
    assert!(!reg.has_session());
    assert!(reg.open().await.is_err());
    drop((a, lease_a));
    assert_eq!(reg.inflight(), 1);
    drop((b, lease_b));
    assert_eq!(reg.inflight(), 0);
    let _new = install(&s, "AW_a", &request(true, "/"));
    let fresh = s.registry.endpoints.scope("key", "worker", "")[0].clone();
    assert!(!Arc::ptr_eq(&reg, &fresh));
    assert_eq!(fresh.inflight(), 0);
    assert_eq!(fresh.spare_streams(), 1);
}
#[tokio::test]
async fn pick_routes_to_least_inflight_and_filters_draining_closed_and_attempted() {
    use super::front::{Candidate, pick};
    let s = fixture();
    s.registry.endpoints.configure(false, 8);
    let mut rx_a = install(&s, "AW_a", &request(true, "/"));
    let a = s.registry.endpoints.scope("key", "worker", "")[0].clone();
    let mut rx_b = install(&s, "AW_b", &request(true, "/"));
    let b = s
        .registry
        .endpoints
        .scope("key", "worker", "")
        .into_iter()
        .find(|r| !Arc::ptr_eq(r, &a))
        .unwrap();
    let cand = |reg: Arc<Registration>| Candidate {
        reg,
        raw: String::new(),
        public: true,
    };
    async fn load(
        reg: &Arc<Registration>,
        rx: &mut mpsc::UnboundedReceiver<DuplexStream>,
        n: usize,
    ) -> Vec<StreamLease> {
        let mut held = Vec::new();
        for _ in 0..n {
            let (_io, lease) = reg.open().await.unwrap();
            let _peer = rx.recv().await.unwrap();
            held.push(lease);
        }
        held
    }

    // With exactly two eligible workers, power-of-two-choices compares both on
    // every draw, so a strict load imbalance must deterministically pick the
    // least-inflight worker. This is the W05 local selection consumer.
    let held_b = load(&b, &mut rx_b, 3).await;
    assert_eq!(a.inflight(), 0);
    assert_eq!(b.inflight(), 3);
    for _ in 0..64 {
        let chosen = pick(vec![cand(a.clone()), cand(b.clone())]).unwrap();
        assert!(
            Arc::ptr_eq(&chosen.reg, &a),
            "p2c must pick the least-inflight worker on every draw"
        );
    }

    // Equal load must reach both workers across draws, not a hardcoded choice.
    drop(held_b);
    settled(&b).await;
    let mut saw_a = false;
    let mut saw_b = false;
    for _ in 0..256 {
        let chosen = pick(vec![cand(a.clone()), cand(b.clone())]).unwrap();
        saw_a |= Arc::ptr_eq(&chosen.reg, &a);
        saw_b |= Arc::ptr_eq(&chosen.reg, &b);
    }
    assert!(saw_a && saw_b, "equal load must reach both workers");

    // A draining worker is ineligible even when it is the only low-load option:
    // load the other worker and confirm selection never lands on the drainer.
    s.registry.endpoints.drain("AW_b", true);
    let held_a = load(&a, &mut rx_a, 3).await;
    assert_eq!(b.inflight(), 0);
    for _ in 0..32 {
        let chosen = pick(vec![cand(a.clone()), cand(b.clone())]).unwrap();
        assert!(
            Arc::ptr_eq(&chosen.reg, &a),
            "draining worker must be excluded regardless of load"
        );
    }
    s.registry.endpoints.drain("AW_b", false);
    drop(held_a);
    settled(&a).await;

    // A worker without a live session is ineligible; a single eligible worker is
    // returned directly; no eligible worker yields None.
    let single = pick(vec![cand(a.clone())]).unwrap();
    assert!(Arc::ptr_eq(&single.reg, &a));
    b.close();
    assert!(!b.has_session());
    for _ in 0..16 {
        let chosen = pick(vec![cand(a.clone()), cand(b.clone())]).unwrap();
        assert!(Arc::ptr_eq(&chosen.reg, &a));
    }
    a.close();
    assert!(pick(vec![cand(a.clone()), cand(b.clone())]).is_none());
}
#[tokio::test]
async fn private_routes_require_exact_endpoint_grant_and_never_video_agent() {
    let s = fixture();
    let mut rx = install(&s, "AW_private", &request(false, "/secret"));
    let app = routes(s.clone());
    for (authorization, expected) in [
        (None, StatusCode::UNAUTHORIZED),
        (Some(token("wrong", "", true)), StatusCode::FORBIDDEN),
        (Some(token("worker", "wrong", true)), StatusCode::FORBIDDEN),
        (Some(token("worker", "", false)), StatusCode::FORBIDDEN),
    ] {
        let mut req = Request::builder().uri("/agents/worker/_/secret");
        if let Some(token) = authorization {
            req = req.header("authorization", format!("Bearer {token}"));
        }
        let response = app
            .clone()
            .oneshot(req.body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
    }
    assert!(
        rx.try_recv().is_err(),
        "rejected authentication must not open a worker stream"
    );
    s.registry.endpoints.configure(true, 0);
    assert_eq!(
        routes(s.clone())
            .oneshot(
                Request::builder()
                    .uri("/agents/worker/_/secret")
                    .body(Body::empty())
                    .unwrap()
            )
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
}
#[tokio::test]
async fn query_credentials_authenticate_the_private_route_and_never_reach_worker_http() {
    let state = fixture();
    let mut streams = install(&state, "AW_query_token", &request(false, "/who"));
    let reg = state.registry.endpoints.scope("key", "worker", "")[0].clone();
    let app = routes(state);
    let token = token("worker", "", true);
    for (query, expected) in [
        (
            format!("access_token={token}&x=1&&b=%2F&access_token=duplicate&&x=%2f"),
            "/who?x=1&&b=%2F&&x=%2f",
        ),
        (
            format!(
                "acc%65ss_token={token}&bad%ZZ=raw+value&access_token=%ZZ&Access_token=app&access_token"
            ),
            "/who?bad%ZZ=raw+value&Access_token=app",
        ),
        (format!("access_token={token}"), "/who"),
    ] {
        let request = Request::builder()
            .uri(format!("/agents/worker/_/who?{query}"))
            .body(Body::empty())
            .unwrap();
        let peer = async {
            let mut stream = streams
                .recv()
                .await
                .expect("private route opened a worker stream");
            assert!(preamble(&mut stream).await.authorized);
            let mut read = BufReader::new(&mut stream);
            let mut first = String::new();
            read.read_line(&mut first).await.unwrap();
            // Do not print any credential into a before-fix failure receipt.
            assert!(
                !first.contains(&token),
                "worker received a signed node credential"
            );
            assert_eq!(first, format!("GET {expected} HTTP/1.1\r\n"));
            loop {
                let mut line = String::new();
                assert_ne!(read.read_line(&mut line).await.unwrap(), 0);
                if line == "\r\n" {
                    break;
                }
                assert!(!line.contains(&token));
            }
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                .await
                .unwrap();
            stream.shutdown().await.unwrap();
        };
        let client = async {
            let response = app.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(to_bytes(response.into_body(), 64).await.unwrap(), "ok");
        };
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            tokio::join!(peer, client);
        })
        .await
        .expect("authenticated worker HTTP exchange must complete");
        settled(&reg).await;
    }
}

#[tokio::test]
async fn actual_http_exchange_preserves_encoded_target_and_streams_both_directions() {
    let s = fixture();
    let mut incoming = install(&s, "AW_stream", &request(false, "/echo/{path:path}"));
    let reg = s.registry.endpoints.scope("key", "worker", "")[0].clone();
    let (tx, rx) = mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(2);
    tx.send(Ok(bytes::Bytes::from_static(b"hello")))
        .await
        .unwrap();
    let body = Body::from_stream(futures_util::stream::unfold(rx, |mut rx| async {
        rx.recv().await.map(|v| (v, rx))
    }));
    let app = routes(s.clone());
    let client = tokio::spawn(async move {
        app.oneshot(
            Request::builder()
                .method("POST")
                .uri("/agents/worker/_/echo/a%2Fb?q=%3F")
                .header("host", "example.test")
                .header("x-lk-completion", "forged")
                .header(
                    "authorization",
                    format!("Bearer {}", token("worker", "", true)),
                )
                .body(body)
                .unwrap(),
        )
        .await
        .unwrap()
    });
    let worker = tokio::spawn(async move {
        let mut stream = incoming.recv().await.unwrap();
        let p = preamble(&mut stream).await;
        assert!(p.authorized);
        assert_eq!(p.route, "/echo/{path:path}");
        assert_eq!(p.scheme, "http");
        let (read, mut write) = tokio::io::split(stream);
        let mut read = BufReader::new(read);
        let mut first = String::new();
        read.read_line(&mut first).await.unwrap();
        assert_eq!(first, "POST /echo/a%2Fb?q=%3F HTTP/1.1\r\n");
        let mut headers = String::new();
        loop {
            let mut line = String::new();
            read.read_line(&mut line).await.unwrap();
            if line == "\r\n" {
                break;
            }
            headers.push_str(&line.to_ascii_lowercase());
        }
        assert!(!headers.contains("authorization:"));
        assert!(!headers.contains("x-lk-completion:"));
        let mut chunk = String::new();
        read.read_line(&mut chunk).await.unwrap();
        assert_eq!(usize::from_str_radix(chunk.trim(), 16).unwrap(), 5);
        let mut hello = [0; 7];
        read.read_exact(&mut hello).await.unwrap();
        assert_eq!(&hello, b"hello\r\n");
        // Send the response before the request body is complete: collecting or
        // serial request/response copying would deadlock this test.
        write.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nX-Lk-Error: must-not-leak\r\n\r\n5\r\nready\r\n").await.unwrap();
        write.flush().await.unwrap();
        let mut tail = Vec::new();
        loop {
            let mut line = String::new();
            read.read_line(&mut line).await.unwrap();
            let len = usize::from_str_radix(line.trim(), 16).unwrap();
            if len == 0 {
                break;
            }
            let mut data = vec![0; len + 2];
            read.read_exact(&mut data).await.unwrap();
            tail.extend_from_slice(&data[..len]);
        }
        assert_eq!(tail, b"world");
        write.write_all(b"4\r\ndone\r\n0\r\n\r\n").await.unwrap();
        write.shutdown().await.unwrap();
    });
    let response = tokio::time::timeout(std::time::Duration::from_secs(2), client)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(!response.headers().contains_key("x-lk-error"));
    assert_eq!(reg.inflight(), 1);
    tx.send(Ok(bytes::Bytes::from_static(b"world")))
        .await
        .unwrap();
    drop(tx);
    assert_eq!(
        to_bytes(response.into_body(), 64).await.unwrap(),
        "readydone"
    );
    worker.await.unwrap();
    settled(&reg).await;
}
#[tokio::test]
async fn client_response_drop_cancels_driver_and_releases_capacity() {
    let s = fixture();
    let mut rx = install(&s, "AW_cancel", &request(true, "/slow"));
    let reg = s.registry.endpoints.scope("key", "worker", "")[0].clone();
    let client = tokio::spawn(
        routes(s).oneshot(
            Request::builder()
                .uri("/agents/worker/_/slow")
                .body(Body::empty())
                .unwrap(),
        ),
    );
    let mut worker = rx.recv().await.unwrap();
    let _ = preamble(&mut worker).await;
    request_head(&mut worker).await;
    // A real incomplete response body keeps the HTTP driver and lease alive.
    worker
        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 999\r\n\r\nx")
        .await
        .unwrap();
    let response = client.await.unwrap().unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(reg.inflight(), 1);
    drop(response);
    settled(&reg).await;
}

#[tokio::test]
async fn websocket_upgrade_splices_raw_frames_and_drains_with_boundary_close() {
    let state = fixture();
    state.registry.endpoints.configure(false, 4);
    state.registry.endpoints.configure_tunnels(1, 1_000_000);
    let mut streams = install(&state, "AW_ws", &request(true, "/socket"));
    let reg = state.registry.endpoints.scope("key", "worker", "")[0].clone();
    let registry = state.registry.endpoints.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, shutdown) = tokio::sync::oneshot::channel::<()>();
    let app = routes(state.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = shutdown.await;
            })
            .await
            .unwrap();
    });
    let worker = tokio::spawn(async move {
        let mut stream = streams.recv().await.unwrap();
        let p = preamble(&mut stream).await;
        assert_eq!(p.route, "/socket");
        let mut stream = BufReader::new(stream);
        let mut head = String::new();
        loop {
            let mut line = String::new();
            stream.read_line(&mut line).await.unwrap();
            head.push_str(&line);
            if line == "\r\n" {
                break;
            }
        }
        assert!(
            head.to_ascii_lowercase()
                .contains("connection: upgrade\r\n")
        );
        assert!(head.to_ascii_lowercase().contains("upgrade: websocket\r\n"));
        stream.get_mut().write_all(b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n\x81\x02ok").await.unwrap();
        let mut frame = [0u8; 8];
        stream.read_exact(&mut frame).await.unwrap();
        assert_eq!(
            frame,
            [0x81, 0x82, 1, 2, 3, 4, b'h' ^ 1, b'i' ^ 2],
            "client frames must remain masked and byte exact"
        );
        stream.get_mut().write_all(&[0x89, 0]).await.unwrap();
        let mut end = [0u8; 1];
        assert_eq!(stream.read(&mut end).await.unwrap(), 0);
    });
    let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
    client.write_all(b"GET /agents/worker/_/socket HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n").await.unwrap();
    let mut client = BufReader::new(client);
    let mut status = String::new();
    client.read_line(&mut status).await.unwrap();
    assert!(status.starts_with("HTTP/1.1 101 "), "{status}");
    loop {
        let mut line = String::new();
        client.read_line(&mut line).await.unwrap();
        if line == "\r\n" {
            break;
        }
    }
    let mut greeting = [0u8; 4];
    client.read_exact(&mut greeting).await.unwrap();
    assert_eq!(greeting, [0x81, 0x02, b'o', b'k']);
    assert_eq!(registry.open_tunnels(), 1);
    assert_eq!(reg.inflight(), 1);
    let blocked = routes(state)
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/agents/worker/_/socket")
                .header("connection", "upgrade")
                .header("upgrade", "websocket")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        blocked.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "max_tunnels must reject an extra handshake, while HTTP streams stay soft-capped"
    );
    client
        .get_mut()
        .write_all(&[0x81, 0x82, 1, 2, 3, 4, b'h' ^ 1, b'i' ^ 2])
        .await
        .unwrap();
    let mut ping = [0u8; 2];
    client.read_exact(&mut ping).await.unwrap();
    assert_eq!(ping, [0x89, 0]);
    let drain = tokio::spawn(async move {
        registry.drain_tunnels().await;
        registry.open_tunnels()
    });
    let mut close = [0u8; 4];
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        client.read_exact(&mut close),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(close, [0x88, 0x02, 0x03, 0xe9]);
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(2), drain)
            .await
            .unwrap()
            .unwrap(),
        0
    );
    drop(client);
    worker.await.unwrap();
    settled(&reg).await;
    let _ = stop.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn informational_head_limit_and_bad_final_status_are_gateway_errors() {
    for (payload, expected) in [
        (
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n".to_string(),
            StatusCode::OK,
        ),
        (
            format!(
                "{}HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
                "HTTP/1.1 103 Early Hints\r\nLink: </x>\r\n\r\n".repeat(8)
            ),
            StatusCode::OK,
        ),
        (
            format!(
                "{}HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
                "HTTP/1.1 103 Early Hints\r\nLink: </x>\r\n\r\n".repeat(9)
            ),
            StatusCode::BAD_GATEWAY,
        ),
        (
            "HTTP/1.1 600 Invalid\r\nContent-Length: 0\r\n\r\n".to_string(),
            StatusCode::BAD_GATEWAY,
        ),
    ] {
        let s = fixture();
        let mut incoming = install(&s, "AW_head", &request(true, "/head"));
        let reg = s.registry.endpoints.scope("key", "worker", "")[0].clone();
        let client = tokio::spawn(
            routes(s).oneshot(
                Request::builder()
                    .uri("/agents/worker/_/head")
                    .body(Body::empty())
                    .unwrap(),
            ),
        );
        let mut worker = incoming.recv().await.unwrap();
        let _ = preamble(&mut worker).await;
        request_head(&mut worker).await;
        worker.write_all(payload.as_bytes()).await.unwrap();
        let response = client.await.unwrap().unwrap();
        assert_eq!(response.status(), expected);
        drop(response);
        settled(&reg).await;
    }
}

#[tokio::test]
async fn truncated_or_failed_completion_bodies_are_errors_never_clean_success() {
    for payload in [b"HTTP/1.1 200 OK\r\nContent-Length: 20\r\n\r\nshort".as_slice(),
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nx\r\n0\r\nx-lk-completion: truncated\r\nx-lk-error: short\r\n\r\n".as_slice()] {
        let s=fixture();let mut incoming=install(&s,"AW_truncated",&request(true,"/body"));let reg=s.registry.endpoints.scope("key","worker","")[0].clone();
        let client=tokio::spawn(routes(s).oneshot(Request::builder().uri("/agents/worker/_/body").body(Body::empty()).unwrap()));
        let mut worker=incoming.recv().await.unwrap();let _=preamble(&mut worker).await;request_head(&mut worker).await;worker.write_all(payload).await.unwrap();worker.shutdown().await.unwrap();
        let response=client.await.unwrap().unwrap();assert_eq!(response.status(),StatusCode::OK);assert!(to_bytes(response.into_body(),128).await.is_err());settled(&reg).await;
    }
}
