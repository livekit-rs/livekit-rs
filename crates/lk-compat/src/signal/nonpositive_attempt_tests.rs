//! Real public HTTP upgrade and websocket bytes; no broker/owner session for0.
use super::*;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn upgrade(address: SocketAddr, token: &str) -> tokio::net::TcpStream {
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream.write_all(format!("GET /rtc?protocol=17&access_token={token} HTTP/1.1\r\nHost: {address}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n").as_bytes()).await.unwrap();
    let mut header = Vec::new();
    while !header.ends_with(b"\r\n\r\n") {
        assert!(header.len() < 16384);
        header.push(stream.read_u8().await.unwrap());
    }
    assert!(
        header.starts_with(b"HTTP/1.1 101 "),
        "{}",
        String::from_utf8_lossy(&header)
    );
    stream
}
async fn read_frame(stream: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
    let first = stream.read_u8().await.unwrap();
    let second = stream.read_u8().await.unwrap();
    assert_eq!(first & 0x80, 0x80, "final frame");
    assert_eq!(second & 0x80, 0, "server frames unmasked");
    let length = match second & 127 {
        126 => stream.read_u16().await.unwrap() as usize,
        127 => stream.read_u64().await.unwrap() as usize,
        n => n as usize,
    };
    assert!(length < 1 << 20);
    let mut payload = vec![0; length];
    stream.read_exact(&mut payload).await.unwrap();
    (first & 15, payload)
}
async fn finish_close(stream: &mut tokio::net::TcpStream) {
    // Actual masked client close acknowledgment, then require server EOF.
    let _ = stream
        .write_all(&[0x88, 0x82, 0, 0, 0, 0, 0x03, 0xe8])
        .await;
    let mut rest = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), stream.read_to_end(&mut rest))
        .await
        .unwrap()
        .unwrap();
    assert!(
        rest.is_empty(),
        "no extra application response after normal closure"
    );
}
#[tokio::test]
async fn actual_zero_attempt_public_ws_is_binary_empty_then_normal_close_without_room_and_stays_healthy()
 {
    let (old, net) = crate::test_support::state().await;
    drop(old);
    let mut state = crate::state::CompatServices::new(Arc::new(crate::test_support::config()))
        .with_signal_connect_attempts(0)
        .assemble(net.clone(), Arc::new(rtc_core::events::NullSink));
    // Real health handler needs the same fresh node stamp installed by server
    // startup. With no node it deliberately answers406, not200.
    state.enable_node(Arc::new(
        crate::routing::LocalNode::serving(
            "ND_zero_attempt_test".into(),
            "127.0.0.1".into(),
            String::new(),
            1,
            rtc_core::util::unix_s(),
        )
        .unwrap(),
    ));
    assert_eq!(
        state.signal_connect_attempts, 0,
        "assembly must retain disabled attempts"
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = crate::http::router(state.clone());
    let (shutdown, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(async {
            let _ = stopped.await;
        })
        .await
        .unwrap();
    });
    let token=crate::auth::jwt::tests::make_token(r#"{"alg":"HS256"}"#,&serde_json::json!({"iss":"devkey","sub":"zero-attempt","exp":99999999999_u64,"video":{"roomJoin":true,"room":"zero-attempt"}}).to_string(),"secret");
    // Repeat the entire public request: a previous empty-start path cannot
    // panic the HTTP service or retain a session/owner between connections.
    for _ in 0..2 {
        let mut stream = tokio::time::timeout(Duration::from_secs(3), upgrade(address, &token))
            .await
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), read_frame(&mut stream))
                .await
                .unwrap(),
            (2, Vec::new())
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), read_frame(&mut stream))
                .await
                .unwrap(),
            (8, vec![0x03, 0xe8])
        );
        finish_close(&mut stream).await;
        assert!(
            !state.manager.room_exists(&state.room_key("zero-attempt")),
            "no owner/broker join or shadow room"
        );
    }
    let mut health = tokio::net::TcpStream::connect(address).await.unwrap();
    health
        .write_all(
            format!("GET / HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n").as_bytes(),
        )
        .await
        .unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), health.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert!(
        response.starts_with(b"HTTP/1.1 200 "),
        "{}",
        String::from_utf8_lossy(&response)
    );
    assert!(response.ends_with(b"OK"));
    let _ = shutdown.send(());
    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
    state.manager.close_all().await;
    state.shutdown_workers().await;
    net.shutdown_and_wait().await;
}
