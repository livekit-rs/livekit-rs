//! Actual HTTP boundary controls; sampled counters here are explicitly injected.
//! The separate official-SDK campaign validates the production RTP sampler.
use super::*;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug)]
struct Sample(Arc<AtomicBool>);
impl crate::routing::NodeStatsProvider for Sample {
    fn poll_system(&self) -> Result<(), String> {
        Ok(())
    }
    fn sample(&self, started_at: i64, updated_at: i64) -> Result<pb::NodeStats, String> {
        Ok(pb::NodeStats {
            started_at,
            updated_at,
            num_tracks_in: i32::from(self.0.load(Ordering::SeqCst)),
            ..Default::default()
        })
    }
}
async fn request(address: SocketAddr, token: &str, path: &str) -> String {
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream.write_all(format!("GET {path}?protocol=17&access_token={token} HTTP/1.1\r\nHost: {address}\r\nConnection: Upgrade, close\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n").as_bytes()).await.unwrap();
    let mut bytes = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), stream.read_to_end(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    String::from_utf8(bytes).unwrap()
}
#[tokio::test]
async fn actual_rtc_validation_and_upgrade_use_503_before_allocator_and_recover() {
    let (mut state, net) = crate::test_support::state().await;
    let full = Arc::new(AtomicBool::new(true));
    let now = rtc_core::util::unix_s();
    let node = Arc::new(
        crate::routing::LocalNode::serving(
            "ND_rtc-capacity".into(),
            "127.0.0.1".into(),
            String::new(),
            1,
            now,
        )
        .unwrap(),
    );
    node.set_stats_provider(Arc::new(Sample(full.clone())));
    assert!(node.update_stats(now));
    state.enable_node(node.clone());
    state
        .enable_local_allocation_selector(crate::room_control::selector::Config {
            kind: "any".into(),
            sort_by: "random".into(),
            algorithm: "random".into(),
            cpu_limit: 0.,
            sysload_limit: 0.,
            region: String::new(),
            regions: vec![],
            track_limit: 1,
            bytes_limit: 0.,
        })
        .unwrap();
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
    let token=crate::auth::jwt::tests::make_token(r#"{"alg":"HS256"}"#,&serde_json::json!({"iss":"devkey","sub":"rtc-capacity","exp":99999999999u64,"video":{"roomJoin":true,"room":"rtc-capacity"}}).to_string(),"secret");
    for path in ["/rtc/validate", "/rtc"] {
        let response = request(address, &token, path).await;
        assert!(response.starts_with("HTTP/1.1 503 "), "{path}: {response}");
        assert!(
            response.ends_with("node has exceeded its configured limit"),
            "{response}"
        );
        assert!(
            !state.manager.room_exists(&state.room_key("rtc-capacity")),
            "reject before room/participant side effects"
        );
    }
    // Fixed utils.go does not require availability here: an old node record
    // with capacity still rejects; selection fallback belongs to allocator.
    assert!(node.update_stats(now - 10));
    assert!(
        request(address, &token, "/rtc/validate")
            .await
            .starts_with("HTTP/1.1 503 ")
    );
    full.store(false, Ordering::SeqCst);
    assert!(node.update_stats(now));
    let response = request(address, &token, "/rtc/validate").await;
    assert!(response.starts_with("HTTP/1.1 200 "), "{response}");
    assert!(response.ends_with("success"), "{response}");
    let _ = shutdown.send(());
    server.await.unwrap();
    state.manager.close_all().await;
    net.shutdown_and_wait().await;
}
