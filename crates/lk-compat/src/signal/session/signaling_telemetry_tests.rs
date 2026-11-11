use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn connect(address: std::net::SocketAddr, token: &str) -> tokio::net::TcpStream {
    let mut peer = tokio::net::TcpStream::connect(address).await.unwrap();
    peer.write_all(format!("GET /rtc?protocol=15&access_token={token} HTTP/1.1\r\nHost: {address}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n").as_bytes()).await.unwrap();
    let mut header = Vec::new();
    while !header.ends_with(b"\r\n\r\n") {
        assert!(header.len() < 8192);
        header.push(peer.read_u8().await.unwrap());
    }
    assert!(header.starts_with(b"HTTP/1.1 101 "), "{header:?}");
    peer
}
async fn write(peer: &mut tokio::net::TcpStream, opcode: u8, payload: &[u8]) {
    assert!(payload.len() < 126);
    let mask = [1, 2, 3, 4];
    let mut frame = vec![0x80 | opcode, 0x80 | payload.len() as u8];
    frame.extend(mask);
    frame.extend(payload.iter().enumerate().map(|(i, b)| *b ^ mask[i % 4]));
    peer.write_all(&frame).await.unwrap();
}
async fn read(peer: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
    let first = peer.read_u8().await.unwrap();
    let second = peer.read_u8().await.unwrap();
    assert_eq!(second & 0x80, 0);
    let length = match second & 0x7f {
        126 => peer.read_u16().await.unwrap() as usize,
        127 => peer.read_u64().await.unwrap().try_into().unwrap(),
        value => value as usize,
    };
    assert!(length < 1 << 20);
    let mut payload = vec![0; length];
    peer.read_exact(&mut payload).await.unwrap();
    (first & 15, payload)
}

#[tokio::test]
async fn actual_websocket_application_counts_survive_room_shutdown_and_recreation() {
    tokio::time::timeout(Duration::from_secs(12), async {
        let (state, net) = crate::test_support::state().await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let router = crate::http::router(state.clone());
        let (shutdown, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            axum::serve(listener, router.into_make_service_with_connect_info::<std::net::SocketAddr>())
                .with_graceful_shutdown(async { let _ = stopped.await; }).await.unwrap();
        });
        let token = crate::auth::jwt::tests::make_token(
            r#"{"alg":"HS256"}"#,
            &serde_json::json!({"iss":"devkey","sub":"signal-telemetry","exp":99999999999u64,
                "video":{"roomJoin":true,"room":"signal-telemetry","canPublish":true,"canSubscribe":true}}).to_string(), "secret");
        let mut in_bytes = 0u64;
        let mut in_packets = 0u64;
        let mut out_bytes = 0u64;
        let mut out_packets = 0u64;
        for generation in 0..2 {
            let mut peer = connect(address, &token).await;
            let (opcode, initial) = read(&mut peer).await;
            assert_eq!(opcode, 2);
            assert!(matches!(decode_binary_request(&[]).unwrap().message, None));
            let initial_response = pb::SignalResponse::decode(initial.as_slice()).unwrap();
            assert!(matches!(initial_response.message, Some(Resp::Join(_))));
            out_bytes += initial.len() as u64;
            out_packets += 1;
            let binary = pb::SignalRequest { message: Some(pb::signal_request::Message::Ping(123)) }.encode_to_vec();
            let json = br#"{"pingReq":{"timestamp":"456","rtt":"7"}}"#;
            for (opcode, payload) in [(2, binary.as_slice()), (1, json.as_slice())] {
                write(&mut peer, opcode, payload).await;
                in_bytes += payload.len() as u64;
                in_packets += 1;
                loop {
                    let (kind, received) = read(&mut peer).await;
                    if kind == 1 || kind == 2 {
                        out_bytes += received.len() as u64;
                        out_packets += 1;
                        let response = if kind == 1 { lk_protocol::json::decode_frame::<pb::SignalResponse>(&received).unwrap() }
                            else { pb::SignalResponse::decode(received.as_slice()).unwrap() };
                        if matches!(response.message, Some(Resp::Pong(_) | Resp::PongResp(_))) { break; }
                    }
                }
            }
            // RFC6455 ping/pong and an undecodable protobuf message are not
            // accepted SignalRequests, even though both traverse real TCP.
            write(&mut peer, 9, b"control").await;
            write(&mut peer, 2, &[0xff]).await;
            loop {
                let (kind, payload) = read(&mut peer).await;
                if kind == 1 || kind == 2 { out_bytes += payload.len() as u64; out_packets += 1; }
                if kind == 8 { assert_eq!(&payload[..2], &1000u16.to_be_bytes()); break; }
            }
            drop(peer);
            let stats = state.manager.node_stats();
            assert_eq!((stats.bytes_in, stats.packets_in), (in_bytes, in_packets));
            assert_eq!((stats.bytes_out, stats.packets_out), (out_bytes, out_packets));
            state.manager.delete_room(&rtc_core::ids::RoomKey::new(
                rtc_core::ids::ProjectId::local(), "signal-telemetry",
            )).await.unwrap();
            let after = state.manager.node_stats();
            assert_eq!((after.bytes_in, after.packets_in), (in_bytes, in_packets));
            assert_eq!((after.bytes_out, after.packets_out), (out_bytes, out_packets));
            assert_eq!(after.rooms, 0, "real generation {generation} has terminated");
        }
        state.manager.close_all().await;
        let final_stats = state.manager.node_stats();
        assert_eq!((final_stats.bytes_in, final_stats.packets_in), (in_bytes, in_packets));
        assert_eq!((final_stats.bytes_out, final_stats.packets_out), (out_bytes, out_packets));
        let _ = shutdown.send(());
        server.await.unwrap();
        net.shutdown_and_wait().await;
    }).await.expect("bounded authenticated websocket lifecycle");
}
