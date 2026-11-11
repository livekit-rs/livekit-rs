//! Append under rtc-runtime net module for an actual old public-API RED.
use super::*;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
fn cfg(tcp: bool) -> NetConfig {
    NetConfig {
        udp_enabled: !tcp,
        udp_port: Some(0),
        port_range: (0, 0),
        tcp_port: tcp.then_some(0),
        interfaces: vec![LocalInterface {
            bind: "127.0.0.1".parse().unwrap(),
            advertise: "127.0.0.1".parse().unwrap(),
        }],
    }
}
fn stun() -> Vec<u8> {
    let mut b = vec![0, 1, 0, 12, 0x21, 0x12, 0xa4, 0x42];
    b.extend_from_slice(&[7; 12]);
    b.extend_from_slice(&[0, 6, 0, 7]);
    b.extend_from_slice(b"a:peerx");
    b.push(0);
    b
}
#[tokio::test]
async fn received_stun_without_any_write_must_not_register_nonstun_remote() {
    let net = NetRouter::start(cfg(false)).await.unwrap();
    let (tx, mut rx) = mpsc::channel(8);
    let c = net.open_transport(TransportId::next(), "a", tx).unwrap();
    let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    peer.send_to(&stun(), c[0].addr).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    peer.send_to(b"non-stun-before-server-write", c[0].addr)
        .await
        .unwrap();
    let got = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await;
    net.shutdown();
    assert!(
        got.is_err(),
        "fixed Pion only registers remote on actual WriteTo, never receive"
    );
}
#[tokio::test]
async fn transport_close_must_close_its_actual_tcp_connection_not_only_remove_maps() {
    let net = NetRouter::start(cfg(true)).await.unwrap();
    let (tx, mut rx) = mpsc::channel(8);
    let id = TransportId::next();
    let c = net.open_transport(id, "a", tx).unwrap();
    let mut peer = tokio::net::TcpStream::connect(c[0].addr).await.unwrap();
    let body = stun();
    peer.write_all(&(body.len() as u16).to_be_bytes())
        .await
        .unwrap();
    peer.write_all(&body).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    net.close_transport(id);
    let mut byte = [0];
    let closed = tokio::time::timeout(Duration::from_millis(300), peer.read(&mut byte)).await;
    net.shutdown();
    assert!(
        matches!(closed, Ok(Ok(0)) | Ok(Err(_))),
        "fixed TCPMux closes the retired owner's actual connection while room sink remains open"
    );
}
