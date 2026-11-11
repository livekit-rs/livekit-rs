//! Real wire tests for source mux registration and captured owner lifetimes.
use super::*;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
fn config(tcp: bool) -> NetConfig {
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
fn binding(user: &str) -> Vec<u8> {
    let padded = (user.len() + 3) & !3;
    let mut b = vec![0; 24 + padded];
    b[..2].copy_from_slice(&1u16.to_be_bytes());
    b[2..4].copy_from_slice(&((4 + padded) as u16).to_be_bytes());
    b[4..8].copy_from_slice(&[0x21, 0x12, 0xa4, 0x42]);
    b[8..20].copy_from_slice(&[7; 12]);
    b[20..22].copy_from_slice(&6u16.to_be_bytes());
    b[22..24].copy_from_slice(&(user.len() as u16).to_be_bytes());
    b[24..24 + user.len()].copy_from_slice(user.as_bytes());
    b
}
async fn received(rx: &mut mpsc::Receiver<InboundPacket>) -> InboundPacket {
    tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap()
}
async fn absent(rx: &mut mpsc::Receiver<InboundPacket>) {
    assert!(
        tokio::time::timeout(Duration::from_millis(100), rx.recv())
            .await
            .is_err(),
        "source mux must not deliver an unregistered remote"
    );
}
async fn ready(net: &NetRouter, base: SocketAddr) {
    let socket = read(&net.inner.udp_sockets).get(&base).cloned().unwrap();
    socket.writable().await.unwrap();
}
async fn reply(peer: &UdpSocket, body: &[u8], source: SocketAddr) {
    let mut bytes = [0; 128];
    let (n, addr) = tokio::time::timeout(Duration::from_secs(2), peer.recv_from(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(addr, source);
    assert_eq!(&bytes[..n], body);
}
#[tokio::test]
async fn only_actual_owner_write_registers_remote_and_cached_owner_precedes_stun_username() {
    let net = NetRouter::start(config(false)).await.unwrap();
    let (a, b) = (TransportId::next(), TransportId::next());
    let (ta, mut ra) = mpsc::channel(8);
    let (tb, mut rb) = mpsc::channel(8);
    let base = net.open_transport(a, "owner-a", ta).unwrap()[0].base;
    net.open_transport(b, "owner-b", tb).unwrap();
    let sender_a = net.sender().for_transport(a).unwrap();
    let sender_b = net.sender().for_transport(b).unwrap();
    let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    peer.send_to(&binding("owner-a:peer"), base).await.unwrap();
    assert_eq!(received(&mut ra).await.transport, a);
    assert!(
        read(&net.inner.by_remote).is_empty(),
        "incoming STUN never registers an address"
    );
    peer.send_to(b"before-any-owner-write", base).await.unwrap();
    absent(&mut ra).await;
    absent(&mut rb).await;
    ready(&net, base).await;
    sender_a.send(
        NetProto::Udp,
        base,
        peer.local_addr().unwrap(),
        b"actual-a-write",
    );
    reply(&peer, b"actual-a-write", base).await;
    peer.send_to(&binding("owner-b:peer"), base).await.unwrap();
    assert_eq!(
        received(&mut ra).await.transport,
        a,
        "cached source association wins even over another valid username"
    );
    absent(&mut rb).await;
    ready(&net, base).await;
    sender_b.send(
        NetProto::Udp,
        base,
        peer.local_addr().unwrap(),
        b"actual-b-write",
    );
    reply(&peer, b"actual-b-write", base).await;
    peer.send_to(b"after-b-write", base).await.unwrap();
    assert_eq!(received(&mut rb).await.transport, b);
    absent(&mut ra).await;
    net.close_transport(a);
    peer.send_to(b"old-a-close-cannot-remove-b", base)
        .await
        .unwrap();
    assert_eq!(received(&mut rb).await.transport, b);
    net.close_transport(b);
    net.shutdown_and_wait().await;
}
#[tokio::test]
async fn stale_captured_sender_cannot_register_or_send_for_replacement_generation() {
    let net = NetRouter::start(config(false)).await.unwrap();
    let id = TransportId::next();
    let (tx, _) = mpsc::channel(8);
    let base = net.open_transport(id, "old", tx).unwrap()[0].base;
    let old = net.sender().for_transport(id).unwrap();
    net.close_transport(id);
    let (tx, mut rx) = mpsc::channel(8);
    net.open_transport(id, "new", tx).unwrap();
    let fresh = net.sender().for_transport(id).unwrap();
    let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    old.send(NetProto::Udp, base, peer.local_addr().unwrap(), b"stale");
    let mut buf = [0; 64];
    assert!(
        tokio::time::timeout(Duration::from_millis(100), peer.recv_from(&mut buf))
            .await
            .is_err()
    );
    assert!(read(&net.inner.by_remote).is_empty());
    peer.send_to(b"still-unregistered", base).await.unwrap();
    absent(&mut rx).await;
    ready(&net, base).await;
    fresh.send(NetProto::Udp, base, peer.local_addr().unwrap(), b"fresh");
    reply(&peer, b"fresh", base).await;
    peer.send_to(b"now-registered", base).await.unwrap();
    assert_eq!(received(&mut rx).await.transport, id);
    net.close_transport(id);
    net.shutdown_and_wait().await;
}
async fn frame(stream: &mut tokio::net::TcpStream, body: &[u8]) {
    stream
        .write_all(&(body.len() as u16).to_be_bytes())
        .await
        .unwrap();
    stream.write_all(body).await.unwrap();
}
#[tokio::test]
async fn closing_one_transport_collects_real_tcp_writer_and_never_delivers_old_frames() {
    let net = NetRouter::start(config(true)).await.unwrap();
    let id = TransportId::next();
    let (tx, mut rx) = mpsc::channel(8);
    let base = net.open_transport(id, "tcp-old", tx).unwrap()[0].base;
    let old = net.sender().for_transport(id).unwrap();
    let mut peer = tokio::net::TcpStream::connect(base).await.unwrap();
    frame(&mut peer, &binding("tcp-old:peer")).await;
    let packet = received(&mut rx).await;
    assert_eq!(packet.transport, id);
    old.send(NetProto::Tcp, base, packet.source, b"actual-reply");
    let mut header = [0; 2];
    peer.read_exact(&mut header).await.unwrap();
    let mut bytes = vec![0; u16::from_be_bytes(header) as usize];
    peer.read_exact(&mut bytes).await.unwrap();
    assert_eq!(bytes, b"actual-reply");
    tokio::time::timeout(Duration::from_secs(2), net.close_transport_and_wait(id))
        .await
        .unwrap();
    assert!(
        lock(&net.inner.tcp_tasks)
            .values()
            .all(|task| task.owner.is_none())
    );
    assert!(read(&net.inner.tcp_conns).is_empty());
    let mut byte = [0];
    let result = tokio::time::timeout(Duration::from_secs(2), peer.read(&mut byte))
        .await
        .unwrap();
    assert!(
        matches!(result, Ok(0) | Err(_)),
        "actual old connection must close, not merely unmap"
    );
    let (tx, mut new_rx) = mpsc::channel(8);
    net.open_transport(id, "tcp-new", tx).unwrap();
    old.send(NetProto::Tcp, base, packet.source, b"old-writer");
    let _ = peer.write_all(b"\0\x03old").await;
    absent(&mut new_rx).await;
    let mut fresh = tokio::net::TcpStream::connect(base).await.unwrap();
    frame(&mut fresh, &binding("tcp-new:peer")).await;
    assert_eq!(received(&mut new_rx).await.transport, id);
    net.close_transport_and_wait(id).await;
    net.shutdown_and_wait().await;
}
#[tokio::test]
async fn global_shutdown_collects_idle_first_frame_tcp_peers_without_an_owner() {
    let net = NetRouter::start(config(true)).await.unwrap();
    let (tx, _) = mpsc::channel(1);
    let addr = net.open_transport(TransportId::next(), "idle", tx).unwrap()[0].base;
    let mut peer = tokio::net::TcpStream::connect(addr).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        // The listener is tracked before the accepted idle peer is added.
        while lock(&net.inner.tcp_tasks).len() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(2), net.shutdown_and_wait())
        .await
        .unwrap();
    assert!(lock(&net.inner.tcp_tasks).is_empty());
    let mut byte = [0];
    assert!(matches!(peer.read(&mut byte).await, Ok(0) | Err(_)));
}

#[tokio::test]
async fn concurrent_tcp_owner_collection_keeps_unrelated_owner_live() {
    let net = NetRouter::start(config(true)).await.unwrap();
    let (a, b) = (TransportId::next(), TransportId::next());
    let (ta, mut ra) = mpsc::channel(8);
    let (tb, mut rb) = mpsc::channel(8);
    let base = net.open_transport(a, "tcp-a", ta).unwrap()[0].base;
    net.open_transport(b, "tcp-b", tb).unwrap();
    let mut pa = tokio::net::TcpStream::connect(base).await.unwrap();
    let mut pb = tokio::net::TcpStream::connect(base).await.unwrap();
    frame(&mut pa, &binding("tcp-a:peer")).await;
    frame(&mut pb, &binding("tcp-b:peer")).await;
    assert_eq!(received(&mut ra).await.transport, a);
    assert_eq!(received(&mut rb).await.transport, b);
    let (left, right) = (net.clone(), net.clone());
    tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(
            left.close_transport_and_wait(a),
            right.close_transport_and_wait(a)
        );
    })
    .await
    .unwrap();
    assert!(
        lock(&net.inner.tcp_tasks)
            .values()
            .all(|task| task.owner.as_ref().is_none_or(|(id, _)| *id != a))
    );
    frame(&mut pb, b"unrelated-b-still-live").await;
    assert_eq!(received(&mut rb).await.data, b"unrelated-b-still-live");
    let mut byte = [0];
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), pa.read(&mut byte))
            .await
            .unwrap(),
        Ok(0) | Err(_)
    ));
    net.close_transport_and_wait(b).await;
    net.shutdown_and_wait().await;
}

#[tokio::test]
async fn wildcard_tcp_without_rewrite_advertises_actual_hosts_and_accepts_real_frames() {
    let mut cfg = config(true);
    cfg.interfaces[0].bind = "0.0.0.0".parse().unwrap();
    cfg.interfaces[0].advertise = "0.0.0.0".parse().unwrap();
    let net = NetRouter::start(cfg).await.unwrap();
    let (tx, mut rx) = mpsc::channel(8);
    let id = TransportId::next();
    let candidates = net.open_transport(id, "wild-no-nat", tx).unwrap();
    assert!(!candidates.is_empty(), "existing UP concrete IPv4 required");
    assert!(
        candidates
            .iter()
            .all(|c| c.addr == c.base && !c.addr.ip().is_unspecified())
    );
    let c = candidates
        .iter()
        .find(|c| c.base.ip().is_loopback())
        .expect("existing loopback must be represented");
    let mut peer = tokio::net::TcpStream::connect(c.base).await.unwrap();
    frame(&mut peer, &binding("wild-no-nat:peer")).await;
    let packet = received(&mut rx).await;
    assert_eq!(packet.bound, c.base);
    assert_eq!(packet.destination, c.addr);
    net.close_transport_and_wait(id).await;
    net.shutdown_and_wait().await;
}
