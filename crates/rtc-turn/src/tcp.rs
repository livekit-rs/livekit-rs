//! TURN over TCP / TLS (RFC 8656 §3.1): STUN and ChannelData framed on a
//! byte stream. One task per connection; the allocation made on a
//! connection is bound to it and deleted when it closes.

use std::future::Future;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;

use crate::allocation::{ClientAddr, FiveTuple, Transport};
use crate::proxy;
use crate::server::{ServerCtx, SinkSource};
use crate::stun;

/// Optional TLS for TCP listeners; the crate must not depend on rustls. Implemented by the binary.
pub type AcceptFuture = Pin<Box<dyn Future<Output = io::Result<Box<dyn AsyncStream>>> + Send>>;

pub trait StreamAcceptor: Send + Sync + 'static {
    fn accept(&self, stream: TcpStream) -> AcceptFuture;
}

/// A bidirectional byte stream (plain TCP or TLS-wrapped).
pub trait AsyncStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncStream for T {}

/// Bounded queue of peer data waiting to be written to the client.
const OUT_QUEUE: usize = 256;
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// A trusted proxy must send its PROXY header within this window.
const PROXY_HEADER_TIMEOUT: Duration = Duration::from_secs(10);
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);
/// A connection without an allocation is closed after this much inactivity.
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const READ_CHUNK: usize = 4096;
static NEXT_UNIX_CONNECTION: AtomicU64 = AtomicU64::new(1);

pub(crate) async fn tcp_listener(
    ctx: Arc<ServerCtx>,
    listener: TcpListener,
    tls: Option<Arc<dyn StreamAcceptor>>,
    proxy_trusted: Option<Vec<(std::net::IpAddr, u8)>>,
    mut shutdown: watch::Receiver<bool>,
    max_connections: usize,
) {
    let local = match listener.local_addr() {
        Ok(a) => a,
        Err(e) => {
            tracing::error!(error = %e, "TURN TCP listener has no local address");
            return;
        }
    };
    let mut conns = JoinSet::new();
    let proxy_trusted = proxy_trusted.map(Arc::new);
    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            Some(_) = conns.join_next(), if !conns.is_empty() => {}
            r = listener.accept() => match r {
                Ok((stream, peer)) => {
                    if conns.len() >= max_connections {
                        tracing::warn!(%peer, "TURN TCP connection limit reached, dropping connection");
                        drop(stream);
                        continue;
                    }
                    let server = stream.local_addr().unwrap_or(local);
                    conns.spawn(connection(
                        ctx.clone(),
                        stream,
                        peer,
                        server,
                        local.ip(),
                        tls.clone(),
                        proxy_trusted.clone(),
                        shutdown.clone(),
                    ));
                }
                Err(e) => {
                    // e.g. EMFILE: back off briefly instead of spinning.
                    tracing::warn!(error = %e, "TURN TCP accept failed");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }
    conns.shutdown().await;
}

async fn write_frame(stream: &mut Box<dyn AsyncStream>, data: &[u8]) -> io::Result<()> {
    let fut = async {
        stream.write_all(data).await?;
        stream.flush().await
    };
    match tokio::time::timeout(WRITE_TIMEOUT, fut).await {
        Ok(r) => r,
        Err(_) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "TURN TCP write timed out",
        )),
    }
}

async fn connection(
    ctx: Arc<ServerCtx>,
    mut stream: TcpStream,
    peer: SocketAddr,
    server: SocketAddr,
    listener_ip: IpAddr,
    tls: Option<Arc<dyn StreamAcceptor>>,
    proxy_trusted: Option<Arc<Vec<(std::net::IpAddr, u8)>>>,
    mut shutdown: watch::Receiver<bool>,
) {
    let _ = stream.set_nodelay(true);
    // PROXY protocol is read on the raw TCP stream before TLS, so the real
    // client address reaches both the TLS layer and TURN (turn.go:219-276).
    let client = if let Some(trusted) = &proxy_trusted {
        if !proxy::is_trusted_proxy(trusted, peer.ip()) {
            // Untrusted peer: close without reading, so a direct client cannot
            // forge a source address (`proxyProtocolPolicy` REJECT).
            tracing::debug!(%peer, "TURN PROXY connection from untrusted address, closing");
            return;
        }
        let parsed =
            tokio::time::timeout(PROXY_HEADER_TIMEOUT, proxy::read_header(&mut stream)).await;
        match parsed {
            Ok(Ok(proxy::ProxySource::Inet(addr))) => ClientAddr::Inet(addr),
            Ok(Ok(proxy::ProxySource::Direct)) => ClientAddr::Inet(peer),
            Ok(Ok(proxy::ProxySource::Unix(address))) => ClientAddr::Unix {
                connection_id: NEXT_UNIX_CONNECTION.fetch_add(1, Ordering::Relaxed),
                address: Arc::new(address),
            },
            Ok(Err(e)) => {
                tracing::debug!(error = %e, %peer, "TURN PROXY header invalid, closing");
                return;
            }
            Err(_) => {
                tracing::debug!(%peer, "TURN PROXY header timed out, closing");
                return;
            }
        }
    } else {
        ClientAddr::Inet(peer)
    };
    let mut stream: Box<dyn AsyncStream> = match tls {
        Some(acceptor) => {
            match tokio::time::timeout(TLS_HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await {
                Ok(Ok(s)) => s,
                Ok(Err(e)) => {
                    tracing::debug!(error = %e, %client, "TURN TLS handshake failed");
                    return;
                }
                Err(_) => {
                    tracing::debug!(%client, "TURN TLS handshake timed out");
                    return;
                }
            }
        }
        None => Box::new(stream),
    };

    let ft = FiveTuple {
        client,
        server,
        transport: Transport::Tcp,
    };
    let (tx, mut rx) = mpsc::channel::<Bytes>(OUT_QUEUE);
    let mut buf = BytesMut::with_capacity(READ_CHUNK);
    let idle = tokio::time::sleep(IDLE_TIMEOUT);
    tokio::pin!(idle);

    'conn: loop {
        tokio::select! {
            _ = shutdown.changed() => break 'conn,
            r = stream.read_buf(&mut buf) => {
                match r {
                    Ok(0) => break 'conn,
                    Ok(_) => {}
                    Err(e) => {
                        tracing::debug!(error = %e, client = %ft.client, "TURN TCP read error");
                        break 'conn;
                    }
                }
                idle.as_mut().reset(tokio::time::Instant::now() + IDLE_TIMEOUT);
                loop {
                    let len = match stun::stream_frame_len(&buf) {
                        Ok(Some(len)) => len,
                        Ok(None) => break,
                        Err(e) => {
                            tracing::debug!(error = %e, client = %ft.client, "TURN TCP framing error, closing");
                            break 'conn;
                        }
                    };
                    if buf.len() < len {
                        buf.reserve(len - buf.len());
                        break;
                    }
                    let frame = buf.split_to(len);
                    if let Some(resp) = ctx.handle_packet(&frame, &ft, SinkSource::Tcp(&tx, listener_ip))
                        && let Err(e) = write_frame(&mut stream, &resp).await {
                            tracing::debug!(error = %e, client = %ft.client, "TURN TCP write error");
                            break 'conn;
                        }
                }
                if buf.capacity() - buf.len() < READ_CHUNK / 4 {
                    buf.reserve(READ_CHUNK);
                }
            }
            Some(out) = rx.recv() => {
                if let Err(e) = write_frame(&mut stream, &out).await {
                    tracing::debug!(error = %e, client = %ft.client, "TURN TCP write error");
                    break 'conn;
                }
            }
            () = &mut idle => {
                if ctx.allocs.contains(&ft) {
                    idle.as_mut().reset(tokio::time::Instant::now() + IDLE_TIMEOUT);
                } else {
                    tracing::debug!(client = %ft.client, "TURN TCP connection idle, closing");
                    break 'conn;
                }
            }
        }
    }

    ctx.connection_closed(&ft);
    let _ = stream.shutdown().await;
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use super::*;
    use crate::auth::{AuthHandler, AuthRequest, long_term_key};
    use crate::server::{ListenerSpec, TurnServer, TurnServerConfig};
    use crate::stun::{Class, Message, MessageBuilder, Method, attr};

    struct Auth;

    impl AuthHandler for Auth {
        fn authenticate(&self, req: &AuthRequest<'_>) -> Option<(String, [u8; 16])> {
            Some((
                req.username.to_string(),
                long_term_key(req.username, req.realm, "pw"),
            ))
        }
    }

    async fn read_frame(s: &mut TcpStream) -> Vec<u8> {
        let mut hdr = [0u8; 4];
        s.read_exact(&mut hdr).await.unwrap();
        let len = stun::stream_frame_len(&hdr).unwrap().unwrap();
        let mut out = hdr.to_vec();
        out.resize(len, 0);
        s.read_exact(&mut out[4..]).await.unwrap();
        out
    }

    #[tokio::test]
    async fn tcp_allocation_removed_on_close() {
        let cfg = TurnServerConfig {
            listeners: vec![ListenerSpec::Tcp("127.0.0.1:0".parse().unwrap())],
            relay_ipv4: Some(Ipv4Addr::LOCALHOST),
            relay_bind_ipv4: IpAddr::V4(Ipv4Addr::LOCALHOST),
            relay_port_range: (0, 0),
            ..TurnServerConfig::default()
        };
        let h = TurnServer::start(cfg, Arc::new(Auth)).await.unwrap();
        let mut s = TcpStream::connect(h.local_addrs()[0]).await.unwrap();

        // Two STUN messages written in one segment are framed correctly.
        let mut bind1 =
            MessageBuilder::new(Method::Binding, Class::Request, [1u8; 12]).build(None, true);
        let bind2 =
            MessageBuilder::new(Method::Binding, Class::Request, [2u8; 12]).build(None, true);
        bind1.extend_from_slice(&bind2);
        s.write_all(&bind1).await.unwrap();
        let r1 = Message::decode(&read_frame(&mut s).await).unwrap();
        let r2 = Message::decode(&read_frame(&mut s).await).unwrap();
        assert_eq!(r1.transaction_id(), &[1u8; 12]);
        assert_eq!(r2.transaction_id(), &[2u8; 12]);

        // Challenge then authenticated allocate.
        let mut b = MessageBuilder::new(Method::Allocate, Class::Request, [3u8; 12]);
        b.add(
            attr::REQUESTED_TRANSPORT,
            &attr::encode_requested_transport(attr::PROTO_UDP),
        );
        s.write_all(&b.build(None, true)).await.unwrap();
        let chal = Message::decode(&read_frame(&mut s).await).unwrap();
        assert_eq!(chal.error_code().unwrap().unwrap().0, 401);
        let nonce = chal.nonce().unwrap().to_vec();

        let key = long_term_key("bob", "livekit", "pw");
        let mut b = MessageBuilder::new(Method::Allocate, Class::Request, [4u8; 12]);
        b.add(
            attr::REQUESTED_TRANSPORT,
            &attr::encode_requested_transport(attr::PROTO_UDP),
        )
        .add(attr::USERNAME, b"bob")
        .add(attr::REALM, b"livekit")
        .add(attr::NONCE, &nonce);
        s.write_all(&b.build(Some(&key), true)).await.unwrap();
        let ok = Message::decode(&read_frame(&mut s).await).unwrap();
        assert_eq!(ok.class(), Class::Success, "{:?}", ok.error_code());
        assert_eq!(h.allocation_count(), 1);

        drop(s);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while h.allocation_count() != 0 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "allocation not removed on close"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        h.shutdown().await;
    }

    /// Both TCP regressions above bind 127.0.0.1 through `TurnServer::start`,
    /// so the embedded off-loopback accept/read path had no coverage. This is
    /// the geometry of the external-TLS TCP profile: listener and client both
    /// on an assigned non-loopback IPv4, relay bind taken from the listener.
    #[tokio::test]
    #[ignore = "requires an assigned non-loopback IPv4 in TURN_TEST_NONLOOPBACK_V4"]
    async fn embedded_tcp_allocate_over_non_loopback_ipv4() {
        let addr: Ipv4Addr = std::env::var("TURN_TEST_NONLOOPBACK_V4")
            .expect("TURN_TEST_NONLOOPBACK_V4 must name an assigned non-loopback IPv4")
            .parse()
            .expect("TURN_TEST_NONLOOPBACK_V4 must be an IPv4 address");
        assert!(
            !addr.is_loopback() && !addr.is_unspecified(),
            "need a real assigned address, got {addr}"
        );
        let ip = IpAddr::V4(addr);
        let cfg = TurnServerConfig {
            listeners: vec![ListenerSpec::Tcp(SocketAddr::new(ip, 0))],
            relay_ipv4: Some(Ipv4Addr::LOCALHOST),
            relay_bind_ipv4: ip,
            relay_port_range: (0, 0),
            ..TurnServerConfig::default()
        };
        let h = TurnServer::start_embedded(
            cfg,
            Arc::new(Auth),
            crate::policy::LocalRelayPolicy::new([ip], Vec::new()),
        )
        .await
        .unwrap();
        let server = h.local_addrs()[0];
        assert_eq!(server.ip(), ip);

        // Bind the client to the same address so the accepted peer is not
        // loopback either; an unbound dial would source from 127.0.0.1.
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.bind(SocketAddr::new(ip, 0)).unwrap();
        let mut s = socket.connect(server).await.unwrap();
        assert_eq!(s.local_addr().unwrap().ip(), ip);

        let mut b = MessageBuilder::new(Method::Allocate, Class::Request, [7u8; 12]);
        b.add(
            attr::REQUESTED_TRANSPORT,
            &attr::encode_requested_transport(attr::PROTO_UDP),
        );
        s.write_all(&b.build(None, true)).await.unwrap();
        let chal = Message::decode(&read_frame(&mut s).await).unwrap();
        assert_eq!(chal.error_code().unwrap().unwrap().0, 401);
        let nonce = chal.nonce().unwrap().to_vec();

        let key = long_term_key("bob", "livekit", "pw");
        let mut b = MessageBuilder::new(Method::Allocate, Class::Request, [8u8; 12]);
        b.add(
            attr::REQUESTED_TRANSPORT,
            &attr::encode_requested_transport(attr::PROTO_UDP),
        )
        .add(attr::USERNAME, b"bob")
        .add(attr::REALM, b"livekit")
        .add(attr::NONCE, &nonce);
        s.write_all(&b.build(Some(&key), true)).await.unwrap();
        let ok = Message::decode(&read_frame(&mut s).await).unwrap();
        assert_eq!(ok.class(), Class::Success, "{:?}", ok.error_code());
        assert_eq!(h.allocation_count(), 1);
        let relayed = ok
            .xor_address(attr::XOR_RELAYED_ADDRESS)
            .expect("relayed address")
            .unwrap();
        assert!(relayed.is_ipv4(), "expected an IPv4 relay, got {relayed}");

        drop(s);
        h.shutdown().await;
    }

    fn proxy_cfg(trusted: Vec<(std::net::IpAddr, u8)>) -> TurnServerConfig {
        TurnServerConfig {
            listeners: vec![ListenerSpec::Tcp("127.0.0.1:0".parse().unwrap())],
            relay_ipv4: Some(Ipv4Addr::LOCALHOST),
            relay_bind_ipv4: IpAddr::V4(Ipv4Addr::LOCALHOST),
            relay_port_range: (0, 0),
            proxy_trusted_cidrs: Some(trusted),
            ..TurnServerConfig::default()
        }
    }

    #[tokio::test]
    async fn proxy_trusted_header_is_parsed_before_tls() {
        // Loopback is a trusted proxy, so a valid PROXY header is consumed and
        // framing resumes on the same stream (a Binding then succeeds).
        let trusted = vec![(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 0)), 8)];
        let h = TurnServer::start(proxy_cfg(trusted), Arc::new(Auth))
            .await
            .unwrap();
        let mut s = TcpStream::connect(h.local_addrs()[0]).await.unwrap();
        s.write_all(b"PROXY TCP4 203.0.113.5 198.51.100.1 5000 443\r\n")
            .await
            .unwrap();
        let bind =
            MessageBuilder::new(Method::Binding, Class::Request, [9u8; 12]).build(None, true);
        s.write_all(&bind).await.unwrap();
        let resp = Message::decode(&read_frame(&mut s).await).unwrap();
        assert_eq!(resp.class(), Class::Success);
        h.shutdown().await;
    }

    #[tokio::test]
    async fn proxy_untrusted_peer_is_closed_without_header() {
        // A direct client outside the trusted ranges is rejected before any
        // header is read, so it cannot forge a source address.
        let trusted = vec![(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 0)), 8)];
        let h = TurnServer::start(proxy_cfg(trusted), Arc::new(Auth))
            .await
            .unwrap();
        let mut s = TcpStream::connect(h.local_addrs()[0]).await.unwrap();
        let mut buf = [0u8; 1];
        assert_eq!(
            s.read(&mut buf).await.unwrap(),
            0,
            "untrusted proxy must be closed"
        );
        h.shutdown().await;
    }

    #[tokio::test]
    async fn proxy_trusted_peer_must_send_valid_header() {
        // A trusted peer that does not begin with a PROXY header is closed.
        let trusted = vec![(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 0)), 8)];
        let h = TurnServer::start(proxy_cfg(trusted), Arc::new(Auth))
            .await
            .unwrap();
        let mut s = TcpStream::connect(h.local_addrs()[0]).await.unwrap();
        s.write_all(&[0x16, 0x03, 0x01, 0x00, 0x05]).await.unwrap();
        let mut buf = [0u8; 1];
        // The server closes after rejecting the header; because unread bytes
        // remain, the socket is reset rather than cleanly closed. Either way no
        // STUN response is served.
        match s.read(&mut buf).await {
            Ok(0) => {}
            Ok(n) => panic!("expected close, read {n} bytes"),
            Err(_) => {}
        }
        h.shutdown().await;
    }
}
