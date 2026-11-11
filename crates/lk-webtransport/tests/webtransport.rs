//! End-to-end WebTransport acceptance over a *real* loopback UDP socket
//! (SPEC-0074 C15, h4): pure-Rust crypto -> Quinn async endpoint -> h3 ->
//! WebTransport session, driving the same wire interactions the Go server
//! expects from browsers/SDK clients.
//!
//! Proven in one run: a full QUIC/TLS 1.3 handshake with real chain
//! validation against the pinned RSA root, the Extended CONNECT upgrade,
//! client- and server-initiated bidirectional streams with raw payload
//! (`0x41` header routing), unidirectional streams in both directions
//! (`0x54` demux) and datagram round trips in both directions.

use std::{net::UdpSocket, time::Duration};

use bytes::Bytes;
use lk_webtransport::{
    Accepted, ServerTls, WtClient, WtServerConnection, client_config, client_endpoint, connect,
    endpoint, server_config, upgrade_connect,
};
use rtc_tls::rustls::{
    RootCertStore,
    pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
};

// Same long-validity public fixtures as the rtc-tls Quinn tests: RSA root ->
// intermediate -> P-256 `localhost` leaf.
const ROOT: &[u8] = include_bytes!("../../rtc-tls/tests/fixtures/expanded/root-rsa.pem");
const LEAF: &[u8] = include_bytes!("../../rtc-tls/tests/fixtures/expanded/p256.pem");
const INTERMEDIATE: &[u8] =
    include_bytes!("../../rtc-tls/tests/fixtures/expanded/intermediate.pem");
const LEAF_KEY: &[u8] = include_bytes!("../../rtc-tls/tests/fixtures/expanded/p256-key.pem");

fn bound_socket() -> (UdpSocket, std::net::SocketAddr) {
    let socket = UdpSocket::bind("127.0.0.1:0").expect("bind udp");
    let addr = socket.local_addr().expect("local addr");
    socket.set_nonblocking(true).expect("nonblocking for tokio");
    (socket, addr)
}

/// Server half: accept one QUIC connection, upgrade its CONNECT and run the
/// scripted stream/datagram interactions in lockstep with the client.
async fn run_server(mut conn: WtServerConnection) -> Result<(), Box<dyn std::error::Error>> {
    // 1) The Extended CONNECT request arrives through the mixed accept pump.
    let accepted = conn.accept().await?;
    let Some(Accepted::Request(req, stream)) = accepted else {
        return Err("first accepted item was not a request".into());
    };
    assert_eq!(req.method(), http::Method::CONNECT);
    assert_eq!(req.uri().path(), "/wtp/rtc");
    let session = upgrade_connect(&mut conn, req, *stream).await?;

    // 2) Client-initiated bidi: echo the payload back and finish.
    let item = tokio::time::timeout(Duration::from_secs(10), conn.accept()).await??;
    let Some(Accepted::Bidi(bidi_session, mut bidi)) = item else {
        return Err("expected a webtransport bidi stream".into());
    };
    assert_eq!(bidi_session, session.id());
    assert!(conn.has_session(bidi_session));
    let ping = bidi.recv_data().await?.ok_or("client bidi closed early")?;
    assert_eq!(ping, Bytes::from_static(b"ping"));
    bidi.send_data(ping.clone()).await?;
    bidi.finish().await?;

    // 3) Client-initiated uni: drain to EOF.
    let uni = tokio::time::timeout(Duration::from_secs(10), conn.accept_uni()).await??;
    let Some((uni_session, mut uni)) = uni else {
        return Err("expected a webtransport uni stream".into());
    };
    assert_eq!(uni_session, session.id());
    let mut collected = BytesMut2::default();
    while let Some(chunk) = uni.recv_data().await? {
        collected.extend_from_slice(&chunk);
    }
    assert_eq!(collected.as_bytes(), b"client-uni");

    // 4) Server-initiated streams + datagram exchange.
    let owned_open = session.owned_bidi_opener();
    let mut sbi = owned_open().await?;
    sbi.send_data(Bytes::from_static(b"server-bi")).await?;
    sbi.finish().await?;
    let mut suni = session.open_uni().await?;
    suni.send_data(Bytes::from_static(b"server-uni")).await?;
    suni.finish().await?;
    session.send_datagram(Bytes::from_static(b"s2c-dgram"))?;
    let inbound = tokio::time::timeout(Duration::from_secs(10), session.read_datagram()).await??;
    assert_eq!(inbound, Bytes::from_static(b"c2s-dgram"));
    // The opener shares this exact session's close fence, even though it owns
    // a clone of the live underlying QUIC connection.
    session.streams().close();
    assert!(
        owned_open().await.is_err(),
        "closed session accepted a new owned stream"
    );
    Ok(())
}

/// Minimal growable byte sink (avoids a `bytes::BufMut` feature gap here).
#[derive(Default)]
struct BytesMut2(Vec<u8>);

impl BytesMut2 {
    fn extend_from_slice(&mut self, data: &[u8]) {
        self.0.extend_from_slice(data);
    }

    fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Client half: dial the session and mirror the server scenario. The session
/// is returned (not dropped) so the endpoint stays alive until the server has
/// drained everything we sent; dropping a Quinn connection discards unsent
/// datagrams, which would otherwise make the teardown racy.
async fn run_client(
    endpoint: &quinn::Endpoint,
    addr: std::net::SocketAddr,
) -> Result<WtClient, lk_webtransport::WtError> {
    let mut client = connect(endpoint, addr, "localhost", "/wtp/rtc").await?;

    // Client-initiated bidi round trip against the server echo.
    let mut bidi = client.session().open_bi().await?;
    bidi.send_data(Bytes::from_static(b"ping")).await?;
    let echo = bidi.recv_data().await?.expect("echo missing");
    assert_eq!(echo, Bytes::from_static(b"ping"));

    // Client-initiated uni stream, drained by the server.
    let mut uni = client.session().open_uni().await?;
    uni.send_data(Bytes::from_static(b"client-uni")).await?;
    uni.finish().await?;

    // Server-initiated bidi stream.
    let (_sid, mut sbi) = client.accept_bi().await?;
    let data = sbi.recv_data().await?.expect("server-bi empty");
    assert_eq!(data, Bytes::from_static(b"server-bi"));

    // Server-initiated uni stream.
    let Some((_sid, mut suni)) = client.accept_uni().await? else {
        panic!("expected a server-initiated uni stream");
    };
    let mut collected = Vec::new();
    while let Some(chunk) = suni.recv_data().await? {
        collected.extend_from_slice(&chunk);
    }
    assert_eq!(collected, b"server-uni");

    // Datagram round trip toward the server.
    client
        .session()
        .send_datagram(Bytes::from_static(b"c2s-dgram"))?;
    let inbound = client.session().read_datagram().await?;
    assert_eq!(inbound, Bytes::from_static(b"s2c-dgram"));
    Ok(client)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn webtransport_bidi_data_integrity_receiving_end() {
    // SPEC-0074 C15 (gate restored): a client write returning `Ok` proves
    // nothing about what the *peer* actually received. This test asserts the
    // receiving end's observation directly: the concatenated bytes the server
    // read to FIN must equal the ordered pattern the client sent, byte for
    // byte, with the exact length and a clean (FIN, not reset) termination.
    //
    // Three 16-byte chunks each carry a distinct sequence tag so a reorder or
    // a dropped/duplicated chunk is a content mismatch, not a pass. The server
    // echoes the bytes it received back to the client, which re-verifies them,
    // closing the loop over one real WebTransport bidi stream on a real UDP
    // socket over the pure-Rust QUIC stack.
    const CHUNK_A: &[u8] = b"c15-integrity-A0"; // 16 bytes
    const CHUNK_B: &[u8] = b"c15-integrity-B0"; // 16 bytes
    const CHUNK_C: &[u8] = b"c15-integrity-C0"; // 16 bytes
    let expected: Vec<u8> = [CHUNK_A, CHUNK_B, CHUNK_C].concat();
    assert_eq!(expected.len(), 48);

    let (socket, addr) = bound_socket();
    let tls = ServerTls::new(
        vec![
            CertificateDer::from_pem_slice(LEAF).unwrap(),
            CertificateDer::from_pem_slice(INTERMEDIATE).unwrap(),
        ],
        PrivateKeyDer::from_pem_slice(LEAF_KEY).unwrap(),
    );
    let scfg = server_config(&tls, b"integrity-token-master-key-0123456789").unwrap();
    let server = endpoint(scfg, b"integrity-reset-key", socket).unwrap();

    let want = expected.clone();
    // The server holds the h3 connection open until the client has read and
    // asserted the echo: dropping `WtServerConnection` (task return) sends a
    // CONNECTION_CLOSE, which would otherwise race the echo read. This is the
    // test synchronizing its own teardown, not a workaround for a server bug.
    let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
    let server_task = tokio::spawn(async move {
        let incoming = server.accept().await.expect("quic accept");
        let conn = incoming.await.expect("quic handshake");
        let mut h3 = WtServerConnection::new(conn).await.expect("h3 server");

        // Upgrade the CONNECT so the session's streams route.
        let Some(Accepted::Request(req, stream)) = h3.accept().await.expect("connect") else {
            panic!("first accepted item was not a request");
        };
        let session = upgrade_connect(&mut h3, req, *stream)
            .await
            .expect("upgrade");

        // Read the ordered chunks until the client FINs the send side.
        let Some(Accepted::Bidi(sid, mut bidi)) =
            tokio::time::timeout(Duration::from_secs(10), h3.accept())
                .await
                .expect("bidi accept deadline")
                .expect("bidi accept")
        else {
            panic!("expected a webtransport bidi stream");
        };
        assert_eq!(sid, session.id());
        let mut received: Vec<u8> = Vec::new();
        let mut chunks_seen = 0usize;
        loop {
            match tokio::time::timeout(Duration::from_secs(10), bidi.recv_data())
                .await
                .expect("recv deadline")
                .expect("recv")
            {
                // `Ok(None)` is the FIN: the send side terminated cleanly, so
                // the stream was not reset.
                None => break,
                Some(chunk) => {
                    chunks_seen += 1;
                    received.extend_from_slice(&chunk);
                }
            }
        }
        // Receiving-end assertions: exact content, length and termination
        // class, independent of whether the client's writes reported `Ok`.
        assert_eq!(received, want, "received bytes differ from sent pattern");
        assert_eq!(received.len(), want.len(), "received length differs");
        assert!(chunks_seen >= 1, "no chunk reached the receiver");
        // Echo the verified-received bytes back so the client re-confirms.
        bidi.send_data(Bytes::from(received.clone()))
            .await
            .expect("echo send");
        bidi.finish().await.expect("echo finish");
        // Hold the connection until the client confirms it read the echo.
        let _ = done_rx.await;
        received
    });

    let (socket, _client_addr) = bound_socket();
    let mut roots = RootCertStore::empty();
    roots
        .add(CertificateDer::from_pem_slice(ROOT).unwrap())
        .expect("root anchor");
    let cfg = client_config(roots).unwrap();
    let client = client_endpoint(cfg, b"client-integrity-reset-key", socket).unwrap();

    let echo = tokio::time::timeout(Duration::from_secs(30), async {
        let c = connect(&client, addr, "localhost", "/wtp/rtc")
            .await
            .expect("connect");
        let mut bidi = c.session().open_bi().await.expect("open bidi");
        for chunk in [CHUNK_A, CHUNK_B, CHUNK_C] {
            bidi.send_data(Bytes::from_static(chunk))
                .await
                .expect("send chunk");
        }
        bidi.finish().await.expect("finish send side");
        // Read the server's echo of what it actually received, to EOF.
        let mut echoed: Vec<u8> = Vec::new();
        loop {
            match tokio::time::timeout(Duration::from_secs(10), bidi.recv_data())
                .await
                .expect("echo recv deadline")
                .expect("echo recv")
            {
                None => break,
                Some(chunk) => echoed.extend_from_slice(&chunk),
            }
        }
        // Keep the endpoint alive until the server task drains.
        drop(bidi);
        (echoed, c)
    })
    .await
    .expect("client scenario deadline");

    let (echoed, _client) = echo;
    // Releasing the server's held connection now that the echo is verified.
    let _ = done_tx.send(());
    let received = tokio::time::timeout(Duration::from_secs(10), server_task)
        .await
        .expect("server deadline")
        .expect("server task");

    assert_eq!(echoed, expected, "echoed bytes differ from sent pattern");
    assert_eq!(
        received, expected,
        "server-received bytes differ from sent pattern"
    );
    assert_eq!(echoed.len(), 48, "receiving-end length must be exactly 48");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn webtransport_session_round_trip() {
    // --- server endpoint on a real UDP socket ---
    let (socket, addr) = bound_socket();
    let tls = ServerTls::new(
        vec![
            CertificateDer::from_pem_slice(LEAF).unwrap(),
            CertificateDer::from_pem_slice(INTERMEDIATE).unwrap(),
        ],
        PrivateKeyDer::from_pem_slice(LEAF_KEY).unwrap(),
    );
    let scfg = server_config(&tls, b"acceptance-token-master-key-0123456789").unwrap();
    let server = endpoint(scfg, b"acceptance-reset-key", socket).unwrap();
    let server_addr = addr;

    let server_task = tokio::spawn(async move {
        let incoming = server.accept().await.expect("quic accept");
        let conn = incoming.await.expect("quic handshake");
        let h3 = WtServerConnection::new(conn).await.expect("h3 server");
        run_server(h3).await.expect("server scenario");
    });

    // --- client endpoint trusting the pinned RSA root (real validation) ---
    let (socket, _client_addr) = bound_socket();
    let mut roots = RootCertStore::empty();
    roots
        .add(CertificateDer::from_pem_slice(ROOT).unwrap())
        .expect("root anchor");
    let cfg = client_config(roots).unwrap();
    let client = client_endpoint(cfg, b"client-reset-key", socket).unwrap();

    let _session = tokio::time::timeout(Duration::from_secs(30), run_client(&client, server_addr))
        .await
        .expect("client scenario deadline")
        .expect("client scenario");
    // The client endpoint (and thus the QUIC connection) stays alive until the
    // server scenario has fully drained our streams and datagrams.
    tokio::time::timeout(Duration::from_secs(10), server_task)
        .await
        .expect("server deadline")
        .expect("server task");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn webtransport_extra_stream_reset_keeps_session_alive() {
    // SPEC-0074 C15 root-cause regression. The `reliable_prefix` matrix mode
    // exposed that a client `RESET_STREAM` on an *extra* WebTransport bidi
    // stream used to be treated as connection-fatal by the Rust server (the
    // whole h3 connection was torn down) while the Go server keeps the session
    // and simply drops the reset stream. The accept pump now classifies a
    // per-stream termination as non-fatal. This test drives the real accept
    // pump over a loopback socket: reset one extra stream, then prove a *later*
    // stream on the SAME session still round-trips - i.e. the session survived.
    //
    // Robust to where the reset surfaces: whether `accept()` skips the reset
    // stream (first-frame termination) or yields it and its read terminates,
    // the server must keep serving and deliver the post-reset stream.
    const WANT: &[u8] = b"c15-after-reset";

    let (socket, addr) = bound_socket();
    let tls = ServerTls::new(
        vec![
            CertificateDer::from_pem_slice(LEAF).unwrap(),
            CertificateDer::from_pem_slice(INTERMEDIATE).unwrap(),
        ],
        PrivateKeyDer::from_pem_slice(LEAF_KEY).unwrap(),
    );
    let scfg = server_config(&tls, b"reset-survive-token-master-key-0123456789").unwrap();
    let server = endpoint(scfg, b"reset-survive-reset-key", socket).unwrap();

    // The server holds the h3 connection open until the client has read the
    // echo: returning from the task would drop `WtServerConnection` and send a
    // CONNECTION_CLOSE that races the client's echo read.
    let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
    let server_task = tokio::spawn(async move {
        let incoming = server.accept().await.expect("quic accept");
        let conn = incoming.await.expect("quic handshake");
        let mut h3 = WtServerConnection::new(conn).await.expect("h3 server");
        let Some(Accepted::Request(req, stream)) = h3.accept().await.expect("connect") else {
            panic!("first accepted item was not a request");
        };
        upgrade_connect(&mut h3, req, *stream)
            .await
            .expect("upgrade");
        loop {
            let item = tokio::time::timeout(Duration::from_secs(10), h3.accept())
                .await
                .expect("accept deadline")
                .expect("a reset extra stream must not be connection-fatal");
            let Some(Accepted::Bidi(_sid, mut bidi)) = item else {
                continue;
            };
            match tokio::time::timeout(Duration::from_secs(10), bidi.recv_data())
                .await
                .expect("recv deadline")
            {
                Ok(Some(chunk)) if chunk.as_ref() == WANT => {
                    bidi.send_data(chunk).await.expect("echo send");
                    bidi.finish().await.expect("echo finish");
                    break;
                }
                // The reset (or a non-matching) extra stream: drop just this
                // stream and keep the session serving the next one.
                _ => continue,
            }
        }
        // Hold the connection until the client confirms it read the echo.
        let _ = done_rx.await;
    });

    let (socket, _caddr) = bound_socket();
    let mut roots = RootCertStore::empty();
    roots
        .add(CertificateDer::from_pem_slice(ROOT).unwrap())
        .expect("root anchor");
    let cfg = client_config(roots).unwrap();
    let client = client_endpoint(cfg, b"client-reset-survive-key", socket).unwrap();

    tokio::time::timeout(Duration::from_secs(30), async {
        let c = connect(&client, addr, "localhost", "/wtp/rtc")
            .await
            .expect("connect");
        // 1) An extra bidi the client writes and then resets.
        let mut doomed = c.session().open_bi().await.expect("open doomed bidi");
        doomed
            .send_data(Bytes::from_static(b"c15-prefix"))
            .await
            .expect("send prefix");
        doomed.reset(0x170d7b68);
        drop(doomed);
        // 2) A later bidi on the SAME session must still round-trip.
        let mut alive = c.session().open_bi().await.expect("open alive bidi");
        alive
            .send_data(Bytes::from_static(WANT))
            .await
            .expect("send alive");
        alive.finish().await.expect("finish alive");
        let echo = tokio::time::timeout(Duration::from_secs(10), alive.recv_data())
            .await
            .expect("echo deadline")
            .expect("echo recv")
            .expect("post-reset stream must echo, proving the session survived");
        assert_eq!(echo.as_ref(), WANT, "post-reset echo content mismatch");
        // Release the server's held connection now that the echo is verified.
        let _ = done_tx.send(());
        drop(alive);
        drop(c);
    })
    .await
    .expect("client scenario deadline");

    tokio::time::timeout(Duration::from_secs(10), server_task)
        .await
        .expect("server deadline")
        .expect("server task");
}
