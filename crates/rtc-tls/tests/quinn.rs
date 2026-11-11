//! Real `quinn_proto::Endpoint` handshake over the pure-Rust crypto adapter
//! (SPEC-0074 C15). No `ring`/`aws-lc`: the QUIC transport state machine is
//! Quinn's, but every cryptographic operation is served by
//! [`rtc_tls::quinn`], which delegates to the RFC 9001-verified
//! [`lk_quic_crypto`] core.
//!
//! This exercises the adapter end to end over an in-process (socket-less) UDP
//! datagram loopback: a completed handshake through the custom
//! `crypto::Session`, an application bidi-stream round trip and an application
//! datagram round trip (proving 1-RTT packet + header protection work for both
//! peers), plus the stateless-retry address-validation path (proving the
//! server `retry_tag`, the handshake-token AEAD and the client `is_valid_retry`
//! agree), and the endpoint stateless keys in isolation.

#![cfg(feature = "quinn")]

use std::{
    collections::{HashMap, VecDeque},
    net::{Ipv6Addr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::{Bytes, BytesMut};
use quinn_proto::{
    ClientConfig, Connection, ConnectionError, ConnectionEvent, ConnectionHandle, DatagramEvent,
    Endpoint, EndpointConfig, EndpointEvent, ReadError, ServerConfig, StreamId, Transmit,
    crypto::{HandshakeTokenKey, HmacKey},
};
use rtc_tls::quinn::{QuinnClientConfig, QuinnServerConfig, ResetKey, TokenKey};
use rtc_tls::rustls::{
    RootCertStore,
    pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
};

// Long-validity public fixtures: RSA root -> intermediate -> P-256 `localhost`
// leaf (see fixtures/expanded/README.md), the same chain as tests/quic.rs.
const ROOT: &[u8] = include_bytes!("fixtures/expanded/root-rsa.pem");
const LEAF: &[u8] = include_bytes!("fixtures/expanded/p256.pem");
const INTERMEDIATE: &[u8] = include_bytes!("fixtures/expanded/intermediate.pem");
const LEAF_KEY: &[u8] = include_bytes!("fixtures/expanded/p256-key.pem");

const ALPN: &[u8] = b"h3";
const MAX_DATAGRAMS: usize = 10;

#[derive(Copy, Clone, PartialEq)]
enum Policy {
    /// Accept the first Initial (no address validation).
    Accept,
    /// Issue a stateless Retry until the client's address is validated.
    Retry,
}

/// One endpoint plus the connections it currently owns and its inbound queue.
struct Side {
    endpoint: Endpoint,
    addr: SocketAddr,
    connections: HashMap<ConnectionHandle, Connection>,
    conn_events: HashMap<ConnectionHandle, VecDeque<ConnectionEvent>>,
    poll_events: HashMap<ConnectionHandle, VecDeque<quinn_proto::Event>>,
    outbound: VecDeque<(Transmit, Bytes)>,
    inbound: VecDeque<(Instant, BytesMut)>,
    accepted: Option<Result<ConnectionHandle, ConnectionError>>,
    timeout: Option<Instant>,
    policy: Policy,
}

impl Side {
    fn new(endpoint: Endpoint, addr: SocketAddr, policy: Policy) -> Self {
        Self {
            endpoint,
            addr,
            connections: HashMap::new(),
            conn_events: HashMap::new(),
            poll_events: HashMap::new(),
            outbound: VecDeque::new(),
            inbound: VecDeque::new(),
            accepted: None,
            timeout: None,
            policy,
        }
    }

    /// Deliver every inbound datagram, then drain all outgoing transmits.
    fn drive(&mut self, now: Instant, peer_addr: SocketAddr) {
        self.drive_inbound(now, peer_addr);
        self.drive_outbound(now);
    }

    fn drive_inbound(&mut self, now: Instant, peer_addr: SocketAddr) {
        let buffer_size = self.endpoint.config().get_max_udp_payload_size() as usize;
        let mut buf = Vec::with_capacity(buffer_size);
        while self.inbound.front().is_some_and(|x| x.0 <= now) {
            let (recv_time, packet) = self.inbound.pop_front().unwrap();
            if let Some(event) = self
                .endpoint
                .handle(recv_time, peer_addr, None, None, packet, &mut buf)
            {
                match event {
                    DatagramEvent::NewConnection(incoming) => match self.policy {
                        Policy::Accept => {
                            self.try_accept(incoming, now);
                        }
                        Policy::Retry if incoming.remote_address_validated() => {
                            self.try_accept(incoming, now);
                        }
                        Policy::Retry => self.retry(incoming),
                    },
                    DatagramEvent::ConnectionEvent(ch, event) => {
                        self.conn_events.entry(ch).or_default().push_back(event);
                    }
                    DatagramEvent::Response(transmit) => {
                        let size = transmit.size;
                        self.outbound.extend(split_transmit(transmit, &buf[..size]));
                        buf.clear();
                    }
                }
            }
        }
    }

    fn drive_outbound(&mut self, now: Instant) {
        let buffer_size = self.endpoint.config().get_max_udp_payload_size() as usize;
        let mut buf = Vec::with_capacity(buffer_size);
        loop {
            let mut endpoint_events: Vec<(ConnectionHandle, EndpointEvent)> = vec![];
            for (ch, conn) in self.connections.iter_mut() {
                if self.timeout.is_some_and(|t| t <= now) {
                    self.timeout = None;
                    conn.handle_timeout(now);
                }
                for (_, mut events) in self.conn_events.drain() {
                    for event in events.drain(..) {
                        conn.handle_event(event);
                    }
                }
                while let Some(event) = conn.poll_endpoint_events() {
                    endpoint_events.push((*ch, event));
                }
                while let Some(event) = conn.poll() {
                    self.poll_events.entry(*ch).or_default().push_back(event);
                }
                while let Some(transmit) = conn.poll_transmit(now, MAX_DATAGRAMS, &mut buf) {
                    let size = transmit.size;
                    self.outbound.extend(split_transmit(transmit, &buf[..size]));
                    buf.clear();
                }
                self.timeout = conn.poll_timeout();
            }
            if endpoint_events.is_empty() {
                break;
            }
            for (ch, event) in endpoint_events {
                if let Some(event) = self.endpoint.handle_event(ch, event)
                    && let Some(conn) = self.connections.get_mut(&ch)
                {
                    conn.handle_event(event);
                }
            }
        }
    }

    fn try_accept(&mut self, incoming: quinn_proto::Incoming, now: Instant) {
        let mut buf = Vec::new();
        match self.endpoint.accept(incoming, now, &mut buf, None) {
            Ok((ch, conn)) => {
                self.connections.insert(ch, conn);
                self.accepted = Some(Ok(ch));
            }
            Err(err) => {
                if let Some(transmit) = err.response {
                    let size = transmit.size;
                    self.outbound.extend(split_transmit(transmit, &buf[..size]));
                }
                self.accepted = Some(Err(err.cause));
            }
        }
    }

    fn retry(&mut self, incoming: quinn_proto::Incoming) {
        let mut buf = Vec::new();
        let transmit = self.endpoint.retry(incoming, &mut buf).unwrap();
        let size = transmit.size;
        self.outbound.extend(split_transmit(transmit, &buf[..size]));
    }
}

/// An in-process client/server Quinn endpoint pair exchanging real QUIC
/// datagrams (no OS sockets), mirroring Quinn's own internal test harness but
/// built solely from its public API.
struct Pair {
    client: Side,
    server: Side,
    time: Instant,
}

impl Pair {
    fn new(policy: Policy) -> Self {
        let now = Instant::now();
        let server_addr = SocketAddr::new(Ipv6Addr::LOCALHOST.into(), 4433);
        let client_addr = SocketAddr::new(Ipv6Addr::LOCALHOST.into(), 44433);

        // Server: custom crypto ServerConfig + a randomized handshake token key.
        let cert_chain = vec![
            CertificateDer::from_pem_slice(LEAF).unwrap(),
            CertificateDer::from_pem_slice(INTERMEDIATE).unwrap(),
        ];
        let key = PrivateKeyDer::from_pem_slice(LEAF_KEY).unwrap();
        // Both ends advertise `reset_stream_at`, the way the WebTransport listener
        // does, so every handshake here also proves an unknown transport parameter
        // is skipped by length instead of aborting the connection (RFC 9000 §7.3).
        let server_crypto = Arc::new(
            QuinnServerConfig::new(cert_chain, key, vec![ALPN.to_vec()])
                .unwrap()
                .with_reset_stream_at(),
        ) as Arc<dyn quinn_proto::crypto::ServerConfig>;
        let token_key = Arc::new(TokenKey::new(
            b"deterministic-test-master-secret-0123456789",
        )) as Arc<dyn quinn_proto::crypto::HandshakeTokenKey>;
        let server_cfg = ServerConfig::new(server_crypto, token_key);
        let server_endpoint = Endpoint::new(
            Arc::new(EndpointConfig::new(
                Arc::new(ResetKey::new(b"server-reset-key"))
                    as Arc<dyn quinn_proto::crypto::HmacKey>,
            )),
            Some(Arc::new(server_cfg)),
            true,
            Some([7u8; 32]),
        );

        // Client endpoint carries no server config; the ClientConfig is supplied
        // per-connect via `Pair::client_config`.
        let client_endpoint = Endpoint::new(
            Arc::new(EndpointConfig::new(
                Arc::new(ResetKey::new(b"client-reset-key"))
                    as Arc<dyn quinn_proto::crypto::HmacKey>,
            )),
            None,
            true,
            Some([9u8; 32]),
        );

        Self {
            client: Side::new(client_endpoint, client_addr, Policy::Accept),
            server: Side::new(server_endpoint, server_addr, policy),
            time: now,
        }
    }

    fn client_config(&self) -> ClientConfig {
        let mut roots = RootCertStore::empty();
        roots
            .add(CertificateDer::from_pem_slice(ROOT).unwrap())
            .unwrap();
        let crypto = Arc::new(
            QuinnClientConfig::with_root_certificates(roots, vec![ALPN.to_vec()])
                .unwrap()
                .with_reset_stream_at(),
        ) as Arc<dyn quinn_proto::crypto::ClientConfig>;
        ClientConfig::new(crypto)
    }

    /// Advance both endpoints until the datagram queues go quiet (bounded).
    fn drive(&mut self) {
        for _ in 0..1000 {
            self.client.drive(self.time, self.server.addr);
            deliver(&mut self.client, &mut self.server, self.time);
            self.server.drive(self.time, self.client.addr);
            deliver(&mut self.server, &mut self.client, self.time);
            // `Connection::is_idle` is crate-private, so settle on queue
            // quiescence: once no datagram is queued to send or awaiting
            // delivery, the exchange has converged.
            let quiet = self.client.outbound.is_empty()
                && self.client.inbound.is_empty()
                && self.server.outbound.is_empty()
                && self.server.inbound.is_empty();
            if quiet {
                return;
            }
            // Nudge timers forward so idle/PTO paths make progress.
            self.time += Duration::from_millis(1);
        }
        panic!("Pair did not reach idle within the step budget");
    }

    fn connect(&mut self) -> (ConnectionHandle, ConnectionHandle) {
        let cfg = self.client_config();
        let (client_ch, conn) = self
            .client
            .endpoint
            .connect(self.time, cfg, self.server.addr, "localhost")
            .expect("begin connect");
        self.client.connections.insert(client_ch, conn);
        self.drive();
        let server_ch = self
            .server
            .accepted
            .take()
            .expect("server never tried to accept")
            .expect("server accept errored");
        // Both sessions must report a completed handshake.
        assert_matches_connected(
            self.client.connections.get_mut(&client_ch).unwrap(),
            self.client.poll_events.entry(client_ch).or_default(),
        );
        assert_matches_connected(
            self.server.connections.get_mut(&server_ch).unwrap(),
            self.server.poll_events.entry(server_ch).or_default(),
        );
        (client_ch, server_ch)
    }
}

fn deliver(from: &mut Side, to: &mut Side, now: Instant) {
    for (packet, buffer) in from.outbound.drain(..) {
        if packet.destination == to.addr {
            to.inbound.push_back((now, buffer.into()));
        }
    }
}

fn assert_matches_connected(conn: &mut Connection, events: &mut VecDeque<quinn_proto::Event>) {
    let mut saw_ready = false;
    let mut saw_connected = false;
    while let Some(event) = events.pop_front() {
        match event {
            quinn_proto::Event::HandshakeDataReady => saw_ready = true,
            quinn_proto::Event::Connected => saw_connected = true,
            other => panic!("unexpected handshake event: {other:?}"),
        }
    }
    assert!(!conn.is_handshaking(), "handshake did not complete");
    assert!(saw_ready, "no HandshakeDataReady event");
    assert!(saw_connected, "no Connected event");
}

fn split_transmit(transmit: Transmit, buffer: &[u8]) -> Vec<(Transmit, Bytes)> {
    let buffer = Bytes::copy_from_slice(buffer);
    match transmit.segment_size {
        None => vec![(transmit, buffer)],
        Some(segment_size) => {
            let mut buffer = buffer;
            let mut out = Vec::new();
            while !buffer.is_empty() {
                let end = segment_size.min(buffer.len());
                let contents = buffer.split_to(end);
                out.push((
                    Transmit {
                        destination: transmit.destination,
                        size: contents.len(),
                        ecn: transmit.ecn,
                        segment_size: None,
                        src_ip: transmit.src_ip,
                    },
                    contents,
                ));
            }
            out
        }
    }
}

/// Collect everything a receive stream currently has available. With no
/// finished bit pending, `Chunks::next` surfaces exhaustion as `Err(Blocked)`,
/// which is the loop's normal termination here.
fn read_all(conn: &mut Connection, id: StreamId) -> Vec<u8> {
    let mut out = Vec::new();
    let mut recv = conn.recv_stream(id);
    let mut chunks = recv.read(true).unwrap();
    loop {
        match chunks.next(usize::MAX) {
            Ok(Some(chunk)) => out.extend_from_slice(&chunk.bytes),
            Ok(None) | Err(ReadError::Blocked) => break,
            Err(e) => panic!("stream read failed: {e:?}"),
        }
    }
    let _ = chunks.finalize();
    out
}

#[test]
fn quinn_endpoint_handshake_streams_and_datagrams_over_pure_rust_crypto() {
    let mut pair = Pair::new(Policy::Accept);
    let (client_ch, server_ch) = pair.connect();

    // --- Application bidi stream: client -> server -> client (echo) ---------
    let stream = pair
        .client
        .connections
        .get_mut(&client_ch)
        .unwrap()
        .streams()
        .open(quinn_proto::Dir::Bi)
        .expect("open bidi");
    pair.client
        .connections
        .get_mut(&client_ch)
        .unwrap()
        .send_stream(stream)
        .write(b"ping")
        .unwrap();
    pair.drive();

    let accepted = pair
        .server
        .connections
        .get_mut(&server_ch)
        .unwrap()
        .streams()
        .accept(quinn_proto::Dir::Bi)
        .expect("server accepts the client-initiated bidi");
    assert_eq!(accepted, stream);
    let got = read_all(
        pair.server.connections.get_mut(&server_ch).unwrap(),
        accepted,
    );
    assert_eq!(
        got, b"ping",
        "bidi request did not traverse 1-RTT protection"
    );

    // Server replies on the same bidi's send side.
    pair.server
        .connections
        .get_mut(&server_ch)
        .unwrap()
        .send_stream(accepted)
        .write(b"pong")
        .unwrap();
    pair.drive();
    let reply = read_all(pair.client.connections.get_mut(&client_ch).unwrap(), stream);
    assert_eq!(
        reply, b"pong",
        "bidi response did not traverse 1-RTT protection"
    );

    // --- Application datagram: server -> client -----------------------------
    pair.server
        .connections
        .get_mut(&server_ch)
        .unwrap()
        .datagrams()
        .send(Bytes::from_static(b"dg-1"), true)
        .expect("server datagram send");
    pair.drive();
    let dg = pair
        .client
        .connections
        .get_mut(&client_ch)
        .unwrap()
        .datagrams()
        .recv()
        .expect("client received the datagram");
    assert_eq!(
        &dg[..],
        b"dg-1",
        "datagram did not traverse 1-RTT protection"
    );
}

#[test]
fn quinn_stateless_retry_address_validation_round_trips() {
    // With the Retry policy the server issues a stateless Retry whose token is
    // produced by the handshake-token AEAD and whose integrity tag is computed
    // by ServerConfig::retry_tag; the client validates it via Session::
    // is_valid_retry and reconnects with the token. A successful connect under
    // this policy proves the full retry path is wired through pure-Rust crypto.
    let mut pair = Pair::new(Policy::Retry);
    let (client_ch, server_ch) = pair.connect();
    // Still functional after the extra retry round trip.
    let stream = pair
        .client
        .connections
        .get_mut(&client_ch)
        .unwrap()
        .streams()
        .open(quinn_proto::Dir::Bi)
        .expect("open bidi after retry");
    pair.client
        .connections
        .get_mut(&client_ch)
        .unwrap()
        .send_stream(stream)
        .write(b"post-retry")
        .unwrap();
    pair.drive();
    // The server accepted the connection during connect, so its end exists;
    // read the stream.
    let accepted = pair
        .server
        .connections
        .get_mut(&server_ch)
        .unwrap()
        .streams()
        .accept(quinn_proto::Dir::Bi)
        .expect("server accepts post-retry bidi");
    let got = read_all(
        pair.server.connections.get_mut(&server_ch).unwrap(),
        accepted,
    );
    assert_eq!(got, b"post-retry");
}

#[test]
fn endpoint_stateless_keys_round_trip() {
    // ResetKey (HMAC-SHA256): sign/verify and reject tampered signatures.
    let reset = ResetKey::new(b"shared-reset-key-material");
    let mut sig = vec![0u8; reset.signature_len()];
    assert_eq!(reset.signature_len(), 32);
    reset.sign(b"stateless reset payload", &mut sig);
    reset
        .verify(b"stateless reset payload", &sig)
        .expect("reset signature verifies");
    sig[0] ^= 1;
    assert!(reset.verify(b"stateless reset payload", &sig).is_err());

    // TokenKey: HKDF-Expand -> AES-256-GCM; seal/open round trip with AAD.
    let token = TokenKey::new(b"master-secret-for-handshake-tokens");
    let aead = token.aead_from_hkdf(b"random-connection-id-bytes");
    let mut data = b"retry token address binding".to_vec();
    let plain = data.clone();
    aead.seal(&mut data, b"aad").expect("token seal");
    assert_ne!(data, plain, "token seal must not be identity");
    let opened = aead.open(&mut data, b"aad").expect("token open");
    assert_eq!(opened, &plain[..]);
    assert!(
        aead.open(&mut data, b"wrong-aad").is_err(),
        "token AEAD must reject a wrong AAD"
    );
}

#[test]
fn quinn_packet_and_header_key_newtypes_round_trip_initial_keys() {
    // Independently exercise the adapter's Initial key derivation and the
    // `PacketKey`/`HeaderKey` newtypes: derive the server-side Initial keys for
    // a known DCID and confirm a packet + header protected under the local keys
    // is exactly reversed by the remote keys — all through `lk_quic_crypto`.
    use quinn_proto::crypto::ServerConfig as _;

    let cert_chain = vec![
        CertificateDer::from_pem_slice(LEAF).unwrap(),
        CertificateDer::from_pem_slice(INTERMEDIATE).unwrap(),
    ];
    let key = PrivateKeyDer::from_pem_slice(LEAF_KEY).unwrap();
    let server_cfg = QuinnServerConfig::new(cert_chain, key, vec![ALPN.to_vec()]).unwrap();

    let dst_cid = quinn_proto::ConnectionId::new(&[0x83, 0x94, 0xc8, 0xf0, 0x3e, 0x51, 0x57, 0x08]);
    let keys = server_cfg
        .initial_keys(1, &dst_cid)
        .expect("version 1 initial keys");

    assert_eq!(keys.packet.local.tag_len(), 16);
    let header = b"initial-header-prefix".to_vec();
    let plaintext = b"an initial-protected CRYPTO frame".to_vec();
    // Build a buf whose first header_len bytes are AAD and whose remainder is
    // plaintext + tag space, exactly as the transport hands it to `encrypt`.
    let header_len = header.len();
    // Same-direction round trip per QUIC key-role convention: the client's
    // write keys (server-side `remote`) protect, the matching read keys (the
    // client's own `local` view) open them. Across one KeyPair, `local`
    // encrypts and `remote` decrypts *different* directions, so each key must
    // seal and open its own traffic.
    let mut buf = header.clone();
    buf.extend_from_slice(&plaintext);
    buf.resize(buf.len() + keys.packet.local.tag_len(), 0);
    keys.packet.local.encrypt(3, &mut buf, header_len);
    let mut payload = BytesMut::from(&buf[header_len..]);
    keys.packet
        .local
        .decrypt(3, &header, &mut payload)
        .expect("initial local round trip");
    assert_eq!(&payload[..], &plaintext[..]);
    let mut buf2 = header.clone();
    buf2.extend_from_slice(&plaintext);
    buf2.resize(buf2.len() + keys.packet.remote.tag_len(), 0);
    keys.packet.remote.encrypt(4, &mut buf2, header_len);
    let mut payload2 = BytesMut::from(&buf2[header_len..]);
    keys.packet
        .remote
        .decrypt(4, &header, &mut payload2)
        .expect("initial remote round trip");
    assert_eq!(&payload2[..], &plaintext[..]);
    // Cross-direction must fail: distinct write keys per direction.
    let mut xf = BytesMut::from(&buf[header_len..]);
    assert!(
        keys.packet.remote.decrypt(3, &header, &mut xf).is_err(),
        "remote key must not open local-key traffic"
    );

    // Header protection, driven the way the transport drives it: `pn_offset` is
    // the position of the packet-number field, the buffer spans four bytes past
    // it and the 16-byte sample starts at `pn_offset + 4` (quinn-proto
    // `packet.rs:209`, `packet.rs:496-500`).
    fn hex(s: &str) -> Vec<u8> {
        let s: String = s.chars().filter(|c| !c.is_whitespace()).collect();
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }
    let mut pkt = hex("c10000000108f067a5502a4262b500407500"); // PN at 18, length tag 2
    assert_eq!(pkt.len(), 18);
    pkt.extend_from_slice(&[0x5au8; 120]); // ciphertext || tag || padding: the sample
    let before = pkt.clone();
    keys.header.local.encrypt(18, &mut pkt);
    assert_eq!(
        pkt[0] & 0xc0,
        0xc0,
        "long-header fixed/type bits altered ({:02x})",
        pkt[0]
    );
    // Only the first byte and the two packet-number bytes move. The ciphertext
    // the AEAD tag was computed over does not: that is the whole RFC 9001
    // §5.4.1 width rule, and the deviation from quinn-proto's own adapter.
    assert_eq!(
        pkt.iter()
            .zip(&before)
            .enumerate()
            .filter(|(i, (a, b))| *i != 0 && a != b)
            .map(|(i, _)| i)
            .collect::<Vec<_>>(),
        vec![18, 19],
        "header protection masked the wrong region"
    );
    keys.header.local.decrypt(18, &mut pkt);
    assert_eq!(pkt, before, "header not restored by the same key");
}

/// RFC 9001 Appendix A.3 -- the published Server Initial (20-byte header, a
/// **two-byte** packet number, 99-byte payload) opened and re-protected through
/// the adapter's `HeaderKey`/`PacketKey` newtypes.
///
/// A same-stack round trip cannot detect a header-protection *width* deviation:
/// masking bytes past the packet number reverses itself, so both peers agree
/// while a conformant third party reads a corrupted ciphertext. RFC 9001
/// §5.4.1 protects only the low bits of the first byte plus the packet number,
/// and the AEAD tag is computed before protection is applied, so a wider mask
/// destroys the tag check. quic-go sends client Initials with a two-byte packet
/// number -- exactly this case (measured on the wire in
/// `tests/interop/agent-wt-diff`, where its packets produced a server that never
/// answered).
///
/// Quinn hands `HeaderKey` the buffer that `header_data || payload` forms at the
/// moment the packet number is parsed, i.e. the packet up to `pn_offset + 4`
/// plus enough payload for the 16-byte sample, so the sample here is the wire
/// bytes at `header_data.len() + 2` -- which is the RFC's own sample for this
/// packet (`pn_offset + 4` == `header_data.len() + pn_len`, because the packet
/// number ends the header).
#[test]
fn quinn_header_key_matches_rfc9001_a3_whole_packet() {
    use quinn_proto::crypto::ServerConfig as _;

    fn hex(s: &str) -> Vec<u8> {
        let s: String = s.chars().filter(|c| !c.is_whitespace()).collect();
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    let cert_chain = vec![
        CertificateDer::from_pem_slice(LEAF).unwrap(),
        CertificateDer::from_pem_slice(INTERMEDIATE).unwrap(),
    ];
    let key = PrivateKeyDer::from_pem_slice(LEAF_KEY).unwrap();
    let server_cfg = QuinnServerConfig::new(cert_chain, key, vec![ALPN.to_vec()]).unwrap();
    let dst_cid = quinn_proto::ConnectionId::new(&[0x83, 0x94, 0xc8, 0xf0, 0x3e, 0x51, 0x57, 0x08]);
    // Server-side Initial keys: `remote` is the client direction, `local` the
    // server's own (rustls `Side::Server` maps local -> server secret).
    let keys = server_cfg
        .initial_keys(1, &dst_cid)
        .expect("version 1 initial keys");

    let header = hex(lk_quic_crypto::vectors::A3_SERVER_INITIAL_HEADER);
    let payload = hex(lk_quic_crypto::vectors::A3_SERVER_INITIAL_PAYLOAD);
    let wire = hex(lk_quic_crypto::vectors::A3_SERVER_INITIAL_PROTECTED);
    let (pn_offset, header_len) = (18usize, header.len());
    assert_eq!(header_len, 20);
    assert_eq!(header[pn_offset..], [0x00, 0x01], "packet number");
    assert_eq!(
        header[pn_offset - 2..pn_offset],
        [0x40, 0x75],
        "Length field"
    );
    let length =
        u16::from_be_bytes([header[pn_offset - 2], header[pn_offset - 1]]) as usize & 0x3fff;
    assert_eq!(length, 2 + payload.len() + 16, "A.3 length");
    assert_eq!(wire.len(), header_len + payload.len() + 16);

    // Quinn hands `HeaderKey` the packet buffer with the sample at
    // `pn_offset + 4`; for this vector the packet number ends the header, so
    // that is the RFC's own sample position and these bytes are verbatim.
    let mut buf = wire[..pn_offset + 4 + 16].to_vec();
    keys.header.local.decrypt(pn_offset, &mut buf);
    assert_eq!(buf[0], 0xc1, "first byte not restored");
    assert_eq!(&buf[pn_offset..header_len], &[0x00, 0x01], "packet number");
    assert_eq!(
        &buf[header_len..],
        &wire[header_len..pn_offset + 20],
        "header protection altered the ciphertext, which breaks a conformant peer's tag check"
    );
    let mut plain = BytesMut::from(&wire[header_len..]);
    keys.packet
        .local
        .decrypt(1, &header, &mut plain)
        .expect("RFC 9001 A.3 payload must open with the adapter keys");
    assert_eq!(
        &plain[..],
        &payload[..],
        "decrypted payload differs from A.3"
    );

    // --- re-protect it: byte-exact RFC wire bytes, and nothing else ---------
    let mut out = header.to_vec();
    out.extend_from_slice(&wire[header_len..pn_offset + 4 + 16]);
    keys.header.local.encrypt(pn_offset, &mut out);
    assert_eq!(
        out,
        wire[..pn_offset + 4 + 16],
        "protected packet differs from the RFC's bytes"
    );

    // A flipped ciphertext byte must be rejected, so the width rule is not
    // "mask whatever is nearby until the tag happens to verify".
    let mut bad = wire.clone();
    bad[header_len + 8] ^= 1;
    let mut plain = BytesMut::from(&bad[header_len..]);
    assert!(
        keys.packet.local.decrypt(1, &header, &mut plain).is_err(),
        "tampered ciphertext must not open"
    );
}

/// The masked region is exactly the packet number, for every length tag.
#[test]
fn quinn_header_key_width_follows_packet_number_length() {
    use quinn_proto::crypto::ServerConfig as _;

    fn hex(s: &str) -> Vec<u8> {
        let s: String = s.chars().filter(|c| !c.is_whitespace()).collect();
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    let cert_chain = vec![
        CertificateDer::from_pem_slice(LEAF).unwrap(),
        CertificateDer::from_pem_slice(INTERMEDIATE).unwrap(),
    ];
    let key = PrivateKeyDer::from_pem_slice(LEAF_KEY).unwrap();
    let server_cfg = QuinnServerConfig::new(cert_chain, key, vec![ALPN.to_vec()]).unwrap();
    let dst_cid = quinn_proto::ConnectionId::new(&[0x01; 8]);
    let keys = server_cfg
        .initial_keys(1, &dst_cid)
        .expect("version 1 initial keys");

    for pn_len in 1..=4usize {
        let pn_offset = 18usize;
        // A well-formed Initial long header (version, DCID, SCID, a 2-byte Length
        // field), so every byte before the packet number carries a plausible
        // value and any write into the header or the ciphertext shows up.
        let mut packet = hex("c0000000010008f067a5502a4262b5004075");
        assert_eq!(packet.len(), pn_offset);
        packet[0] = 0xc0 | (pn_len as u8 - 1);
        packet.extend_from_slice(&[0x11, 0x22, 0x33, 0x44]); // packet number field
        packet.extend_from_slice(&[0x77u8; 64]); // ciphertext || tag || padding
        let before = packet.clone();
        keys.header.local.encrypt(pn_offset, &mut packet);
        assert_eq!(
            packet[0] & 0xc0,
            0xc0,
            "pn_len={pn_len}: fixed/type bits altered ({:02x})",
            packet[0]
        );
        let masked = packet
            .iter()
            .zip(&before)
            .enumerate()
            .filter(|&(i, (a, b))| i != 0 && a != b)
            .map(|(i, _)| i)
            .collect::<Vec<_>>();
        assert_eq!(
            masked,
            (pn_offset..pn_offset + pn_len).collect::<Vec<_>>(),
            "pn_len={pn_len}: masked region was {masked:?}"
        );
        keys.header.local.decrypt(pn_offset, &mut packet);
        assert_eq!(packet, before, "pn_len={pn_len}: not reversed");
    }
}
